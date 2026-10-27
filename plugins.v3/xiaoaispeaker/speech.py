"""
speech.py — 语音控制：会话轮询、去重与口令执行。

小爱没有"把第三方插件当技能"的入口，能用的只有云端那份对话记录：用户说完话，
小米云端做 ASR + NLP，音箱先按小爱自己的理解回答，插件再从
`userprofile.mina.mi.com/device_profile/v2/conversation` 读到这句话，打断它正在放的
回答，改放我们要放的东西。所以这一层的三个难点全是"读记录"带来的：

1. **什么时候算新话**。每条记录只有服务端时间戳，本机时钟不可信，所以基线只能用
   服务端返回的时间（`primed` 之前不处理任何一条），而且"取记录失败"必须与
   "这轮没有记录"分开 —— 把失败当空记录会把那段时间说的话永久吞掉。
2. **同一条记录会被反复读到**。接口一次给 5 条，相邻两轮必然重叠，所以按
   `作用域 + request_id 或 (时间戳, 文本)` 做 LRU 去重。
3. **说话到播放之间有小爱的回答**。命中口令就先 pause+stop 抢下声道（`interrupt`），
   需要时先播一句"正在搜索"，否则会听到它在找歌的同时我们的歌已经在响。

轮询节奏与上游一致：默认 2 秒，失败退避到 30 秒封顶，空闲加倍（开了 AI 对话时
不加倍，因为对话的追问窗口依赖及时轮询）。轮询由宿主的周期任务驱动，插件不自建线程。

口令表与匹配在 voice.py，选曲打分在 text.py/library.py，本模块只负责"听到 → 决定 →
交给设备"，因此可以拿一个假的设备对象完整测通。
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

# 与上游 conversation_poll_interval 的量纲一致：2 秒起步，失败最多退到 30 秒
DEFAULT_POLL_SECONDS = 2.0
MIN_POLL_SECONDS = 1.0
MAX_POLL_SECONDS = 30.0
IDLE_POLL_SECONDS = 5.0
# 侧栏页要能看到"小爱最近听到了什么"，留这么几条在内存里
RECORD_BUFFER = 50
# 去重表上限（上游 LRU 1000）：一台音箱一天说不了几百句，超出后丢最旧的
SEEN_LIMIT = 1000


class SpeechListener:
    """按设备轮询对话记录，把新说的话交给 `handler`。

    `handler(text, device_id, hardware)` 由插件实现，返回要播报的话（空串表示不播）。
    """

    def __init__(self, handler: Callable[[str, str, str], str],
                 get_client: Callable[[], Tuple[Optional[Any], str]],
                 get_devices: Callable[[], List[Dict[str, Any]]],
                 speak: Optional[Callable[[str, str], None]] = None,
                 interval: float = DEFAULT_POLL_SECONDS,
                 chat_active: Any = False) -> None:
        self._handler = handler
        self._get_client = get_client
        self._get_devices = get_devices
        self._speak_fn = speak or (lambda _device_id, _text: None)
        self._interval = _clamp_interval(interval)
        self._chat_active = chat_active
        self._lock = threading.Lock()
        self._baseline: Dict[str, int] = {}
        self._primed: Dict[str, bool] = {}
        self._seen: "OrderedDict[str, float]" = OrderedDict()
        self._records: List[Dict[str, Any]] = []
        self._failures = 0
        self._next_at = 0.0
        self._handled = 0
        self._last_error = ""
        self._last_poll_at = 0.0

    # ---------------- 配置 ----------------

    def configure(self, interval: float, chat_active: Any = None) -> None:
        self._interval = _clamp_interval(interval)
        if chat_active is not None:
            self._chat_active = chat_active

    @property
    def handled(self) -> int:
        return self._handled

    def status(self) -> Dict[str, Any]:
        return {
            "interval": round(self._interval, 1),
            "next_in": max(0.0, round(self._next_at - time.time(), 1)),
            "handled": self._handled,
            "devices": len(self._baseline),
            "error": self._last_error,
            "last_poll_at": _fmt(self._last_poll_at),
        }

    def recent(self, limit: int = RECORD_BUFFER) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._records[-limit:][::-1])

    # ---------------- 轮询 ----------------

    def tick(self, devices: Optional[List[Dict[str, Any]]] = None) -> int:
        """宿主周期任务调一次。返回本轮处理的记录条数（0 也可能是"还没到点"）。"""
        now = time.time()
        if now < self._next_at:
            return 0
        self._last_poll_at = now
        client, error = self._get_client()
        if client is None:
            self._backoff(error or "未登录")
            return 0
        rows = devices if devices is not None else self._get_devices()
        handled = 0
        for device in rows or []:
            device_id = str(device.get("device_id") or "")
            if not device_id:
                continue
            try:
                records = client.get_latest_ask(device_id, str(device.get("hardware") or ""))
            except Exception as err:  # 鉴权失效之类，整轮退避但不打断其它设备
                # 异常原文多半是英文的 HTTP 串，页面上直接挂出来就是一句英文提示
                name = str(device.get("name") or device.get("alias") or "").strip() or device_id[:8]
                self._last_error = f"{name}：读不到对话记录（{err}）"
                continue
            if records is None:
                # 取不到 ≠ 没有记录：基线不推进，下一轮还会看到这些话
                self._last_error = f"{device_id}: 读对话记录失败"
                continue
            handled += self._consume(device_id, records)
        if self._last_error:
            self._backoff(self._last_error)
            return handled
        self._failures = 0
        # 空闲降频：没有 AI 对话在等追问时才拉长间隔
        gap = self._interval if (handled or self._chat_on()) else min(
            self._interval * 2, IDLE_POLL_SECONDS)
        # 从本轮**结束**起算，不是从开始：一轮问小米要几秒，从开始算的话慢轮一跑完
        # 「下次时间」就已经在过去，宿主下一次 tick 立刻又发一轮 —— 配 2 秒实际是贴着
        # 打，两轮还会重叠，刷出一堆 skipped 告警。
        self._next_at = time.time() + gap
        return handled

    def _chat_on(self) -> bool:
        """对话窗口是否还开着：`chat_active` 可以是布尔常量，也可以是插件给的可调用。"""
        value = self._chat_active
        return bool(value() if callable(value) else value)

    def _backoff(self, error: str) -> None:
        self._last_error = error
        self._failures += 1
        delay = min(MAX_POLL_SECONDS,
                    self._interval * (2 ** min(self._failures, 5)))
        self._next_at = time.time() + delay

    def reset(self) -> None:
        """换设备/改配置后调用：时间基线与去重表都不再适用。"""
        with self._lock:
            self._baseline.clear()
            self._primed.clear()
            self._seen.clear()
            self._next_at = 0.0
            self._failures = 0

    def forget_device(self, device_id: str) -> None:
        with self._lock:
            self._baseline.pop(device_id, None)
            self._primed.pop(device_id, None)

    def _consume(self, device_id: str, records: List[Dict[str, Any]]) -> int:
        handled = 0
        for record in records:
            timestamp = int(record.get("timestamp_ms") or 0)
            text = str(record.get("question") or "").strip()
            if not timestamp:
                continue
            if not self._primed.get(device_id):
                # 首轮只对齐基线：把登录前几小时说的话当成新命令会让音箱突然唱歌
                self._baseline[device_id] = timestamp
                self._primed[device_id] = True
                continue
            if timestamp <= int(self._baseline.get(device_id) or 0):
                continue
            self._baseline[device_id] = max(int(self._baseline.get(device_id) or 0), timestamp)
            if not text:
                continue
            key = f"{device_id}:{record.get('request_id') or f'{timestamp}:{text}'}"
            with self._lock:
                if key in self._seen:
                    continue
                self._seen[key] = time.time()
                while len(self._seen) > SEEN_LIMIT:
                    self._seen.popitem(last=False)
                self._records.append({"device_id": device_id, "at": _fmt(timestamp / 1000.0),
                                      "text": text, "answer": str(record.get("answer") or "")})
                self._records = self._records[-RECORD_BUFFER:]
            handled += 1
            self._handled += 1
            try:
                reply = self._handler(text, device_id, "")
            except Exception as err:
                self._last_error = f"处理口令失败: {err}"
                reply = ""
            if reply:
                self._speak(device_id, reply)
        return handled

    def _speak(self, device_id: str, text: str) -> None:
        try:
            self._speak_fn(device_id, text)
        except Exception as err:
            self._last_error = f"播报失败: {err}"


def _clamp_interval(value: Any) -> float:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return DEFAULT_POLL_SECONDS
    return max(MIN_POLL_SECONDS, min(MAX_POLL_SECONDS, seconds))


def _fmt(stamp: float) -> str:
    if not stamp:
        return ""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stamp))
