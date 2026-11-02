"""
speech.py — 对话监听：轮询、时间基线、去重与处理回调。

小爱没有"把第三方插件当技能"的入口，能用的只有云端那份对话记录：用户说完话，
小米云端做 ASR + NLP，音箱先按小爱自己的理解回答，插件再从
`userprofile.mina.mi.com/device_profile/v2/conversation` 读到这句话，打断它正在放的
回答，改放我们要放的东西。所以这一层的三个难点全是"读记录"带来的：

1. **什么时候算新话**。每条记录只有服务端时间戳，本机时钟不可信，所以基线只能用
   服务端返回的时间（`primed` 之前不处理任何一条），而且"取记录失败"必须与
   "这轮没有记录"分开 —— 把失败当空记录会把那段时间说的话永久吞掉。
2. **同一条记录会被反复读到**。接口一次给 5 条，相邻两轮必然重叠，所以按
   `作用域 + request_id 或 (时间戳, 文本)` 做 LRU 去重。
3. **说话到播放之间有小爱的回答**。插件回调按命中的动作决定是否抢占声道，
   点歌等动作可先 pause+stop，音量、播放方式等状态操作不打断当前歌曲。

默认每 2 秒轮询，失败退避到 30 秒封顶，成功后恢复配置间隔。
轮询由宿主周期任务驱动，同一监听器只允许一轮读取，AI 处理由回调独立分发。

口令表与匹配在 voice.py，选曲打分在 text.py/library.py。本模块把新记录交给 handler，
并把回执交给 speak；动作选择与设备控制由插件主类负责，可用替身回调独立验证监听行为。
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
                 interval: float = DEFAULT_POLL_SECONDS) -> None:
        self._handler = handler
        self._get_client = get_client
        self._get_devices = get_devices
        self._speak_fn = speak or (lambda _device_id, _text: None)
        self._interval = _clamp_interval(interval)
        self._lock = threading.Lock()
        self._poll_lock = threading.Lock()
        self._generation = 0
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

    def configure(self, interval: float) -> None:
        self._interval = _clamp_interval(interval)

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
        if not self._poll_lock.acquire(blocking=False):
            return 0
        try:
            return self._poll(devices)
        finally:
            self._poll_lock.release()

    def _poll(self, devices: Optional[List[Dict[str, Any]]]) -> int:
        """一次互斥的读取；配置重置后丢弃尚在网络中的旧响应。"""
        now = time.time()
        if now < self._next_at:
            return 0
        generation = self._generation
        self._last_error = ""
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
                if generation != self._generation:
                    return handled
                # 异常原文多半是英文的 HTTP 串，页面上直接挂出来就是一句英文提示
                name = str(device.get("name") or device.get("alias") or "").strip() or device_id[:8]
                self._last_error = f"{name}：读不到对话记录（{err}）"
                continue
            if generation != self._generation:
                return handled
            if records is None:
                # 取不到 ≠ 没有记录：基线不推进，下一轮还会看到这些话
                self._last_error = f"{device_id}: 读对话记录失败"
                continue
            handled += self._consume(device_id, records, str(device.get("hardware") or ""), generation)
        if generation != self._generation:
            return handled
        if self._last_error:
            self._backoff(self._last_error)
            return handled
        self._failures = 0
        # 从本轮**结束**起算，不是从开始：一轮问小米要几秒，从开始算的话慢轮一跑完
        # 「下次时间」就已经在过去，宿主下一次 tick 立刻又发一轮 —— 配 2 秒实际是贴着
        # 打，两轮还会重叠，刷出一堆 skipped 告警。
        self._next_at = time.time() + self._interval
        return handled

    def _backoff(self, error: str) -> None:
        self._last_error = error
        self._failures += 1
        delay = min(MAX_POLL_SECONDS,
                    self._interval * (2 ** min(self._failures, 5)))
        self._next_at = time.time() + delay

    def reset(self) -> None:
        """换设备/改配置后调用：时间基线与去重表都不再适用。"""
        with self._lock:
            self._generation += 1
            self._baseline.clear()
            self._primed.clear()
            self._seen.clear()
            self._next_at = 0.0
            self._failures = 0
            self._last_error = ""

    def forget_device(self, device_id: str) -> None:
        with self._lock:
            self._baseline.pop(device_id, None)
            self._primed.pop(device_id, None)

    def _consume(self, device_id: str, records: List[Dict[str, Any]],
                 hardware: str = "", generation: Optional[int] = None) -> int:
        if not self._primed.get(device_id):
            # 首轮整批只对齐基线，一条都不执行。小米回的是**按时间升序**的最近若干条：
            # 把第一条（最旧的那条）当基线、剩下几条当新命令，等于让音箱在宿主一重启就把
            # 几小时前说过的那句「播放…」重唱一遍 —— 界面看到的就是"重启后先播放、紧接着
            # 又被下一句口令停掉"。基线取这批里最新的一条，后面只会执行真正新说的话。
            newest = max((int(record.get("timestamp_ms") or 0) for record in records), default=0)
            if not newest:
                # 一批里全是拿不到时间戳的记录：没什么可对齐的，下一轮再看
                return 0
            self._baseline[device_id] = max(int(self._baseline.get(device_id) or 0), newest)
            self._primed[device_id] = True
            with self._lock:
                for record in records:
                    self._remember(device_id, record)
            return 0
        handled = 0
        for record in records:
            if generation is not None and generation != self._generation:
                break
            timestamp = int(record.get("timestamp_ms") or 0)
            text = str(record.get("question") or "").strip()
            if not timestamp:
                continue
            if timestamp <= int(self._baseline.get(device_id) or 0):
                continue
            self._baseline[device_id] = max(int(self._baseline.get(device_id) or 0), timestamp)
            if not text:
                continue
            with self._lock:
                if not self._remember(device_id, record):
                    continue
            handled += 1
            self._handled += 1
            try:
                reply = self._handler(text, device_id, hardware)
            except Exception as err:
                self._last_error = f"处理口令失败: {err}"
                reply = ""
            if reply:
                self._speak(device_id, reply)
        return handled

    def _remember(self, device_id: str, record: Dict[str, Any]) -> bool:
        """进「最近对话」与去重表；已在表里就返回 False。调用方持锁。"""
        timestamp = int(record.get("timestamp_ms") or 0)
        text = str(record.get("question") or "").strip()
        if not timestamp or not text:
            return False
        key = f"{device_id}:{record.get('request_id') or f'{timestamp}:{text}'}"
        if key in self._seen:
            return False
        self._seen[key] = time.time()
        while len(self._seen) > SEEN_LIMIT:
            self._seen.popitem(last=False)
        self._records.append({"device_id": device_id, "at": _fmt(timestamp / 1000.0),
                              "text": text, "answer": str(record.get("answer") or "")})
        self._records = self._records[-RECORD_BUFFER:]
        return True

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
