"""
intents.py — 把匹配到的意图落到设备上，并维护语音侧的三种状态。

`SpeechListener` 只负责"听到了什么"，这里负责"于是做什么"。分成四个小块，
都是为了语音控制才需要、单独测也比塞进插件主文件清楚：

- `Selection`：从曲库索引里挑出一条曲目 / 一串曲目（song / artist / playlist / 收藏 / 随机）；
- `SleepTimer`：睡眠定时（到点播完就停、或再播 N 首播完就停），状态落盘所以重启不丢；
- `Memory`：「上一次说这句话播了哪首」的映射表，同话直接重播，省掉一次搜索也省掉误判；
- `Assistant` / `AIAnalyzer`：口令表没命中时，用 MoviePilot 自己配置的智能助手兜底解析。

播放模式（顺序/列表循环/随机/单曲循环/单曲播放）不在这里判，它属于队列本身的行为，
在插件的 `_advance` 里实现；这里只负责把模式写进队列状态。
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from .text import chinese_to_number
from .voice import Match

# 睡眠定时的上限：300 分钟。给到几小时没意义（音箱会自己休眠），
# 而一个写错的"一万分钟后停止"会一直占着这条状态
MAX_TIMER_MINUTES = 300
# 「上次这么说播了这首」的表大小与有效期：过期条目的路径多半已经不在库里了
MEMORY_LIMIT = 200
MEMORY_TTL_SECONDS = 30 * 86400.0

_MODE_ALIASES: Dict[str, str] = {
    "order": "order", "sequential": "order", "顺序": "order",
    "loop": "loop", "list": "loop", "列表循环": "loop",
    "random": "random", "shuffle": "random", "随机": "random",
    "single": "single", "singleloop": "single", "单曲循环": "single",
    "singleplay": "single_play", "single_play": "single_play", "单曲播放": "single_play",
}

# AI 兜底只接受这些动作，且必须给出可核对的参数；否则宁可回"没听懂"
_AI_ACTIONS = {
    "play_song", "play_artist", "play_playlist", "play_index", "next", "previous",
    "pause", "resume", "stop", "set_volume", "set_play_mode", "favorite",
}


class SleepTimer:
    """睡眠定时：`stop_at`（时间戳）或 `stop_after_songs`（剩余首数）二选一。"""

    def __init__(self, get_data: Any, save_data: Any) -> None:
        self._get = get_data
        self._save = save_data

    def state(self) -> Dict[str, Any]:
        raw = self._get()
        return raw if isinstance(raw, dict) else {}

    def set_minutes(self, minutes: int) -> Dict[str, Any]:
        value = max(1, min(int(minutes), MAX_TIMER_MINUTES))
        state = {"stop_at": time.time() + value * 60.0, "device_id": ""}
        self._save(state)
        return state

    def set_songs(self, count: int, device_id: str = "") -> Dict[str, Any]:
        value = max(1, min(int(count), 500))
        state = {"stop_after_songs": value, "device_id": device_id}
        self._save(state)
        return state

    def cancel(self) -> None:
        self._save({})

    def remaining_text(self) -> str:
        state = self.state()
        songs = state.get("stop_after_songs")
        if isinstance(songs, (int, float)) and songs > 0:
            return f"再播 {int(songs)} 首后停止"
        stop_at = float(state.get("stop_at") or 0)
        if stop_at > time.time():
            minutes = int(round((stop_at - time.time()) / 60.0))
            return f"还有约 {minutes} 分钟停止"
        return "没有设置定时停止"

    def due(self) -> Tuple[bool, str]:
        """到点/到首数了就 (True, 文案)。"""
        state = self.state()
        stop_at = float(state.get("stop_at") or 0)
        if stop_at and time.time() >= stop_at:
            self.cancel()
            return True, "定时时间到"
        return False, ""

    def on_track_finished(self) -> Tuple[bool, str]:
        """每播完一首调用：计数减到 0 就停。"""
        state = self.state()
        songs = state.get("stop_after_songs")
        if not isinstance(songs, (int, float)) or songs <= 0:
            return False, ""
        left = int(songs) - 1
        if left <= 0:
            self.cancel()
            return True, "定时播放的歌曲数已到"
        state["stop_after_songs"] = left
        self._save(state)
        return False, ""


class Memory:
    """「这句话 → 上次播了哪首」。上游 voice_memory_enabled 的同款作用。

    只记搜索类口令（play_song/play_artist/play_playlist）命中的第一条：用户重复说同一句
    通常就是要同一首，命中后跳过打分直接播，省得"稻香"偶尔被算法判成另一版本。
    """

    def __init__(self, get_data: Any, save_data: Any) -> None:
        self._get = get_data
        self._save = save_data

    def _rows(self) -> List[Dict[str, Any]]:
        raw = self._get()
        return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []

    def lookup(self, kind: str, text: str) -> str:
        key = f"{kind}|{text}"
        now = time.time()
        for item in self._rows():
            if str(item.get("key") or "") != key:
                continue
            if now - float(item.get("at") or 0) > MEMORY_TTL_SECONDS:
                continue
            return str(item.get("path") or "")
        return ""

    def remember(self, kind: str, text: str, path: str) -> None:
        if not path:
            return
        key = f"{kind}|{text}"
        rows = [item for item in self._rows() if str(item.get("key") or "") != key]
        rows.append({"key": key, "path": path, "at": time.time()})
        rows.sort(key=lambda item: float(item.get("at") or 0), reverse=True)
        self._save(rows[:MEMORY_LIMIT])


class Assistant:
    """MoviePilot 自己配置的智能助手（设定 → AI 大模型）—— 插件不再配一份 key。

    读法与站内其它插件一致：`app.sdk.config.settings` 取 `LLM_*`，交给
    `app.agent.llm.LLMHelper.get_llm(...)`（这是个协程，而调用方可能在同步线程里，
    所以自己兜一个事件循环跑完）。宿主缺件、没配 key/模型都只回一句话说明，
    不抛异常 —— AI 是口令表之外的一个入口，不该把语音链路拖下水。
    """

    def __init__(self, timeout: int = 6) -> None:
        self.timeout = max(2, min(int(timeout or 6), 30))
        self.settings, self.error = self._read()

    def call(self, value: Any) -> Any:
        """跑掉宿主返回的协程。测试里换成同步实现，就能替掉整条 LLM 通路。"""
        return self._resolve(value)

    @staticmethod
    def _read() -> Tuple[Optional[Dict[str, Any]], str]:
        try:
            from app.sdk.config import settings
        except Exception as exc:  # pragma: no cover - 宿主结构变化时只降级
            return None, f"读不到 MoviePilot 的智能助手配置：{exc}"
        config = {
            "provider": getattr(settings, "LLM_PROVIDER", None) or "openai",
            "model": str(getattr(settings, "LLM_MODEL", "") or "").strip(),
            "api_key": str(getattr(settings, "LLM_API_KEY", "") or "").strip(),
            "base_url": getattr(settings, "LLM_BASE_URL", None),
            "base_url_preset": getattr(settings, "LLM_BASE_URL_PRESET", None),
            "api_protocol": getattr(settings, "LLM_API_PROTOCOL", None),
            "user_agent": getattr(settings, "LLM_USER_AGENT", None),
            "use_proxy": getattr(settings, "LLM_USE_PROXY", True),
            "thinking_level": getattr(settings, "LLM_THINKING_LEVEL", None),
        }
        if not config["api_key"]:
            return None, "MoviePilot 未配置智能助手的 API Key（设定 → AI 大模型）"
        if not config["model"]:
            return None, "MoviePilot 未配置智能助手的模型"
        return config, ""

    @property
    def available(self) -> bool:
        return self.settings is not None

    def describe(self) -> Dict[str, Any]:
        """给状态面板的四项：用的是哪家模型，或者为什么不能用（键名与 ConfigOut 同）。"""
        if not self.available:
            return {"ai_ready": False, "ai_provider": "", "ai_model": "",
                    "ai_message": self.error}
        return {"ai_ready": True,
                "ai_provider": str(self.settings.get("provider") or ""),
                "ai_model": str(self.settings.get("model") or ""),
                "ai_message": "使用 MoviePilot 智能助手"}

    def ask(self, system: str, user: str,
            history: Optional[List[Dict[str, str]]] = None,
            temperature: Optional[float] = None) -> str:
        """一次系统提示 + 用户话的问答；失败回空串，语音链路不接受异常。"""
        if not self.available:
            return ""
        try:
            from app.agent.llm import LLMHelper
            from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
        except Exception:  # pragma: no cover - 宿主没装 LLM 依赖
            self.error = "宿主未安装智能助手依赖（app.agent.llm / langchain）"
            return ""
        messages: List[Any] = [SystemMessage(content=system)]
        for turn in history or []:
            role = str(turn.get("role") or "")
            content = str(turn.get("content") or "")
            if not content:
                continue
            if role == "assistant":
                messages.append(AIMessage(content=content))
            elif role == "user":
                messages.append(HumanMessage(content=content))
        messages.append(HumanMessage(content=user))
        kwargs = dict(self.settings)
        if temperature is not None:
            kwargs["temperature"] = temperature
        # 宿主 get_llm 不收 response_format：要 JSON 全靠提示词约束，_parse 也容得下围栏与散文。
        try:
            text = self.call(self._invoke(LLMHelper, messages, kwargs))
        except Exception as exc:
            self.error = f"智能助手调用失败：{exc}"
            text = ""
        return str(text or "").strip()

    async def _invoke(self, helper: Any, messages: List[Any],
                      kwargs: Dict[str, Any]) -> str:
        """建模型、限时取回复、把模型交还宿主：整段跑在 self.call 起的那个事件循环里。

        超时用宿主自己的写法（asyncio.wait_for 包 ainvoke）：get_llm 没有 timeout 参数，
        只有在这里等，配置页那项 ai_timeout 才真的管用。
        """
        llm = await helper.get_llm(streaming=False, **kwargs)
        try:
            response = await asyncio.wait_for(llm.ainvoke(messages), timeout=self.timeout)
        finally:
            closer = getattr(helper, "close_llm", None)
            if callable(closer):
                await closer(llm)
        extract = getattr(helper, "extract_text_content", None)
        content = getattr(response, "content", response)
        if callable(extract):
            return str(extract(content, fallback_to_string=True) or "")
        return str(content or "")

    @staticmethod
    def _resolve(value: Any) -> Any:
        """跑掉协程：没有事件循环就直接 asyncio.run，已经有了就另起一个循环。

        语音监听在自己的线程里，正常走前者；从 API 线程进来时可能已经有 loop，
        `asyncio.run` 在那个上下文里会直接报错，所以退到新建循环并关掉。
        """
        if not asyncio.iscoroutine(value):
            return value
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(value)
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(value)
        finally:
            loop.close()


class AIAnalyzer:
    """把没命中口令表的话交给 MoviePilot 的智能助手判定意图。

    只在配置里显式开了 `ai_enabled` 且宿主配好了助手时才启用；返回的动作必须落在
    _AI_ACTIONS 里，置信度 low 或动作 unknown 一律当作没判出来。这是"多一个入口"
    而不是"更聪明的匹配器"：模型给的动作最终仍走同一套执行代码，权限不比口令表大。
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.assistant = config.get("assistant") \
            if isinstance(config.get("assistant"), Assistant) else Assistant(
                int(config.get("timeout") or 6))

    @property
    def enabled(self) -> bool:
        return bool(self.assistant.available)

    def analyze(self, text: str) -> Optional[Match]:
        """没命中口令表时的一句话 → 意图。判不出来就回 None，让上层说"没听懂"。"""
        content = self.assistant.ask(_SYSTEM_PROMPT, text, temperature=0)
        return self._parse(_content_json(content), text)

    def chat(self, text: str, history: Optional[List[Dict[str, str]]] = None) -> str:
        """AI 对话：把这句话当聊天而不是命令，回一段要播报的话。

        与 `analyze` 同一个助手、同一个开关，只是提示词不同；上限压到 120 字，
        因为 TTS 播长段落时音箱会占用声道很久，用户已经走开了还在念。
        """
        content = self.assistant.ask(_CHAT_PROMPT, text, history=history, temperature=0.7)
        if not content:
            return ""
        return " ".join(str(content).split())[:120]

    @staticmethod
    def _parse(content: Any, raw: str) -> Optional[Match]:
        data = content if isinstance(content, dict) else _content_json(str(content or ""))
        if not isinstance(data, dict):
            return None
        action = str(data.get("action") or "").strip().lower()
        if action not in _AI_ACTIONS or str(data.get("confidence") or "").lower() == "low":
            return None
        params = data.get("params") or {}
        if not isinstance(params, dict):
            params = {}
        kind = action
        param = ""
        argument = str(params.get("name") or params.get("query") or "").strip()
        if action == "set_volume":
            value = params.get("volume")
            if isinstance(value, str):
                value = chinese_to_number(value.strip())
            if value is None:
                return None
            if str(params.get("mode") or "").lower() in ("up", "down"):
                param = str(params.get("mode") or "").lower()
                argument = ""
            else:
                param = "absolute"
                argument = str(int(value))
        elif action in ("pause", "stop"):
            kind = "stop"
        elif action == "resume":
            kind = "resume"
        elif action == "play_index":
            kind = "play_index"
            number = params.get("index")
            if isinstance(number, str):
                number = chinese_to_number(number.strip())
            if not isinstance(number, (int, float)) or int(number) < 1:
                return None
            argument = str(int(number))
        elif action == "set_play_mode":
            kind = "set_play_mode"
            param = _MODE_ALIASES.get(str(params.get("mode") or "").strip().lower(), "")
            if not param:
                return None
        elif action == "favorite":
            param = "remove" if str(params.get("mode") or "") in ("remove", "取消") else "add"
        return Match(kind, param, "ai", argument, raw)


_SYSTEM_PROMPT = (
    "你是音箱控制意图解析器。把用户的一句话判定为 JSON："
    '{"action":"play_song|play_artist|play_playlist|play_index|next|previous|pause|resume|stop|'
    'set_volume|set_play_mode|favorite|unknown",'
    '"params":{"name":"歌名/歌手/歌单","volume":数字,"mode":"up|down|absolute|random|order|single|loop|remove"},'
    '"confidence":"low|medium|high"}。'
    "只输出 JSON，不要解释。听不清或与播放无关时 action=unknown、confidence=low。"
)

_CHAT_PROMPT = (
    "你是一台小爱音箱上的陪聊。回答要短、口语化、适合直接念出来："
    "不超过三句话，不用 markdown、不用列表、不贴代码；没有依据就说不确定。"
    "用户在跟音箱说话，不是在跟搜索引擎说话。"
)


def _content_json(content: str) -> Optional[Dict[str, Any]]:
    """模型偶尔把 JSON 包在 ```json 围栏里，或前后带一句解释：只取第一个对象。"""
    text = (content or "").strip()
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        data = json.loads(text[start:end + 1])
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


class Selection:
    """口令 → 要播的曲目集合。"""

    def __init__(self, index: Any, favorites: List[str]) -> None:
        self._index = index
        self._favorites = [path for path in (favorites or []) if path]

    @property
    def ready(self) -> bool:
        return bool(getattr(self._index, "ready", False))

    # ---- 单曲 ----

    def song(self, query: str, remembered: str = "") -> Tuple[str, List[str], str]:
        """返回 (要播的路径, 队列路径列表, 展示名)。找不到时路径为空。"""
        if remembered:
            hit = self._index.get(remembered)
            if hit is not None:
                return remembered, self._neighbours(hit), _label(hit)
        hits = self._index.search_song(query)
        if not hits:
            return "", [], ""
        _, song = hits[0]
        path = str(song.get("path") or "")
        return path, self._neighbours(song), _label(song)

    def _neighbours(self, song: Any) -> List[str]:
        """同目录（同一张专辑）连播：语音点歌最常见的期待是"把这张碟放完"。"""
        album = str(song.album if hasattr(song, "album") else song.get("album") or "")
        directory = _dirname(str(song.path if hasattr(song, "path") else song.get("path") or ""))
        paths: List[str] = []
        for item in self._index.songs(100000):
            if _dirname(str(item.get("path") or "")) != directory:
                continue
            if album and str(item.get("album") or "") and str(item.get("album") or "") != album:
                continue
            paths.append(str(item.get("path") or ""))
        return paths or [str(song.path if hasattr(song, "path") else song.get("path") or "")]

    # ---- 批量 ----

    def artist(self, name: str) -> Tuple[List[str], str]:
        rows = self._index.find_songs_by_artist(name)
        paths: List[str] = []
        for _, song in rows:
            path = str(song.get("path") or "")
            if path and path not in paths:
                paths.append(path)
        if not paths:
            return [], ""
        artist = str(rows[0][1].get("artist") or name)
        return paths, f"{artist} 的 {len(paths)} 首歌"

    def playlist(self, name: str) -> Tuple[List[str], str]:
        target = (name or "").strip()
        if target in ("收藏", "我的收藏", "favorite", "favorites"):
            return list(self._favorites), f"我的收藏（{len(self._favorites)} 首）"
        found = self._index.find_playlist_by_name(target)
        if found is None:
            hits = self._index.search_playlist(target)
            if hits:
                found = self._index.get_playlist(int(hits[0][1].get("id") or 0))
        if found is None:
            return [], ""
        songs = self._index.playlist_songs(found)
        return [item.path for item in songs], f"{found.name}（{len(songs)} 首）"

    def random_songs(self, count: int = 20) -> Tuple[List[str], str]:
        rows = self._index.songs(100000)
        if not rows:
            return [], ""
        pick = random.sample(rows, min(max(1, int(count)), len(rows)))
        return [str(item.get("path") or "") for item in pick], f"随机 {len(pick)} 首"

    def playlist_of_path(self, path: str) -> List[str]:
        return self._neighbours({"path": path, "album": "", "title": ""})


def normalize_mode(raw: Any) -> str:
    return _MODE_ALIASES.get(str(raw or "").strip().lower(), "")


def parse_index_number(text: str) -> Optional[int]:
    number = chinese_to_number((text or "").strip())
    return number if number and number > 0 else None


def parse_positive_number(text: str) -> Optional[int]:
    """任意正整数（分钟数、首数）：不做音量那种 0-100 夹逼。"""
    body = (text or "").strip()
    number = chinese_to_number(body)
    if number is None:
        match = re.search(r"\d{1,4}", body)
        number = int(match.group()) if match else None
    return number if number and number > 0 else None


def parse_volume_number(text: str) -> Optional[int]:
    number = chinese_to_number((text or "").strip())
    if number is None:
        match = re.search(r"\d{1,3}", text or "")
        number = int(match.group()) if match else None
    if number is None:
        return None
    return max(0, min(100, int(number)))


def _dirname(path: str) -> str:
    return str(path).rsplit("/", 1)[0] if "/" in str(path) else ""


def _label(song: Any) -> str:
    artist = str(song.artist if hasattr(song, "artist") else song.get("artist") or "").strip()
    title = str(song.title if hasattr(song, "title") else song.get("title") or "").strip()
    return f"{artist} - {title}" if artist else title
