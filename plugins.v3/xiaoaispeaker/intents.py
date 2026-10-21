"""
intents.py — 把匹配到的意图落到设备上，并维护语音侧的三种状态。

`SpeechListener` 只负责"听到了什么"，这里负责"于是做什么"。分成四个小块，
都是为了语音控制才需要、单独测也比塞进插件主文件清楚：

- `Selection`：从曲库索引里挑出一条曲目 / 一串曲目（song / artist / playlist / 收藏 / 随机）；
- `SleepTimer`：睡眠定时（到点播完就停、或再播 N 首播完就停），状态落盘所以重启不丢；
- `Memory`：「上一次说这句话播了哪首」的映射表，同话直接重播，省掉一次搜索也省掉误判；
- `AIAnalyzer`：口令表没命中时，把一个 OpenAI 兼容的 chat 接口当兜底解析器。

播放模式（顺序/列表循环/随机/单曲循环/单曲播放）不在这里判，它属于队列本身的行为，
在插件的 `_advance` 里实现；这里只负责把模式写进队列状态。
"""

from __future__ import annotations

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


class AIAnalyzer:
    """把没命中口令表的话交给一个 OpenAI 兼容接口判定意图。

    只在插件里显式配了 api_key 时才启用；返回的动作必须落在 _AI_ACTIONS 里，
    置信度 low 或动作 unknown 一律当作没判出来。这是"多一个入口"而不是"更聪明的
    匹配器"：模型给的动作最终仍走同一套执行代码，权限不比口令表大。
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.api_url = str(config.get("api_url") or "").rstrip("/")
        self.api_key = str(config.get("api_key") or "")
        self.model = str(config.get("model") or "qwen-flash")
        try:
            self.timeout = max(2, min(int(config.get("timeout") or 6), 30))
        except (TypeError, ValueError):
            self.timeout = 6

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.api_url)

    def analyze(self, text: str) -> Optional[Match]:
        """没命中口令表时的一句话 → 意图。判不出来就回 None，让上层说"没听懂"。"""
        content = self._post([{"role": "system", "content": _SYSTEM_PROMPT},
                              {"role": "user", "content": text}],
                            response_format={"type": "json_object"}, temperature=0)
        return self._parse(_content_json(content), text)

    def chat(self, text: str, history: Optional[List[Dict[str, str]]] = None) -> str:
        """AI 对话：把这句话当聊天而不是命令，回一段要播报的话。

        与 `analyze` 同一条网络通路、同一个开关，只是提示词不同；上限压到 120 字，
        因为 TTS 播长段落时音箱会占用声道很久，用户已经走开了还在念。
        """
        content = self._post([{"role": "system", "content": _CHAT_PROMPT},
                              *(history or []),
                              {"role": "user", "content": text}],
                             temperature=0.7)
        if not content:
            return ""
        return " ".join(str(content).split())[:120]

    def _post(self, messages: List[Dict[str, str]], **extra: Any) -> str:
        """发一次 chat/completions，失败回空串（语音链路不接受异常）。"""
        if not self.enabled:
            return ""
        try:  # 宿主网络层由插件传入，这里只做请求体与结果判定
            from app.sdk.network import RequestUtils
        except Exception:  # pragma: no cover - 宿主缺件时直接不启用
            return ""
        payload = {"model": self.model, "messages": messages}
        payload.update({key: value for key, value in extra.items() if value is not None})
        try:
            response = RequestUtils(headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            }, timeout=self.timeout).post_dict(
                f"{self.api_url}/chat/completions", data=json.dumps(payload))
        except Exception:
            return ""
        if not isinstance(response, dict):
            return ""
        try:
            return str((((response.get("choices") or [{}])[0])
                        .get("message") or {}).get("content") or "")
        except (AttributeError, IndexError, TypeError):
            return ""

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
