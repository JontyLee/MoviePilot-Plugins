"""
voice.py — 口令表与匹配引擎：把"小爱同学说的话"变成一个可执行的意图。

判定顺序与上游 songloft-plugin-miot（Apache-2.0 © 2026 hanxi，
https://github.com/songloft-org/songloft-plugin-miot）的 src/music/service.ts
`matchCommand` 一致，逐条对齐：

1. 只按「子串出现」匹配关键词，跨全部口令取**最长关键词**，长度相同再比业务优先级
   —— 防止 "播放" 抢走 "播放歌单" 的命中；
2. 最先出现的搜索口令（play_song/playlist/artist/index）之后的文字是**名称参数**，
   不再允许被当成控制口令 —— 否则「播放歌曲 停止播放」会去暂停；
3. 第一轮零命中时跑**跳字子序列**兜底（关键词中间最多插 4 个字，且关键词 ≥3 字，
   避免"停止/切歌"这类短词误触发）；play_index 不参与兜底；
4. 全部落空才用内置的"停止"关键词兜底。

与上游的三处有意不同：
- 序号口令额外校验 `第N首` 的 N 可解析且 ≥1（上游在 execute 阶段才判），因为
  插件侧没有独立的"跳播"实现，落到 play_index 就必须能定位队列条目；
- 音量支持「大一点/小一点」的**增量**语义与绝对值两种，绝对值直接读数字并夹到 0-100；
- 睡眠定时解析放在这里（复用 text.py 的中文数字），上游在另一模块。
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .text import (
    chinese_to_number,
    parse_song_count,
    parse_song_index,
    parse_time_minutes,
    strip_song_suffix,
)

# 口令类型优先级（数字小者优先，与上游 COMMAND_PRIORITY 同量级；新增的类型排在后面）
COMMAND_PRIORITY: Dict[str, int] = {
    "play_artist": 1,
    "play_song": 1,
    "play_recommend": 1,
    "play_playlist": 2,
    "play_index": 2,
    "set_play_mode": 3,
    "set_volume": 4,
    "favorite": 5,
    "next": 6,
    "previous": 7,
    "sleep_timer": 7,
    "cancel_sleep_timer": 7,
    "query_sleep_timer": 7,
    "resume": 8,
    "stop": 8,
    "query_playing": 9,
    "tts": 9,
}

# 搜索类口令：命中后其后的文字是名称，不再参与二次匹配
SEARCH_TYPES = ("play_song", "play_artist", "play_playlist", "play_index")

# 跳字兜底：允许插入的字符数与参与兜底的最短关键词（与上游 FUZZY_* 一致）
FUZZY_MAX_GAP = 4
FUZZY_MIN_KEYWORD_LEN = 3

# 内置"停止"兜底词（上游 BUILTIN_STOP_KEYWORDS 的主体）
BUILTIN_STOP_KEYWORDS = (
    "暂停播放", "停止播放", "暂停音乐", "关掉音乐", "关闭音乐", "停止音乐",
    "别播了", "别放了", "停一下", "关机", "暂停", "停止", "pause", "stop",
)

# 口令表：(类型, 子参数, 关键词…)。内置关键词由这里维护，用户可以另加自己的口令
# （见 build_commands）：匹配按"最长关键词优先"，自定义词和内置词撞车时长的赢，
# 不需要额外的冲突处理。
DEFAULT_COMMANDS: Tuple[Tuple[str, str, Tuple[str, ...]], ...] = (
    ("play_playlist", "", ("播放歌单", "放歌单", "播放列表", "来一个歌单",
                           "来一首歌单", "放一首歌单", "播放这个歌单")),
    ("play_artist", "", ("播放歌手", "放歌手", "播放这个歌手", "播放该歌手")),
    ("play_song", "", ("播放歌曲", "放歌曲", "我想听", "我要听", "来一首歌曲", "播放音乐",
                       "帮我播放", "帮我放", "帮我听", "来一首", "放一首", "播一首", "放个")),
    ("play_recommend", "", ("随便播", "随便放", "随便来一首", "随机来一首", "推荐一首",
                            "随便听", "随机播一首")),
    ("play_index", "", ("播放第", "跳到第", "切到第", "跳转到第", "放第")),
    ("set_play_mode", "random", ("随机播放", "随机模式", "打乱播放")),
    ("set_play_mode", "single", ("单曲循环", "循环播放这首", "单曲")),
    ("set_play_mode", "single_play", ("单曲播放", "只播放这首", "播完这首停止")),
    ("set_play_mode", "loop", ("列表循环", "循环播放", "循环")),
    ("set_play_mode", "order", ("顺序播放", "按顺序播")),
    ("set_volume", "absolute", ("设置音量", "音量调到", "音量设为", "声音调到", "音量", "声音")),
    ("set_volume", "up", ("大声一点", "声音大一点", "音量大一点", "大点声", "大声",
                          "音量调高", "音量调大", "声音调大", "声音调高", "调高音量",
                          "调大音量", "把音量调高", "把音量调大", "调高一点", "调大一点")),
    ("set_volume", "down", ("小声一点", "声音小一点", "音量小一点", "小点声", "小声",
                            "音量调低", "音量调小", "声音调小", "调低音量", "调小音量",
                            "把音量调低", "把音量调小", "调低一点", "调小一点")),
    ("next", "", ("下一首", "切歌", "换一首", "下一曲", "切到下一首")),
    ("previous", "", ("上一首", "上一曲", "前一首")),
    ("resume", "", ("继续播放", "恢复播放", "接着播", "接着放", "继续")),
    ("stop", "", ("暂停播放", "停止播放", "暂停音乐", "停一下", "别播了", "关掉音乐", "关机", "关闭", "暂停")),
    ("favorite", "add", ("收藏歌曲", "收藏这首歌", "喜欢这首歌", "收藏这首", "收藏")),
    ("favorite", "remove", ("取消收藏", "取消收藏歌曲", "不喜欢的")),
    ("sleep_timer", "", ("分钟后停止播放", "分钟后暂停播放", "分钟后停止", "分钟后关闭",
                         "小时后停止播放", "小时后停止", "小时后关闭", "半小时后停止",
                         "首歌后停止播放", "首歌后停止", "首后停止播放", "首后停止",
                         "首歌后暂停", "首后关闭", "分钟之后停", "小时之后停")),
    ("cancel_sleep_timer", "", ("取消定时", "取消倒计时", "取消睡眠定时", "取消定时停止",
                                "不用定时了", "取消停止")),
    ("query_sleep_timer", "", ("还有多久停", "定时还剩", "倒计时还剩", "还剩多久")),
    ("query_playing", "", ("现在放的什么", "这是什么歌", "放的什么歌", "当前歌曲", "什么歌")),
    ("tts", "", ("对小爱说", "播报", "通知")),
)

_TRAILING_PUNCT_RE = re.compile(r"[。.!！?？，,、~～\s]+$")

# 口令类型的中文名：侧栏页的开关列表用，不参与匹配
COMMAND_LABELS: Dict[str, str] = {
    "play_playlist": "播放歌单",
    "play_artist": "播放歌手",
    "play_song": "播放歌曲",
    "play_recommend": "随便播点",
    "play_index": "播放第 N 首",
    "set_play_mode": "播放方式",
    "set_volume": "音量",
    "next": "下一首",
    "previous": "上一首",
    "resume": "继续播放",
    "stop": "暂停/停止",
    "favorite": "收藏",
    "sleep_timer": "定时停止",
    "cancel_sleep_timer": "取消定时",
    "query_sleep_timer": "查询定时",
    "query_playing": "查询在播",
    "tts": "播报",
}

# 子参数的中文名：口令编辑器按类型列参数用，不参与匹配
PARAM_LABELS: Dict[str, str] = {
    "": "整句即命令",
    "absolute": "后面跟数值",
    "up": "调大",
    "down": "调小",
    "random": "随机",
    "order": "顺序",
    "loop": "列表循环",
    "single": "单曲循环",
    "single_play": "单曲播放",
    "add": "做",
    "remove": "反向",
}

# 用户自定义口令的上限与单条长度：口令表是每次判定都过一遍的，堆多了白白变慢
MAX_CUSTOM_KEYWORDS = 60
MAX_KEYWORD_LEN = 20


class Match:
    """一次口令匹配的结果。"""

    __slots__ = ("kind", "param", "keyword", "argument", "raw")

    def __init__(self, kind: str, param: str, keyword: str, argument: str, raw: str) -> None:
        self.kind = kind
        self.param = param
        self.keyword = keyword
        self.argument = argument
        self.raw = raw

    def as_dict(self) -> Dict[str, Any]:
        return {"kind": self.kind, "param": self.param, "keyword": self.keyword,
                "argument": self.argument, "raw": self.raw}

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"<Match {self.kind}{':' + self.param if self.param else ''} '{self.argument}'>"


def default_commands() -> List[Dict[str, Any]]:
    """口令表的可序列化形式（侧栏页按类型显示开关与说明）。"""
    return [
        {"kind": kind, "param": param, "keywords": list(keywords)}
        for kind, param, keywords in DEFAULT_COMMANDS
    ]


def enabled_commands(enabled: Optional[Iterable[str]] = None,
                     ) -> List[Tuple[str, str, Tuple[str, ...]]]:
    """按「启用的口令类型」过滤口令表。`enabled` 为 None 表示全开。"""
    if enabled is None:
        return list(DEFAULT_COMMANDS)
    allowed = set(enabled)
    return [row for row in DEFAULT_COMMANDS if row[0] in allowed]


def kind_params() -> Dict[str, List[str]]:
    """每个口令类型在内置表里用过的子参数，顺序照内置表。"""
    out: Dict[str, List[str]] = {}
    for kind, param, _keywords in DEFAULT_COMMANDS:
        params = out.setdefault(kind, [])
        if param not in params:
            params.append(param)
    return out


def kind_options() -> List[Dict[str, Any]]:
    """口令类型清单（中文名 + 可选子参数），口令编辑器的下拉框用。"""
    return [
        {"kind": kind,
         "label": COMMAND_LABELS.get(kind, kind),
         "params": [{"value": param,
                     "label": PARAM_LABELS.get(param, param)}
                    for param in params]}
        for kind, params in kind_params().items()
    ]


def clean_keyword(text: Any) -> str:
    """收敛一条用户填的口令：压空白、剥尾标点、限长度；不合规回空串（调用方丢弃）。"""
    word = _TRAILING_PUNCT_RE.sub("", " ".join(str(text or "").split()))
    if not word or len(word) > MAX_KEYWORD_LEN:
        return ""
    if not re.search(r"[\u4e00-\u9fffA-Za-z0-9]", word):
        # 整串都是标点的口令永远匹配不上，还能把内置词全遮掉，直接不收
        return ""
    return word


def build_commands(enabled: Optional[Iterable[str]] = None,
                   customs: Optional[Sequence[Dict[str, Any]]] = None,
                   hidden: Optional[Iterable[str]] = None,
                   ) -> List[Tuple[str, str, Tuple[str, ...]]]:
    """真正生效的口令表 = 内置表（按类型开关过滤、扣掉被藏起来的词）+ 用户自己加的口令。

    用户加的词只挂到内置已有的类型/子参数上：匹配之后走的还是同一套执行代码，
    自定义口令拿不到比内置更大的权限，也不会造出新的动作。
    """
    blocked = {clean_keyword(item) for item in (hidden or [])}
    rows: List[Tuple[str, str, Tuple[str, ...]]] = []
    for kind, param, keywords in enabled_commands(enabled):
        left = tuple(word for word in keywords if word not in blocked)
        if left:
            rows.append((kind, param, left))
    allowed_params = kind_params()
    taken = {word for _k, _p, words in rows for word in words}
    for item in customs or []:
        if not isinstance(item, dict):
            continue
        word = clean_keyword(item.get("keyword"))
        kind = str(item.get("kind") or "")
        if not word or kind not in allowed_params or word in taken:
            continue
        params = allowed_params[kind]
        param = str(item.get("param") or "")
        if param not in params:
            param = params[0]
        taken.add(word)
        rows.append((kind, param, (word,)))
    return rows


def normalize_utterance(text: str) -> str:
    """语音记录里的原始串：去空白、剥尾部标点，大小写留给匹配层（关键词含拉丁词）。"""
    return _TRAILING_PUNCT_RE.sub("", (text or "").strip())


def _rune_len(text: str) -> int:
    return len(text or "")


def _fuzzy_locate(query: str, keyword: str) -> Optional[Tuple[int, int]]:
    """跳字匹配：返回 (关键词最后一个字在句中的位置, 中间插入的字数)，不匹配给 None。

    与上游一致的边界：关键词至少 3 字（"停止/切歌"这种短词不参与，否则"别停在那儿"
    就会被切成 stop），插入不超过 FUZZY_MAX_GAP 个字。
    """
    if not keyword or _rune_len(keyword) < FUZZY_MIN_KEYWORD_LEN:
        return None
    position = 0
    inserted = 0
    gap = 0
    last = -1
    for index, char in enumerate(query):
        if position < len(keyword) and char == keyword[position]:
            position += 1
            gap = 0
            last = index
            if position == len(keyword):
                return last, inserted
            continue
        if position:
            gap += 1
            inserted += 1
            if gap > FUZZY_MAX_GAP:
                return None
    return None


def match_command(text: str,
                  commands: Optional[Sequence[Tuple[str, str, Tuple[str, ...]]]] = None,
                  ) -> Optional[Match]:
    """把一句话匹配成一个意图；匹配不到返回 None（调用方决定是回"没听懂"还是交给 AI）。"""
    query = normalize_utterance(text)
    if not query:
        return None
    rows = list(commands) if commands is not None else list(DEFAULT_COMMANDS)
    if not rows:
        return None

    # 最先出现的搜索口令后面是名称参数，不能再把其中的文字当成新的控制口令
    # （「播放歌曲 停止播放」应当点歌，不该暂停）。同一起点取最长关键词。
    argument_start = len(query) + 1
    anchor: Optional[Tuple[int, int]] = None
    for kind, _param, keywords in rows:
        if kind not in SEARCH_TYPES:
            continue
        for keyword in keywords:
            if not keyword:
                continue
            index = query.find(keyword)
            if index < 0:
                continue
            length = _rune_len(keyword)
            if anchor is None or index < anchor[0] or (index == anchor[0] and length > anchor[1]):
                anchor = (index, length)
                argument_start = index + len(keyword)

    best: Optional[Match] = None
    best_len = 0
    best_priority = 99
    for kind, param, keywords in rows:
        priority = COMMAND_PRIORITY.get(kind, 99)
        for keyword in keywords:
            index = query.find(keyword)
            if index < 0 or index >= argument_start:
                continue
            if kind == "play_index" and parse_song_index(query[index:]) is None:
                # "播放第一周" 里的"第一"不是序号，不能让跳播抢下这句
                continue
            length = _rune_len(keyword)
            if length > best_len or (length == best_len and priority < best_priority):
                best_len = length
                best_priority = priority
                best = Match(kind, param, keyword, query[index + len(keyword):].strip(), query)
    if best:
        return _finalize(best)

    # 第二轮：精确子序列零命中时跑跳字兜底（"我今天想听稻香" 命中 "我想听"）。
    # tiebreak 与第一轮同：最长关键词 > 插入最少 > 高优先级。
    best_inserted = None
    for kind, param, keywords in rows:
        if kind == "play_index":
            continue
        priority = COMMAND_PRIORITY.get(kind, 99)
        for keyword in keywords:
            found = _fuzzy_locate(query, keyword)
            if found is None:
                continue
            last, inserted = found
            length = _rune_len(keyword)
            better = (length > best_len
                      or (length == best_len and (best_inserted is None or inserted < best_inserted))
                      or (length == best_len and inserted == best_inserted
                          and priority < best_priority))
            if better:
                best_len = length
                best_inserted = inserted
                best_priority = priority
                best = Match(kind, param, keyword, query[last + 1:].strip(), query)
    if best:
        return _finalize(best)

    lowered = query.lower()
    hits = sorted((item for item in BUILTIN_STOP_KEYWORDS if item in lowered),
                  key=lambda item: -_rune_len(item))
    if hits:
        return Match("stop", "", hits[0], "", query)
    return None


def _finalize(match: Match) -> Match:
    """搜索类口令的参数做一次尾缀清理；序号口令把关键词与参数拼回去再解析。"""
    if match.kind == "play_index":
        number = parse_song_index(f"{match.keyword}{match.argument}")
        match.argument = str(number) if number else ""
        return match
    if match.kind in SEARCH_TYPES:
        match.argument = clean_name(match.argument)
    if match.kind == "set_volume" and match.param == "absolute":
        match.argument = _volume_text(match.raw, match.keyword)
    if match.kind == "sleep_timer":
        minutes, songs = parse_timer(match.raw)
        match.param = "songs" if songs else ("minutes" if minutes else "")
        match.argument = str(songs or minutes or "")
    return match


_LEAD_FILLER_RE = re.compile(r"^(?:一首|一个|这首|那首|这个|那个)+")


def clean_name(argument: str) -> str:
    """名称参数：剥掉开头的量词（"放一首 平凡之路"）与"的歌曲/的音乐"这类尾缀。"""
    text = (argument or "").strip()
    text = _LEAD_FILLER_RE.sub("", text)
    text = strip_song_suffix(text).strip()
    return _TRAILING_PUNCT_RE.sub("", text)


def _volume_text(raw: str, keyword: str) -> str:
    """绝对音量：取关键词后面那段数字（支持中文数字与"百分之N"）。"""
    index = raw.find(keyword)
    tail = raw[index + len(keyword):] if index >= 0 else raw
    match = re.search(rf"([0-9]+|[{_digits_class()}]+)", tail)
    if not match:
        return ""
    return str(chinese_to_number(match.group(1)) or "")


def _digits_class() -> str:
    return "一二两三四五六七八九十百零"


# 关键词与数字之间允许出现的东西：任何非数字字符。写成"排除数字类"而不是 `[^0-9]`，
# 否则贪婪跳过会把中文数字也跳过去，"音量调到三十"就只剩"十"= 10。
_SKIP_CLASS = f"^{_digits_class()}0-9"
_NUMBER_GROUP = rf"([0-9]+|[{_digits_class()}]+)"


def parse_volume(text: str) -> Optional[int]:
    """从一个串里取绝对音量（供配置页与 AI 兜底用）。"""
    body = text or ""
    match = re.search(rf"(?:音量|声音)[{_SKIP_CLASS}]*{_NUMBER_GROUP}", body)
    if not match:
        return _clamp_volume(chinese_to_number(body.strip()))
    return _clamp_volume(chinese_to_number(match.group(1)))


def _clamp_volume(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    return max(0, min(100, int(value)))


def parse_timer(text: str) -> Tuple[Optional[int], Optional[int]]:
    """睡眠定时：返回 (分钟, 首数)，两者只会命中一个。"""
    body = text or ""
    count = re.search(rf"([{_digits_class()}0-9]+)\s*首", body)
    if count:
        songs = chinese_to_number(count.group(1))
        if songs:
            return None, songs
    return parse_time_minutes(body), None


def parse_count(text: str) -> Optional[int]:
    """"再放 N 首"里的 N（口令表外的辅助解析，留给调用方做连播上限）。"""
    return parse_song_count(text)
