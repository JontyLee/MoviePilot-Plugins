"""
中文口令匹配层：繁简折叠、拼音、分词与打分。

语音控制的难点全落在「用户对小爱说了什么」和「库里到底有没有这首」之间那点字面
差异上：识别结果不带标点、可能有同音错字、歌名里可能有繁体或数字。这一模块只做
纯文本判定，不碰设备也不碰库，所以可以脱离网络独立测。

阈值与打分阶梯逐条移植自 songloft-plugin-miot（Apache-2.0 © 2026 hanxi，
https://github.com/songloft-org/songloft-plugin-miot）的 src/indexing/manager.ts
与 src/indexing/segmenter.ts；改动只有两处：JS 的 surrogate-pair 计数换成 Python
码点计数（语义相同，都是"字数"），以及把 index/art 两条路径共用的归一化收敛到本
模块。数据表来源见 data/NOTICE.md。
"""

from __future__ import annotations

import gzip
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

# 低于 MIN_MATCH_SCORE 的命中一律当作"库里没有"：宁可回一句没找到，也不要
# 因为一个字重合就放错歌。这两个常量与上游一致，不要单独调。
MIN_MATCH_SCORE = 40
MAX_SEARCH_RESULTS = 10
TOKEN_FUZZY_MIN_LEN = 2
# 拼音查表结果缓存上限（上游 PINYIN_CACHE_LIMIT）：整库重建时同一批字段会被反复转，
# 缓存换来重建耗时从秒级掉到百毫秒级，上限防止无界增长
PINYIN_CACHE_LIMIT = 20000
FIELD_WEIGHT: Dict[str, float] = {"title": 1.0, "artist": 0.85, "album": 0.7}

# 归一化时剥掉的装饰性标点（上游 normalizeForMatch 的字符类，逐字对齐）
_STRIP_TABLE = {ord(ch): None for ch in
                " \t\n\r\v\f　《》【】「」『』〔〕〈〉（）()[]{}，,。.、·!！?？~～—-_:：;；'\"“”‘’…"}
# 列表类搜索的分词：只按空格与顿号逗号切，"的"留在词里（歌单名常带"的"）
_LIST_SPLIT_RE = re.compile(r"[\s，,、]+")
# 口令分词：比列表更宽，冒号引号之类也要切开
_SPLIT_RE = re.compile(r"[\s，,、。.!！?？;；:：\"'“”‘’（）()【】\[\]《》<>/\\|]+")
# 口语里的请求/语气词。整词命中才丢，不做子串删除 —— 一首《我们》仍要是歌名；
# 全部被丢掉时退回原始切分，否则"播放歌曲"这种说法会切成空。
_STOPWORDS = frozenset("""
我 你 他 她 想 要 听 想听 放 播 播放 来 点 首 那 这 那首 这首 一首 一下 一 了 吧 啊 嘛 呢
请 帮 帮我 给 给我 唱 歌 歌曲 音乐 换 再 给我来 来一首 来首 的 和 与 跟 或 着 把
""".split())

_CHINESE_DIGITS = {"〇": "0", "零": "0", "一": "1", "幺": "1", "二": "2", "两": "2",
                   "三": "3", "四": "4", "五": "5", "六": "6", "七": "7", "八": "8",
                   "九": "9"}
_DIGIT_CHARS = "0-9一二两三四五六七八九十百千零〇"

_DATA_DIR = Path(__file__).parent / "data"


@lru_cache(maxsize=2)
def _load_table(name: str) -> Dict[str, str]:
    """读随插件分发的静态字表；读不到就退化成"不做这项转换"。

    字表缺失意味着安装被破坏，但口令里全简体的常见情况仍然能匹配，
    比让整个插件起不来划算。
    """
    try:
        with gzip.open(_DATA_DIR / name, "rt", encoding="utf-8") as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def is_cjk(char: str) -> bool:
    code = ord(char)
    return (0x3400 <= code <= 0x4dbf) or (0x4e00 <= code <= 0x9fff) or (0xf900 <= code <= 0xfaff)


def to_simplified(text: str) -> str:
    """逐字繁→简。只做字符级折叠，不做词组转换，够用且不会有歧义。"""
    table = _load_table("traditional-simplified.json.gz")
    if not table:
        return text
    return "".join(table.get(ch, ch) for ch in text or "")


def normalize(text: str) -> str:
    """匹配用归一化键：繁→简 + 小写 + 剥装饰标点。索引侧与查询侧必须走同一个函数。"""
    if not text:
        return ""
    return to_simplified(text).lower().translate(_STRIP_TABLE)


@lru_cache(maxsize=PINYIN_CACHE_LIMIT)
def to_pinyin(text: str) -> str:
    """汉字逐字转无声调拼音，非中文段原样小写保留，空格分隔便于子串匹配。"""
    if not text:
        return ""
    table = _load_table("pinyin.json.gz")
    if not table:
        return text.lower()
    parts: List[str] = []
    for ch in text:
        syllable = table.get(ch)
        if syllable:
            parts.append(syllable)
        elif ch.isspace():
            continue
        else:
            parts.append(ch.lower())
    return " ".join(parts)


def normalize_chinese_digits(text: str) -> str:
    """把「九四六」换成「946」，用于歌单名的逐字数字等价比较。"""
    return "".join(_CHINESE_DIGITS.get(ch, ch) for ch in (text or ""))


def segment(text: str) -> List[str]:
    """口令切词：切开后丢掉整词命中的语气词，丢光就退回原始切分。"""
    raw = [part for part in _SPLIT_RE.split(to_simplified(text or "")) if part]
    kept = [part for part in raw if part not in _STOPWORDS]
    return kept or raw


def tokenize(query: str) -> Tuple[List[str], List[str]]:
    """返回 (归一化词, 对应拼音)。拼音只在词够长时才算，与上游一致。"""
    tokens = [normalize(part) for part in segment(query)]
    tokens = [part for part in tokens if part]
    pinyins = [to_pinyin(token) if len(token) >= TOKEN_FUZZY_MIN_LEN else "" for token in tokens]
    return tokens, pinyins


# 助词"的"。上游有意不做分词（避免误伤《她的吉他》这类真歌名），代价是语音里
# "播放周杰伦的稻香"整句变成一个词、被汉字闸门拦下。这里不改正则切分，只在
# 原样匹配之外**追加**一条按"的"切开的可能，取两者最高分：召回率涨，误放不变。
_PARTICLE_RE = re.compile("的")
# 口语请求前缀。停用词表只在"整个词就是它"时生效，而语音里 "我想听稻香" 是一整块，
# 汉字闸门会因为它要 "我/想/听" 也出现在歌名里而判不匹配 —— 这里额外给一种"把开头的
# 请求动词剥掉"的切分。只剥开头、且剥完必须还剩东西，所以《我想听你说》这类真歌名
# 仍由原样切分那一路拿到高分，两种取最大不会互相压制。
_FILLER_PREFIX_RE = re.compile(
    r"^(?:你帮我|帮我|给我|请|麻烦|我想听|我想|我想要|我要|想听|来一首|来首|来|点一首|点|听|播放|放|唱|一首|一下)+")


def _strip_filler(token: str) -> str:
    stripped = _FILLER_PREFIX_RE.sub("", token)
    return stripped if stripped and stripped != token else ""


def tokenize_variants(query: str) -> List[Tuple[List[str], List[str]]]:
    """口令的全部切分方式：原样在前，剥请求前缀、按"的"切开在后。"""
    primary = tokenize(query)
    variants = [primary]
    tokens = list(primary[0])
    filled = [_strip_filler(token) or token for token in tokens]
    if filled != tokens and any(filled):
        kept = [token for token in filled if token]
        pinyins = [to_pinyin(token) if len(token) >= TOKEN_FUZZY_MIN_LEN else ""
                   for token in kept]
        variants.append((kept, pinyins))
        tokens = kept
    if any(_PARTICLE_RE.search(token) for token in tokens):
        split: List[str] = []
        for token in tokens:
            parts = [part for part in _PARTICLE_RE.split(token) if part]
            if len(parts) > 1 and any(len(part) >= 2 for part in parts):
                split.extend(parts)
            else:
                split.append(token)
        if split != tokens:
            pinyins = [to_pinyin(token) if len(token) >= TOKEN_FUZZY_MIN_LEN else ""
                       for token in split]
            variants.append((split, pinyins))
    return variants


def best_variant_score(fields: Dict[str, str],
                       variants: Sequence[Tuple[List[str], List[str]]]) -> float:
    """对一组切分取最高分（fields 必须是同一 normalize 键空间里的归一化字段）。"""
    best = 0.0
    for tokens, pinyins in variants:
        score = score_tokens(tokens, pinyins, fields)
        if score > best:
            best = score
    return best


def edit_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left or not right:
        return len(left) or len(right)
    previous = list(range(len(right) + 1))
    for i, lc in enumerate(left, 1):
        current = [i]
        for j, rc in enumerate(right, 1):
            current.append(min(previous[j] + 1, current[j - 1] + 1,
                               previous[j - 1] + (0 if lc == rc else 1)))
        previous = current
    return previous[-1]


def similarity(left: str, right: str) -> float:
    span = max(len(left), len(right))
    if not span:
        return 0.0
    return 1.0 - edit_distance(left, right) / span


def fuzzy_score(needle: str, candidate: str) -> float:
    """单个关键词对一个归一化候选串的 0-100 分（上游 fuzzyScoreLower）。"""
    if not needle or not candidate:
        return 0.0
    if candidate == needle:
        return 100.0
    if needle in candidate:
        return 50.0 + 1.0 / len(candidate)
    if candidate in needle:
        return 40.0 + 1.0 / len(candidate)
    sim = similarity(needle, candidate)
    if sim > 0.5:
        return sim * 30.0
    return 0.0


def fuzzy_search(query: str, items: Sequence[Tuple[str, str]],
                 limit: int = MAX_SEARCH_RESULTS) -> List[Tuple[float, str, str]]:
    """在 (归一化键, 原值) 列表里按分检索。

    多词查询要求每个词都作为子串出现、取最低分（上游 fuzzySearchList），
    和单词的模糊打分区分开：前者是"又要歌手又要歌名"，后者是容忍错字。
    """
    terms = [normalize(part) for part in _LIST_SPLIT_RE.split((query or "").strip()) if part]
    if not terms or not items:
        return []
    scored: List[Tuple[float, str, str]] = []
    for key, value in items:
        if len(terms) == 1:
            score = fuzzy_score(terms[0], key)
            if score > 0:
                scored.append((score, key, value))
            continue
        lowest = None
        for term in terms:
            if term not in key:
                lowest = None
                break
            single = fuzzy_score(term, key)
            lowest = single if lowest is None else min(lowest, single)
        if lowest:
            scored.append((lowest, key, value))
    scored.sort(key=lambda row: -row[0])
    return scored[:limit]


def token_strength(token: str, token_pinyin: str, field: str, field_pinyin: str) -> float:
    """一个词对一个字段的最强证据（0..1，不含字段权重）。

    阶梯：完全相等 → 字段含词 → 词含字段 → 编辑距离 → 拼音。短词（单字）只走
    前两级，因为"她"这种单字允许模糊会误放行一大片。
    """
    if not field or not token:
        return 0.0
    if field == token:
        return 1.0
    if token in field:
        return 0.9
    fuzzyable = len(token) >= TOKEN_FUZZY_MIN_LEN
    if fuzzyable and len(field) >= 2 and field in token:
        return 0.7
    if fuzzyable:
        sim = similarity(token, field)
        if sim >= 0.6:
            return sim * 0.7
        if token_pinyin and field_pinyin:
            if token_pinyin in field_pinyin:
                return 0.62
            pinyin_sim = similarity(token_pinyin, field_pinyin)
            if pinyin_sim >= 0.75:
                return pinyin_sim * 0.55
    return 0.0


def cjk_direct_match(tokens: Sequence[str], title: str, artist: str) -> bool:
    """每个汉字都必须在标题或艺人里以原字出现。

    这是同音字的硬闸门：「到香」和「稻香」拼音完全一致，若允许纯拼音命中就会放
    错歌 —— 用户听得出来。错字容忍只保留在编辑距离那一层，且仍要求原字出现。
    """
    hay = title + artist
    for token in tokens:
        for char in token:
            if is_cjk(char) and char not in hay:
                return False
    return True


def score_tokens(tokens: Sequence[str], pinyins: Sequence[str],
                 fields: Dict[str, str]) -> float:
    """按 AND 语义给一条曲目打分（0-100）。fields 取 title/artist/album。

    任一够长的词在三个字段上都落空 → 整条判 0；最终得分是命中词的加权强度按
    词数平均，所以"只说歌手"和"歌手+歌名连读"都能落在同一量纲上比较。
    """
    if not tokens:
        return 0.0
    title = fields.get("title", "")
    artist = fields.get("artist", "")
    album = fields.get("album", "")
    if not cjk_direct_match(tokens, title, artist):
        return 0.0
    pinyin_fields = {name: to_pinyin(value) for name, value in
                     (("title", title), ("artist", artist), ("album", album))}
    weighted_sum = 0.0
    matched = 0
    for index, token in enumerate(tokens):
        token_pinyin = pinyins[index] if index < len(pinyins) else ""
        strength = max(
            FIELD_WEIGHT["title"] * token_strength(token, token_pinyin, title, pinyin_fields["title"]),
            FIELD_WEIGHT["artist"] * token_strength(token, token_pinyin, artist, pinyin_fields["artist"]),
            FIELD_WEIGHT["album"] * token_strength(token, token_pinyin, album, pinyin_fields["album"]),
        )
        if strength > 0:
            matched += 1
            weighted_sum += strength
        elif len(token) >= TOKEN_FUZZY_MIN_LEN:
            return 0.0
    if not matched:
        return 0.0
    return weighted_sum / len(tokens) * 100.0


def match_song(query: str, fields: Dict[str, str]) -> float:
    """一步到位：口令对一条曲目（原始字段）打分。索引与临时查询都走这里。"""
    variants = tokenize_variants(query)
    normalized = {name: normalize(value) for name, value in fields.items()}
    return best_variant_score(normalized, variants)


def artist_score(query: str, artist: str, threshold: float = 50.0) -> float:
    """只比艺人名（上游 findSongsByArtist 用 artistLower 单字段打分，阈值 50）。"""
    field = normalize(artist)
    if not field:
        return 0.0
    field_pinyin = to_pinyin(artist)
    best = 0.0
    for tokens, pinyins in tokenize_variants(query):
        if not tokens:
            continue
        total = 0.0
        for index, token in enumerate(tokens):
            strength = token_strength(token, pinyins[index] if index < len(pinyins) else "",
                                      field, field_pinyin)
            if strength <= 0:
                break
            total += strength
        else:
            best = max(best, total / len(tokens) * 100.0)
    return best if best >= threshold else 0.0


def chinese_to_number(text: str) -> Optional[int]:
    """解析「三」「十二」「一百零五」这类中文数字，上限四位。"""
    digits = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
              "六": 6, "七": 7, "八": 8, "九": 9}
    units = {"十": 10, "百": 100, "千": 1000}
    text = (text or "").strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    total = 0
    current = 0
    matched = False
    for char in text:
        if char in digits:
            current = digits[char]
            matched = True
        elif char in units:
            total += (current or 1) * units[char]
            current = 0
            matched = True
        elif char in ("零", "〇"):
            matched = True
        else:
            return None
    if not matched:
        return None
    return total + current


def parse_song_index(text: str) -> Optional[int]:
    """从口令里取「第 N 首」的序号。"""
    match = re.search(r"第\s*([0-9]+|[一二两三四五六七八九十百千零〇]+)\s*首", text or "")
    return chinese_to_number(match.group(1)) if match else None


def parse_time_minutes(text: str) -> Optional[int]:
    """从口令里取时长（分钟）：半小时 / 一个半小时 / 六十分钟 / 1小时20分。"""
    body = text or ""
    number = f"([{_DIGIT_CHARS}]+)"
    if "半小时" in body and not re.search(number + r"\s*个?半小时", body):
        return 30
    both = re.search(number + r"\s*个?小时" + r".*?" + number + r"\s*分", body)
    if both:
        return (chinese_to_number(both.group(1)) or 0) * 60 + (chinese_to_number(both.group(2)) or 0)
    hour = re.search(number + r"\s*个?半小时", body)
    if hour:
        return (chinese_to_number(hour.group(1)) or 1) * 60 + 30
    hour_only = re.search(number + r"\s*个?小时", body)
    if hour_only:
        return (chinese_to_number(hour_only.group(1)) or 0) * 60
    minute = re.search(number + r"\s*分(?:钟)?", body)
    if minute:
        return chinese_to_number(minute.group(1))
    if "半" in body:
        return 30
    return None


def parse_song_count(text: str) -> Optional[int]:
    """从口令里取「再放 N 首」的 N。"""
    match = re.search(rf"([{_DIGIT_CHARS}]+)\s*首", text or "")
    return chinese_to_number(match.group(1)) if match else None


def best_match(needle: str, candidates: Sequence[str],
               threshold: float = 50.0) -> Optional[Tuple[float, str]]:
    """在一个原始串列表里找归一化后最像 needle 的那条（上游 findPlaylistByName 的第三段）。

    阈值默认 50 = fuzzyScoreLower 的"反向包含"档：宁可回一句没找到，
    也不要"播放歌单 我的"结果放了一个含"我"字的杂牌歌单。
    """
    key = normalize(needle)
    if not key:
        return None
    best: Optional[Tuple[float, str]] = None
    for candidate in candidates:
        score = fuzzy_score(key, normalize(candidate))
        if score >= threshold and (best is None or score > best[0]):
            best = (score, candidate)
    return best


def strip_song_suffix(text: str) -> str:
    """剥掉「…的歌曲 / …的音乐」这类结尾，避免它们污染曲名匹配。"""
    return re.sub(r"的?(歌曲|歌儿|歌|音乐|曲子|曲|所有歌|全部歌)\s*$", "", text or "")


def fuzzy_subseq(keyword: str, text: str, max_gap: int = 4, min_len: int = 3) -> bool:
    """关键词以"允许插入噪声"的方式子序列匹配（口令里夹了语气词时用）。"""
    needle = normalize(keyword)
    hay = normalize(text)
    if not needle or not hay:
        return False
    if len(needle) < min_len:
        return needle in hay
    position = 0
    gap = 0
    for char in hay:
        if position < len(needle) and char == needle[position]:
            position += 1
            gap = 0
            if position == len(needle):
                return True
            continue
        if position:
            gap += 1
            if gap > max_gap:
                return False
    return position == len(needle)
