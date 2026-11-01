"""
lyrics.py — 库内音频的歌词读取与 LRC 解析。

上游 songloft-plugin-miot 的歌词是向宿主曲库要的（`/api/v1/songs/{id}/lyric`），
MoviePilot 没有这个接口，也没有"曲目 ID"这个概念。这里改成 MP 里能确定的两件事：
同目录的 `.lrc`/`.txt` 侧车文件，以及音频自己带的歌词标签。两者都没有就是没有，
插件不去联网找词，也不假装能找到。

解析只做显示要用的最小集合：时间戳、正文、`[ar:]`/`[ti:]` 这类元信息行丢掉。
不支持多时间戳一行（`[00:12.00][00:34.00]同一句`）以外的花活 —— 那种写法在
真机上由播放器自己展开，我们这里按第一时间戳排序即可。
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:  # 有 mutagen 才读标签；MP 运行环境里有，插件不强依赖
    from mutagen import File as _MutagenFile  # type: ignore
except Exception:  # pragma: no cover - 取决于宿主环境
    _MutagenFile = None

# 一行里可以有多个时间戳；分钟:秒.毫秒（毫秒 1-3 位都见过）
_TIME_RE = re.compile(r"\[(\d{1,3}):(\d{1,2})(?:[.:](\d{1,3}))?\]")
# 元信息行：[ar:歌手] [ti:标题] [al:] [by:] [offset:] [length:] [re:] [ve:]
_META_RE = re.compile(r"^\[(ar|ti|al|by|offset|length|re|ve|au)\s*:", re.I)
_LRC_SUFFIXES = (".lrc", ".txt")

# 各容器的歌词标签键：ID3 的 USLT（含带语言的键）、Vorbis 系的大小写三种、MP4 的 ©lyr
_TAG_KEYS = ("USLT", "lyrics", "LYRICS", "Lyrics", "\xa9lyr", "©lyr")


def _ms(fraction: str) -> int:
    """毫秒位补齐：`.5` 是 500ms，`.50` 是 500ms，`.500` 也是。"""
    digits = (fraction or "").ljust(3, "0")[:3]
    return int(digits or 0)


def parse_lrc(text: str) -> List[Dict[str, Any]]:
    """LRC 文本 → 按时间升序的行列表；没有时间戳的行按原文顺序给出（time=-1）。"""
    rows: List[Dict[str, Any]] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        stamps = _TIME_RE.findall(line)
        body = _TIME_RE.sub("", line).strip()
        if not stamps:
            if _META_RE.match(line):
                continue
            rows.append({"time": -1.0, "text": body or line})
            continue
        if _META_RE.match(line):
            continue
        for minute, second, fraction in stamps:
            seconds = int(minute) * 60 + int(second) + _ms(fraction) / 1000.0
            rows.append({"time": round(seconds, 3), "text": body})
    rows.sort(key=lambda row: (row["time"], 0))
    return rows


def decode_bytes(payload: bytes) -> str:
    """侧车歌词的编码不能假定是 UTF-8：国内曲库大量是 GBK。"""
    for encoding in ("utf-8-sig", "gb18030", "big5", "utf-16", "latin-1"):
        try:
            return payload.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            continue
    return ""


def sidecar_path(audio: Path) -> Optional[Path]:
    """同名侧车歌词：`歌.mp3` 对应 `歌.lrc`（其次 `歌.txt`），也认 `歌.mp3.lrc`。"""
    candidates = [audio.with_suffix(suffix) for suffix in _LRC_SUFFIXES]
    candidates.append(audio.parent / f"{audio.name}.lrc")
    for candidate in candidates:
        try:
            if candidate.is_file():
                return candidate.resolve()
        except OSError:
            continue
    return None


def within_roots(path: Path, roots: List[Path]) -> bool:
    """侧车文件也必须在库内：符号链接出去就成读任意文件了。"""
    try:
        real = path.resolve()
    except OSError:
        return False
    for root in roots:
        try:
            root_real = root.resolve()
        except OSError:
            continue
        if root_real in real.parents or real == root_real:
            return True
    return False


def read_embedded(audio: Path) -> str:
    """音频自己带的歌词标签；没有 mutagen 或读失败都是空串。"""
    if _MutagenFile is None:
        return ""
    try:
        handle = _MutagenFile(str(audio))
    except Exception:
        return ""
    if handle is None:
        return ""
    tags = getattr(handle, "tags", None)
    if not tags:
        return ""
    for key in _TAG_KEYS:
        try:
            value = tags.get(key)
        except Exception:
            continue
        text = _tag_text(value)
        if text:
            return text
    return ""


def _tag_text(value: Any) -> str:
    """标签值的形状各容器都不一样：列表、帧对象、bytes 都要能落地成字符串。"""
    if value is None:
        return ""
    if isinstance(value, list):
        for item in value:
            text = _tag_text(item)
            if text:
                return text
        return ""
    if isinstance(value, (bytes, bytearray)):
        return decode_bytes(bytes(value))
    for attr in ("text", "lyrics", "value"):
        raw = getattr(value, attr, None)
        if isinstance(raw, (str, bytes, bytearray)):
            if isinstance(raw, (bytes, bytearray)):
                raw = decode_bytes(bytes(raw))
            if raw.strip():
                return str(raw)
    return str(value) if isinstance(value, str) else ""


def load_lyric(audio: Path, roots: List[Path]) -> Tuple[Dict[str, Any], str]:
    """取一首库内音频的歌词：侧车优先（人肉校对过的多），其次内嵌标签。

    返回 (结果字典, 原因)。原因取值给上层拼提示：`empty` / `not_found`。
    """
    sidecar = sidecar_path(audio)
    if sidecar and within_roots(sidecar, roots):
        try:
            text = decode_bytes(sidecar.read_bytes())
        except OSError:
            text = ""
        rows = parse_lrc(text)
        if rows:
            return {"lines": rows, "source": "sidecar",
                    "name": sidecar.name, "has_time": any(r["time"] >= 0 for r in rows)}, ""
    embedded = read_embedded(audio)
    rows = parse_lrc(embedded)
    if rows:
        return {"lines": rows, "source": "tag", "name": audio.name,
                "has_time": any(r["time"] >= 0 for r in rows)}, ""
    if embedded.strip():
        # 有歌词但没有时间戳：整段当纯文本给前端，不做逐句跟随
        plain = [{"time": -1.0, "text": line.strip()}
                 for line in embedded.splitlines() if line.strip()]
        return {"lines": plain, "source": "tag", "name": audio.name, "has_time": False}, ""
    return {"lines": [], "source": "", "name": audio.name, "has_time": False}, "not_found"


def active_index(lines: List[Dict[str, Any]], position: Optional[float]) -> int:
    """当前该显示第几行：最后一个时间戳不超过播放位置的行。"""
    if not lines or position is None or position < 0:
        return -1
    if all(float(row.get("time") or -1) < 0 for row in lines):
        return -1
    picked = -1
    for index, row in enumerate(lines):
        stamp = float(row.get("time") or -1)
        if stamp < 0:
            continue
        if stamp <= position + 0.5:
            picked = index
        else:
            break
    return picked
