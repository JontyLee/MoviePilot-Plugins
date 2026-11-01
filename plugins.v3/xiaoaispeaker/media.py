"""
media.py — 音箱可拉取的音频：目录浏览、路径安全边界与签名直链。

小爱音箱是自己（经云端下发的地址）去 HTTP 拉流的，所以插件必须给出一个
音箱网络可达的绝对 URL。这里负责三件事：
1. 只在配置的可播放目录内枚举/搜索文件，越界一律拒；
2. 用插件自己的密钥给「路径 + 过期时间」签名，让 /stream 这个必须匿名可达的
   端点不至于变成任意文件读取；
3. 按 Range 分片回文件，音箱拖动进度或重试时不会整首重传。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import mimetypes
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse

# 音箱能直接解码的容器/编码。flac 只有部分型号支持，ape/wv 这类基本不出声，
# 所以默认列表偏保守，其余靠配置里的「音频扩展名」放开。
DEFAULT_AUDIO_EXTS: Tuple[str, ...] = (
    ".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".oga", ".opus", ".wma",
)

# 一次列举的上限：侧栏页是给人翻的，不是索引器；目录里几万个文件时
# 全盘扫一次会把请求时间拉到用户以为页面挂了。
MAX_BROWSE_ITEMS = 500

# 排除目录名（与宿主的 Jellyfin 缩略图缓存等噪声目录保持一致）
SKIP_DIRS = {".trickplay", "@eaDir", "#recycle", ".thumbnails", "node_modules"}

_CHUNK_SIZE = 512 * 1024

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)$", re.I)

# 非 mp3 现场转成 mp3：小爱的 player_play_url 只稳当吃 mp3，m4a/flac 推过去常常是
# 「云端受理、设备亮灯不出声」（上游 songloft-plugin-miot 把它做成 force_mp3 开关，
# 他们的 issue #62 就是这个现象）。主程序镜像里带 ffmpeg，转码留在本机，不外发。
FFMPEG_BIN = "ffmpeg"
MP3_SUFFIXES = (".mp3",)


def parse_dirs(raw: Any) -> List[Path]:
    """把配置里的多行/逗号分隔目录解析成去重、展开过的绝对路径列表。"""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        parts: Iterable[str] = [str(item) for item in raw]
    else:
        parts = re.split(r"[\r\n,]+", str(raw))
    result: List[Path] = []
    seen = set()
    for part in parts:
        # 去空白、再去引号、最后看一遍空白：配置里写成 " /media/music " 要能落回路径
        text = part.strip().strip('"').strip()
        if not text:
            continue
        path = Path(os.path.expanduser(os.path.expandvars(text)))
        try:
            key = str(path.resolve())
        except OSError:
            continue
        if key in seen:
            continue
        seen.add(key)
        result.append(path)
    return result


def parse_exts(raw: Any) -> Tuple[str, ...]:
    """音频扩展名配置：留空用默认列表，非法项直接丢弃而不是猜。"""
    if not raw:
        return DEFAULT_AUDIO_EXTS
    exts: List[str] = []
    for part in re.split(r"[,\s]+", str(raw)):
        item = part.strip().lower().lstrip(".")
        if not item or not re.fullmatch(r"[a-z0-9]{1,8}", item):
            continue
        exts.append(f".{item}")
    return tuple(dict.fromkeys(exts)) or DEFAULT_AUDIO_EXTS


def normalize_base_url(raw: str) -> str:
    """音箱拉流用的 MP 外部地址：补 scheme、去尾斜杠，并拒掉带查询串/前缀的写法。

    主程序的外部访问地址允许逗号分隔多个域名，但直链只能有一个 host，取第一个。
    """
    text = next((item.strip() for item in str(raw or "").split(",") if item.strip()), "")
    text = text.rstrip("/")
    if not text:
        return ""
    if "//" not in text:
        text = f"http://{text}"
    parsed = urlparse_safe(text)
    if parsed is None:
        return ""
    return f"{parsed[0]}://{parsed[1]}{parsed[2]}".rstrip("/")


def urlparse_safe(text: str) -> Optional[Tuple[str, str, str]]:
    """返回 (scheme, netloc, path)，解析不出 host 时给 None。

    host 里带空白的必须判非法：配置里写成 "192.168.1.10 3000" 这类手误，urlparse
    会把整串当成 netloc 而不会报错，拼出来的直链音箱一定拉不到，不如在这里拦掉。
    """
    parsed = urlparse(text)
    netloc = parsed.netloc
    if not netloc or any(char.isspace() for char in netloc):
        return None
    return parsed.scheme or "http", netloc, parsed.path or ""


def _is_audio(path: Path, exts: Tuple[str, ...]) -> bool:
    return path.is_file() and path.suffix.lower() in exts


def resolve_playable(path_text: str, roots: List[Path],
                     exts: Tuple[str, ...]) -> Tuple[Optional[Path], str]:
    """把调用方给的路径收敛到「库内真实音频文件」。

    返回 (Path|None, reason)。/stream 是匿名可达端点，任何一步放宽都等于把
    整个文件系统暴露成可下载；所以这里用 realpath 而不是字符串前缀比较 ——
    软链、`..`、`/data/music/../etc` 与同一目录的别名形式都必须判成同一个东西。
    """
    raw = (path_text or "").strip()
    if not raw:
        return None, "empty_path"
    candidate = Path(unquote(raw))
    if not candidate.is_absolute():
        return None, "not_absolute"
    try:
        real = candidate.resolve()
    except OSError:
        return None, "unresolvable"
    if not real.is_file():
        return None, "not_found"
    if not roots:
        return None, "no_roots"
    if real.suffix.lower() not in exts:
        return None, "not_audio"
    allowed = False
    for root in roots:
        try:
            root_real = root.resolve()
        except OSError:
            continue
        if real == root_real or root_real in real.parents:
            allowed = True
            break
    if not allowed:
        return None, "out_of_library"
    return real, ""


def browse(directory: Path, roots: List[Path], exts: Tuple[str, ...],
           limit: int = MAX_BROWSE_ITEMS) -> Dict[str, Any]:
    """列出一个目录里的子目录与音频文件（不递归），带越界与配额保护。"""
    try:
        real = directory.resolve()
    except OSError:
        return {"reason": "unresolvable", "dirs": [], "files": [], "cwd": str(directory)}
    if not real.is_dir():
        return {"reason": "not_dir", "dirs": [], "files": [], "cwd": str(directory)}
    inside = any(real == root.resolve() or root.resolve() in real.parents
                 for root in roots if root.is_dir()) if roots else False
    if not inside:
        return {"reason": "out_of_library", "dirs": [], "files": [], "cwd": str(real)}

    dirs: List[Dict[str, Any]] = []
    files: List[Dict[str, Any]] = []
    reason = ""
    try:
        entries = sorted(real.iterdir(), key=lambda p: p.name.lower())
    except OSError as err:
        return {"reason": f"read_failed: {err}", "dirs": [], "files": [], "cwd": str(real)}
    for entry in entries:
        name = entry.name
        if name.startswith(".") or name in SKIP_DIRS:
            continue
        try:
            if entry.is_dir():
                dirs.append({"name": name, "path": str(entry)})
                continue
            if _is_audio(entry, exts):
                stat = entry.stat()
                files.append({
                    "name": name,
                    "path": str(entry),
                    "title": strip_index(name),
                    "size": stat.st_size,
                    "mtime": int(stat.st_mtime),
                })
        except OSError:
            continue
        if len(dirs) + len(files) >= limit:
            reason = "truncated"
            break
    return {"reason": reason, "cwd": str(real), "dirs": dirs, "files": files,
            "total": len(dirs) + len(files)}


def search(keyword: str, roots: List[Path], exts: Tuple[str, ...],
           limit: int = 100) -> List[Dict[str, Any]]:
    """按文件名在库内搜索音频，命中数到 limit 就停。

    深度上限 6 层：整库几万个文件时无脑 rglob 会先把请求拖死；真正的整库
    索引进侧栏页是后续版本的事（README 里也这么写）。
    """
    text = (keyword or "").strip().lower()
    if not text:
        return []
    hits: List[Dict[str, Any]] = []
    for root in roots:
        if not root.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            depth = Path(dirpath).relative_to(root).parts if Path(dirpath) != root else ()
            dirnames[:] = [d for d in dirnames
                           if not d.startswith(".") and d not in SKIP_DIRS and len(depth) < 6]
            for name in filenames:
                if not text in name.lower():
                    continue
                candidate = Path(dirpath) / name
                if not _is_audio(candidate, exts):
                    continue
                try:
                    stat = candidate.stat()
                except OSError:
                    continue
                hits.append({
                    "name": name,
                    "path": str(candidate),
                    "title": strip_index(name),
                    "size": stat.st_size,
                    "mtime": int(stat.st_mtime),
                })
                if len(hits) >= limit:
                    return hits
    return hits


def strip_index(filename: str) -> str:
    """去掉 "01 - " / "01." 这类音轨前缀，只做展示用。"""
    stem = re.sub(r"\.[A-Za-z0-9]{1,5}$", "", filename)
    return re.sub(r"^\s*\d{1,3}\s*[-_.、)]\s*", "", stem) or stem


def sign_path(secret: str, path: str, expires: int) -> str:
    """对「路径 + 过期时间」签名，只取前 32 位十六进制，够用且短。"""
    payload = f"{path}|{expires}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    return digest[:32]


def verify_sign(secret: str, path: str, expires: int, signature: str) -> bool:
    """定长比较，避免把签名做成计时侧信道。"""
    if not signature or expires < int(_now()):
        return False
    return hmac.compare_digest(sign_path(secret, path, expires), signature[:32])


def _now() -> float:
    return time.time()


def pack_link(secret: str, path: str, expires: int) -> str:
    """把「有效期.签名.路径」压成一段纯 ASCII，直接当 URL 里唯一那个参数值。

    小米云端会把交给它的 URL 里的 `&` 换成空格（实测：推过去的是
    `path=...&exp=...&sig=...`，音箱发回来的是 `path=...%20exp=...%20sig=...`），
    多参数直链在音箱那边必然散架。所以整条链只留一个值，并且这个值不含
    `%`、`+`、`&`、空格 —— 中间任何一层再编码或解码都动不了它。
    """
    digest = sign_path(secret, path, int(expires))
    body = base64.urlsafe_b64encode(path.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{int(expires)}.{digest}.{body}"


def unpack_link(raw: str) -> Tuple[str, int, str]:
    """还原 pack_link 的那一段；形状不对就回空，让调用方按「链接无效」处理。"""
    parts = (raw or "").strip().split(".")
    if len(parts) != 3:
        return "", 0, ""
    expires_text, signature, body = parts
    # base64url 的字母表是 -_，标准解码器只认 +/：换回去再严格校验
    standard = body.replace("-", "+").replace("_", "/")
    try:
        expires = int(expires_text)
        path = base64.b64decode(standard + "=" * (-len(standard) % 4),
                                validate=True).decode("utf-8")
    except Exception:
        return "", 0, ""
    return path, expires, signature


def stream_url(base_url: str, plugin_id: str, secret: str, path: Path,
               ttl: int) -> str:
    """拼出音箱要访问的完整直链。"""
    text = str(path)
    expires = int(_now()) + max(60, int(ttl))
    return (f"{base_url}/api/v1/plugin/{plugin_id}/stream"
            f"?k={pack_link(secret, text, expires)}")


def content_disposition(name: str) -> str:
    """能安全塞进 HTTP 头的 Content-Disposition。

    头必须是 latin-1 编码，直接把中文名塞进 filename= 会让框架在编码头时抛
    UnicodeEncodeError —— 整个取流请求变成 500，而且只有文件名带非 ASCII 的
    歌曲才踩得到。这里给一个 ASCII 兜底名，再用 RFC 5987 的 filename* 带上原名。
    """
    name = str(name or "")
    stem, suffix = os.path.splitext(name)
    fallback = re.sub(r"[^\x20-\x7e]", "", stem).replace('"', "").strip() or "audio"
    tail = re.sub(r"[^\x20-\x7e]", "", suffix).strip()
    return (f'inline; filename="{fallback}{tail}"; '
            f"filename*=UTF-8''{quote(name, safe='')}")


def status_text(status: Optional[int]) -> str:
    """player_get_play_status 的 status 数字转文案。"""
    mapping = {0: "已停止", 1: "播放中", 2: "已暂停", -1: "读不到"}
    if status is None:
        return "未知"
    return mapping.get(int(status), f"未知({status})")


def content_type(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(path.name)
    if guessed:
        return guessed
    return {
        ".flac": "audio/flac",
        ".m4a": "audio/mp4",
        ".opus": "audio/opus",
        ".wma": "audio/x-ms-wma",
    }.get(path.suffix.lower(), "audio/mpeg")


def parse_range(header: Optional[str], size: int) -> Tuple[int, int]:
    """解析 Range 头，返回闭区间；不合规的写法一律回整文件。"""
    if not header or size <= 0:
        return 0, max(size - 1, 0)
    match = _RANGE_RE.match(header.strip())
    if not match:
        return 0, size - 1
    start_text, end_text = match.group(1), match.group(2)
    if not start_text:
        # bytes=-N 是「最后 N 字节」
        if not end_text:
            return 0, size - 1
        length = min(int(end_text), size)
        return size - length, size - 1
    start = int(start_text)
    end = int(end_text) if end_text else size - 1
    if start >= size or end < start:
        return 0, size - 1
    return start, min(end, size - 1)


def iter_file(path: Path, start: int, end: int) -> Iterable[bytes]:
    """按块读取 [start, end] 区间，避免大文件整首进内存。"""
    with open(path, "rb") as handle:
        handle.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            block = handle.read(min(_CHUNK_SIZE, remaining))
            if not block:
                break
            remaining -= len(block)
            yield block


class TranscodeError(RuntimeError):
    """转码起不来（ffmpeg 没了、文件坏了）。调用方当次回 502，不静默降级。"""


def ffmpeg_available() -> bool:
    """本机有没有 ffmpeg。开关开着但机器上没有时宁可播源格式，不要把播放整条弄坏。"""
    return bool(shutil.which(FFMPEG_BIN))


def needs_transcode(path: Path, force: bool) -> bool:
    """只在开了开关、且这条本来就不是 mp3 时才转 —— 能给音箱播的 mp3 不必再拉一次 ffmpeg。"""
    return bool(force) and path.suffix.lower() not in MP3_SUFFIXES


def transcode_command(path: Path) -> List[str]:
    """转 mp3 的 argv。列表形式交给 exec，不经过 shell，路径里的空格与引号都不参与解析。"""
    return [
        FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-nostdin",
        "-i", str(path),
        # m4a/flac 常带封面图流，只取第一条音频
        "-map", "0:a:0", "-vn",
        "-acodec", "libmp3lame", "-q:a", "2",
        "-f", "mp3", "pipe:1",
    ]


def start_transcode(path: Path) -> "subprocess.Popen":
    """拉起 ffmpeg。在回响应头之前起，坏文件与起不来才能当成一次明确的失败回给人。"""
    try:
        return subprocess.Popen(
            transcode_command(path),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
    except OSError as exc:
        raise TranscodeError(f"起不了 ffmpeg：{exc}")


def iter_transcode(proc: "subprocess.Popen") -> Iterable[bytes]:
    """把 ffmpeg 的输出边转边给（长度未知，所以整条不给 Range）。"""
    try:
        while True:
            block = proc.stdout.read(_CHUNK_SIZE) if proc.stdout else b""
            if not block:
                break
            yield block
    finally:
        # 页面关掉、音箱断开都要把 ffmpeg 收掉，不然每次中断留一个进程
        if proc.poll() is None:
            proc.kill()
        proc.wait()


def parse_query(query: Any) -> Tuple[str, int, str]:
    """从查询参数里取出 path/exp/sig，Mapping（FastAPI 的 query_params）和原始串都吃。

    parse_qsl 只吃字符串，直接把 Mapping 喂给它会只剩键名、什么都取不到。
    """
    if isinstance(query, str):
        params: Dict[str, str] = {
            key: value
            for key, value in (item.split("=", 1) for item in query.split("&") if "=" in item)
        }
    else:
        try:
            params = dict(query or {})
        except (TypeError, ValueError):
            params = {}
    return unpack_link(str(params.get("k") or ""))
