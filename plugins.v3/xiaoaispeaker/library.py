"""
library.py — 曲库索引：把配置目录里的音频变成「一句话能对上」的条目。

v0.1.0 只有目录浏览和文件名子串搜索，够人翻，不够语音用：用户说"播放周杰伦的稻香"，
文件名里未必有"稻香"两个字（可能是《稻香.mp3》、可能写成繁体、可能被识别成"到香"）。
这一层把库里的曲目一次性读进内存，按上游 songloft-plugin-miot 的打分方式做模糊匹配，
并且把"文件夹 / m3u"当成歌单来支持"播放歌单 XX"。

元数据来源按可靠性排序：文件里的标签（mutagen，MP 运行环境里有，但插件不强依赖）、
文件名 `歌手 - 标题` 切分、目录结构（`根/歌手/专辑/曲目`）。三者都可能缺失，所以
每条都留了空字段，匹配层（text.py）自己会跳过空字段。

索引在后台线程重建，读侧永远拿一个已经建好的快照（换引用而不是原地改），所以语音
轮询不会因为扫盘而卡住。落盘的只有"路径 → 上次读到的标签 + mtime/size"这张增量表：
几万台曲库每次冷启动重读标签是等不出来的，而重建一次全量内存索引本来只要几百毫秒。
"""

from __future__ import annotations

import gzip
import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import media as _media
from .text import (
    MAX_SEARCH_RESULTS,
    MIN_MATCH_SCORE,
    artist_score,
    best_match,
    best_variant_score,
    chinese_to_number,
    fuzzy_search,
    normalize,
    normalize_chinese_digits,
    tokenize_variants,
)

try:  # 有标签就读标签，没有就退回文件名与目录结构
    from mutagen import File as _MutagenFile  # type: ignore
except Exception:  # pragma: no cover - 取决于宿主环境
    _MutagenFile = None

# 索引上限：与上游 max_song_index 的量纲一致（要能容下上万首的库），
# 再高就只是把内存和重建时间换成一个更慢的"我还在等"
DEFAULT_SONG_LIMIT = 10000
MIN_SONG_LIMIT = 200
MAX_SONG_LIMIT = 100000

# 目录被当成歌单的条件：深度上限与最少曲目数。根目录下随手一层是歌手/专辑，
# 再深的分层（disc 1/2、CD-A）不是给人报名字用的歌单
PLAYLIST_MAX_DEPTH = 3
PLAYLIST_MIN_SONGS = 2
# 一个歌单最多记多少首，防止"整个库"这种顶层目录变成一个几万首的歌单
PLAYLIST_MAX_SONGS = 2000

_PLAYLIST_EXTS = (".m3u", ".m3u8")
_TRACK_PREFIX_RE = re.compile(r"^\s*\d{1,3}\s*[-_.、)]\s*")
# 只写空格分隔的音轨号（"03 双截鼓"）也常见；限制在一两位且后面必须紧跟非数字，
# 免得《1944》《24 格》这种真名字被削成半截
_BARE_INDEX_RE = re.compile(r"^\d{1,2}\s+(?=[^\d\s])")
_ARTIST_TITLE_RE = re.compile(r"^\s*(?P<artist>[^-\[\(]{1,40}?)\s*-\s+(?P<title>.+?)\s*$")
_BRACKET_NOISE_RE = re.compile(r"\[(?P<inner>[^\]]*)\]")
_NUMERIC_NAME_RE = re.compile(r"^[\d\s.、,]+$")

_TAG_CACHE_VERSION = 1


def clamp_song_limit(raw: Any) -> int:
    """配置里的索引上限：非法值退回默认，合法值夹到区间内。"""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_SONG_LIMIT
    return max(MIN_SONG_LIMIT, min(MAX_SONG_LIMIT, value))


def _clean_title(raw: str) -> str:
    """剥掉标题里的 "[官方MV]"、"（Live）" 这类尾巴之外的装饰，只留可搜索的部分。"""
    text = _BRACKET_NOISE_RE.sub(" ", raw or "")
    return re.sub(r"\s{2,}", " ", text).strip()


def _split_name(stem: str) -> Tuple[str, str]:
    """从 `01 - 歌手 - 标题` / `03 标题` 这类文件名里切出（歌手, 标题）；切不出来就都给标题。"""
    body = _TRACK_PREFIX_RE.sub("", stem).strip()
    body = _BARE_INDEX_RE.sub("", body).strip()
    match = _ARTIST_TITLE_RE.match(body)
    if match:
        artist = match.group("artist").strip()
        title = match.group("title").strip()
        # "周杰伦 - 稻香" 之外还有 "01 - 稻香"：前者艺术家不能是纯数字
        if artist and title and not _NUMERIC_NAME_RE.fullmatch(artist):
            return artist, title
    return "", body


def _tag_text(value: Any) -> str:
    """easy 模式返回值可能是列表（一个标签多个值）或字符串，统一成一个字符串。"""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    return str(getattr(value, "text", value) or "").strip()


def read_tags(path: Path) -> Dict[str, Any]:
    """读一首曲目的标签；读不到就按文件名与目录结构补。

    用 mutagen 的 easy 模式：mp3/mp4/flac/ogg 的标签键各不相同（TIT2 / ©nam /
    Vorbis 的小写名），easy 模式统一成 title/artist/album，比按容器猜键可靠。
    个别损坏文件会抛异常或返回 None，那不该让整轮扫描失败——一条曲目的元数据
    缺失只是它匹配不上，不是库不可用。
    """
    title = artist = album = ""
    duration = 0
    if _MutagenFile is not None:
        try:
            audio = _MutagenFile(str(path), easy=True)
            if audio is not None:
                info = getattr(audio, "info", None)
                if info is not None:
                    duration = int(getattr(info, "length", 0) or 0)
                tags = getattr(audio, "tags", None) or {}
                title = _tag_text(tags.get("title"))
                artist = _tag_text(tags.get("artist"))
                album = _tag_text(tags.get("album"))
        except Exception:
            pass

    stem = path.stem
    guess_artist, guess_title = _split_name(stem)
    title = _clean_title(title or guess_title or stem)
    artist = artist or guess_artist
    if not album:
        # 目录约定：曲目的父目录是专辑，父目录的父目录是歌手
        album = path.parent.name
    return {"title": title, "artist": artist, "album": album, "duration": duration}


def _artist_from_path(path: Path, roots: List[Path]) -> str:
    """标签与文件名都没给艺术家时，试着从 `根/歌手/专辑/…` 的层级里取。"""
    for root in roots:
        try:
            parts = path.relative_to(root).parts
        except ValueError:
            continue
        if len(parts) >= 3:
            return parts[-3] if parts[-2] == path.parent.name else parts[-2]
    return ""


class Song:
    """一条曲目：原始字段 + 匹配用的归一化字段。"""

    __slots__ = ("path", "name", "title", "artist", "album", "duration",
                 "size", "mtime", "title_lower", "artist_lower", "album_lower")

    def __init__(self, path: str, name: str, title: str, artist: str, album: str,
                 duration: int, size: int, mtime: int) -> None:
        self.path = path
        self.name = name
        self.title = title
        self.artist = artist
        self.album = album
        self.duration = duration
        self.size = size
        self.mtime = mtime
        self.title_lower = normalize(title)
        self.artist_lower = normalize(artist)
        self.album_lower = normalize(album)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "name": self.name,
            "title": self.title,
            "artist": self.artist,
            "album": self.album,
            "duration": self.duration,
            "size": self.size,
            "mtime": self.mtime,
        }

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"<Song {self.artist} - {self.title}>"


class Playlist:
    """一个歌单：名字 + 成员曲目路径。名字来自目录名或 m3u 文件名。"""

    __slots__ = ("id", "name", "path", "kind", "song_paths")

    def __init__(self, pid: int, name: str, path: str, kind: str) -> None:
        self.id = pid
        self.name = name
        self.path = path
        self.kind = kind  # dir | m3u
        self.song_paths: List[str] = []

    @property
    def song_count(self) -> int:
        return len(self.song_paths)

    def as_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "name": self.name, "path": self.path,
                "kind": self.kind, "song_count": self.song_count}


class LibraryIndex:
    """内存曲库索引。

    用法：`index.refresh()`（后台线程）→ `index.search_song("稻香")` /
    `index.find_playlist_by_name("946")`。读侧只碰公开查询方法，全部返回快照数据，
    重建期间的并发读最多拿到上一轮的索引，不会看到半截状态。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._build_lock = threading.Lock()
        self._songs: List[Song] = []
        self._by_path: Dict[str, Song] = {}
        self._playlists: List[Playlist] = []
        self._playlist_by_id: Dict[int, Playlist] = {}
        self._songs_by_playlist: Dict[int, List[Song]] = {}
        self._tag_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_path: Optional[Path] = None
        self._built_at = 0.0
        self._scanned_at = 0.0
        self._last_error = ""
        self._skipped = 0
        self._truncated = False

    # ---------- 构建 ----------

    def configure(self, cache_path: Optional[Path]) -> None:
        """指定标签增量缓存文件；换目录（重装插件）时自动丢弃旧缓存。"""
        if cache_path == self._cache_path:
            return
        self._cache_path = cache_path
        self._tag_cache = {}
        if cache_path is None:
            return
        try:
            if cache_path.exists():
                with gzip.open(cache_path, "rt", encoding="utf-8") as handle:
                    raw = json.load(handle)
                if isinstance(raw, dict) and int(raw.get("v") or 0) == _TAG_CACHE_VERSION:
                    entries = raw.get("songs")
                    if isinstance(entries, dict):
                        self._tag_cache = entries
        except (OSError, ValueError):
            self._tag_cache = {}

    @property
    def ready(self) -> bool:
        return bool(self._songs)

    @property
    def song_count(self) -> int:
        return len(self._songs)

    @property
    def playlist_count(self) -> int:
        return len(self._playlists)

    def status(self) -> Dict[str, Any]:
        """给前端展示与排障用的索引概况。"""
        return {
            "ready": self.ready,
            "songs": len(self._songs),
            "playlists": len(self._playlists),
            "built_at": _format_time(self._built_at),
            "scanned_at": _format_time(self._scanned_at),
            "seconds": round(max(self._built_at - self._scanned_at, 0.0), 1) if self._built_at else 0,
            "skipped": self._skipped,
            "truncated": self._truncated,
            "error": self._last_error,
            "cached_tags": len(self._tag_cache),
        }

    def collect(self, roots: List[Path], exts: Tuple[str, ...],
                limit: int = DEFAULT_SONG_LIMIT) -> int:
        """全量重扫（同步）。返回入库曲目数。

        语音轮询走的是 `refresh`，只有手动"重建索引"和插件启动才直接调这里。
        """
        started = time.time()
        found: List[Tuple[Path, os.stat_result]] = []
        playlists_src: List[Tuple[str, str, str]] = []
        try:
            found, playlists_src, skipped, truncated = _scan(roots, exts, limit)
        except Exception as err:  # pragma: no cover - 文件系统异常不该打断插件
            with self._lock:
                self._last_error = f"scan_failed: {err}"
            return 0
        scanned = time.time()
        songs: List[Song] = []
        for path, stat in found:
            song = self._song_from(path, stat, roots)
            if song is not None:
                songs.append(song)
        playlists = self._build_playlists(playlists_src, songs, roots)
        with self._lock:
            self._songs = songs
            self._by_path = {item.path: item for item in songs}
            self._playlists = playlists
            self._playlist_by_id = {item.id: item for item in playlists}
            self._songs_by_playlist = {}
            self._scanned_at = scanned
            self._built_at = time.time()
            self._skipped = skipped
            self._truncated = truncated
            self._last_error = ""
        self._save_cache()
        return len(songs)

    def refresh(self, roots: List[Path], exts: Tuple[str, ...],
                limit: int = DEFAULT_SONG_LIMIT, force: bool = False) -> bool:
        """后台重建。同一时刻只允许一轮在跑，重复调用直接忽略。

        返回是否真的启动了重建。
        """
        if not force and self.ready and (time.time() - self._built_at) < _MIN_REFRESH_SECONDS:
            return False
        if not self._build_lock.acquire(blocking=False):
            return False
        try:
            threading.Thread(
                target=self._collect_guarded,
                args=(list(roots), tuple(exts), limit),
                name="xiaoai-library-index",
                daemon=True,
            ).start()
        except RuntimeError:  # pragma: no cover - 解释器退出阶段
            self._build_lock.release()
            return False
        return True

    def _collect_guarded(self, roots: List[Path], exts: Tuple[str, ...], limit: int) -> None:
        try:
            self.collect(roots, exts, limit)
        finally:
            self._build_lock.release()

    def _song_from(self, path: Path, stat: os.stat_result,
                   roots: List[Path]) -> Optional[Song]:
        """一条文件 → 一个 Song，标签优先、增量缓存兜底。"""
        key = str(path)
        mtime = int(stat.st_mtime)
        size = int(stat.st_size)
        cached = self._tag_cache.get(key)
        if (isinstance(cached, dict) and int(cached.get("m") or 0) == mtime
                and int(cached.get("s") or 0) == size):
            title = str(cached.get("t") or "")
            artist = str(cached.get("a") or "")
            album = str(cached.get("b") or "")
            duration = int(cached.get("d") or 0)
        else:
            tags = read_tags(path)
            title, artist, album = tags["title"], tags["artist"], tags["album"]
            duration = tags["duration"]
            self._tag_cache[key] = {"m": mtime, "s": size, "t": title,
                                    "a": artist, "b": album, "d": duration}
        if not title:
            title = _media.strip_index(path.name)
        if not artist:
            artist = _artist_from_path(path, roots)
        return Song(key, path.name, title, artist, album, duration, size, mtime)

    def _build_playlists(self, sources: List[Tuple[str, str, str]],
                         songs: List[Song], roots: List[Path]) -> List[Playlist]:
        """目录歌单 + m3u 歌单，成员按已入库的曲目解析。"""
        by_path = {item.path: item for item in songs}
        playlists: List[Playlist] = []
        next_id = 1
        for kind, name, raw_path in sources:
            playlist = Playlist(next_id, name, raw_path, kind)
            next_id += 1
            if kind == "m3u":
                playlist.song_paths = _parse_m3u(Path(raw_path), roots, by_path)
            else:
                directory = Path(raw_path)
                for item in songs:
                    if item.path.startswith(raw_path + os.sep):
                        playlist.song_paths.append(item.path)
            if len(playlist.song_paths) < (1 if kind == "m3u" else PLAYLIST_MIN_SONGS):
                continue
            playlists.append(playlist)
        return playlists

    def _save_cache(self) -> None:
        if self._cache_path is None:
            return
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._cache_path.with_suffix(self._cache_path.suffix + ".tmp")
            with gzip.open(tmp, "wt", encoding="utf-8") as handle:
                json.dump({"v": _TAG_CACHE_VERSION, "songs": self._tag_cache}, handle)
            os.replace(tmp, self._cache_path)
        except (OSError, ValueError):
            pass

    def prune(self, alive: Dict[str, int]) -> None:
        """把已经不在库里的标签记录去掉（`alive` 为 路径→mtime）。"""
        if not self._tag_cache:
            return
        for key in list(self._tag_cache):
            stamp = alive.get(key)
            if stamp is None or int(self._tag_cache[key].get("m") or 0) != stamp:
                self._tag_cache.pop(key, None)

    # ---------- 查询 ----------

    def get(self, path: str) -> Optional[Song]:
        return self._by_path.get(path or "")

    def songs(self, limit: int = 500) -> List[Dict[str, Any]]:
        return [item.as_dict() for item in self._songs[:limit]]

    def search_song(self, query: str, limit: int = MAX_SEARCH_RESULTS,
                    min_score: float = MIN_MATCH_SCORE) -> List[Tuple[float, Dict[str, Any]]]:
        """按口令在库里找曲目（上游 searchSong）。分数降序，低于阈值的不要。

        归一化字段在建索引时就绪，查询侧只做打分；切分方式（原样 / 按"的"拆开）
        取最高分，所以"播放周杰伦的稻香"和"播放周杰伦 稻香"落到同一条曲目。
        """
        variants = tokenize_variants(query)
        if not variants or not variants[0][0]:
            return []
        scored: List[Tuple[float, Song]] = []
        for item in self._songs:
            score = best_variant_score({"title": item.title_lower,
                                        "artist": item.artist_lower,
                                        "album": item.album_lower}, variants)
            if score >= min_score:
                scored.append((score, item))
        scored.sort(key=lambda row: (-row[0], row[1].title))
        return [(score, song.as_dict()) for score, song in scored[:limit]]

    def find_songs_by_artist(self, query: str,
                             limit: int = MAX_SEARCH_RESULTS) -> List[Tuple[float, Dict[str, Any]]]:
        """只按艺人名找（上游 findSongsByArtist：单字段、阈值 50）。"""
        scored = [(artist_score(query, item.artist), item) for item in self._songs]
        hits = [(score, song.as_dict()) for score, song in scored if score > 0]
        hits.sort(key=lambda row: -row[0])
        return dedupe(hits)[:limit]

    def playlists(self, limit: int = 200) -> List[Dict[str, Any]]:
        return [item.as_dict() for item in self._playlists[:limit]]

    def search_playlist(self, query: str,
                        limit: int = MAX_SEARCH_RESULTS) -> List[Tuple[float, Dict[str, Any]]]:
        """模糊搜歌单名；空口令回前 limit 个（上游 searchPlaylist 的"默认歌单"语义）。"""
        items = [(normalize(item.name), item.as_dict()) for item in self._playlists]
        if not (query or "").strip():
            return [(0.0, row) for _, row in items[:limit]]
        return fuzzy_search(query, items, limit)

    def find_playlist_by_name(self, name: str) -> Optional[Playlist]:
        """精确 → 中文数字等价 → 模糊，三段命中（上游 findPlaylistByName）。"""
        text = normalize(name or "")
        if not text:
            return None
        for item in self._playlists:
            if normalize(item.name) == text:
                return item
        numeric = normalize_chinese_digits(text)
        for item in self._playlists:
            if normalize_chinese_digits(normalize(item.name)) == numeric:
                return item
        hit = best_match(text, [item.name for item in self._playlists])
        if not hit:
            return None
        return next((item for item in self._playlists if item.name == hit[1]), None)

    def get_playlist(self, pid: int) -> Optional[Playlist]:
        return self._playlist_by_id.get(int(pid or 0))

    def playlist_songs(self, playlist: Playlist) -> List[Song]:
        """歌单成员，按目录序（上游 getSongsOfPlaylist）。"""
        cached = self._songs_by_playlist.get(playlist.id)
        if cached is not None:
            return cached
        resolved = [self._by_path[path] for path in playlist.song_paths if path in self._by_path]
        self._songs_by_playlist[playlist.id] = resolved
        return resolved

    def find_song_in_playlist(self, playlist: Playlist,
                              name: str) -> Optional[Tuple[int, Song]]:
        """在歌单内定位曲目，返回 (序号, 曲目)（上游 findSongInPlaylist）。"""
        songs = self.playlist_songs(playlist)
        if not songs:
            return None
        variants = tokenize_variants(name)
        if not variants or not variants[0][0]:
            return None
        scored = [(best_variant_score({"title": item.title_lower,
                                       "artist": item.artist_lower,
                                       "album": item.album_lower}, variants), index, item)
                  for index, item in enumerate(songs)]
        scored.sort(key=lambda row: -row[0])
        best = scored[0]
        if best[0] < MIN_MATCH_SCORE:
            return None
        return best[1], best[2]

    def find_artist_albums(self, artist: str) -> List[Tuple[str, List[Song]]]:
        """按歌手收集专辑（语音"播放 XX 的歌"要一个能连播的集合）。"""
        key = normalize(artist)
        if not key:
            return []
        groups: Dict[str, List[Song]] = {}
        for item in self._songs:
            if key and key in item.artist_lower:
                groups.setdefault(item.album or item.title, []).append(item)
        return sorted(groups.items(), key=lambda row: -len(row[1]))


def _scan(roots: List[Path], exts: Tuple[str, ...],
          limit: int) -> Tuple[List[Tuple[Path, os.stat_result]],
                               List[Tuple[str, str, str]], int, bool]:
    """一次遍历同时产出音频文件清单与歌单来源（目录 / m3u）。

    深度上限沿用浏览那套：库的目录层级一般不超过 5 层，再深下去多半是
    归档盘或软件目录；上限换来的是"整库重建能在几秒内完成"。
    """
    found: List[Tuple[Path, os.stat_result]] = []
    sources: List[Tuple[str, str, str]] = []
    skipped = 0
    max_depth = 6
    for root in roots:
        if not root.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            current = Path(dirpath)
            rel_parts = () if current == root else current.relative_to(root).parts
            dirnames[:] = sorted(name for name in dirnames
                                 if not name.startswith(".") and name not in _media.SKIP_DIRS
                                 and len(rel_parts) < max_depth)
            try:
                entries = [(name, (current / name).stat()) for name in filenames]
            except OSError:
                skipped += len(filenames)
                dirnames[:] = []
                continue
            for name, stat in entries:
                lower = name.lower()
                if lower.endswith(_PLAYLIST_EXTS) and len(rel_parts) <= PLAYLIST_MAX_DEPTH:
                    sources.append(("m3u", Path(name).stem, str(current / name)))
                    continue
                if not lower.endswith(exts):
                    continue
                if stat.st_size <= 0:
                    skipped += 1
                    continue
                found.append((current / name, stat))
                if len(found) >= limit:
                    return found, sources, skipped, True
            has_audio = any(name.lower().endswith(exts) for name in filenames)
            if (current != root and has_audio and len(rel_parts) <= PLAYLIST_MAX_DEPTH):
                sources.append(("dir", current.name, str(current)))
    return found, sources, skipped, False


def _parse_m3u(path: Path, roots: List[Path],
               by_path: Dict[str, Song]) -> List[str]:
    """解析 m3u：相对路径按 m3u 所在目录展开，只保留确实入库的条目。"""
    try:
        raw = path.read_bytes()
    except OSError:
        return []
    for encoding in ("utf-8-sig", "gb18030", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        return []
    resolved: List[str] = []
    base_dir = path.parent
    for line in text.splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        candidate = Path(entry)
        if not candidate.is_absolute():
            candidate = base_dir / entry
        try:
            # 索引里的路径是 os.walk 原样给出的，可能带 /tmp → /private/tmp 这类软链差异，
            # 所以两种写法都试一次；判据仍是"这条路径确实在库里"，越库引用自然落空
            variants = (os.path.normpath(str(candidate)), str(candidate.resolve()))
        except OSError:
            continue
        for text in variants:
            if text in by_path and text not in resolved:
                resolved.append(text)
                break
        if len(resolved) >= PLAYLIST_MAX_SONGS:
            break
    return resolved


def parse_track_index(text: str, total: int) -> Optional[int]:
    """口令里的"第 N 首"，越界返回 None（上游越界会退回第一首，这里交给调用方决定）。"""
    match = re.search(r"第\s*([0-9]+|[一二两三四五六七八九十百千零〇]+)\s*首", text or "")
    if not match:
        return None
    number = chinese_to_number(match.group(1))
    if number is None or number < 1 or number > total:
        return None
    return number - 1


def format_song(song: Dict[str, Any]) -> str:
    """给 TTS 与命令行用的一行曲目描述。"""
    artist = str(song.get("artist") or "").strip()
    title = str(song.get("title") or song.get("name") or "").strip()
    return f"{artist} - {title}" if artist else title


def _format_time(stamp: float) -> str:
    if not stamp:
        return ""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stamp))


def dedupe(items: Iterable[Tuple[float, Dict[str, Any]]]) -> List[Tuple[float, Dict[str, Any]]]:
    """按路径去重，保留分数更高的那条。"""
    best: Dict[str, Tuple[float, Dict[str, Any]]] = {}
    for score, song in items:
        path = str(song.get("path") or "")
        current = best.get(path)
        if current is None or score > current[0]:
            best[path] = (score, song)
    return list(best.values())


# 手动重建之外的自动节流：语音未命中触发的重建不该变成扫盘循环
_MIN_REFRESH_SECONDS = 60.0
