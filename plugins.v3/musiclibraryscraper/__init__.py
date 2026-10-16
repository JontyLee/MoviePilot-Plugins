"""
musiclibraryscraper — MoviePilot v3 插件

用 Music Tag Web 作为音乐元数据来源，MP 侧走 ScrapingChain 原生 MUSIC 写入。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.scraping import ScrapingChain
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.media import (
    MetaInfoPath,
    MusicInfo,
)
from app.sdk.network import RequestUtils
from app.plugins import _PluginBase


# MP 支持的音乐扩展名，独立于视频扩展名白名单。
_MUSIC_EXT = (".flac", ".mp3", ".m4a", ".ape", ".wav", ".wv", ".tta",
              ".aiff", ".aif", ".opus", ".ogg", ".mpc", ".dsf", ".dff", ".mka")


class _MusicScrapeChain(ScrapingChain):
    """复用 libraryscraper 的 trickplay 排除策略，避免 Jellyfin 预览目录被递归。"""

    def scrape_metadata(self, fileitem, *args, **kwargs):
        if fileitem and MusicLibraryScraper._is_trickplay_path(Path(fileitem.path)):
            return False, "跳过 Jellyfin 预览缓存目录"
        return super().scrape_metadata(fileitem, *args, **kwargs)


class MusicLibraryScraper(_PluginBase):
    """扫描音乐库，向 Music Tag Web 拉候选，交由 MP ScrapingChain 完成写入。"""

    plugin_name = "音乐库刮削 (MTW)"
    plugin_desc = "定时扫描音乐目录，用 Music Tag Web 的网易云 / QQ / 酷我等源做识别，写入沿用 MP 原生刮削链路。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/163music_A.png"
    plugin_version = "0.1.0"
    plugin_author = "local"
    author_url = ""
    plugin_config_prefix = "musiclibraryscraper_"
    plugin_order = 8
    user_level = 1

    _scheduler: Optional[BackgroundScheduler] = None
    _event = Event()

    _enabled: bool = False
    _onlyonce: bool = False
    _cron: Optional[str] = None
    _scraper_paths: str = ""
    _exclude_paths: str = ""
    _force_all: bool = False
    _mtw_base_url: str = ""
    _mtw_username: str = ""
    _mtw_password: str = ""
    _mtw_resource: str = "netease"
    _mtw_select_mode: int = 0
    _min_score: int = 3
    _token: Optional[str] = None
    _token_expiry: float = 0.0

    def init_plugin(self, config: Dict[str, Any] = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._cron = config.get("cron") or None
            self._scraper_paths = config.get("scraper_paths") or ""
            self._exclude_paths = config.get("exclude_paths") or ""
            self._force_all = config.get("mode") == "force_all"
            self._mtw_base_url = (config.get("mtw_base_url") or "").rstrip("/")
            self._mtw_username = config.get("mtw_username") or ""
            self._mtw_password = config.get("mtw_password") or ""
            self._mtw_resource = config.get("mtw_resource") or "netease"
            try:
                self._mtw_select_mode = int(config.get("mtw_select_mode") or 0)
            except (TypeError, ValueError):
                self._mtw_select_mode = 0
            try:
                self._min_score = int(config.get("min_score") or 3)
            except (TypeError, ValueError):
                self._min_score = 3

        self.stop_service()

        if not (self._enabled or self._onlyonce):
            return

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.__scan,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="音乐库刮削（一次性）",
            )
            self._onlyonce = False
            self.update_config({
                "onlyonce": False,
                "enabled": self._enabled,
                "cron": self._cron,
                "scraper_paths": self._scraper_paths,
                "exclude_paths": self._exclude_paths,
                "mode": "force_all" if self._force_all else "",
                "mtw_base_url": self._mtw_base_url,
                "mtw_username": self._mtw_username,
                "mtw_password": self._mtw_password,
                "mtw_resource": self._mtw_resource,
                "mtw_select_mode": self._mtw_select_mode,
                "min_score": self._min_score,
            })

        if self._enabled and not self._scheduler:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if self._enabled and self._cron:
            logger.info(f"[music-scraper] 注册定时任务 cron={self._cron}")
            self._scheduler.add_job(
                func=self.__scan,
                trigger=CronTrigger.from_crontab(self._cron),
                name="音乐库刮削",
            )
        elif self._enabled:
            logger.info("[music-scraper] 未配置 cron，使用默认 0 3 * * 7")
            self._scheduler.add_job(
                func=self.__scan,
                trigger=CronTrigger.from_crontab("0 3 * * 7"),
                name="音乐库刮削",
            )

        if self._scheduler and self._scheduler.get_jobs() and not self._scheduler.running:
            self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as err:
            logger.debug(f"[music-scraper] 停止任务异常: {err}")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "mode", "label": "覆盖模式",
                                "items": [
                                    {"title": "只补空 tag", "value": ""},
                                    {"title": "覆盖已有 tag", "value": "force_all"},
                                ]
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VCronField", "props": {
                                "model": "cron", "label": "执行周期", "placeholder": "5 位 cron，留空默认 0 3 * * 7"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "mtw_resource", "label": "MTW 识别源",
                                "items": [
                                    {"title": "网易云 netease", "value": "netease"},
                                    {"title": "QQ 音乐 qmusic", "value": "qmusic"},
                                    {"title": "酷我 kuwo", "value": "kuwo"},
                                    {"title": "酷狗 kugou", "value": "kugou"},
                                    {"title": "咪咕 migu", "value": "migu"},
                                    {"title": "指纹 acoustid", "value": "acoustid"},
                                    {"title": "智能 smart_tag", "value": "smart_tag"},
                                ]
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "mtw_base_url",
                                "label": "Music Tag Web 地址",
                                "placeholder": "http://192.168.x.x:8001"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "mtw_select_mode", "label": "MTW 候选排序",
                                "items": [
                                    {"title": "0 默认", "value": 0},
                                    {"title": "1 精准匹配（时长）", "value": 1},
                                ]
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "mtw_username", "label": "MTW 用户名", "type": "text"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "mtw_password", "label": "MTW 密码",
                                "type": "password", "placeholder": "仅本机保存，不入 git"
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VTextarea", "props": {
                                "model": "scraper_paths", "rows": 5,
                                "label": "音乐库路径（一行一个目录）",
                                "placeholder": "/media/music/周杰伦\n/media/music"
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VTextarea", "props": {
                                "model": "exclude_paths", "rows": 2,
                                "label": "排除路径",
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VAlert", "props": {
                                "type": "info", "variant": "tonal",
                                "text": "MTW 与 MP 需挂载同一份音乐库路径，否则写标签会落到不同文件。"
                                        "覆盖模式=覆盖已有 tag 时，任何非空 tag 都会被重写。"
                            }}
                        ]},
                    ]},
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "mode": "",
            "cron": "0 3 * * 7",
            "scraper_paths": "",
            "exclude_paths": "",
            "mtw_base_url": "",
            "mtw_username": "",
            "mtw_password": "",
            "mtw_resource": "netease",
            "mtw_select_mode": 0,
            "min_score": 3,
        }

    def get_page(self) -> List[dict]:
        return []

    # ---------------- 主体流程 ----------------

    def __scan(self):
        if not self._scraper_paths or not self._mtw_base_url:
            logger.warning("[music-scraper] 未配置 scraper_paths 或 mtw_base_url，跳过")
            return

        exclude_paths = [p for p in (self._exclude_paths or "").split("\n") if p.strip()]
        files: List[Path] = []
        for root in (p for p in self._scraper_paths.split("\n") if p.strip()):
            root_path = Path(root)
            if not root_path.exists():
                logger.warning(f"[music-scraper] 路径不存在: {root}")
                continue
            for fp in root_path.rglob("*"):
                if self._event.is_set():
                    logger.info("[music-scraper] 收到停止信号")
                    return
                if not fp.is_file():
                    continue
                if fp.suffix.lower() not in _MUSIC_EXT:
                    continue
                if self._is_trickplay_path(fp):
                    continue
                if any(fp.is_relative_to(Path(ex)) for ex in exclude_paths if ex):
                    continue
                files.append(fp)

        logger.info(f"[music-scraper] 发现 {len(files)} 个音频文件，开始处理")

        ok, skipped, failed = 0, 0, 0
        for fp in files:
            if self._event.is_set():
                break
            if not self._force_all and self.__has_core_tags(fp):
                skipped += 1
                continue
            if self.__scrape_file(fp):
                ok += 1
            else:
                failed += 1

        logger.info(f"[music-scraper] 完成：成功 {ok}，跳过已打 tag {skipped}，失败 {failed}")

    def __has_core_tags(self, path: Path) -> bool:
        """粗略判断是否已具备 artist/album/title。用 MetaInfoPath 会读 tag 里的 title。"""
        try:
            meta = MetaInfoPath(path)
            return bool(meta.title and (meta.artist or getattr(meta, "artists", None)))
        except Exception:
            return False

    def __scrape_file(self, path: Path) -> bool:
        candidates = self._mtw_fetch_candidates(path)
        if not candidates:
            logger.debug(f"[music-scraper] 无候选: {path}")
            return False
        picked = self._mtw_pick_best(path, candidates)
        if not picked:
            logger.info(f"[music-scraper] 未达阈值 min_score={self._min_score}: {path.name}")
            return False
        info = self._candidate_to_musicinfo(picked, path)
        if not info:
            logger.debug(f"[music-scraper] 候选转 MusicInfo 失败: {path}")
            return False
        fileitem = schemas.FileItem(
            type="file",
            name=path.name,
            path=str(path),
            extension=path.suffix.lstrip("."),
            size=path.stat().st_size if path.exists() else 0,
        )
        ok, msg = _MusicScrapeChain().scrape_metadata(
            fileitem=fileitem,
            mediainfo=info,
            overwrite=self._force_all,
        )
        if ok:
            logger.info(f"[music-scraper] 刮削成功: {path.name} → {info.title}")
            return True
        logger.warning(f"[music-scraper] 刮削失败: {path.name} — {msg}")
        return False

    # ---------------- MTW HTTP 客户端 ----------------

    def _mtw_headers(self) -> Dict[str, str]:
        token = self._mtw_token()
        return {"Authorization": f"JWT {token}"} if token else {}

    def _mtw_token(self) -> Optional[str]:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token
        if not (self._mtw_username and self._mtw_password):
            logger.warning("[music-scraper] 未配置 MTW 账密，将匿名请求（可能失败）")
            return None
        resp = RequestUtils(headers={"Content-Type": "application/json"}).post_dict(
            url=f"{self._mtw_base_url}/api/token/",
            jsondata={"username": self._mtw_username, "password": self._mtw_password},
        )
        if not resp or not resp.get("token"):
            logger.error(f"[music-scraper] MTW JWT 获取失败: {resp}")
            return None
        self._token = resp["token"]
        self._token_expiry = time.time() + 6 * 24 * 3600
        return self._token

    def _mtw_fetch_candidates(self, path: Path) -> List[dict]:
        body = {
            "resource": self._mtw_resource,
            "title": path.name,
            "full_path": str(path),
        }
        resp = RequestUtils(
            headers={"Content-Type": "application/json", **self._mtw_headers()},
        ).post_dict(
            url=f"{self._mtw_base_url}/api/fetch_id3_by_title/",
            jsondata=body,
        )
        if not resp:
            return []
        data = resp.get("data") if isinstance(resp, dict) else None
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        return data if isinstance(data, list) else []

    def _mtw_fetch_lyric(self, song_id: str) -> Optional[str]:
        resp = RequestUtils(
            headers={"Content-Type": "application/json", **self._mtw_headers()},
        ).post_dict(
            url=f"{self._mtw_base_url}/api/fetch_lyric/",
            jsondata={"resource": self._mtw_resource, "song_id": str(song_id)},
        )
        if not resp:
            return None
        data = resp.get("data")
        return data if isinstance(data, str) and data and not data.startswith("未找到") else None

    # ---------------- 候选匹配 & 字段映射 ----------------

    def _mtw_pick_best(self, path: Path, candidates: List[dict]) -> Optional[dict]:
        meta = MetaInfoPath(path)
        want = {
            "title": (meta.title or "").strip().lower(),
            "artist": (meta.artist or "").strip().lower(),
            "album": (meta.album or "").strip().lower(),
            "duration": getattr(meta, "duration", None),
        }
        best, best_score = None, 0
        for c in candidates:
            score = self._score_candidate(c, want)
            if score > best_score:
                best, best_score = c, score
        return best if best_score >= self._min_score else None

    @staticmethod
    def _score_candidate(c: dict, want: dict) -> int:
        """MTW utils.match_song 简化版：title / artist / album 各 0/1。"""
        score = 0
        got_title = str(c.get("name") or c.get("title") or "").strip().lower()
        if want["title"] and got_title and (want["title"] in got_title or got_title in want["title"]):
            score += 1
        artists_raw = c.get("artists") or c.get("artist") or []
        if isinstance(artists_raw, str):
            got_artist = artists_raw.strip().lower()
        elif isinstance(artists_raw, list):
            got_artist = " ".join(
                str(a.get("name") if isinstance(a, dict) else a) for a in artists_raw
            ).strip().lower()
        else:
            got_artist = ""
        if want["artist"] and got_artist and (want["artist"] in got_artist or got_artist in want["artist"]):
            score += 1
        album_obj = c.get("album") or {}
        got_album = ""
        if isinstance(album_obj, dict):
            got_album = str(album_obj.get("name") or album_obj.get("title") or "").strip().lower()
        elif isinstance(album_obj, str):
            got_album = album_obj.strip().lower()
        if want["album"] and got_album and (want["album"] in got_album or got_album in want["album"]):
            score += 1
        return score

    def _candidate_to_musicinfo(self, c: dict, path: Path) -> Optional[MusicInfo]:
        """把 MTW 候选折叠成 MP MusicInfo。

        MTW 各源（netease / qmusic / kuwo / kugou / migu）在候选里字段命名不完全一致，
        常见差异：artist 是字符串而非 list、album 是字符串而非 dict、cover 字段位于
        顶层 album_img / pic / album.picUrl；year 常见是 "YYYY-MM-DD" 完整日期串。
        lyrics 已经在候选里返回，不再二次调 fetch_lyric。
        """
        title = str(c.get("name") or c.get("title") or "").strip()
        if not title:
            return None

        artist_list = self._normalize_artists(c.get("artists") or c.get("artist"))

        album_obj = c.get("album")
        if isinstance(album_obj, dict):
            album = str(album_obj.get("name") or album_obj.get("title") or "").strip() or None
        elif isinstance(album_obj, str):
            album = album_obj.strip() or None
        else:
            album = None

        cover_url = None
        for key in ("album_img", "pic", "cover", "img", "album_pic", "picture"):
            value = c.get(key)
            if isinstance(value, str) and value:
                cover_url = value
                break
        if not cover_url and isinstance(album_obj, dict):
            for key in ("picUrl", "pic", "cover", "img"):
                value = album_obj.get(key)
                if isinstance(value, str) and value:
                    cover_url = value
                    break

        album_artist = None
        for key in ("albumartist", "album_artist"):
            value = c.get(key)
            if isinstance(value, str) and value.strip():
                album_artist = value.strip()
                break

        year: Optional[int] = None
        for key in ("year", "publish_time", "publishTime", "pub_time", "ptime"):
            publish = c.get(key)
            year = self._normalize_year(publish)
            if year:
                break

        duration_s = self._normalize_duration(c.get("duration") or c.get("dt"))

        track_number = c.get("tracknumber") or c.get("track_number") or c.get("index") or c.get("no")
        try:
            track_number = int(track_number) if track_number is not None and track_number != 0 else None
        except (TypeError, ValueError):
            track_number = None

        total_tracks = c.get("totaltracks") or c.get("total_tracks")
        try:
            total_tracks = int(total_tracks) if total_tracks else None
        except (TypeError, ValueError):
            total_tracks = None

        disc_number = c.get("discnumber") or c.get("disc_number")
        try:
            disc_number = int(disc_number) if disc_number else None
        except (TypeError, ValueError):
            disc_number = None

        total_discs = c.get("totaldiscs") or c.get("total_discs")
        try:
            total_discs = int(total_discs) if total_discs else None
        except (TypeError, ValueError):
            total_discs = None

        lyrics: Optional[str] = None
        raw_lyrics = c.get("lyrics") or c.get("lyric") or c.get("lrc")
        if isinstance(raw_lyrics, str) and raw_lyrics.strip() and not raw_lyrics.startswith("未找到"):
            lyrics = raw_lyrics

        composers = self._normalize_artists(c.get("composer"))
        genres: List[str] = []
        for key in ("genre", "language"):
            value = c.get(key)
            if isinstance(value, str) and value.strip():
                genres.append(value.strip())

        song_id = str(c.get("id") or c.get("song_id") or c.get("mid") or "").strip() or None
        source = str(c.get("resource") or self._mtw_resource).strip()

        return MusicInfo(
            type="音乐",
            music_type="recording",
            title=title,
            artist=artist_list[0] if artist_list else None,
            artists=artist_list,
            composers=composers,
            album=album,
            album_artist=album_artist or (artist_list[0] if artist_list else None),
            year=year,
            track_number=track_number,
            total_tracks=total_tracks,
            disc_number=disc_number,
            total_discs=total_discs,
            duration=duration_s,
            cover_url=cover_url,
            lyrics=lyrics,
            genres=genres,
            raw_data={
                "mtw_resource": source,
                "mtw_song_id": song_id,
                "mtw_candidate": c,
            },
            field_sources={
                "title": "remote",
                "artist": "remote",
                "album": "remote",
                "year": "remote",
                "lyrics": "remote",
                "cover_url": "remote",
            },
        )

    @staticmethod
    def _normalize_artists(value: Any) -> List[str]:
        """把 artist/artists/composer 三种常见结构统一为 list[str]。"""
        if isinstance(value, str):
            parts = [p.strip() for p in value.replace(";", "/").split("/") if p.strip()]
            return parts or [value.strip()]
        if isinstance(value, list):
            return [
                str(a.get("name") if isinstance(a, dict) else a).strip()
                for a in value
                if (isinstance(a, dict) and a.get("name")) or (isinstance(a, str) and a.strip())
            ]
        return []

    @staticmethod
    def _normalize_year(value: Any) -> Optional[int]:
        if isinstance(value, int) and 1900 <= value <= 2100:
            return value
        if isinstance(value, str):
            digits = "".join(ch for ch in value[:10] if ch.isdigit())
            if len(digits) >= 4:
                try:
                    year = int(digits[:4])
                    if 1900 <= year <= 2100:
                        return year
                except ValueError:
                    return None
        if isinstance(value, (int, float)) and value > 1e9:
            try:
                ts = float(value)
                if ts > 1e12:
                    ts /= 1000
                return datetime.utcfromtimestamp(ts).year
            except Exception:
                return None
        return None

    @staticmethod
    def _normalize_duration(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)):
            seconds = int(value)
            if seconds > 10000:
                seconds //= 1000
            return seconds if seconds > 0 else None
        return None

    # ---------------- 工具 ----------------

    @staticmethod
    def _is_trickplay_path(path: Path) -> bool:
        return any(parent.name.lower().endswith(".trickplay") for parent in path.parents) or (
            path.name.lower().endswith(".trickplay") and path.is_dir()
        )
