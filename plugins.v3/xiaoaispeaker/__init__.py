"""
xiaoaispeaker — MoviePilot v3 插件（插件名：小爱音箱）

把 MoviePilot 的音乐库推到小爱音箱上放：侧栏播放台（设备、音量、播放控制、队列连播）、
库内目录浏览与搜索、以及一组远程命令。播放走「插件签发一条带签名与有效期的 HTTP 直链，
音箱自己经云端拉流」这条路，不依赖 Emby/Jellyfin，也不需要额外部署 Music Tag Web。

v0.1.0 的边界（有意为之，不是遗漏）：
- 只支持小米云端 Mina 接口（登录 → serviceToken → /remote/ubus），不做 MiIO 本地 RPC：
  局域网直连要额外拿 token 与 miio 密钥、按型号试 siid/piid，收益不抵复杂度。
- 只放库内文件与调用方显式给的一条 URL，不做歌词、不做触屏封面匹配。
- 队列连播靠轮询 player_get_play_status 判「播完」，不是音箱侧的真实播放列表。
- /stream 必须匿名可达（音箱不会带上 MP 的登录态），因此用「路径 + 过期时间」的 HMAC 签名
  收敛成「只能取配置目录内、在有效期内的音频文件」，签名不匹配或路径越库一律 403。

出处与许可：小米云端的登录三步流程、clientSign 算法、ubus 请求体、需要走 play_music 的
型号白名单，移植自 Songloft 的「智能音箱」插件 songloft-plugin-miot
（https://github.com/songloft-org/songloft-plugin-miot ，Apache-2.0 © 2026 hanxi），
并与 Yonsm/MiService（MIT）交叉核对；详见 mina.py 顶部说明与同目录
LICENSE.upstream-songloft-miot.txt。
"""

from __future__ import annotations

import secrets
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.interval import IntervalTrigger
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.plugins import _PluginBase
from app.schemas.types import EventType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger

from . import media as _media
from . import mina as _mina
from .mina import (
    LOGIN_NEED_CAPTCHA,
    LOGIN_NEED_VERIFY,
    LOGIN_SUCCESS,
    MinaAuthError,
    MinaError,
    XiaoaiAuth,
    XiaoaiClient,
    token_from_manual,
)
from .schemas import (
    ActionOut,
    BrowseOut,
    ConfigIn,
    ConfigOut,
    ControlIn,
    DeviceOut,
    DeviceSelectIn,
    DevicesOut,
    DirOut,
    FileOut,
    LoginIn,
    LoginOut,
    PlayIn,
    PlayOut,
    PlayingOut,
    QueueIn,
    QueueItemOut,
    QueueOut,
    SearchOut,
    StatusOut,
    TtsIn,
    VolumeIn,
)

try:  # 主程序较新版本才有：一次性任务交给宿主调度器，插件不再自建 scheduler
    from app.sdk import scheduler as scheduler_sdk
except Exception:  # pragma: no cover - 老版本主程序没有该模块
    scheduler_sdk = None

# plugindata 键：票据、设备缓存、队列、签名密钥、最近一次推流
_KEY_TOKEN = "token"
_KEY_DEVICES = "devices"
_KEY_QUEUE = "queue"
_KEY_SECRET = "secret"
_KEY_LAST = "last"

# 音箱状态是云端读来的，前端每几秒刷一次；给一个很短的缓存避免把小米接口打爆
_STATUS_CACHE_SECONDS = 3.0
# 设备列表基本不变，缓存久一些；侧栏页手动刷新时强制重取
_DEVICES_CACHE_SECONDS = 300.0
# 静默续期失败后的冷却，避免每个请求都去撞小米风控
_REFRESH_COOLDOWN_SECONDS = 300.0
# 连播判定：推流后这么久仍读到「停止」才认为真的播完了
_ADVANCE_GRACE_SECONDS = 25.0
_ADVANCE_TICK_SECONDS = 20

_DEFAULT_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "show_sidebar_nav": True,
    "username": "",
    "password": "",
    "default_device_id": "",
    "music_dirs": "",
    "base_url": "",
    "play_mode": "auto",
    "audio_exts": "",
    "audio_id": "",
    "stream_ttl": 3600,
    "default_volume": 0,
    "auto_next": True,
    "keep_light": False,
    "tts_announce": False,
}

_PLAY_MODES = ("auto", "url", "music")

_FORM_NOTES = (
    "使用说明：\n"
    "1. 音箱自己经 HTTP 拉流，因此「外部访问地址」必须是音箱能访问到的 MoviePilot 地址"
    "（局域网 IP:端口即可，不要写 127.0.0.1）。留空时沿用主程序的「外部访问地址」。\n"
    "2. 「音乐库路径」一行一个目录，只有这些目录（含子目录）里的音频文件会被列进侧栏页，"
    "也只有它们能被 /stream 取到；目录外的路径一律拒绝。\n"
    "3. 直链带签名与有效期（默认 3600 秒），签名密钥由插件首次运行时随机生成、只存在本机。\n"
    "4. 小米账号密码与登录票据只写入本机配置与插件数据目录；需要图形验证码或手机/邮箱验证码时，"
    "请在侧栏页的登录区填写，插件会把这次登录会话保留到验证码交回来。\n"
    "5. 「播放接口」选不定就用自动：白名单内的型号（部分带触屏/新固件的音箱）只认 "
    "player_play_music，其余走 player_play_url。推上去不出声可以先切成另一种再试。\n"
    "6. 登录票据约 12 小时到期，插件会用 passToken 静默续期；续期失败才需要重新登录。"
)

_LOG = "[xiaoai]"


class XiaoaiSpeaker(_PluginBase):
    """小爱音箱播放台：小米云端登录、设备控制、库内选曲与签名直链。"""

    plugin_name = "小爱音箱"
    plugin_desc = (
        "把音乐库推到小爱音箱：侧栏播放台（设备/音量/播放控制/队列连播）、"
        "库内目录浏览与搜索、远程命令；播放直链由插件签名限时，不依赖媒体服务器。"
    )
    plugin_icon = "https://raw.githubusercontent.com/JontyLee/MoviePilot-Plugins/main/icons/xiaoai.svg"
    plugin_version = "0.1.0"
    plugin_author = "JontyLee"
    author_url = "https://github.com/JontyLee"
    plugin_config_prefix = "xiaoaispeaker_"
    plugin_order = 45
    user_level = 1

    # 配置项（初值即 _DEFAULT_CONFIG，init_plugin 里按保存的配置覆盖）
    _enabled: bool = False
    _show_sidebar_nav: bool = True
    _username: str = ""
    _password: str = ""
    _default_device_id: str = ""
    _music_dirs: str = ""
    _base_url: str = ""
    _play_mode: str = "auto"
    _audio_exts: str = ""
    _audio_id: str = ""
    _stream_ttl: int = 3600
    _default_volume: int = 0
    _auto_next: bool = True
    _keep_light: bool = False
    _tts_announce: bool = False

    def init_plugin(self, config: Dict[str, Any] = None) -> None:
        """读取配置并准备运行态（登录会话、缓存、锁）。可重复调用。"""
        # 锁与运行态放实例 __dict__ 并用 setdefault：保存配置会重跑 init_plugin，
        # 直接把进行中的登录会话或连播计数重置掉，等于把人正在填的验证码丢掉。
        self.__dict__.setdefault("_op_lock", threading.Lock())
        self.__dict__.setdefault("_login_lock", threading.Lock())
        self.__dict__.setdefault("_auth", None)
        self.__dict__.setdefault("_auth_state", "")
        self.__dict__.setdefault("_captcha_image", "")
        self.__dict__.setdefault("_status_cache", {})
        self.__dict__.setdefault("_devices_cache", {})
        self.__dict__.setdefault("_refresh_attempt_at", 0.0)
        self.__dict__.setdefault("_idle_ticks", 0)
        self.__dict__.setdefault("_stop_flag", threading.Event())

        if config:
            self._enabled = bool(config.get("enabled"))
            self._show_sidebar_nav = bool(config.get("show_sidebar_nav", True))
            self._username = str(config.get("username") or "").strip()
            self._password = str(config.get("password") or "")
            self._default_device_id = str(config.get("default_device_id") or "").strip()
            self._music_dirs = str(config.get("music_dirs") or "")
            self._base_url = _media.normalize_base_url(str(config.get("base_url") or ""))
            self._play_mode = self._clamp_play_mode(config.get("play_mode"))
            self._audio_exts = str(config.get("audio_exts") or "")
            self._audio_id = str(config.get("audio_id") or "").strip()
            self._stream_ttl = self._clamp_stream_ttl(config.get("stream_ttl"))
            self._default_volume = self._clamp_volume(config.get("default_volume"))
            self._auto_next = bool(config.get("auto_next", True))
            self._keep_light = bool(config.get("keep_light"))
            self._tts_announce = bool(config.get("tts_announce"))
            # 换账号就丢掉旧的登录会话与票据：那属于上一个账号，继续用会把
            # 上一个账号的设备列表和音箱当成当前配置的结果。
            if config.get("username") and config.get("username") != self.__dict__.get("_last_username"):
                self.__dict__["_last_username"] = config.get("username")
                self.__dict__["_auth"] = None
                self.__dict__["_auth_state"] = ""
                self.__dict__["_devices_cache"] = {}
                self.__dict__["_status_cache"] = {}

    @staticmethod
    def _clamp_play_mode(raw: Any) -> str:
        mode = str(raw or "").strip().lower()
        return mode if mode in _PLAY_MODES else "auto"

    @staticmethod
    def _clamp_stream_ttl(raw: Any) -> int:
        """有效期收敛到 5 分钟 ~ 3 天：签名 URL 是匿名可达的，不给无限期。"""
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            return int(_DEFAULT_CONFIG["stream_ttl"])
        return min(max(value, 300), 259200)

    @staticmethod
    def _clamp_volume(raw: Any) -> int:
        """0 = 不改动音箱音量；其余夹到 0-100。"""
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            return 0
        return min(max(value, 0), 100)

    def get_state(self) -> bool:
        return bool(self._enabled)

    # ---------------- 配置与库范围 ----------------

    def _roots(self) -> List[Path]:
        return _media.parse_dirs(self._music_dirs)

    def _exts(self) -> Tuple[str, ...]:
        return _media.parse_exts(self._audio_exts)

    def _effective_base_url(self) -> Tuple[str, str]:
        """返回 (地址, 来源)：配置优先，其次主程序的外部访问地址。"""
        if self._base_url:
            return self._base_url, "config"
        try:
            from app.sdk.config import settings
            domain = _media.normalize_base_url(str(settings.APP_DOMAIN or ""))
        except Exception:  # pragma: no cover - 宿主配置读取异常形状不固定
            domain = ""
        return (domain, "app_domain") if domain else ("", "")

    def _config_snapshot(self) -> Dict[str, Any]:
        """当前配置的全量字典：update_config 是整体替换，任何写回都必须用这份。"""
        return {
            "enabled": self._enabled,
            "show_sidebar_nav": self._show_sidebar_nav,
            "username": self._username,
            "password": self._password,
            "default_device_id": self._default_device_id,
            "music_dirs": self._music_dirs,
            "base_url": self._base_url,
            "play_mode": self._play_mode,
            "audio_exts": self._audio_exts,
            "audio_id": self._audio_id,
            "stream_ttl": self._stream_ttl,
            "default_volume": self._default_volume,
            "auto_next": self._auto_next,
            "keep_light": self._keep_light,
            "tts_announce": self._tts_announce,
        }

    def _save_config(self) -> None:
        self.update_config(self._config_snapshot())

    def _secret(self) -> str:
        """直链签名密钥：首次随机生成后留在本机，换密钥会让已发出的链接失效（可接受）。"""
        data = self.get_data(_KEY_SECRET)
        if isinstance(data, str) and data:
            return data
        secret = secrets.token_hex(16)
        self.save_data(_KEY_SECRET, secret)
        return secret

    # ---------------- 票据与客户端 ----------------

    def _token(self) -> Optional[Dict[str, Any]]:
        data = self.get_data(_KEY_TOKEN)
        if not isinstance(data, dict):
            return None
        return data if data.get("user_id") else None

    def _build_client(self, token: Optional[Dict[str, Any]] = None) -> XiaoaiClient:
        return XiaoaiClient(token or {}, audio_id=self._audio_id)

    def _client(self) -> Tuple[Optional[XiaoaiClient], str]:
        """拿一个可用客户端；票据过期时先试静默续期。"""
        token = self._token()
        if not token:
            return None, "尚未登录小米账号，请在侧栏页登录"
        client = self._build_client(token)
        if client.token_expired():
            fresh, err = self._silent_refresh()
            if not fresh:
                return None, err or "小米登录票据已过期，请重新登录"
            client = self._build_client(fresh)
        if not client.is_usable():
            return None, "小米登录票据不可用，请重新登录"
        return client, ""

    def _silent_refresh(self) -> Tuple[Optional[Dict[str, Any]], str]:
        """用存档里的 passToken 换一个新的 serviceToken（带冷却）。"""
        now = time.time()
        if now - float(self.__dict__.get("_refresh_attempt_at") or 0) < _REFRESH_COOLDOWN_SECONDS:
            return None, "小米登录票据已过期，静默续期刚失败过，请重新登录"
        self.__dict__["_refresh_attempt_at"] = now
        token = self._token()
        if not token:
            return None, "尚未登录小米账号，请在侧栏页登录"
        try:
            state, fresh, message = XiaoaiAuth.from_token(token).refresh()
        except Exception as err:  # pragma: no cover - 网络层异常形状不固定
            logger.warning(f"{_LOG} 静默续期异常: {err}")
            return None, f"静默续期失败：{err}"
        if state != LOGIN_SUCCESS or not fresh:
            logger.warning(f"{_LOG} 静默续期未成功: {message}")
            return None, f"{message or '静默续期失败'}，请重新登录"
        merged = dict(fresh)
        merged.setdefault("username", token.get("username"))
        self.save_data(_KEY_TOKEN, merged)
        self.__dict__["_devices_cache"] = {}
        logger.info(f"{_LOG} 已静默续期小米登录票据")
        return merged, ""

    # ---------------- 设备 ----------------

    def _device_by_id(self, device_id: str) -> Optional[Dict[str, Any]]:
        devices, _ = self._devices_cached(force=False)
        for item in devices:
            if item.get("device_id") == device_id:
                return item
        return None

    def _target_device(self, device_id: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], str]:
        """解析要操作的音箱：显式指定 > 配置默认 > 唯一一台在线 > 唯一一台。"""
        devices, _ = self._devices_cached(force=False)
        wanted = (device_id or self._default_device_id or "").strip()
        if wanted:
            for item in devices:
                if item.get("device_id") == wanted:
                    return item, ""
            # 默认设备可能已被移出账号：明确说出来，不要悄悄换一台播放
            return None, f"音箱 {wanted} 不在当前账号的设备列表里，请重新选择"
        if not devices:
            return None, "账号下没有可用音箱，请先刷新设备列表"
        online = [d for d in devices if d.get("presence") != "offline"]
        pick = (online or devices)[0]
        return pick, ""

    def _devices_cached(self, force: bool = False) -> Tuple[List[Dict[str, Any]], str]:
        """设备列表：优先用缓存，过期或强制时重新拉取（失败则退回上次结果）。"""
        cache = self.__dict__.get("_devices_cache") or {}
        cached_at = float(cache.get("at") or 0)
        if not force and cache.get("devices") and time.time() - cached_at < _DEVICES_CACHE_SECONDS:
            return list(cache["devices"]), str(cache.get("fetched_at") or "")
        client, err = self._client()
        if not client:
            if cache.get("devices"):
                # 拿不到新数据时仍回旧列表：否则票据一过期，侧栏页的设备下拉就整个空掉
                return list(cache["devices"]), str(cache.get("fetched_at") or "")
            return [], err
        try:
            devices = client.get_devices()
        except MinaAuthError as exc:
            logger.warning(f"{_LOG} 设备列表鉴权失败: {exc}")
            if cache.get("devices"):
                return list(cache["devices"]), str(cache.get("fetched_at") or "")
            return [], "小米登录票据已失效，请重新登录"
        except MinaError as exc:
            logger.warning(f"{_LOG} 设备列表获取失败: {exc}")
            return (list(cache["devices"]), str(cache.get("fetched_at") or "")) if cache.get("devices") else ([], str(exc))
        if not devices:
            # 空列表既可能是「账号真的没音箱」也可能是票据失效/接口变更，交给调用方判读
            logger.warning(f"{_LOG} 设备列表为空（票据可用但没有主控设备）")
        fetched_at = self._now_text()
        self.__dict__["_devices_cache"] = {
            "at": time.time(), "fetched_at": fetched_at, "devices": devices
        }
        self.save_data(_KEY_DEVICES, devices)
        return devices, fetched_at

    def _devices_out(self, devices: List[Dict[str, Any]]) -> List[DeviceOut]:
        rows: List[DeviceOut] = []
        for item in devices:
            hardware = str(item.get("hardware") or "")
            rows.append(DeviceOut(
                device_id=str(item.get("device_id") or ""),
                name=str(item.get("name") or item.get("alias") or ""),
                alias=str(item.get("alias") or ""),
                model=str(item.get("model") or ""),
                hardware=hardware,
                presence=str(item.get("presence") or "offline"),
                is_default=bool(self._default_device_id)
                          and item.get("device_id") == self._default_device_id,
                needs_music_api=hardware in _mina.MUSIC_API_MODELS,
            ))
        return rows

    # ---------------- 播放 ----------------

    def _push(self, url: str, device: Dict[str, Any], title: str = "",
              path: str = "", source: str = "") -> Tuple[bool, str, str]:
        """把一条直链推给音箱，成功时记下最近一次播放。返回 (ok, message, api)。"""
        client, err = self._client()
        if not client:
            return False, err, ""
        device_id = str(device.get("device_id") or "")
        try:
            result = client.play(
                device_id=device_id,
                url=url,
                hardware=str(device.get("hardware") or ""),
                play_mode=self._play_mode,
                keep_light=self._keep_light,
            )
        except MinaAuthError as exc:
            return False, f"小米登录票据已失效：{exc}", ""
        except MinaError as exc:
            return False, f"推送失败：{exc}", ""
        api = str(result.get("api") or "")
        if not result.get("ok"):
            return False, str(result.get("error") or "音箱拒绝该播放请求"), api
        if self._default_volume:
            try:
                client.set_volume(device_id, self._default_volume)
            except MinaError as exc:
                # 音量失败不影响已经推上去的曲目，只把这条限制说清楚
                logger.info(f"{_LOG} 设置音量失败: {exc}")
        self.save_data(_KEY_LAST, {
            "device_id": device_id,
            "device_name": str(device.get("name") or ""),
            "path": path,
            "title": title,
            "url": url,
            "source": source,
            "api": api,
            "started_at": time.time(),
            "at": self._now_text(),
        })
        self.__dict__["_idle_ticks"] = 0
        return True, f"已推送到 {device.get('name') or device_id}", api

    def _play_file(self, path_text: str, device_id: Optional[str] = None,
                   queue_paths: Optional[List[str]] = None) -> PlayOut:
        """播放库内一个文件（可同时设定队列）。"""
        base_url, _ = self._effective_base_url()
        if not base_url:
            return PlayOut(ok=False, message="没有配置音箱可访问的外部地址，请在插件配置里填写「外部访问地址」")
        real, reason = _media.resolve_playable(path_text, self._roots(), self._exts())
        if not real:
            return PlayOut(ok=False, message=self._reject_text(reason))
        device, err = self._target_device(device_id)
        if not device:
            return PlayOut(ok=False, message=err)
        url = _media.stream_url(base_url, self._plugin_id(), self._secret(), real, self._stream_ttl)
        with self._play_lock():
            ok, message, api = self._push(
                url, device, title=_media.strip_index(real.name),
                path=str(real), source="library")
        queue = None
        if ok:
            paths = [str(p) for p in (queue_paths or []) if str(p).strip()]
            if paths:
                queue = self._set_queue_state(paths, self._index_of(paths, str(real)),
                                              str(device.get("device_id") or ""))
            else:
                queue = self._queue_out()
        return PlayOut(ok=ok, message=message, api=api,
                       playing=self._playing_out(device, path=str(real), url=url) if ok else None,
                       queue=queue)

    def _play_url(self, url: str, device_id: Optional[str] = None) -> PlayOut:
        """播放调用方给定的一条外部直链（不签名、不改写，原样推给音箱）。"""
        text = (url or "").strip()
        if not text.lower().startswith(("http://", "https://")):
            return PlayOut(ok=False, message="只接受 http(s) 音频直链")
        device, err = self._target_device(device_id)
        if not device:
            return PlayOut(ok=False, message=err)
        with self._play_lock():
            ok, message, api = self._push(text, device, title=text.rsplit("/", 1)[-1][:80],
                                         path="", source="url")
        return PlayOut(ok=ok, message=message, api=api,
                       playing=self._playing_out(device, url=text) if ok else None,
                       queue=self._queue_out())

    def _play_keyword(self, keyword: str, device_id: Optional[str] = None) -> PlayOut:
        """按曲名在库内搜索，播第一个命中（远程命令用的快捷入口）。"""
        hits = _media.search(keyword, self._roots(), self._exts(), limit=20)
        if not hits:
            return PlayOut(ok=False, message=f"音乐库里没找到「{keyword}」，检查关键词或「音乐库路径」")
        paths = [hit["path"] for hit in hits]
        return self._play_file(paths[0], device_id=device_id, queue_paths=paths)

    def _set_queue_state(self, paths: List[str], index: int, device_id: str) -> QueueOut:
        """只写队列状态，不推流（推流由调用方负责，避免两处各推一次）。"""
        items: List[Dict[str, Any]] = []
        for raw in paths:
            real, reason = _media.resolve_playable(raw, self._roots(), self._exts())
            if not real:
                logger.warning(f"{_LOG} 队列丢弃 {raw}: {reason}")
                continue
            items.append({"path": str(real), "name": real.name,
                          "title": _media.strip_index(real.name), "size": real.stat().st_size})
        safe_index = index if 0 <= index < len(items) else (0 if items else -1)
        state = {
            "device_id": device_id,
            "index": safe_index,
            "items": items,
            "source": "library",
            "updated_at": self._now_text(),
        }
        self.save_data(_KEY_QUEUE, state)
        self.__dict__["_idle_ticks"] = 0
        return self._queue_out(state)

    @staticmethod
    def _index_of(paths: List[str], target: str) -> int:
        for position, raw in enumerate(paths):
            if str(raw) == target:
                return position
        return 0

    def _queue_state(self) -> Dict[str, Any]:
        data = self.get_data(_KEY_QUEUE)
        if not isinstance(data, dict):
            return {"device_id": "", "index": -1, "items": [], "source": ""}
        items = data.get("items")
        if not isinstance(items, list):
            items = []
        data["items"] = [item for item in items if isinstance(item, dict) and item.get("path")]
        try:
            data["index"] = int(data.get("index"))
        except (TypeError, ValueError):
            data["index"] = -1
        return data

    def _queue_out(self, state: Optional[Dict[str, Any]] = None) -> QueueOut:
        state = state or self._queue_state()
        items = state.get("items") or []
        return QueueOut(
            device_id=str(state.get("device_id") or ""),
            index=int(state.get("index") or 0) if items else -1,
            total=len(items),
            items=[QueueItemOut(path=str(item.get("path") or ""),
                                name=str(item.get("name") or ""),
                                title=str(item.get("title") or ""),
                                size=int(item.get("size") or 0)) for item in items],
            auto_next=self._auto_next,
            source=str(state.get("source") or ""),
        )

    def _advance(self, direction: str) -> PlayOut:
        """切到下一首/上一首（next 也用于连播推进）。"""
        state = self._queue_state()
        items = state.get("items") or []
        if not items:
            return PlayOut(ok=False, message="队列为空")
        current = int(state.get("index") or 0)
        if direction == "next":
            if current + 1 >= len(items):
                return PlayOut(ok=True, message="队列已播到最后一首")
            target = current + 1
        elif direction == "prev":
            target = max(current - 1, 0)
        else:
            target = max(min(int(direction), len(items) - 1), 0)
        path = str(items[target].get("path") or "")
        result = self._play_file(path, device_id=state.get("device_id") or None)
        if result.ok:
            state["index"] = target
            self.save_data(_KEY_QUEUE, state)
            result.queue = self._queue_out(state)
        return result

    def _control(self, action: str, device_id: Optional[str] = None) -> PlayOut:
        """播放控制：play/pause/stop 交给音箱，next/prev/数字由插件自己推队列位置。"""
        if action in ("next", "prev") or action.isdigit():
            return self._advance(action)
        if action == "toggle":
            playing = self._playing_out(device=self._target_device(device_id)[0] or {})
            action = "pause" if playing.status == 1 else "play"
        if action not in ("play", "pause", "stop"):
            return PlayOut(ok=False, message=f"不支持的操作：{action}")
        device, err = self._target_device(device_id)
        if not device:
            return PlayOut(ok=False, message=err)
        client, err = self._client()
        if not client:
            return PlayOut(ok=False, message=err)
        try:
            result = client.play_operation(str(device.get("device_id") or ""), action)
        except MinaAuthError as exc:
            return PlayOut(ok=False, message=f"小米登录票据已失效：{exc}")
        except MinaError as exc:
            return PlayOut(ok=False, message=f"控制失败：{exc}")
        label = {"play": "继续播放", "pause": "已暂停", "stop": "已停止"}[action]
        if action == "stop":
            self.__dict__["_idle_ticks"] = 0
        return PlayOut(ok=bool(result.get("ok")),
                       message=label if result.get("ok") else str(result.get("error") or "音箱没有受理"),
                       api=str(result.get("api") or ""),
                       playing=self._playing_out(device, force=True) if result.get("ok") else None,
                       queue=self._queue_out())

    def _volume(self, volume: int, device_id: Optional[str] = None) -> ActionOut:
        device, err = self._target_device(device_id)
        if not device:
            return ActionOut(ok=False, message=err)
        client, err = self._client()
        if not client:
            return ActionOut(ok=False, message=err)
        value = self._clamp_volume(volume)
        try:
            result = client.set_volume(str(device.get("device_id") or ""), value)
        except (MinaAuthError, MinaError) as exc:
            return ActionOut(ok=False, message=f"设置音量失败：{exc}")
        return ActionOut(ok=bool(result.get("ok")),
                         message=f"音量 {value}" if result.get("ok") else str(result.get("error") or ""),
                         playing=self._playing_out(device, force=True) if result.get("ok") else None)

    def _tts(self, text: str, device_id: Optional[str] = None) -> ActionOut:
        device, err = self._target_device(device_id)
        if not device:
            return ActionOut(ok=False, message=err)
        client, err = self._client()
        if not client:
            return ActionOut(ok=False, message=err)
        try:
            result = client.text_to_speech(str(device.get("device_id") or ""), text)
        except (MinaAuthError, MinaError) as exc:
            return ActionOut(ok=False, message=f"播报失败：{exc}")
        ok = result.get("ok")
        if ok is None:
            return ActionOut(ok=True, message=str(result.get("error") or "音箱回执超时，可能已播报"),
                             playing=self._playing_out(device, force=True))
        return ActionOut(ok=bool(ok),
                         message="已播报" if ok else str(result.get("error") or "音箱拒绝播报"),
                         playing=self._playing_out(device, force=True) if ok else None)

    # ---------------- 状态读取 ----------------

    def _playing_out(self, device: Optional[Dict[str, Any]] = None,
                     force: bool = False, path: str = "", url: str = "") -> PlayingOut:
        """当前播放状态：云端读到的 + 插件记住的那条直链。"""
        device = device or self._target_device()[0] or {}
        device_id = str(device.get("device_id") or "")
        last = self.get_data(_KEY_LAST)
        last = last if isinstance(last, dict) else {}
        out = PlayingOut(
            device_id=device_id,
            device_name=str(device.get("name") or last.get("device_name") or ""),
            path=path or str(last.get("path") or ""),
            url=url or str(last.get("url") or ""),
            title=str(last.get("title") or ""),
            source=str(last.get("source") or ""),
            updated_at=self._now_text(),
        )
        if not device_id:
            out.status_text = "无设备"
            return out
        raw = self._device_status(device_id, force=force)
        if raw.get("error"):
            out.error = str(raw["error"])
            out.status_text = "读不到"
            return out
        out.status = int(raw.get("status") if raw.get("status") is not None else -1)
        out.status_text = _media.status_text(raw.get("status"))
        out.volume = raw.get("volume") if isinstance(raw.get("volume"), int) else None
        out.position = raw.get("position") if isinstance(raw.get("position"), (int, float)) else None
        out.duration = raw.get("duration") if isinstance(raw.get("duration"), (int, float)) else None
        return out

    def _device_status(self, device_id: str, force: bool = False) -> Dict[str, Any]:
        """player_get_play_status 结果，带 3 秒缓存。"""
        cache = self.__dict__.setdefault("_status_cache", {})
        entry = cache.get(device_id) or {}
        if not force and entry.get("data") and time.time() - float(entry.get("at") or 0) < _STATUS_CACHE_SECONDS:
            return dict(entry["data"])
        client, err = self._client()
        if not client:
            return {"error": err, "status": -1}
        try:
            status = client.get_status(device_id)
        except (MinaAuthError, MinaError) as exc:
            logger.debug(f"{_LOG} 读取播放状态失败: {exc}")
            return {"error": str(exc), "status": -1}
        data = {"status": status.get("status"), "volume": status.get("volume"),
                "position": status.get("position"), "duration": status.get("duration")}
        cache[device_id] = {"at": time.time(), "data": data}
        return dict(data)

    # ---------------- 连播 ----------------

    def _tick(self) -> None:
        """宿主周期任务：发现「播完了」就推队列下一首。"""
        if not self._enabled or not self._auto_next:
            return
        if self._stop_event().is_set():
            return
        state = self._queue_state()
        items = state.get("items") or []
        index = int(state.get("index") or 0)
        if not items or index < 0 or index >= len(items):
            return
        device_id = str(state.get("device_id") or "")
        if not device_id:
            fallback = self._target_device()[0] or {}
            device_id = str(fallback.get("device_id") or "")
        if not device_id:
            return
        last = self.get_data(_KEY_LAST)
        last = last if isinstance(last, dict) else {}
        status = self._device_status(device_id, force=True)
        if status.get("error"):
            return
        if int(status.get("status") or 0) != 0:
            self.__dict__["_idle_ticks"] = 0
            return
        elapsed = time.time() - float(last.get("started_at") or 0)
        if elapsed < _ADVANCE_GRACE_SECONDS:
            return
        self.__dict__["_idle_ticks"] = int(self.__dict__.get("_idle_ticks") or 0) + 1
        # 连续两次读到停止：起播初期的瞬时 0 状态不会连着出现两次
        if self.__dict__["_idle_ticks"] < 2:
            return
        self.__dict__["_idle_ticks"] = 0
        logger.info(f"{_LOG} 队列 {index + 1}/{len(items)} 已播完，推进下一首")
        if self._stop_event().is_set():
            return
        result = self._advance("next")
        if self._tts_announce and result.ok:
            advanced = self._queue_state()
            rows = advanced.get("items") or []
            position = int(advanced.get("index") or 0)
            if 0 <= position < len(rows):
                title = str(rows[position].get("title") or "").strip()
                if title:
                    self._tts(f"接下来播放 {title}", device_id=device_id)

    def _stop_event(self) -> threading.Event:
        return self.__dict__.setdefault("_stop_flag", threading.Event())

    def _play_lock(self) -> threading.Lock:
        return self.__dict__.setdefault("_op_lock", threading.Lock())

    # ---------------- 登录 ----------------

    def _login(self, payload: LoginIn) -> LoginOut:
        """开始或续跑一次登录会话（验证码回来时只带 code）。"""
        code = (payload.code or "").strip()
        manual = (payload.manual_token or "").strip()
        with self._login_lock():
            if manual:
                return self._login_manual(manual)
            auth: Optional[XiaoaiAuth] = self.__dict__.get("_auth")
            if code and auth and self.__dict__.get("_auth_state") in (LOGIN_NEED_CAPTCHA, LOGIN_NEED_VERIFY):
                state, token, message = (
                    auth.submit_verify_code(code)
                    if self.__dict__.get("_auth_state") == LOGIN_NEED_VERIFY
                    else auth.submit_captcha(code)
                )
            else:
                username = (payload.username or self._username or "").strip()
                password = payload.password or self._password or ""
                if not username or not password:
                    return LoginOut(state="failed", message="请填写小米账号和密码",
                                    logged_in=bool(self._token()))
                auth = XiaoaiAuth(username, password)
                self.__dict__["_auth"] = auth
                state, token, message = auth.login()
            return self._finish_login(state, token, message, auth)

    def _finish_login(self, state: str, token: Optional[Dict[str, Any]],
                      message: str, auth: Optional[XiaoaiAuth]) -> LoginOut:
        """把登录状态机的一次结果落到 plugindata 与前端可读的形状。"""
        self.__dict__["_auth_state"] = state
        captcha = ""
        if state == LOGIN_NEED_CAPTCHA:
            captcha = str((token or {}).get("captcha_image") or "")
            self.__dict__["_captcha_image"] = captcha
        if state == LOGIN_SUCCESS and token:
            if auth is not None:
                token.setdefault("username", auth.username)
            self.save_data(_KEY_TOKEN, token)
            self.__dict__["_auth"] = None
            self.__dict__["_captcha_image"] = ""
            self.__dict__["_devices_cache"] = {}
            devices, _ = self._devices_cached(force=True)
            logger.info(f"{_LOG} 登录成功，设备 {len(devices)} 台")
            return LoginOut(state=state, message=message or "登录成功",
                            logged_in=True, device_count=len(devices))
        if state == LOGIN_FAILED:
            self.__dict__["_auth"] = None
            self.__dict__["_captcha_image"] = ""
        return LoginOut(state=state, message=message,
                        captcha_image=captcha or self.__dict__.get("_captcha_image", ""),
                        logged_in=bool(self._token()),
                        device_count=len(self.__dict__.get("_devices_cache", {}).get("devices") or []))

    def _login_manual(self, raw: str) -> LoginOut:
        """粘贴 cookie 兜底：账密登不上（风控/无手机号）时仍可接进音箱。"""
        values: Dict[str, str] = {}
        for part in raw.replace("\n", ";").split(";"):
            if "=" in part:
                name, _, value = part.partition("=")
                values[name.strip().lower()] = value.strip().strip('"')
        user_id = values.get("userid") or ""
        service_token = values.get("servicetoken") or raw
        token = token_from_manual(user_id, service_token,
                                  device_id=values.get("deviceid", ""))
        if not token.get("user_id"):
            return LoginOut(state="failed", message="粘贴内容里没有 userId，请把整串 cookie 粘进来",
                            logged_in=bool(self._token()))
        client = self._build_client(token)
        try:
            alive = client.validate()
        except MinaError as exc:
            alive = False
            logger.info(f"{_LOG} 手工票据校验失败: {exc}")
        if not alive:
            return LoginOut(state="failed", message="手工票据校验没通过（device_list 非 0），请重新粘贴或改回账密登录",
                            logged_in=bool(self._token()))
        self.save_data(_KEY_TOKEN, token)
        self.__dict__["_devices_cache"] = {}
        devices, _ = self._devices_cached(force=True)
        return LoginOut(state=LOGIN_SUCCESS, message=f"手工票据已生效，设备 {len(devices)} 台",
                        logged_in=True, device_count=len(devices))

    def _logout(self) -> LoginOut:
        for key in (_KEY_TOKEN, _KEY_LAST):
            try:
                self.del_data(key)
            except Exception as exc:  # pragma: no cover
                logger.warning(f"{_LOG} 清除 {key} 失败: {exc}")
        self.__dict__["_auth"] = None
        self.__dict__["_auth_state"] = ""
        self.__dict__["_devices_cache"] = {}
        self.__dict__["_status_cache"] = {}
        self.save_data(_KEY_QUEUE, {"device_id": "", "index": -1, "items": []})
        return LoginOut(state="failed", message="已退出登录并清除本机票据", logged_in=False)

    # ---------------- 库浏览 ----------------

    def _browse(self, path_text: str) -> BrowseOut:
        roots = self._roots()
        if not roots:
            return BrowseOut(reason="no_dirs", roots=[])
        text = (path_text or "").strip()
        target = Path(text) if text else roots[0]
        result = _media.browse(target, roots, self._exts())
        base_url, _ = self._effective_base_url()
        secret = self._secret()
        files = [FileOut(
            name=item["name"], path=item["path"], title=item["title"],
            size=int(item.get("size") or 0), mtime=int(item.get("mtime") or 0),
            stream_url=(_media.stream_url(base_url, self._plugin_id(), secret,
                                          Path(item["path"]), self._stream_ttl)
                        if base_url else ""),
        ) for item in result.get("files") or []]
        parents = str(Path(result.get("cwd") or "").parent)
        parent = "" if parents in ("/", "", ".") else parents
        return BrowseOut(
            cwd=str(result.get("cwd") or ""),
            parent=parent,
            roots=[str(root) for root in roots],
            dirs=[DirOut(name=item["name"], path=item["path"]) for item in result.get("dirs") or []],
            files=files,
            reason=str(result.get("reason") or ""),
            truncated=result.get("reason") == "truncated",
        )

    def _search(self, keyword: str) -> SearchOut:
        roots = self._roots()
        if not roots:
            return SearchOut(keyword=keyword, reason="no_dirs")
        hits = _media.search(keyword, roots, self._exts())
        base_url, _ = self._effective_base_url()
        secret = self._secret()
        rows = [FileOut(
            name=item["name"], path=item["path"], title=item["title"],
            size=int(item.get("size") or 0), mtime=int(item.get("mtime") or 0),
            stream_url=(_media.stream_url(base_url, self._plugin_id(), secret,
                                          Path(item["path"]), self._stream_ttl)
                        if base_url else ""),
        ) for item in hits]
        return SearchOut(keyword=keyword, items=rows,
                         reason="" if rows else "no_hits")

    @staticmethod
    def _reject_text(reason: str) -> str:
        """把 resolve_playable 的内部判据翻成给人看的一句话。"""
        return {
            "empty_path": "没有给出文件路径",
            "not_absolute": "只接受库内绝对路径",
            "unresolvable": "路径无法解析（软链或权限问题）",
            "not_found": "文件不存在（可能已被移动或删除）",
            "not_audio": "这个扩展名不在可播放列表里，可在插件配置中放开",
            "no_roots": "还没有配置「音乐库路径」",
            "out_of_library": "这个文件不在配置的音乐库目录内",
        }.get(reason, f"不能播放该路径（{reason}）")

    def _plugin_id(self) -> str:
        return self.__class__.__name__

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------------- 状态汇总 ----------------

    def _status(self, force_devices: bool = False) -> StatusOut:
        """侧栏页首屏：票据、设备、当前播放、队列、配置回显与待办提示。"""
        token = self._token() or {}
        devices, fetched_at = self._devices_cached(force=force_devices)
        base_url, base_source = self._effective_base_url()
        roots = self._roots()
        missing = [str(root) for root in roots if not root.is_dir()]
        default_device = self._device_by_id(self._default_device_id) if self._default_device_id else None
        if default_device is None and devices:
            default_device = self._target_device()[0]
        client, client_err = self._client()
        needs: List[str] = []
        notices: List[str] = []
        if not token:
            needs.append("登录：填写小米账号密码，或在高级里粘贴 cookie 票据")
        if not roots:
            needs.append("配置：填写「音乐库路径」")
        elif missing:
            notices.append("这些库目录当前读不到：" + "、".join(missing[:3]))
        if not base_url:
            needs.append("配置：填写音箱可访问的「外部访问地址」")
        if client_err and token:
            notices.append(client_err)
        queue_state = self._queue_state()
        # 拿不到客户端 = 票据过期或已被小米判失效，两种都要重新登录
        token_expired = bool(token) and (client is None or client.token_expired())
        return StatusOut(
            enabled=self._enabled,
            version=self.plugin_version,
            logged_in=bool(token),
            token_saved_at=str(token.get("saved_at") or ""),
            manual_token=bool(token.get("manual")),
            token_expired=token_expired,
            needs=needs,
            notices=notices,
            devices=self._devices_out(devices),
            default_device=(self._devices_out([default_device])[0] if default_device else None),
            playing=self._playing_out(default_device),
            queue=self._queue_out(queue_state),
            config=ConfigOut(
                enabled=self._enabled,
                show_sidebar_nav=self._show_sidebar_nav,
                username=self._username,
                password_set=bool(self._password),
                default_device_id=self._default_device_id,
                music_dirs=self._music_dirs,
                dir_count=len(roots),
                missing_dirs=missing,
                base_url=self._base_url,
                base_url_effective=base_url,
                base_url_source=base_source,
                play_mode=self._play_mode,
                audio_exts=self._audio_exts,
                audio_id=self._audio_id or _mina.DEFAULT_MUSIC_AUDIO_ID,
                stream_ttl=self._stream_ttl,
                default_volume=self._default_volume,
                auto_next=self._auto_next,
                keep_light=self._keep_light,
                tts_announce=self._tts_announce,
            ),
            extra={
                "devices_fetched_at": fetched_at,
                "exts": list(self._exts()),
                "music_api_models": list(_mina.MUSIC_API_MODELS),
            },
        )

    # ---------------- 命令 ----------------

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """远程命令：状态 / 播放 / 控制 / 音量 / 播报。"""
        return [
            {
                "cmd": "/xiaoai_status",
                "event": EventType.PluginAction,
                "desc": "小爱音箱：查看登录、设备与播放状态",
                "category": "小爱音箱",
                "data": {"action": "xiaoai_status"},
            },
            {
                "cmd": "/xiaoai_play",
                "event": EventType.PluginAction,
                "desc": "小爱音箱：播放库内曲目（参数为曲名或库内绝对路径）",
                "category": "小爱音箱",
                "data": {"action": "xiaoai_play"},
            },
            {
                "cmd": "/xiaoai_control",
                "event": EventType.PluginAction,
                "desc": "小爱音箱：play|pause|stop|next|prev|<队列序号>",
                "category": "小爱音箱",
                "data": {"action": "xiaoai_control"},
            },
            {
                "cmd": "/xiaoai_volume",
                "event": EventType.PluginAction,
                "desc": "小爱音箱：设置音量 0-100",
                "category": "小爱音箱",
                "data": {"action": "xiaoai_volume"},
            },
            {
                "cmd": "/xiaoai_tts",
                "event": EventType.PluginAction,
                "desc": "小爱音箱：播报一段文字",
                "category": "小爱音箱",
                "data": {"action": "xiaoai_tts"},
            },
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None) -> None:
        """只处理本插件的动作；参数文本在 event_data.arg_str（宿主 command.py 注入）。"""
        event_data = (event.event_data if event else None) or {}
        action = str(event_data.get("action") or "")
        if not action.startswith("xiaoai_"):
            return
        arg = str(event_data.get("arg_str") or "").strip()
        channel = event_data.get("channel")
        userid = event_data.get("user")
        handlers = {
            "xiaoai_status": lambda: self._cmd_status(),
            "xiaoai_play": lambda: self._cmd_play(arg),
            "xiaoai_control": lambda: self._cmd_control(arg),
            "xiaoai_volume": lambda: self._cmd_volume(arg),
            "xiaoai_tts": lambda: self._cmd_tts(arg),
        }
        handler = handlers.get(action)
        if not handler:
            return
        try:
            title, text = handler()
        except Exception as err:  # pragma: no cover - 不让单次命令把事件线程带崩
            logger.error(f"{_LOG} 命令 {action} 执行异常: {err}")
            title, text = "小爱音箱命令执行失败", str(err)
        self.post_message(title=title, text=text, channel=channel, userid=userid)

    def _cmd_status(self) -> Tuple[str, str]:
        status = self._status()
        lines = [
            f"登录：{'已登录' if status.logged_in else '未登录'}"
            f"（票据 {status.token_saved_at or '无'}）",
            f"账号：{status.config.username or '未填写'}｜设备 {len(status.devices)} 台",
        ]
        for row in status.devices:
            lines.append(
                f"· {row.name or row.device_id}｜{row.hardware or row.model}"
                f"｜{'在线' if row.presence != 'offline' else '离线'}"
                f"{'（默认）' if row.is_default else ''}"
            )
        lines.append(
            f"当前：{status.playing.status_text}"
            f"｜{status.playing.title or '未记录曲目'}"
            f"｜音量 {status.playing.volume if status.playing.volume is not None else '未知'}"
        )
        if status.queue.total:
            lines.append(f"队列：{status.queue.index + 1}/{status.queue.total}"
                         f"｜连播 {'开' if status.queue.auto_next else '关'}")
        if status.needs:
            lines.append("待办：" + "；".join(status.needs))
        return "小爱音箱状态", "\n".join(lines)

    def _cmd_play(self, arg: str) -> Tuple[str, str]:
        if not arg:
            return "小爱音箱", "用法：/xiaoai_play <曲名关键词 或 库内绝对路径>"
        result = (self._play_file(arg) if arg.startswith("/") else self._play_keyword(arg))
        return "小爱音箱播放", result.message + (f"\n接口：{result.api}" if result.api else "")

    def _cmd_control(self, arg: str) -> Tuple[str, str]:
        action = (arg or "pause").split()[0].lower()
        result = self._control(action)
        return "小爱音箱控制", result.message

    def _cmd_volume(self, arg: str) -> Tuple[str, str]:
        if not arg:
            current = self._playing_out(force=True).volume
            return "小爱音箱音量", f"用法：/xiaoai_volume <0-100>（当前 {current}）"
        try:
            value = int(arg.split()[0])
        except ValueError:
            return "小爱音箱音量", "音量要是 0-100 的整数"
        result = self._volume(value)
        return "小爱音箱音量", result.message

    def _cmd_tts(self, arg: str) -> Tuple[str, str]:
        if not arg:
            return "小爱音箱播报", "用法：/xiaoai_tts <文本>"
        result = self._tts(arg)
        return "小爱音箱播报", result.message

    # ---------------- API ----------------

    def get_api(self) -> List[Dict[str, Any]]:
        """侧栏页与外部调用接口；/stream 例外（音箱匿名拉流）。"""
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": StatusOut,
                "summary": "登录、设备、当前播放、队列与配置汇总",
            },
            {
                "path": "/devices",
                "endpoint": self.api_devices,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": DevicesOut,
                "summary": "重新拉取音箱设备列表",
            },
            {
                "path": "/device/select",
                "endpoint": self.api_device_select,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": ActionOut,
                "summary": "设置默认音箱",
            },
            {
                "path": "/login",
                "endpoint": self.api_login,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": LoginOut,
                "summary": "登录小米账号（含验证码续跑与手工票据）",
            },
            {
                "path": "/logout",
                "endpoint": self.api_logout,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": LoginOut,
                "summary": "退出登录并清除本机票据",
            },
            {
                "path": "/play",
                "endpoint": self.api_play,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": PlayOut,
                "summary": "播放库内文件、外部直链或按曲名搜索",
            },
            {
                "path": "/queue",
                "endpoint": self.api_queue,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": PlayOut,
                "summary": "设定播放队列并从指定位置开始播",
            },
            {
                "path": "/control",
                "endpoint": self.api_control,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": PlayOut,
                "summary": "播放控制：play/pause/stop/next/prev/序号",
            },
            {
                "path": "/volume",
                "endpoint": self.api_volume,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": ActionOut,
                "summary": "设置音量",
            },
            {
                "path": "/tts",
                "endpoint": self.api_tts,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": ActionOut,
                "summary": "播报文本",
            },
            {
                "path": "/browse",
                "endpoint": self.api_browse,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": BrowseOut,
                "summary": "浏览库内目录",
            },
            {
                "path": "/search",
                "endpoint": self.api_search,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": SearchOut,
                "summary": "按文件名搜索库内音频",
            },
            {
                "path": "/config",
                "endpoint": self.api_config,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": StatusOut,
                "summary": "保存插件配置（缺省字段保留原值）",
            },
            {
                # 音箱自己来拉流，带不上 MP 的登录态：靠签名 + 有效期 + 库内路径判据收权限
                "path": "/stream",
                "endpoint": self.api_stream,
                "methods": ["GET"],
                "allow_anonymous": True,
                "response_model": None,
                "response_class": StreamingResponse,
                "summary": "音频直流（签名限时，供音箱匿名访问）",
                "responses": {
                    200: {"content": {"audio/mpeg": {}, "audio/flac": {}}, "description": "音频文件"},
                    206: {"content": {"audio/mpeg": {}}, "description": "Range 分片"},
                    403: {"content": {"application/json": {}}, "description": "签名无效或路径越库"},
                },
            },
        ]

    def api_status(self) -> StatusOut:
        """GET /status —— 侧栏页轮询用，设备走缓存。"""
        return self._status()

    def api_devices(self) -> DevicesOut:
        """GET /devices —— 强制刷新设备列表。"""
        devices, fetched_at = self._devices_cached(force=True)
        _, client_err = self._client()
        return DevicesOut(ok=bool(devices) or not client_err,
                          message=client_err,
                          devices=self._devices_out(devices),
                          fetched_at=fetched_at)

    def api_device_select(self, payload: DeviceSelectIn) -> ActionOut:
        """POST /device/select —— 把某台设备设为默认（写回配置）。"""
        device_id = (payload.device_id or "").strip()
        if device_id:
            if not self._device_by_id(device_id):
                return ActionOut(ok=False, message="该设备不在当前设备列表里，请先刷新")
        self._default_device_id = device_id
        self._save_config()
        return ActionOut(ok=True, message="已设置默认音箱" if device_id else "已取消默认音箱",
                         playing=self._playing_out(self._device_by_id(device_id) if device_id else None,
                                                   force=True))

    def api_login(self, payload: LoginIn) -> LoginOut:
        """POST /login —— 开始登录，或把验证码交回进行中的那次会话。"""
        return self._login(payload)

    def api_logout(self) -> LoginOut:
        """POST /logout —— 清除本机票据。"""
        return self._logout()

    def api_play(self, payload: PlayIn) -> PlayOut:
        """POST /play —— {path} | {url} | {keyword}，可带 queue 路径列表。"""
        if payload.path:
            return self._play_file(payload.path, device_id=payload.device_id,
                                   queue_paths=payload.queue)
        if payload.url:
            return self._play_url(payload.url, device_id=payload.device_id)
        if payload.keyword:
            return self._play_keyword(payload.keyword, device_id=payload.device_id)
        return PlayOut(ok=False, message="请给出 path、url 或 keyword 之一")

    def api_queue(self, payload: QueueIn) -> PlayOut:
        """POST /queue —— 设定队列并从 index 开始播。"""
        paths = [str(item) for item in (payload.paths or []) if str(item).strip()]
        if not paths:
            return PlayOut(ok=False, message="队列为空")
        device, err = self._target_device(payload.device_id)
        if not device:
            return PlayOut(ok=False, message=err)
        index = min(max(int(payload.index or 0), 0), len(paths) - 1)
        first = paths[index]
        result = self._play_file(first, device_id=str(device.get("device_id") or ""),
                                queue_paths=paths)
        if result.ok:
            state = self._queue_state()
            state["index"] = index
            self.save_data(_KEY_QUEUE, state)
            result.queue = self._queue_out(state)
        return result

    def api_control(self, payload: ControlIn) -> PlayOut:
        """POST /control —— {action: play|pause|stop|toggle|next|prev|<序号>}。"""
        return self._control((payload.action or "pause").strip().lower(), payload.device_id)

    def api_volume(self, payload: VolumeIn) -> ActionOut:
        """POST /volume —— {volume: 0-100}。"""
        return self._volume(payload.volume, payload.device_id)

    def api_tts(self, payload: TtsIn) -> ActionOut:
        """POST /tts —— {text}。"""
        text = (payload.text or "").strip()
        if not text:
            return ActionOut(ok=False, message="播报内容为空")
        return self._tts(text[:200], payload.device_id)

    def api_browse(self, path: str = "") -> BrowseOut:
        """GET /browse?path=<目录> —— 只列配置库内的目录与音频。"""
        return self._browse(path)

    def api_search(self, keyword: str = "") -> SearchOut:
        """GET /search?keyword=<曲名> —— 库内文件名搜索，深度上限 6 层。"""
        return self._search((keyword or "").strip())

    def api_config(self, payload: ConfigIn) -> StatusOut:
        """POST /config —— 只改传上来的字段，密码留空表示保留原值。"""
        data = payload.model_dump(exclude_none=True)
        password = data.pop("password", None)
        with self._play_lock():
            if "music_dirs" in data:
                self._music_dirs = str(data.pop("music_dirs") or "")
            if "base_url" in data:
                self._base_url = _media.normalize_base_url(str(data.pop("base_url") or ""))
            if "play_mode" in data:
                self._play_mode = self._clamp_play_mode(data.pop("play_mode"))
            if "audio_exts" in data:
                self._audio_exts = str(data.pop("audio_exts") or "")
            if "audio_id" in data:
                self._audio_id = str(data.pop("audio_id") or "").strip()
            if "stream_ttl" in data:
                self._stream_ttl = self._clamp_stream_ttl(data.pop("stream_ttl"))
            if "default_volume" in data:
                self._default_volume = self._clamp_volume(data.pop("default_volume"))
            if "username" in data:
                username = str(data.pop("username") or "").strip()
                if username != self._username:
                    self._username = username
                    self.__dict__["_auth"] = None
                    self.__dict__["_auth_state"] = ""
            if password:
                self._password = str(password)
            for key, attr, as_bool in (("auto_next", "_auto_next", True),
                                       ("keep_light", "_keep_light", True),
                                       ("tts_announce", "_tts_announce", True),
                                       ("default_device_id", "_default_device_id", False)):
                if key in data:
                    value = data.pop(key)
                    setattr(self, attr, bool(value) if as_bool else str(value or "").strip())
            self._save_config()
        return self._status(force_devices=True)

    def api_stream(self, request: Request):
        """GET /stream?path=&exp=&sig= —— 校验签名后按 Range 回文件。"""
        path, expires, signature = _media.parse_query(dict(request.query_params))
        if not _media.verify_sign(self._secret(), path, expires, signature):
            return JSONResponse(status_code=403, content={"detail": "链接无效或已过期"})
        real, reason = _media.resolve_playable(path, self._roots(), self._exts())
        if not real:
            logger.warning(f"{_LOG} /stream 拒绝 {path}: {reason}")
            return JSONResponse(status_code=403, content={"detail": self._reject_text(reason)})
        try:
            size = real.stat().st_size
        except OSError as exc:
            return JSONResponse(status_code=404, content={"detail": f"读不到文件：{exc}"})
        start, end = _media.parse_range(request.headers.get("Range"), size)
        partial = start > 0 or end < size - 1
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Disposition": f'inline; filename="{real.name}"',
            "Cache-Control": "no-cache",
        }
        if partial:
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        return StreamingResponse(
            _media.iter_file(real, start, end),
            status_code=206 if partial else 200,
            media_type=_media.content_type(real),
            headers=headers,
        )

    # ---------------- 页面 / 渲染 ----------------

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """Vue 联邦渲染，产物目录 dist/assets。"""
        return "vue", "dist/assets"

    def get_sidebar_nav(self) -> List[Dict[str, Any]]:
        """主界面左侧导航入口（全页组件 ./AppPage）。"""
        if not self.get_state() or not self._show_sidebar_nav:
            return []
        return [
            {
                "nav_key": "main",
                "title": "小爱音箱",
                "icon": "mdi-speaker",
                "section": "organize",
                "permission": "manage",
                "order": self.plugin_order,
            }
        ]

    def get_page(self) -> List[Dict[str, Any]]:
        """Vue 模式下插件详情页由远程 ./Page 渲染。"""
        return []

    def get_form(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """原生配置表单（插件管理里的配置页）。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "show_sidebar_nav", "label": "显示侧栏入口"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "auto_next", "label": "队列自动连播"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "keep_light", "label": "播放时常亮"}}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "username", "label": "小米账号（手机号/邮箱/ID）"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "password", "label": "小米账号密码", "type": "password"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "default_device_id", "label": "默认音箱设备ID",
                                "placeholder": "留空则在侧栏页选择设备"}}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VTextarea", "props": {
                                "model": "music_dirs", "rows": 3,
                                "label": "音乐库路径（一行一个目录，只有这些目录可被播放）",
                                "placeholder": "/media/music\n/nas/music"}}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "base_url", "label": "外部访问地址（音箱能访问到的 MP 地址）",
                                "placeholder": "http://192.168.1.10:3000，留空用主程序设置"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "play_mode", "label": "播放接口",
                                "items": [
                                    {"title": "自动（按型号白名单）", "value": "auto"},
                                    {"title": "player_play_url", "value": "url"},
                                    {"title": "player_play_music", "value": "music"},
                                ]}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "default_volume", "label": "播放前设置音量（0=不改）",
                                "type": "number", "min": 0, "max": 100}}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "audio_exts", "label": "可播放扩展名（留空用默认）",
                                "placeholder": "mp3 m4a aac flac wav ogg opus wma"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "stream_ttl", "label": "直链有效期（秒，300-259200）",
                                "type": "number"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "audio_id", "label": "play_music audioID（留空用默认）"}}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VSwitch", "props": {"model": "tts_announce",
                                                               "label": "连播时先播报下一曲"}}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VAlert", "props": {
                                "type": "info", "variant": "tonal", "text": _FORM_NOTES}}
                        ]},
                    ]},
                ],
            }
        ], dict(_DEFAULT_CONFIG)

    # ---------------- 服务 ----------------

    def get_service(self) -> List[Dict[str, Any]]:
        """连播轮询：交宿主调度器托管，插件不自建 scheduler。"""
        if not self._enabled or not self._auto_next:
            return []
        return [
            {
                "id": "XiaoaiSpeaker.AutoNext",
                "name": "小爱音箱队列连播",
                "trigger": IntervalTrigger(seconds=_ADVANCE_TICK_SECONDS),
                "func": self._tick,
                "kwargs": {},
            }
        ]

    def stop_service(self) -> None:
        """停用：投递停止信号并取消本插件登记的一次性任务。"""
        self._stop_event().set()
        try:
            remove = getattr(scheduler_sdk, "remove_plugin_once_job", None) if scheduler_sdk else None
            if remove:
                remove(self._plugin_id(), "tick")
        except Exception as err:  # pragma: no cover
            logger.debug(f"{_LOG} 取消一次性任务异常: {err}")
