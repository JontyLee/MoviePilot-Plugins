"""
xiaoaispeaker — MoviePilot v3 插件（插件名：小爱音箱）

把 MoviePilot 的音乐库推到小爱音箱上放：侧栏页面（播放台 / 音乐库 / 歌单与收藏 / 定时任务 /
语音控制 / 账号与设备 / 配置）、曲库索引与文本匹配、小爱对话监听驱动的口令引擎、设备分组同步、
定时任务、歌词，以及一组远程命令。播放走「插件签发一条带签名与有效期的 HTTP 直链，音箱自己经
云端拉流」这条路，不依赖 Emby/Jellyfin，也不需要额外部署 Music Tag Web。

当前版本的边界（有意为之，不是遗漏）：
- 只支持小米云端 Mina 接口（登录 → serviceToken → /remote/ubus），不做 MiIO 本地 RPC：
  局域网直连要额外拿 token 与 miio 密钥、按型号试 siid/piid，收益不抵复杂度。
- 歌词只在本页面上显示（同目录 .lrc/.txt 与音频内嵌标签）：触屏音箱的本机歌词要那块的
  Music API 与设备侧 UI，插件既拿不到也没有渲染处。
- 不做 DLNA/UPnP 接收端：SSDP 组播与监听端口要在宿主容器里另开，第三方投送也就无从认证；
  程序化投送等价物是 /play + /stream 那对签名直链接口。
- 定时任务按星期/几号触发，不做法定节假日调休：上游把 holiday-cn 数据集编进插件产物，宿主
  容器里的插件既没有那份数据也没有更新渠道。
- 队列连播靠轮询 player_get_play_status 判「播完」，不是音箱侧的真实播放列表。
- /stream 必须匿名可达（音箱不会带上 MP 的登录态），因此用「路径 + 过期时间」的 HMAC 签名
  收敛成「只能取配置目录内、在有效期内的音频文件」，签名不匹配或路径越库一律 403。
- AI 兜底与自由对话不自备第二份密钥与端点：借主程序「设定 → AI 大模型」配好的那个助手，
  宿主没配就在配置页写明缺哪一项并退回到只用口令表，插件这里不留 key。
- 「启用插件」「显示侧栏入口」只能在插件自己的配置页改：宿主投影侧栏入口前先调 get_state()，
  而插件管理页那个开关写的是宿主自己的实例记录，插不进这一项。
- 账号只回显掩码（13****34 / ab***@example.com）：/status 带着整份配置每 5 秒被轮一次，
  明文账号名等于把手机号长期挂在任何能看到侧栏的人眼前。写回时带 * 的值一律当作没改，
  免得页面把掩码原样存回去，把真账号覆盖成打不上的一串。

出处与许可：小米云端的登录三步流程、clientSign 算法、ubus 请求体、需要走 play_music 的
型号白名单，移植自 Songloft 的「智能音箱」插件 songloft-plugin-miot
（https://github.com/songloft-org/songloft-plugin-miot ，Apache-2.0 © 2026 hanxi），
并与 Yonsm/MiService（MIT）交叉核对；详见 mina.py 顶部说明与同目录
LICENSE.upstream-songloft-miot.txt。
"""

from __future__ import annotations

import json
import os
import random
import re
import secrets
import tempfile
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

from . import library as _library
from . import lyrics as _lyrics
from . import media as _media
from . import mina as _mina
from . import intents as _intents
from . import speech as _speech
from . import voice as _voice
from .mina import (
    LOGIN_FAILED,
    LOGIN_NEED_CAPTCHA,
    LOGIN_NEED_VERIFY,
    LOGIN_QR_EXPIRED,
    LOGIN_QR_WAITING,
    LOGIN_SUCCESS,
    MinaAuthError,
    MinaError,
    XiaoaiAuth,
    XiaoaiClient,
    XiaoaiQrLogin,
    exchange_pass_token,
)
from .schemas import (
    ActionOut,
    BrowseOut,
    CommandKindOut,
    CommandOut,
    CommandParamOut,
    CommandWordOut,
    CommandsIn,
    CommandsOut,
    ConfigIn,
    ConfigOut,
    ControlIn,
    DeviceOut,
    DeviceSelectIn,
    DevicesOut,
    DirOut,
    FavoriteIn,
    FavoriteOut,
    FavoritesOut,
    FileOut,
    GroupIn,
    GroupOut,
    GroupsOut,
    LibraryOut,
    LoginIn,
    LoginOut,
    LyricLineOut,
    LyricOut,
    ModeIn,
    PlayIn,
    PlaylistOut,
    PlaylistsOut,
    PlayOut,
    PlayingOut,
    QrIn,
    QrOut,
    QueueIn,
    QueueItemOut,
    QueueOut,
    SearchOut,
    SongsOut,
    StatusOut,
    TaskIn,
    TaskLogsOut,
    TaskRunIn,
    TasksOut,
    TimerIn,
    TimerOut,
    TtsIn,
    VoiceIn,
    VoiceOut,
    VoiceRecordOut,
    VolumeIn,
)

try:  # 主程序较新版本才有：一次性任务交给宿主调度器，插件不再自建 scheduler
    from app.sdk import scheduler as scheduler_sdk
except Exception:  # pragma: no cover - 老版本主程序没有该模块
    scheduler_sdk = None

# plugindata 键：票据、设备缓存、队列、签名密钥、最近一次推流、收藏、睡眠定时、语音记忆
_KEY_TOKEN = "token"
# 本机固定的小米 deviceId：跟着这台实例走，换账号也不换（见 _device_id）
_KEY_DEVICE = "device_id"
_KEY_DEVICES = "devices"
_KEY_QUEUE = "queue"
_KEY_SECRET = "secret"
_KEY_LAST = "last"
_KEY_FAVORITES = "favorites"
_KEY_TIMER = "sleep_timer"
_KEY_MEMORY = "voice_memory"
_KEY_COMMANDS = "voice_commands"
_KEY_WORDS = "voice_words"
_KEY_GROUPS = "device_groups"
_KEY_TASKS = "scheduled_tasks"
_KEY_TASK_LOGS = "schedule_logs"

# 音箱状态是云端读来的，前端每几秒刷一次；给一个很短的缓存避免把小米接口打爆
_STATUS_CACHE_SECONDS = 3.0
# 设备列表基本不变，缓存久一些；侧栏页手动刷新时强制重取
_DEVICES_CACHE_SECONDS = 300.0
# 静默续期失败后的冷却，避免每个请求都去撞小米风控
_REFRESH_COOLDOWN_SECONDS = 300.0
# 连播判定：推流后这么久仍读到「停止」才认为真的播完了
_ADVANCE_GRACE_SECONDS = 25.0
_ADVANCE_TICK_SECONDS = 20
# 顺序/单曲播放模式下"没有下一首了"的固定回执：调用方按它区分"切歌成功"和"到此为止"
_QUEUE_END_MESSAGE = "队列已播到最后一首"
# 播放方式的中文名：语音回执与 /mode 接口共用
_MODE_TEXT = {
    "order": "顺序播放",
    "loop": "列表循环",
    "random": "随机播放",
    "single": "单曲循环",
    "single_play": "只播这一首",
}
# 定时任务能做的动作（与上游 schedule/executor.ts 同一套），以及各自需要的参数
_TASK_ACTIONS = {
    "play_playlist": "播放歌单",
    "play_playlist_from": "歌单从某首播",
    "stop": "停止播放",
    "set_volume": "设置音量",
    "set_play_mode": "设置播放方式",
    "enable_monitor": "开启语音监听",
    "disable_monitor": "关闭语音监听",
}
# 执行日志只在内存与 plugindata 里各留这么多条：够回看一周，又不至于把数据文件撑大
_TASK_LOG_KEEP = 200
# 星期几的中文名（0=周日，与 JS getDay() 一致，任务里勾的也是这个编号）
_TASK_WEEK_TEXT = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"]
# 任务条数上限：一次巡检要把每条都判一遍，也为了 plugindata 文件不至于无限膨胀
_TASK_MAX = 50
# 索引重建的最短间隔：语音未命中会触发一次重建，不能让它变成扫盘循环
_LIBRARY_MIN_REFRESH_SECONDS = 60.0
# AI 对话窗口：说过话之后这么久内仍按满速轮询，追问才接得上
_CHAT_WINDOW_SECONDS = 120.0
# 带给模型的历史条数（一问一答算两条）
_CHAT_TURNS = 8

# AI 兜底没有自己的端点与密钥：读宿主「设定 → AI 大模型」，见 intents.Assistant

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
    "force_mp3": False,
    "audio_id": "",
    "stream_ttl": 3600,
    "default_volume": 0,
    "auto_next": True,
    "keep_light": False,
    "tts_announce": False,
    "voice_enabled": False,
    "voice_devices": "",
    "voice_poll": 2,
    "voice_interrupt": True,
    "voice_hint_tts": False,
    "voice_hint_text": "正在搜索，请稍候",
    "voice_reply": True,
    "voice_memory": True,
    "ai_enabled": False,
    "ai_chat": False,
    "ai_timeout": 6,
    "library_limit": _library.DEFAULT_SONG_LIMIT,
    "library_refresh": 60,
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
    "6. 库里的 m4a/flac 推过去常常是「云端受理、音箱不出声」——小爱只稳当解码 mp3。"
    "打开「非 mp3 现场转成 mp3」后，/stream 边放边用本机 ffmpeg 转；这一条不再支持"
    "拖动进度，也要求机器上有 ffmpeg。\n"
    "7. 登录票据约 12 小时到期，插件会用 passToken 静默续期；续期失败才需要重新登录。"
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
    plugin_version = "0.6.2"
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
    _force_mp3: bool = False
    _audio_id: str = ""
    _stream_ttl: int = 3600
    _default_volume: int = 0
    _auto_next: bool = True
    _keep_light: bool = False
    _tts_announce: bool = False
    _voice_enabled: bool = False
    _voice_devices: str = ""
    _voice_poll: int = 2
    _voice_interrupt: bool = True
    _voice_hint_tts: bool = False
    _voice_hint_text: str = "正在搜索，请稍候"
    _voice_reply: bool = True
    _voice_memory: bool = True
    _ai_enabled: bool = False
    _ai_chat: bool = False
    _ai_timeout: int = 6
    _library_limit: int = _library.DEFAULT_SONG_LIMIT
    _library_refresh: int = 60

    def init_plugin(self, config: Dict[str, Any] = None) -> None:
        """读取配置并准备运行态（登录会话、缓存、锁）。可重复调用。"""
        # 锁与运行态放实例 __dict__ 并用 setdefault：保存配置会重跑 init_plugin，
        # 直接把进行中的登录会话或连播计数重置掉，等于把人正在填的验证码丢掉。
        self.__dict__.setdefault("_op_lock", threading.Lock())
        self.__dict__.setdefault("_login_lock", threading.Lock())
        self.__dict__.setdefault("_auth", None)
        self.__dict__.setdefault("_auth_state", "")
        self.__dict__.setdefault("_captcha_image", "")
        self.__dict__.setdefault("_qr_login", None)
        self.__dict__.setdefault("_status_cache", {})
        self.__dict__.setdefault("_devices_cache", {})
        self.__dict__.setdefault("_refresh_attempt_at", 0.0)
        self.__dict__.setdefault("_idle_ticks", 0)
        self.__dict__.setdefault("_stop_flag", threading.Event())
        self.__dict__.setdefault("_library_index", _library.LibraryIndex())
        self.__dict__.setdefault("_speech_listener", None)
        # 每次重跑都丢掉助手缓存：宿主设定里换了 provider/模型或补上 key，状态面板
        # 得立刻反映新的，不能继续报上一次读到的那一家
        self.__dict__.pop("_assistant_cache", None)
        self.__dict__.setdefault("_library_error", "")

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
            self._force_mp3 = bool(config.get("force_mp3"))
            self._audio_id = str(config.get("audio_id") or "").strip()
            self._stream_ttl = self._clamp_stream_ttl(config.get("stream_ttl"))
            self._default_volume = self._clamp_volume(config.get("default_volume"))
            self._auto_next = bool(config.get("auto_next", True))
            self._keep_light = bool(config.get("keep_light"))
            self._tts_announce = bool(config.get("tts_announce"))
            self._voice_enabled = bool(config.get("voice_enabled"))
            self._voice_devices = str(config.get("voice_devices") or "").strip()
            self._voice_poll = self._clamp_poll(config.get("voice_poll"))
            self._voice_interrupt = bool(config.get("voice_interrupt", True))
            self._voice_hint_tts = bool(config.get("voice_hint_tts"))
            self._voice_hint_text = str(config.get("voice_hint_text") or "正在搜索，请稍候").strip()
            self._voice_reply = bool(config.get("voice_reply", True))
            self._voice_memory = bool(config.get("voice_memory", True))
            self._ai_enabled = bool(config.get("ai_enabled"))
            self._ai_chat = bool(config.get("ai_chat"))
            self._ai_timeout = self._clamp_ai_timeout(config.get("ai_timeout"))
            self._library_limit = _library.clamp_song_limit(config.get("library_limit"))
            self._library_refresh = self._clamp_library_refresh(config.get("library_refresh"))
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

    @staticmethod
    def _clamp_poll(raw: Any) -> int:
        """语音轮询间隔（秒）：1~30。低于 1 秒是在刷小米接口，高于 30 秒用户已经走开了。"""
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            return int(_DEFAULT_CONFIG["voice_poll"])
        return min(max(value, 1), 30)

    @staticmethod
    def _clamp_ai_timeout(raw: Any) -> int:
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            return 6
        return min(max(value, 2), 30)

    @staticmethod
    def _clamp_library_refresh(raw: Any) -> int:
        """索引自动重建周期（分钟）：0 = 不自动重建，只靠手动和未命中触发。"""
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            return int(_DEFAULT_CONFIG["library_refresh"])
        return min(max(value, 0), 1440)

    def get_state(self) -> bool:
        """宿主眼里的「插件已启用」：侧栏入口、周期任务、远程命令都按这一项准入。

        宿主在投影 `get_sidebar_nav` / `get_service` 之前先调 `get_state()`，而插件管理页
        那个开关写的是宿主自己的实例记录，碰不到这里的 `enabled` —— 所以这一项必须在插件
        自己的配置页给出开关，否则「装好了但永远不会出现侧栏」是死结。
        """
        return bool(self._enabled)

    # ---------------- 配置与库范围 ----------------

    def _roots(self) -> List[Path]:
        return _media.parse_dirs(self._music_dirs)

    def _exts(self) -> Tuple[str, ...]:
        return _media.parse_exts(self._audio_exts)

    # ---------------- 曲库索引 ----------------

    def _library(self) -> _library.LibraryIndex:
        """内存索引（每个插件实例一份）。落盘的只有标签增量表。"""
        index: _library.LibraryIndex = self.__dict__.setdefault("_library_index",
                                                                 _library.LibraryIndex())
        try:
            index.configure(Path(self.get_data_path()) / "library-tags.json.gz")
        except Exception as err:  # pragma: no cover - 数据目录不可写时只做内存索引
            self.__dict__["_library_error"] = f"索引缓存不可用：{err}"
        return index

    def _library_rebuild(self, force: bool = False) -> bool:
        """起一轮索引重建；同一时刻只会有一轮在跑。"""
        roots = self._roots()
        if not roots:
            return False
        return self._library().refresh(roots, self._exts(), self._library_limit, force=force)

    def _library_status(self) -> Dict[str, Any]:
        status = self._library().status()
        status["roots"] = [str(item) for item in self._roots()]
        status["limit"] = self._library_limit
        status["refresh_minutes"] = self._library_refresh
        error = self.__dict__.get("_library_error")
        if error:
            status["error"] = str(error)
        return status

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

    @staticmethod
    def _mask_account(value: Any) -> str:
        """账号只回显形状，不回显身份。

        侧栏页每 5 秒带 whole config 轮一次 /status，明文账号名等于把手机号或邮箱
        长期挂在任何能看到这块页面的人眼前；配置页那格也只需要知道"存的是哪个账号"。
        """
        text = str(value or "").strip()
        if not text:
            return ""
        if "@" in text:
            name, _, domain = text.partition("@")
            return f"{name[:2]}***@{domain}"
        if text.isdigit() and len(text) >= 7:
            return f"{text[:2]}****{text[-2:]}"
        return f"{text[:2]}***"

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
            "force_mp3": self._force_mp3,
            "audio_id": self._audio_id,
            "stream_ttl": self._stream_ttl,
            "default_volume": self._default_volume,
            "auto_next": self._auto_next,
            "keep_light": self._keep_light,
            "tts_announce": self._tts_announce,
            "voice_enabled": self._voice_enabled,
            "voice_devices": self._voice_devices,
            "voice_poll": self._voice_poll,
            "voice_interrupt": self._voice_interrupt,
            "voice_hint_tts": self._voice_hint_tts,
            "voice_hint_text": self._voice_hint_text,
            "voice_reply": self._voice_reply,
            "voice_memory": self._voice_memory,
            "ai_enabled": self._ai_enabled,
            "ai_chat": self._ai_chat,
            "ai_timeout": self._ai_timeout,
            "library_limit": self._library_limit,
            "library_refresh": self._library_refresh,
        }

    def _save_config(self) -> None:
        self.update_config(self._config_snapshot())

    # ---------------- 状态落盘 ----------------
    #
    # 这三个方法覆盖了宿主 PluginBase 的同名接口：宿主把状态写进 plugindata 表，
    # 而本机实测这个插件在该表里一条记录都没有（签名密钥、小米登录票据、设备缓存、
    # 队列、收藏在重载或重启后全部蒸发 —— 于是每次重载都换一把签名密钥，先发出去的
    # 直链莫名 403，小米那边也每次都被当成新设备要求二次校验）。落到插件自己的数据
    # 目录就不必赌宿主的会话是否提交，文件在不在也一眼能验。
    # plugin_id 参数保留但不用：数据目录本来就按插件隔离。

    def _state_file(self, key: str) -> Path:
        # key 全是本模块里的常量，仍旧先洗一遍才敢拿去当文件名
        safe = re.sub(r"[^0-9A-Za-z_.-]", "_", str(key or "")) or "state"
        return Path(self.get_data_path()) / "state" / f"{safe}.json"

    def save_data(self, key: str, value: Any, plugin_id: Optional[str] = None) -> None:  # type: ignore[override]
        """写状态：落到临时文件再原子替换，权限 0600（里面有登录票据）。"""
        target = self._state_file(key)
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            handle = tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=target.parent,
                prefix=f".{target.name}.", suffix=".tmp", delete=False)
            try:
                with handle:
                    json.dump(value, handle, ensure_ascii=False)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(handle.name, 0o600)
                os.replace(handle.name, target)
            except BaseException:
                os.unlink(handle.name)
                raise
        except Exception as exc:  # pragma: no cover - 只剩磁盘与权限会走到这里
            logger.warning(f"{_LOG} 状态写入失败 {key}: {exc}")

    def get_data(self, key: Optional[str] = None,  # type: ignore[override]
                 plugin_id: Optional[str] = None) -> Any:
        """读状态：文件缺失或读不动都按「没有」处理，坏文件不该把播放台整个拖死。"""
        if not key:
            return None
        try:
            with open(self._state_file(key), "r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return None

    def del_data(self, key: Optional[str] = None,  # type: ignore[override]
                 plugin_id: Optional[str] = None) -> None:
        """删状态（退出登录用）：删不干净等于票据还留在盘上。"""
        if not key:
            return
        try:
            os.unlink(self._state_file(key))
        except FileNotFoundError:
            pass
        except Exception as exc:
            logger.warning(f"{_LOG} 状态删除失败 {key}: {exc}")

    def _secret(self) -> str:
        """直链签名密钥：首次随机生成后留在插件数据目录，换密钥会让已发出的链接失效（可接受）。"""
        data = self.get_data(_KEY_SECRET)
        if isinstance(data, str) and data:
            return data
        secret = secrets.token_hex(16)
        self.save_data(_KEY_SECRET, secret)
        return secret

    # ---------------- 票据与客户端 ----------------

    def _device_id(self) -> str:
        """本机固定的小米 deviceId，登录、扫码与 UA 都用它。

        每次重新登录换一个新 deviceId，小米会当成「又一台新设备」：于是每回都要
        图形验证码或二次校验，而上一次存的 passToken 也随之作废 —— 表现就是
        「明明填对了却总说登录失败」。首次生成后留在本机，换账号也不换。
        """
        stored = self.get_data(_KEY_DEVICE)
        if isinstance(stored, str) and stored.strip():
            return stored.strip()
        fresh = _mina.generate_device_id()
        self.save_data(_KEY_DEVICE, fresh)
        return fresh

    def _remember_device_id(self, device_id: str) -> None:
        """票据里带的 deviceId 以票据为准（扫码那次的会话绑的就是它）。"""
        if not device_id:
            return
        if str(self.get_data(_KEY_DEVICE) or "") != device_id:
            self.save_data(_KEY_DEVICE, device_id)

    def _save_token(self, token: Dict[str, Any]) -> int:
        """登录成功后的统一落账：存票据、丢掉上一个账号的缓存、拉一次设备。"""
        self.save_data(_KEY_TOKEN, token)
        self._remember_device_id(str(token.get("device_id") or ""))
        self.__dict__["_auth"] = None
        self.__dict__["_auth_state"] = ""
        self.__dict__["_captcha_image"] = ""
        self.__dict__["_qr_login"] = None
        self.__dict__["_devices_cache"] = {}
        self.__dict__["_status_cache"] = {}
        devices, _ = self._devices_cached(force=True)
        logger.info(f"{_LOG} 登录成功（{token.get('login_method') or 'password'}），"
                    f"设备 {len(devices)} 台")
        return len(devices)

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

    # ---------------- 设备分组（多房间同步） ----------------

    def _groups(self) -> List[Dict[str, Any]]:
        """分组列表：一台设备最多属于一个组，加入新组时自动从别的组移出来。"""
        raw = self.get_data(_KEY_GROUPS)
        rows: List[Dict[str, Any]] = []
        for item in raw if isinstance(raw, list) else []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            members = [str(mid).strip() for mid in (item.get("device_ids") or []) if str(mid).strip()]
            if not name or len(members) < 2:
                continue
            rows.append({"name": name, "device_ids": list(dict.fromkeys(members))})
        return rows

    def _save_groups(self, rows: List[Dict[str, Any]]) -> None:
        self.save_data(_KEY_GROUPS, rows)

    def _group_name_of(self, device_id: str) -> str:
        if not device_id:
            return ""
        for row in self._groups():
            if device_id in row["device_ids"]:
                return str(row["name"])
        return ""

    def _fanout_ids(self, device_id: str) -> List[str]:
        """要一起收到这条指令的设备：所在分组的成员（含它自己），没分组就只它一台。

        与上游一致：全组共用同一份队列与播放方式，指令一次性下发，所以随机模式、
        自动续播也不会各放各的。
        """
        if not device_id:
            return []
        name = self._group_name_of(device_id)
        if not name:
            return [device_id]
        for row in self._groups():
            if row["name"] == name:
                members = [item for item in row["device_ids"] if item != device_id]
                return [device_id] + members
        return [device_id]

    def _fanout_devices(self, device_id: str) -> List[Dict[str, Any]]:
        """把 `_fanout_ids` 解析成设备字典，落在当前账号设备列表之外的成员直接跳过。"""
        devices, _ = self._devices_cached(force=False)
        by_id = {str(item.get("device_id") or ""): item for item in devices}
        rows: List[Dict[str, Any]] = []
        for mid in self._fanout_ids(device_id):
            device = by_id.get(mid)
            if device:
                rows.append(device)
            elif mid != device_id:
                logger.warning(f"{_LOG} 分组成员 {mid} 已不在设备列表里，跳过")
        return rows or ([by_id[device_id]] if device_id in by_id else [])

    def _group_follow(self, device: Dict[str, Any], action) -> List[str]:
        """对同一分组的其他成员各做一次 action，返回失败清单（成功时为空）。

        组内某台离线只记一条告警、其余照常，这与上游一致：音箱各自经云端收指令，
        没有帧级同步，轻微起播时差属正常，不做补偿。
        """
        device_id = str(device.get("device_id") or "")
        failures: List[str] = []
        for other in self._fanout_devices(device_id)[1:]:
            name = str(other.get("name") or other.get("device_id") or "音箱")
            try:
                ok, error = action(other)
            except Exception as exc:  # pragma: no cover - 网络层异常形状不固定
                ok, error = False, str(exc)
            if not ok:
                failures.append(f"{name}：{error or '未受理'}")
                logger.warning(f"{_LOG} 分组同步失败 {name}: {error}")
        return failures

    @staticmethod
    def _group_suffix(total: int, failures: List[str]) -> str:
        """把「全组几台、哪几台没跟上」拼进回执，只有一台时什么都不加。"""
        if total <= 1:
            return ""
        text = f"（已同步 {total} 台"
        if failures:
            text += f"，未跟上：{'、'.join(failures)}"
        return text + "）"

    def _groups_out(self, ok: bool = True, message: str = "") -> GroupsOut:
        devices, _ = self._devices_cached(force=False)
        names = {str(item.get("device_id") or ""): str(item.get("name") or item.get("alias") or "")
                 for item in devices}
        return GroupsOut(ok=ok, message=message,
                         items=[GroupOut(name=row["name"], device_ids=row["device_ids"],
                                         devices=[names.get(mid, mid) for mid in row["device_ids"]])
                                for row in self._groups()])

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

        def follow(target: Dict[str, Any]) -> Tuple[bool, str]:
            other = str(target.get("device_id") or "")
            outcome = client.play(
                device_id=other,
                url=url,
                hardware=str(target.get("hardware") or ""),
                play_mode=self._play_mode,
                keep_light=self._keep_light,
            )
            if outcome.get("ok") and self._default_volume:
                try:
                    client.set_volume(other, self._default_volume)
                except MinaError:
                    pass
            return bool(outcome.get("ok")), str(outcome.get("error") or "")

        failures = self._group_follow(device, follow)
        members = len(self._fanout_devices(device_id))
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
        # 成功也留一行：音箱不出声时，「压根没推出去」和「云端受理了但设备没去拉流」
        # 是两个完全不同的方向，而只存最近一次播放的话，日志里什么痕迹都没有。
        # 直链本身带签名，不落日志；落的是音箱被要求去访问的地址与这条链接的格式。
        base, base_src = self._effective_base_url()
        kind = "外链"
        if path:
            kind = _media.content_type(Path(path))
            if _media.needs_transcode(Path(path), self._force_mp3):
                # 音箱实际拿到的是转好的 mp3，日志报源格式会让人以为转码没生效
                kind = f"{kind}→mp3"
        logger.info(
            f"{_LOG} 已推送 {Path(path).name or title or '外部直链'}"
            f" 接口={api} 类型={kind}"
            f" 设备={device.get('name') or device_id}"
            f" 拉流地址={base or '未配置'}（来源={base_src or '无'}）"
        )
        return True, (f"已推送到 {device.get('name') or device_id}"
                      + self._group_suffix(members, failures)), api

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
        # 播放方式是"这台音箱现在怎么播"，换一批曲目不该把它重置：
        # 说"随机播放"之后切歌单还应该是随机，随机进度则跟着新队列重来
        mode = str(self._queue_state().get("mode") or "order")
        state = {
            "device_id": device_id,
            "index": safe_index,
            "items": items,
            "source": "library",
            "mode": mode,
            "random_played": [],
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
            return {"device_id": "", "index": -1, "items": [], "source": "", "mode": "order"}
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
            mode=str(state.get("mode") or "order"),
        )

    def _advance(self, direction: str) -> PlayOut:
        """切到下一首/上一首/指定序号（next 也用于连播推进）。

        下一首去哪由队列的播放方式决定，这一点与上游 playlist-manager 一致：
        顺序播完就停、列表循环回到第一首、随机跳过已播过的（播完一轮自动清空重来）、
        单曲循环重播当前、单曲播放播完就停。
        """
        state = self._queue_state()
        items = state.get("items") or []
        if not items:
            return PlayOut(ok=False, message="队列为空")
        current = int(state.get("index") or 0)
        if direction == "next":
            target = self._next_index(state, current)
            if target is None:
                return PlayOut(ok=True, message=_QUEUE_END_MESSAGE)
        elif direction == "prev":
            target = max(current - 1, 0)
        else:
            target = max(min(int(direction), len(items) - 1), 0)
        path = str(items[target].get("path") or "")
        result = self._play_file(path, device_id=state.get("device_id") or None)
        if result.ok:
            state["index"] = target
            self._note_played(state, target)
            self.save_data(_KEY_QUEUE, state)
            result.queue = self._queue_out(state)
        return result

    def _next_index(self, state: Dict[str, Any], current: int) -> Optional[int]:
        """下一首的序号；返回 None 表示"到此为止，别再播了"。"""
        items = state.get("items") or []
        total = len(items)
        if total <= 0:
            return None
        mode = str(state.get("mode") or "order")
        if mode == "single":
            return current if 0 <= current < total else 0
        if mode == "single_play":
            return None
        if mode == "loop":
            return (current + 1) % total
        if mode == "random":
            played = {int(value) for value in (state.get("random_played") or [])
                      if str(value).lstrip("-").isdigit()}
            left = [position for position in range(total) if position not in played]
            if not left:
                played = {current}
                left = [position for position in range(total) if position not in played]
            return int(random.choice(left)) if left else None
        following = current + 1
        return following if following < total else None

    def _note_played(self, state: Dict[str, Any], target: int) -> None:
        if str(state.get("mode") or "") != "random":
            state["random_played"] = []
            return
        played = [int(value) for value in (state.get("random_played") or [])
                  if str(value).lstrip("-").isdigit()]
        if target not in played:
            played.append(target)
        if len(played) >= len(state.get("items") or []):
            played = [target]
        state["random_played"] = played

    def _set_mode(self, mode: str) -> bool:
        """把播放方式写进队列状态。

        队列为空时也写：说"随机播放"的人要的是"接下来都随机"，不是"这堆里现在随机"。
        """
        state = self._queue_state()
        state["mode"] = mode
        if mode != "random":
            state["random_played"] = []
        self.save_data(_KEY_QUEUE, state)
        return True

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

        def follow(target: Dict[str, Any]) -> Tuple[bool, str]:
            outcome = client.play_operation(str(target.get("device_id") or ""), action)
            return bool(outcome.get("ok")), str(outcome.get("error") or "")

        failures = self._group_follow(device, follow)
        suffix = self._group_suffix(len(self._fanout_devices(str(device.get("device_id") or ""))), failures)
        return PlayOut(ok=bool(result.get("ok")),
                       message=(label if result.get("ok")
                                else str(result.get("error") or "音箱没有受理")) + suffix,
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

        def follow(target: Dict[str, Any]) -> Tuple[bool, str]:
            outcome = client.set_volume(str(target.get("device_id") or ""), value)
            return bool(outcome.get("ok")), str(outcome.get("error") or "")

        failures = self._group_follow(device, follow)
        suffix = self._group_suffix(len(self._fanout_devices(str(device.get("device_id") or ""))), failures)
        return ActionOut(ok=bool(result.get("ok")),
                         message=(f"音量 {value}" if result.get("ok")
                                  else str(result.get("error") or "")) + suffix,
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

        def follow(target: Dict[str, Any]) -> Tuple[bool, str]:
            outcome = client.text_to_speech(str(target.get("device_id") or ""), text)
            return bool(outcome.get("ok") is not False), str(outcome.get("error") or "")

        failures = self._group_follow(device, follow)
        suffix = self._group_suffix(len(self._fanout_devices(str(device.get("device_id") or ""))), failures)
        ok = result.get("ok")
        if ok is None:
            return ActionOut(ok=True,
                             message=str(result.get("error") or "音箱回执超时，可能已播报") + suffix,
                             playing=self._playing_out(device, force=True))
        return ActionOut(ok=bool(ok),
                         message=("已播报" if ok else str(result.get("error") or "音箱拒绝播报")) + suffix,
                         playing=self._playing_out(device, force=True) if ok else None)

    # ---------------- 语音控制 ----------------

    def _timer(self) -> _intents.SleepTimer:
        return _intents.SleepTimer(
            lambda: self.get_data(_KEY_TIMER),
            lambda state: self.save_data(_KEY_TIMER, state),
        )

    def _memory(self) -> _intents.Memory:
        return _intents.Memory(
            lambda: self.get_data(_KEY_MEMORY),
            lambda rows: self.save_data(_KEY_MEMORY, rows),
        )

    def _favorites(self) -> List[str]:
        raw = self.get_data(_KEY_FAVORITES)
        return [str(item) for item in raw if item] if isinstance(raw, list) else []

    def _toggle_favorite(self, path: str, remove: bool = False) -> Tuple[bool, str]:
        """收藏/取消收藏当前这一首：只存库内路径，"播放我的收藏"再按顺序排。"""
        rows = self._favorites()
        real, reason = _media.resolve_playable(path, self._roots(), self._exts())
        if not real and not remove:
            return False, self._reject_text(reason)
        key = str(real or path)
        if remove:
            if key not in rows:
                return False, "这一首本来就不在收藏里"
            rows = [item for item in rows if item != key]
        elif key in rows:
            return True, "已经在收藏里了"
        else:
            rows.append(key)
        self.save_data(_KEY_FAVORITES, rows[-2000:])
        return True, ("已取消收藏" if remove else f"已收藏，共 {len(rows)} 首")

    def _listener(self) -> Optional[_speech.SpeechListener]:
        """语音监听器（懒建）。语音控制关掉时返回 None；没登录时轮询自己退避。"""
        if not self._voice_enabled:
            return None
        listener = self.__dict__.get("_speech_listener")
        if not isinstance(listener, _speech.SpeechListener):
            listener = _speech.SpeechListener(
                handler=self._on_voice_text,
                get_client=self._client,
                get_devices=self._voice_monitored,
                speak=lambda device_id, text: self._tts(text, device_id=device_id),
                interval=float(self._voice_poll),
                chat_active=self._chat_active,
            )
            self.__dict__["_speech_listener"] = listener
        listener.configure(float(self._voice_poll))
        return listener

    def _voice_monitored(self) -> List[Dict[str, Any]]:
        """要读对话记录的设备：配置里点名的那几台，没点名就用当前默认设备。

        轮询是按设备计费（请求数）的，全账号扫一遍再过滤会把小米接口打爆；
        真要控制客厅那台，就把它的 device_id 填进配置。
        """
        devices, _ = self._devices_cached()
        wanted = [item.strip() for item in re.split(r"[,;\s]+", self._voice_devices or "") if item.strip()]
        if not wanted:
            device, _err = self._target_device()
            return [device] if device else []
        return [device for device in devices
                if str(device.get("device_id") or "") in wanted]

    def _on_voice_text(self, text: str, device_id: str, hardware: str) -> str:
        """一句话 → 要做的事 → 要播报的话。硬件参数留着，型号不同要换接口的地方在 _push。"""
        text = (text or "").strip()
        if not text:
            return ""
        logger.info(f"{_LOG} 语音「{text}」设备 {device_id or '-'}")
        match = self._match_voice(text)
        if match is None:
            return self._chat_reply(text, device_id)
        if self._voice_interrupt:
            self._interrupt(device_id)
        if self._voice_hint_tts and match.kind in _voice.SEARCH_TYPES:
            self._tts(self._voice_hint_text, device_id=device_id or None)
        reply = self._run_intent(match, device_id)
        if reply and match.kind in _voice.SEARCH_TYPES and self._voice_memory:
            self._remember(match, text)
        return reply if self._voice_reply else ""

    def _match_voice(self, text: str) -> Optional[_voice.Match]:
        """一句话的三级判定：口令表 → 记忆表 → AI 兜底（顺序与上游一致）。"""
        match = _voice.match_command(text, self._voice_table())
        if match is not None:
            match.argument = match.argument or self._word_fallback(match)
            return match
        if self._voice_memory:
            replay = self._memory().lookup("play_song", text)
            if replay:
                # 上次这么说播过一首：直接重播，省掉一次搜索也省掉误判
                return _voice.Match("play_song", "", "memory", text, text)
        if self._ai_available():
            return self._analyzer().analyze(text)
        return None

    def _voice_commands(self) -> List[str]:
        """启用的口令类型：配置里没关就全开。"""
        raw = self.get_data(_KEY_COMMANDS)
        if not isinstance(raw, dict) or not raw:
            return [row[0] for row in _voice.DEFAULT_COMMANDS]
        return sorted({str(row[0]) for row in _voice.DEFAULT_COMMANDS
                       if raw.get(str(row[0]), True)})

    def _clean_word(self, row: Any) -> Optional[Dict[str, str]]:
        """把用户填的一条口令收敛成 {keyword, kind, param, argument}；不合规给 None。"""
        if not isinstance(row, dict):
            return None
        keyword = _voice.clean_keyword(row.get("keyword"))
        kind = str(row.get("kind") or "").strip()
        params = _voice.kind_params().get(kind)
        if not keyword or params is None:
            return None
        param = str(row.get("param") or "")
        if param not in params:
            param = params[0]
        argument = " ".join(str(row.get("argument") or "").split())[:40]
        return {"keyword": keyword, "kind": kind, "param": param, "argument": argument}

    # 光把整句说完还不够、必须补「内容」的口令：点歌类要歌名，绝对音量要数值，播报要文本
    _WORD_NEEDS_ARGUMENT = frozenset(
        ("play_song", "play_artist", "play_playlist", "play_index", "tts"))

    def _word_gap(self, word: Dict[str, str]) -> str:
        """这句口令光说完够不够：够 → 空串；不够 → 提示语（调用方拒绝收录）。

        「播放周杰伦」这种整句就是内容的口令，不加兜底内容命中了也只是一次空搜索，
        所以要么让用户把内容填进来（命中后又只说了这句时拿它当内容），要么不收。
        """
        if word["argument"]:
            return ""
        needs = word["kind"] in self._WORD_NEEDS_ARGUMENT or (
            word["kind"] == "set_volume" and word["param"] == "absolute")
        if not needs:
            return ""
        hit = _voice.match_command(word["keyword"],
                                   [(word["kind"], word["param"], (word["keyword"],))])
        if hit is not None and hit.argument:
            return ""
        return f"「{word['keyword']}」说完还得补上内容，请把「内容」一起填上"

    def _word_fallback(self, match: _voice.Match) -> str:
        """自定义口令命中后没有尾随内容时，用用户给这条口令配的默认内容。"""
        if match.argument:
            return ""
        for word in self._voice_words():
            if word["keyword"] == match.keyword and word["kind"] == match.kind:
                return word["argument"]
        return ""

    def _voice_words(self) -> List[Dict[str, str]]:
        """用户自己加的口令；存在插件自己的文件里，跟类型开关分开。"""
        raw = self.get_data(_KEY_WORDS)
        rows = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(rows, list):
            return []
        out: List[Dict[str, str]] = []
        for row in rows:
            item = self._clean_word(row)
            if item and item not in out:
                out.append(item)
        return out[:_voice.MAX_CUSTOM_KEYWORDS]

    def _hidden_words(self) -> List[str]:
        """被用户从内置表里藏起来的口令词。"""
        raw = self.get_data(_KEY_WORDS)
        rows = raw.get("hidden") if isinstance(raw, dict) else None
        if not isinstance(rows, list):
            return []
        out: List[str] = []
        for row in rows:
            word = _voice.clean_keyword(row)
            if word and word not in out:
                out.append(word)
        return out

    def _save_words(self, items: List[Dict[str, str]], hidden: List[str]) -> None:
        self.save_data(_KEY_WORDS, {"items": items, "hidden": hidden,
                                    "at": self._now_text()})

    def _voice_table(self) -> List[Tuple[str, str, Tuple[str, ...]]]:
        """当前生效的口令表：内置表按类型开关过滤，扣掉被藏起来的词，再叠上用户加的词。

        用户加的词只能挂到内置已有的类型/子参数上，匹配完走的还是同一套执行代码，
        自定义口令拿不到比内置更大的动作。
        """
        return _voice.build_commands(self._voice_commands(),
                                     self._voice_words(),
                                     self._hidden_words())

    def _assistant(self) -> _intents.Assistant:
        """MoviePilot 配置里的那个智能助手：每次新建都重读一遍宿主配置。

        插件不再自备 api_key/base_url/model —— 主程序设定里配了哪家就用哪家，
        没配就当 AI 兜底不可用，并能在状态里说清是哪一项没配。
        """
        cached = self.__dict__.get("_assistant_cache")
        if isinstance(cached, _intents.Assistant):
            return cached
        assistant = _intents.Assistant(self._ai_timeout)
        self.__dict__["_assistant_cache"] = assistant
        return assistant

    def _ai_available(self) -> bool:
        return bool(self._ai_enabled and self._assistant().available)

    def _analyzer(self) -> _intents.AIAnalyzer:
        """AI 兜底与对话用的分析器：跟着宿主配置与插件开关走，改完即生效。"""
        return _intents.AIAnalyzer({"assistant": self._assistant(),
                                    "timeout": self._ai_timeout})

    def _chat_active(self) -> bool:
        """对话窗口还开着就按满速轮询：追问隔 5 秒才读到很出戏。"""
        if not (self._ai_chat and self._ai_available()):
            return False
        return time.time() - float(self.__dict__.get("_chat_at") or 0.0) < _CHAT_WINDOW_SECONDS

    def _chat_reply(self, text: str, device_id: str) -> str:
        """口令表没命中时的一次 AI 对话；没开 ai_chat 就什么都不做。

        上下文只留在本轮进程内存里，不落 plugindata：家里人跟音箱说的话多半
        不该写进配置库，而且对话窗口本来就是短命的。
        """
        if not (self._ai_chat and self._ai_available()):
            return ""
        history = list(self.__dict__.get("_chat_history") or [])
        answer = self._analyzer().chat(text, history[-_CHAT_TURNS:])
        if not answer:
            return ""
        history += [{"role": "user", "content": text},
                    {"role": "assistant", "content": answer}]
        self.__dict__["_chat_history"] = history[-_CHAT_TURNS * 2:]
        self.__dict__["_chat_at"] = time.time()
        return answer

    def _chat_reset(self) -> None:
        self.__dict__["_chat_history"] = []
        self.__dict__["_chat_at"] = 0.0

    def _interrupt(self, device_id: str) -> None:
        """抢在小爱的回答之前把声道清出来。失败不打扰用户：顶多听到半句"我帮你找找"。"""
        if not device_id:
            return
        client, _err = self._client()
        if not client:
            return
        try:
            client.interrupt(device_id)
        except (MinaAuthError, MinaError):
            pass

    def _current_path(self) -> str:
        """队列里当前这一首的路径；队列空了就退回最近一次推送的记录。"""
        state = self._queue_state()
        items = state.get("items") or []
        index = int(state.get("index") or 0)
        if 0 <= index < len(items):
            return str(items[index].get("path") or "")
        last = self.get_data(_KEY_LAST)
        return str(last.get("path") or "") if isinstance(last, dict) else ""

    def _remember(self, match: _voice.Match, text: str) -> None:
        path = self._current_path()
        if path:
            self._memory().remember(match.kind, text, path)

    def _run_intent(self, match: _voice.Match, device_id: str) -> str:
        """执行一个意图，返回要播报的话（空串 = 没什么可说的）。"""
        kind = match.kind
        target = device_id or None
        selection = _intents.Selection(self._library(), self._favorites())

        if kind == "play_song":
            if not selection.ready:
                return "曲库还没索引好，请先在侧栏页重建索引"
            remembered = self._memory().lookup("play_song", match.argument) if self._voice_memory else ""
            path, queue, label = selection.song(match.argument, remembered)
            if not path:
                self._library_rebuild(force=False)
                return f"没找到{match.argument}"
            result = self._play_file(path, device_id=target, queue_paths=queue)
            return f"播放 {label}" if result.ok else str(result.message or "播放失败")

        if kind == "play_artist":
            if not selection.ready:
                return "曲库还没索引好，请先在侧栏页重建索引"
            paths, label = selection.artist(match.argument)
            if not paths:
                return f"没找到{match.argument}的歌"
            result = self._play_file(paths[0], device_id=target, queue_paths=paths)
            return f"播放{label}" if result.ok else str(result.message or "播放失败")

        if kind == "play_playlist":
            paths, label = selection.playlist(match.argument)
            if not paths and selection.ready:
                paths, label = selection.playlist(match.argument)
            if not paths:
                return f"没找到歌单{match.argument}" if match.argument else "没有可用的歌单"
            index = _library.parse_track_index(match.raw, len(paths)) or 0
            result = self._play_file(paths[index], device_id=target, queue_paths=paths)
            return f"播放歌单 {label}" if result.ok else str(result.message or "播放失败")

        if kind == "play_recommend":
            paths, label = selection.random_songs()
            if not paths:
                return "曲库里没有可播的曲目"
            result = self._play_file(paths[0], device_id=target, queue_paths=paths)
            if result.ok:
                self._set_mode("random")
            return f"随便放点：{label}" if result.ok else str(result.message or "播放失败")

        if kind == "play_index":
            number = _intents.parse_index_number(match.argument)
            if not number:
                return "没听清要播第几首"
            state = self._queue_state()
            total = len(state.get("items") or [])
            if not total:
                return "队列为空，先选一首歌"
            if number > total:
                return f"队列里只有 {total} 首"
            result = self._advance(str(number - 1))
            if not result.ok:
                return str(result.message or "切歌失败")
            row = (state.get("items") or [])[number - 1]
            return f"播放第 {number} 首 {str(row.get('title') or '')}".strip()

        if kind == "set_play_mode":
            mode = match.param or _intents.normalize_mode(match.argument)
            if not mode:
                return "没听清要哪种播放方式"
            self._set_mode(mode)
            return _MODE_TEXT.get(mode, "已调整播放方式")

        if kind == "set_volume":
            if match.param == "up":
                return self._nudge_volume(10, target)
            if match.param == "down":
                return self._nudge_volume(-10, target)
            value = _intents.parse_volume_number(match.argument)
            if value is None:
                return "没听清要调到多少音量"
            result = self._volume(value, device_id=target)
            return f"音量 {value}" if result.ok else str(result.message or "设置音量失败")

        if kind in ("next", "previous"):
            result = self._advance("next" if kind == "next" else "prev")
            if result.message == _QUEUE_END_MESSAGE:
                return str(result.message)
            return "" if result.ok else str(result.message or "切歌失败")
        if kind == "resume":
            result = self._control("play", device_id=target)
            return "" if result.ok else str(result.message or "继续播放失败")
        if kind == "stop":
            result = self._control("stop", device_id=target)
            return "" if result.ok else str(result.message or "停止失败")

        if kind == "favorite":
            current = self._current_path()
            if not current:
                return "现在没有正在播的歌，收藏不了"
            ok, message = self._toggle_favorite(current, remove=match.param == "remove")
            return message if ok else (message or "收藏操作失败")

        if kind == "sleep_timer":
            if match.param == "songs":
                count = _intents.parse_positive_number(match.argument)
                if not count:
                    return "没听清要再播几首"
                self._timer().set_songs(count, device_id)
                return f"好的，再播 {count} 首后停止"
            minutes = _intents.parse_positive_number(match.argument)
            if not minutes:
                return "没听清要定时多久"
            self._timer().set_minutes(minutes)
            return f"好的，{minutes} 分钟后停止播放"
        if kind == "cancel_sleep_timer":
            self._timer().cancel()
            return "已取消定时停止"
        if kind == "query_sleep_timer":
            return self._timer().remaining_text()

        if kind == "query_playing":
            state = self._queue_state()
            items = state.get("items") or []
            index = int(state.get("index") or 0)
            if 0 <= index < len(items):
                return f"现在播的是 {str(items[index].get('title') or '')}".strip()
            playing = self._playing_out(self._target_device(target)[0] or {})
            return f"现在播的是 {playing.title}".strip() if playing.title else "现在没有播歌"

        if kind == "tts":
            if not match.argument:
                return ""
            self._tts(match.argument, device_id=target)
            return ""
        return ""

    def _nudge_volume(self, delta: int, device_id: Optional[str]) -> str:
        """按增量调音量：先读当前值，避免把"大声一点"变成固定值。"""
        device, err = self._target_device(device_id)
        if not device:
            return err
        client, err = self._client()
        if not client:
            return err
        try:
            current = client.get_volume(str(device.get("device_id") or ""))
        except (MinaAuthError, MinaError):
            current = -1
        if current < 0:
            current = int(self._default_volume or 50)
        result = self._volume(max(0, min(100, current + delta)), device_id=device_id)
        return str(result.message or ("已调整音量" if result.ok else "调整音量失败"))

    def _timer_state_out(self) -> Dict[str, Any]:
        """睡眠定时的可序列化状态（侧栏页与 /status.extra 共用）。"""
        state = self._timer().state()
        stop_at = float(state.get("stop_at") or 0)
        songs = state.get("stop_after_songs")
        songs = int(songs) if isinstance(songs, (int, float)) and songs > 0 else 0
        return {
            "active": bool(stop_at > time.time()) or songs > 0,
            "stop_at": self._stamp_text(stop_at) if stop_at > time.time() else "",
            "stop_after_songs": songs,
            "remaining": self._timer().remaining_text(),
        }

    def _voice_state_out(self) -> Dict[str, Any]:
        """监听器概况：没开启就只回 enabled=False，不去创建监听器。"""
        if not self._voice_enabled:
            return {"enabled": False, "interval": int(self._voice_poll),
                    "handled": 0, "devices": [], "error": "", "last_poll_at": "",
                    "chat": False, "chat_active": False}
        listener = self._listener()
        if listener is None:
            return {"enabled": False, "interval": int(self._voice_poll),
                    "handled": 0, "devices": [], "error": "", "last_poll_at": ""}
        status = listener.status()
        status["enabled"] = True
        status["chat"] = bool(self._ai_chat and self._ai_available())
        status["chat_active"] = self._chat_active()
        status["devices"] = [str(device.get("device_id") or "")
                             for device in self._voice_monitored()]
        return status

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
        """宿主周期任务：睡眠定时到点、曲库索引保鲜、定时任务到点、队列连播。"""
        if not self._enabled:
            return
        if self._stop_event().is_set():
            return
        self._tick_timer()
        self._tick_library()
        self._tick_tasks()
        if self._auto_next:
            self._tick_advance()

    def _tick_timer(self) -> None:
        """定时到点就停：只认"到时间"这一种，按首数停止在播完一首时判。"""
        try:
            due, message = self._timer().due()
        except Exception as err:  # pragma: no cover - 落盘状态被手改坏
            logger.warning(f"{_LOG} 读取睡眠定时失败: {err}")
            return
        if not due:
            return
        logger.info(f"{_LOG} {message}，停止播放")
        device_id = str(self._queue_state().get("device_id") or "")
        if not device_id:
            device_id = str((self._target_device()[0] or {}).get("device_id") or "")
        self._control("stop", device_id=device_id or None)

    def _tick_library(self) -> None:
        """索引保鲜：首轮起来就建，之后按配置周期重扫。周期 0 表示只手动。"""
        if not self._roots():
            return
        index = self._library()
        now = time.time()
        if not index.ready:
            self.__dict__["_index_at"] = now
            self._library_rebuild(force=True)
            return
        minutes = int(self._library_refresh or 0)
        if minutes <= 0:
            return
        if now - float(self.__dict__.get("_index_at") or 0.0) < minutes * 60.0:
            return
        self.__dict__["_index_at"] = now
        self._library_rebuild(force=False)

    def _tick_advance(self) -> None:
        """发现「播完了」就推队列下一首。"""
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
        try:
            stopped, message = self._timer().on_track_finished()
        except Exception as err:  # pragma: no cover - 落盘状态被手改坏
            logger.warning(f"{_LOG} 更新睡眠定时失败: {err}")
            stopped, message = False, ""
        if stopped:
            logger.info(f"{_LOG} {message}，停止连播")
            self._control("stop", device_id=device_id)
            return
        result = self._advance("next")
        if self._tts_announce and result.ok:
            advanced = self._queue_state()
            rows = advanced.get("items") or []
            position = int(advanced.get("index") or 0)
            # 顺序播到最后一首、或"单曲播放"模式播完：_advance 会原地返回成功，
            # 这时并没有"接下来"，别播一句报下一首而音箱已经安静了
            if position != index and 0 <= position < len(rows):
                title = str(rows[position].get("title") or "").strip()
                if title:
                    self._tts(f"接下来播放 {title}", device_id=device_id)

    def _voice_tick(self) -> None:
        """宿主周期任务：读一轮小爱对话记录，把新说的话交给口令引擎。

        监听器内部还会按配置的间隔掐表（含失败退避），所以这里的注册间隔只是
        "最快多久醒一次"，配置改小不会真的把小米接口打得更频繁。
        """
        if not self._enabled or not self._voice_enabled:
            return
        if self._stop_event().is_set():
            return
        listener = self._listener()
        if listener is None:
            return
        try:
            listener.tick()
        except Exception as err:  # pragma: no cover - 轮询自身已兜异常，这里只防 handler 之外漏出
            logger.warning(f"{_LOG} 语音轮询异常: {err}")

    # ---------------- 定时任务 ----------------

    def _tasks(self) -> List[Dict[str, Any]]:
        """读回定时任务：形状不对的行直接丢掉，一条坏任务不该拖垮整轮巡检。"""
        data = self.get_data(_KEY_TASKS)
        rows = data.get("items") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            return []
        return [task for task in (self._clean_task(row) for row in rows) if task]

    def _save_tasks(self, rows: List[Dict[str, Any]]) -> None:
        self.save_data(_KEY_TASKS, {"items": rows, "at": self._now_text()})

    @staticmethod
    def _ints(value: Any, low: int, high: int) -> List[int]:
        if not isinstance(value, (list, tuple)):
            return []
        out: List[int] = []
        for item in value:
            try:
                number = int(item)
            except (TypeError, ValueError):
                continue
            if low <= number <= high and number not in out:
                out.append(number)
        return out

    def _clean_task(self, row: Any) -> Optional[Dict[str, Any]]:
        """把一条任务收敛成可执行形状；不合规的直接判成 None（调用方丢弃）。"""
        if not isinstance(row, dict):
            return None
        action = str(row.get("action") or "")
        if action not in _TASK_ACTIONS:
            return None
        name = str(row.get("name") or "").strip()[:40]
        if not name:
            return None
        schedule = row.get("schedule") if isinstance(row.get("schedule"), dict) else {}
        time_text = str(schedule.get("time") or "").strip()
        if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", time_text):
            return None
        stype = str(schedule.get("type") or "weekly")
        if stype not in ("weekly", "monthly"):
            stype = "weekly"
        weekdays = sorted(self._ints(schedule.get("weekdays"), 0, 6))
        monthdays = sorted(self._ints(schedule.get("monthdays"), 1, 31))
        if not (weekdays if stype == "weekly" else monthdays):
            return None
        target = row.get("target") if isinstance(row.get("target"), dict) else {}
        params = row.get("params") if isinstance(row.get("params"), dict) else {}
        now = self._now_text()
        return {
            "id": str(row.get("id") or f"task_{int(time.time() * 1000)}"),
            "name": name,
            "enabled": bool(row.get("enabled")),
            "action": action,
            "schedule": {"type": stype, "time": time_text,
                         "weekdays": weekdays, "monthdays": monthdays},
            "target": {"all": bool(target.get("all")),
                       "device_ids": [str(item).strip() for item in
                                      (target.get("device_ids") or []) if str(item).strip()]},
            "params": dict(params),
            "created_at": str(row.get("created_at") or now),
            "updated_at": now,
        }

    def _task_logs(self, limit: int = 0) -> List[Dict[str, Any]]:
        data = self.get_data(_KEY_TASK_LOGS)
        rows = data.get("items") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            return []
        rows = [row for row in rows if isinstance(row, dict)]
        return rows[-limit:] if limit else rows

    def _append_task_logs(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        kept = self._task_logs() + [row for row in rows if isinstance(row, dict)]
        self.save_data(_KEY_TASK_LOGS, {"items": kept[-_TASK_LOG_KEEP:],
                                        "at": self._now_text()})

    def _task_log(self, task: Dict[str, Any], device: str,
                  ok: bool, message: str) -> Dict[str, Any]:
        return {"task_id": str(task.get("id") or ""), "task_name": str(task.get("name") or ""),
                "action": str(task.get("action") or ""), "device": device,
                "success": bool(ok), "message": str(message or ""), "at": self._now_text()}

    @staticmethod
    def _schedule_hit(schedule: Dict[str, Any], now: datetime) -> bool:
        """weekly 看星期几（0=周日），monthly 看几号。

        上游那套"法定节假日感知"没搬：它把假期表编进插件产物里，宿主容器里的插件
        没有那份数据也没有更新渠道。要"只工作日"或"只周末"就分建两条任务勾不同的周几。
        """
        if str(schedule.get("time") or "") != now.strftime("%H:%M"):
            return False
        if str(schedule.get("type")) == "monthly":
            return now.day in set(int(item) for item in schedule.get("monthdays") or [])
        return (now.isoweekday() % 7) in set(int(item) for item in schedule.get("weekdays") or [])

    @staticmethod
    def _schedule_text(schedule: Dict[str, Any]) -> str:
        time_text = str(schedule.get("time") or "")
        if str(schedule.get("type")) == "monthly":
            days = "、".join(f"{item} 号" for item in schedule.get("monthdays") or [])
            return f"每月 {days} {time_text}"
        # 「每」+「周一」正好连成「每周一」，不必再插「的」这种废话
        days = "、".join(_TASK_WEEK_TEXT[item] for item in schedule.get("weekdays") or [])
        return f"每{days} {time_text}"

    def _task_devices(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        target = task.get("target") or {}
        devices, _ = self._devices_cached(force=False)
        if target.get("all"):
            # 「全部音箱」先只挑在线的：离线的那台每次都留一条失败日志，一周下来全是噪音
            return [item for item in devices if str(item.get("presence") or "") == "online"] or devices
        ids = [str(item) for item in (target.get("device_ids") or [])]
        if not ids:
            device, _ = self._target_device()
            return [device] if device else []
        by_id = {str(item.get("device_id") or ""): item for item in devices}
        return [by_id[item] for item in ids if item in by_id]

    def _run_task(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        action = str(task.get("action") or "")
        if action in ("enable_monitor", "disable_monitor"):
            ok, message = self._task_monitor(action)
            return [self._task_log(task, "全部", ok, message)]
        devices = self._task_devices(task)
        if not devices:
            return [self._task_log(task, "", False, "没有可用的目标音箱（先登录小米账号）")]
        logs: List[Dict[str, Any]] = []
        covered: set = set()
        for device in devices:
            device_id = str(device.get("device_id") or "")
            if device_id in covered:
                continue
            ok, message = self._run_on_device(task, device)
            logs.append(self._task_log(task, str(device.get("name") or device_id), ok, message))
            if ok:
                # 同组的其它成员已被 _control/_volume/_push 带动，别再作为独立目标各下发一次
                covered.update(str(item.get("device_id") or "")
                               for item in self._fanout_devices(device_id)[1:])
        return logs

    def _run_on_device(self, task: Dict[str, Any], device: Dict[str, Any]) -> Tuple[bool, str]:
        action = str(task.get("action") or "")
        device_id = str(device.get("device_id") or "")
        params = task.get("params") or {}
        if action in ("play_playlist", "play_playlist_from"):
            return self._task_play(task, device_id, params)
        if action == "stop":
            out = self._control("stop", device_id=device_id)
            return bool(out.ok), str(out.message or "已停止")
        if action == "set_volume":
            try:
                volume = int(params.get("volume"))
            except (TypeError, ValueError):
                return False, "任务里没有音量值"
            out = self._volume(volume, device_id=device_id)
            return bool(out.ok), str(out.message or "")
        if action == "set_play_mode":
            mode = _intents.normalize_mode(str(params.get("play_mode") or ""))
            if not mode:
                return False, "任务里没有有效的播放方式"
            if not self._set_mode(mode):
                return False, "设置播放方式失败（音箱可能离线）"
            return True, f"已设为{_MODE_TEXT.get(mode, mode)}"
        return False, f"不支持的任务动作：{action}"

    def _task_monitor(self, action: str) -> Tuple[bool, str]:
        enable = action == "enable_monitor"
        if self._voice_enabled == enable:
            return True, f"语音监听已经是{'开' if enable else '关'}的"
        self._voice_enabled = enable
        self._save_config()
        self._refresh_jobs()
        return True, "语音监听已开启" if enable else "语音监听已关闭"

    def _task_play(self, task: Dict[str, Any], device_id: str,
                   params: Dict[str, Any]) -> Tuple[bool, str]:
        index = self._library()
        if not index.ready:
            return False, "曲库索引还没建好，先到「音乐库」栏重建索引"
        playlist = None
        try:
            pid = int(params.get("playlist_id") or 0)
        except (TypeError, ValueError):
            pid = 0
        if pid:
            playlist = index.get_playlist(pid)
        name = str(params.get("playlist_name") or "").strip()
        if playlist is None and name:
            playlist = index.find_playlist_by_name(name)
        if playlist is None:
            return False, f"找不到歌单「{name or pid}」，可能索引过期，重建后再试"
        paths = [song.path for song in index.playlist_songs(playlist)]
        if not paths:
            return False, f"歌单「{playlist.name}」里没有可读到的音频"
        start = 0
        note = ""
        if str(task.get("action")) == "play_playlist_from":
            song_name = str(params.get("song_name") or "").strip()
            hit = index.find_song_in_playlist(playlist, song_name) if song_name else None
            if hit:
                start = hit[0]
                note = f"（从「{hit[1].title or hit[1].name}」开始）"
            elif song_name:
                note = f"（没找到「{song_name}」，从第一首开始）"
        elif str(params.get("start") or "first") == "random" and len(paths) > 1:
            start = random.randrange(len(paths))
            note = "（随机起始）"
        mode = _intents.normalize_mode(str(params.get("play_mode") or ""))
        if mode:
            self._set_mode(mode)
        extras: List[str] = []
        volume = params.get("volume")
        if volume not in (None, ""):
            try:
                out = self._volume(int(volume), device_id=device_id)
                extras.append(f"音量 {int(volume)}" if out.ok else f"音量没设成：{out.message}")
            except (TypeError, ValueError):
                pass
        minutes = params.get("stop_after_minutes")
        try:
            if minutes and int(minutes) > 0:
                self._timer().set_minutes(int(minutes))
                extras.append(f"{int(minutes)} 分钟后停")
        except (TypeError, ValueError):
            pass
        result = self._play_file(paths[start], device_id=device_id, queue_paths=paths)
        if not result.ok:
            return False, f"播放歌单「{playlist.name}」失败：{result.message}"
        suffix = f"（{'，'.join(extras)}）" if extras else ""
        return True, f"播放歌单「{playlist.name}」{note}{suffix}：{result.message}"

    def _tick_tasks(self) -> None:
        """分钟级去重的到点执行：巡检 20 秒一次，同一分钟只认一次，任务不会双触发。"""
        now = datetime.now()
        minute = now.strftime("%Y-%m-%d %H:%M")
        if self.__dict__.get("_task_minute") == minute:
            return
        self.__dict__["_task_minute"] = minute
        for task in self._tasks():
            if not task["enabled"]:
                continue
            if not self._schedule_hit(task["schedule"], now):
                continue
            logger.info(f"{_LOG} 定时任务触发：{task['name']}（{_TASK_ACTIONS.get(task['action'], task['action'])}）")
            try:
                self._append_task_logs(self._run_task(task))
            except Exception as err:  # pragma: no cover - 单条任务异常不外溢
                logger.warning(f"{_LOG} 定时任务 {task['name']} 执行异常: {err}")
                self._append_task_logs([self._task_log(task, "", False, f"执行异常：{err}")])

    def _stop_event(self) -> threading.Event:
        return self.__dict__.setdefault("_stop_flag", threading.Event())

    def _play_lock(self) -> threading.Lock:
        return self.__dict__.setdefault("_op_lock", threading.Lock())

    def _refresh_jobs(self) -> None:
        """让宿主按当前配置重建本插件的周期任务。

        语音任务只在 `voice_enabled` 为真时才登记，而从侧栏页保存配置不会走插件管理
        那条链路，不主动喊一声的话，打开监听也不会有轮询真的跑起来。
        """
        update = getattr(scheduler_sdk, "update_plugin_job", None) if scheduler_sdk else None
        if not update:
            return
        try:
            update(self._plugin_id())
        except Exception as err:  # pragma: no cover - 调度器不可用不影响配置已落盘
            logger.warning(f"{_LOG} 重建周期任务失败: {err}")

    # ---------------- 登录 ----------------

    def _login(self, payload: LoginIn) -> LoginOut:
        """开始或续跑一次登录会话（验证码回来时只带 code）。"""
        code = (payload.code or "").strip()
        user_id = (payload.user_id or "").strip()
        pass_token = (payload.pass_token or "").strip()
        with self._login_lock:
            if user_id or pass_token:
                return self._login_manual(user_id, pass_token)
            auth: Optional[XiaoaiAuth] = self.__dict__.get("_auth")
            if payload.resend:
                # 「重发」不是再点一次提交：小米那次会话的码只发一遍，
                # 重新发起一次登录才会拿到新的 notificationUrl 与新码。
                auth = None
                self.__dict__["_auth"] = None
                self.__dict__["_auth_state"] = ""
            if code and auth and self.__dict__.get("_auth_state") in (LOGIN_NEED_CAPTCHA, LOGIN_NEED_VERIFY):
                state, token, message = (
                    auth.submit_verify_code(code)
                    if self.__dict__.get("_auth_state") == LOGIN_NEED_VERIFY
                    else auth.submit_captcha(code)
                )
            else:
                username = str(payload.username or "").strip()
                if "*" in username:  # 掩码回填不算账号
                    username = ""
                username = (username or self._username).strip()
                password = payload.password or self._password or ""
                if not username or not password:
                    return LoginOut(state="failed", message="请填写小米账号和密码",
                                    logged_in=bool(self._token()))
                auth = XiaoaiAuth(username, password, device_id=self._device_id())
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
            count = self._save_token(token)
            return LoginOut(state=state, message=message or "登录成功",
                            logged_in=True, device_count=count)
        if state == LOGIN_FAILED:
            self.__dict__["_auth"] = None
            self.__dict__["_captcha_image"] = ""
            logger.warning(f"{_LOG} 登录失败：{message or '小米没有给出原因'}")
        elif state in (LOGIN_NEED_CAPTCHA, LOGIN_NEED_VERIFY):
            logger.info(f"{_LOG} 登录停在 {state}：{message or '需要人工填码'}")
        return LoginOut(state=state, message=message,
                        captcha_image=captcha or self.__dict__.get("_captcha_image", ""),
                        logged_in=bool(self._token()),
                        device_count=len(self.__dict__.get("_devices_cache", {}).get("devices") or []))

    def _qr(self, action: str) -> QrOut:
        """扫码登录：start 取一张新的二维码，poll 问一次「扫了没」。

        轮询由前端驱动，插件不起线程：一次 poll 最长阻塞 QR_POLL_TIMEOUT 秒，
        页面一关就没有后续请求，不会留下一个没人收结果的后台循环。
        """
        polling = str(action or "").strip().lower() == "poll"
        with self._login_lock:
            cached = len(((self.__dict__.get("_devices_cache") or {}).get("devices") or []))
            qr: Optional[XiaoaiQrLogin] = self.__dict__.get("_qr_login")
            if not polling:
                qr = XiaoaiQrLogin(device_id=self._device_id())
                self.__dict__["_qr_login"] = qr
                ok, err = qr.start()
                if not ok:
                    self.__dict__["_qr_login"] = None
                    logger.warning(f"{_LOG} 获取二维码失败: {err or '小米没有给出原因'}")
                    return QrOut(state=LOGIN_FAILED, message=err or "获取二维码失败",
                                 logged_in=bool(self._token()), device_count=cached)
                image = qr.image()
                if not image:
                    logger.warning(f"{_LOG} 二维码图片取回来了是空的，重新获取一张试试")
                return QrOut(state=LOGIN_QR_WAITING,
                             message=qr.tips or "用小米音箱/米家 App 扫下面的二维码",
                             qr_image=image, expires_in=qr.seconds_left(),
                             logged_in=bool(self._token()), device_count=cached)

            if qr is None or not qr.ready():
                return QrOut(state=LOGIN_QR_EXPIRED,
                             message="没有进行中的扫码会话，点「获取二维码」重新开始",
                             logged_in=bool(self._token()), device_count=cached)
            state, token, message = qr.poll()
            if state == LOGIN_SUCCESS and token:
                return QrOut(state=LOGIN_SUCCESS, message=message or "扫码登录成功",
                             logged_in=True, device_count=self._save_token(token))
            if state == LOGIN_QR_WAITING:
                return QrOut(state=LOGIN_QR_WAITING, message=message,
                             expires_in=qr.seconds_left(),
                             logged_in=bool(self._token()), device_count=cached)
            self.__dict__["_qr_login"] = None
            logger.info(f"{_LOG} 扫码未成功: {state} {message or '小米没有给出原因'}")
            return QrOut(state=state, message=message,
                         logged_in=bool(self._token()), device_count=cached)

    def _login_manual(self, user_id: str, pass_token: str) -> LoginOut:
        """Cookie 登录：只要浏览器 cookie 里的 userId 与 passToken 两项。

        passToken 是长期票据，拿它现换一张 serviceToken 才算验过这串凭据：
        换到了本机也就有了能静默续期的票据，比直接相信浏览器里那张多半已经
        躺了十几个小时的 serviceToken 可靠得多。
        """
        user_id = (user_id or "").strip()
        pass_token = (pass_token or "").strip()
        if ";" in pass_token or "\n" in pass_token:
            return LoginOut(state="failed",
                            message="Pass Token 只填 cookie 的值本身，别把整串 cookie 粘进来",
                            logged_in=bool(self._token()))
        if not user_id or not pass_token:
            return LoginOut(state="failed",
                            message="userId 与 passToken 都要填：先在浏览器里登录 "
                                    "account.xiaomi.com，再从 DevTools 的 Cookie 里复制这两项的值"
                                    "（passToken 只在这个域的 Cookie 里）",
                            logged_in=bool(self._token()))
        state, token, message = exchange_pass_token(
            pass_token, user_id, device_id=self._device_id(), login_method="manual")
        if state != LOGIN_SUCCESS or not token:
            # 小米在这里只给一个码（70016 = 登录验证失败），最常见的原因是票据是从
            # 别的域复制的、或者那次网页登录已经被风控作废
            logger.warning(f"{_LOG} Cookie 登录换票失败: {message or '小米没有给出原因'}")
            return LoginOut(state="failed",
                            message=f"passToken 换票失败：{message}；"
                                    "重新登录 account.xiaomi.com 后从该域的 Cookie 里复制这两项，"
                                    "仍不行就改用扫码登录",
                            logged_in=bool(self._token()))
        count = self._save_token(token)
        return LoginOut(state=LOGIN_SUCCESS,
                        message=f"passToken 有效，已换到新票据（设备 {count} 台）",
                        logged_in=True, device_count=count)

    def _logout(self) -> LoginOut:
        for key in (_KEY_TOKEN, _KEY_LAST):
            try:
                self.del_data(key)
            except Exception as exc:  # pragma: no cover
                logger.warning(f"{_LOG} 清除 {key} 失败: {exc}")
        self.__dict__["_auth"] = None
        self.__dict__["_auth_state"] = ""
        self.__dict__["_qr_login"] = None
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

    @staticmethod
    def _stamp_text(stamp: float) -> str:
        return datetime.fromtimestamp(stamp).strftime("%Y-%m-%d %H:%M:%S") if stamp else ""

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
            needs.append("登录：填账号密码、扫码，或填 cookie 里的 userId 与 passToken，任选一种")
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
        library_status = self._library_status()
        if self._voice_enabled and not token:
            needs.append("语音控制：读不到小爱的对话记录，需要先在上面登录小米账号")
        elif self._voice_enabled and not self._voice_monitored():
            needs.append("语音控制：没有可监听的设备，检查默认音箱或「监听设备」")
        elif self._voice_enabled and not library_status.get("ready") and roots:
            notices.append("语音点歌还不可用：曲库索引尚未建好，稍等或手动点「重建索引」")
        return StatusOut(
            enabled=self._enabled,
            version=self.plugin_version,
            logged_in=bool(token),
            token_saved_at=str(token.get("saved_at") or ""),
            manual_token=str(token.get("login_method") or "") == "manual",
            login_method=str(token.get("login_method") or ""),
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
                username=self._mask_account(self._username),
                username_set=bool(self._username),
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
                force_mp3=self._force_mp3,
                audio_id=self._audio_id or _mina.DEFAULT_MUSIC_AUDIO_ID,
                stream_ttl=self._stream_ttl,
                default_volume=self._default_volume,
                auto_next=self._auto_next,
                keep_light=self._keep_light,
                tts_announce=self._tts_announce,
                voice_enabled=self._voice_enabled,
                voice_devices=self._voice_devices,
                voice_poll=self._voice_poll,
                voice_interrupt=self._voice_interrupt,
                voice_hint_tts=self._voice_hint_tts,
                voice_hint_text=self._voice_hint_text,
                voice_reply=self._voice_reply,
                voice_memory=self._voice_memory,
                ai_enabled=self._ai_enabled,
                ai_chat=self._ai_chat,
                ai_timeout=self._ai_timeout,
                **self._assistant().describe(),
                library_limit=self._library_limit,
                library_refresh=self._library_refresh,
            ),
            extra={
                "devices_fetched_at": fetched_at,
                "exts": list(self._exts()),
                "music_api_models": list(_mina.MUSIC_API_MODELS),
                "library": library_status,
                "timer": self._timer_state_out(),
                "voice": self._voice_state_out(),
                "favorites": len(self._favorites()),
                "groups": [item.model_dump() for item in self._groups_out().items],
                "default_group": self._group_name_of(
                    str((default_device or {}).get("device_id") or "")),
                "tasks": [self._task_row(task) for task in self._tasks()],
                "task_logs": self._task_logs(10)[::-1],
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
                "summary": "登录小米账号（账密/验证码续跑与重发/Cookie 两项票据）",
            },
            {
                "path": "/login/qrcode",
                "endpoint": self.api_login_qrcode,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": QrOut,
                "summary": "扫码登录：获取二维码 / 查询扫码结果",
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
                "path": "/lyric",
                "endpoint": self.api_lyric,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": LyricOut,
                "summary": "读取库内音频的歌词（侧车 .lrc 或内嵌标签）",
            },
            {
                "path": "/mode",
                "endpoint": self.api_mode,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": ActionOut,
                "summary": "设置播放方式（顺序/循环/随机/单曲循环/单曲播放）",
            },
            {
                "path": "/timer",
                "endpoint": self.api_timer_status,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": TimerOut,
                "summary": "查询睡眠定时停止状态",
            },
            {
                "path": "/timer",
                "endpoint": self.api_timer,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": TimerOut,
                "summary": "设置/取消睡眠定时（按分钟或按首数）",
            },
            {
                "path": "/favorites",
                "endpoint": self.api_favorites,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": FavoritesOut,
                "summary": "收藏列表",
            },
            {
                "path": "/favorites",
                "endpoint": self.api_favorite,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": FavoritesOut,
                "summary": "收藏/取消收藏一首（省略 path 则作用于当前在播）",
            },
            {
                "path": "/library",
                "endpoint": self.api_library,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": LibraryOut,
                "summary": "曲库索引状态（点数、歌单数、上次重建时间）",
            },
            {
                "path": "/library/refresh",
                "endpoint": self.api_library_refresh,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": LibraryOut,
                "summary": "立刻重建曲库索引（后台执行）",
            },
            {
                "path": "/playlists",
                "endpoint": self.api_playlists,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": PlaylistsOut,
                "summary": "歌单列表（目录歌单与 m3u）",
            },
            {
                "path": "/playlist",
                "endpoint": self.api_playlist,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": SongsOut,
                "summary": "展开一个歌单的曲目",
            },
            {
                "path": "/voice",
                "endpoint": self.api_voice,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": VoiceOut,
                "summary": "语音监听状态与小爱最近听到的话",
            },
            {
                "path": "/voice/test",
                "endpoint": self.api_voice_test,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": VoiceOut,
                "summary": "试一句话会被判成哪个口令，可选直接执行",
            },
            {
                "path": "/commands",
                "endpoint": self.api_commands,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": CommandsOut,
                "summary": "口令表：类型开关、内置口令、自定义口令",
            },
            {
                "path": "/commands",
                "endpoint": self.api_commands_save,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": CommandsOut,
                "summary": "改口令表：开关类型、加删自定义口令、藏起或还原内置口令",
            },
            {
                "path": "/groups",
                "endpoint": self.api_groups,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": GroupsOut,
                "summary": "设备分组列表（多房间同步）",
            },
            {
                "path": "/groups",
                "endpoint": self.api_group_save,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": GroupsOut,
                "summary": "新建/修改/删除设备分组",
            },
            {
                "path": "/tasks",
                "endpoint": self.api_tasks,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": TasksOut,
                "summary": "定时任务列表",
            },
            {
                "path": "/tasks",
                "endpoint": self.api_task_save,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": TasksOut,
                "summary": "新建/修改/删除定时任务",
            },
            {
                "path": "/tasks/run",
                "endpoint": self.api_task_run,
                "methods": ["POST"],
                "auth": "bear",
                "response_model": TaskLogsOut,
                "summary": "立刻执行一次指定定时任务",
            },
            {
                "path": "/tasks/logs",
                "endpoint": self.api_task_logs,
                "methods": ["GET"],
                "auth": "bear",
                "response_model": TaskLogsOut,
                "summary": "定时任务执行日志",
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
        """POST /login —— 开始登录，或把验证码交回进行中的那次会话。

        小米这条路会抛网络与协议异常，页面只显示「登录失败」的话没人知道该改什么：
        异常统一折成 state=failed 带原因回来，原样留在插件日志里。
        """
        try:
            return self._login(payload)
        except Exception as exc:
            logger.warning(f"{_LOG} 登录流程异常: {exc}")
            return LoginOut(state="failed", message=f"登录流程出错：{exc}",
                            logged_in=bool(self._token()))

    def api_logout(self) -> LoginOut:
        """POST /logout —— 清除本机票据。"""
        return self._logout()

    def api_login_qrcode(self, payload: QrIn) -> QrOut:
        """POST /login/qrcode —— {action:"start"} 取二维码，{action:"poll"} 问一次结果。

        和 /login 一样不把异常抛给页面：小米这条链路上的异常名字千奇百怪，
        折成一句可读的 message 才有排查价值。
        """
        try:
            return self._qr(payload.action)
        except Exception as exc:
            logger.warning(f"{_LOG} 扫码流程异常: {exc}")
            return QrOut(state=LOGIN_FAILED, message=f"扫码流程出错：{exc}",
                         logged_in=bool(self._token()))

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

    def api_lyric(self, path: str = "") -> LyricOut:
        """GET /lyric?path=<库内音频> —— 侧车 .lrc/.txt 优先，其次音频内嵌歌词标签。"""
        real, reason = _media.resolve_playable(path, self._roots(), self._exts())
        if not real:
            return LyricOut(ok=False, message=self._reject_text(reason), path=str(path or ""))
        data, _ = _lyrics.load_lyric(real, self._roots())
        lines = [LyricLineOut(time=float(row.get("time") or -1),
                              text=str(row.get("text") or ""))
                 for row in data.get("lines") or []]
        return LyricOut(ok=bool(lines), message="" if lines else "这首歌没有歌词文件，也没有内嵌歌词标签",
                        path=str(real), name=str(data.get("name") or real.name),
                        source=str(data.get("source") or ""),
                        has_time=bool(data.get("has_time")), lines=lines)

    def api_mode(self, payload: ModeIn) -> ActionOut:
        """POST /mode —— {mode: order|loop|random|single|single_play}。"""
        mode = _intents.normalize_mode(payload.mode)
        if not mode:
            return ActionOut(ok=False, message="播放方式只能是 order/loop/random/single/single_play")
        self._set_mode(mode)
        return ActionOut(ok=True, message=f"已设为{_MODE_TEXT.get(mode, mode)}")

    def api_timer(self, payload: TimerIn) -> TimerOut:
        """POST /timer —— {minutes} | {songs} | {cancel:true}，返回设置后的定时状态。"""
        timer = self._timer()
        if payload.cancel:
            timer.cancel()
            return self._timer_model("已取消定时停止")
        if payload.songs:
            device_id = str((self._target_device(payload.device_id)[0] or {}).get("device_id") or "")
            timer.set_songs(int(payload.songs), device_id)
        elif payload.minutes:
            timer.set_minutes(int(payload.minutes))
        else:
            return self._timer_model("请给出 minutes 或 songs")
        return self._timer_model()

    def api_timer_status(self) -> TimerOut:
        """GET /timer —— 当前定时停止状态。"""
        return self._timer_model()

    def api_favorites(self) -> FavoritesOut:
        """GET /favorites —— 收藏列表（库里已经删掉的标 exists=false）。"""
        return self._favorites_out()

    def api_favorite(self, payload: FavoriteIn) -> FavoritesOut:
        """POST /favorites —— {path?, remove}；不传 path 就作用于当前在播这一首。"""
        path = (payload.path or "").strip() or self._current_path()
        if not path:
            return FavoritesOut(ok=False, message="没有指定曲目，现在也没有在播的歌")
        ok, message = self._toggle_favorite(path, remove=payload.remove)
        return self._favorites_out(ok=ok, message=message)

    def api_library(self) -> LibraryOut:
        """GET /library —— 曲库索引状态。"""
        return self._library_model()

    def api_library_refresh(self) -> LibraryOut:
        """POST /library/refresh —— 立刻起一轮重扫（后台线程，不等结果）。"""
        if not self._roots():
            return self._library_model(ok=False, message="还没有配置「音乐库路径」")
        started = self._library_rebuild(force=True)
        return self._library_model(
            message="已开始重建索引，稍后刷新查看" if started else "索引正在重建中，请稍后再试")

    def api_playlists(self) -> PlaylistsOut:
        """GET /playlists —— 目录歌单与 m3u 歌单列表。"""
        rows = self._library().playlists()
        return PlaylistsOut(ok=bool(rows),
                            message="" if rows else "曲库里还没有可播的歌单（先重建索引）",
                            items=[PlaylistOut(id=str(row.get("id")), name=str(row.get("name") or ""),
                                               kind=str(row.get("kind") or ""),
                                               path=str(row.get("path") or ""),
                                               count=int(row.get("song_count") or 0))
                                   for row in rows])

    def api_playlist(self, playlist_id: int = 0) -> SongsOut:
        """GET /playlist?playlist_id=<歌单号> —— 展开一个歌单的曲目路径。"""
        playlist = self._library().get_playlist(int(playlist_id or 0))
        if not playlist:
            return SongsOut(ok=False, message="没有这个歌单，先 GET /playlists 看编号")
        songs = self._library().playlist_songs(playlist)
        return SongsOut(ok=bool(songs), message="" if songs else "这个歌单里没有可读到的音频",
                        name=playlist.name, paths=[song.path for song in songs],
                        items=[self._file_out(song) for song in songs[:500]])

    def api_voice(self) -> VoiceOut:
        """GET /voice —— 监听状态 + 小爱最近听到的话。"""
        return self._voice_model(records=True)

    def api_voice_test(self, payload: VoiceIn) -> VoiceOut:
        """POST /voice/test —— {text, execute}：不接音箱也能看出一句话被判成了什么。"""
        text = (payload.text or "").strip()[:200]
        out = self._voice_model(records=True)
        if not text:
            return out.model_copy(update={"ok": False, "message": "请输入一句话"})
        match = self._match_voice(text)
        # 判定过程记一行：口令表命不命中是确定的，AI 兜底每次可能给不同结果，
        # 用户说「同一个词两次结果不一样」时只能靠这行日志分辨是哪一级给的答案。
        logger.info(f"{_LOG} 口令判定「{text}」执行={bool(payload.execute)} → "
                    f"{match.kind if match else '未接手'}"
                    f"（{'口令表' if match and match.keyword not in ('ai', 'memory') else '记忆表/AI' if match else '三级都没命中'}）")
        if match is None:
            out.ok = False
            out.message = ("这句话没有接手：口令表和记忆表都没命中，"
                           + ("AI 兜底这次也没判出来（结果本身有随机性，可以再试一次）"
                              if self._ai_available() else "AI 兜底没开"))
            return out
        out.ok = True
        out.kind = match.kind
        out.argument = match.argument
        out.matched = match.keyword or match.kind
        if payload.execute:
            out.reply = self._run_intent(match, (payload.device_id or "").strip())
            out.message = "已执行" + (f"：{out.reply}" if out.reply else "（音箱没有回话）")
        else:
            out.message = "仅判定，未执行；勾上「直接执行」才会真的推到音箱"
        return out

    def api_commands(self) -> CommandsOut:
        """GET /commands —— 口令表：类型开关、内置口令（标出被藏起来的）、用户自定义口令。"""
        enabled = set(self._voice_commands())
        hidden = set(self._hidden_words())
        rows: List[CommandOut] = []
        by_kind: Dict[str, CommandOut] = {}
        for row in _voice.default_commands():
            kind = str(row.get("kind"))
            words = [str(word) for word in (row.get("keywords") or [])]
            item = by_kind.get(kind)
            if item is None:
                item = CommandOut(kind=kind, label=_voice.COMMAND_LABELS.get(kind, kind),
                                  enabled=kind in enabled)
                by_kind[kind] = item
                rows.append(item)
            # 一个类型可能有几行子参数（播放方式、音量、收藏）：词并到一起显示
            item.keywords = item.keywords + [word for word in words
                                             if word not in item.keywords]
            item.hidden = sorted(set(item.hidden) | ({word for word in words if word in hidden}))
        customs = [CommandWordOut(
            keyword=word["keyword"], kind=word["kind"], param=word["param"],
            argument=word["argument"],
            label=_voice.COMMAND_LABELS.get(word["kind"], word["kind"]),
            param_label=_voice.PARAM_LABELS.get(word["param"], word["param"]))
            for word in self._voice_words()]
        kinds = [CommandKindOut(
            kind=row["kind"], label=row["label"],
            params=[CommandParamOut(value=param["value"], label=param["label"])
                    for param in row["params"]])
            for row in _voice.kind_options()]
        return CommandsOut(items=rows, customs=customs, kinds=kinds)

    def api_commands_save(self, payload: CommandsIn) -> CommandsOut:
        """POST /commands —— 改哪块给哪块：类型开关、加口令、删口令、藏内置、还原内置。

        每个字段都是"没给就不动"，所以前端一次只改一件事也不会把别的改没；
        `reset` 才是把类型开关和自定义口令一起恢复默认。
        """
        if payload.reset:
            self.save_data(_KEY_COMMANDS, {})
            self.save_data(_KEY_WORDS, {})
            out = self.api_commands()
            out.message = "口令表已全部恢复默认"
            return out

        known = {str(row[0]) for row in _voice.DEFAULT_COMMANDS}
        if payload.enabled is not None:
            wanted = {str(item).strip() for item in payload.enabled} & known
            self.save_data(_KEY_COMMANDS, {kind: (kind in wanted) for kind in known})

        items = self._voice_words()
        hidden = self._hidden_words()
        builtins = {str(word) for row in _voice.DEFAULT_COMMANDS for word in row[2]}
        done: List[str] = []
        skipped: List[str] = []

        index = {word["keyword"]: pos for pos, word in enumerate(items)}
        for row in payload.add or []:
            word = self._clean_word(row if isinstance(row, dict)
                                    else row.model_dump())
            if not word:
                skipped.append("有一条口令太空、太长或者类型不对，没收")
                continue
            text = word["keyword"]
            if text in builtins:
                skipped.append(f"「{text}」内置本来就认，不用加")
                continue
            gap = self._word_gap(word)
            if gap:
                skipped.append(gap)
                continue
            if text in hidden:
                hidden.remove(text)
            current = items[index[text]] if text in index else None
            if current == word:
                skipped.append(f"「{text}」已经加过了")
            elif current is not None:
                items[index[text]] = word
                done.append(f"「{text}」改成了{_voice.COMMAND_LABELS.get(word['kind'], word['kind'])}")
            elif len(items) >= _voice.MAX_CUSTOM_KEYWORDS:
                skipped.append(f"自定义口令最多 {_voice.MAX_CUSTOM_KEYWORDS} 条，「{text}」没加进去")
            else:
                items.append(word)
                index[text] = len(items) - 1
                done.append(f"已加「{text}」")

        for text in (payload.remove or []) + (payload.hide or []):
            word = _voice.clean_keyword(text)
            if not word:
                continue
            if word in index:
                items.pop(index.pop(word))
                index = {row["keyword"]: pos for pos, row in enumerate(items)}
                done.append(f"已删「{word}」")
            elif word in builtins and word not in hidden:
                hidden.append(word)
                done.append(f"已藏起「{word}」")
            else:
                skipped.append(f"「{word}」不在口令表里")

        for text in payload.unhide or []:
            word = _voice.clean_keyword(text)
            if word in hidden:
                hidden.remove(word)
                done.append(f"已还原「{word}」")

        if payload.add or payload.remove or payload.hide or payload.unhide:
            self._save_words(items, hidden)

        out = self.api_commands()
        tips = list(dict.fromkeys(skipped))
        out.message = "、".join(done[:6]) if done else (
            "类型开关已保存" if payload.enabled is not None
            else "、".join(tips[:3]) or "没有要改的")
        if done and tips:
            out.message += "；" + "、".join(tips[:2])
        return out

    def _file_out(self, song: Any) -> FileOut:
        return FileOut(name=str(getattr(song, "name", "") or ""),
                       path=str(getattr(song, "path", "") or ""),
                       title=str(getattr(song, "title", "") or ""),
                       size=int(getattr(song, "size", 0) or 0),
                       mtime=int(getattr(song, "mtime", 0) or 0))

    def _favorites_out(self, ok: bool = True, message: str = "") -> FavoritesOut:
        index = self._library()
        rows: List[FavoriteOut] = []
        for path in self._favorites():
            song = index.get(path)
            rows.append(FavoriteOut(path=path,
                                    title=str(getattr(song, "title", "") or "")
                                          or _media.strip_index(Path(path).name),
                                    exists=Path(path).is_file()))
        return FavoritesOut(ok=ok, message=message, items=rows,
                            queue=self._queue_out())

    def _library_model(self, ok: bool = True, message: str = "") -> LibraryOut:
        status = self._library_status()
        return LibraryOut(
            ok=ok, message=message,
            ready=bool(status.get("ready")),
            songs=int(status.get("songs") or 0),
            playlists=int(status.get("playlists") or 0),
            roots=[str(item) for item in status.get("roots") or []],
            limit=int(status.get("limit") or 0),
            refresh_minutes=int(status.get("refresh_minutes") or 0),
            built_at=str(status.get("built_at") or ""),
            skipped=int(status.get("skipped") or 0),
            truncated=bool(status.get("truncated")),
            error=str(status.get("error") or ""),
        )

    def _timer_model(self, message: str = "") -> TimerOut:
        state = self._timer_state_out()
        return TimerOut(active=bool(state.get("active")),
                        stop_at=str(state.get("stop_at") or ""),
                        stop_after_songs=int(state.get("stop_after_songs") or 0),
                        remaining=str(state.get("remaining") or ""),
                        message=message)

    def _voice_model(self, records: bool = False) -> VoiceOut:
        state = self._voice_state_out()
        listener = self.__dict__.get("_speech_listener")
        rows = listener.recent(20) if (records and isinstance(listener, _speech.SpeechListener)) else []
        return VoiceOut(enabled=bool(state.get("enabled")),
                        chat=bool(state.get("chat")),
                        chat_active=bool(state.get("chat_active")),
                        interval=int(self._voice_poll),
                        handled=int(state.get("handled") or 0),
                        devices=[str(item) for item in state.get("devices") or []],
                        commands=self._voice_commands(),
                        error=str(state.get("error") or ""),
                        last_poll_at=str(state.get("last_poll_at") or ""),
                        records=[VoiceRecordOut(device_id=str(row.get("device_id") or ""),
                                                at=str(row.get("at") or ""),
                                                text=str(row.get("text") or ""),
                                                answer=str(row.get("answer") or ""))
                                 for row in rows])

    def api_groups(self) -> GroupsOut:
        """GET /groups —— 设备分组列表。"""
        return self._groups_out()

    def api_group_save(self, payload: GroupIn) -> GroupsOut:
        """POST /groups —— 建组/改组/删组；一台设备只属于一个组。"""
        name = (payload.name or "").strip()[:40]
        if not name:
            return self._groups_out(ok=False, message="分组名字不能为空")
        rows = self._groups()
        if payload.remove:
            kept = [row for row in rows if row["name"] != name]
            if len(kept) == len(rows):
                return self._groups_out(ok=False, message=f"没有名为「{name}」的分组")
            self._save_groups(kept)
            return self._groups_out(message=f"已删除分组「{name}」")
        members = [str(item).strip() for item in (payload.device_ids or []) if str(item).strip()]
        members = list(dict.fromkeys(members))
        if len(members) < 2:
            return self._groups_out(ok=False, message="分组至少要有两台音箱才有意义")
        devices, _ = self._devices_cached(force=False)
        known = {str(item.get("device_id") or "") for item in devices}
        unknown = [item for item in members if item not in known]
        if unknown:
            return self._groups_out(ok=False,
                                    message=f"这些设备不在当前账号的设备列表里：{'、'.join(unknown)}")
        # 成员互斥：加入新组时从其它组里摘出去，免得同一台被两组各推一次
        rows = [row for row in rows if row["name"] != name]
        taken = set(members)
        for row in rows:
            row["device_ids"] = [item for item in row["device_ids"] if item not in taken]
        rows = [row for row in rows if len(row["device_ids"]) >= 2]
        rows.append({"name": name, "device_ids": members})
        self._save_groups(rows)
        return self._groups_out(message=f"分组「{name}」已保存，共 {len(members)} 台")

    def api_tasks(self) -> TasksOut:
        """GET /tasks —— 定时任务列表，每条附带人可读的触发规则。"""
        return self._tasks_out()

    def _task_row(self, task: Dict[str, Any]) -> Dict[str, Any]:
        action = str(task.get("action") or "")
        return {**task,
                "action_text": _TASK_ACTIONS.get(action, action),
                "rule": self._schedule_text(task.get("schedule") or {})}

    def _tasks_out(self, ok: bool = True, message: str = "") -> TasksOut:
        return TasksOut(ok=ok, message=message,
                        items=[self._task_row(task) for task in self._tasks()])

    def api_task_save(self, payload: TaskIn) -> TasksOut:
        """POST /tasks —— 新建/修改/删除一条定时任务。"""
        rows = self._tasks()
        task_id = str(payload.id or "").strip()
        if payload.remove:
            removed = next((item for item in rows if item["id"] == task_id), None)
            if not removed:
                return self._tasks_out(ok=False, message="没有这条定时任务")
            self._save_tasks([item for item in rows if item["id"] != task_id])
            return self._tasks_out(message=f"已删除定时任务「{removed['name']}」")
        task = self._clean_task(payload.model_dump())
        if not task:
            return self._tasks_out(
                ok=False,
                message="任务不完整：要有名字、合法动作、HH:MM 的时间，weekly 勾星期几、monthly 勾几号")
        if task["action"] in ("play_playlist", "play_playlist_from"):
            params = task["params"]
            pid = str(params.get("playlist_id") or "").strip()
            if pid in ("", "0"):
                pid = ""
            if not pid and not str(params.get("playlist_name") or "").strip():
                return self._tasks_out(ok=False, message="播放歌单的任务要先选中一个歌单")
        if len(rows) >= _TASK_MAX and not any(item["id"] == task["id"] for item in rows):
            return self._tasks_out(ok=False, message=f"定时任务最多 {_TASK_MAX} 条，先删掉几条")
        existed = next((item for item in rows if item["id"] == task["id"]), None)
        if existed:
            task["created_at"] = existed["created_at"]
            rows = [task if item["id"] == task["id"] else item for item in rows]
        else:
            rows.append(task)
        self._save_tasks(rows)
        verb = "已更新" if existed else "已创建"
        return self._tasks_out(message=f"{verb}任务「{task['name']}」：{self._schedule_text(task['schedule'])}")

    def api_task_run(self, payload: TaskRunIn) -> TaskLogsOut:
        """POST /tasks/run —— 立刻跑一次某个任务（不等到点，用来验证它真能执行）。"""
        task = next((item for item in self._tasks() if item["id"] == str(payload.id or "").strip()),
                    None)
        if not task:
            return TaskLogsOut(ok=False, message="没有这条定时任务",
                               items=self._task_logs(20)[::-1])
        logs = self._run_task(task)
        self._append_task_logs(logs)
        return TaskLogsOut(ok=all(item["success"] for item in logs), message="", items=logs[::-1])

    def api_task_logs(self) -> TaskLogsOut:
        """GET /tasks/logs —— 最近的定时任务执行日志（新的在前）。"""
        rows = self._task_logs(50)[::-1]
        return TaskLogsOut(message="" if rows else "还没有执行记录", items=rows)

    def api_config(self, payload: ConfigIn) -> StatusOut:
        """POST /config —— 只改传上来的字段，密码留空表示保留原值。"""
        data = payload.model_dump(exclude_none=True)
        password = data.pop("password", None)
        # enabled 决定宿主要不要登记本插件的侧栏与周期任务，voice_* 决定语音任务
        # 是否存在与多久醒一次：改过这些都要请宿主重建
        affects_jobs = any(key in data for key in ("enabled", "voice_enabled", "voice_poll"))
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
                # 回显的是掩码：原样送回来或留空都不算用户改了账号，否则一保存就把
                # 真账号写成 "13****34"，下次登录必挂在密码之前
                if username and "*" not in username and username != self._username:
                    self._username = username
                    self.__dict__["_auth"] = None
                    self.__dict__["_auth_state"] = ""
            if password:
                self._password = str(password)
            # enabled / show_sidebar_nav 与那些开关一样是布尔，但属性名不遵循 _<键名>，
            # 单列一行：改 enabled 就是改宿主眼里的插件状态（get_state），侧栏与周期
            # 任务都卡在这一项上
            if "enabled" in data:
                self._enabled = bool(data.pop("enabled"))
            if "show_sidebar_nav" in data:
                self._show_sidebar_nav = bool(data.pop("show_sidebar_nav"))
            for key, attr, as_bool in (("auto_next", "_auto_next", True),
                                       ("keep_light", "_keep_light", True),
                                       ("tts_announce", "_tts_announce", True),
                                       ("default_device_id", "_default_device_id", False)):
                if key in data:
                    value = data.pop(key)
                    setattr(self, attr, bool(value) if as_bool else str(value or "").strip())
            for key in ("voice_enabled", "voice_interrupt", "voice_hint_tts",
                        "voice_reply", "voice_memory", "ai_enabled", "ai_chat",
                        "force_mp3"):
                if key in data:
                    setattr(self, f"_{key}", bool(data.pop(key)))
            if "voice_hint_text" in data:
                self._voice_hint_text = (str(data.pop("voice_hint_text") or "").strip()
                                         or str(_DEFAULT_CONFIG["voice_hint_text"]))
            if "voice_devices" in data:
                self._voice_devices = str(data.pop("voice_devices") or "").strip()
            if "voice_poll" in data:
                self._voice_poll = self._clamp_poll(data.pop("voice_poll"))
            if "ai_timeout" in data:
                self._ai_timeout = self._clamp_ai_timeout(data.pop("ai_timeout"))
            if "library_limit" in data:
                self._library_limit = _library.clamp_song_limit(data.pop("library_limit"))
            if "library_refresh" in data:
                self._library_refresh = self._clamp_library_refresh(data.pop("library_refresh"))
            self._save_config()
        # 改过语音/设备就要重读配置：监听器的时间基线属于上一批设备，
        # 留着会把旧设备已处理过的序号当成新设备的基线，漏掉第一批口令
        listener = self.__dict__.get("_speech_listener")
        if isinstance(listener, _speech.SpeechListener):
            listener.configure(float(self._voice_poll))
            listener.reset()
        if affects_jobs:
            self._refresh_jobs()
        return self._status(force_devices=True)

    def api_stream(self, request: Request):
        """GET /stream?k=<有效期.签名.路径> —— 校验签名后按 Range 回文件（开了转码则回 mp3 流）。

        只有这一个参数：小米云端会把 URL 里的 `&` 换成空格，多参数直链到音箱手里就散了。
        """
        path, expires, signature = _media.parse_query(dict(request.query_params))
        # 先记「有人来取流」这一行：音箱不出声时，「云端没让它来」和「来了但没解码」
        # 是两个方向，而这句只有在真机上才分得出来。记文件名与请求端，不记签名
        client = getattr(request, "client", None)
        logger.info(
            f"{_LOG} /stream 请求进来: {Path(path).name or '未给出路径'}"
            f" 来自={getattr(client, 'host', '') or '未知'}"
            f" Range={request.headers.get('Range') or '无'}"
        )
        if not _media.verify_sign(self._secret(), path, expires, signature):
            # 不另开一行：上面那句已经记下来的是谁在取什么，这里只补「为什么拒」。
            # 三种原因得分开：链接整段读不出来 = 中间层把 URL 弄坏了，不是过期
            if not signature:
                why = "链接读不出内容（URL 被中间层改写过？）"
            elif expires < int(time.time()):
                why = "已过期"
            else:
                why = "签名不对（密钥换过，或链接是别人拼的）"
            logger.warning(f"{_LOG} /stream 拒绝 {Path(path).name or '未给出路径'}: {why}")
            return JSONResponse(status_code=403, content={"detail": "链接无效或已过期"})
        real, reason = _media.resolve_playable(path, self._roots(), self._exts())
        if not real:
            logger.warning(f"{_LOG} /stream 拒绝 {path}: {reason}")
            return JSONResponse(status_code=403, content={"detail": self._reject_text(reason)})
        try:
            size = real.stat().st_size
        except OSError as exc:
            return JSONResponse(status_code=404, content={"detail": f"读不到文件：{exc}"})
        if _media.needs_transcode(real, self._force_mp3):
            streamed = self._stream_transcoded(real)
            if streamed is not None:
                return streamed
            # 机器上没有 ffmpeg：回落到源格式继续播，开关不该把本来能放的弄成不能放
        header = (request.headers.get("Range") or "").strip()
        start, end = _media.parse_range(header, size)
        # 音箱几乎都发 `Range: bytes=0-`。上游 songloft 对这一句回的是 206 并把
        # Content-Range 对上整首（它的访问日志：range="bytes=0-" status=206
        # content_range="bytes 0-N/N"），而回 200 时有些固件会把这条判成「不可分段的
        # 音频」而不去解码。所以客户端明确要整首时也按 206 回。
        # 越界那种（bytes=99999-）不在此列：那按「从头播」处理，仍回 200 整文件。
        whole = (f"bytes=0-", f"bytes=0-{size - 1}")
        partial = header.lower() in whole or start > 0 or end < size - 1
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Disposition": _media.content_disposition(real.name),
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

    def _stream_transcoded(self, real: Path):
        """现场转 mp3 的回流。返回 None 表示这台机器没有 ffmpeg，交回调用方播源格式。

        转码是流式的、长度未知，所以这一条不能给 Content-Length，也不能装成支持
        Range：音箱拖动进度时会被当作整首重传，而 mp3 是小爱唯一稳当吃的格式。
        """
        if not _media.ffmpeg_available():
            logger.warning(f"{_LOG} 开了转码开关但这台机器上没有 ffmpeg，改为直接播源格式")
            return None
        try:
            proc = _media.start_transcode(real)
        except _media.TranscodeError as exc:
            logger.warning(f"{_LOG} 转码失败 {real.name}: {exc}")
            return JSONResponse(status_code=502, content={"detail": str(exc)})
        return StreamingResponse(
            _media.iter_transcode(proc),
            status_code=200,
            media_type="audio/mpeg",
            headers={
                "Accept-Ranges": "none",
                "Cache-Control": "no-cache",
                "Content-Disposition": _media.content_disposition(f"{real.stem}.mp3"),
            },
        )

    # ---------------- 页面 / 渲染 ----------------

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """Vue 联邦渲染，产物目录 dist/assets。"""
        return "vue", "dist/assets"

    def get_sidebar_nav(self) -> List[Dict[str, Any]]:
        """主界面「发现」分组的全页入口（远程组件 ./AppPage）。

        section/permission 跟宿主内置的豆瓣中心、PT监护室保持一致：发现分区
        的插件都用 `discovery`，order 落在这两者附近，免得入口在分组里乱序。
        """
        if not self.get_state() or not self._show_sidebar_nav:
            return []
        return [
            {
                "nav_key": "main",
                "title": "小爱音箱",
                "icon": "mdi-speaker",
                "section": "discovery",
                "permission": "discovery",
                "order": 22,
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
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VSwitch", "props": {"model": "tts_announce",
                                                               "label": "连播时先播报下一曲"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VSwitch", "props": {
                                "model": "force_mp3",
                                "label": "非 mp3 现场转成 mp3（需要本机有 ffmpeg）"}}
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
        """巡检与语音轮询：交宿主调度器托管，插件不自建 scheduler。"""
        if not self._enabled:
            return []
        services = [
            {
                "id": "XiaoaiSpeaker.Tick",
                "name": "小爱音箱播放台巡检",
                "trigger": IntervalTrigger(seconds=_ADVANCE_TICK_SECONDS),
                "func": self._tick,
                "kwargs": {},
            }
        ]
        if self._voice_enabled:
            services.append(
                {
                    "id": "XiaoaiSpeaker.Voice",
                    "name": "小爱音箱语音口令",
                    "trigger": IntervalTrigger(seconds=max(int(self._voice_poll or 2), 1)),
                    "func": self._voice_tick,
                    "kwargs": {},
                }
            )
        return services

    def stop_service(self) -> None:
        """停用：投递停止信号、取消本插件登记的一次性任务、丢掉监听器的时间基线。"""
        self._stop_event().set()
        listener = self.__dict__.get("_speech_listener")
        if isinstance(listener, _speech.SpeechListener):
            listener.reset()
        self._chat_reset()
        try:
            remove = getattr(scheduler_sdk, "remove_plugin_once_job", None) if scheduler_sdk else None
            if remove:
                remove(self._plugin_id(), "tick")
        except Exception as err:  # pragma: no cover
            logger.debug(f"{_LOG} 取消一次性任务异常: {err}")
