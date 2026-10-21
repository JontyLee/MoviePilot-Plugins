"""
schemas.py — 小爱音箱插件的 API 出入参模型。

端点全部按 V3 约定声明具体的 response_model（插件自定义业务模型，不套宿主
统一 envelope），前端按这里的字段读取；写请求的 body 也用这些模型校验，
避免把任意字典当成合法请求。
"""

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, Field


class DeviceOut(BaseModel):
    """一台小爱音箱设备。"""

    device_id: str
    name: str = ""
    alias: str = ""
    model: str = ""
    hardware: str = ""
    presence: str = "offline"
    is_default: bool = False
    needs_music_api: bool = False


class DevicesOut(BaseModel):
    """设备列表结果。"""

    ok: bool = True
    message: str = ""
    devices: List[DeviceOut] = Field(default_factory=list)
    fetched_at: str = ""


class PlayingOut(BaseModel):
    """当前播放状态（云端读到的状态 + 插件自己记的那条直链）。"""

    device_id: str = ""
    device_name: str = ""
    status: int = -1
    status_text: str = "未知"
    volume: Optional[int] = None
    position: Optional[int] = None
    duration: Optional[int] = None
    title: str = ""
    path: str = ""
    url: str = ""
    source: str = ""
    updated_at: str = ""
    error: str = ""


class QueueItemOut(BaseModel):
    """队列里的一首曲目。"""

    path: str
    name: str = ""
    title: str = ""
    size: int = 0


class QueueOut(BaseModel):
    """播放队列。"""

    device_id: str = ""
    index: int = -1
    total: int = 0
    items: List[QueueItemOut] = Field(default_factory=list)
    auto_next: bool = False
    source: str = ""


class FileOut(BaseModel):
    """库内一个音频文件。"""

    name: str
    path: str
    title: str = ""
    size: int = 0
    mtime: int = 0
    stream_url: str = ""


class DirOut(BaseModel):
    """库内一个子目录。"""

    name: str
    path: str


class BrowseOut(BaseModel):
    """目录浏览结果。"""

    cwd: str = ""
    parent: str = ""
    roots: List[str] = Field(default_factory=list)
    dirs: List[DirOut] = Field(default_factory=list)
    files: List[FileOut] = Field(default_factory=list)
    reason: str = ""
    truncated: bool = False


class SearchOut(BaseModel):
    """库内文件名搜索结果。"""

    keyword: str = ""
    items: List[FileOut] = Field(default_factory=list)
    reason: str = ""


class LoginOut(BaseModel):
    """登录（含验证码续跑）的结果状态。"""

    state: str = "failed"
    message: str = ""
    captcha_image: str = ""
    logged_in: bool = False
    device_count: int = 0


class ActionOut(BaseModel):
    """只关心成功与否的操作结果。"""

    ok: bool = False
    message: str = ""
    playing: Optional[PlayingOut] = None


class PlayOut(BaseModel):
    """推送播放的结果。"""

    ok: bool = False
    message: str = ""
    api: str = ""
    error: str = ""
    playing: Optional[PlayingOut] = None
    queue: Optional[QueueOut] = None


class ConfigOut(BaseModel):
    """配置回显：密码与票据只回「是否已设置」，不回明文。"""

    enabled: bool = False
    show_sidebar_nav: bool = True
    username: str = ""
    password_set: bool = False
    default_device_id: str = ""
    music_dirs: str = ""
    dir_count: int = 0
    missing_dirs: List[str] = Field(default_factory=list)
    base_url: str = ""
    base_url_effective: str = ""
    base_url_source: str = ""
    play_mode: str = "auto"
    audio_exts: str = ""
    audio_id: str = ""
    stream_ttl: int = 3600
    default_volume: int = 0
    auto_next: bool = True
    keep_light: bool = False
    tts_announce: bool = False


class ConfigIn(BaseModel):
    """配置写入：字段缺省（不出现在 body 里）时保留原值。

    「启用插件」与「显示侧栏入口」不在这里改：那两项要宿主重新注册服务与导航，
    只有在插件管理页保存才会走到那条链路。
    """

    username: Optional[str] = None
    password: Optional[str] = None
    default_device_id: Optional[str] = None
    music_dirs: Optional[str] = None
    base_url: Optional[str] = None
    play_mode: Optional[str] = None
    audio_exts: Optional[str] = None
    audio_id: Optional[str] = None
    stream_ttl: Optional[int] = None
    default_volume: Optional[int] = None
    auto_next: Optional[bool] = None
    keep_light: Optional[bool] = None
    tts_announce: Optional[bool] = None


class LoginIn(BaseModel):
    """登录请求：首次带账密，续跑只带验证码。"""

    username: Optional[str] = None
    password: Optional[str] = None
    code: Optional[str] = None
    manual_token: Optional[str] = None


class PlayIn(BaseModel):
    """播放请求：path（库内文件）、url（外部直链）或 keyword（按名搜库内第一个）。"""

    path: Optional[str] = None
    url: Optional[str] = None
    keyword: Optional[str] = None
    device_id: Optional[str] = None
    queue: Optional[List[str]] = None


class QueueIn(BaseModel):
    """队列请求。"""

    paths: List[str] = Field(default_factory=list)
    index: int = 0
    device_id: Optional[str] = None


class ControlIn(BaseModel):
    """播放控制请求。"""

    action: str = "pause"
    device_id: Optional[str] = None


class VolumeIn(BaseModel):
    """音量请求。"""

    volume: int = 50
    device_id: Optional[str] = None


class TtsIn(BaseModel):
    """播报请求。"""

    text: str = ""
    device_id: Optional[str] = None


class DeviceSelectIn(BaseModel):
    """选择默认音箱。"""

    device_id: str = ""


class StatusOut(BaseModel):
    """侧栏页首屏要的全部信息。"""

    enabled: bool = False
    version: str = ""
    logged_in: bool = False
    token_saved_at: str = ""
    token_expired: bool = False
    manual_token: bool = False
    needs: List[str] = Field(default_factory=list)
    notices: List[str] = Field(default_factory=list)
    devices: List[DeviceOut] = Field(default_factory=list)
    default_device: Optional[DeviceOut] = None
    playing: PlayingOut = Field(default_factory=PlayingOut)
    queue: QueueOut = Field(default_factory=QueueOut)
    config: ConfigOut = Field(default_factory=ConfigOut)
    extra: dict[str, Any] = Field(default_factory=dict)
