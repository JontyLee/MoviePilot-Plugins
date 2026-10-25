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
    mode: str = "order"


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


class QrOut(BaseModel):
    """扫码登录的一次结果。

    只回图片本身：二维码的内容（loginUrl）是一次性的登录凭证，回出去等于
    谁都能替本人把这次登录做完，所以它只留在服务端。
    """

    state: str = "failed"
    message: str = ""
    qr_image: str = ""
    expires_in: int = 0
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
    """配置回显：密码与票据只回「是否已设置」，账号只回掩码形状。"""

    enabled: bool = False
    show_sidebar_nav: bool = True
    # 掩码后的账号（13****34 / ab***@example.com）：够认得出"存的是哪个"，不构成身份
    username: str = ""
    username_set: bool = False
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
    force_mp3: bool = False
    audio_id: str = ""
    stream_ttl: int = 3600
    default_volume: int = 0
    auto_next: bool = True
    keep_light: bool = False
    tts_announce: bool = False
    voice_enabled: bool = False
    voice_devices: str = ""
    voice_poll: int = 2
    voice_interrupt: bool = True
    voice_hint_tts: bool = False
    voice_hint_text: str = "正在搜索，请稍候"
    voice_reply: bool = True
    voice_memory: bool = True
    ai_enabled: bool = False
    ai_chat: bool = False
    ai_timeout: int = 6
    # 这三项不是配置，是宿主「设定 → AI 大模型」的读数：插件不另配一份 key
    ai_ready: bool = False
    ai_provider: str = ""
    ai_model: str = ""
    ai_message: str = ""
    library_limit: int = 10000
    library_refresh: int = 60


class ConfigIn(BaseModel):
    """配置写入：字段缺省（不出现在 body 里）时保留原值。

    `enabled` 就是宿主眼里的「插件已启用」—— 它读的是 `get_state()`，而 `get_state()`
    取这一项，所以「启用插件」「显示发现侧栏入口」只能在这里改：插件管理页那个开关写的是
    宿主自己的实例记录，插不进插件的 `get_state()`，只在那儿打开不会出现侧栏，也不会登记
    周期任务。保存后插件主动喊一次 `update_plugin_job` 让宿主重建任务。
    """

    enabled: Optional[bool] = None
    show_sidebar_nav: Optional[bool] = None
    # 账号只在用户真的改了那一格时才送：留空或送回来的是掩码，一律当作「不改」
    username: Optional[str] = None
    password: Optional[str] = None
    default_device_id: Optional[str] = None
    music_dirs: Optional[str] = None
    base_url: Optional[str] = None
    play_mode: Optional[str] = None
    audio_exts: Optional[str] = None
    force_mp3: Optional[bool] = None
    audio_id: Optional[str] = None
    stream_ttl: Optional[int] = None
    default_volume: Optional[int] = None
    auto_next: Optional[bool] = None
    keep_light: Optional[bool] = None
    tts_announce: Optional[bool] = None
    voice_enabled: Optional[bool] = None
    voice_devices: Optional[str] = None
    voice_poll: Optional[int] = None
    voice_interrupt: Optional[bool] = None
    voice_hint_tts: Optional[bool] = None
    voice_hint_text: Optional[str] = None
    voice_reply: Optional[bool] = None
    voice_memory: Optional[bool] = None
    ai_enabled: Optional[bool] = None
    ai_chat: Optional[bool] = None
    ai_timeout: Optional[int] = None
    library_limit: Optional[int] = None
    library_refresh: Optional[int] = None


class LoginIn(BaseModel):
    """登录请求：首次带账密，续跑只带验证码，重发只带 resend。

    Cookie 登录按两项填：浏览器 cookie 里的 userId 与 passToken（只取值本身）。
    """

    username: Optional[str] = None
    password: Optional[str] = None
    code: Optional[str] = None
    resend: bool = False
    user_id: Optional[str] = None
    pass_token: Optional[str] = None


class QrIn(BaseModel):
    """扫码登录：action=start 取一张新的，action=poll 问一次扫码结果。"""

    action: str = "start"


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


class ModeIn(BaseModel):
    """播放方式请求。"""

    mode: str = "order"


class TimerIn(BaseModel):
    """睡眠定时请求：minutes/songs 二选一设置，cancel=true 取消。"""

    minutes: Optional[int] = None
    songs: Optional[int] = None
    device_id: Optional[str] = None
    cancel: bool = False


class TimerOut(BaseModel):
    """睡眠定时状态。"""

    active: bool = False
    stop_at: str = ""
    stop_after_songs: int = 0
    remaining: str = ""
    message: str = ""


class FavoriteIn(BaseModel):
    """收藏请求：path 为空时对队列里当前这首操作。"""

    path: Optional[str] = None
    remove: bool = False


class FavoriteOut(BaseModel):
    """一首收藏曲目。"""

    path: str
    title: str = ""
    exists: bool = True


class FavoritesOut(BaseModel):
    """收藏列表。"""

    ok: bool = True
    message: str = ""
    items: List[FavoriteOut] = Field(default_factory=list)
    playing: Optional[PlayingOut] = None
    queue: Optional[QueueOut] = None


class PlaylistOut(BaseModel):
    """一个歌单。"""

    id: str = ""
    name: str = ""
    kind: str = ""
    path: str = ""
    count: int = 0


class PlaylistsOut(BaseModel):
    """歌单列表。"""

    ok: bool = True
    message: str = ""
    items: List[PlaylistOut] = Field(default_factory=list)


class SongsOut(BaseModel):
    """一组曲目（歌单展开 / 随机一批）。"""

    ok: bool = True
    message: str = ""
    name: str = ""
    paths: List[str] = Field(default_factory=list)
    items: List[FileOut] = Field(default_factory=list)


class LibraryOut(BaseModel):
    """曲库索引状态。"""

    ok: bool = True
    message: str = ""
    ready: bool = False
    songs: int = 0
    playlists: int = 0
    roots: List[str] = Field(default_factory=list)
    limit: int = 0
    refresh_minutes: int = 0
    built_at: str = ""
    skipped: int = 0
    truncated: bool = False
    error: str = ""


class VoiceIn(BaseModel):
    """语音口令测试请求。"""

    text: str = ""
    device_id: Optional[str] = None
    execute: bool = False


class VoiceRecordOut(BaseModel):
    """小爱最近听到的一句话。"""

    device_id: str = ""
    at: str = ""
    text: str = ""
    answer: str = ""


class VoiceOut(BaseModel):
    """语音监听状态 + 一次口令测试的结果。"""

    ok: bool = True
    message: str = ""
    enabled: bool = False
    chat: bool = False
    chat_active: bool = False
    interval: int = 0
    handled: int = 0
    devices: List[str] = Field(default_factory=list)
    commands: List[str] = Field(default_factory=list)
    error: str = ""
    last_poll_at: str = ""
    records: List[VoiceRecordOut] = Field(default_factory=list)
    matched: str = ""
    kind: str = ""
    argument: str = ""
    reply: str = ""


class CommandsIn(BaseModel):
    """口令开关：enabled 为要启用的口令类型列表，缺省（null）表示全开。"""

    enabled: Optional[List[str]] = None


class CommandOut(BaseModel):
    """一类口令。"""

    kind: str
    label: str = ""
    enabled: bool = True
    keywords: List[str] = Field(default_factory=list)


class CommandsOut(BaseModel):
    """口令表。"""

    ok: bool = True
    message: str = ""
    items: List[CommandOut] = Field(default_factory=list)


class LyricLineOut(BaseModel):
    """歌词的一行：time 为 -1 表示这一句没有时间戳。"""

    time: float = -1.0
    text: str = ""


class LyricOut(BaseModel):
    """一首曲目的歌词（侧车 .lrc 或内嵌标签）。"""

    ok: bool = True
    message: str = ""
    path: str = ""
    name: str = ""
    source: str = ""
    has_time: bool = False
    lines: List[LyricLineOut] = Field(default_factory=list)


class GroupOut(BaseModel):
    """一个设备分组：组内任一音箱收到指令，全组一起动。"""

    name: str
    device_ids: List[str] = Field(default_factory=list)
    devices: List[str] = Field(default_factory=list)


class GroupsOut(BaseModel):
    """分组列表。"""

    ok: bool = True
    message: str = ""
    items: List[GroupOut] = Field(default_factory=list)


class GroupIn(BaseModel):
    """分组写入：name 必填；remove=true 删除该组；device_ids 至少要两台。"""

    name: str = ""
    device_ids: Optional[List[str]] = None
    remove: bool = False


class TaskIn(BaseModel):
    """定时任务写入：整条定义原样传上来，删除只给 id + remove。

    schedule/target/params 的内部形状由插件的 _clean_task 判定（时间格式、周几范围、
    动作需要的参数），这里只保证传进来的是字典，免得前端多带一个字段就被 422 掉。
    """

    id: Optional[str] = None
    name: str = ""
    enabled: bool = True
    action: str = "play_playlist"
    schedule: dict[str, Any] = Field(default_factory=dict)
    target: dict[str, Any] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    remove: bool = False


class TasksOut(BaseModel):
    """定时任务列表。"""

    ok: bool = True
    message: str = ""
    items: List[dict[str, Any]] = Field(default_factory=list)


class TaskRunIn(BaseModel):
    """立刻跑一次某个任务：不等到点就能验出它到底能不能执行。"""

    id: str = ""


class TaskLogsOut(BaseModel):
    """定时任务执行日志。"""

    ok: bool = True
    message: str = ""
    items: List[dict[str, Any]] = Field(default_factory=list)


class StatusOut(BaseModel):
    """侧栏页首屏要的全部信息。"""

    enabled: bool = False
    version: str = ""
    logged_in: bool = False
    token_saved_at: str = ""
    token_expired: bool = False
    manual_token: bool = False
    login_method: str = ""
    needs: List[str] = Field(default_factory=list)
    notices: List[str] = Field(default_factory=list)
    devices: List[DeviceOut] = Field(default_factory=list)
    default_device: Optional[DeviceOut] = None
    playing: PlayingOut = Field(default_factory=PlayingOut)
    queue: QueueOut = Field(default_factory=QueueOut)
    config: ConfigOut = Field(default_factory=ConfigOut)
    extra: dict[str, Any] = Field(default_factory=dict)
