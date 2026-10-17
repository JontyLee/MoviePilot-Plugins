"""
musiclibraryscraper — MoviePilot v3 插件（插件名：MusicTagWeb音乐库刮削）

用 Music Tag Web（MTW）作为唯一音乐元数据来源，MP 侧走 ScrapingChain 原生 MUSIC 写入。
识别源、字段、封面都来自 MTW 的 /apimt/fetch_id3_by_title/，插件不直连任何音乐平台 API。

v0.2.0：
- 增量扫描：done/failed 状态账（存 plugindata），mtime+size 命中即跳过，不再重复解析 tag。
- 数据页：Vue 联邦交互页（Page.vue），展示 已处理/失败/待重试，支持逐行与批量重试。
- 调度：改用宿主 get_service() 周期任务 + 一次性任务，插件不再自建 BackgroundScheduler。
- 对外：get_api() 提供 /status /files /retry /scan；get_command() 提供状态与重试命令。

v0.2.1（修好 MTW 调用契约，之前一次都没真的刮成功过）：
- 路径前缀 /api/ → /apimt/（MTW 的 Django 路由挂在 /apimt/ 下，旧写法一律 404）。
- 检索关键词用清洗后的曲名（tag 优先、目录结构兜底），不再传 "01 - 轨迹.flac" 这种原始文件名 ——
  MTW 的 fetch_id3_by_title 是拿 title 当搜索词用的，传文件名只会召回噪音。
- 请求体补齐 title/artist/album/full_path/resource，与 MTW 前端调用完全一致。
- RequestUtils.post_dict(jsondata=) → post_json(json=)（宿主没有 post_dict 这个方法）。
- 本地 tag 改由 mutagen 读取：MP 的 MetaInfoPath 对音频返回 MetaVideo，根本没有
  artist/album，导致“已有 tag 跳过”永远判 False、匹配也无从下手。
- 比对归一化改为直接复用宿主 app.domain.music.music_text_key（NFKD + casefold +
  rust zhconv 繁转简），本地 NFKC 实现仅作宿主缺该函数时的兜底。注：MTW 的搜索侧
  本身已做繁简归一（綿綿/绵绵 召回同一批），需要归一的只有插件的比对侧。
- 补空模式的完成判据从 title+artist 收紧为 title+artist+album+内嵌封面：实测整库
  1039 个文件的文字标签早已刮齐，只剩约 36% 缺封套图，旧判据会让插件永远空转。

v0.3.0：
- 识别源改为多选（mtw_resource 存数组，仍兼容旧的单字符串），留空 = 全部源：
  netease / qmusic / kuwo / kugou / migu / musicbrainz / smart_tag。实测 qq、spotify、
  acoustid、all 恒返回空候选，没列进默认。
- 逐源按配置顺序查询并合并候选，候选上标记命中的源，写入与状态账都记真实来源；
  某源已出满分（3/3）候选即停止后续查询，避免每个文件都打满 7 次请求。
- 配置页把最容易误解的几件事讲清楚：元数据来源、识别源留空 = 全部源、只补缺 / 强制覆盖
  分别会动到哪些字段，以及最终是否覆盖还要叠加宿主「媒体刮削」的音乐三项策略。
- 更名 MusicTagWeb音乐库刮削、换 MTW 官方图标、说明里点明元数据来源。
  目录名/类名/配置前缀保持不变，否则 config_data 与 plugindata 会整体失效。

v0.3.1：
- 配置说明改为集中式：全部收进页面底部一个说明块，选项本身只留标签，不再用 persistent-hint
  跟在控件下面（排版被长提示撑坏）。
- 补上「查询与选取」规则说明，回答"排序默认是什么规则"：按源顺序逐源查询，某源出 3/3 满分候选
  即停；都没满分则合并全部候选取最高分；曲名/艺人/专辑 各 1 分（繁简与大小写归一后互相包含
  即命中）；同分时按源顺序、同源内按 MTW 返回顺序取第一个；最高分低于 min_score 判失败。
  匹配的本地身份优先读 tag，缺字段按 艺人/专辑 (年)/NN - 曲名 目录结构兜底。
- 移除 mtw_select_mode：它只被解析和回显、从不进任何请求体，也不是 MTW 的入参（MTW 前端源码里
  查不到这个概念），候选取舍一直由上面的打分规则决定，属于死配置。
- 数据页页脚仍是英文的真因：Vuetify 3 的 VDataTable 没有 footerProps（那是 v2 的 API），
  :footer-props 会被当成未知属性丢掉，页脚一直读 locale 默认值。v3 把 makeVDataTableFooterProps
  平铺进 VDataTable 自身，所以 items-per-page-text / items-per-page-options / page-text
  必须直接写在 <v-data-table> 上。
- 图标改用 MTW 界面自带的 avator.webp，已下载进仓库 icons/，市场引用仓库 raw 地址而不是热链实例。

v0.3.2（整体复盘后的健壮性修复）：
- 扫描只有一个入口：宿主周期任务与一次性任务都改走带互斥的 _run_scan_guarded()。此前
  get_service() 把 __scan 直接交给调度器，绕开了 _scan_running 标志，周 cron 与手动
  /scan、/retry、"立即运行一次" 可以并发跑；两本账各自在开头读、结尾写，后写完的会把
  先写的整本盖掉，已刮成功的记录退回未处理。
- MTW 侧故障不再烧重试额度：响应异常/401/接口错误归类成"MTW 不可达"或"MTW 接口或鉴权失败"，
  既不计入 MAX_RETRY、也不写 failed 账。此前 MTW 重启一次或 JWT 过期，一整轮会把整库每个文件
  各记一次失败，连错 5 轮就全体永久跳过（mtime+size 不变就再也不碰）。
- 连续 5 个文件栽在 MTW 故障上就中止本轮（熔断），不再往下白打请求；中止时不 prune 账本。
- JWT 过期按令牌自身的 exp 声明（或 MTW 返回的 expired_at）计算，不再硬编 6 天；请求被判
  鉴权失败时强制重登一次再重试。
- 停止信号：stop_service() 不再 set() 完立刻 clear()（等于从没停过），扫描开始时才 clear；
  信号量由类属性改为每实例独立。
- 扫描每处理 25 个文件落一次账，长时间运行中途容器重启不再丢掉整轮进度。
- /retry 只接受落在 scraper_paths 内、且不在 exclude_paths 内的路径，客户端传什么都改不了库外文件。
- add_plugin_once_job 返回 False（调度器没在跑）时回落后台线程，不再假装"已提交"。
- 数据页翻页：v-data-table 的 pageCount 由内存里的 items 长度算出，而 items 只是服务端返回的
  当页（≤ limit 条），pageCount 恒为 1 → 下一页按钮永久禁用，这才是 "1 of 1" 的真因
  （v0.3.1 只把文案翻成中文，功能仍是坏的）。改用 v-data-table-server，总条数交给 items-length。
"""
from __future__ import annotations

import base64
import json
import re
import threading
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger
from fastapi import Request

from app import schemas
from app.chain.scraping import ScrapingChain
from app.plugins import _PluginBase
from app.schemas.types import EventType
from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.media import MusicInfo
from app.sdk.network import RequestUtils

try:  # 宿主较新版本才有 SDK 调度门面；缺失时回退到后台线程
    from app.sdk import scheduler as scheduler_sdk
except Exception:  # pragma: no cover - 视宿主版本而定
    scheduler_sdk = None


# MP 支持的音乐扩展名，独立于视频扩展名白名单。
_MUSIC_EXT = (".flac", ".mp3", ".m4a", ".ape", ".wav", ".wv", ".tta",
              ".aif", ".aiff", ".opus", ".ogg", ".mpc", ".dsf", ".dff", ".mka")

# done/failed/runs 状态账的 plugindata key。
_KEY_DONE = "done"
_KEY_FAILED = "failed"
_KEY_RUNS = "runs"
# 默认 cron：每周日 03:00（POSIX DoW 0=周日）。
_DEFAULT_CRON = "0 3 * * 0"
# _score_candidate 的满分：曲名 / 艺人 / 专辑 各 1 分。
_MAX_SCORE = 3
# MTW 实测能返回候选的识别源（顺序即查询顺序）：qq / spotify / acoustid / all 恒返回空，
# 不要列进来。migu 单源给 20 条，musicbrainz 给繁体字形，其余各 10 条。
_DEFAULT_RESOURCES = ("netease", "qmusic", "kuwo", "kugou", "migu", "musicbrainz", "smart_tag")

# MTW 侧故障的原因前缀（不可达 / 接口或鉴权失败）。这类失败说明插件没学到任何关于该文件的信息，
# 因此不计入 MAX_RETRY、也不写 failed 账：否则 MTW 重启一次就给整库每个文件记一笔失败，
# 连错满 MAX_RETRY 轮后全体永久跳过，mtime+size 不变就再也刷不动。
_UNREACHABLE_REASON = "MTW 不可达"
_AUTH_REASON = "MTW 接口或鉴权失败"
_TRANSPORT_REASONS = (_UNREACHABLE_REASON, _AUTH_REASON)
# 熔断：连续多少个文件栽在 MTW 故障上就中止本轮，别在 MTW 挂着的时段空打上千次请求。
_ABORT_AFTER_TRANSPORT = 5
# 扫描过程中每处理多少个文件落一次账：整轮动辄数小时，中途容器重启不该把进度全丢掉。
_SAVE_EVERY = 25

# 配置页底部的一段集中说明：来源、查询与选取规则、两档写入模式、宿主策略叠加。
# 不跟在单个配置项下面，选项本身只留最短的标签，细节全部收在这里。
_FORM_NOTES = (
    "【元数据来源】全部取自 Music Tag Web（MTW）的识别结果，插件不直连任何音乐平台。"
    "识别源可多选，留空即全部源（网易云 / QQ音乐 / 酷我 / 酷狗 / 咪咕 / MusicBrainz / 智能）；"
    "qq、spotify、acoustid、all 实测恒返回空候选，没有列进来。\n"
    "【查询与选取】每个文件按这里的源顺序逐源查询，某源出现 3/3 满分候选就立刻停止，不再问后面的源；"
    "都没满分则合并所有候选取最高分。分数为曲名 / 艺人 / 专辑 各 1 分（先做繁简与大小写归一，"
    "互相包含即算命中）；同分时按源顺序、同源内按 MTW 返回顺序取第一个。"
    "最高分仍低于「最低匹配分数」（默认 3，即要求三项全中）就记为失败、不写入。"
    "用于匹配的本地身份优先读文件已有的 tag，缺哪项就按 艺人/专辑 (年)/NN - 曲名 的目录结构兜底。\n"
    "【写入模式】只补缺：曲名+艺人+专辑+内嵌封面 四项齐全的文件按 mtime+size 记账直接跳过"
    "（不再请求 MTW），缺哪项补哪项，已有非空标签保持不动，手工修改不会被覆盖。"
    "强制覆盖：每轮忽略增量账、全库重扫重问，候选与现有标签不同就重写，封面重新下载内嵌、"
    "歌词强制重写，手工改过的曲名/艺人/专辑/年份/轨道号会被替换；候选里没有的字段不会清空，"
    "插件不认识的自定义注释字段也不会删。\n"
    "【叠加宿主策略】最终是否覆盖还取宿主「设置 → 媒体刮削」音乐三项策略与这里的较强者："
    "宿主标签策略=覆盖时，即使这里选只补缺也会重写；=跳过时，两种模式都不写该项。"
    "宿主默认 标签仅缺失、封面仅缺失、歌词更优才换。\n"
    "【同一份库】MTW 与 MP 需挂载同一份音乐库路径，否则写标签会落到不同文件。"
)

# 各容器格式的 tag key：ID3 帧名 / MP4 atom / Vorbis 注释 / APEv2。
_TAG_KEYS = {
    "title": ("TIT2", "\u00a9nam", "title", "TITLE"),
    "artist": ("TPE1", "\u00a9ART", "artist", "ARTIST"),
    "album": ("TALB", "\u00a9alb", "album", "ALBUM"),
}
# 内嵌封面：ID3 用 APIC，MP4 用 covr，Vorbis/APE 用 picture 注释块。
_COVER_KEYS = ("APIC", "covr", "picture", "metadata_block_picture", "METADATA_BLOCK_PICTURE")
# 常见 “NN - 曲名” / “NN. 曲名” 前缀。
_TRACK_PREFIX_RE = re.compile(r"^\s*\d{1,3}\s*[-._)]\s*")
# 目录名尾部的 “ (2017)” / “ [2017]” 年份。
_YEAR_SUFFIX_RE = re.compile(r"\s*[\[(]\s*(?:19|20)\d{2}\s*[\])]\s*$")
# 比较用归一化：去掉所有空白与非字母数字符号（宿主归一化不可用时的兜底）。
_NORM_STRIP_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")

try:  # 宿主自带的音乐文本归一化：NFKD + casefold + 繁转简（rust zhconv）
    from app.domain.music import music_text_key as _music_text_key
except Exception:  # pragma: no cover - 视宿主版本而定
    _music_text_key = None


def _norm_text(value: Any) -> str:
    """归一化标题/艺人/专辑，供候选比对。

    优先复用宿主 app.domain.music.music_text_key —— 它已经统一了繁简、全半角、
    变音符并忽略排版符号，本插件的失败样本（綿綿/陳奕迅/這所有的一切）全部命中。
    宿主没这个函数时退回 NFKC + 去符号的本地实现。
    """
    if value is None:
        return ""
    text = str(value)
    if _music_text_key is not None:
        try:
            key = _music_text_key(text)
            if key:
                return str(key)
        except Exception:
            pass
    return _NORM_STRIP_RE.sub("", unicodedata.normalize("NFKC", text).casefold())


def _normalize_resources(value: Any) -> List[str]:
    """把 mtw_resource 归一成去重保序的源列表。

    配置项从单选改成了多选，但库里可能还存着旧的单字符串（"netease"），
    也可能被前端写成逗号/分号分隔的串。直接对字符串迭代拿到的是字符，
    所以任何入口都必须先过这里。空值回落到"全部源"。
    """
    if isinstance(value, str):
        raw: List[Any] = value.replace(",", ";").split(";")
    elif isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        raw = []
    out: List[str] = []
    for item in raw:
        if item is None:
            continue
        name = str(item).strip()
        if name and name not in out:
            out.append(name)
    return out or list(_DEFAULT_RESOURCES)


def _epoch_of(value: Any) -> Optional[float]:
    """把秒/毫秒时间戳或 ISO 日期串换算成 epoch 秒，认不出来返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        # 秒级与毫秒级都可能：1e11 秒是公元 5138 年，超过就按毫秒处理
        return float(value / 1000) if value > 1e11 else float(value)
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{10,13}", text):
        return _epoch_of(int(text))
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _jwt_exp(token: str) -> Optional[float]:
    """只解 JWT 载荷里的 exp，不验签：令牌是本机从 MTW 拿的，验签要服务端密钥、没必要。"""
    try:
        segment = str(token).split(".")[1]
        segment += "=" * (-len(segment) % 4)
        claims = json.loads(base64.urlsafe_b64decode(segment.encode()))
    except Exception:
        return None
    return _epoch_of(claims.get("exp") if isinstance(claims, dict) else None)


def _token_expiry_at(token: str, payload: Dict[str, Any]) -> float:
    """令牌什么时候过期按它自己的声明算，不再硬编 6 天。

    MTW 的 JWT 生命周期是服务端配置：换密钥、改过期设置、管理员踢人都可能让它提前失效。
    猜长了就会长期带着一个废 token 跑完整轮，每个文件都"无候选"，还一路记成刮削失败；
    取不到声明时宁可只缓存 1 小时，多登一次比整轮白跑便宜。
    """
    for key in ("expired_at", "expires_at", "expiry", "expire", "exp"):
        epoch = _epoch_of(payload.get(key)) if isinstance(payload, dict) else None
        if epoch:
            return epoch
    return _jwt_exp(token) or (time.time() + 3600)


def _mtw_failure_reason(resp: Any) -> Optional[str]:
    """判断这次 MTW 响应是不是"没给出结论"，返回故障原因；None 表示响应可用（含空候选）。

    RequestUtils.post_json 对超时、连不上、非 2xx、非 JSON 一律返回 None，拿不到状态码，
    所以只能按响应形状分类：DRF 的错误体带 detail/code（JWT 失效就是
    {"detail": "Given token not valid for any token type", "code": "token_not_valid"}），
    正常检索结果一定带 data 数组。空候选是 {"data": []}，属于可用响应。
    """
    if resp is None:
        return _UNREACHABLE_REASON
    if not isinstance(resp, dict):
        return _UNREACHABLE_REASON
    if isinstance(resp.get("data"), list):
        return None
    if any(key in resp for key in ("detail", "error", "exception", "code")):
        return _AUTH_REASON
    return _UNREACHABLE_REASON


def _first_tag_value(tags, keys) -> str:
    """从 mutagen 标签容器里按候选 key 取第一个非空文本。"""
    for key in keys:
        try:
            value = tags.get(key)
        except Exception:
            continue
        if value is None:
            continue
        # ID3 Frame 把文本放在 .text 列表里
        text = getattr(value, "text", value)
        if isinstance(text, (list, tuple)):
            text = text[0] if text else None
        if isinstance(text, bytes):
            try:
                text = text.decode("utf-8", "ignore")
            except Exception:
                continue
        if isinstance(text, str) and text.strip():
            return text.strip()
    return ""


def _read_music_tags(path: Path) -> Dict[str, Any]:
    """读取本地音乐 tag。

    MP 的 MetaInfoPath 对音频返回 MetaVideo（没有 artist/album），不能用于音乐判定，
    因此这里直接用 mutagen。失败时返回空值，由调用方按“缺 tag”处理。
    """
    out: Dict[str, Any] = {
        "title": "", "artist": "", "album": "", "duration": None, "has_cover": False,
    }
    try:
        from mutagen import File as MutagenFile

        audio = MutagenFile(str(path))
    except Exception:
        return out
    if audio is None:
        return out
    tags = getattr(audio, "tags", None)
    if tags is not None:
        for field, keys in _TAG_KEYS.items():
            out[field] = _first_tag_value(tags, keys)
            if not out[field] and field in ("title", "artist"):
                # Vorbis/APE 的 key 大小写不敏感，mutagen 已归一，这里兜底遍历一次
                try:
                    for key in list(tags.keys()):
                        if key.lower() == field:
                            out[field] = _first_tag_value(tags, (key,))
                            if out[field]:
                                break
                except Exception:
                    pass
        for key in _COVER_KEYS:
            try:
                if tags.get(key):
                    out["has_cover"] = True
                    break
            except Exception:
                continue
    if not out["has_cover"]:
        try:
            out["has_cover"] = bool(getattr(audio, "pictures", None))
        except Exception:
            pass
    try:
        info = getattr(audio, "info", None)
        length = getattr(info, "length", None) if info else None
        if length:
            out["duration"] = int(length)
    except Exception:
        pass
    return out


def _path_identity(path: Path) -> Dict[str, str]:
    """按 “艺人/专辑 (年)/NN - 曲名” 目录结构推断身份，供未打 tag 的文件参与匹配。"""
    parents = list(path.parents)
    album_dir = parents[0].name if len(parents) >= 1 else ""
    artist_dir = parents[1].name if len(parents) >= 2 else ""
    return {
        "title": _TRACK_PREFIX_RE.sub("", path.stem).strip(),
        "artist": artist_dir.strip(),
        "album": _YEAR_SUFFIX_RE.sub("", album_dir).strip(),
    }


def _local_identity(path: Path) -> Dict[str, Any]:
    """本地身份：tag 优先，缺失字段用目录结构兜底。

    MTW 的 fetch_id3_by_title 是把 title 当搜索关键词用的，传原始文件名
    （"01 - 轨迹.flac"）只会召回一堆噪音，必须先清洗成干净曲名。
    """
    tags = _read_music_tags(path)
    from_path = _path_identity(path)
    return {
        "title": (tags["title"] or from_path["title"]).strip(),
        "artist": (tags["artist"] or from_path["artist"]).strip(),
        "album": (tags["album"] or from_path["album"]).strip(),
        "duration": tags["duration"],
    }


class _MusicScrapeChain(ScrapingChain):
    """复用 libraryscraper 的 trickplay 排除策略，避免 Jellyfin 预览目录被递归。"""

    def scrape_metadata(self, fileitem, *args, **kwargs):
        if fileitem and MusicLibraryScraper._is_trickplay_path(Path(fileitem.path)):
            return False, "跳过 Jellyfin 预览缓存目录"
        return super().scrape_metadata(fileitem, *args, **kwargs)


class MusicLibraryScraper(_PluginBase):
    """扫描音乐库，向 Music Tag Web 拉候选，交由 MP ScrapingChain 完成写入。

    目录名 / 类名 / 配置前缀沿用 v0.2.x：MP 的 config_data 以 plugin_config_prefix 为键、
    plugindata 以类名为键，改名会让已配置的 MTW 地址账密和已积累的刮削状态账一起失效。
    """

    plugin_name = "MusicTagWeb音乐库刮削"
    plugin_desc = (
        "定时扫描音乐目录，元数据统一取自 Music Tag Web（MTW）的多源识别"
        "（网易云 / QQ / 酷我 / 酷狗 / 咪咕 / MusicBrainz / smart_tag），"
        "再沿用 MP 原生音乐刮削链路写入标签与封面。"
    )
    plugin_icon = "https://raw.githubusercontent.com/JontyLee/MoviePilot-Plugins/main/icons/avator.webp"
    plugin_version = "0.3.2"
    plugin_author = "JontyLee"
    author_url = "https://github.com/JontyLee"
    plugin_config_prefix = "musiclibraryscraper_"
    plugin_order = 8
    user_level = 1

    # 配置项
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: Optional[str] = None
    _scraper_paths: str = ""
    _exclude_paths: str = ""
    _force_all: bool = False
    _mtw_base_url: str = ""
    _mtw_username: str = ""
    _mtw_password: str = ""
    _mtw_resources: List[str] = list(_DEFAULT_RESOURCES)
    _min_score: int = 3

    # MTW 令牌缓存
    _token: Optional[str] = None
    _token_expiry: float = 0.0

    # 失败自动重试上限：retries 达到该值后，定时扫描不再自动重试（仅手动重试）。
    MAX_RETRY: int = 5

    def init_plugin(self, config: Dict[str, Any] = None):
        # 每实例独立的扫描锁、运行标志与停止信号（都放 __dict__，避免类属性被多实例共享）。
        # 运行标志用 setdefault 而不是直接置 False：保存配置会重跑 init_plugin，
        # 那时把在跑的扫描标记成"没在跑"，就等于又开了一条并发写账本的口子。
        self.__dict__.setdefault("_scan_lock", threading.Lock())
        self.__dict__.setdefault("_scan_running", False)
        self.__dict__.setdefault("_stop_flag", threading.Event())

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
            self._mtw_resources = _normalize_resources(config.get("mtw_resource"))
            try:
                self._min_score = int(config.get("min_score") or 3)
            except (TypeError, ValueError):
                self._min_score = 3

        # 勾选“立即运行一次”：回写配置关闭该开关，并投递一次性扫描。
        if self._onlyonce:
            self._onlyonce = False
            # update_config 是整体替换而不是合并（宿主 PluginInstanceOper.save_config_data
            # 直接 record.config_data = deepcopy(config_data)），所以必须把全量配置写回去，
            # 只写 {"onlyonce": False} 会把其余配置一起清掉。密码本来就在这份配置里，
            # 明文落 systemconfig 是 MP 存储插件配置的方式，插件侧无法在不丢配置的前提下回避。
            self.update_config({
                "enabled": self._enabled,
                "onlyonce": False,
                "cron": self._cron,
                "scraper_paths": self._scraper_paths,
                "exclude_paths": self._exclude_paths,
                "mode": "force_all" if self._force_all else "",
                "mtw_base_url": self._mtw_base_url,
                "mtw_username": self._mtw_username,
                "mtw_password": self._mtw_password,
                # 配置键保持单数，值自 v0.3.0 起是数组（读侧兼容旧的单字符串）
                "mtw_resource": self._mtw_resources,
                "min_score": self._min_score,
            })
            self._kick_scan()

    def get_state(self) -> bool:
        return self._enabled

    # ---------------- 命令 ----------------

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程命令：查询刮削状态 / 重试失败项。"""
        return [
            {
                "cmd": "/music_scraper_status",
                "event": EventType.PluginAction,
                "desc": "MTW 音乐库刮削：查看运行状态与统计",
                "category": "音乐刮削",
                "data": {"action": "music_scraper_status"},
            },
            {
                "cmd": "/music_scraper_retry",
                "event": EventType.PluginAction,
                "desc": "MTW 音乐库刮削：重试全部失败项",
                "category": "音乐刮削",
                "data": {"action": "music_scraper_retry"},
            },
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None):
        """处理属于本插件的 PluginAction 事件。"""
        event_data = (event.event_data if event else None) or {}
        action = event_data.get("action")
        if action == "music_scraper_status":
            self._cmd_status()
        elif action == "music_scraper_retry":
            self._cmd_retry()

    def _cmd_status(self):
        """命令：打印最近一次运行汇总与分桶计数。"""
        stats = self._build_status()
        self.post_message(
            title=f"{self.plugin_name} 状态",
            text=(
                f"最近扫描：{stats['runs'].get('ts') or '从未运行'}\n"
                f"发现 {stats['runs'].get('scanned', 0)}｜命中缓存跳过 "
                f"{stats['runs'].get('from_cache', 0)}｜已有tag跳过 "
                f"{stats['runs'].get('skipped_tagged', 0)}\n"
                f"成功 {stats['runs'].get('ok', 0)}｜失败 "
                f"{stats['runs'].get('failed', 0)}｜MTW 故障 "
                f"{stats['runs'].get('transport', 0)}\n"
                f"已处理 {stats['counts']['done']}｜待重试 "
                f"{stats['counts']['retryable']}｜超限 "
                f"{stats['counts']['exhausted']}｜待处理 "
                f"{stats['counts']['pending_unknown']}"
            ),
        )

    def _cmd_retry(self):
        """命令：投递一次“重试全部失败项”的扫描。"""
        failed = self._load_failed()
        paths = list(failed.keys())
        if not paths:
            self.post_message(title=f"{self.plugin_name} 重试", text="没有失败项可重试。")
            return
        started = self._kick_scan(force=True, paths=paths)
        self.post_message(
            title=f"{self.plugin_name} 重试",
            text=f"已提交 {len(paths)} 个失败项的重试。" if started else "已有扫描在运行，请稍后再试。",
        )

    # ---------------- API ----------------

    def get_api(self) -> List[Dict[str, Any]]:
        """插件对外接口，供 Vue 数据页与 Agent 调用（bear 认证）。"""
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "扫描状态与统计汇总",
            },
            {
                "path": "/files",
                "endpoint": self.api_files,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "按桶分页查询文件（done/failed/pending）",
            },
            {
                "path": "/retry",
                "endpoint": self.api_retry,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "重试指定路径或全部失败项",
            },
            {
                "path": "/scan",
                "endpoint": self.api_scan,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "触发一次扫描",
            },
        ]

    def api_status(self, request: Request) -> Dict[str, Any]:
        """GET /status —— 返回汇总、计数、配置回显（不含密码）与最近失败。"""
        return self._build_status()

    def api_files(self, request: Request) -> Dict[str, Any]:
        """GET /files —— bucket=done|failed|pending&limit&offset&search 分页查询。"""
        qp = request.query_params
        bucket = (qp.get("bucket") or "done").lower()
        try:
            limit = max(1, min(int(qp.get("limit") or 50), 500))
        except (TypeError, ValueError):
            limit = 50
        try:
            offset = max(0, int(qp.get("offset") or 0))
        except (TypeError, ValueError):
            offset = 0
        search = (qp.get("search") or "").strip().lower()

        rows = self._collect_rows(bucket)
        if search:
            rows = [r for r in rows if search in r["path"].lower() or search in (r["name"] or "").lower()]
        rows.sort(key=lambda r: r["path"])
        return {
            "bucket": bucket,
            "total": len(rows),
            "offset": offset,
            "limit": limit,
            "items": rows[offset:offset + limit],
        }

    async def api_retry(self, request: Request) -> Dict[str, Any]:
        """POST /retry —— body {paths:[...]} 或 {bucket:"failed", all:true}。"""
        try:
            body = await request.json()
        except Exception:
            body = {}
        paths = body.get("paths") if isinstance(body, dict) else None
        all_failed = bool(isinstance(body, dict) and body.get("all"))
        if (not paths) and all_failed:
            paths = list(self._load_failed().keys())
        if not paths:
            return {"accepted": False, "reason": "no_paths", "count": 0}
        # 只接受库内的音频文件：调用方传什么就刮什么是把插件当成了任意文件写接口，
        # 换个 token 就能改到音乐库之外的文件。
        accepted = [str(p) for p in paths if self._is_library_path(str(p))]
        rejected = len(paths) - len(accepted)
        if rejected:
            logger.warning(f"[music-scraper] /retry 拒绝 {rejected} 个库外或非音频路径")
        if not accepted:
            return {"accepted": False, "reason": "out_of_library", "count": 0}
        started = self._kick_scan(force=True, paths=accepted)
        return {
            "accepted": started,
            "count": len(accepted),
            "rejected": rejected,
            "running": self._is_running(),
            "reason": None if started else "busy",
        }

    async def api_scan(self, request: Request) -> Dict[str, Any]:
        """POST /scan —— body {force?:bool}，触发一次（增量或全量）扫描。"""
        try:
            body = await request.json()
        except Exception:
            body = {}
        force = bool(isinstance(body, dict) and body.get("force")) or self._force_all
        started = self._kick_scan(force=force)
        return {"accepted": started, "running": self._is_running(),
                "reason": None if started else "busy"}

    # ---------------- 页面 / 渲染 ----------------

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """声明 Vue 联邦渲染，产物目录 dist/assets。"""
        return "vue", "dist/assets"

    def get_page(self) -> List[dict]:
        """Vue 模式下详情页由远程 ./Page 渲染，这里返回空。"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """周期扫描服务，由宿主调度器托管（插件自建 scheduler 已移除）。"""
        if not self._enabled:
            return []
        return [
            {
                "id": "MusicLibraryScraper.Scan",
                "name": "MTW 音乐库刮削",
                "trigger": self._build_cron_trigger(self._cron),
                # 必须走带互斥的入口：直接给 __scan 会绕开运行标志，cron 与手动触发
                # 就能并发跑，两本账各自读到结尾再写回，后写的把先写的整本盖掉。
                "func": self._run_scan_guarded,
                "kwargs": {},
            }
        ]

    def stop_service(self):
        """插件停用：投递停止信号并取消宿主一次性任务（可重复调用）。"""
        # 只置位、不在这里 clear：clear 会把刚投递的停止信号吞掉，正在跑的扫描
        # 永远观察不到，等于停不下来。标志改由下一轮扫描开始时复位。
        self._stop_event().set()
        try:
            remove = getattr(scheduler_sdk, "remove_plugin_once_job", None) if scheduler_sdk else None
            if remove:
                remove(self.__class__.__name__, "scan_once")
        except Exception as err:  # pragma: no cover
            logger.debug(f"[music-scraper] 取消一次性任务异常: {err}")

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
                                "model": "mode", "label": "写入模式",
                                "items": [
                                    {"title": "只补缺（推荐）", "value": ""},
                                    {"title": "强制覆盖已有标签", "value": "force_all"},
                                ]
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VCronField", "props": {
                                "model": "cron", "label": "执行周期", "placeholder": "5 位 cron，留空默认 0 3 * * 0"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "mtw_base_url",
                                "label": "Music Tag Web 地址",
                                "placeholder": "http://192.168.x.x:8001"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "min_score", "label": "最低匹配分数（0-3）", "type": "number",
                                "placeholder": "默认 3"
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "mtw_resource", "label": "MTW 识别源（可多选，留空=全部）",
                                "multiple": True, "chips": True, "closable": True,
                                "items": [
                                    {"title": "网易云 netease", "value": "netease"},
                                    {"title": "QQ 音乐 qmusic", "value": "qmusic"},
                                    {"title": "酷我 kuwo", "value": "kuwo"},
                                    {"title": "酷狗 kugou", "value": "kugou"},
                                    {"title": "咪咕 migu", "value": "migu"},
                                    {"title": "MusicBrainz", "value": "musicbrainz"},
                                    {"title": "智能 smart_tag", "value": "smart_tag"},
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
                                "text": _FORM_NOTES
                            }}
                        ]},
                    ]},
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "mode": "",
            "cron": _DEFAULT_CRON,
            "scraper_paths": "",
            "exclude_paths": "",
            "mtw_base_url": "",
            "mtw_username": "",
            "mtw_password": "",
            # 留空 = 全部源（读侧 _normalize_resources 保证），不要把 7 个源预填进去：
            # 预填会让两个配置页长得不一样，也和说明文案对不上。
            "mtw_resource": [],
            "min_score": 3,
        }

    # ---------------- 状态账 ----------------

    def _load_done(self) -> Dict[str, dict]:
        data = self.get_data(_KEY_DONE)
        return data if isinstance(data, dict) else {}

    def _load_failed(self) -> Dict[str, dict]:
        data = self.get_data(_KEY_FAILED)
        return data if isinstance(data, dict) else {}

    def _load_runs(self) -> dict:
        data = self.get_data(_KEY_RUNS)
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------------- 主体流程 ----------------

    def _build_cron_trigger(self, raw_cron: Optional[str]) -> CronTrigger:
        """非法/空 cron 回落默认，避免插件整体 load_failed。"""
        raw = (raw_cron or "").strip() or _DEFAULT_CRON
        try:
            trigger = CronTrigger.from_crontab(raw)
        except (ValueError, KeyError) as err:
            logger.warning(f"[music-scraper] cron 表达式非法：{raw!r} → {err}，回落默认 {_DEFAULT_CRON}")
            trigger = CronTrigger.from_crontab(_DEFAULT_CRON)
        return trigger

    def _is_running(self) -> bool:
        return bool(self.__dict__.get("_scan_running"))

    def _stop_event(self) -> threading.Event:
        """每实例独立的扫描停止信号（init_plugin 之前也可用）。"""
        return self.__dict__.setdefault("_stop_flag", threading.Event())

    def _begin_scan(self) -> bool:
        """占用扫描槽位；已有扫描在跑时返回 False。顺带复位上一轮的停止信号。"""
        lock = self.__dict__.setdefault("_scan_lock", threading.Lock())
        with lock:
            if self.__dict__.get("_scan_running"):
                return False
            self.__dict__["_scan_running"] = True
        self._stop_event().clear()
        return True

    def _end_scan(self):
        lock = self.__dict__.setdefault("_scan_lock", threading.Lock())
        with lock:
            self.__dict__["_scan_running"] = False

    def _execute_scan(self, force: bool = False,
                      target_paths: Optional[List[str]] = None) -> None:
        """执行一轮扫描并释放槽位；调用方必须已经 _begin_scan() 占到槽。"""
        try:
            self.__scan(force=force, target_paths=target_paths)
        except Exception as err:  # pragma: no cover
            logger.error(f"[music-scraper] 扫描异常: {err}")
        finally:
            self._end_scan()

    def _run_scan_guarded(self, force: bool = False,
                          target_paths: Optional[List[str]] = None) -> bool:
        """宿主调度器用的同步入口：抢不到槽位就跳过本轮，不与已 in-flight 的扫描并发。"""
        if not self._begin_scan():
            logger.info("[music-scraper] 已有扫描在运行，本次触发跳过")
            return False
        self._execute_scan(force=force, target_paths=target_paths)
        return True

    def _kick_scan(self, force: bool = False, paths: Optional[List[str]] = None) -> bool:
        """投递一次扫描：无参优先交宿主一次性任务，带参/强制走后台线程。"""
        if not force and not paths:
            add = getattr(scheduler_sdk, "add_plugin_once_job", None) if scheduler_sdk else None
            if add:
                try:
                    # 宿主只在调度器运行且注册成功时返回 True；返回 False 时任务其实没排上，
                    # 不回落后台线程就会对着界面报"已提交"、实际什么都没跑。
                    if add(
                        self.__class__.__name__,
                        "scan_once",
                        self._run_scan_guarded,
                        "MTW 音乐库刮削（一次性）",
                        delay_seconds=3,
                    ):
                        return True
                    logger.debug("[music-scraper] 一次性任务未注册（调度器没在跑），回落后台线程")
                except Exception as err:  # pragma: no cover
                    logger.debug(f"[music-scraper] 一次性任务投递失败，回落后台线程: {err}")
        return self._start_scan_async(force=force, paths=paths)

    def _start_scan_async(self, force: bool = False, paths: Optional[List[str]] = None) -> bool:
        """在后台线程执行扫描；若已有扫描在跑则拒绝，避免并发重复处理。"""
        if not self._begin_scan():
            return False
        threading.Thread(
            target=self._execute_scan,
            kwargs={"force": force, "target_paths": paths},
            name="music-scraper-scan",
            daemon=True,
        ).start()
        return True

    def _is_library_path(self, path: str) -> bool:
        """外部传入的重试路径是否真的是库内的音频文件。

        两侧都做 realpath：scraper_paths 可能是软链，客户端传的路径也可能带 ../，
        只做字符串前缀比较两种都能绕过。
        """
        try:
            target = Path(path).resolve()
            roots = [Path(r).resolve() for r in (self._scraper_paths or "").split("\n") if r.strip()]
        except (OSError, RuntimeError):
            return False
        if not roots or not any(target == r or target.is_relative_to(r) for r in roots):
            return False
        for ex in (p for p in (self._exclude_paths or "").split("\n") if p.strip()):
            try:
                ex_path = Path(ex).resolve()
            except (OSError, RuntimeError):
                continue
            if target == ex_path or target.is_relative_to(ex_path):
                return False
        return target.suffix.lower() in _MUSIC_EXT and target.is_file()

    def _enumerate_files(self) -> List[Path]:
        """按配置枚举音频文件（保留扩展名、trickplay、排除路径过滤）。"""
        exclude_paths = [p for p in (self._exclude_paths or "").split("\n") if p.strip()]
        files: List[Path] = []
        for root in (p for p in (self._scraper_paths or "").split("\n") if p.strip()):
            root_path = Path(root)
            if not root_path.exists():
                logger.warning(f"[music-scraper] 路径不存在: {root}")
                continue
            for fp in root_path.rglob("*"):
                if self._stop_event().is_set():
                    logger.info("[music-scraper] 收到停止信号")
                    return files
                if not fp.is_file():
                    continue
                if fp.suffix.lower() not in _MUSIC_EXT:
                    continue
                if self._is_trickplay_path(fp):
                    continue
                if any(fp.is_relative_to(Path(ex)) for ex in exclude_paths if ex):
                    continue
                files.append(fp)
        return files

    def __scan(self, force: bool = False, target_paths: Optional[List[str]] = None):
        """增量扫描主流程。

        force：忽略 done 缓存并强制重打（覆盖模式下也强制）。
        target_paths：仅处理这批路径（供手动重试），此时跳过缓存与重试上限。
        """
        if not self._scraper_paths or not self._mtw_base_url:
            logger.warning("[music-scraper] 未配置 scraper_paths 或 mtw_base_url，跳过")
            return

        started = time.time()
        done = self._load_done()
        failed = self._load_failed()
        targeted = bool(target_paths)

        if targeted:
            files = [Path(p) for p in target_paths if p]
        else:
            files = self._enumerate_files()

        logger.info(
            f"[music-scraper] 开始处理 {len(files)} 个文件"
            f"（mode={'覆盖' if (force or self._force_all) else '补空'}"
            f"{'，定向重试' if targeted else '，增量'}）"
        )

        scanned = from_cache = skipped_tagged = ok = failed_n = exhausted = 0
        transport = aborted = 0
        now_iso = self._now_iso()
        effective_force = force or (self._force_all and not targeted)
        live_keys = set()  # 整盘扫描时实际存在（stat 成功）的文件，供 prune 用
        dirty = 0  # 自上次落账以来变更过的条目数
        transport_streak = 0  # 连续 MTW 故障的文件数（命中缓存不计入，也不算中断）

        for fp in files:
            if self._stop_event().is_set():
                logger.info("[music-scraper] 收到停止信号，提前结束")
                break
            scanned += 1
            key = str(fp)
            try:
                st = fp.stat()
                mtime, size = int(st.st_mtime), int(st.st_size)
            except OSError as err:
                logger.debug(f"[music-scraper] 无法 stat：{key} — {err}")
                continue
            live_keys.add(key)

            # 增量：命中 done 缓存直接跳过（不读 tag）。
            if not effective_force and not targeted and key in done:
                prev = done[key]
                if isinstance(prev, dict) and prev.get("mtime") == mtime and prev.get("size") == size:
                    from_cache += 1
                    continue

            # 失败上限：非强制、非定向时，超限且文件未变则不再自动重试。
            if not effective_force and not targeted and key in failed:
                prev = failed[key]
                if isinstance(prev, dict) \
                        and int(prev.get("retries", 0)) >= self.MAX_RETRY \
                        and prev.get("mtime") == mtime and prev.get("size") == size:
                    exhausted += 1
                    continue

            # 补空模式：tag 与封面都齐了才跳过。
            if not effective_force and self.__is_complete(fp):
                prev = done.get(key)
                source = prev.get("source") if isinstance(prev, dict) else None
                done[key] = {"mtime": mtime, "size": size, "ts": now_iso, "source": source}
                failed.pop(key, None)
                skipped_tagged += 1
                dirty += 1
                continue

            okf, reason, score, source = self.__scrape_file(fp)
            if reason.startswith(_TRANSPORT_REASONS):
                # MTW 侧故障：插件没学到任何关于这个文件的信息，所以既不写 failed 账、
                # 也不涨 retries，原样留着下一轮再处理。连续踩满就熔断中止本轮。
                transport += 1
                transport_streak += 1
                if not targeted and transport_streak >= _ABORT_AFTER_TRANSPORT:
                    logger.warning(
                        f"[music-scraper] 连续 {transport_streak} 个文件是 MTW 故障，"
                        f"判为 MTW 整体不可用，本轮中止"
                    )
                    aborted = True
                    break
            else:
                transport_streak = 0
                if okf:
                    done[key] = {"mtime": mtime, "size": size, "ts": now_iso, "source": source}
                    failed.pop(key, None)
                    ok += 1
                    dirty += 1
                else:
                    prev = failed.get(key)
                    prev_retries = int(prev.get("retries", 0)) if isinstance(prev, dict) else 0
                    failed[key] = {
                        "mtime": mtime, "size": size,
                        "retries": prev_retries + 1,
                        "last_ts": now_iso, "reason": reason, "score": score,
                    }
                    done.pop(key, None)
                    failed_n += 1
                    dirty += 1

            # 定期落账：整轮动辄几小时，中途容器重启不该把已刮好的部分全丢掉。
            if dirty >= _SAVE_EVERY:
                self.save_data(_KEY_DONE, done)
                self.save_data(_KEY_FAILED, failed)
                dirty = 0

        # 清理：仅整盘扫描且未中断/未熔断时剔除磁盘上已消失的记账项，避免状态无限膨胀。
        # 熔断或提前退出时 live_keys 只覆盖了一部分目录，这时候 prune 会误删未走到的条目。
        if not targeted and not aborted and not self._stop_event().is_set():
            for k in list(done.keys()):
                if k not in live_keys:
                    done.pop(k, None)
            for k in list(failed.keys()):
                if k not in live_keys:
                    failed.pop(k, None)

        # 未处理到的文件（熔断/停止后剩下的）也算待处理：不并进来会让"待处理"在
        # 熔断那一轮直接报 0，看起来像库已经扫完了。
        not_attempted = max(0, len(files) - scanned)
        pending = (scanned - from_cache - skipped_tagged - ok
                   - failed_n - exhausted - transport) + not_attempted
        summary = {
            "ts": now_iso,
            "scanned": scanned,
            "from_cache": from_cache,
            "skipped_tagged": skipped_tagged,
            "ok": ok,
            "failed": failed_n,
            "exhausted": exhausted,
            "transport": transport,
            "aborted": aborted,
            "pending": max(0, pending),
            "force": effective_force,
            "targeted": targeted,
            "cron": self._cron or _DEFAULT_CRON,
            "duration_s": round(time.time() - started, 1),
        }
        self.save_data(_KEY_DONE, done)
        self.save_data(_KEY_FAILED, failed)
        self.save_data(_KEY_RUNS, summary)
        logger.info(
            f"[music-scraper] 完成：命中缓存 {from_cache}，已有tag {skipped_tagged}，"
            f"成功 {ok}，失败 {failed_n}，超限跳过 {exhausted}，MTW 故障 {transport}"
            f"{'（本轮熔断中止，未处理的下轮再来）' if aborted else ''}"
            f"（{summary['duration_s']}s）"
        )

    def __is_complete(self, path: Path) -> bool:
        """补空模式的“够格”判据：title + artist + album + 内嵌封面。

        只判 title/artist 会让已刮齐标签、但缺封面的文件永远被跳过，而那恰恰是
        真实音乐库最常见剩下的缺口（MP 写入链路会把 cover_url 下载后内嵌）。
        """
        tags = _read_music_tags(path)
        return bool(
            tags["title"] and tags["artist"] and tags["album"] and tags["has_cover"]
        )

    def __scrape_file(self, path: Path) -> Tuple[bool, str, Optional[int], str]:
        """处理单个文件，返回 (是否成功, 失败原因, 命中分数, 命中来源)。"""
        candidates, failure = self._mtw_fetch_candidates(path)
        if failure:
            # MTW 侧故障：不能算这个文件的锅，调用方按原因前缀区分后不记 retries。
            return False, failure, None, ""
        if not candidates:
            return False, "MTW 无候选", None, ""
        picked, score = self._mtw_pick_best(path, candidates)
        if not picked:
            return False, f"未达阈值（最低匹配分数 {self._min_score}）", score, ""
        source = str(picked.get("resource") or "").strip()
        info = self._candidate_to_musicinfo(picked, path)
        if not info:
            return False, "候选信息不完整，无法转换", score, source
        try:
            fileitem = schemas.FileItem(
                type="file",
                name=path.name,
                path=str(path),
                extension=path.suffix.lstrip("."),
                size=path.stat().st_size if path.exists() else 0,
            )
        except OSError:
            fileitem = schemas.FileItem(
                type="file", name=path.name, path=str(path),
                extension=path.suffix.lstrip("."), size=0,
            )
        ok, msg = _MusicScrapeChain().scrape_metadata(
            fileitem=fileitem,
            mediainfo=info,
            overwrite=self._force_all,
        )
        if ok:
            logger.info(f"[music-scraper] 刮削成功: {path.name} → {info.title}（{source or 'MTW'}）")
            return True, "", score, source
        logger.warning(f"[music-scraper] 刮削失败: {path.name} — {msg}")
        return False, f"写入失败：{msg}", score, source

    # ---------------- 查询辅助（供 status/files） ----------------

    def _build_status(self) -> Dict[str, Any]:
        """组装状态汇总：runs + 分桶计数 + 配置回显（不含密码）+ 最近失败。"""
        done = self._load_done()
        failed = self._load_failed()
        runs = self._load_runs()
        retryable = sum(1 for v in failed.values() if int(v.get("retries", 0)) < self.MAX_RETRY)
        exhausted = len(failed) - retryable
        recent_failed = sorted(
            ({"path": k, **(v if isinstance(v, dict) else {})} for k, v in failed.items()),
            key=lambda r: r.get("last_ts") or "",
            reverse=True,
        )[:10]
        return {
            "enabled": self._enabled,
            "running": self._is_running(),
            "max_retry": self.MAX_RETRY,
            "runs": runs,
            "counts": {
                "done": len(done),
                "failed": len(failed),
                "retryable": retryable,
                "exhausted": exhausted,
                "pending_unknown": runs.get("pending", 0),
            },
            "config": {
                "cron": self._cron or _DEFAULT_CRON,
                "mode": "force_all" if self._force_all else "fill_empty",
                "scraper_paths": self._scraper_paths,
                "exclude_paths": self._exclude_paths,
                "mtw_base_url": self._mtw_base_url,
                "mtw_resource": self._mtw_resources,
                "min_score": self._min_score,
                "has_username": bool(self._mtw_username),
                # 绝不回传密码。
            },
            "recent_failed": recent_failed,
        }

    def _collect_rows(self, bucket: str) -> List[Dict[str, Any]]:
        """把某桶整理成表格行 {path,name,mtime,size,status,reason,retries,score,ts}。"""
        rows: List[Dict[str, Any]] = []
        if bucket == "failed":
            failed = self._load_failed()
            for path, v in failed.items():
                v = v if isinstance(v, dict) else {}
                rows.append({
                    "path": path, "name": Path(path).name,
                    "mtime": v.get("mtime"), "size": v.get("size"),
                    "status": "failed", "reason": v.get("reason"),
                    "retries": v.get("retries"), "score": v.get("score"),
                    "ts": v.get("last_ts"),
                })
        elif bucket == "pending":
            done = self._load_done()
            failed = self._load_failed()
            known = set(done) | set(failed)
            for fp in self._enumerate_files():
                key = str(fp)
                if key in known:
                    continue
                try:
                    st = fp.stat()
                    mtime, size = int(st.st_mtime), st.st_size
                except OSError:
                    mtime, size = None, None
                rows.append({
                    "path": key, "name": fp.name,
                    "mtime": mtime, "size": size,
                    "status": "pending", "reason": None,
                    "retries": None, "score": None, "ts": None,
                })
        else:  # done
            done = self._load_done()
            for path, v in done.items():
                v = v if isinstance(v, dict) else {}
                rows.append({
                    "path": path, "name": Path(path).name,
                    "mtime": v.get("mtime"), "size": v.get("size"),
                    "status": "done", "reason": None,
                    "retries": None, "score": None,
                    "ts": v.get("ts"), "source": v.get("source"),
                })
        return rows

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
        resp = RequestUtils(headers={"Content-Type": "application/json"}).post_json(
            url=f"{self._mtw_base_url}/apimt/token/",
            json={"username": self._mtw_username, "password": self._mtw_password},
        )
        if not resp or not resp.get("token"):
            # 不回显响应体，避免 MTW 的错误信息里夹带的敏感内容进日志。
            logger.error("[music-scraper] MTW JWT 获取失败，检查地址与账密")
            return None
        self._token = resp["token"]
        self._token_expiry = _token_expiry_at(resp["token"], resp)
        return self._token

    def _mtw_drop_token(self):
        """丢掉缓存的令牌，下次请求重新登录。"""
        self._token = None
        self._token_expiry = 0.0

    def _mtw_request(self, endpoint: str, body: Dict[str, Any],
                     auth_retry: bool = True) -> Tuple[Any, Optional[str]]:
        """POST MTW 的 /apimt/<endpoint>/，返回 (响应体, 故障原因)。

        故障原因非空表示这次请求没能给出结论（不可达 / 鉴权 / 接口报错），调用方不能把它
        当成"这个文件查不到"。鉴权失败会强制重登一次再重试，覆盖 MTW 换密钥、轮换 JWT
        这些让缓存令牌提前失效的情况。
        """
        resp = RequestUtils(
            headers={"Content-Type": "application/json", **self._mtw_headers()},
        ).post_json(url=f"{self._mtw_base_url}/apimt/{endpoint}/", json=body)
        reason = _mtw_failure_reason(resp)
        if reason == _AUTH_REASON and auth_retry:
            self._mtw_drop_token()
            if self._mtw_token():
                return self._mtw_request(endpoint, body, auth_retry=False)
        return resp, reason

    def _mtw_fetch_candidates(self, path: Path) -> Tuple[List[dict], Optional[str]]:
        """按配置的源依次向 MTW 拉候选并合并，返回 (候选, MTW 侧故障原因)。

        默认全源：单源 top10 经常整批是翻唱/钢琴版（实测 min_score=3 只能过 57%），
        换源比放宽阈值安全。某源已经出过满分候选就不再往下问，控制请求量。
        某个源挂了不影响其它源；只有全部候选为空且期间确有请求失败时才回报故障原因，
        这时"查不到"其实说不准是曲库没有还是 MTW 不行，不能记在文件头上。
        """
        identity = _local_identity(path)
        if not identity["title"]:
            return [], None
        want = {
            "title": identity["title"],
            "artist": identity["artist"],
            "album": identity["album"],
            "duration": identity["duration"],
        }
        merged: List[dict] = []
        failure: Optional[str] = None
        best_score = 0
        for resource in _normalize_resources(self._mtw_resources):
            # MTW 前端契约：title/artist/album 取自本地 tag，full_path 仅作参考
            body = {
                "resource": resource,
                "title": identity["title"],
                "artist": identity["artist"],
                "album": identity["album"],
                "full_path": str(path),
            }
            resp, reason = self._mtw_request("fetch_id3_by_title", body)
            if reason:
                failure = reason
                continue
            data = resp.get("data") if isinstance(resp, dict) else None
            if isinstance(data, dict) and "data" in data:
                data = data["data"]
            if not isinstance(data, list):
                continue
            for c in data:
                if not isinstance(c, dict):
                    continue
                # 以本插件的请求参数为准标记来源，MTW 回什么都不信
                c["resource"] = resource
                merged.append(c)
                if best_score < _MAX_SCORE:
                    best_score = max(best_score, self._score_candidate(c, want))
            if best_score >= _MAX_SCORE:
                break
        if not merged and failure:
            return [], failure
        return merged, None

    # ---------------- 候选匹配 & 字段映射 ----------------

    def _mtw_pick_best(self, path: Path, candidates: List[dict]) -> Tuple[Optional[dict], int]:
        identity = _local_identity(path)
        want = {
            "title": identity["title"],
            "artist": identity["artist"],
            "album": identity["album"],
            "duration": identity["duration"],
        }
        best, best_score = None, 0
        for c in candidates:
            score = self._score_candidate(c, want)
            if score > best_score:
                best, best_score = c, score
        return (best if best_score >= self._min_score else None), best_score

    @staticmethod
    def _score_candidate(c: dict, want: dict) -> int:
        """MTW utils.match_song 简化版：title / artist / album 各 0/1。"""
        score = 0
        want_title = _norm_text(want.get("title"))
        got_title = _norm_text(c.get("name") or c.get("title"))
        if want_title and got_title and (want_title in got_title or got_title in want_title):
            score += 1
        artists_raw = c.get("artists") or c.get("artist") or c.get("singer") or []
        if isinstance(artists_raw, (list, tuple)):
            got_artist = _norm_text(" ".join(
                str(a.get("name") if isinstance(a, dict) else a) for a in artists_raw
            ))
        else:
            got_artist = _norm_text(artists_raw)
        want_artist = _norm_text(want.get("artist"))
        if want_artist and got_artist and (want_artist in got_artist or got_artist in want_artist):
            score += 1
        album_obj = c.get("album") or {}
        if isinstance(album_obj, dict):
            got_album = _norm_text(album_obj.get("name") or album_obj.get("title"))
        else:
            got_album = _norm_text(album_obj)
        want_album = _norm_text(want.get("album"))
        if want_album and got_album and (want_album in got_album or got_album in want_album):
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
        source = str(c.get("resource") or "").strip()

        # MusicInfo 的 type / artist 不是 init 参数（type 是 init=False 派生字段，
        # 艺人统一走 artists: list[str]）；media_source 枚举里没有 netease/qmusic，
        # 留着不填，来源与歌曲 id 记在 raw_data 里。
        return MusicInfo(
            music_type="recording",
            title=title,
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
                "artists": "remote",
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
