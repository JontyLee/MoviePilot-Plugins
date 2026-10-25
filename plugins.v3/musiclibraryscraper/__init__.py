"""
musiclibraryscraper —— MoviePilot v3 插件（插件名：TagFlow音乐库刮削）

周期扫描音乐目录，为每个文件检索元数据，再沿用 MP 原生的音乐刮削链（ScrapingChain）写入
标签与封面。元数据来自插件直连的各平台公开检索接口（网易云 / QQ音乐 / 酷狗 / 咪咕 /
MusicBrainz，构造与解析在 sources.py）：不需要额外部署服务，也没有第二个后端可切。

一个文件从被扫到写标签，中间过四道判断，每道各由一个配置项决定：
1. 要不要处理它 —— 宿主音频白名单 ∩ 插件支持列表、realpath 必须落在库内（指向库外的软链不碰）、
   排除目录整棵剪掉；
2. 拿什么身份去问 —— 优先读文件已有的 tag，缺哪项就按 艺人/专辑 (年)/NN - 曲名 的目录结构兜底，
   只认严格位于音乐库之下的目录；发给平台的只有清洗后的「曲名-艺人」检索词，库内路径不出本机；
3. 候选能不能用 —— 曲名 / 艺人 / 专辑 各 1 分（繁简与大小写归一后互相包含即算命中），按配置的
   源顺序逐源问、某源出到 3/3 满分即停，最高分仍低于 min_score 就不写入；
4. 要不要覆盖 —— 只补缺（四项齐全则按 mtime+size 记账跳过）或强制覆盖，最终取插件与宿主
   「设置 → 媒体刮削」里更强的一方；歌词默认交给宿主的 LyricsChain。

几条一直成立的判据，动代码前先读：
- 「服务侧没给答案」和「曲库里查不到」是两件事。源连不上、被风控、响应形状不认识，都不写失败账、
  不烧 retries，连续出现则熔断；只有源正常答了而候选不达标才记在文件头上。账本里存量的旧文案
  （MTW 那三条）仍留在 _TRANSPORT_REASONS 里，否则升级后同一类失败会被拆成两种记账方式。
- 写标签不可回滚，所以 min_score 在服务端收敛到 1-3、路径必须落在库内、宁可不匹配也不猜身份
  （目录兜底猜不出来就留空，让它判「未达阈值」）。
- 扫描只有一个入口：周期任务、/scan、/retry、「立即运行一次」都走带互斥的 guarded 入口。两本账
  并发读写，后写完的会把先写的整本盖掉。
- 单个文件崩掉不能带走整轮：宿主 ScrapingChain 外面包了一层，异常既不记成功也不记失败，
  下一轮照原样再来。
- update_config 是整体替换，任何写回配置的入口都要带齐全部键（见 _config_snapshot）。

目录名 / 类名 / 配置前缀 / 各项键名从 v0.1 起没动过：改它们会让已存配置与已积累的状态账一起失效，
所以更名只发生在展示层。界面文案之外的判据细节见 docs/ 的使用说明，逐项变更历史见
package.v3.json 的 history。
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

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

# 识别源：端点与候选解析在 sources.py，本模块只负责发请求（沿用 RequestUtils 那一条缝）
from . import sources as _sources

try:  # 宿主较新版本才有 SDK 调度门面；缺失时回退到后台线程
    from app.sdk import scheduler as scheduler_sdk
except Exception:  # pragma: no cover - 视宿主版本而定
    scheduler_sdk = None


# MP 支持的音乐扩展名，独立于视频扩展名白名单。
_MUSIC_EXT = (".flac", ".mp3", ".m4a", ".ape", ".wav", ".wv", ".tta",
              ".aif", ".aiff", ".opus", ".ogg", ".mpc", ".dsf", ".dff", ".mka")
# 与宿主音频白名单求交后的结果缓存（见 _supported_audio_ext）。
_AUDIO_EXT_TTL = 300.0
_AUDIO_EXT_CACHE: Dict[str, Any] = {"ts": 0.0, "exts": None}

# done/failed/runs 状态账的 plugindata key。
_KEY_DONE = "done"
_KEY_FAILED = "failed"
_KEY_RUNS = "runs"
# 定向重试（手动点某一行/批量重试）的汇总单独存这里：它只覆盖几个文件，混进 runs
# 会把整盘汇总刷成 scanned=1、pending=0，看起来像库已经扫完了。
_KEY_TARGETED_RUNS = "last_targeted"
# 默认 cron：每周日 03:00。星期用名字而不是数字：宿主把这段字符串交给 APScheduler
# （CronTrigger.from_crontab），它的编号是 0=周一…6=周日，和 crontab 的 0/7=周日 差一位，
# 而且不认 7。"0 3 * * 0" 在这个调度器上是周一，写名字才没有歧义。
_DEFAULT_CRON = "0 3 * * sun"
# _score_candidate 的满分：曲名 / 艺人 / 专辑 各 1 分。
_MAX_SCORE = 3
# 打分的三项，顺序即失败原因里的表述顺序。
_MATCH_FIELDS = (("title", "曲名"), ("artist", "艺人"), ("album", "专辑"))
# 逐项判定的三种状态：对上 / 有得比但没对上 / 本地缺这项（无从比起）。
_HIT = "hit"
_MISS = "miss"
_NO_LOCAL = "no_local"
# 识别源：sources.py 里那五个平台的公开匿名检索接口，顺序即默认查询顺序。
# qq / spotify / acoustid / all / kuwo / smart_tag 都不在其中 —— 前四个恒返回空候选或不是
# 文本检索源，酷我答不出专辑，smart_tag 没有对外接口（详见 sources.py 的模块说明）。
_DEFAULT_RESOURCES = _sources.RESOURCES

# "服务侧没给答案"的原因：连不上、被风控、响应形状不认识。这类失败说明插件对这个文件
# 什么都没学到，因此不计入 MAX_RETRY、也不写 failed 账：否则平台限流一次就给整库每个文件
# 记一笔失败，连错满 MAX_RETRY 轮后全体永久跳过，mtime+size 不变就再也刷不动。
_UNREACHABLE_REASON = "识别源不可达"
# 旧版本的同类文案留在判据里：账本存的是写下来那一刻的字符串，只认新名字会把升级前记下的
# 故障重新当成文件失败去烧重试额度。
_LEGACY_TRANSPORT_REASONS = ("MTW 不可达", "MTW 鉴权失败", "MTW 接口报错", "内置识别源不可达")
_TRANSPORT_REASONS = (_UNREACHABLE_REASON,) + _LEGACY_TRANSPORT_REASONS
# 无候选是确定答案「曲库里查不到」，与上面相反，要按文件失败记账。
_NO_CANDIDATE_REASON = "识别源无候选"
# v0.6.x 之前那几个 MTW 后端的配置键。识别源全部由插件直连平台之后它们不再被读取，
# 只用来在升级后说明一次「你原来配的地址与账密去哪了」；下次保存配置（整体替换）就把它们清掉。
_LEGACY_CONFIG_KEYS = ("source_backend", "mtw_base_url", "mtw_username", "mtw_password")
# 熔断：连续多少个文件栽在「插件什么都没学到」上就中止本轮，别在平台风控的时段空打上千次请求。
_ABORT_AFTER_TRANSPORT = 5
# 单次检索的超时（秒）。平台接口比自建服务更容易慢（限流时干脆挂着），而每个文件最多问五个源，
# 这个值直接决定一轮卡住时能白等多久。
_SEARCH_TIMEOUT = 10.0
# 扫描中途落账的最小间隔（秒）：整轮动辄数小时，中途容器重启不该把进度全丢掉。
# 按时间而不是按变更次数：plugindata 是单行 JSON，每次 save 都要把整本账重新序列化，
# 固定"每 25 次落一次"在 5 万条目的库里会打出上千次全量写。
_FLUSH_INTERVAL = 60.0
# /files?bucket=pending 的结果缓存时长（秒）：该桶要全库枚举，搜索框每停顿一下就会发一次请求。
_PENDING_TTL = 30.0

# 配置页底部的一段集中说明：来源、查询与选取规则、两档写入模式、宿主策略叠加。
# 不跟在单个配置项下面，选项本身只留最短的标签，细节全部收在这里。
# 这里是「原生表单」那一份，宿主插件管理页用的就是它，判据必须自足；
# Vue 配置页只留短句，长文在 docs/musiclibraryscraper.md。
_FORM_NOTES = (
    "【识别源】插件直连 网易云 / QQ音乐 / 酷狗 / 咪咕 / MusicBrainz 的公开检索接口，不必部署"
    "任何服务；多选留空即全部源。发给平台的只有清洗后的「曲名-艺人」，库内路径不出本机。\n"
    "【怎么算命中】曲名 / 艺人 / 专辑 各 1 分（繁简与大小写归一、互相包含即算命中），按配置顺序"
    "逐源查询、某源出到 3/3 满分即停；都没满分就合并全部候选取最高分，同分时按源顺序取先答的那一个。"
    "最高分低于「最低匹配分数」（默认 3，即三项全中）就记失败、不写入。分数只在（1-3）之间生效，"
    "越界按最近的一界执行；调到 1-2 表示只对上曲名或艺人也照样改写标签，而标签写进文件、无法回滚。\n"
    "【本地身份从哪来】优先读文件已有的标签，缺哪项就按 艺人/专辑 (年)/NN - 曲名 的目录结构兜底；"
    "只有严格位于音乐库路径之下的目录才算艺人/专辑，库根本身的名字不参与猜测 —— 猜不出来就留空，"
    "宁可判「未达阈值」，也不拿目录名凑一个假身份去改写标签。\n"
    "【写入模式】只补缺：曲名+艺人+专辑+内嵌封面四项齐全的文件按 mtime+size 跳过，缺哪项补哪项；"
    "强制覆盖：每轮忽略增量账、全库重扫，手工改过的标签会被替换（候选里没有的字段不会清空）。"
    "最终是否覆盖取这里与宿主「设置 → 媒体刮削」音乐三项策略中较强的一方；歌词默认交给宿主。\n"
    "【会处理的文件】宿主音频白名单与本插件支持列表的交集；Jellyfin 的 .trickplay 缓存目录整棵"
    "跳过，指向库外的软链不碰（写标签是按路径落到真实文件的）。\n"
    "【侧栏入口】勾上并启用插件后，主界面「整理」下出现本页，逐文件核对与补齐缺项都在那里做。"
    "逐项判据、失败归因与各源现状见仓库内 docs/musiclibraryscraper.md。"
)

# 各容器格式的 tag key：ID3 帧名 / MP4 atom / Vorbis 注释 / APEv2。
_TAG_KEYS = {
    "title": ("TIT2", "\u00a9nam", "title", "TITLE"),
    "artist": ("TPE1", "\u00a9ART", "artist", "ARTIST"),
    "album": ("TALB", "\u00a9alb", "album", "ALBUM"),
    "year": ("TDRC", "\u00a9day", "date", "YEAR"),
    "tracknumber": ("TRCK", "trkn", "tracknumber", "TRACK"),
}
# 内嵌封面的 key 形状（各容器读法不同，见 _has_embedded_cover）：
# MP4 atom / Vorbis 注释按全名匹配，ID3 的 APIC 帧与 APEv2 的 Cover Art 按前缀匹配。
_COVER_KEYS = ("covr", "picture", "metadata_block_picture")
_COVER_KEY_PREFIXES = ("apic", "cover art")
# 内嵌歌词的 key 形状（各容器读法不同，见 _has_embedded_lyrics）：
# MP4 atom / Vorbis 注释按全名匹配，ID3 的 USLT 帧与 APEv2 的 Lyrics 按前缀匹配。
_LYRIC_KEYS = ("lyrics", "\u00a9lyr")
_LYRIC_KEY_PREFIXES = ("uslt", "lyrics")
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


def _resource_tokens(value: Any) -> List[str]:
    """把识别源配置拆成去空白后的字符串项（不判断认不认识）。

    这一项从单选改成过多选，库里可能还存着旧的单字符串（"netease"），也可能被前端写成
    逗号/分号分隔的串。直接对字符串迭代拿到的是字符，所以任何入口都必须先过这里。
    """
    if isinstance(value, str):
        raw: List[Any] = value.replace(",", ";").split(";")
    elif isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        raw = []
    return [str(item).strip() for item in raw if item is not None and str(item).strip()]


def _resources_are_listed(value: Any) -> bool:
    """配置里到底点没点源：决定回写要不要保住「留空」这个状态。

    把留空展开成全部源再写回去，等于替用户显式选中了这五个 —— 以后插件再加一个源，
    这份装机永远不会自动去问它，而界面上看是一片空白，没人会想到是某次保存写死的。
    """
    return bool(_resource_tokens(value))


def _normalize_resources(value: Any, warn: bool = False) -> List[str]:
    """把识别源配置归一成去重保序、且只含已知源的列表；空值回落到「全部源」。

    未知值必须在这里就丢掉：一个必然报错的源（旧配置残留的 kuwo / smart_tag、或 API 写坏的
    数组）会让每个文件都多打一次失败请求，而拉候选那边会把「有源没答」当成服务侧故障 ——
    结果是整轮在前几个文件就熔断，全库永远停在待处理。

    整份配置恰好只剩被忽略的那几个时，退回全部源而不是交出一个空名单：那会被记成
    「曲库里查不到」，而真实的故障在插件自己的配置里。
    """
    out: List[str] = []
    unknown: List[str] = []
    tokens = _resource_tokens(value)
    for name in tokens:
        if name in out:
            continue
        if name in _DEFAULT_RESOURCES:
            out.append(name)
        elif name not in unknown:
            unknown.append(name)
    if unknown and warn:
        logger.warning(
            f"[music-scraper] 忽略不认识的识别源 {'、'.join(unknown)}"
            f"（可用：{'、'.join(_DEFAULT_RESOURCES)}）"
        )
    if tokens and not out and warn:
        logger.warning("[music-scraper] 配置的识别源没有一个可用，已改用全部识别源")
    return out or list(_DEFAULT_RESOURCES)


def _host_audio_extensions() -> Optional[List[str]]:
    """宿主写入链实际接受的音频扩展名（设置里的 RMT_AUDIOEXT，可在界面上改）。

    ScrapingChain 在写标签前会按这份白名单过滤文件，不在里面的直接返回
    "刮削路径中没有支持的音频文件"。插件要先看它一眼，否则每个不收的扩展名都要
    先白打满全部识别源、再被记一次刮削失败，烧满 retries 才永久跳过。

    读不到（宿主改版、配置还没加载）与"读到了但一个都不收"必须分开：前者只能按插件
    自己的列表扫，后者是真的什么都不该扫。两者都返回空列表时，调用方无法区分，
    要么把白名单清空变成硬扫全库、要么把可用列表误判成空。None 表示读不到。
    """
    exts: Any = None
    try:
        from app.application.configuration import get_chain_runtime_config_snapshot

        exts = get_chain_runtime_config_snapshot().audio_extensions
    except Exception:
        try:
            exts = getattr(settings, "RMT_AUDIOEXT", None)
        except Exception:
            exts = None
    if exts is None:
        return None
    out: List[str] = []
    if isinstance(exts, str):
        exts = exts.split(",")
    for item in exts or []:
        name = str(item).lower().strip()
        if not name:
            continue
        out.append(name if name.startswith(".") else f".{name}")
    return out


def _supported_audio_ext() -> Tuple[str, ...]:
    """本插件真正会处理的扩展名：本地候选列表 ∩ 宿主白名单。

    读不到宿主配置时按本地列表走（此时可能包含宿主不收的扩展名，功能不受影响、
    只是那几个文件会记成失败）。宿主明确一个都不收时返回空集，不能退回去按本地列表硬扫。

    结果缓存 5 分钟：宿主那份配置快照是按需构建的，而 /retry 上千个路径、
    _enumerate_files 的每个文件都要问一次"这个扩展名收不收"；白名单不是每秒钟都在
    变的东西，改了最迟下一轮生效就够。测试把 _AUDIO_EXT_CACHE["ts"] 置 0 即可强制重算。
    """
    now = time.time()
    cached = _AUDIO_EXT_CACHE["exts"]
    if cached is not None and now - _AUDIO_EXT_CACHE["ts"] < _AUDIO_EXT_TTL:
        return cached
    host = _host_audio_extensions()
    if host is None:
        exts = _MUSIC_EXT
    else:
        host_set = set(host)
        exts = tuple(ext for ext in _MUSIC_EXT if ext in host_set)
        if not exts:
            logger.warning(
                "[music-scraper] 宿主音频白名单里没有本插件认识的格式，本轮不会处理任何文件"
                f"（宿主收：{'、'.join(host) or '（空）'}）"
            )
    _AUDIO_EXT_CACHE["exts"] = exts
    _AUDIO_EXT_CACHE["ts"] = now
    return exts


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


def _has_embedded_cover(audio: Any, tags: Any) -> bool:
    """容器里是否已经内嵌封面。

    不能只按帧名 `tags.get("APIC")` 判 ID3：APIC 是可重复帧，mutagen 取这类帧的口径是
    `getall("APIC")`（键还可能带描述）；APEv2 的封面 key 是 "Cover Art (Front)"，
    根本不在按帧名罗列的那份表里。判错的代价不对称 —— 已经有封面的文件被判成没封面，
    补空模式就永远认为它"没刮齐"：每轮重打满全部识别源，还因为候选不达标白涨
    retries，几轮之后一个本来完整的文件被永久列成超限。
    """
    # mutagen 的通用图片接口：文件对象（FLAC/OGG/MP4/APE…）和 tag 容器（ID3）两处都可能暴露
    for holder in (audio, tags):
        try:
            if getattr(holder, "pictures", None):
                return True
        except Exception:
            continue
    try:
        if tags.getall("APIC"):
            return True
    except Exception:
        pass
    try:
        keys = list(tags.keys())
    except Exception:
        return False
    for key in keys:
        name = str(key).lower()
        if not (name.startswith(_COVER_KEY_PREFIXES) or name in _COVER_KEYS):
            continue
        try:
            if tags.get(key):
                return True
        except Exception:
            continue
    return False


def _has_embedded_lyrics(audio: Any, tags: Any) -> bool:
    """容器里是否已经内嵌歌词。

    判据口径同 _has_embedded_cover：ID3 的 USLT 是可重复帧，只能 getall("USLT")。
    侧车 .lrc 不算内嵌，歌词是否缺失的合并判定放在缺陷清单层做。
    """
    try:
        if tags.getall("USLT"):
            return True
    except Exception:
        pass
    try:
        keys = list(tags.keys())
    except Exception:
        return False
    for key in keys:
        name = str(key).lower()
        if not (name.startswith(_LYRIC_KEY_PREFIXES) or name in _LYRIC_KEYS):
            continue
        try:
            if tags.get(key):
                return True
        except Exception:
            continue
    return False


def _candidate_has_lyric(c: dict) -> bool:
    """候选是否已带可用歌词，判据口径同 _candidate_to_musicinfo。"""
    raw = c.get("lyrics") or c.get("lyric") or c.get("lrc")
    return bool(isinstance(raw, str) and raw.strip() and not raw.startswith("未找到"))


def _is_persistable_lyric(raw: Any) -> bool:
    """能否把这份歌词文本写进文件 tag，判据口径同 _candidate_has_lyric。"""
    return bool(isinstance(raw, str) and raw.strip() and not raw.startswith("未找到"))


def _read_lyric_text(tags: Any) -> str:
    """取容器里第一条能用的内嵌歌词文本，判据口径同 _has_embedded_lyrics。

    只有补齐缺项那个弹层要它：那里得让人看见文件里已有的歌词再决定改不改，
    而扫描链路上每一遍都只要"有没有"，取文本是白读一遍大字段。
    """
    try:
        for frame in tags.getall("USLT") or []:
            text = getattr(frame, "text", None)
            if isinstance(text, str) and text.strip():
                return text.strip()
    except Exception:
        pass
    try:
        keys = [key for key in tags.keys()
                if str(key).lower().startswith(_LYRIC_KEY_PREFIXES)
                or str(key).lower() in _LYRIC_KEYS]
    except Exception:
        return ""
    return _first_tag_value(tags, keys)


def _read_music_tags(path: Path, with_lyrics_text: bool = False) -> Dict[str, Any]:
    """读取本地音乐 tag。

    MP 的 MetaInfoPath 对音频返回 MetaVideo（没有 artist/album），不能用于音乐判定，
    因此这里直接用 mutagen。失败时返回空值，由调用方按“缺 tag”处理；
    unreadable 单独标出来，供"写入前后各读一次算差集"那条链路分辨"文件真没 tag"
    与"这次根本没读出来" —— 后者算出来的差集会把所有字段说成被清空。
    """
    out: Dict[str, Any] = {
        "title": "", "artist": "", "album": "", "year": "", "tracknumber": "",
        "duration": None, "has_cover": False, "has_lyrics": False,
        "lyrics": "", "unreadable": False,
    }
    try:
        from mutagen import File as MutagenFile

        audio = MutagenFile(str(path))
    except Exception:
        out["unreadable"] = True
        return out
    if audio is None:
        out["unreadable"] = True
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
        out["has_cover"] = _has_embedded_cover(audio, tags)
        out["has_lyrics"] = _has_embedded_lyrics(audio, tags)
        if with_lyrics_text and out["has_lyrics"]:
            out["lyrics"] = _read_lyric_text(tags)
    try:
        info = getattr(audio, "info", None)
        length = getattr(info, "length", None) if info else None
        if length:
            out["duration"] = int(length)
    except Exception:
        pass
    return out


_EDIT_FIELDS = ("title", "artist", "album", "year", "tracknumber", "lyrics")

# 一次写入真正落到文件的改动。done 账里存的就是这份，数据页点开一行看的也是它 ——
# "打算写什么"和"文件里现在是什么"是两件事，宿主刮削策略可能把覆盖拦下来。
_DIFF_TEXT_FIELDS = ("title", "artist", "album", "year", "tracknumber")
# 文本字段的前后值各截到这里：账本是单行 JSON，整段歌词或一长串 feat. 艺人会把它撑肥。
_DIFF_VALUE_MAX = 120
# 封面与歌词只记有无，理由同上。
_DIFF_MARK_FIELDS = (("cover", "has_cover"), ("lyrics", "has_lyrics"))


def _clip_diff_value(value: str) -> str:
    return value if len(value) <= _DIFF_VALUE_MAX else f"{value[:_DIFF_VALUE_MAX]}…"


def _sibling_lyric(path: Path) -> str:
    """同名 .lrc 的文本（没有或读不出返回空）。

    补齐弹层要把它当"已经有了"填进去：歌词在旁边的 .lrc 里，和文件里没写歌词是两件
    事 —— 不给出这个值，用户只能自己另开文件复制一遍。
    """
    lrc = path.with_suffix(".lrc")
    if not lrc.is_file():
        return ""
    for encoding in ("utf-8", "gb18030", "latin-1"):
        try:
            text = lrc.read_text(encoding=encoding)
        except (OSError, UnicodeDecodeError):
            continue
        return text.strip()
    return ""


def _tag_diff(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    """写入前后两次 tag 读取的差集：{字段: [改前, 改后]}，没变的字段一个都不记。

    任何一侧读不出来（没装 mutagen、容器不认识）就当"看不出改动"返回空：把读失败
    算成"所有字段都被清空"会让整库凭空多出一批改写记录。
    """
    if not before or not after or before.get("unreadable") or after.get("unreadable"):
        return {}
    out: Dict[str, List[str]] = {}
    for field in _DIFF_TEXT_FIELDS:
        was = str(before.get(field) or "").strip()
        now = str(after.get(field) or "").strip()
        if was != now:
            out[field] = [_clip_diff_value(was), _clip_diff_value(now)]
    for field, key in _DIFF_MARK_FIELDS:
        was, now = bool(before.get(key)), bool(after.get(key))
        if was != now:
            out[field] = ["有" if was else "无", "有" if now else "无"]
    return out


def _edit_payload_fields(raw: Any) -> Tuple[Dict[str, str], bool]:
    """单项编辑字段收口：只认白名单，值一律转成去空白字符串。

    编辑是精确写文件的操作，静默丢掉一个字段比拒收更容易把人误导到
    「改成功了但没生效」上，所以未知字段与不合法歌词整体判 invalid；
    空值按「不改」跳过，清空不在本版能力里。
    """
    if not isinstance(raw, dict):
        return {}, True
    out: Dict[str, str] = {}
    for key, value in raw.items():
        field = str(key).strip().lower()
        if field not in _EDIT_FIELDS:
            return {}, True
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            return {}, True
        text = str(value).strip()
        if not text:
            continue
        if field == "lyrics" and not _is_persistable_lyric(text):
            return {}, True
        out[field] = text
    return out, False


def _write_music_tags(path: Path, fields: Dict[str, str]) -> bool:
    """按用户明确给出的字段写 tag，只改传入的字段，绝不碰封面。

    同 _read_music_tags 的口径直接用 mutagen：宿主的元数据链路是整包覆盖，
    单项编辑要的是一次只动一个字段。失败返回 False，由调用方报 write_failed，
    不把半写入状态说成成功。
    """
    try:
        from mutagen import File as MutagenFile

        audio = MutagenFile(str(path))
        if audio is None:
            return False
        if audio.tags is None:
            try:
                audio.add_tags()
            except Exception:
                return False
        tags = audio.tags
        from mutagen.id3 import ID3
        if isinstance(tags, ID3):
            _write_id3_tags(tags, fields)
        else:
            from mutagen.mp4 import MP4
            if isinstance(audio, MP4):
                _write_mp4_tags(tags, fields)
            else:
                try:
                    from mutagen.apev2 import APEv2
                    is_ape = isinstance(tags, APEv2)
                except Exception:
                    is_ape = False
                if is_ape:
                    _write_ape_tags(tags, fields)
                else:
                    _write_vcomment_tags(tags, fields)
        audio.save()
        return True
    except Exception:
        return False


def _write_id3_tags(tags, fields: Dict[str, str]) -> None:
    from mutagen.id3 import TALB, TDRC, TIT2, TPE1, TRCK, USLT

    frames = {"title": TIT2, "artist": TPE1, "album": TALB,
              "year": TDRC, "tracknumber": TRCK}
    for field, cls in frames.items():
        if field in fields:
            tags.setall(cls(encoding=3, text=fields[field]))
    if "lyrics" in fields:
        tags.setall(USLT(encoding=3, lang="XXX", desc="", text=fields["lyrics"]))


def _write_mp4_tags(tags, fields: Dict[str, str]) -> None:
    atoms = {"title": "©nam", "artist": "©ART", "album": "©alb", "year": "©day"}
    for field, atom in atoms.items():
        if field in fields:
            tags[atom] = [fields[field]]
    if "tracknumber" in fields:
        head = fields["tracknumber"].split("/")[0].strip()
        try:
            total = int(tags.get("trkn", [(0, 0)])[0][1])
        except (TypeError, ValueError, IndexError):
            total = 0
        tags["trkn"] = [(min(65535, max(0, int(head))), total)]
    if "lyrics" in fields:
        tags["©lyr"] = [fields["lyrics"]]


def _write_vcomment_tags(tags, fields: Dict[str, str]) -> None:
    keys = {"title": "TITLE", "artist": "ARTIST", "album": "ALBUM",
            "year": "DATE", "tracknumber": "TRACKNUMBER", "lyrics": "LYRICS"}
    for field, key in keys.items():
        if field in fields:
            tags[key] = fields[field]


def _write_ape_tags(tags, fields: Dict[str, str]) -> None:
    keys = {"title": "Title", "artist": "Artist", "album": "Album",
            "year": "Year", "tracknumber": "Track", "lyrics": "Lyrics"}
    for field, key in keys.items():
        if field in fields:
            tags[key] = fields[key]


def _path_identity(path: Path, roots: Sequence[Path] = ()) -> Dict[str, str]:
    """按 “艺人/专辑 (年)/NN - 曲名” 目录结构推断身份，供未打 tag 的文件参与匹配。

    roots 给定时只认严格落在库根之下的目录：库根自己以及它上面的层级不是专辑/艺人目录。
    不 clamp 的话 /media/music/01 - 轨迹.flac 会得到 album="music"、
    /media/music/周杰伦/01 - x.flac 会得到 artist="media" —— 这种凭空造出来的名字
    既会被当检索词发给平台，比对又是双向包含（want in got 也算命中），一个目录名
    就可能白送一个匹配分；把 min_score 放宽到 1-2 时，够格的文件会被按错艺人改标签，
    而写进文件的标签没法回滚。宁可留空判「未达阈值」，也不要假身份。
    """
    parents = list(path.parents)

    def _name(index: int) -> str:
        if len(parents) <= index:
            return ""
        dirpath = parents[index]
        if roots and not any(
            dirpath != root and dirpath.is_relative_to(root) for root in roots
        ):
            return ""
        return dirpath.name

    return {
        "title": _TRACK_PREFIX_RE.sub("", path.stem).strip(),
        "artist": _name(1).strip(),
        "album": _YEAR_SUFFIX_RE.sub("", _name(0)).strip(),
    }


def _local_identity(path: Path, tags: Optional[Dict[str, Any]] = None,
                    roots: Sequence[Path] = ()) -> Dict[str, Any]:
    """本地身份：tag 优先，缺失字段用目录结构兜底。

    各平台的检索接口都是拿曲名当关键词用的，传原始文件名
    （"01 - 轨迹.flac"）只会召回一堆噪音，必须先清洗成干净曲名。

    tags 由调用方传入时不再重复解析文件：mutagen 读一次不便宜，一轮里补空判定、
    拉候选、挑选三处都要用同一份结果。
    """
    if tags is None:
        tags = _read_music_tags(path)
    from_path = _path_identity(path, roots)
    return {
        "title": (tags["title"] or from_path["title"]).strip(),
        "artist": (tags["artist"] or from_path["artist"]).strip(),
        "album": (tags["album"] or from_path["album"]).strip(),
        "duration": tags["duration"],
    }


def _want_from(identity: Dict[str, Any]) -> Dict[str, Any]:
    """把本地身份收敛成匹配用的期望值，拉候选和挑最佳必须用同一份口径。"""
    return {
        "title": identity["title"],
        "artist": identity["artist"],
        "album": identity["album"],
        "duration": identity.get("duration"),
    }


class _MusicScrapeChain(ScrapingChain):
    """复用 libraryscraper 的 trickplay 排除策略，避免 Jellyfin 预览目录被递归。"""

    def scrape_metadata(self, fileitem, *args, **kwargs):
        if fileitem and MusicLibraryScraper._is_trickplay_path(Path(fileitem.path)):
            return False, "跳过 Jellyfin 预览缓存目录"
        return super().scrape_metadata(fileitem, *args, **kwargs)


class MusicLibraryScraper(_PluginBase):
    """扫描音乐库，按识别源检索候选，交由 MP ScrapingChain 完成写入。

    目录名 / 类名 / 配置前缀沿用 v0.2.x：MP 的 config_data 以 plugin_config_prefix 为键、
    plugindata 以类名为键，改它们会让已存配置和已积累的状态账一起失效，所以更名只动展示层。
    """

    plugin_name = "TagFlow音乐库刮削"
    plugin_desc = (
        "定时扫描音乐目录，元数据取自插件直连的各平台公开检索接口"
        "（网易云 / QQ音乐 / 酷狗 / 咪咕 / MusicBrainz），再沿用 MP 原生音乐刮削链路写入标签与封面；"
        "侧栏「整理」下常驻数据页，失败与有缺项逐文件可查可补。"
    )
    plugin_icon = "https://raw.githubusercontent.com/JontyLee/MoviePilot-Plugins/main/icons/tagflow.svg"
    plugin_version = "0.9.0"
    plugin_author = "JontyLee"
    author_url = "https://github.com/JontyLee"
    plugin_config_prefix = "musiclibraryscraper_"
    plugin_order = 8
    user_level = 1

    # 配置项
    _enabled: bool = False
    # 主界面「整理」分区的侧栏入口。默认开：这个插件的价值就在数据页，
    # 而数据页原本只能从插件管理页两层点进去。
    _show_sidebar_nav: bool = True
    _onlyonce: bool = False
    _cron: Optional[str] = None
    _scraper_paths: str = ""
    _exclude_paths: str = ""
    _force_all: bool = False
    # 本轮真正要问的源（配置留空即全部源）
    _active_resources: List[str] = list(_DEFAULT_RESOURCES)
    # 配置里到底点没点源：回写要保住「留空」，见 _resources_are_listed。
    # 默认 False（= 留空）：没配过源的安装就该回写成"全部源"，而不是替它点名五个。
    _resources_listed: bool = False
    _min_score: int = 3
    # 是否允许在本地已有/候选自带的歌词之外，主动去识别源拉歌词回填
    _fill_lyrics: bool = False

    # 失败自动重试上限：retries 达到该值后，定时扫描不再自动重试（仅手动重试）。
    MAX_RETRY: int = 5

    def init_plugin(self, config: Dict[str, Any] = None):
        # 每实例独立的扫描锁、运行标志与停止信号（都放 __dict__，避免类属性被多实例共享）。
        # 运行标志用 setdefault 而不是直接置 False：保存配置会重跑 init_plugin，
        # 那时把在跑的扫描标记成"没在跑"，就等于又开了一条并发写账本的口子。
        self.__dict__.setdefault("_scan_lock", threading.Lock())
        self.__dict__.setdefault("_scan_running", False)
        self.__dict__.setdefault("_stop_flag", threading.Event())
        self.__dict__.setdefault("_pending_cache", None)
        self.__dict__.setdefault("_deficient_cache", None)

        if config:
            self._enabled = bool(config.get("enabled"))
            # 缺键按开：升级过来的装机没有这一项，默认关会让入口"升级完就消失"，
            # 而它本来什么也没改变过，只是多了一个入口。
            self._show_sidebar_nav = bool(config.get("show_sidebar_nav", True))
            self._onlyonce = bool(config.get("onlyonce"))
            self._cron = config.get("cron") or None
            self._scraper_paths = config.get("scraper_paths") or ""
            self._exclude_paths = config.get("exclude_paths") or ""
            self._force_all = config.get("mode") == "force_all"
            # warn=True：只在配置入口告警一次，不要每个文件拉候选时都刷一遍日志。
            # 这一项改名为 resources（MTW 那套源名单已经不在了），读侧留一次兼容：
            # 老装机配过的多选名单不该因为改名整个退回全部源。
            raw_resources = config.get("resources") or config.get("mtw_resource")
            self._resources_listed = _resources_are_listed(raw_resources)
            self._active_resources = _normalize_resources(raw_resources, warn=True)
            self._min_score = self._clamp_min_score(config.get("min_score"))
            self._fill_lyrics = bool(config.get("fill_lyrics"))
            if any(key in config for key in _LEGACY_CONFIG_KEYS):
                logger.warning(
                    "[music-scraper] 识别源已全部由插件直连平台，不再支持 Music Tag Web 后端："
                    "配置里的 MTW 地址与账密将被忽略，下次保存配置时一并清掉"
                )

        # 勾选“立即运行一次”：回写配置关闭该开关，并投递一次性扫描。
        if self._onlyonce:
            self._onlyonce = False
            # update_config 是整体替换而不是合并（宿主 PluginInstanceOper.save_config_data
            # 直接 record.config_data = deepcopy(config_data)），所以必须把全量配置写回去，
            # 只写 {"onlyonce": False} 会把其余配置一起清掉。
            self.update_config(self._config_snapshot())
            self._kick_scan()

    def _config_snapshot(self) -> Dict[str, Any]:
        """当前配置的全量形状（键必须与 get_form 的 defaults 一致）。

        两处整体替换要用它：onlyonce 回写，以及侧栏配置对话框的 POST /config
        —— 那个对话框不走宿主「插件管理 → 配置」那条链路，落盘得插件自己来。
        """
        return {
            "enabled": self._enabled,
            "show_sidebar_nav": self._show_sidebar_nav,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "scraper_paths": self._scraper_paths,
            "exclude_paths": self._exclude_paths,
            "mode": "force_all" if self._force_all else "",
            # 配置键用复数，值一直是数组（读侧兼容旧的单字符串与旧的 mtw_resource 键）。
            # 用户没点名就回写空数组：把 _active_resources 那五个展开写回去，"留空=全部源"
            # 这条规则会在第一次回写时变成"显式选中这五个"，以后新增的源永远轮不到。
            "resources": list(self._active_resources) if self._resources_listed else [],
            "min_score": self._min_score,
            "fill_lyrics": self._fill_lyrics,
        }

    @staticmethod
    def _clamp_min_score(raw: Any) -> int:
        """最低匹配分数只接受 1.._MAX_SCORE，越界按最近的那一界执行并告警。

        配置页的 min/max 只是浏览器属性，原生表单和 /api 都能塞进任意值，而两头都不可逆：
        0 或负数会让"只对上曲名"的候选整库改写标签（写进文件就回不来）；
        大于满分则永远达不到阈值，整库都会被记成失败、烧满 retries 后永久跳过。
        0 与 1 行为本就等价（一分不中的候选根本选不出来），所以降到 1 不改变意图。
        """
        text = "" if raw is None else str(raw).strip()
        if not text:
            return _MAX_SCORE
        try:
            value = int(float(text))
        except (TypeError, ValueError):
            logger.warning(f"[music-scraper] min_score {raw!r} 不是数字，按默认 {_MAX_SCORE} 执行")
            return _MAX_SCORE
        if 1 <= value <= _MAX_SCORE:
            return value
        clamped = min(max(value, 1), _MAX_SCORE)
        logger.warning(
            f"[music-scraper] min_score {value} 超出 1-{_MAX_SCORE}，按 {clamped} 执行"
            f"（{_MAX_SCORE} = 曲名/艺人/专辑 全中）"
        )
        return clamped

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
                "desc": "音乐库刮削：查看运行状态与统计",
                "category": "音乐刮削",
                "data": {"action": "music_scraper_status"},
            },
            {
                "cmd": "/music_scraper_retry",
                "event": EventType.PluginAction,
                "desc": "音乐库刮削：重试全部失败项",
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

    def _reason_lines(self, stats: Dict[str, Any]) -> str:
        """失败原因分布的文本行（命令输出用）：最多列 3 类，其余并成一条计数。

        没有这段聚合的话，"阈值降一档能救回多少"只能靠一页页翻表格估。
        """
        groups = stats.get("failure_reasons") or []
        if not groups:
            return ""
        lines = [f"失败原因（最低匹配分数 {stats['config']['min_score']}）："]
        min_score = stats['config']['min_score']
        for item in groups[:3]:
            score = item.get("best_score")
            line = f"· {item['reason']} {item['count']} 个"
            if isinstance(score, int):
                line += f"，最高 {score} 分"
                if min_score - score > 0:
                    line += f"，阈值降 {min_score - score} 分可命中"
            if not item["retryable"]:
                line += "，均已超限不再自动重试"
            lines.append(line)
        rest = groups[3:]
        if rest:
            lines.append(f"· 其他 {len(rest)} 类共 {sum(g['count'] for g in rest)} 个")
        return "\n".join(lines) + "\n"

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
                f"{stats['runs'].get('failed', 0)}｜服务侧故障 "
                f"{stats['runs'].get('transport', 0)}｜处理异常 "
                f"{stats['runs'].get('error', 0)}\n"
                f"已处理 {stats['counts']['done']}｜待重试 "
                f"{stats['counts']['retryable']}｜超限 "
                f"{stats['counts']['exhausted']}｜待处理 "
                f"{stats['counts']['pending_unknown']}\n"
                + self._reason_lines(stats)
                + f"定时 {stats['config']['cron']}｜下次 {stats['config']['cron_next']}"
                + f"\n识别源 {'、'.join(stats['config']['resources']) or '未配置'}"
                + (
                    f"\n最近定向重试：{stats['last_targeted'].get('ts')}"
                    f"，成功 {stats['last_targeted'].get('ok', 0)}"
                    f"｜失败 {stats['last_targeted'].get('failed', 0)}"
                    if stats.get("last_targeted", {}).get("ts") else ""
                )
            ),
        )

    def _filter_retry_paths(self, paths: List[str]) -> Tuple[List[str], int]:
        """把外部给来的重试路径收敛到库内音频文件，返回 (可用路径, 被拒数量)。

        命令入口和 /retry 必须走同一套判据：否则 /music_scraper_retry 会照着账本里
        已经不存在、或已经挪出库的路径白跑一轮（stat 失败直接跳过，账本原样留着，
        看起来像"重试过了但还是失败"）。
        """
        accepted = [str(p) for p in paths if self._is_library_path(str(p))]
        return accepted, len(paths) - len(accepted)

    def _cmd_retry(self):
        """命令：投递一次“重试全部失败项”的扫描。"""
        paths = list(self._load_failed().keys())
        if not paths:
            self.post_message(title=f"{self.plugin_name} 重试", text="没有失败项可重试。")
            return
        accepted, rejected = self._filter_retry_paths(paths)
        if not accepted:
            self.post_message(
                title=f"{self.plugin_name} 重试",
                text=f"{len(paths)} 个失败项都已不在库内（文件被删或移走），账本留到下一轮清理。",
            )
            return
        started = self._kick_scan(force=True, paths=accepted)
        tail = f"（{rejected} 个已不在库内，跳过）" if rejected else ""
        self.post_message(
            title=f"{self.plugin_name} 重试",
            text=(f"已提交 {len(accepted)} 个失败项的重试{tail}。"
                  if started else "已有扫描在运行，请稍后再试。"),
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
                "summary": "按桶分页查询文件（done/failed/pending/deficient）",
            },
            {
                "path": "/file",
                "endpoint": self.api_file,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "单个文件的当前标签、缺项与改写记录",
            },
            {
                "path": "/retry",
                "endpoint": self.api_retry,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "重试指定路径或全部失败项",
            },
            {
                "path": "/edit",
                "endpoint": self.api_edit,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "编辑单个文件的元数据字段",
            },
            {
                "path": "/scan",
                "endpoint": self.api_scan,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "触发一次扫描",
            },
            {
                "path": "/config",
                "endpoint": self.api_get_config,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "读取可编辑配置（不含密码）",
            },
            {
                "path": "/config",
                "endpoint": self.api_config,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "保存配置（只改传上来的键）",
            },
        ]

    def api_status(self, request: Request) -> Dict[str, Any]:
        """GET /status —— 返回汇总、计数、配置回显（不含密码）与最近失败。"""
        return self._build_status()

    def api_files(self, request: Request) -> Dict[str, Any]:
        """GET /files —— bucket=done|failed|pending|deficient&limit&offset&search&written 分页查询。"""
        qp = request.query_params
        try:
            limit = max(1, min(int(qp.get("limit") or 50), 500))
        except (TypeError, ValueError):
            limit = 50
        try:
            offset = max(0, int(qp.get("offset") or 0))
        except (TypeError, ValueError):
            offset = 0
        bucket = (qp.get("bucket") or "done").lower()
        if bucket not in ("done", "failed", "pending", "deficient"):
            # 不认识的桶名不能悄悄落到 done 那一档：回显的还是请求的那个名字，
            # 调用方拿到一批"已处理"的行却以为是自己传的桶，看着像筛选坏了。
            return {
                "bucket": bucket, "total": 0, "offset": offset,
                "limit": limit, "items": [], "reason": "unknown_bucket",
            }
        search = (qp.get("search") or "").strip().lower()

        rows = self._collect_rows(bucket)
        if search:
            rows = [r for r in rows if search in r["path"].lower() or search in (r["name"] or "").lower()]
        # written=1：只看本插件改写过的。整个库跑过一遍后 done 桶绝大多数是宿主或往轮
        # 写的，翻找"这个插件到底改了谁"要把它们筛出去。只对 done 有意义，其余桶没有
        # 改写记录，硬筛会变成空列表，所以按桶判断而不是无条件应用。
        if bucket == "done" and str(qp.get("written") or "").strip().lower() in ("1", "true", "yes"):
            rows = [r for r in rows if r.get("set")]
        if bucket == "done":
            # 三轮稳定排序从次要键做起：改写过的整段提到最前，段内按最近一次改动倒序，
            # 没改写过的段沿用写时间倒序。其余桶仍按路径，翻页顺序才是稳的。
            rows.sort(key=lambda r: str(r.get("ts") or ""), reverse=True)
            rows.sort(key=lambda r: str(r.get("set_ts") or ""), reverse=True)
            rows.sort(key=lambda r: 0 if r.get("set") else 1)
        else:
            rows.sort(key=lambda r: r["path"])
        return {
            "bucket": bucket,
            "total": len(rows),
            "offset": offset,
            "limit": limit,
            "items": rows[offset:offset + limit],
        }

    def api_file(self, request: Request) -> Dict[str, Any]:
        """GET /file?path= —— 单个文件的现状：已有标签、缺项、本插件的改写记录。

        补齐弹层要"已有的先填进去"，而 /files 的行里没有 tag 值（一页 50 条每条都读
        一遍 tag 太贵，多数行根本不会被点开），所以点开谁才读谁。
        """
        path = str(request.query_params.get("path") or "").strip()
        if not path:
            return {"accepted": False, "reason": "no_path"}
        if not self._is_library_path(path):
            # 文件在扫描之后被移走/删掉，和"根本不在库里"要分开说：前者是账本过期，
            # 后者是有人拿这个接口当任意文件读。
            try:
                gone = not Path(path).exists()
            except OSError:
                gone = False
            return {"accepted": False, "reason": "not_found" if gone else "out_of_library"}
        fp = Path(path)
        tags = _read_music_tags(fp, with_lyrics_text=True)
        if tags.get("unreadable"):
            return {"accepted": False, "reason": "unreadable", "path": path}
        lyrics_from = ""
        if tags["has_lyrics"]:
            lyrics_from = "embedded"
        else:
            # 内嵌没有、同名 .lrc 有 —— 把侧车文本原样给出并标明来源，值是否带时间轴
            # 由人决定要不要留，插件不替他猜。
            sibling = _sibling_lyric(fp)
            if sibling:
                tags["lyrics"] = sibling
                lyrics_from = "sibling"
        entry = self._load_done().get(str(fp)) or {}
        return {
            "accepted": True,
            "path": str(fp),
            "name": fp.name,
            "tags": {field: str(tags.get(field) or "") for field in _EDIT_FIELDS},
            "has_cover": bool(tags["has_cover"]),
            "lyrics_from": lyrics_from,
            "missing": self._missing_fields(fp, tags),
            "rewrote": (entry.get("set") if isinstance(entry, dict) else None) or {},
            "set_ts": entry.get("set_ts") if isinstance(entry, dict) else None,
            "source": entry.get("source") if isinstance(entry, dict) else None,
            "ts": entry.get("ts") if isinstance(entry, dict) else None,
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
        accepted, rejected = self._filter_retry_paths(paths)
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

    async def api_edit(self, request: Request) -> Dict[str, Any]:
        """POST /edit —— body {path:"...",fields:{...}} 单项编辑库内文件的元数据。"""
        try:
            body = await request.json()
        except Exception:
            body = {}
        path = str(body.get("path") or "").strip() if isinstance(body, dict) else ""
        if not path:
            return {"accepted": False, "reason": "no_path"}
        # 只接受库内的音频文件：调用方传什么就改什么是把插件当成了任意文件写接口，
        # 换个 token 就能改到音乐库之外的文件。
        if not self._is_library_path(path):
            return {"accepted": False, "reason": "out_of_library"}
        fields, invalid = _edit_payload_fields(body.get("fields"))
        if invalid or not fields:
            return {"accepted": False, "reason": "invalid_fields"}
        target = Path(path)
        before = _read_music_tags(target)
        if not _write_music_tags(target, fields):
            return {"accepted": False, "reason": "write_failed"}
        # 手工补齐也算本插件改写：不记进 done 的话，在这页改过的文件和库里的其它文件
        # 混在一起，"我动过哪几个"就找不回来了。差集按文件现状算，宿主或别的程序同时
        # 改过的字段会一起显出来，比照抄提交上来的字段诚实。
        diff = _tag_diff(before, _read_music_tags(target))
        if diff:
            done = self._load_done()
            try:
                st = target.stat()
                mtime, size = int(st.st_mtime), st.st_size
            except OSError:
                mtime = size = None
            entry = done.get(path) if isinstance(done.get(path), dict) else {}
            now_iso = self._now_iso()
            entry["mtime"] = mtime if mtime is not None else entry.get("mtime")
            entry["size"] = size if size is not None else entry.get("size")
            entry["ts"] = now_iso
            entry["source"] = entry.get("source") or "manual"
            merged = dict(entry.get("set") or {})
            # 手工改写字段优先于历史扫描记录：同一个字段被两处动过，这一页要显示最新那次
            merged.update(diff)
            entry["set"] = merged
            entry["set_ts"] = now_iso
            done[path] = entry
            self.save_data(_KEY_DONE, done)
        # tag 刚被改过，两份缓存必须作废：否则缺陷清单还会按编辑前的状态报缺项。
        self.__dict__["_pending_cache"] = None
        self.__dict__["_deficient_cache"] = None
        return {"accepted": True, "updated": sorted(fields)}

    async def api_scan(self, request: Request) -> Dict[str, Any]:
        """POST /scan —— body {force?:bool}，触发一次（增量或全量）扫描。"""
        try:
            body = await request.json()
        except Exception:
            body = {}
        force = bool(isinstance(body, dict) and body.get("force")) or self._force_all
        # 界面上的"全量覆盖"既要绕过增量账、也要真的覆盖写：只给 force 的话
        # 标签还是会按"只补缺"写，按钮名字和实际行为对不上。
        started = self._kick_scan(force=force, overwrite=force)
        return {"accepted": started, "running": self._is_running(),
                "reason": None if started else "busy"}

    def api_get_config(self, request: Request) -> Dict[str, Any]:
        """GET /config —— 当前可编辑配置。

        侧栏全页里的配置对话框要用它起头：宿主只把 api/pluginId/navKey 三个 props
        交给 ./AppPage，initialConfig 那条链路在这个挂载方式下拿不到，只能插件自己回。
        这份配置里没有凭据（识别源是各平台的匿名检索接口），回显的就是全部真相。

        回的是「存下来的配置原值」而不是本轮生效值：多选留空的意思是"全部源"，
        把 _active_resources 那五个源回显回去，对话框会把它当成用户显式选中的名单，
        下一次保存就把"留空"这条规则写没了。audio_extensions 是只读说明项，
        POST 那边按配置键过滤，传回来也会被丢掉。
        """
        config = dict(self._config_snapshot())
        config["onlyonce"] = False
        config["audio_extensions"] = list(_supported_audio_ext())
        return config

    async def api_config(self, request: Request) -> Dict[str, Any]:
        """POST /config —— 只改传上来的键，其余按当前配置原样带着。

        这条路径绕开了宿主「插件管理 → 配置」的整体替换：先拿当前全量做底、按键覆盖，
        再交给 init_plugin 走同一套归一化（cron 兜底、min_score 收界、源名单过滤都在那里），
        最后整体写回。请求里认不出来的键不会被写进配置，免得留下表单没定义却存在于配置里的野键。
        """
        try:
            payload = await request.json()
        except Exception:
            return {"ok": False, "message": "请求体不是合法的 JSON"}
        if not isinstance(payload, dict):
            return {"ok": False, "message": "请求体要是一个 JSON 对象"}
        known = set(self._config_snapshot())
        merged = {**self._config_snapshot(),
                  **{key: value for key, value in payload.items() if key in known}}
        self.init_plugin(merged)
        self.update_config(self._config_snapshot())
        # enabled 决定宿主要不要登记这个插件，cron 决定什么时候跑：改过任何一个
        # 都要请宿主重建，没改也重建一次没有代价。
        if "enabled" in payload or "cron" in payload:
            self._refresh_jobs()
        status = self._build_status()
        status.update({"ok": True, "message": "配置已保存"})
        return status

    def _refresh_jobs(self) -> None:
        """让宿主按当前配置重建本插件的周期任务。

        从侧栏对话框保存不会走插件管理那条链路，不喊这一声的话：勾了「启用插件」
        而宿主那边还是停用状态，周期扫描登记不上，侧栏入口也不会出现。
        """
        update = getattr(scheduler_sdk, "update_plugin_job", None) if scheduler_sdk else None
        if not update:
            return
        try:
            update(self.__class__.__name__)
        except Exception as err:  # pragma: no cover - 调度器不可用不影响配置已落盘
            logger.warning(f"[music-scraper] 重建周期任务失败: {err}")

    # ---------------- 页面 / 渲染 ----------------

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """声明 Vue 联邦渲染，产物目录 dist/assets。"""
        return "vue", "dist/assets"

    def get_page(self) -> List[dict]:
        """Vue 模式下详情页由远程 ./Page 渲染，这里返回空。"""
        return []

    def get_sidebar_nav(self) -> List[Dict[str, Any]]:
        """主界面「整理」分区的全页入口（远程组件 ./AppPage）。

        section 用 organize、permission 用 manage，跟这一区里的音乐资源管理（32）、
        字幕助手（30）、brushflow（45）同一套：这一区的插件都会动库里文件。
        order 33 紧贴音乐资源管理，两条音乐相关的挨在一起，也不把字幕那一对（30、48）切断。
        """
        if not self.get_state() or not self._show_sidebar_nav:
            return []
        return [
            {
                "nav_key": "main",
                "title": "音乐库刮削",
                "icon": "mdi-music-circle-outline",
                "section": "organize",
                "permission": "manage",
                "order": 33,
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """周期扫描服务，由宿主调度器托管（插件自建 scheduler 已移除）。"""
        if not self._enabled:
            return []
        return [
            {
                "id": "MusicLibraryScraper.Scan",
                "name": "音乐库刮削",
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
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "show_sidebar_nav", "label": "整理区侧栏入口"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
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
                                "model": "cron", "label": "执行周期", "placeholder":
                                "5 位 cron；星期 0=周一…6=周日、7=周日，留空默认 0 3 * * sun"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "min_score", "label": "最低匹配分数（1-3）", "type": "number",
                                "min": 1, "max": 3, "placeholder": "默认 3"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {
                                "model": "fill_lyrics", "label": "识别源补歌词（默认交给宿主）"
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "resources", "label": "识别源（可多选，留空=全部）",
                                "multiple": True, "chips": True, "closable": True,
                                "items": [
                                    {"title": "网易云 netease", "value": "netease"},
                                    {"title": "QQ 音乐 qmusic", "value": "qmusic"},
                                    {"title": "酷狗 kugou", "value": "kugou"},
                                    {"title": "咪咕 migu", "value": "migu"},
                                    {"title": "MusicBrainz", "value": "musicbrainz"},
                                ]
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
            "show_sidebar_nav": True,
            "onlyonce": False,
            "mode": "",
            "cron": _DEFAULT_CRON,
            "scraper_paths": "",
            "exclude_paths": "",
            # 留空 = 全部源（读侧 _normalize_resources 保证）。不要把五个源预填进去：
            # 预填了就没有「留空即全部」这条规则，配置页和说明也对不上。
            "resources": [],
            "min_score": 3,
            # writeback 一定会带这个键（onlyonce 回写里就有），defaults 不声明它，
            # 回写后就成了「表单没定义却在配置里存在」的野键，整体替换的护栏也拦不住。
            "fill_lyrics": False,
        }

    # ---------------- 状态账 ----------------

    def _load_book(self, key: str) -> Dict[str, dict]:
        """读一本账：外层和条目都必须是 dict，retries 必须是整数。

        plugindata 可能来自旧版本、手工编辑或写到一半的 JSON。条目不是 dict 时，
        后面每一处 prev.get() 都会抛 AttributeError，从 /status 到整轮扫描一起崩；
        归一成空 dict 只表示"这条记录什么都不知道"，那个文件会被重新刮一次。

        retries 也要归一：它是 `int(prev.get("retries", 0))` 直接吃的那个值，写成非数字
        （null、"三次"、旧版本的 dict）都会抛 TypeError，而抛在逐文件循环里等于整轮中止 ——
        崩的位置由账本里的键序决定，下一轮枚举走到同一个键还是同一个位置，于是那之后的
        文件永远处理不到、汇总也永远不落账。
        """
        data = self.get_data(key)
        if not isinstance(data, dict):
            return {}
        out: Dict[str, dict] = {}
        for k, v in data.items():
            if not isinstance(v, dict):
                out[k] = {}
                continue
            # 只在坏值上动手：done 条目本来没有 retries，不该为此多写一个键
            if "retries" in v and not isinstance(v["retries"], int):
                try:
                    v["retries"] = int(v["retries"] or 0)
                except (TypeError, ValueError):
                    v["retries"] = 0
            out[k] = v
        return out

    def _load_done(self) -> Dict[str, dict]:
        return self._load_book(_KEY_DONE)

    def _load_failed(self) -> Dict[str, dict]:
        return self._load_book(_KEY_FAILED)

    def _load_runs(self, key: str = _KEY_RUNS) -> dict:
        data = self.get_data(key)
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------------- 主体流程 ----------------

    @staticmethod
    def _normalize_dow(raw_cron: str) -> str:
        """把 crontab 习惯里表示周日的星期 7 换成 sun。

        APScheduler 的星期上限是 6，直接写 7 会让整条表达式校验失败；用户写 7 想说的
        就是周日，所以只换这一个值、保留其余字段，而不是把整条表达式扔掉回落默认。
        0-6 原样交给调度器（它按 0=周一 解析），不做任何"猜意图"的位移。
        """
        parts = raw_cron.split()
        if len(parts) == 5 and parts[4] == "7":
            parts[4] = "sun"
            return " ".join(parts)
        return raw_cron

    @staticmethod
    def _next_fire_text(trigger: Any) -> str:
        """触发器的下次触发时间文本；拿不到就说"未知"，不拿它当校验用。"""
        try:
            nxt = trigger.get_next_fire_time(None, datetime.now())
        except Exception:
            return "未知"
        return nxt.strftime("%Y-%m-%d %H:%M (%a)") if nxt else "未知"

    def _cron_text(self) -> Tuple[str, str]:
        """(生效的 cron, 下次触发时间)。界面要看得见日子实际落在周几。"""
        raw = self._normalize_dow((self._cron or "").strip() or _DEFAULT_CRON)
        try:
            trigger = CronTrigger.from_crontab(raw)
        except (ValueError, KeyError) as err:
            return _DEFAULT_CRON, f"未生效（{err}，已回落默认）"
        return raw, self._next_fire_text(trigger)

    def _build_cron_trigger(self, raw_cron: Optional[str]) -> CronTrigger:
        """非法/空 cron 回落默认，避免插件整体 load_failed。"""
        raw = self._normalize_dow((raw_cron or "").strip() or _DEFAULT_CRON)
        try:
            trigger = CronTrigger.from_crontab(raw)
        except (ValueError, KeyError) as err:
            logger.warning(f"[music-scraper] cron 表达式非法：{raw!r} → {err}，回落默认 {_DEFAULT_CRON}")
            raw = _DEFAULT_CRON
            trigger = CronTrigger.from_crontab(raw)
        # 把解析结果写进日志：星期编号按调度器语义（0=周一…6=周日）而不是 crontab
        # （0/7=周日），只回显表达式的话，"每周日"实际跑在周一这种事在日志里看不出来。
        logger.info(f"[music-scraper] 定时扫描 cron={raw} → 下次 {self._next_fire_text(trigger)}")
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
            # 复位停止信号必须在锁内、占位之前：放在锁外时，恰好挤在这个窗口里到达的
            # stop_service 会被抹掉，而它想停的正是紧接着要开始的这一轮 —— 界面按了
            # "停止"、日志里却一句"收到停止信号"都没有，只能等整轮跑完。
            self._stop_event().clear()
            self.__dict__["_scan_running"] = True
        return True

    def _end_scan(self):
        lock = self.__dict__.setdefault("_scan_lock", threading.Lock())
        with lock:
            self.__dict__["_scan_running"] = False

    def _execute_scan(self, force: bool = False,
                      target_paths: Optional[List[str]] = None,
                      overwrite: bool = False) -> None:
        """执行一轮扫描并释放槽位；调用方必须已经 _begin_scan() 占到槽。"""
        try:
            self.__scan(force=force, target_paths=target_paths, overwrite=overwrite)
        except Exception as err:  # pragma: no cover
            logger.error(f"[music-scraper] 扫描异常: {err}")
        finally:
            self._end_scan()

    def _run_scan_guarded(self, force: bool = False,
                          target_paths: Optional[List[str]] = None,
                          overwrite: bool = False) -> bool:
        """宿主调度器用的同步入口：抢不到槽位就跳过本轮，不与已 in-flight 的扫描并发。"""
        if not self._begin_scan():
            logger.info("[music-scraper] 已有扫描在运行，本次触发跳过")
            return False
        self._execute_scan(force=force, target_paths=target_paths, overwrite=overwrite)
        return True

    def _kick_scan(self, force: bool = False, paths: Optional[List[str]] = None,
                   overwrite: bool = False) -> bool:
        """投递一次扫描：无参优先交宿主一次性任务，带参/强制走后台线程。"""
        if not force and not paths:
            if self.__dict__.get("_scan_running"):
                # 一次性任务的回调 3 秒后才会跑，那时候抢不到槽位就静默跳过。
                # 这里不先挡一下，/scan 与"立即运行一次"会对着界面报"已提交"，
                # 实际一轮都没跑，用户只能等下一句"已有扫描在运行"出现在日志里。
                logger.info("[music-scraper] 已有扫描在运行，本次触发跳过")
                return False
            add = getattr(scheduler_sdk, "add_plugin_once_job", None) if scheduler_sdk else None
            if add:
                try:
                    # 宿主只在调度器运行且注册成功时返回 True；返回 False 时任务其实没排上，
                    # 不回落后台线程就会对着界面报"已提交"、实际什么都没跑。
                    if add(
                        self.__class__.__name__,
                        "scan_once",
                        self._run_scan_guarded,
                        "音乐库刮削（一次性）",
                        delay_seconds=3,
                    ):
                        return True
                    logger.debug("[music-scraper] 一次性任务未注册（调度器没在跑），回落后台线程")
                except Exception as err:  # pragma: no cover
                    logger.debug(f"[music-scraper] 一次性任务投递失败，回落后台线程: {err}")
        return self._start_scan_async(force=force, paths=paths, overwrite=overwrite)

    def _start_scan_async(self, force: bool = False, paths: Optional[List[str]] = None,
                          overwrite: bool = False) -> bool:
        """在后台线程执行扫描；若已有扫描在跑则拒绝，避免并发重复处理。"""
        if not self._begin_scan():
            return False
        threading.Thread(
            target=self._execute_scan,
            kwargs={"force": force, "target_paths": paths, "overwrite": overwrite},
            name="music-scraper-scan",
            daemon=True,
        ).start()
        return True

    def _is_library_path(self, path: str) -> bool:
        """外部传入的重试路径是否真的是库内的音频文件。

        与 _enumerate_files 的软链收敛用同一份 realpath 基准：两边口径不一致时，
        /retry 拒收的路径扫描侧却照样写，界面看起来就是"筛选没生效"。
        """
        try:
            target = Path(path).resolve()
        except (OSError, RuntimeError):
            return False
        roots_real, excludes_real = self._library_bounds()
        if not self._target_in_library(target, roots_real, excludes_real):
            return False
        return (target.suffix.lower() in _supported_audio_ext() and target.is_file())

    def _enumerate_files(self) -> List[Path]:
        """按配置枚举音频文件（宿主白名单、扩展名、trickplay、排除路径过滤）。

        用 os.walk 而不是 Path.rglob：rglob 的 ** 会递归进软链目录，库内一个回环软链
        就能让枚举永远不返回，而扫描槽位已经被互斥占住 —— 之后每一轮 cron 与手动触发
        都只会记"已有扫描在运行"。顺带在目录级剪掉 .trickplay，不用再对里面每个
        缩略图文件做一次 stat。

        followlinks=False 只挡住了软链**目录**，软链**文件**照样出现在 filenames 里，
        而宿主写标签是按我们递给它的路径写的：库内一个指向 /etc 或库外曲库的 .flac 软链
        会被写穿出去。/retry 用 realpath 判定拒收同一批路径，扫描侧必须用同一套口径
        （两边都拿 realpath 比 _library_bounds 那两份基准）。
        """
        allowed_ext = set(_supported_audio_ext())
        excludes = [Path(e) for e in (self._exclude_paths or "").split("\n") if e.strip()]
        roots_real, excludes_real = self._library_bounds()
        files: List[Path] = []
        links_out = 0
        for root in (p for p in (self._scraper_paths or "").split("\n") if p.strip()):
            if self._stop_event().is_set():
                # 内层 break 只退出了当前这个库的 os.walk；不在这儿也挡一次，
                # 停止之后剩下的库照样被遍历，只是最后被整轮当成"扫过的文件"记账。
                logger.info("[music-scraper] 收到停止信号")
                break
            root_path = Path(root)
            if not root_path.exists():
                logger.warning(f"[music-scraper] 路径不存在: {root}")
                continue
            for dirpath, dirnames, filenames in os.walk(root_path, followlinks=False):
                if self._stop_event().is_set():
                    logger.info("[music-scraper] 收到停止信号")
                    break
                current = Path(dirpath)
                # 排除目录可能配的是同一目录的别名或软链形式：整棵目录按 realpath 判一次，
                # 一个目录一次系统调用，比逐文件判定便宜、也不会把别名形式的排除漏掉。
                try:
                    current_real = current.resolve()
                except (OSError, RuntimeError):
                    current_real = current
                if any(current_real == e or current_real.is_relative_to(e)
                       for e in excludes_real):
                    dirnames[:] = []
                    continue
                # Jellyfin 预览缓存目录整棵剪掉：里面全是 .jpg/.mp4，逐个 stat 纯属浪费
                dirnames[:] = [d for d in dirnames if not d.lower().endswith(".trickplay")]
                for name in filenames:
                    if Path(name).suffix.lower() not in allowed_ext:
                        continue
                    fp = current / name
                    if not fp.is_file():
                        continue
                    if self._is_trickplay_path(fp):
                        continue
                    if any(fp.is_relative_to(e) for e in excludes):
                        continue
                    if fp.is_symlink():
                        # 只对软链做 realpath：库里九成九不是软链，没必要为每个文件多一次系统调用
                        try:
                            real = fp.resolve()
                        except (OSError, RuntimeError):
                            continue
                        if not self._target_in_library(real, roots_real, excludes_real):
                            links_out += 1
                            continue
                    files.append(fp)
        if links_out:
            # 不写日志用户只会看到"发现 N 个文件"比目录里的少，且查不出为什么
            logger.warning(
                f"[music-scraper] 已跳过 {links_out} 个指向库外的音频软链"
                "（写标签会落到 scraper_paths 之外；确有需要请把目标目录也加进库路径）"
            )
        return files

    def _identity_roots(self) -> List[Path]:
        """配置里写的库根（不做 realpath）：给目录兜底的身份当边界用。

        不 realpath 是有意的 —— 待判定的路径都是 os.walk 从这份配置拼出来的，字符串
        形式一致才比得对；库根本身是软链时，realpath 反而会让每一个文件都判成"在库根之外"。
        按配置原文缓存，改配置立刻换 key，不需要 TTL。
        """
        raw = self._scraper_paths or ""
        cached = self.__dict__.get("_roots_cache")
        if cached and cached[0] == raw:
            return cached[1]
        roots = [Path(item).expanduser() for item in
                 (p for p in raw.split("\n") if p.strip())]
        self.__dict__["_roots_cache"] = (raw, roots)
        return roots

    def _library_bounds(self) -> Tuple[List[Path], List[Path]]:
        """配置库路径与排除路径的 realpath 列表：软链收敛判定和 /retry 库内判定共用同一份基准。

        两侧都要 realpath：scraper_paths 本身可能是软链，客户端传的路径也可能带 ../，
        只做字符串前缀比较两种都能绕过。
        """
        roots: List[Path] = []
        for raw in (p for p in (self._scraper_paths or "").split("\n") if p.strip()):
            try:
                roots.append(Path(raw).resolve())
            except (OSError, RuntimeError):
                continue
        excludes: List[Path] = []
        for raw in (p for p in (self._exclude_paths or "").split("\n") if p.strip()):
            try:
                excludes.append(Path(raw).resolve())
            except (OSError, RuntimeError):
                continue
        return roots, excludes

    @staticmethod
    def _target_in_library(target: Path, roots_real: List[Path],
                           excludes_real: List[Path]) -> bool:
        """已经 resolve 过的路径是否落在库内且不在排除目录里。"""
        if not roots_real:
            return False
        if not any(target == r or target.is_relative_to(r) for r in roots_real):
            return False
        return not any(target == e or target.is_relative_to(e) for e in excludes_real)

    def __scan(self, force: bool = False, target_paths: Optional[List[str]] = None,
               overwrite: bool = False):
        """增量扫描主流程。

        force：忽略 done 缓存并强制重打。
        overwrite：是否覆盖已有标签（"强制覆盖"模式或界面上的"全量覆盖"）。
        target_paths：仅处理这批路径（供手动重试），此时跳过缓存与重试上限。
        """
        if not self._scraper_paths:
            logger.warning("[music-scraper] 未配置 scraper_paths，跳过")
            return

        started = time.time()
        # 账本要变了，pending 桶的缓存立刻作废（它靠 done/failed 做差集）
        self.__dict__["_pending_cache"] = None
        self.__dict__["_deficient_cache"] = None
        done = self._load_done()
        failed = self._load_failed()
        targeted = bool(target_paths)

        if targeted:
            files = [Path(p) for p in target_paths if p]
        else:
            files = self._enumerate_files()

        scanned = from_cache = skipped_tagged = ok = failed_n = exhausted = deficient = 0
        transport = errors = aborted = written = 0
        now_iso = self._now_iso()
        effective_force = force or (self._force_all and not targeted)
        # 覆盖写只由"强制覆盖"模式或界面上的"全量覆盖"决定：定向重试的 force 只是要绕过
        # 增量账和重试上限，不该顺带把手工改过的标签替换掉。
        effective_overwrite = bool(overwrite) or self._force_all
        logger.info(
            f"[music-scraper] 开始处理 {len(files)} 个文件"
            f"（mode={'覆盖' if effective_overwrite else '补空'}"
            f"{'，定向重试' if targeted else '，增量'}）"
        )
        live_keys = set()  # 整盘扫描时实际存在（stat 成功）的文件，供 prune 用
        dirty = False  # 自上次落账以来两本账有没有变更
        last_flush = time.time()
        # 连续"插件没学到任何东西"的文件数：识别源故障或单文件异常都算
        transport_streak = 0
        # 定向重试的批次也可能很大（/retry 全部失败项）：平台整体风控时一样要熔断，
        # 否则几千个文件 × 全部源会一路打到底。只有小批次才值得整批跑完。
        may_abort = len(files) > _ABORT_AFTER_TRANSPORT

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

            try:
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

                # 每个文件的 tag 只解析一次：补空判定与匹配身份共用这份读取结果
                tags = _read_music_tags(fp)

                # "有缺项"：核心项齐了、但次级字段还缺的文件。照 _missing_fields 的口径
                # 就地判这份已读的 tags，不再调函数（它每次会重解析 + 落盘扫封面）。
                # 放在 __is_complete 的 continue 之前，否则补空判够格就跳过了永远数不到。
                if tags.get("title") and tags.get("artist") and tags.get("album") \
                        and (not tags.get("year") or not tags.get("tracknumber")
                             or not tags.get("has_cover") or not tags.get("has_lyrics")):
                    deficient += 1

                # 补空模式：tag 与封面都齐了才跳过。
                if not effective_force and self.__is_complete(fp, tags):
                    prev = done.get(key)
                    source = prev.get("source") if isinstance(prev, dict) else None
                    done[key] = self._done_entry(prev, mtime, size, now_iso, source)
                    failed.pop(key, None)
                    skipped_tagged += 1
                    dirty = True
                    continue

                okf, reason, score, source, written_fields = self.__scrape_file(
                    fp, tags=tags, overwrite=effective_overwrite)
            except Exception as err:
                # 单个文件把插件或宿主打崩不能带走整轮：崩在第七千个文件上，前面六千多个
                # 判断还没落账（落账每 60 秒一次），槽位释放、下一轮从头又崩在同一条路径上
                # —— 账本里的坏值、宿主 ScrapingChain 的内部异常都是确定性的，于是全库
                # 永远停在待处理。这个文件既不记成功也不记失败，下一轮照原样再来；
                # 连着出现就按环境异常熔断，不烧这个文件的 retries。
                errors += 1
                transport_streak += 1
                logger.error(f"[music-scraper] 处理异常：{key} — {err}")
                if may_abort and transport_streak >= _ABORT_AFTER_TRANSPORT:
                    logger.warning(
                        f"[music-scraper] 连续 {transport_streak} 个文件处理异常，"
                        f"判为环境异常，本轮中止"
                    )
                    aborted = True
                    break
                continue

            if reason.startswith(_TRANSPORT_REASONS):
                # 识别源没给出结论：插件没学到任何关于这个文件的信息，所以既不写 failed 账、
                # 也不涨 retries，原样留着下一轮再处理。连续踩满就熔断中止本轮。
                transport += 1
                transport_streak += 1
                if may_abort and transport_streak >= _ABORT_AFTER_TRANSPORT:
                    logger.warning(
                        f"[music-scraper] 连续 {transport_streak} 个文件的识别源都没有给出答案，"
                        f"判为识别源整体不可用，本轮中止"
                    )
                    aborted = True
                    break
            else:
                transport_streak = 0
                if okf:
                    done[key] = self._done_entry(done.get(key), mtime, size, now_iso,
                                                 source, written_fields)
                    if written_fields:
                        written += 1
                    failed.pop(key, None)
                    ok += 1
                    dirty = True
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
                    dirty = True
                    # 失败也要一行一行留痕：一轮里几十个文件逐个问满 7 个源可能静默跑上百分钟，
                    # 日志一片空白看着就像卡死，而账本里的原因又要刷开数据页才看得到。
                    logger.info(
                        f"[music-scraper] 未通过：{key} — {reason}"
                        f"{'，分数 ' + str(score) if score is not None else ''}"
                        f"（第 {prev_retries + 1}/{self.MAX_RETRY} 次）"
                    )

            # 定期落账：整轮动辄几小时，中途容器重启不该把已刮好的部分全丢掉。
            # 按时间而不是次数：plugindata 是单行 JSON，每次写都要整本重新序列化。
            if dirty and (time.time() - last_flush) >= _FLUSH_INTERVAL:
                self.save_data(_KEY_DONE, done)
                self.save_data(_KEY_FAILED, failed)
                dirty = False
                last_flush = time.time()

        # 清理：仅整盘扫描且未中断/未熔断时剔除磁盘上已消失的记账项，避免状态无限膨胀。
        # 熔断或提前退出时 live_keys 只覆盖了一部分目录，这时候 prune 会误删未走到的条目。
        if not targeted and not aborted and not self._stop_event().is_set():
            for k in list(done.keys()):
                if k not in live_keys:
                    done.pop(k, None)
            for k in list(failed.keys()):
                if k not in live_keys:
                    failed.pop(k, None)

        # "待处理"要和同一时刻数据页的待处理桶是同一个口径，否则两边对不上号、
        # 看着像有文件凭空消失。桶是"这份文件列表里既不在 done 也不在 failed 的那些"，
        # 这里就直接照它算：识别源故障、处理异常的文件都不落账所以仍然待处理，熔断/停止
        # 没走到的也一样，上一轮已经记过失败的则回到失败桶（用计数器加减时很容易漏一种）。
        pending = sum(
            1 for fp in files
            if str(fp) not in done and str(fp) not in failed
        )
        summary = {
            "ts": now_iso,
            "scanned": scanned,
            "from_cache": from_cache,
            "skipped_tagged": skipped_tagged,
            "ok": ok,
            # 成功里真正动了文件的那部分：宿主覆盖策略拦下、或候选值与文件里的一样，
            # 都算成功但不算改写 —— 数据页要说的是"插件动过我的文件吗"。
            "written": written,
            "failed": failed_n,
            "exhausted": exhausted,
            "transport": transport,
            "error": errors,
            "aborted": aborted,
            "pending": pending,
            "deficient": deficient,
            "force": effective_force,
            "targeted": targeted,
            "cron": self._normalize_dow((self._cron or "").strip()) or _DEFAULT_CRON,
            "duration_s": round(time.time() - started, 1),
        }
        self.save_data(_KEY_DONE, done)
        self.save_data(_KEY_FAILED, failed)
        # 定向重试只覆盖几个文件：写进 runs 会把整盘汇总刷成 scanned=1、pending=0，
        # 数据页看起来就像整库已经扫完了。它单独记在 last_targeted。
        self.save_data(_KEY_TARGETED_RUNS if targeted else _KEY_RUNS, summary)
        self.__dict__["_pending_cache"] = None
        self.__dict__["_deficient_cache"] = None
        logger.info(
            f"[music-scraper] {'定向重试' if targeted else '扫描'}完成："
            f"命中缓存 {from_cache}，已有tag {skipped_tagged}，"
            f"成功 {ok}（改写 {written}），失败 {failed_n}，超限跳过 {exhausted}，识别源故障 {transport}"
            f"{'，处理异常 ' + str(errors) if errors else ''}"
            f"{'（本轮熔断中止，未处理的下轮再来）' if aborted else ''}"
            f"（{summary['duration_s']}s）"
        )

    @staticmethod
    def _done_entry(prev: Any, mtime: int, size: int, now_iso: str, source: Any,
                    written_fields: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
        """done 账的一条记录，改写记录（set）跨轮留住。

        written_fields 非空就是本轮改的，连 set_ts 一起刷新；本轮没动文件就照抄上一条 ——
        增量模式下绝大多数文件下一轮走的是"已齐、跳过"，不抄的话那条改写记录第一轮之后
        就消失了，"哪些文件是本插件写过的"这个历史事实会变成"上一轮写了谁"。
        没改过文件的条目干脆不挂 set 键：账本是单行 JSON，整库都挂个空 dict 是白胖。
        """
        entry: Dict[str, Any] = {"mtime": mtime, "size": size, "ts": now_iso, "source": source}
        if written_fields:
            entry["set"] = written_fields
            entry["set_ts"] = now_iso
        elif isinstance(prev, dict) and prev.get("set"):
            entry["set"] = prev["set"]
            entry["set_ts"] = prev.get("set_ts") or prev.get("ts")
        return entry

    def __is_complete(self, path: Path, tags: Optional[Dict[str, Any]] = None) -> bool:
        """补空模式的“够格”判据：title + artist + album + 内嵌封面。

        只判 title/artist 会让已刮齐标签、但缺封面的文件永远被跳过，而那恰恰是
        真实音乐库最常见剩下的缺口（MP 写入链路会把 cover_url 下载后内嵌）。
        """
        if tags is None:
            tags = _read_music_tags(path)
        return bool(
            tags["title"] and tags["artist"] and tags["album"] and tags["has_cover"]
        )

    def __scrape_file(self, path: Path, tags: Optional[Dict[str, Any]] = None,
                      overwrite: bool = False) -> Tuple[bool, str, Optional[int], str, Dict[str, List[str]]]:
        """处理单个文件，返回 (是否成功, 失败原因, 命中分数, 命中来源, 本插件改写了哪几项)。

        最后一项只在写成功时非空：它是写入前后各读一次 tag 算出来的差集，所以宿主策略
        拦下覆盖、或候选值本来就与文件里的一样，都会诚实地回空 dict。
        """
        identity = _local_identity(path, tags, roots=self._identity_roots())
        if not identity["title"]:
            # 和"识别源无候选"分开：那种是问过了、曲库里确实查不到，这种是一次都没问 ——
            # 该做的是把这类文件（整张 CD 压成一个 wav）排出扫描范围，不是放宽阈值。
            return False, "本地读不出曲名（tag 与目录结构都没给出）", None, "", {}
        candidates, failure = self._fetch_candidates(path, identity=identity)
        if failure:
            # 服务侧故障：不能算这个文件的锅，调用方按原因前缀区分后不记 retries。
            return False, failure, None, "", {}
        if not candidates:
            return False, _NO_CANDIDATE_REASON, None, "", {}
        picked, score, hits = self._best_candidate(path, candidates, identity=identity)
        if not picked or score < self._min_score:
            # 分数不够时把"差在哪一项"一起写进原因：2/3 分本身不够用来看该改文件名、
            # 该放宽阈值，还是该把这类文件排出扫描范围 —— 曲名没对上和本地根本没有曲名，
            # 要做的决定完全相反。
            miss = self._miss_text(hits)
            detail = f"未达阈值（最低匹配分数 {self._min_score}）"
            return False, f"{detail}：{miss}" if miss else detail, score, "", {}
        source = str(picked.get("resource") or "").strip()
        if self._fill_lyrics and not _candidate_has_lyric(picked):
            lyric = self._fetch_lyric(source, picked)
            if lyric:
                picked["lyrics"] = lyric
        info = self._candidate_to_musicinfo(picked, path)
        if not info:
            return False, "候选信息不完整，无法转换", score, source, {}
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
            overwrite=overwrite,
        )
        if ok:
            logger.info(f"[music-scraper] 刮削成功: {path.name} → {info.title}（{source or '未知源'}）")
            # 改写了哪几项以文件为准：拿候选自己拼"我写了什么"，会在宿主按自己的
            # 覆盖策略拦下那一笔时说假话（而这句话是数据页上"插件动过我的文件吗"的依据）。
            return True, "", score, source, _tag_diff(
                tags if tags is not None else _read_music_tags(path),
                _read_music_tags(path))
        logger.warning(f"[music-scraper] 刮削失败: {path.name} — {msg}")
        return False, f"写入失败：{msg}", score, source, {}

    # ---------------- 查询辅助（供 status/files） ----------------

    @staticmethod
    def __aggregate_reasons(failed: Dict[str, dict]) -> List[Dict[str, Any]]:
        """把失败账本按原因聚合成 [{reason, count, best_score, retryable}]，按数量降序。"""
        grouped: Dict[str, Dict[str, Any]] = {}
        for entry in failed.values():
            reason = str(entry.get("reason") or "").strip() or "未记录原因"
            item = grouped.setdefault(reason, {
                "reason": reason, "count": 0, "best_score": None, "retryable": 0,
            })
            item["count"] += 1
            score = entry.get("score")
            if isinstance(score, int) and not isinstance(score, bool):
                if item["best_score"] is None or score > item["best_score"]:
                    item["best_score"] = score
            if int(entry.get("retries") or 0) < MusicLibraryScraper.MAX_RETRY:
                item["retryable"] += 1
        return sorted(grouped.values(), key=lambda r: -r["count"])

    def _build_status(self) -> Dict[str, Any]:
        """组装状态汇总：runs + 分桶计数 + 配置回显（不含密码）+ 最近失败。"""
        cron_now = self._cron_text()
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
        # 失败原因分布：逐行看 reason 要一页页翻，而"阈值要不要降一档、要不要多开一个源"
        # 是按原因聚合出来的一个决定。best_score 是这一类里最高的分数，它和 min_score 只差
        # 1 时，降一档阈值就能救回这一整类 —— 没有这个数，用户只能瞎猜。
        failure_reasons = self.__aggregate_reasons(failed)
        return {
            "enabled": self._enabled,
            "running": self._is_running(),
            "max_retry": self.MAX_RETRY,
            "runs": runs,
            # 定向重试的汇总单独一处：混进 runs 会让整盘统计被一个文件的重试盖掉。
            "last_targeted": self._load_runs(_KEY_TARGETED_RUNS),
            "counts": {
                "done": len(done),
                # 其中真正被本插件改写过的有几个：数据页的「只看本插件改写」开关要显示
                # 数量，也用来一眼看出这个插件在库里到底动了多少文件。
                "rewritten": sum(1 for v in done.values()
                                 if isinstance(v, dict) and v.get("set")),
                "failed": len(failed),
                "retryable": retryable,
                "exhausted": exhausted,
                "pending_unknown": runs.get("pending", 0),
                "deficient": runs.get("deficient", 0),
            },
            "failure_reasons": failure_reasons,
            "config": {
                "cron": cron_now[0],
                "cron_next": cron_now[1],
                "mode": "force_all" if self._force_all else "fill_empty",
                # 侧栏入口开没开：数据页与配置对话框都按这一项说"入口在不在整理区"
                "show_sidebar_nav": self._show_sidebar_nav,
                "scraper_paths": self._scraper_paths,
                "exclude_paths": self._exclude_paths,
                # 本轮实际要问的源（配置留空即全部）：回显生效的那一份，不是配置原样
                "resources": self._active_resources,
                "min_score": self._min_score,
                "fill_lyrics": self._fill_lyrics,
                # 实际会处理的扩展名（本地候选列表 ∩ 宿主白名单）：宿主把某个格式移出
                # 白名单时，这里能立刻看出来插件已经不再碰它。
                "audio_extensions": list(_supported_audio_ext()),
            },
            "recent_failed": recent_failed,
        }

    def _collect_rows(self, bucket: str) -> List[Dict[str, Any]]:
        """把某桶整理成表格行 {path,name,mtime,size,status,reason,retries,score,ts}；
        done 另带 source 与改写记录 set/set_ts，deficient 另带 missing。"""
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
            cached = self.__dict__.get("_pending_cache")
            if cached and time.time() - cached["ts"] < _PENDING_TTL:
                return list(cached["rows"])
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
            # 待处理 = 全盘遍历做差集，是这几个接口里最贵的一档。数据页翻页、搜索、
            # 刷新都会再打一次，短时间缓存一份，扫描开始/结束时主动作废。
            self.__dict__["_pending_cache"] = {"ts": time.time(), "rows": list(rows)}
        elif bucket == "deficient":
            cached = self.__dict__.get("_deficient_cache")
            if cached and time.time() - cached["ts"] < _PENDING_TTL:
                return list(cached["rows"])
            for fp in self._enumerate_files():
                missing = self._missing_fields(fp)
                if not missing:
                    continue
                key = str(fp)
                try:
                    st = fp.stat()
                    mtime, size = int(st.st_mtime), st.st_size
                except OSError:
                    mtime, size = None, None
                rows.append({
                    "path": key, "name": fp.name,
                    "mtime": mtime, "size": size,
                    "status": "deficient", "reason": ",".join(missing),
                    "retries": None, "score": None, "ts": None,
                    "missing": missing,
                })
            self.__dict__["_deficient_cache"] = {"ts": time.time(), "rows": list(rows)}
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
                    # 改写记录跟着行走：数据页要据此把本插件动过的文件排在前面并挂角标，
                    # 点开看改了什么不必再发一次 /file（前值后值都在这个 dict 里）。
                    "set": v.get("set") or None, "set_ts": v.get("set_ts"),
                })
        return rows

    def _missing_fields(self, path: Path,
                        tags: Optional[Dict[str, Any]] = None) -> List[str]:
        """该文件缺哪些元数据项，返回缺项名列表；齐全则返回空。

        tags 可以由调用方先读好带进来（/file 本来就要读一次带歌词的版本）。
        """
        if tags is None:
            tags = _read_music_tags(path)
        missing = [f for f in ("title", "artist", "album", "year", "tracknumber")
                   if not str(tags.get(f) or "").strip()]
        if not tags.get("has_cover"):
            missing.append("cover")
        if not tags.get("has_lyrics") and not path.with_suffix(".lrc").exists():
            missing.append("lyrics")
        return missing

    @staticmethod
    def _source_request(spec: Dict[str, Any]) -> Tuple[Any, Optional[str]]:
        """按 sources.build_request 的描述发一次请求，返回 (响应体, 故障标记)。

        状态码自己拿：宿主 RequestUtils.get 会把非 2xx 折成 None，限流(429)、风控(403)
        和真连不上就分不开 —— 平台答了 429 不该被记成"这个文件在曲库里查不到"，
        那会让人白改阈值，还会把一次限流摊成全库每个文件的重试额度。
        """
        ru = RequestUtils(headers=dict(spec.get("headers") or {}))
        try:
            if spec.get("json") is not None:
                resp = ru.post(url=spec["url"], json=spec["json"], timeout=_SEARCH_TIMEOUT)
            else:
                resp = ru.get_res(url=spec["url"], params=spec.get("params") or {},
                                  timeout=_SEARCH_TIMEOUT)
        except Exception as error:
            logger.warning(f"[music-scraper] 识别源请求异常：{spec.get('url')} — {error}")
            return None, _sources.UNREACHABLE
        if resp is None:
            return None, _sources.UNREACHABLE
        try:
            status = int(getattr(resp, "status_code", 0) or 0)
            try:
                payload = resp.json()
            except Exception:
                payload = None
        finally:
            try:
                resp.close()
            except Exception:
                pass
        if status >= 400:
            return payload, _sources.API_ERROR
        if not isinstance(payload, (dict, list)):
            # 2xx 但解不出对象：多半是被网关或平台的验证页截了，不是曲库给的答案
            return payload, _sources.UNREACHABLE
        return payload, None

    def _fetch_candidates(self, path: Path,
                                  identity: Optional[Dict[str, Any]] = None
                                  ) -> Tuple[List[dict], Optional[str]]:
        """逐个直连平台检索接口拉候选，返回 (候选, 服务侧故障原因)。

        按源顺序问，某源出到满分候选就立刻停止；只有在**没有任何源给出可解析答案**时才回报
        故障，否则一个源正常返回空列表就是"这份曲库查不到"的确定答案，要按文件失败记账。
        """
        if identity is None:
            identity = _local_identity(path, roots=self._identity_roots())
        if not identity["title"]:
            return [], None
        want = _want_from(identity)
        keyword = _sources.build_keyword(identity["title"], identity["artist"])
        merged: List[dict] = []
        best_score = 0
        answered = 0
        for resource in self._active_resources:
            spec = _sources.build_request(resource, keyword)
            if not spec:
                continue
            payload, error = self._source_request(spec)
            if error:
                continue
            candidates, parse_error = _sources.parse_candidates(resource, payload)
            if parse_error:
                # 平台改版或被截：这源没给出结论，继续问下一个
                continue
            # 只有解析成候选才算"这个源答了"：200 + JSON 但形状不认识（平台改版）
            # 和"曲库里查不到"是两件事 —— 把它算成答案，整库会被记成无候选、烧满重试
            # 后永久跳过，而该做的其实是插件这边跟着改版修解析。
            answered += 1
            for c in candidates:
                c["resource"] = resource
                merged.append(c)
                if best_score < _MAX_SCORE:
                    best_score = max(best_score, self._score_candidate(c, want))
            if best_score >= _MAX_SCORE:
                break
        if not merged and not answered:
            return [], _UNREACHABLE_REASON
        return merged, None

    # ---------------- 候选匹配 & 字段映射 ----------------

    def _pick_best(self, path: Path, candidates: List[dict],
                       identity: Optional[Dict[str, Any]] = None
                       ) -> Tuple[Optional[dict], int]:
        best, best_score, _ = self._best_candidate(path, candidates, identity=identity)
        return (best if best_score >= self._min_score else None), best_score

    def _best_candidate(self, path: Path, candidates: List[dict],
                            identity: Optional[Dict[str, Any]] = None
                            ) -> Tuple[Optional[dict], int, Optional[Dict[str, str]]]:
        """返回 (分最高的候选, 分数, 该候选的逐项判定)。

        分数不够时调用方要把"差在哪一项"写进失败原因：只知道 2/3 分，就分不清是
        候选本身对不上，还是本地压根没有这项可以去对（整张 CD 压成一个 wav 的文件
        就是后者），两种情况要做的决定完全相反。
        """
        if identity is None:
            identity = _local_identity(path, roots=self._identity_roots())
        want = _want_from(identity)
        best, best_score, best_hits = None, 0, None
        for c in candidates:
            hits = self._field_hits(c, want)
            score = sum(1 for state in hits.values() if state == _HIT)
            if best is None or score > best_score:
                best, best_score, best_hits = c, score, hits
        return best, max(best_score, 0), best_hits

    @staticmethod
    def _field_hits(c: dict, want: dict) -> Dict[str, str]:
        """沿用 Music Tag Web 的 utils.match_song 约定：曲名 / 艺人 / 专辑 逐项判定。

        每项是 hit（对上）/ miss（有得比但没对上）/ no_local（本地缺这项，永远比不了）。
        三者必须分开：no_local 记 0 分是"信息不全"，把它说成"没对上"会让人去改文件名，
        而真正该做的是把这类文件排除出扫描范围。
        """
        hits: Dict[str, str] = {}

        want_title = _norm_text(want.get("title"))
        got_title = _norm_text(c.get("name") or c.get("title"))
        if not want_title:
            hits["title"] = _NO_LOCAL
        else:
            hits["title"] = _HIT if (
                got_title and (want_title in got_title or got_title in want_title)
            ) else _MISS

        artists_raw = c.get("artists") or c.get("artist") or c.get("singer") or []
        if isinstance(artists_raw, (list, tuple)):
            got_artist = _norm_text(" ".join(
                str(a.get("name") if isinstance(a, dict) else a) for a in artists_raw
            ))
        else:
            got_artist = _norm_text(artists_raw)
        want_artist = _norm_text(want.get("artist"))
        if not want_artist:
            hits["artist"] = _NO_LOCAL
        else:
            hits["artist"] = _HIT if (
                got_artist and (want_artist in got_artist or got_artist in want_artist)
            ) else _MISS

        album_obj = c.get("album") or {}
        if isinstance(album_obj, dict):
            got_album = _norm_text(album_obj.get("name") or album_obj.get("title"))
        else:
            got_album = _norm_text(album_obj)
        want_album = _norm_text(want.get("album"))
        if not want_album:
            hits["album"] = _NO_LOCAL
        else:
            hits["album"] = _HIT if (
                got_album and (want_album in got_album or got_album in want_album)
            ) else _MISS

        return hits

    @staticmethod
    def _miss_text(hits: Optional[Dict[str, str]]) -> str:
        """把逐项判定写成一句人话：差哪几项、分别是因为什么。"""
        if not hits:
            return ""
        parts: List[str] = []
        for key, label in _MATCH_FIELDS:
            state = hits.get(key)
            if state == _HIT:
                continue
            if state == _NO_LOCAL:
                parts.append(f"本地无{label}")
            elif state == _MISS:
                parts.append(f"{label}未对上")
        return "、".join(parts)

    @staticmethod
    def _score_candidate(c: dict, want: dict) -> int:
        """沿用 Music Tag Web 的 utils.match_song 约定：title / artist / album 各 0/1。"""
        hits = MusicLibraryScraper._field_hits(c, want)
        return sum(1 for state in hits.values() if state == _HIT)

    def _fetch_lyric(self, source: str, picked: dict) -> Optional[str]:
        if not _sources.supports_lyrics(source):
            return None
        song_id = str(picked.get("id") or picked.get("song_id") or picked.get("mid") or "").strip()
        if not song_id:
            return None
        spec = _sources.build_lyric_request(source, song_id)
        if not spec:
            return None
        payload, err = self._source_request(spec)
        if err or not payload:
            return None
        return _sources.parse_lyric(source, payload)

    def _candidate_to_musicinfo(self, c: dict, path: Path) -> Optional[MusicInfo]:
        """把候选折叠成 MP MusicInfo。

        各源（netease / qmusic / kugou / migu / musicbrainz）在候选里字段命名不完全一致，
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
                "source": source,
                "song_id": song_id,
                "candidate": c,
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
