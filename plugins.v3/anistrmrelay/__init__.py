"""
ANi-Strm 自用版（内置 CF 透传中转）。

基于 honue/MoviePilot-Plugins 的 ANi-Strm 2.5.1（GPL-3.0）：按季度与番剧目录生成 STRM，
通过 Cloudflare 透传中转提供目录与媒体流，凭证由 FlareSolverr 自动维护。
致谢原作者 honue；源站与 Alist 风格目录接口由 open ANi 字幕组提供。
"""
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse
from urllib.request import getproxies

import httpx
import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.utils.http import RequestUtils

# open ANi 官方 worker 源站（内置中转的上游）
DEFAULT_UPSTREAM = "https://openani.an-i.workers.dev"
# 过 CF 挑战时浏览器实际使用的 User-Agent 提示（cf_clearance 与 UA 绑定）
DEFAULT_CF_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36 Edg/155.0.0.0"
)
# cf_clearance 保活心跳间隔（分钟），需明显小于 CF 凭证约30分钟的有效期
RELAY_HEARTBEAT_MINUTES = 5
# 保活连续失败容忍：单次心跳失败先按短间隔补试，连续达到该次数才判定凭证死亡并停机
RELAY_HEARTBEAT_MAX_MISSES = 2
# 保活失败后的补试间隔（分钟），覆盖 CF 短暂排队/网络抖动，远小于凭证有效期
RELAY_HEARTBEAT_RETRY_MINUTES = 3
# CF 风控升级容忍：FlareSolverr 已换到新证、探活仍被判挑战时按补试间隔重探，
# 连续达到该次数才认定该出口IP被 CF 持续拒绝并停用中转（约18分钟）
RELAY_ESCALATION_MAX_MISSES = 6
# FlareSolverr 凭证自动续期间隔（分钟）：cf_clearance 绝对有效期约30分钟且本域
# 不滚动续发，必须赶在到期前用 FlareSolverr 重新解题取新凭证。到点检查只发生在
# 5分钟一拍的心跳上且实测会被探活阻塞推迟（14分钟配置下实际出现过29分钟间隔），
# 按12分钟留出一拍余量，最坏间隔仍在有效期内
FLARESOLVER_REFRESH_MINUTES = 12
# 自动恢复重试间隔（分钟）：__relay_stop 摘除任务后，按此间隔自动排回心跳，
# 持续重试 FlareSolverr 解题，无需用户手动重载插件；0 或负值禁用自动恢复
DEFAULT_RELAY_AUTO_RECOVER_MINUTES = 15
# 自动恢复最大连续失败次数：连续 N 次解题+探活均失败才彻底放弃，避免 FlareSolverr
# 长期不可用时反复白烧解题时间；每次成功探活计数清零；0 表示不限制
DEFAULT_RELAY_AUTO_RECOVER_MAX_ATTEMPTS = 8
# 同一拍内刚换到的新证被探活判挑战页时的冷却（秒）：CF 对出口IP的短时风控升级会连新证
# 一起拦（同时段中转大批并发请求同样命中挑战页、FlareSolverr 自己也解不出题），此时立刻
# 二次解题只会白烧 140 秒并把还能用的证顶掉，改为交给短间隔补试重探
FS_RERESOLVE_COOLDOWN_SECONDS = 90
# 定时更新（strm补齐）默认周期：每天 04:00，避开观看高峰；置空则禁用
DEFAULT_CRON = "0 4 * * *"
# CF 挑战页特征标记
CHALLENGE_MARKS = (b"Just a moment", b"challenge-platform")
# 中转转发时透传的客户端请求头（UA/Cookie 一律由插件注入，其余精简避免指纹干扰）
RELAY_PASSTHROUGH_REQ_HEADERS = frozenset(
    {"range", "if-range", "content-type", "accept", "accept-language"}
)
# 中转转发时透传的上游响应头
RELAY_PASSTHROUGH_RESP_HEADERS = frozenset(
    {"range", "content-type", "content-length", "content-range", "accept-ranges"}
)
# 中转流式读取块大小
RELAY_CHUNK_SIZE = 64 * 1024
# 目录列举请求的最小间隔（秒）。openani 源站曾因高频目录请求超配额被整域强制人机
# 挑战（honue/MoviePilot-Plugins#147），批量回填多季度时按此间隔温和限速
FOLDER_REQUEST_INTERVAL_SECONDS = 1.0
# 源站目录实测145秒后才返回200；外层需覆盖两次180秒读取及连接/写入/池等待，不能20秒先断。
FOLDER_REQUEST_TIMEOUT_SECONDS = 480.0
RELAY_CONTROL_READ_TIMEOUT_SECONDS = 180.0

# 全进程共享的 httpx 异步客户端（目录HTTP/2、媒体HTTP/1.1；
# 连接池常驻复用，插件重载不关闭以避免跨事件循环竞态）。
# 媒体文件流与控制面（目录列举/自检）分池：播放器快进、停止会中断媒体流并可能触发
# Cloudflare 连接级重置，同池的目录列举会被连带打挂（整季回填表现为全线文件数=0）
_RELAY_CLIENT: Optional[httpx.AsyncClient] = None
_RELAY_CONTROL_CLIENT: Optional[httpx.AsyncClient] = None
_RELAY_CLIENT_PROXY: Optional[str] = None
_RELAY_CONTROL_CLIENT_PROXY: Optional[str] = None

# 进程级滚动凭证记忆：保存本插件捕获到的最新 cf_clearance、配套UA及其签发时间。
# 插件配置为整对象替换语义，外部以旧快照更新配置会把刚捕获的滚动凭证顶回死 token；
# 该记忆跨插件重载存活，init 时与配置值按签发时间戳择优，防止滚动续命被写回抹除。
# UA 同样要记忆：cf_clearance 与解题浏览器UA成对绑定，配置整对象替换丢掉UA时
# 必须沿用同一次解题的UA，否则新证配旧UA会直接被重新校验
_CLEARANCE_MEMORY: Dict[str, Any] = {"token": "", "ua": "", "issued_ts": 0}

# 进程级回填互斥记号：插件重载会先后出现两个实例，各自排一轮"保存后自动补齐"，
# 两轮全季度目录列举同时在跑等于把请求速率翻倍打到CF（实测正是整季读超时的诱因），
# 因此同一进程内只允许一轮回填在跑。holder 用实例id，超过时限视为上一轮已卡死可接管
_BACKFILL_GUARD: Dict[str, Any] = {"holder": 0, "ts": 0.0}
_BACKFILL_LOCK = threading.Lock()
# 回填互斥记号的失活判定（秒）：单季最慢环节是目录重试链，远小于该值
BACKFILL_GUARD_STALE_SECONDS = 600


def _clearance_issued_ts(token: str) -> int:
    """
    解析 cf_clearance 内嵌的签发时间戳。

    CF 常见写法是 xxx-1793150056-1.2.1.1-xxx（夹在两个连字符间的十位 unix 秒）；
    实测凭证位数/分隔会随 CF 版本变化，解析不到就退化到整串里任一段落在
    2023~2033 年范围内的十位数字，再退不到才返回 0。

    :param token: cf_clearance 裸值
    :return: unix 秒级时间戳，解析不到返回 0
    """
    token = token or ""
    match = re.search(r"-(\d{10})-", token) or re.search(r"(?<!\d)(1[7-9]\d{8})(?!\d)", token)
    return int(match.group(1)) if match else 0


def _new_relay_client(proxy: Optional[str] = None, control: bool = False) -> httpx.AsyncClient:
    """
    新建中转客户端：目录使用HTTP/2，媒体使用HTTP/1.1隔离播放器提前断开的影响。

    :return: httpx.AsyncClient 实例
    """
    return httpx.AsyncClient(
        # HTTP/2播放流被ffprobe/播放器提前关闭后，复用连接实测会仅到响应头而正文停流。
        # HTTP/1.1未读完的响应会关闭该连接，不让探测/快进污染其它播放请求。
        http2=control,
        follow_redirects=True,
        timeout=httpx.Timeout(
            15.0, read=RELAY_CONTROL_READ_TIMEOUT_SECONDS if control else 300.0,
            write=15.0 if control else 60.0, pool=15.0,
        ),
        proxy=proxy,
        trust_env=False,
    )


def _relay_client(control: bool = False, proxy: Optional[str] = None) -> httpx.AsyncClient:
    """
    获取（必要时创建）中转专用的共享异步 HTTP 客户端。

    :param control: True 取目录列举/自检等控制面请求池，False 取媒体文件流池
    :return: 目录HTTP/2或媒体HTTP/1.1、自动跟随重定向的 httpx.AsyncClient 单例
    """
    global _RELAY_CLIENT, _RELAY_CONTROL_CLIENT
    global _RELAY_CLIENT_PROXY, _RELAY_CONTROL_CLIENT_PROXY
    if control:
        if (_RELAY_CONTROL_CLIENT is None or _RELAY_CONTROL_CLIENT.is_closed
                or _RELAY_CONTROL_CLIENT_PROXY != proxy):
            _RELAY_CONTROL_CLIENT = _new_relay_client(proxy, control=True)
            _RELAY_CONTROL_CLIENT_PROXY = proxy
        return _RELAY_CONTROL_CLIENT
    if _RELAY_CLIENT is None or _RELAY_CLIENT.is_closed or _RELAY_CLIENT_PROXY != proxy:
        _RELAY_CLIENT = _new_relay_client(proxy)
        _RELAY_CLIENT_PROXY = proxy
    return _RELAY_CLIENT


def _drop_relay_client(control: bool = False, proxy: Optional[str] = None) -> None:
    """
    摘除中转出站客户端引用，让下一次请求在全新连接上重开。

    Cloudflare 以 RST_STREAM 拆连接后，同一条 HTTP/2 连接上后续所有流都会被连带
    重置；摘掉引用即可自愈。不主动 aclose：那会打断同一请求池上其它正在播放的
    媒体流，旧客户端由其上的流自然结束后回收。

    :param control: True 摘控制面请求池，False 摘媒体文件流池
    """
    global _RELAY_CLIENT, _RELAY_CONTROL_CLIENT
    if control and _RELAY_CONTROL_CLIENT_PROXY == proxy:
        _RELAY_CONTROL_CLIENT = None
    elif not control and _RELAY_CLIENT_PROXY == proxy:
        _RELAY_CLIENT = None


class ANiStrmRelay(_PluginBase):
    plugin_name = "ANi Strm 自用版（CF中转）"
    plugin_desc = "自动获取当季所有番剧生成strm；内置CF中转代理携带cf_clearance直连openani，凭证由FlareSolverr全自动解题续期（插件设置或MP系统设置提供地址，未配置则不启用），无需粘贴cookie（基于 honue 版 ANi-Strm 迭代）"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "1.1.0"
    plugin_author = "JontyLee（基于 honue ANi-Strm）"
    author_url = "https://github.com/JontyLee"
    plugin_config_prefix = "anistrm_"
    plugin_order = 15
    auth_level = 2

    _enabled = False
    _use_proxy = True
    _cron = DEFAULT_CRON
    _storageplace = None
    _selected_seasons: List[str] = []
    _proxy_base = None
    _scheduler: Optional[BackgroundScheduler] = None
    # ↓ 内置 CF 中转相关配置 ↓
    _relay_enabled = False
    _cf_clearance = ""
    _cf_user_agent = ""
    _relay_token = ""
    _relay_public_base = ""
    _rewrite_existing = False
    # FlareSolverr 服务地址（如 http://<地址>:8191）：配置后 cf_clearance 到期前
    # 由它自动重新解题续发新证与配套UA，cookie 设置项可留空
    _fs_url = ""
    # 用户在插件设置里显式填写的 FlareSolverr 地址：回写配置只写这个原始值，
    # 免得自动采用的系统值被固化进插件配置、此后系统改地址也跟不动
    _fs_url_explicit = ""
    # 上次 FlareSolverr 续期成功时间戳（心跳线程据此判断周期到点）
    _fs_last_ts = 0.0
    # 内置中转实际转发的上游源站（即原始 proxy_base 配置值）
    _relay_upstream = ""
    # 本轮保活连续失败次数（成功后清零，达到阈值才停机）
    _heartbeat_misses = 0
    # 连续"新证仍被CF判挑战"的风控升级次数（探活成功后清零，达到阈值才停用中转）
    _escalation_misses = 0
    # 中转命中挑战页的上次告警时间戳：CF风控升级时并发播放请求会成片命中，逐条打印会淹掉日志
    _last_challenge_warn_ts = 0.0
    # 自动恢复相关
    _auto_recover_attempts = 0
    _auto_recover_day: str = ""  # 记录本轮自动恢复所属日期（YYYY-MM-DD），跨天自动重置计数
    _relay_auto_recover_minutes: int = DEFAULT_RELAY_AUTO_RECOVER_MINUTES
    _relay_auto_recover_max: int = DEFAULT_RELAY_AUTO_RECOVER_MAX_ATTEMPTS

    def __init__(self):
        super().__init__()
        self._client = AniStrmClient()
        self._strm_service = StrmFileService()
        # 待处理的凭证急换请求：中转回源命中挑战页时置位，由保活心跳线程消费
        self._urgent_refresh = threading.Event()
        # 插件停止/重载标记：回填线程按季度检查此位，及时让位给新实例而不是继续打源站
        self._task_should_stop = False
        self._config_generation = 0
        self._relay_ready = False
        self._relay_pause_reason = ""
        self._startup_pull_pending = False
        self._state_lock = threading.RLock()

    def init_plugin(self, config: dict = None):
        """
        读取配置并注册定时任务；开启内置中转时把目录读取与strm生成地址切换到中转入口。

        :param config: 插件配置字典
        """
        self.stop_service()
        # 同一实例被重新初始化（保存配置）时要重新允许它跑回填
        self._task_should_stop = False
        self._urgent_refresh.clear()

        config = config or {}
        # 用户配置表示开启意图，缺依赖/CF 故障只改变运行状态，不能反写为用户关掉了开关。
        self._enabled_requested = bool(config.get("enabled", False))
        self._enabled = self._enabled_requested
        use_proxy = config.get("use_proxy")
        self._use_proxy_requested = True if use_proxy is None else bool(use_proxy)
        self._use_proxy = self._use_proxy_requested
        cron = config.get("cron")
        self._cron = DEFAULT_CRON if cron is None else (cron or "").strip()
        self._storageplace = config.get("storageplace") or "/downloads/strm"
        if "selected_seasons" in config:
            self._selected_seasons = config.get("selected_seasons") or []
        else:
            self._selected_seasons = ["latest"]
        self._proxy_base = config.get("proxy_base") or DEFAULT_UPSTREAM
        self._relay_requested = bool(config.get("relay_enabled"))
        self._relay_enabled = self._relay_requested
        self._relay_ready = False
        self._relay_pause_reason = ""
        self._startup_pull_pending = self._enabled_requested
        self._escalation_misses = 0
        raw_clearance = (config.get("cf_clearance") or "").strip()
        self._cf_clearance = self._normalize_clearance(raw_clearance)
        # 进程级记忆里有更新的滚动凭证时优先采用：防止整对象替换的配置写回
        # 把心跳刚捕获的新 token 顶成旧快照里的死 token（顶号即全灭）
        use_memory_clearance = bool(_CLEARANCE_MEMORY["token"]
                and _CLEARANCE_MEMORY["issued_ts"] > _clearance_issued_ts(self._cf_clearance))
        if use_memory_clearance:
            logger.info(
                f"ANi-Strm采用进程级滚动凭证（签发于"
                f"{datetime.fromtimestamp(_CLEARANCE_MEMORY['issued_ts'])}），"
                f"忽略配置快照中更旧的cf_clearance"
            )
            self._cf_clearance = _CLEARANCE_MEMORY["token"]
        if raw_clearance and self._cf_clearance != raw_clearance:
            logger.info(
                "ANi-Strm检测到cf_clearance配置值粘贴了整串浏览器Cookie，"
                "已自动提取其中的cf_clearance生效"
            )
        # FlareSolverr 地址：插件设置优先，留空则自动采用 MP 系统设置里的服务地址
        self._fs_url_explicit = (config.get("fs_url") or "").strip()
        self._fs_url = self._fs_url_explicit or self.__system_flaresolverr_url()
        # UA 由解题浏览器提供、与 cf_clearance 成对生效：配置整对象替换丢掉该项时
        # 沿用进程记忆里同一次解题的UA，都没有才退到内置默认UA
        self._cf_user_agent = (
            (_CLEARANCE_MEMORY["ua"] if use_memory_clearance else "")
            or (config.get("cf_user_agent") or "").strip()
            or _CLEARANCE_MEMORY["ua"]
            or DEFAULT_CF_UA
        )
        self._relay_token = (config.get("relay_token") or "").strip()
        self._relay_public_base = (config.get("relay_public_base") or "").strip().rstrip("/")
        self._rewrite_existing = bool(config.get("rewrite_existing"))
        self._relay_proxy = (config.get("relay_proxy") or "").strip()
        # 自动恢复配置：relay_auto_recover_minutes ≤ 0 表示禁用自动恢复（完全依赖手动）
        _rac_min_raw = config.get("relay_auto_recover_minutes")
        self._relay_auto_recover_minutes = (
            DEFAULT_RELAY_AUTO_RECOVER_MINUTES if _rac_min_raw is None else int(_rac_min_raw or 0)
        )
        _rac_max_raw = config.get("relay_auto_recover_max")
        self._relay_auto_recover_max = (
            DEFAULT_RELAY_AUTO_RECOVER_MAX_ATTEMPTS if _rac_max_raw is None else int(_rac_max_raw or 0)
        )
        self._upstream_proxy = None
        self._upstream_proxy_source = "TUN/直连"
        self._upstream_proxy_error = ""
        try:
            self._upstream_proxy = self.__resolve_upstream_proxy()
        except ValueError as err:
            self._upstream_proxy_error = str(err)
            if self._relay_requested:
                self._enabled = False
                self._relay_enabled = False
                self._relay_pause_reason = str(err)
                logger.warning(f"ANi-Strm中转出站代理配置不可用，运行已暂停：{err}")

        # FlareSolverr 是凭证的唯一来源：cf_clearance 约30分钟绝对到期且本域不滚动续发，
        # 手工粘贴撑不住持续播放。没有可用地址就带着死证上线，只会把源站打成整轮人机挑战，
        # 因此此时连定时拉取一起不启用
        if (self._enabled or self._relay_enabled) and not self._fs_url:
            logger.warning(
                "ANi-Strm未取到FlareSolverr地址（插件设置与MP系统设置的站点分组都是空的），"
                "cf_clearance无法自动续期，插件与内置中转均不启用"
            )
            self._enabled = False
            self._relay_enabled = False
            self._relay_pause_reason = "未配置FlareSolverr地址，运行已暂停；配置开关保持不变"

        # 中转转发目标始终是原始反代/源站地址
        self._relay_upstream = self._proxy_base
        effective_base = self._proxy_base
        if self._relay_enabled:
            if self._relay_public_base:
                # 目录列表与 strm 文件统一走本插件的中转入口，凭据只在转发时注入
                effective_base = self._relay_public_base
                # 中转入口是宿主机内网地址，不能再走外部代理
                self._use_proxy = False
            else:
                logger.warning("ANi-Strm已开启内置中转但未配置中转访问地址，暂停拉取与中转，不回退直连")
                self._relay_enabled = False
                self._enabled = False
                self._relay_pause_reason = "未配置中转访问地址，运行已暂停；配置开关保持不变"
        self._client.set_use_proxy(self._use_proxy)
        self._client.set_proxy_base(effective_base)
        # 目录请求同样经中转入口时必须携带 rt 令牌，否则被中转 403 拦下
        self._client.set_query_suffix(
            f"rt={self._relay_token}" if (self._relay_enabled and self._relay_token) else ""
        )
        logger.info(
            f"ANi-Strm配置加载：enabled={self._enabled}, "
            f"use_proxy={self._use_proxy}, base={effective_base}, "
            f"relay={self._relay_enabled}(upstream={self._relay_upstream}, "
            f"cookie={'已配置' if self._cf_clearance else '未配置'}, "
            f"flaresolverr={self._fs_url or '未配置'}), "
            f"seasons={self._selected_seasons or []}, storage={self._storageplace}, "
            f"upstream_proxy={self._upstream_proxy_source}"
        )
        # 每次初始化（重启/保存配置）重置当日自动恢复计数：视为新的一天从头再来
        self._auto_recover_attempts = 0
        self._auto_recover_day = ""
        self.__update_config()

        if not (self._enabled or self._relay_enabled):
            logger.info("ANi-Strm未启用且未开启中转，跳过任务注册")
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if self._enabled and self._cron:
            try:
                self._scheduler.add_job(
                    func=self.__task,
                    trigger=CronTrigger.from_crontab(self._cron),
                    name="ANiStrmRelay定时更新",
                    id="anistrm_relay_update",
                    args=[self._config_generation],
                )
                logger.info(f"ANi-Strm定时更新任务已注册：{self._cron}")
            except Exception as err:
                logger.error(f"ANi-Strm定时任务配置错误：{err}")

        if self._relay_enabled:
            self._heartbeat_misses = 0
            if self._fs_url:
                # FlareSolverr 自动续期由保活心跳线程编排：首拍即换证、周期到点换证、
                # 挑战页急换都在同一线程串行，避免与独立续期任务重复解题
                self._fs_last_ts = 0.0
                logger.info(
                    f"ANi-Strm已启用FlareSolverr凭证自动续期（{self._fs_url}，"
                    f"来源：{'插件设置' if self._fs_url_explicit else '系统设置'}），"
                    f"cf_clearance与UA无需手工粘贴，首拍换证后每"
                    f"{FLARESOLVER_REFRESH_MINUTES}分钟换证"
                )
            # 保存/插件重载/MP重启后都会重走本初始化：约15秒即打第一拍探活，
            # 尽早定性cf_clearance并捕获滚动凭证，不等完整心跳间隔
            self._scheduler.add_job(
                func=self.__relay_heartbeat,
                trigger=IntervalTrigger(minutes=RELAY_HEARTBEAT_MINUTES),
                name="ANiStrmRelay中转保活",
                id="anistrm_relay_heartbeat",
                next_run_time=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=15),
                args=[self._config_generation],
            )
            logger.info(f"ANi-Strm中转cf_clearance保活任务已注册（首拍15秒后，此后每{RELAY_HEARTBEAT_MINUTES}分钟）")

        # 解题实测会超过100秒，固定延迟45秒并不能保证拿到有效凭证。
        # 开启中转时由首次成功心跳排启动补齐；未开中转则立即排程。
        if self._enabled:
            if self._relay_requested:
                logger.info("ANi-Strm启动补齐等待FlareSolverr解题及凭证探活成功，不提前拉取目录")
            else:
                self.__schedule_startup_pull()

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def __generation_active(self, generation: int) -> bool:
        """停止和配置重载都会使旧任务失效，避免它们覆盖新一轮的状态。"""
        return not self._task_should_stop and generation == self._config_generation

    def __schedule_startup_pull(self, delay: int = 3):
        """每轮初始化只排一次补齐；中转模式必须先完成凭证验证。"""
        if not (self._enabled and self._startup_pull_pending and self._scheduler):
            return
        if self._relay_requested and not self._relay_ready:
            return
        self._scheduler.add_job(
            func=self.__task,
            trigger="date",
            run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=delay),
            name="ANiStrmRelay保存后自动补齐",
            id="anistrm_relay_startup_pull",
            args=[self._config_generation],
            replace_existing=True,
        )
        logger.info(f"ANi-Strm启动补齐已排程（{delay}秒后；中转凭证已验证或未启用中转）")

    def __task(self, generation: Optional[int] = None):
        """
        拉取目标季度目录并补齐（或按配置改写）strm 文件。

        同一进程内只允许一轮回填在跑：插件重载会先后存在两个实例，各自排一轮
        「保存后自动补齐」，两轮目录列举并发跑等于把请求速率翻倍打到CF，实测正是
        整季读超时（文件数=0）的诱因。
        """
        generation = self._config_generation if generation is None else generation
        if not self.__generation_active(generation) or not self._enabled:
            return
        if self._relay_requested and not self._relay_ready:
            logger.info("ANi-Strm中转凭证尚未验证，跳过本次目录拉取")
            return
        run_key = id(self)
        with _BACKFILL_LOCK:
            holder = _BACKFILL_GUARD["holder"]
            busy = bool(holder and (
                time.time() - _BACKFILL_GUARD["ts"] < BACKFILL_GUARD_STALE_SECONDS
            ))
            if not busy:
                _BACKFILL_GUARD["holder"] = run_key
                _BACKFILL_GUARD["ts"] = time.time()
        if busy:
            logger.info(
                f"ANi-Strm已有另一轮回填在跑（{int(time.time() - _BACKFILL_GUARD['ts'])}秒前开始），"
                f"本实例本轮跳过，避免目录请求并发翻倍冲击CF"
            )
            with self._state_lock:
                if self.__generation_active(generation) and self._startup_pull_pending:
                    self.__schedule_startup_pull(delay=30)
            return
        try:
            with self._state_lock:
                if not self.__generation_active(generation):
                    return
                self._startup_pull_pending = False
                if self._scheduler and self._scheduler.get_job("anistrm_relay_startup_pull"):
                    self._scheduler.remove_job("anistrm_relay_startup_pull")
            self.__run_task(generation)
        finally:
            if _BACKFILL_GUARD["holder"] == run_key:
                _BACKFILL_GUARD["holder"] = 0
                _BACKFILL_GUARD["ts"] = 0.0

    def __run_task(self, generation: int):
        """
        按季度回填 strm 的执行体，由 __task 串行调度。
        """
        seasons = self.__get_target_seasons()
        if not self.__generation_active(generation):
            return
        if not seasons:
            logger.info("未选择任何季度，任务结束")
            return

        logger.info(
            f"ANi-Strm任务开始：seasons={seasons}, storage={self._storageplace}, "
            f"proxy={self._client._get_openani_base()}, use_proxy={self._use_proxy}"
        )

        total_files = 0
        total_created = 0
        total_exists = 0
        total_updated = 0
        total_failed = 0
        total_missing_series = 0

        query_suffix = f"rt={self._relay_token}" if (self._relay_enabled and self._relay_token) else None

        # 连续空季检测：CF 降级/凭证失效时 get_season_entries 返回空列表，若继续逐季
        # 重试会在源站排队期反复消耗请求配额（实测 31 季全线 503 约 15 分钟），
        # 连续 2 个季节为空即判定源站当前不可用，跳过剩余季度避免无效冲击。
        consecutive_empty_seasons = 0
        MAX_CONSECUTIVE_EMPTY_SEASONS = 2

        for season in seasons:
            # 插件被停止/重载时本线程还在跑：立即让位，别继续与新实例并发打源站
            if not self.__generation_active(generation) or not self._enabled:
                logger.info("ANi-Strm插件已停止或重载，本轮回填提前结束")
                return
            _BACKFILL_GUARD["ts"] = time.time()
            file_entries = self._client.get_season_entries(season)
            if not self.__generation_active(generation) or not self._enabled:
                return
            season_total = len(file_entries)
            if season_total == 0:
                consecutive_empty_seasons += 1
                if consecutive_empty_seasons >= MAX_CONSECUTIVE_EMPTY_SEASONS:
                    remaining = seasons[seasons.index(season) + 1:]
                    logger.warning(
                        f"ANi-Strm季度 {season} 连续第 {consecutive_empty_seasons} 次为空，"
                        f"判定源站当前不可用（CF降级或凭证失效），"
                        f"跳过剩余 {len(remaining)} 个季度：{remaining[:5]}"
                        f"{'...' if len(remaining) > 5 else ''}"
                    )
                    total_missing_series += 1  # 计为整体未完成
                    break
            else:
                consecutive_empty_seasons = 0
            season_created = 0
            season_exists = 0
            season_updated = 0
            season_failed = 0
            logger.info(f"ANi-Strm开始处理季度：{season}，文件数={season_total}")
            missing_series = self._client.pop_failed_series()
            if missing_series:
                total_missing_series += len(missing_series)
                logger.warning(
                    f"ANi-Strm季度 {season} 有{len(missing_series)}个目录未取得，"
                    f"本季按其余条目补齐：{missing_series[:5]}"
                )
            for file_entry in file_entries:
                if not self.__generation_active(generation) or not self._enabled:
                    return
                status = self._strm_service.touch_strm_file(
                    storage_path=self._storageplace,
                    file_name=file_entry["name"],
                    file_url=file_entry["url"],
                    relative_dir=file_entry.get("relative_dir"),
                    query_suffix=query_suffix,
                    rewrite_existing=self._rewrite_existing,
                )
                if status == "created":
                    season_created += 1
                elif status == "exists":
                    season_exists += 1
                elif status == "updated":
                    season_updated += 1
                else:
                    season_failed += 1

            total_files += season_total
            total_created += season_created
            total_exists += season_exists
            total_updated += season_updated
            total_failed += season_failed
            logger.info(
                f"ANi-Strm季度处理完成：{season}，总数={season_total}，"
                f"新增={season_created}，跳过={season_exists}，改写={season_updated}，失败={season_failed}"
            )

        logger.info(
            f"ANi-Strm任务完成：季度数={len(seasons)}，文件总数={total_files}，"
            f"新增={total_created}，跳过={total_exists}，改写={total_updated}，失败={total_failed}"
        )

        # 全季度回填只应是一次性动作：完成一轮完整遍历后自动切回最新季，避免定时任务反复全量拉取冲击配额
        with self._state_lock:
            if not self.__generation_active(generation):
                return
            if "all" in self._selected_seasons and total_files > 0:
                if not total_failed and not total_missing_series:
                    logger.info("ANi-Strm全季度回填完成，拉取季度已自动切回最新季")
                else:
                    logger.warning(
                        f"ANi-Strm全季度回填有异常（失败={total_failed}，"
                        f"目录未取得={total_missing_series}），已切回最新季；"
                        "缺失条目可在 CF 凭证恢复后重新选择全部季度手动补齐"
                    )
                self._selected_seasons = ["latest"]
                self.__update_config(generation=generation)

    def __get_target_seasons(self) -> List[str]:
        """
        展开选择项为实际季度列表（latest 解析为远端最新季，all 展开为全部可用季度）。

        :return: 去重后的季度名列表
        """
        if self._selected_seasons:
            seasons: List[str] = []
            for season in self._selected_seasons:
                if season == "latest":
                    latest = self._client.get_current_season()
                    if latest:
                        seasons.append(latest)
                elif season == "all":
                    seasons.extend(self._client.get_available_seasons())
                else:
                    seasons.append(season)
            return list(dict.fromkeys(seasons))
        return []

    def get_current_season_list(self) -> List[str]:
        """
        获取当前季度目录下的文件名列表。

        :return: 文件名列表
        """
        return self._client.get_current_season_list()

    def get_state(self) -> bool:
        """
        插件是否处于启用状态。

        :return: 启用为 True
        """
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        注册内置 CF 中转发接口。

        播放链路（Emby/Jellyfin 按 strm 直连）不经过宿主网络栈，因此中转入口必须
        允许匿名访问，由可选的 rt 令牌限制滥用；路径参数固定转发到本插件配置的上游源站。
        """
        return [
            {
                "path": "/relay/{full_path:path}",
                "endpoint": self.api_relay,
                "methods": ["GET", "HEAD", "POST"],
                "allow_anonymous": True,
                "summary": "ANi CF透传中转",
                "description": "携带cf_clearance与匹配UA，将strm播放与目录列表请求透传至上游源站",
            },
            {
                "path": "/relay",
                "endpoint": self.api_relay_selfcheck,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "中转自检与地址提示",
                "description": "探测上游连通性并给出应填入「中转访问地址」的完整前缀",
            },
        ]

    @staticmethod
    def __system_flaresolverr_url() -> str:
        """
        读取 MP 系统设置中的 FlareSolverr 地址，作为插件未单独填写时的默认来源。

        系统设置项由 Settings.update_setting 同时写 app.env 与内存实例，插件在加载
        时读到的就是当前生效值；在系统设置的站点分组里配过 FlareSolverr 就不必再填本项。

        :return: 系统配置的 FlareSolverr 地址，未配置返回空串
        """
        return (getattr(settings, "FLARESOLVERR_URL", None) or "").strip()

    def __resolve_upstream_proxy(self) -> Optional[str]:
        """解题、探活和回源共用一个代理策略，内网控制 API 始终独立直连。"""
        scheme = urlparse(self._proxy_base).scheme.lower()
        proxy = self._relay_proxy
        if proxy:
            self._upstream_proxy_source = "插件设置"
        else:
            environment = getproxies()
            proxy = environment.get(scheme) or environment.get("all")
            if proxy:
                self._upstream_proxy_source = "环境代理"
            elif self._use_proxy_requested and settings.PROXY:
                proxy = settings.PROXY.get(scheme)
                if proxy:
                    self._upstream_proxy_source = "MP系统代理"
        if not proxy:
            return None
        parsed = urlparse(proxy)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("中转出站须使用http://或https://代理（建议sing-box mixed入站）")
        if parsed.username or parsed.password:
            raise ValueError("FlareSolverr临时浏览器不支持代理认证，请使用受内网访问限制的无认证入口")
        return proxy

    @staticmethod
    def _normalize_clearance(raw: str) -> str:
        """
        容错解析用户粘贴的凭证：只接受整串浏览器 Cookie 误粘的场景。

        cf_clearance 与出口IP+UA绑定，注入时必须是裸 token；浏览器开发者工具里
        常见的"复制整行 Cookie"会带上 _ga 等其它键值，直接注入会拼出畸形请求头，
        CF 必然继续挑战。此处从多 cookie 串中提取 cf_clearance 的值，纯 token
        输入原样返回。

        :param raw: 设置项中的原始输入
        :return: 仅含 cf_clearance token 的字符串（找不到时返回空串或原值）
        """
        raw = (raw or "").strip()
        if not raw:
            return ""
        if "cf_clearance=" not in raw and ";" not in raw:
            return raw
        for segment in raw.split(";"):
            name, _, value = segment.strip().partition("=")
            if name.strip().lower() == "cf_clearance":
                return value.strip()
        return raw

    def _rotate_clearance(self, resp: httpx.Response, generation: Optional[int] = None,
                          credentials: Optional[Tuple[str, str]] = None) -> None:
        """
        跟随 Cloudflare 的滚动续期：放行响应重新下发 cf_clearance 时热替换内存凭证。

        仅在响应实际下发新 cf_clearance 时更新，不假定源站会滚动续期；
        周期换证仍由 FlareSolverr 完成。撤销类空值（DELETED/空串）忽略，
        保留旧值等待自动换证与探活。
        捕获即持久化：滚动仅在凭证真正变化时发生，写入并不频繁，而插件重载会
        丢失内存值——若不立即落库，重载后将继续携带已被滚动作废的旧凭证导致挑战。

        :param resp: 已收到响应头的上游请求（可为未读取的流式响应）
        """
        generation = self._config_generation if generation is None else generation
        if not self.__generation_active(generation):
            return
        new_value = ""
        for set_cookie in resp.headers.get_list("set-cookie"):
            name, _, value = set_cookie.split(";", 1)[0].partition("=")
            if name.strip().lower() == "cf_clearance":
                new_value = value.strip()
        if not new_value or new_value.upper() == "DELETED" or new_value == self._cf_clearance:
            return
        with self._state_lock:
            if not self.__generation_active(generation):
                return
            # 在途响应只能刷新发出该请求时的那组凭证，不能顶掉FlareSolverr刚换的新证。
            if credentials is not None and credentials != (self._cf_clearance, self._cf_user_agent):
                return
            self._cf_clearance = new_value
            _CLEARANCE_MEMORY["token"] = new_value
            _CLEARANCE_MEMORY["ua"] = credentials[1] if credentials else self._cf_user_agent
            _CLEARANCE_MEMORY["issued_ts"] = _clearance_issued_ts(new_value) or int(time.time())
            logger.info("ANi-Strm中转已捕获滚动刷新的cf_clearance（热生效并已持久化）")
            self.__update_config(generation=generation)

    def __fs_due(self) -> bool:
        """
        判断 FlareSolverr 周期续期是否到点（相对上次成功时间达到间隔）。

        :return: 到点返回 True；本次执行后推迟时间戳，由调用方更新
        """
        if not self._fs_url:
            return False
        if self._fs_last_ts and time.time() - self._fs_last_ts < FLARESOLVER_REFRESH_MINUTES * 60:
            return False
        return True

    def __fs_refresh(self):
        """
        FlareSolverr 凭证续期：调用其解完 CF 挑战后回传的 cf_clearance 与配套 UA 成对热替换。

        cf_clearance 与出口IP+UA绑定且本域不滚动续发，绝对有效期约30分钟；
        FlareSolverr 常驻 profile 实测可通过该域的托管挑战。凭证、UA 与内置默认值
        三者必须同换同用，续期失败只告警不改现有值，由保活心跳兜底判定停机。

        :return: 是否成功取到新凭证
        """
        generation = self._config_generation
        if not self._fs_url or not self.__generation_active(generation):
            return False
        err: Optional[Exception] = None
        resp: Optional[httpx.Response] = None
        payload = {
            "cmd": "request.get",
            "url": self._relay_upstream.rstrip("/") + "/",
            "maxTimeout": 140000,
        }
        if self._upstream_proxy:
            payload["proxy"] = {"url": self._upstream_proxy}
        for attempt in (1, 2):
            if not self.__generation_active(generation):
                return False
            try:
                resp = httpx.post(
                    self._fs_url.rstrip("/") + "/v1",
                    json=payload,
                    timeout=180.0,
                    trust_env=False,
                )
                if not self.__generation_active(generation):
                    return False
                err = None
                break
            except Exception as first_err:
                if not self.__generation_active(generation):
                    return False
                err = first_err
                if attempt == 1:
                    logger.warning(f"ANi-Strm FlareSolverr续期首次异常（{first_err}），30秒后重试一次")
                    time.sleep(30)
        if err is not None or resp is None:
            logger.warning(f"ANi-Strm FlareSolverr续期失败（保留现有凭证）：{err}")
            return False
        try:
            payload = resp.json()
        except Exception as parse_err:
            logger.warning(f"ANi-Strm FlareSolverr响应解析失败：{parse_err}")
            return False
        if payload.get("status") != "ok":
            logger.warning(
                f"ANi-Strm FlareSolverr未解题成功：{str(payload.get('message'))[:120]}"
            )
            return False
        solution = payload.get("solution") or {}
        clearance = ""
        for cookie in solution.get("cookies") or []:
            if cookie.get("name") == "cf_clearance":
                clearance = cookie.get("value") or ""
                break
        ua = solution.get("userAgent") or ""
        if not clearance or not ua:
            logger.warning("ANi-Strm FlareSolverr响应缺少cf_clearance或UA，忽略本次续期")
            return False
        with self._state_lock:
            if not self.__generation_active(generation):
                return False
            self._relay_ready = False
            self._cf_clearance = clearance
            self._cf_user_agent = ua
            _CLEARANCE_MEMORY["token"] = clearance
            _CLEARANCE_MEMORY["ua"] = ua
            _CLEARANCE_MEMORY["issued_ts"] = _clearance_issued_ts(clearance) or int(time.time())
            self._fs_last_ts = time.time()
            logger.info(
                f"ANi-Strm已从FlareSolverr取得新的cf_clearance（签发于"
                f"{datetime.fromtimestamp(_CLEARANCE_MEMORY['issued_ts'])}，配套UA已同步，热生效并已持久化）"
            )
            self.__update_config(generation=generation)
        return True

    def __relay_heartbeat(self, generation: Optional[int] = None):
        """
        cf_clearance 保活心跳：定期以解题浏览器UA+凭证直连上游探活，顺带捕获滚动刷新的新凭证。

        配置了 FlareSolverr 时兼任续期执行器：首拍即换证、周期到点换证、探活失败后的
        兜底换证都在本线程串行完成，中转回源命中挑战页时置位急换。没有 FlareSolverr
        来源时插件根本不会启用（见 init_plugin 门禁），因此不再保留手工粘贴凭证的分支。
        """
        generation = self._config_generation if generation is None else generation
        if not self._relay_enabled or not self.__generation_active(generation):
            return
        refresh_attempted = False
        refreshed = False
        try:
            if self._urgent_refresh.is_set() or self.__fs_due():
                # 挑战页急换优先消费；无急换时按周期到点主动换证
                self._urgent_refresh.clear()
                refresh_attempted = True
                refreshed = self.__fs_refresh()
            if not self.__generation_active(generation):
                return
            credentials = (self._cf_clearance, self._cf_user_agent)
            resp = self.__relay_probe(credentials=credentials)
            if not self.__generation_active(generation):
                return
            body_head = resp.content[:4096]
            challenged = any(mark in body_head for mark in CHALLENGE_MARKS) or (
                resp.status_code in (403, 429, 503)
                and b"cloudflare" in resp.content.lower()
            ) or resp.headers.get("cf-mitigated") == "challenge"
            if challenged:
                raise RuntimeError("上游返回CF人机挑战页，cf_clearance已失效（或出口IP/UA变更）")
            if resp.status_code != 200:
                raise RuntimeError(f"上游探活失败：HTTP {resp.status_code}")
            if not all(credentials):
                raise RuntimeError("FlareSolverr尚未取得cf_clearance与配套UA，不能开始中转或目录补齐")
            with self._state_lock:
                if not self.__generation_active(generation):
                    return
                if credentials != (self._cf_clearance, self._cf_user_agent):
                    self.__relay_retry_soon()
                    return
                self._rotate_clearance(resp, generation=generation, credentials=credentials)
                self._relay_ready = True
                self._heartbeat_misses = 0
                self._escalation_misses = 0
                self._relay_pause_reason = ""
                self.__schedule_startup_pull()
            logger.info(
                f"ANi-Strm中转保活心跳正常（上游HTTP {resp.status_code}，"
                f"cf_clearance有效期至下次心跳）"
            )
        except Exception as err:
            if not self.__generation_active(generation):
                return
            self._relay_ready = False
            self._relay_pause_reason = str(err)
            # 本拍开头刚换到的新证立刻被判挑战：这是 CF 对该出口IP的短时风控升级
            # （同时段中转的并发请求会成片命中挑战页，FlareSolverr 自己也解不出题），
            # 不是凭证死亡——不催二次解题，只按短间隔重探
            just_renewed = refreshed or bool(
                self._fs_last_ts
                and time.time() - self._fs_last_ts < FS_RERESOLVE_COOLDOWN_SECONDS
            )
            if just_renewed:
                self._heartbeat_misses = 0
                self._escalation_misses += 1
                logger.warning(
                    f"ANi-Strm新凭证未通过探活（第{self._escalation_misses}次，{err}），"
                    f"{RELAY_HEARTBEAT_RETRY_MINUTES}分钟后重探，"
                    f"连续{RELAY_ESCALATION_MAX_MISSES}次才暂停中转"
                )
                if self._escalation_misses >= RELAY_ESCALATION_MAX_MISSES:
                    self.__relay_stop(f"FlareSolverr已换新证但CF持续判挑战（{err}），风控未解除")
                    return
                self.__relay_retry_soon()
                return
            self._heartbeat_misses += 1
            self._urgent_refresh.set()
            logger.warning(
                f"ANi-Strm中转保活第{self._heartbeat_misses}次失败（{err}），每拍最多解题一次"
            )
            if not refresh_attempted:
                refreshed = self.__fs_refresh()
            if not self.__generation_active(generation):
                return
            if refreshed:
                # 换证成功但本拍未验证：下一拍（3分钟内）用新证重探
                self._heartbeat_misses = 0
                self.__relay_retry_soon()
                return
            if self._heartbeat_misses < RELAY_HEARTBEAT_MAX_MISSES:
                logger.warning(
                    f"ANi-Strm FlareSolverr换证失败（第{self._heartbeat_misses}次），"
                    f"{RELAY_HEARTBEAT_RETRY_MINUTES}分钟后补试，连续{RELAY_HEARTBEAT_MAX_MISSES}次才停机"
                )
                self.__relay_retry_soon()
                return
            self.__relay_stop(f"{err}；FlareSolverr 也无法取得新的cf_clearance")

    def __relay_retry_soon(self):
        """
        把保活心跳改排成短间隔补试，覆盖 CF 排队抖动与瞬时网络异常，不必等完整周期。
        """
        next_run = datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(minutes=RELAY_HEARTBEAT_RETRY_MINUTES)
        try:
            if self._scheduler and self._scheduler.get_job("anistrm_relay_heartbeat"):
                self._scheduler.add_job(
                    func=self.__relay_heartbeat,
                    trigger=IntervalTrigger(minutes=RELAY_HEARTBEAT_MINUTES),
                    name="ANiStrmRelay中转保活",
                    id="anistrm_relay_heartbeat",
                    next_run_time=next_run,
                    replace_existing=True,
                    args=[self._config_generation],
                )
        except Exception as reschedule_err:
            logger.warning(f"ANi-Strm保活补试任务重排失败：{reschedule_err}")

    def __relay_stop(self, reason: str):
        """
        停用内置CF中转：摘除保活、续期与拉取任务并发送通知，避免死证下持续打源站。

        若配置了自动恢复（relay_auto_recover_minutes > 0 且未超上限），
        停机后立即排一次单拍 auto-recover 任务（只解题+探活，不跑全量回填），
        成功后恢复完整保活链，无需用户手动重载。

        :param reason: 触发停机的失败摘要
        """
        logger.warning(f"ANi-Strm内置CF中转运行已暂停（配置开关保留）：{reason}")
        self._relay_enabled = False
        self._enabled = False
        self._relay_ready = False
        self._relay_pause_reason = reason
        self.__update_config()
        for job_id in (
            "anistrm_relay_heartbeat",
            "anistrm_fs_refresh",
            "anistrm_relay_update",
            "anistrm_relay_startup_pull",
        ):
            try:
                if self._scheduler and self._scheduler.get_job(job_id):
                    self._scheduler.remove_job(job_id)
            except Exception as remove_err:
                logger.warning(f"ANi-Strm摘除任务 {job_id} 失败：{remove_err}")

        # 自动恢复：排入一次 __relay_auto_recover（30 秒后执行，等 CF 冷却期稍过）
        can_auto_recover = (
            self._relay_auto_recover_minutes > 0
            and self._relay_auto_recover_max > 0
            and self._auto_recover_attempts < self._relay_auto_recover_max
            and self._scheduler is not None
        )
        if can_auto_recover:
            self._auto_recover_attempts += 1
            delay_min = self._relay_auto_recover_minutes
            next_run = datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=30)
            try:
                self._scheduler.add_job(
                    func=self.__relay_auto_recover,
                    trigger="date",
                    run_date=next_run,
                    name="ANiStrmRelay自动恢复",
                    id="anistrm_relay_auto_recover",
                    args=[self._config_generation, delay_min],
                    replace_existing=True,
                )
                logger.info(
                    f"ANi-Strm已排自动恢复（第{self._auto_recover_attempts}/"
                    f"{self._relay_auto_recover_max}次，30秒后解题探活；"
                    f"成功后按{delay_min}分钟间隔持续保活）"
                )
            except Exception as sched_err:
                logger.warning(f"ANi-Strm自动恢复排程失败：{sched_err}")
        elif self._relay_auto_recover_max > 0 and self._auto_recover_attempts >= self._relay_auto_recover_max:
            logger.warning(
                f"ANi-Strm自动恢复已达上限（{self._relay_auto_recover_max}次），"
                "彻底停止，需手动保存配置或重载插件恢复"
            )

        self.post_message(
            title="【ANi Strm 中转】CF凭证不可用，运行已暂停（配置保留）",
            text=(
                f"**插件**：{self.plugin_name} v{self.plugin_version}\n"
                f"**时间**：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"**源站**：{self._relay_upstream}\n"
                f"**原因**：{reason[:120]}\n"
                "\n"
                "**影响**\n"
                "- 内置CF中转运行已暂停，strm 播放请求将返回 503；用户开关未关闭\n"
                "- 保活心跳、凭证续期与定时拉取任务均已摘除，不会自动重试\n"
                + (
                    f"\n**自动恢复**：今日第{self._auto_recover_attempts}次尝试已排入；"
                    f"当日连续失败{self._relay_auto_recover_max}次后停止，次日00:00自动从第1次重新开始\n"
                    if can_auto_recover else ""
                )
                + "\n"
                "**手动恢复**（如需立即尝试）\n"
                f"检查 FlareSolverr（地址：{self._fs_url or '未配置'}）是否存活并保存配置，"
                "保活、续期与定时拉取立即恢复；cf_clearance 与配套UA全部由 FlareSolverr 现场解题提供，无需手工粘贴任何cookie"
            ),
        )

    def __relay_auto_recover(self, generation: Optional[int] = None, recover_interval_min: int = DEFAULT_RELAY_AUTO_RECOVER_MINUTES):
        """
        自动恢复单拍：先解题换证，再探活验证，成功则恢复完整保活链。

        此任务由 __relay_stop 排入，在停机冷却期（约 30 秒）后执行。
        成功：重置计数、重新注册心跳（5 分钟间隔）、触发启动补齐。
        失败：按 recover_interval_min 间隔再排下一拍；达当日上限后推明日零点重启计数重跑。
        """
        generation = self._config_generation if generation is None else generation
        if not self.__generation_active(generation):
            return

        # 跨天重置：今日已耗尽，明天从第1次重新开始
        today_str = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d")
        if self._auto_recover_day and self._auto_recover_day != today_str:
            logger.info(
                f"ANi-Strm自动恢复：新的一天（{today_str}），重置计数，开始新一轮尝试"
            )
            self._auto_recover_attempts = 0
        self._auto_recover_day = today_str

        # 1. 解题换证（复用 __fs_refresh，不经过心跳逻辑）
        refreshed = self.__fs_refresh()
        if not self.__generation_active(generation):
            return

        if not refreshed:
            logger.warning(
                f"ANi-Strm自动恢复第{self._auto_recover_attempts}次：FlareSolverr解题失败，"
                f"{recover_interval_min}分钟后重试"
            )
            self.__schedule_next_auto_recover(generation, recover_interval_min)
            return

        # 2. 探活验证
        try:
            credentials = (self._cf_clearance, self._cf_user_agent)
            resp = self.__relay_probe(credentials=credentials)
            body_head = resp.content[:4096]
            challenged = any(mark in body_head for mark in CHALLENGE_MARKS) or (
                resp.status_code in (403, 429, 503)
                and b"cloudflare" in resp.content.lower()
            ) or resp.headers.get("cf-mitigated") == "challenge"
            if challenged:
                raise RuntimeError("上游仍返回CF人机挑战页（出口IP可能被持续风控）")
            if resp.status_code != 200:
                raise RuntimeError(f"上游探活失败：HTTP {resp.status_code}")
        except Exception as probe_err:
            logger.warning(
                f"ANi-Strm自动恢复第{self._auto_recover_attempts}次：探活失败（{probe_err}），"
                f"{recover_interval_min}分钟后重试"
            )
            self.__schedule_next_auto_recover(generation, recover_interval_min)
            return

        # 3. 成功：恢复完整保活链
        recovered_attempt = self._auto_recover_attempts  # 先记录，清零后日志用原始值
        with self._state_lock:
            if not self.__generation_active(generation):
                return
            self._auto_recover_attempts = 0
            self._heartbeat_misses = 0
            self._escalation_misses = 0
            self._relay_enabled = True
            self._enabled = self._enabled_requested
            self._relay_ready = True
            self._relay_pause_reason = ""
            self._startup_pull_pending = True
            self._fs_last_ts = time.time()
            self.__update_config(generation=generation)

        # 摘除本 auto-recover 任务，重新注册正常保活心跳
        try:
            if self._scheduler and self._scheduler.get_job("anistrm_relay_auto_recover"):
                self._scheduler.remove_job("anistrm_relay_auto_recover")
        except Exception:
            pass
        try:
            if self._scheduler:
                self._scheduler.add_job(
                    func=self.__relay_heartbeat,
                    trigger=IntervalTrigger(minutes=RELAY_HEARTBEAT_MINUTES),
                    name="ANiStrmRelay中转保活",
                    id="anistrm_relay_heartbeat",
                    next_run_time=
                        datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=15),
                    replace_existing=True,
                    args=[generation],
                )
        except Exception:
            logger.warning("ANi-Strm自动恢复成功但保活任务重注册失败，下次保存配置时恢复")

        self.__schedule_startup_pull()
        logger.info(
            f"ANi-Strm自动恢复成功（第{recovered_attempt}次），"
            "保活心跳已重启，cf_clearance 有效，启动补齐已排程"
        )

    def __schedule_next_auto_recover(self, generation: int, recover_interval_min: int):
        """
        排下一拍自动恢复。
        策略：
        - 未达当日上限：按 recover_interval_min 间隔继续重试。
        - 达到当日上限：标记今日已耗尽，排到明天 00:00 重置计数并重新来一轮；
          不永久的"彻底放弃"——只要 CF/FlareSolverr 某天恢复就能自动续命。
        """
        if self._auto_recover_attempts >= self._relay_auto_recover_max:
            # 今日 8 次耗尽，推到明天零点重置计数从头再来
            tomorrow = datetime.now(tz=pytz.timezone(settings.TZ)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
            logger.warning(
                f"ANi-Strm自动恢复已达当日上限（{self._relay_auto_recover_max}次），"
                f"今日停止；已排明日{tomorrow.strftime('%Y-%m-%d %H:%M')}自动重置并从第1次重新开始"
            )
            self._auto_recover_attempts = self._relay_auto_recover_max + 1  # 标记今日已耗尽
            self._auto_recover_day = ""  # 清空日期，触发明日的重置逻辑
            try:
                self._scheduler.add_job(
                    func=self.__relay_auto_recover,
                    trigger="date",
                    run_date=tomorrow,
                    name="ANiStrmRelay自动恢复",
                    id="anistrm_relay_auto_recover",
                    args=[generation, recover_interval_min],
                    replace_existing=True,
                )
            except Exception as sched_err:
                logger.warning(f"ANi-Strm自动恢复次日重排失败：{sched_err}")
            return
        next_run = datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(minutes=recover_interval_min)
        try:
            self._scheduler.add_job(
                func=self.__relay_auto_recover,
                trigger="date",
                run_date=next_run,
                name="ANiStrmRelay自动恢复",
                id="anistrm_relay_auto_recover",
                args=[generation, recover_interval_min],
                replace_existing=True,
            )
        except Exception as sched_err:
            logger.warning(f"ANi-Strm自动恢复重排失败：{sched_err}")

    def __relay_probe(self, credentials: Optional[Tuple[str, str]] = None) -> httpx.Response:
        """
        保活单次探活：以解题UA+凭证对上游根路径POST目录请求。

        Cloudflare 在配额紧张时会将携带有效凭证的正常请求长时间排队，单次读超时
        并不代表凭证失效；网络类异常间隔30秒重试一次，仍失败才交由调用方判定，
        避免偶发排队把中转误停用。

        :return: 上游探活响应（内容已读取）
        """
        last_err: Optional[Exception] = None
        clearance, ua = credentials or (self._cf_clearance, self._cf_user_agent)
        for attempt in (1, 2):
            try:
                with httpx.Client(
                    http2=True,
                    follow_redirects=True,
                    timeout=httpx.Timeout(15.0, read=60.0, write=15.0, pool=15.0),
                    proxy=self._upstream_proxy,
                    trust_env=False,
                ) as client:
                    return client.post(
                        f"{self._relay_upstream.rstrip('/')}/",
                        headers={
                            "User-Agent": ua,
                            "Cookie": f"cf_clearance={clearance}",
                        },
                        content=b'{"password":""}',
                    )
            except Exception as err:
                last_err = err
                if attempt == 1:
                    logger.warning(f"ANi-Strm保活探活首次异常（{err}），30秒后重试一次")
                    time.sleep(30)
        raise last_err

    async def api_relay(self, full_path: str, request: Request):
        """
        CF 透传中转入口：注入 cf_clearance 与解题 UA，目录HTTP/2、媒体HTTP/1.1回源。

        支持 GET/HEAD 文件流（含 Range 断点续传）与 POST 目录列表；命中挑战页时返回
        502 并催 FlareSolverr 换证自愈，strm 文件本身不含凭证、无需重新生成。

        :param full_path: 相对于上游源站的路径（由宿主路由解出）
        :param request: 当前请求（媒体服务器或本插件目录请求）
        :return: 上游响应透传，或错误提示 JSON
        """
        generation = self._config_generation
        if not self._relay_enabled or not self.__generation_active(generation):
            return JSONResponse(
                {
                    "success": False,
                    "message": self._relay_pause_reason or (
                        "ANi-Strm内置中转未启用：请配置FlareSolverr和中转访问地址后开启中转"
                    ),
                },
                status_code=503,
            )
        if self._relay_token and request.query_params.get("rt") != self._relay_token:
            logger.warning(f"ANi-Strm中转拒绝了无效令牌请求：/{full_path}")
            return JSONResponse(
                {"success": False, "message": "中转访问令牌无效"},
                status_code=403,
            )
        if not self._cf_clearance or not self._relay_ready:
            # 自动续期尚未产出首张凭证：催一拍换证，让调用方稍后重试而不是死等
            if not self._cf_clearance:
                self._urgent_refresh.set()
            return JSONResponse(
                {
                    "success": False,
                    "message": "cf_clearance正在由FlareSolverr获取并验证，请稍后重试",
                },
                status_code=503,
            )

        upstream_proxy = self._upstream_proxy
        credentials = (self._cf_clearance, self._cf_user_agent)
        upstream_url = f"{self._relay_upstream.rstrip('/')}/{quote(full_path, safe='/')}"
        headers = {
            key: value
            for key, value in request.headers.items()
            if key.lower() in RELAY_PASSTHROUGH_REQ_HEADERS
        }
        headers["User-Agent"] = credentials[1]
        headers["Cookie"] = f"cf_clearance={credentials[0]}"
        query = [
            (key, value)
            for key, value in request.query_params.multi_items()
            if key != "rt"
        ]
        body = await request.body()

        # POST 是目录列举、GET/HEAD 是媒体文件流：分池出站，被拆掉的连接不殃及另一类请求
        is_listing = request.method.upper() not in ("GET", "HEAD")
        # 列举请求体量小、重放安全，被远端拆流后换新连接再来一次；文件流不重放
        attempts = 2 if is_listing else 1
        last_err: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            client = _relay_client(control=is_listing, proxy=upstream_proxy)
            try:
                upstream_request = client.build_request(
                    method=request.method,
                    url=upstream_url,
                    headers=headers,
                    content=body if body else None,
                    params=query if query else None,
                )
                resp = await client.send(upstream_request, stream=True)
            except httpx.HTTPError as err:
                last_err = err
                _drop_relay_client(control=is_listing, proxy=upstream_proxy)
                logger.warning(
                    f"ANi-Strm中转上游请求失败（第{attempt}次）："
                    f"{request.method} {upstream_url} - {err}"
                )
                continue

            if not self.__generation_active(generation):
                await resp.aclose()
                return JSONResponse({"success": False, "message": "插件已重载，请重试"}, status_code=503)
            self._rotate_clearance(resp, generation=generation, credentials=credentials)
            resp_headers = {
                key.lower(): value
                for key, value in resp.headers.items()
                if key.lower() in RELAY_PASSTHROUGH_RESP_HEADERS
            }
            ctype = resp_headers.get("content-type", "")

            if ctype.startswith(("text/", "application/json")):
                try:
                    await resp.aread()
                except httpx.HTTPError as err:
                    # 响应头已到手、正文被 RST_STREAM 半路拆断：这条连接已被远端拆掉，
                    # 换新连接重放，避免整轮回填跟着一起挂
                    await resp.aclose()
                    last_err = err
                    _drop_relay_client(control=is_listing, proxy=upstream_proxy)
                    logger.warning(
                        f"ANi-Strm中转响应被远端拆流（第{attempt}次）：{upstream_url} - {err}"
                    )
                    continue
                head = resp.content[:4096]
                if not self.__generation_active(generation):
                    await resp.aclose()
                    return JSONResponse({"success": False, "message": "插件已重载，请重试"}, status_code=503)
                challenged = any(mark in head for mark in CHALLENGE_MARKS) or (
                    resp.status_code in (403, 429, 503)
                    and b"cloudflare" in resp.content.lower()
                ) or resp.headers.get("cf-mitigated") == "challenge"
                if challenged:
                    # CF 风控升级时并发播放请求会成片命中挑战页，同一分钟内只打一条
                    now = time.time()
                    if now - self._last_challenge_warn_ts >= 60:
                        self._last_challenge_warn_ts = now
                        logger.warning(
                            "ANi-Strm中转收到CF挑战页（本条1分钟内不重复打印）："
                            "已置位急换，cf_clearance由FlareSolverr自动更新后自愈"
                        )
                    with self._state_lock:
                        if (self.__generation_active(generation)
                                and credentials == (self._cf_clearance, self._cf_user_agent)):
                            self._urgent_refresh.set()
                            self._relay_ready = False
                    return JSONResponse(
                        {
                            "success": False,
                            "message": (
                                "上游CF校验升级，正在自动重新解题换发cf_clearance，请稍后重试"
                                "（strm文件不含凭证，无需重新生成）"
                            ),
                        },
                        status_code=502,
                    )
                return Response(
                    status_code=resp.status_code,
                    headers={
                        key: value
                        for key, value in resp_headers.items()
                        if key.lower() != "content-length"
                    },
                    content=resp.content,
                )

            async def _chunks(response: httpx.Response = resp, url: str = upstream_url):
                """
                按块透传上游字节流；播放器快进/停止提前断开属正常情况。
                """
                try:
                    async for chunk in response.aiter_raw():
                        yield chunk
                except httpx.HTTPError as err:
                    # 上游中途拆流：记下并摘掉该连接池，后续请求在新连接上重开
                    logger.warning(f"ANi-Strm中转流式传输中断：{url} - {err}")
                    _drop_relay_client(control=False, proxy=upstream_proxy)
                finally:
                    await response.aclose()

            return StreamingResponse(
                _chunks(), status_code=resp.status_code, headers=resp_headers
            )

        logger.error(
            f"ANi-Strm中转上游回源失败（共尝试{attempts}次）："
            f"{request.method} {upstream_url} - {last_err}"
        )
        return JSONResponse(
            {
                "success": False,
                "message": (
                    f"目录请求失败：{last_err}" if is_listing else f"上游请求失败：{last_err}"
                ),
            },
            status_code=502,
        )

    async def api_relay_selfcheck(self, request: Request):
        """
        中转自检：探测上游连通性，并给出「中转访问地址」应填写的完整前缀。

        插件路由由宿主按实例ID装配，路径前缀以实际注册结果为准，故此处直接回显
        当前请求自身的 URL 作为建议值，避免手工推断插件ID大小写。

        :param request: 自检请求（需bearer登录态）
        :return: 自检结果 JSON
        """
        suggested_base = str(request.url).split("?")[0].rstrip("/")
        result: Dict[str, Any] = {
            "relay_enabled": self._relay_enabled,
            "relay_requested": self._relay_requested,
            "relay_ready": self._relay_ready,
            "plugin_version": self.plugin_version,
            "pause_reason": self._relay_pause_reason,
            "upstream": self._relay_upstream or DEFAULT_UPSTREAM,
            "cookie_configured": bool(self._cf_clearance),
            "user_agent": self._cf_user_agent,
            "token_configured": bool(self._relay_token),
            "flaresolverr": self._fs_url,
            "suggested_public_base": suggested_base,
            "proxy_source": self._upstream_proxy_source,
            "proxy_configured": bool(self._upstream_proxy),
        }
        probe_url = f"{(self._relay_upstream or DEFAULT_UPSTREAM).rstrip('/')}/"
        if self._upstream_proxy_error:
            result["probe"] = {"ok": False, "error": self._upstream_proxy_error}
            result["mp_egress_ip"] = "代理配置不可用，未发送上游请求"
            return JSONResponse(result)
        try:
            egress_resp = await _relay_client(control=True, proxy=self._upstream_proxy).get(
                f"{self._relay_upstream.rstrip('/')}/cdn-cgi/trace", timeout=10.0
            )
            trace = dict(line.split("=", 1) for line in egress_resp.text.splitlines() if "=" in line)
            result["mp_egress_ip"] = trace.get("ip", "同域出口探测未返回IP")
        except httpx.HTTPError as err:
            result["mp_egress_ip"] = f"探测失败：{err}"
        try:
            headers = {"User-Agent": self._cf_user_agent}
            if self._cf_clearance:
                headers["Cookie"] = f"cf_clearance={self._cf_clearance}"
            resp = await _relay_client(control=True, proxy=self._upstream_proxy).post(
                probe_url, headers=headers, content='{"password":""}', timeout=20.0
            )
            body_head = resp.content[:4096]
            challenged = any(mark in body_head for mark in CHALLENGE_MARKS) or (
                resp.status_code in (403, 429, 503)
                and b"cloudflare" in resp.content.lower()
            ) or resp.headers.get("cf-mitigated") == "challenge"
            result["probe"] = {
                "status": resp.status_code,
                "cf_mitigated": resp.headers.get("cf-mitigated", ""),
                "challenged": challenged,
                "ok": resp.status_code == 200 and not challenged,
            }
        except httpx.HTTPError as err:
            result["probe"] = {"ok": False, "error": str(err)}
        return JSONResponse(result)

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        season_options = self.__build_season_options()
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 4,
                                },
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 4,
                                },
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "use_proxy",
                                            "label": "使用代理",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 4,
                                },
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "relay_enabled",
                                            "label": "内置CF中转",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "proxy_base",
                                            "label": "反代/源站地址",
                                            "placeholder": DEFAULT_UPSTREAM,
                                            "hint": "上游源站；开启内置中转时作为中转转发目标，留空使用官方默认",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "定时拉取周期",
                                            "placeholder": DEFAULT_CRON,
                                            "hint": "cron表达式，启用插件时按期补齐strm；留空停用定时拉取（保活失败会自动暂停）",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6,
                                },
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "selected_seasons",
                                            "label": "拉取季度",
                                            "items": season_options,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "hint": "按所选季度检查并补齐strm；已存在文件会自动跳过。留空则本次不执行",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "storageplace",
                                            "label": "Strm存储地址",
                                            "placeholder": "/downloads/strm",
                                            "hint": "按上游季度/番剧子目录保存，如 /media/strm/2026-10/番剧名；不额外追加openani",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "fs_url",
                                            "label": "FlareSolverr 地址",
                                            "placeholder": "留空则自动使用系统设置（站点分组）中的 FlareSolverr",
                                            "hint": "建议使用http://flaresolverr:8191而非会随容器重建变化的IP；留空沿用系统设置。缺少地址只暂停运行，不清除开启意图",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "relay_proxy",
                                            "label": "中转出站代理",
                                            "placeholder": "http://<sing-box地址>:7890",
                                            "hint": "浏览器解题、探活与回源统一使用；留空沿用环境HTTP(S)_PROXY或已启用的MP系统代理。内网FlareSolverr API始终直连，只支持无认证HTTP(S)入口",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "relay_token",
                                            "label": "中转访问令牌",
                                            "placeholder": "留空则内网匿名放行",
                                            "hint": "中转接口匿名开放，建议设置随机串防滥用，strm地址会自动附带",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 8,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "relay_public_base",
                                            "label": "中转访问地址",
                                            "placeholder": "http://<MP地址>:3000/api/v1/plugin/ANiStrmRelay/relay",
                                            "hint": "媒体服务器可达的本插件中转前缀；可在插件API文档中调用 /relay 自检接口获取建议值",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 4,
                                },
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "rewrite_existing",
                                            "label": "改写已存在strm",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 4,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "relay_auto_recover_minutes",
                                            "label": "自动恢复间隔(分钟)",
                                            "placeholder": "15",
                                            "hint": "CF凭证停机后自动重试FlareSolverr解题的间隔；0或负值禁用自动恢复",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 4,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "relay_auto_recover_max",
                                            "label": "自动恢复最大次数",
                                            "placeholder": "8",
                                            "hint": "连续失败N次后彻底放弃（需手动保存配置恢复）；0表示不限制",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                },
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "自动从open ANi抓取下载直链生成strm文件\n"
                                            "默认选择最新季，也支持季度多选，自动补齐所选季度缺失的strm\n"
                                             "存储目录下保留上游季度及番剧子目录，现有文件不会自动迁移或删除\n"
                                            "通过目录监控转移到link媒体库文件夹 如/downloads/link/strm mp会完成刮削",
                                            "style": "white-space: pre-line;",
                                        },
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": "内置CF中转用法：\n"
                                            "前提：需配置FlareSolverr与中转访问地址；缺依赖或连续验证失败只暂停运行，开关仍保持用户设置\n"
                                            "配好 FlareSolverr（本插件地址留空即沿用系统设置）后开启内置CF中转并保存，"
                                            "cf_clearance 与配套UA自动取得并验证（解题可能超过100秒），验证成功后才开始补齐目录，全程零粘贴\n"
                                            "旧 strm 是直链的，勾选「改写已存在strm」保存一次换成中转地址，之后取消勾选\n"
                                            "CF 对同一出口IP短时风控升级时播放会短暂返回502，插件自动换证或重探后自愈，strm文件不含凭证、无需重新生成\n"
                                            "解题、探活与回源共用中转出站代理；内网FlareSolverr API直连。媒体服务器需能直连MP（有环境代理时配置NO_PROXY）\n"
                                            "固定出口请在sing-box锁定节点，不能依赖会换服务器的自动测速组；暂停后修复并保存或重载恢复",
                                            "style": "white-space: pre-line;",
                                        },
                                    },
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "use_proxy": True,
            "cron": DEFAULT_CRON,
            "storageplace": "/downloads/strm",
            "selected_seasons": ["latest"],
            "proxy_base": DEFAULT_UPSTREAM,
            "relay_enabled": False,
            "cf_clearance": "",
            "cf_user_agent": "",
            "relay_token": "",
            "relay_public_base": "",
            "rewrite_existing": False,
            "fs_url": "",
            "relay_proxy": "",
            "relay_auto_recover_minutes": DEFAULT_RELAY_AUTO_RECOVER_MINUTES,
            "relay_auto_recover_max": DEFAULT_RELAY_AUTO_RECOVER_MAX_ATTEMPTS,
        }

    def __build_season_options(self) -> List[Dict[str, str]]:
        """
        构造「拉取季度」下拉项（最新季 + 全部季度 + 上游可用季度）。

        :return: 下拉选项列表
        """
        if self._relay_requested and not (self._relay_enabled and self._relay_ready):
            seasons = list(self._client._season_options_cache) or [self._client._get_local_season()]
        else:
            seasons = self._client.get_available_seasons() or [self._client._get_local_season()]
        return [
            {"title": "最新季", "value": "latest"},
            {"title": "全部季度", "value": "all"},
        ] + [
            {"title": season, "value": season} for season in seasons
        ]

    def __update_config(self, generation: Optional[int] = None):
        """
        回写运行期配置（含中转各项），保持宿主整体替换语义下配置不丢失。
        """
        generation = self._config_generation if generation is None else generation
        with self._state_lock:
            if not self.__generation_active(generation):
                return
            self.update_config({
                "cron": self._cron,
                "enabled": self._enabled_requested,
                "use_proxy": self._use_proxy_requested,
                "storageplace": self._storageplace,
                "selected_seasons": self._selected_seasons,
                "proxy_base": self._proxy_base,
                "relay_enabled": self._relay_requested,
                "cf_clearance": self._cf_clearance,
                "cf_user_agent": self._cf_user_agent,
                "relay_token": self._relay_token,
                "relay_public_base": self._relay_public_base,
                "rewrite_existing": self._rewrite_existing,
                "fs_url": self._fs_url_explicit,
                "relay_proxy": self._relay_proxy,
                "relay_auto_recover_minutes": self._relay_auto_recover_minutes,
                "relay_auto_recover_max": self._relay_auto_recover_max,
            })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        注销定时任务（共享HTTP客户端常驻，不随插件重载关闭）。
        """
        # 正在跑的补档线程不是调度任务，shutdown 管不到它：置位让它跑完当前季度就收手
        with self._state_lock:
            self._task_should_stop = True
            self._relay_ready = False
            self._config_generation += 1
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    # wait=False：FlareSolverr 解题线程可能正卡在140秒超时里，
                    # 等它跑完会把插件重载（乃至宿主禁用/启用插件的请求）一起拖住
                    self._scheduler.shutdown(wait=False)
                self._scheduler = None
        except Exception as err:
            logger.error(f"退出插件失败：{err}")


class AniStrmClient:
    """open ANi Alist 风格目录接口的访问客户端。"""

    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

    def __init__(self, request_factory=None, proxy_base: Optional[str] = None, use_proxy: bool = False):
        self._request_factory = request_factory or self._build_request_utils
        self._proxy_base = self.normalize_proxy_base(proxy_base)
        self._season_options_cache: List[str] = []
        self._use_proxy = use_proxy
        self._direct_session: Optional[requests.Session] = None
        # 目录请求的固定查询后缀（走内置中转时携带 rt 令牌；直连为空）
        self._query_suffix = ""
        # 上一次目录列举请求时间戳（配合最小间隔限速）
        self._last_folder_request_ts = 0.0
        # 本轮季度列举中取不到条目的番剧目录名（逐个隔离，不再拖垮整季）
        self._failed_series: List[str] = []

    def pop_failed_series(self) -> List[str]:
        """
        取出并清空最近一次季度列举中未取得的上游目录名列表（整季重试可能重复记同一目录）。

        :return: 失败的番剧目录名列表
        """
        failed = list(dict.fromkeys(self._failed_series))
        self._failed_series = []
        return failed

    def set_query_suffix(self, suffix: str):
        """
        设置目录请求统一追加的查询后缀并清空季度缓存。
        """
        self._query_suffix = (suffix or "").strip().lstrip("?")
        self._season_options_cache = []

    def set_proxy_base(self, proxy_base: Optional[str]):
        self._proxy_base = self.normalize_proxy_base(proxy_base)
        self._season_options_cache = []

    def set_use_proxy(self, use_proxy: bool):
        self._use_proxy = use_proxy
        self._season_options_cache = []

    def get_current_season(self, idx_month: Optional[int] = None, now: Optional[datetime] = None) -> str:
        remote_season = self._get_latest_remote_season()
        if remote_season:
            return remote_season
        return self._get_local_season(idx_month=idx_month, now=now)

    def _get_local_season(self, idx_month: Optional[int] = None, now: Optional[datetime] = None) -> str:
        current = now or datetime.now()
        current_month = idx_month or current.month
        season_month = ((current_month - 1) // 3) * 3 + 1
        return f"{current.year}-{season_month}"

    def get_current_season_list(self) -> List[str]:
        season = self.get_current_season()
        return self.get_season_list(season)

    def get_season_list(self, season: str) -> List[str]:
        entries = self.get_season_entries(season)
        return [entry["name"] for entry in entries if entry.get("name")]

    def get_season_entries(self, season: str) -> List[Dict[str, str]]:
        self._failed_series = []

        def operation():
            return self._collect_folder_entries(f"{season}/", relative_dir=season)

        entries = self._with_retry(operation, default=None)
        if entries is None:
            self._failed_series.append(season)
            return []
        return entries

    def _get_latest_remote_season(self) -> Optional[str]:
        def operation():
            payload = self._fetch_folder_payload(f"{self._get_openani_base()}/")
            return self._extract_latest_season(payload.get("files") or [])

        return self._with_retry(operation, default=None)

    def get_available_seasons(self, use_cache: bool = True) -> List[str]:
        if use_cache and self._season_options_cache:
            return list(self._season_options_cache)

        def operation():
            payload = self._fetch_folder_payload(f"{self._get_openani_base()}/")
            seasons = []
            for file_info in payload.get("files") or []:
                name = file_info.get("name") or ""
                mime_type = file_info.get("mimeType") or ""
                if mime_type != self.FOLDER_MIME_TYPE:
                    continue
                parts = name.split("-", 1)
                if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                    continue
                seasons.append(name)
            seasons.sort(key=lambda item: tuple(map(int, item.split("-"))), reverse=True)
            return seasons

        seasons = self._with_retry(operation, default=[])
        self._season_options_cache = list(seasons)
        return seasons

    def _fetch_folder_payload(self, url: str) -> Dict[str, Any]:
        wait = FOLDER_REQUEST_INTERVAL_SECONDS - (time.time() - self._last_folder_request_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_folder_request_ts = time.time()
        if self._query_suffix and "?" not in url:
            url = f"{url}?{self._query_suffix}"
        response = self._request_factory().post_res(
            url=url,
            data='{"password":""}',
            raise_exception=True,
        )
        if response is None:
            raise ValueError(f"目录请求失败：{url}")
        try:
            if response.status_code != 200:
                # 中转命中CF挑战时回的是 502 JSON（{"success": false, ...}），它没有 files 字段，
                # 照原样认下来就成了「这个目录是空的」：整季秒回文件数=0且一条告警都不打
                raise ValueError(
                    f"目录响应异常：HTTP {response.status_code} {str(response.text)[:120]}"
                )
            payload = response.json()
        finally:
            response.close()
        if not isinstance(payload, dict) or "files" not in payload:
            raise ValueError(f"目录响应缺少files字段：{str(payload)[:120]}")
        return payload

    def _build_request_utils(self) -> RequestUtils:
        if not self._use_proxy and self._direct_session is None:
            self._direct_session = requests.Session()
            self._direct_session.trust_env = False
        return RequestUtils(
            ua=settings.USER_AGENT if settings.USER_AGENT else None,
            proxies=settings.PROXY if self._use_proxy and settings.PROXY else None,
            session=self._direct_session if not self._use_proxy else None,
            timeout=FOLDER_REQUEST_TIMEOUT_SECONDS,
        )

    def _collect_folder_entries(self, folder_path: str, relative_dir: str = "") -> List[Dict[str, str]]:
        """
        递归列举目录条目；子目录单独重试并隔离失败。

        一个番剧目录取不到就抛到最外层，会让整季在重试耗尽后返回空列表（日志里成片
        的「文件数=0」），其余几百个本可正常补齐的条目跟着一起丢掉，故子目录失败只跳过该目录。

        :param folder_path: 相对源站的目录路径
        :param relative_dir: 写入 strm 的相对目录
        :return: 该目录（含子目录）下的文件条目
        """
        payload = self._fetch_folder_payload(f"{self._get_openani_base()}/{folder_path}")
        entries: List[Dict[str, str]] = []
        for file_info in payload.get("files") or []:
            name = file_info.get("name") or ""
            if not name:
                continue
            mime_type = file_info.get("mimeType") or ""
            if mime_type == self.FOLDER_MIME_TYPE:
                child_relative_dir = f"{relative_dir}/{name}".strip("/")
                child_folder_path = f"{folder_path.rstrip('/')}/{quote(name, safe='')}/"
                child_entries = self._with_retry(
                    lambda path=child_folder_path, rel=child_relative_dir:
                    self._collect_folder_entries(path, rel),
                    default=None,
                    tries=2,
                    delay=3,
                )
                if child_entries is None:
                    self._failed_series.append(child_relative_dir or name)
                    continue
                entries.extend(child_entries)
                continue

            encoded_name = quote(name, safe="")
            file_url = f"{self._get_openani_base()}/{folder_path.rstrip('/')}/{encoded_name}"
            entries.append(
                {
                    "name": name,
                    "url": file_url,
                    "relative_dir": relative_dir,
                }
            )
        return entries

    @staticmethod
    def normalize_proxy_base(proxy_base: Optional[str]) -> str:
        if not proxy_base:
            return DEFAULT_UPSTREAM
        return proxy_base.strip().rstrip("/")

    def _get_openani_base(self) -> str:
        return self._proxy_base or DEFAULT_UPSTREAM

    def normalize_stream_link(self, link: str) -> str:
        parsed = urlparse(link)
        if parsed.netloc not in {"resources.ani.rip", "openani.an-i.workers.dev"}:
            return link

        proxy_parsed = urlparse(self._get_openani_base())
        return urlunparse(
            (
                proxy_parsed.scheme,
                proxy_parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )

    @staticmethod
    def _with_retry(operation, default, tries: int = 3, delay: int = 3):
        """
        带指数退避 + 随机抖动的重试。

        使用 Fibonacci 阶梯延迟（delay × Fib[i]），再叠加 0~delay×0.5 的随机抖动，
        防止多请求并发重试时形成同步冲击波触发上游限速。最大单步等待不超过
        delay × 8（当前 ≈24s）加上抖动上限 1.5s，整体远小于目录级 1s 间隔。
        """
        import math, random
        fib = [0, 1, 1, 2, 3, 5, 8]  # Fib[tries+1] 覆盖最大重试索引
        remaining = tries
        while remaining > 0:
            try:
                return operation()
            except Exception as err:
                remaining -= 1
                if remaining == 0:
                    logger.warning(f"ANiStrmRelay请求失败，已达到最大重试次数：{err}")
                    break
                fib_idx = min(tries + 1 - remaining + 1, len(fib) - 1)
                base_wait = delay * fib[fib_idx]
                # 0 ~ delay*0.5 的随机抖动，避免并发重试同步触发
                jitter = random.uniform(0, delay * 0.5)
                total_wait = min(base_wait, 30.0) + jitter  # 上限 30s
                logger.warning(
                    f"未获取到文件信息，约 {total_wait:.1f} 秒后重试（尝试 {tries - remaining}/{tries}）..."
                )
                time.sleep(total_wait)
        logger.warning("请确保当前季度番剧文件夹存在或检查网络问题")
        return default

    @staticmethod
    def _extract_latest_season(files: List[Dict[str, str]]) -> Optional[str]:
        seasons: List[Tuple[int, int]] = []
        for file_info in files:
            name = file_info.get("name") or ""
            mime_type = file_info.get("mimeType") or ""
            if mime_type != AniStrmClient.FOLDER_MIME_TYPE:
                continue
            parts = name.split("-", 1)
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                continue
            seasons.append((int(parts[0]), int(parts[1])))
        if not seasons:
            return None
        year, month = max(seasons)
        return f"{year}-{month}"


class StrmFileService:
    """strm 文件创建与内容规范化服务。"""

    @staticmethod
    def build_season_url(season: str, file_name: str, base_url: str) -> str:
        encoded_filename = quote(file_name, safe="")
        return f"{base_url.rstrip('/')}/{season}/{encoded_filename}"

    @staticmethod
    def normalize_stream_url(url: str) -> str:
        if url.endswith(".mp4"):
            return url
        if url.endswith(".mp4?d=true"):
            return url[:-7]
        if "?d=mp4" in url:
            return url.replace("?d=mp4", ".mp4")
        if "?d=true" in url and ".mp4?d=true" not in url:
            return url.replace("?d=true", "")
        return f"{url}.mp4"

    def touch_strm_file(
        self,
        storage_path: str,
        file_name: str,
        season: Optional[str] = None,
        file_url: Optional[str] = None,
        base_url: str = DEFAULT_UPSTREAM,
        relative_dir: Optional[str] = None,
        query_suffix: Optional[str] = None,
        rewrite_existing: bool = False,
    ) -> str:
        """
        写入（或按需改写）单个 strm 文件。

        :param query_suffix: 追加到播放地址的查询串（如中转访问令牌），normalize 之后追加
        :param rewrite_existing: 已存在且内容不同时是否改写，返回 "updated"
        :return: created / exists / updated / failed
        """
        if not storage_path:
            logger.error("创建strm源文件失败：未配置存储目录")
            return "failed"

        if file_url:
            src_url = self.normalize_stream_url(file_url)
        else:
            if not season:
                logger.error("创建strm源文件失败：未提供季度信息")
                return "failed"
            src_url = self.build_season_url(season, file_name, base_url=base_url)
        if query_suffix and "?" not in src_url:
            src_url = f"{src_url}?{query_suffix}"

        directory = Path(storage_path)
        if relative_dir:
            directory = directory / relative_dir
        file_path = directory / f"{file_name}.strm"
        if file_path.exists():
            if not rewrite_existing:
                logger.debug(f"ANi-Strm跳过已存在文件：{file_path.name}")
                return "exists"
            try:
                if file_path.read_text(encoding="utf-8").strip() == src_url:
                    return "exists"
            except OSError:
                pass
            try:
                file_path.write_text(src_url, encoding="utf-8")
                logger.debug(f"ANi-Strm改写已存在文件：{file_path.name}")
                return "updated"
            except Exception as err:
                logger.error(f"改写strm源文件失败：{file_path.name} - {err}")
                return "failed"

        try:
            directory.mkdir(parents=True, exist_ok=True)
            file_path.write_text(src_url, encoding="utf-8")
            logger.debug(f"ANi-Strm创建成功：{file_path.name}")
            return "created"
        except Exception as err:
            logger.error(f"创建strm源文件失败：{file_path.name} - {err}")
            return "failed"
