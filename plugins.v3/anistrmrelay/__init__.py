"""
ANi-Strm 自用迭代（内置 CF 透传中转）。

基于 honue/MoviePilot-Plugins 的 ANi-Strm 2.5.1（GPL-3.0）迭代：目录接口、季度选择与
刮削链路沿用原版行为，新增内置 Cloudflare 透传中转，解决 openani 源站 2026-08 起因
超配额被整域强制人机挑战（honue/MoviePilot-Plugins#147）后 strm 无法播放的问题。
致谢：原作者 honue 及上游 #147/#142 的失效排查讨论；源站与 Alist 风格目录接口由
open ANi 字幕组提供。
"""
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse

import httpx
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
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
RELAY_HEARTBEAT_MINUTES = 12
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

# 全进程共享的 httpx 异步客户端（HTTP/2 出站，与已验证可通过 cf_clearance 重放的
# curl HTTP/2 指纹同级；连接池常驻复用，插件重载不关闭以避免跨事件循环竞态）
_RELAY_CLIENT: Optional[httpx.AsyncClient] = None


def _relay_client() -> httpx.AsyncClient:
    """
    获取（必要时创建）中转专用的共享异步 HTTP 客户端。

    :return: 启用 HTTP/2、自动跟随重定向的 httpx.AsyncClient 单例
    """
    global _RELAY_CLIENT
    if _RELAY_CLIENT is None or _RELAY_CLIENT.is_closed:
        _RELAY_CLIENT = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,
            timeout=httpx.Timeout(15.0, read=300.0, write=60.0, pool=15.0),
        )
    return _RELAY_CLIENT


class ANiStrmRelay(_PluginBase):
    plugin_name = "ANi Strm 自用版（CF中转）"
    plugin_desc = "自动获取当季所有番剧生成strm；内置CF中转代理携带cf_clearance直连openani，解决源站人机挑战后strm无法播放的问题（基于 honue 版 ANi-Strm 迭代）"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "0.1.4"
    plugin_author = "JontyLee（基于 honue ANi-Strm）"
    author_url = "https://github.com/JontyLee"
    plugin_config_prefix = "anistrm_"
    plugin_order = 15
    auth_level = 2

    _enabled = False
    _use_proxy = True
    _onlyonce = False
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
    # 内置中转实际转发的上游源站（即原始 proxy_base 配置值）
    _relay_upstream = ""
    # 滚动续期凭证上次持久化的时间戳（节流）
    _relay_persist_ts = 0.0

    def __init__(self):
        super().__init__()
        self._client = AniStrmClient()
        self._strm_service = StrmFileService()

    def init_plugin(self, config: dict = None):
        """
        读取配置并注册定时任务；开启内置中转时把目录读取与strm生成地址切换到中转入口。

        :param config: 插件配置字典
        """
        self.stop_service()

        config = config or {}
        self._enabled = config.get("enabled", False)
        use_proxy = config.get("use_proxy")
        self._use_proxy = True if use_proxy is None else use_proxy
        self._onlyonce = config.get("onlyonce", False)
        self._storageplace = config.get("storageplace") or "/downloads/strm"
        if "selected_seasons" in config:
            self._selected_seasons = config.get("selected_seasons") or []
        else:
            self._selected_seasons = ["latest"]
        self._proxy_base = config.get("proxy_base") or DEFAULT_UPSTREAM
        self._relay_enabled = bool(config.get("relay_enabled"))
        raw_clearance = (config.get("cf_clearance") or "").strip()
        self._cf_clearance = self._normalize_clearance(raw_clearance)
        if raw_clearance and self._cf_clearance != raw_clearance:
            logger.info(
                "ANi-Strm检测到cf_clearance配置项粘贴了整串浏览器Cookie，"
                "已自动提取其中的cf_clearance生效；建议保存后仅保留该值"
            )
        self._cf_user_agent = (config.get("cf_user_agent") or "").strip() or DEFAULT_CF_UA
        if self._cf_clearance and not (config.get("cf_user_agent") or "").strip():
            logger.warning(
                "ANi-Strm已配置cf_clearance但未填写验证时的浏览器UA，中转将按内置默认UA发送；"
                "cf_clearance与UA绑定，两者必须成对来自同一解题浏览器，否则会继续触发挑战"
            )
        self._relay_token = (config.get("relay_token") or "").strip()
        self._relay_public_base = (config.get("relay_public_base") or "").strip().rstrip("/")
        self._rewrite_existing = bool(config.get("rewrite_existing"))

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
                logger.warning("ANi-Strm已开启内置中转但未配置中转访问地址，回退为直连反代地址")
                self._relay_enabled = False
        self._client.set_use_proxy(self._use_proxy)
        self._client.set_proxy_base(effective_base)
        # 目录请求同样经中转入口时必须携带 rt 令牌，否则被中转 403 拦下
        self._client.set_query_suffix(
            f"rt={self._relay_token}" if (self._relay_enabled and self._relay_token) else ""
        )
        logger.info(
            f"ANi-Strm配置加载：enabled={self._enabled}, onlyonce={self._onlyonce}, "
            f"use_proxy={self._use_proxy}, base={effective_base}, "
            f"relay={self._relay_enabled}(upstream={self._relay_upstream}, "
            f"cookie={'已配置' if self._cf_clearance else '未配置'}), "
            f"seasons={self._selected_seasons or []}, storage={self._storageplace}"
        )

        if not (self._enabled or self._onlyonce or self._relay_enabled):
            logger.info("ANi-Strm未启用、未触发立即运行且未开启中转，跳过任务注册")
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if self._onlyonce:
            logger.info("ANi-Strm服务启动，立即运行一次")
            self._scheduler.add_job(
                func=self.__task,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="ANiStrmRelay文件创建",
            )
            self._onlyonce = False

        if self._relay_enabled:
            self._scheduler.add_job(
                func=self.__relay_heartbeat,
                trigger=IntervalTrigger(minutes=RELAY_HEARTBEAT_MINUTES),
                name="ANiStrmRelay中转保活",
                id="anistrm_relay_heartbeat",
            )
            logger.info(f"ANi-Strm中转cf_clearance保活任务已注册（每{RELAY_HEARTBEAT_MINUTES}分钟）")

        self.__update_config()

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def __task(self):
        """
        拉取目标季度目录并补齐（或按配置改写）strm 文件。
        """
        seasons = self.__get_target_seasons()
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

        query_suffix = f"rt={self._relay_token}" if (self._relay_enabled and self._relay_token) else None

        for season in seasons:
            file_entries = self._client.get_season_entries(season)
            season_total = len(file_entries)
            season_created = 0
            season_exists = 0
            season_updated = 0
            season_failed = 0
            logger.info(f"ANi-Strm开始处理季度：{season}，文件数={season_total}")
            for file_entry in file_entries:
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

    def _rotate_clearance(self, resp: httpx.Response) -> None:
        """
        跟随 Cloudflare 的滚动续期：放行响应重新下发 cf_clearance 时热替换内存凭证。

        CF 在会话保持活跃期间会随正常响应重新 Set-Cookie 新的 cf_clearance，
        中转跟住这个滚动即可把"一次手贴"的有效窗口延续为整段连续观看期；
        撤销类空值（DELETED/空串）忽略，保留旧值等待人工重新验证。
        持久化按 5 分钟节流，避免每个播放请求都写库。

        :param resp: 已收到响应头的上游请求（可为未读取的流式响应）
        """
        new_value = ""
        for set_cookie in resp.headers.get_list("set-cookie"):
            name, _, value = set_cookie.split(";", 1)[0].partition("=")
            if name.strip().lower() == "cf_clearance":
                new_value = value.strip()
        if not new_value or new_value.upper() == "DELETED" or new_value == self._cf_clearance:
            return
        self._cf_clearance = new_value
        logger.info("ANi-Strm中转已捕获滚动刷新的cf_clearance（热生效）")
        now = time.time()
        if now - self._relay_persist_ts >= 300:
            self._relay_persist_ts = now
            self.__update_config()

    def __relay_heartbeat(self):
        """
        cf_clearance 保活心跳：定期以解题浏览器UA+凭证直连上游探活，顺带捕获滚动刷新的新凭证。

        运行于 APScheduler 工作线程，不能复用绑定 FastAPI 事件循环的全局异步客户端，
        使用独立的短生命周期同步 HTTP/2 客户端。探活失败（命中挑战页或网络异常）时
        停用内置中转、摘除本任务并发送通知，避免媒体服务器持续打到已失效的中转。
        """
        if not self._cf_clearance:
            return
        try:
            with httpx.Client(
                http2=True,
                follow_redirects=True,
                timeout=httpx.Timeout(15.0, read=30.0, write=10.0, pool=10.0),
            ) as client:
                resp = client.post(
                    f"{self._relay_upstream.rstrip('/')}/",
                    headers={
                        "User-Agent": self._cf_user_agent,
                        "Cookie": f"cf_clearance={self._cf_clearance}",
                    },
                    content=b'{"password":""}',
                )
            body_head = resp.content[:4096]
            challenged = any(mark in body_head for mark in CHALLENGE_MARKS) or (
                resp.status_code in (403, 429, 503)
                and b"cloudflare" in resp.content.lower()
            ) or resp.headers.get("cf-mitigated") == "challenge"
            if challenged:
                raise RuntimeError("上游返回CF人机挑战页，cf_clearance已失效（或出口IP/UA变更）")
            self._rotate_clearance(resp)
            logger.debug("ANi-Strm中转cf_clearance保活心跳正常")
        except Exception as err:
            logger.warning(f"ANi-Strm中转保活失败，自动停用内置中转：{err}")
            self._relay_enabled = False
            self.__update_config()
            try:
                if self._scheduler:
                    self._scheduler.remove_job("anistrm_relay_heartbeat")
            except Exception as remove_err:
                logger.warning(f"ANi-Strm摘除保活任务失败：{remove_err}")
            self.post_message(
                title="【ANi Strm 中转】保活失败，内置CF中转已停用",
                text=(
                    f"**插件**：{self.plugin_name} v{self.plugin_version}\n"
                    f"**时间**：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"**源站**：{self._relay_upstream}\n"
                    f"**原因**：{str(err)[:120]}\n"
                    "\n"
                    "**影响**\n"
                    "- 内置CF中转已自动停用，strm 播放请求将返回 503\n"
                    "- 保活心跳已摘除，不会自动重试\n"
                    "\n"
                    "**恢复步骤**\n"
                    "1. 用与服务器相同出口IP的浏览器访问源站，重新完成人机验证\n"
                    "2. 在插件设置填入新的 cf_clearance 与该浏览器UA\n"
                    "3. 重新打开「内置CF中转」开关并保存，保活自动恢复"
                ),
            )

    async def api_relay(self, full_path: str, request: Request):
        """
        CF 透传中转入口：注入 cf_clearance 与解题 UA 后按 HTTP/2 转发到上游源站。

        支持 GET/HEAD 文件流（含 Range 断点续传）与 POST 目录列表；命中挑战页时返回
        502 并提示更新 cookie，strm 文件本身不含凭证、无需重新生成。

        :param full_path: 相对于上游源站的路径（由宿主路由解出）
        :param request: 当前请求（媒体服务器或本插件目录请求）
        :return: 上游响应透传，或错误提示 JSON
        """
        if not self._relay_enabled:
            return JSONResponse(
                {"success": False, "message": "ANi-Strm内置中转未启用，请在插件设置中开启"},
                status_code=503,
            )
        if self._relay_token and request.query_params.get("rt") != self._relay_token:
            logger.warning(f"ANi-Strm中转拒绝了无效令牌请求：/{full_path}")
            return JSONResponse(
                {"success": False, "message": "中转访问令牌无效"},
                status_code=403,
            )
        if not self._cf_clearance:
            logger.warning("ANi-Strm中转未配置cf_clearance，请求已拒绝；请过完人机验证后在插件设置填入")
            return JSONResponse(
                {"success": False, "message": "未配置cf_clearance：请先在浏览器过人机验证后填入"},
                status_code=503,
            )

        upstream_url = f"{self._relay_upstream.rstrip('/')}/{quote(full_path, safe='/')}"
        headers = {
            key: value
            for key, value in request.headers.items()
            if key.lower() in RELAY_PASSTHROUGH_REQ_HEADERS
        }
        headers["User-Agent"] = self._cf_user_agent
        headers["Cookie"] = f"cf_clearance={self._cf_clearance}"
        query = [
            (key, value)
            for key, value in request.query_params.multi_items()
            if key != "rt"
        ]
        body = await request.body()

        client = _relay_client()
        try:
            upstream_request = client.build_request(
                method=request.method,
                url=upstream_url,
                headers=headers,
                data=body if body else None,
                params=query if query else None,
            )
            resp = await client.send(upstream_request, stream=True)
        except httpx.HTTPError as err:
            logger.error(f"ANi-Strm中转上游请求失败：{request.method} {upstream_url} - {err}")
            return JSONResponse(
                {"success": False, "message": f"上游请求失败：{err}"},
                status_code=502,
            )

        self._rotate_clearance(resp)
        resp_headers = {
            key.lower(): value
            for key, value in resp.headers.items()
            if key.lower() in RELAY_PASSTHROUGH_RESP_HEADERS
        }
        ctype = resp_headers.get("content-type", "")

        try:
            if ctype.startswith(("text/", "application/json")):
                await resp.aread()
                head = resp.content[:4096]
                challenged = any(mark in head for mark in CHALLENGE_MARKS) or (
                    resp.status_code in (403, 429, 503)
                    and b"cloudflare" in resp.content.lower()
                )
                if challenged:
                    logger.warning(
                        "ANi-Strm中转收到CF挑战页：cf_clearance已失效（或出口IP/UA变更），"
                        "请在同出口IP浏览器重新过一次人机验证并更新插件中的cookie"
                    )
                    return JSONResponse(
                        {
                            "success": False,
                            "message": (
                                "cf_clearance失效：请在与本机相同出口IP的浏览器重新过一次"
                                "人机验证，更新插件设置中的cookie即可，strm文件无需重新生成"
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

            async def _chunks():
                """
                按块透传上游字节流；播放器快进/停止提前断开属正常情况。
                """
                try:
                    async for chunk in resp.aiter_raw():
                        yield chunk
                finally:
                    await resp.aclose()

            return StreamingResponse(
                _chunks(), status_code=resp.status_code, headers=resp_headers
            )
        except httpx.HTTPError as err:
            await resp.aclose()
            logger.warning(f"ANi-Strm中转流式传输中断：{upstream_url} - {err}")
            return JSONResponse(
                {"success": False, "message": f"流式传输中断：{err}"},
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
            "upstream": self._relay_upstream or DEFAULT_UPSTREAM,
            "cookie_configured": bool(self._cf_clearance),
            "user_agent": self._cf_user_agent,
            "token_configured": bool(self._relay_token),
            "suggested_public_base": suggested_base,
        }
        probe_url = f"{(self._relay_upstream or DEFAULT_UPSTREAM).rstrip('/')}/"
        try:
            egress_resp = await _relay_client().get("https://ifconfig.co/ip", timeout=10.0)
            result["mp_egress_ip"] = egress_resp.text.strip()
        except httpx.HTTPError as err:
            result["mp_egress_ip"] = f"探测失败：{err}"
        try:
            headers = {"User-Agent": self._cf_user_agent}
            if self._cf_clearance:
                headers["Cookie"] = f"cf_clearance={self._cf_clearance}"
            resp = await _relay_client().post(
                probe_url, headers=headers, data='{"password":""}', timeout=20.0
            )
            body_head = resp.content[:4096]
            challenged = any(mark in body_head for mark in CHALLENGE_MARKS) or (
                resp.status_code in (403, 429, 503)
                and b"cloudflare" in resp.content.lower()
            )
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
                                    "md": 3,
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
                                    "md": 3,
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
                                    "md": 3,
                                },
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 3,
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
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "cf_clearance",
                                            "label": "cf_clearance Cookie",
                                            "rows": 2,
                                            "placeholder": "浏览器过一次人机验证后复制 cf_clearance 的值（不含其它cookie）",
                                            "hint": "cf_clearance 与出口IP+User-Agent绑定；过期后播放出现502提示时，重新验证并更新此处即可，已生成的strm文件不需要重新生成",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cf_user_agent",
                                            "label": "验证时的浏览器UA",
                                            "placeholder": DEFAULT_CF_UA,
                                            "hint": "必须与取得cookie的浏览器完全一致，否则会重新触发挑战",
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
                                },
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "自动从open ANi抓取下载直链生成strm文件\n"
                                            "默认选择最新季，也支持季度多选，自动补齐所选季度缺失的strm\n"
                                            "配合目录监控使用，strm文件创建在/downloads/strm\n"
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
                                            "1. 用浏览器（与MP同一出口IP）访问反代/源站地址并完成人机验证，复制 cf_clearance\n"
                                            "2. 填入 cookie 与验证时浏览器的UA，填写「中转访问地址」后开启内置CF中转\n"
                                            "3. 勾选「改写已存在strm」运行一次，旧直链自动替换为中转地址，之后取消勾选\n"
                                            "cookie过期时播放会返回502提示，仅需更新cookie，strm文件无需重新生成\n"
                                            "中转接口匿名可达，建议设置访问令牌；媒体服务器需能直连MP（开启中转后请勿再配http_proxy）",
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
            "onlyonce": False,
            "storageplace": "/downloads/strm",
            "selected_seasons": ["latest"],
            "proxy_base": DEFAULT_UPSTREAM,
            "relay_enabled": False,
            "cf_clearance": "",
            "cf_user_agent": "",
            "relay_token": "",
            "relay_public_base": "",
            "rewrite_existing": False,
        }

    def __build_season_options(self) -> List[Dict[str, str]]:
        """
        构造「拉取季度」下拉项（最新季 + 全部季度 + 上游可用季度）。

        :return: 下拉选项列表
        """
        seasons = self._client.get_available_seasons() or [self._client._get_local_season()]
        return [
            {"title": "最新季", "value": "latest"},
            {"title": "全部季度", "value": "all"},
        ] + [
            {"title": season, "value": season} for season in seasons
        ]

    def __update_config(self):
        """
        回写运行期配置（含中转各项），保持宿主整体替换语义下配置不丢失。
        """
        self.update_config(
            {
                "onlyonce": self._onlyonce,
                "enabled": self._enabled,
                "use_proxy": self._use_proxy,
                "storageplace": self._storageplace,
                "selected_seasons": self._selected_seasons,
                "proxy_base": self._proxy_base,
                "relay_enabled": self._relay_enabled,
                "cf_clearance": self._cf_clearance,
                "cf_user_agent": self._cf_user_agent,
                "relay_token": self._relay_token,
                "relay_public_base": self._relay_public_base,
                "rewrite_existing": self._rewrite_existing,
            }
        )

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        注销定时任务（共享HTTP客户端常驻，不随插件重载关闭）。
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
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
        # 目录请求的固定查询后缀（走内置中转时携带 rt 令牌；直连为空）
        self._query_suffix = ""

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
        def operation():
            return self._collect_folder_entries(f"{season}/")

        return self._with_retry(operation, default=[])

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
        if self._query_suffix and "?" not in url:
            url = f"{url}?{self._query_suffix}"
        response = self._request_factory().post(
            url=url,
            data='{"password":""}',
        )
        if not response:
            raise ValueError(f"目录请求失败：{url}")
        try:
            return response.json()
        finally:
            response.close()

    def _build_request_utils(self) -> RequestUtils:
        return RequestUtils(
            ua=settings.USER_AGENT if settings.USER_AGENT else None,
            proxies=settings.PROXY if self._use_proxy and settings.PROXY else None,
        )

    def _collect_folder_entries(self, folder_path: str, relative_dir: str = "") -> List[Dict[str, str]]:
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
                entries.extend(self._collect_folder_entries(child_folder_path, child_relative_dir))
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
        remaining = tries
        wait_seconds = delay
        while remaining > 0:
            try:
                return operation()
            except Exception as err:
                remaining -= 1
                if remaining == 0:
                    logger.warning(f"ANiStrmRelay请求失败，已达到最大重试次数：{err}")
                    break
                logger.warning(f"未获取到文件信息，{wait_seconds}秒后重试 ...")
                time.sleep(wait_seconds)
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
