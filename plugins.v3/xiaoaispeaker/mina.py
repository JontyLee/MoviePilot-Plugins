"""
mina.py — 小米账号登录 + 小爱音箱（Mina/MIoT）云端控制协议。

出处与许可：本模块把 Songloft「智能音箱」插件 songloft-plugin-miot
（https://github.com/songloft-org/songloft-plugin-miot ，Apache-2.0 © 2026 hanxi，
TypeScript/QuickJS）的小米云端接入约定移植到 Python：登录三步流程、扫码登录流程、
clientSign 算法、ubus 请求体、设备型号白名单与 play_music 载荷都按它的契约实现，并按
Yonsm/MiService（MIT）与 Music Tag Web（GPL-3.0，其小米协议本身转调 miservice）的
同类实现交叉核对。两处与 miot 取值不一致的地方在对应位置的注释里点明。移植而非复制：
语言、运行时、错误处理与网络层都是本仓库自己的写法（宿主 RequestUtils + requests 会话）。
上游只在 README 与 plugin.json 里声明 Apache-2.0、仓库内没有 LICENSE 正文，本目录的
LICENSE.upstream-songloft-miot.txt 用的是 apache.org 的规范全文（未改动一个字），
并按 Apache-2.0 4(b) 在此声明这些约定经过修改与重写；本仓库整体是 GPL-3.0，
Apache-2.0 与 GPL-3.0 单向兼容，所以在保留上述声明与正文的前提下可以合进本仓库。

不实现的部分（有意省略，不是遗漏）：
- MiIO/MIoT RPC（api.io.mi.com 的签名请求）：miot 只用它在特定型号上走
  `siid-aiid` 的 TTS action，普通 `mibrain/text_to_speech` 已经能播报；签名链路
  需要额外的 xiaomiio serviceToken 与 HMAC 细节，收益不抵复杂度。
- 触屏歌词的 /music/search audioID 匹配：需要型号白名单命中且用户开歌词模式，
  默认走 player_play_url 与固定 audioID。
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import random
import re
import string
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

from app.sdk.logging import logger
from app.sdk.network import RequestUtils

# ---------------- 常量（与 miot mina/constants.ts 一致） ----------------

ACCOUNT_BASE_URL = "https://account.xiaomi.com"
MINA_API_BASE_URL = "https://api2.mina.mi.com"
MINA_SID = "micoapi"

# 登录 UA 里的 %s 由 deviceId 填充（Android 米家客户端）
USER_AGENT_TEMPLATE = "Android-7.1.1-1.0.0-ONEPLUS A3010-136-%s APP/xiaomi.smarthome APPV/62830"

# 小米云接口把 JSON 前面拼了这一段，直接 json.loads 会失败
JSON_PREFIX = "&&&START&&&"

SERVICE_TOKEN_VALID_HOURS = 12

# passToken 是长期票据：serviceToken 过期时用它静默换新的，不用用户重填账密
PASS_TOKEN_VALID_DAYS = 30

# 播放走云端 ubus，慢的时候能到十几秒；太短的超时会被当成设备离线
HTTP_TIMEOUT = 20

# 登录的跳转链长度上限（小米的 STS 跳转通常 2~3 跳）
MAX_REDIRECTS = 10

# 这类二次校验地址要先访问对应的 /identity/list 建立会话，直接提交验证码必失败
IDENTITY_AUTH_START = "/fe/service/identity/authStart"

# 需要走 player_play_music 的型号（miot DEFAULT_MUSIC_API_MODELS）。
# miservice 的那份是这里的子集，取更全的这份；配不准时用 play_mode="url" 覆盖。
MUSIC_API_MODELS: Tuple[str, ...] = (
    "X08C", "X08E", "X8F", "X4B", "LX05", "OH11", "OH2", "OH2P", "X6A",
    "LX04", "L05B", "L05C", "LX06", "L06A", "X08A", "X10A", "L15A", "L16A", "L17A",
)

# play_music 的默认 audioID：决定封面/歌词归属的云端条目，两家实现给的值不同
# （miot 1732418460076477549 / miservice 1582971365183456177），取 miot 的这份，
# 并在配置里可改 —— 选错顶多是触屏不显示封面，不影响出声。
DEFAULT_MUSIC_AUDIO_ID = "1732418460076477549"
MUSIC_CP_ID = "355454500"

# player_get_play_status 的 status 语义：0 停止、1 播放中、2 暂停
PLAY_STATUS_STOPPED = 0
PLAY_STATUS_PLAYING = 1
PLAY_STATUS_PAUSED = 2

# 语音识别记录（"小爱同学听到的话"）走的是 userprofile 域名而不是 api2.mina：
# %s=hardware（型号代号）、%d=毫秒时间戳、%d=条数上限。时间戳要取请求当刻的值，
# 小米按它做分页游标，写死一个会让记录永远停在第一批。
CONVERSATION_API = (
    "https://userprofile.mina.mi.com/device_profile/v2/conversation"
    "?source=dialogu&hardware=%s&timestamp=%d&limit=%d"
)
# 不认 userprofile 接口的型号（miot GET_ASK_BY_MINA）：这些设备只能退到 ubus 的
# nlp_result_get，那条路径拿不到 record.time，得自己维护时间基线
MINA_ASK_HARDWARE: Tuple[str, ...] = ("M01",)
# 每轮取几条。语音一来就播歌，5 条足够覆盖"连着说两句"且不把响应体撑大
CONVERSATION_LIMIT = 5

# 日志前缀与插件保持一致：出问题时一条 grep 就能把整条登录链路捞出来
_LOG_TAG = "[xiaoai]"

LOGIN_SUCCESS = "success"
LOGIN_NEED_CAPTCHA = "need_captcha"
LOGIN_NEED_VERIFY = "need_verify"
LOGIN_FAILED = "failed"

# 扫码登录（对齐 miot src/qrcode/qrcode.ts）：二维码挂在 mijia 会话上，
# 手机确认后拿到 passToken，再换成本插件真正要的 micoapi serviceToken。
QR_LOGIN_SID = "mijia"
QR_LONG_POLLING_URL = f"{ACCOUNT_BASE_URL}/longPolling/loginUrl"
# 二维码有效期以接口回的 timeout 为准，这个是取不到时的兜底（秒）
QR_DEFAULT_VALID_SECONDS = 300
# lp 是服务端长轮询：没人扫就一直挂着。客户端只等这么久就当「还在等」，
# 否则前端每点一次要转满 20 秒，看着像卡死
QR_POLL_TIMEOUT = 8
# 轮询次数上限：expires_at 已经能判过期，这一层防的是网络一直超时、状态却永不过期
QR_MAX_POLLS = 60

LOGIN_QR_WAITING = "qr_waiting"
LOGIN_QR_EXPIRED = "qr_expired"


class MinaError(Exception):
    """小米云端返回的业务失败（含鉴权失效），调用方据此决定重试或重新登录。"""


class MinaAuthError(MinaError):
    """票据失效：401 或响应明确说没权限，需要重新登录或刷新 serviceToken。"""


def generate_device_id() -> str:
    """生成 16 位大写设备标识，登录与 UA 都要用。

    必须持久化：每次登录换一个新 deviceId，小米风控更容易要验证码，
    passToken 也会跟着票据上下文一起废掉。
    """
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(16))


def _md5_upper(text: str) -> str:
    """小米登录要求的密码哈希：MD5 十六进制大写。"""
    return hashlib.md5(text.encode("utf-8")).hexdigest().upper()


def _client_sign(nonce: Any, ssecurity: str) -> str:
    """clientSign = base64(sha1("nonce={nonce}&{ssecurity}"))。"""
    raw = f"nonce={nonce}&{ssecurity}".encode("utf-8")
    return base64.b64encode(hashlib.sha1(raw).digest()).decode("utf-8")


def _strip_prefix(text: str) -> str:
    return (text or "").replace(JSON_PREFIX, "").strip()


def _as_int(value: Any) -> int:
    """把云端给的时间戳（数字或数字串）转成 int，转不了就是 0（调用方据此丢弃该条）。"""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _parse_json(text: str) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(_strip_prefix(text))
    except (TypeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _request_id() -> str:
    """ubus 的 requestId，形如 app_ios_ + 30 位随机串。"""
    alphabet = string.ascii_lowercase + string.digits
    return "app_ios_" + "".join(random.choice(alphabet) for _ in range(30))


def _form_body(params: Dict[str, Any]) -> str:
    return "&".join(f"{quote(str(k))}={quote(str(v))}" for k, v in params.items())


def _cookie_value(cookies: Dict[str, str], name: str) -> str:
    value = cookies.get(name) or ""
    return value.strip('"')


def _set_cookie_map(response: Any) -> Dict[str, str]:
    """只看这一次响应头里的 Set-Cookie，名字 → 值。

    单独拆出来是因为重定向链末端的 serviceToken 必须以「这一跳自己下的」为准：
    cookie 表里可能还留着别的 sid 换过的同名票据。
    """
    out: Dict[str, str] = {}
    for header in str((response.headers or {}).get("Set-Cookie", "") or "").split(", "):
        if not header or "=" not in header:
            continue
        name, _, value = header.split(";")[0].partition("=")
        if name:
            out[name.strip()] = value.strip('"')
    return out


def _collect_cookies(response: Any, cookies: Dict[str, str]) -> None:
    """把响应里的 Set-Cookie 并进调用方维护的 cookie 表。

    只用 requests 的 resp.cookies 不够：手动跟随重定向时，中间跳（尤其是签发
    serviceToken/passToken 的那一跳）的 cookie 必须逐跳收下来，否则最后拿不到票据。
    """
    try:
        for key, value in (getattr(response, "cookies", {}) or {}).items():
            cookies[key] = str(value)
    except Exception:  # pragma: no cover - 极端情况下 cookie jar 不是映射
        pass
    cookies.update(_set_cookie_map(response))


def _redirect_target(response: Any, current_url: str) -> Optional[str]:
    location = response.headers.get("Location") or ""
    if not location:
        return None
    if location.startswith("http"):
        return location
    if location.startswith("/"):
        parsed = urlparse(current_url)
        return f"{parsed.scheme}://{parsed.netloc}{location}"
    base = current_url.rsplit("/", 1)[0]
    return f"{base}/{location}"


def _fetch_image(url: str, user_agent: str, cookies: Dict[str, str]) -> str:
    """取一张小米回的二进制图（验证码、二维码），返回 data URI。

    图和票据在同一次响应里：验证码那次要顺带收下 ick，提交验证码时得带上，
    所以 cookies 就地更新，不返回新表。
    """
    try:
        response = RequestUtils(
            headers={"User-Agent": user_agent},
            cookies=dict(cookies),
            timeout=HTTP_TIMEOUT,
        ).get_res(url, allow_redirects=False, raise_exception=False)
    except Exception as err:  # pragma: no cover - 网络层异常形状不固定
        logger.warning(f"{_LOG_TAG} 图片请求异常: {err}")
        return ""
    if response is None:
        return ""
    _collect_cookies(response, cookies)
    raw = response.content or b""
    if not raw:
        return ""
    mime = "image/png"
    ctype = str((response.headers or {}).get("Content-Type") or "").split(";")[0].strip().lower()
    if ctype.startswith("image/"):
        mime = ctype
    return f"data:{mime};base64," + base64.b64encode(raw).decode("ascii")


class XiaoaiAuth:
    """小米账号登录状态机。

    一次登录可能停在三个地方：直接成功、要图形验证码、要手机/邮箱验证码。
    验证码不是「重试一次就好」的分支 —— 实例必须活着跨到用户把码填回来，
    所以这里把中间态（step1 的 _sign/qs/callback、ick、verifyUrl、已收的 cookie）
    都存在 self 上，由插件持有实例本身。
    """

    def __init__(self, username: str, password: str, device_id: str = "",
                 login_method: str = "password") -> None:
        self.username = username
        self.password = password
        self.login_method = login_method or "password"
        self.device_id = device_id or generate_device_id()
        self.user_agent = USER_AGENT_TEMPLATE % self.device_id
        self.cookies: Dict[str, str] = {
            "sdkVersion": "3.8.6",
            "deviceId": self.device_id,
        }
        self._step1: Dict[str, Any] = {}
        self._captcha_url: str = ""
        self._verify_url: str = ""
        self._ssecurity: str = ""
        self._user_id: str = ""
        self._location: str = ""

    # ---------------- 请求 ----------------

    def _get(self, url: str, allow_redirects: bool = False,
             timeout: int = HTTP_TIMEOUT) -> Any:
        return RequestUtils(
            headers={"User-Agent": self.user_agent},
            cookies=dict(self.cookies),
            timeout=timeout,
        ).get_res(url, allow_redirects=allow_redirects, raise_exception=False)

    def _post(self, url: str, params: Dict[str, Any]) -> Any:
        return RequestUtils(
            headers={
                "User-Agent": self.user_agent,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            cookies=dict(self.cookies),
            timeout=HTTP_TIMEOUT,
        ).post_res(url, data=_form_body(params), allow_redirects=False,
                    raise_exception=False)

    def _follow(self, url: str, max_hops: int = 10) -> None:
        """跟随重定向链，只为了收集每一跳的 cookie（passToken/serviceToken）。"""
        current = url
        for _ in range(max_hops):
            response = self._get(current)
            if response is None:
                return
            _collect_cookies(response, self.cookies)
            if response.status_code not in (301, 302, 303, 307, 308):
                return
            current = _redirect_target(response, current)
            if not current:
                return

    # ---------------- 登录流程 ----------------

    def login(self) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """开始登录，返回 (state, token, message)。"""
        step1 = self._step1_login()
        if step1 is None:
            return LOGIN_FAILED, None, "获取小米登录签名失败（网络或接口变更）"
        self._step1 = step1
        return self._step2_login("")

    def submit_captcha(self, code: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """提交图形验证码后继续第二步。"""
        if not self._step1:
            return LOGIN_FAILED, None, "验证码会话已过期，请重新发起登录"
        self._captcha_url = ""
        return self._step2_login(code)

    def submit_verify_code(self, code: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """提交手机/邮箱验证码：验票据 → 收 passToken → 换 serviceToken。"""
        if not self._verify_url:
            return LOGIN_FAILED, None, "没有待验证的二次校验会话，请重新发起登录"
        location = self._verify_ticket(code)
        if not location:
            return LOGIN_NEED_VERIFY, None, "验证码不正确或已失效"
        self._follow(location)
        self._verify_url = ""
        state, token, message = self._service_login()
        if state == LOGIN_SUCCESS:
            message = "二次校验通过，登录成功"
        return state, token, message

    @classmethod
    def from_token(cls, token: Dict[str, Any]) -> "XiaoaiAuth":
        """从存档票据恢复会话：只为拿 passToken 做静默续期，不需要账密。"""
        token = token or {}
        mina = (token.get("services") or {}).get(MINA_SID) or {}
        auth = cls(
            username=str(token.get("username") or ""),
            password="",
            device_id=str(token.get("device_id") or ""),
            login_method=str(token.get("login_method") or "password"),
        )
        auth._user_id = str(token.get("user_id") or "")
        auth._ssecurity = str(mina.get("ssecurity") or "")
        for name, value in (("passToken", token.get("pass_token")),
                            ("cUserId", token.get("c_user_id")),
                            ("userId", auth._user_id)):
            if value:
                auth.cookies[str(name)] = str(value)
        return auth

    def refresh(self) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """用 passToken 静默换一个新的 serviceToken。"""
        if not _cookie_value(self.cookies, "passToken"):
            hint = "请重新扫码" if self.login_method == "qrcode" else "请填写账号密码重新登录"
            return (LOGIN_FAILED, None,
                    f"本地没有 passToken，无法静默续期，{hint}")
        return self._service_login()

    def _step1_login(self) -> Optional[Dict[str, Any]]:
        url = f"{ACCOUNT_BASE_URL}/pass/serviceLogin?sid={MINA_SID}&_json=true"
        response = self._get(url)
        if response is None:
            return None
        _collect_cookies(response, self.cookies)
        return _parse_json(response.text or "")

    def _step2_login(self, captcha: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
        url = f"{ACCOUNT_BASE_URL}/pass/serviceLoginAuth2?_json=true"
        params: Dict[str, Any] = {
            "user": self.username,
            "hash": _md5_upper(self.password),
            "callback": self._step1.get("callback") or "",
            "sid": self._step1.get("sid") or MINA_SID,
            "qs": self._step1.get("qs") or "",
            "_sign": self._step1.get("_sign") or "",
        }
        if captcha:
            params["captCode"] = captcha
        response = self._post(url, params)
        if response is None:
            return LOGIN_FAILED, None, "小米登录请求失败（网络或风控）"
        _collect_cookies(response, self.cookies)
        data = _parse_json(response.text or "")
        if not data:
            return LOGIN_FAILED, None, "小米登录响应无法解析"
        # 只记字段名与返回码：这一步是分岔口，事后光看「登录失败」没人知道
        # 小米到底要验证码、要短信还是直接拒了（字段值可能是票据，不落日志）
        logger.info(
            f"{_LOG_TAG} 登录第二步 HTTP={getattr(response, 'status_code', '?')} "
            f"code={data.get('code')} fields={sorted(str(k) for k in data)}"
        )

        # 二次校验（手机/邮箱验证码）：小米在给出这个地址的同时就把码发出去了，
        # 插件不需要（也不该）替用户去访问它 —— 那是浏览器里另一份会话。
        notification = data.get("notificationUrl")
        if notification:
            self._verify_url = self._abs_url(notification)
            channel = "邮箱" if "email" in self._verify_url else "手机短信"
            return (LOGIN_NEED_VERIFY, None,
                    f"小米要求{channel}二次校验，验证码已发往账号绑定的{channel}；"
                    "收不到就点「重发」重新发起一次")

        # 图形验证码：兼容小米返回过的多种字段名
        captcha_raw = ""
        for field in ("captchaUrl", "captchaURL", "captcha_url", "captcha", "imgUrl"):
            value = data.get(field)
            if isinstance(value, str) and value.strip():
                captcha_raw = value.strip()
                break
        if captcha_raw:
            self._captcha_url = self._abs_url(captcha_raw)
            image = self._fetch_captcha(self._captcha_url)
            hint = "小米要求图形验证码，请在下方填写"
            if not image:
                hint = "小米要求图形验证码，但图片获取失败（见插件日志），可重试登录"
            return (LOGIN_NEED_CAPTCHA, {"captcha_image": image}, hint)

        location = data.get("location")
        if not location:
            code = data.get("code")
            desc = str(data.get("description") or data.get("desc") or "").strip()
            message = f"小米拒绝了本次登录：code={code}"
            if desc:
                message += f"，说明：{desc}"
            # 密码这条路被风控时不会给理由，扫码是同一账号下另一条能走通的路
            message += "；账密反复登不上就改用扫码登录"
            return LOGIN_FAILED, None, message

        self._ssecurity = str(data.get("ssecurity") or "")
        self._user_id = str(data.get("userId") or "")
        # 没有 location 的尾段会缺 clientSign 参数，这里按 miot 的拼法补
        self._location = self._append_params(location, {
            "_userIdNeedEncrypt": "true",
            "clientSign": _client_sign(data.get("nonce"), self._ssecurity),
        })
        return self._step3_login()

    def _step3_login(self) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """跟随 location 的重定向链，从 cookie 收 serviceToken。"""
        if not self._location:
            return LOGIN_FAILED, None, "登录会话已过期，请重新发起登录"
        service_token, detail = self._walk_location(self._location)
        if not service_token:
            return LOGIN_FAILED, None, f"没有取到 serviceToken：{detail}"
        return self._finish(service_token, "登录成功")

    def _walk_location(self, location: str) -> Tuple[str, str]:
        """走完跳转链取 serviceToken，失败时给一句能查的原因。

        认「最后一跳自己下的」那颗 serviceToken：cookie 表里可能还留着别的
        sid 换过的同名票据，只读表会把上一个服务的票据当成这次的（小米不报错，
        后面每次 ubus 都 401，最难查的一种失败）。
        """
        service_token = ""
        current = (location or "").strip()
        hops = 0
        last_status = 0
        while current and hops < MAX_REDIRECTS:
            hops += 1
            response = self._get(current)
            if response is None:
                return "", f"第 {hops} 跳没有响应（网络或风控中断）"
            _collect_cookies(response, self.cookies)
            hop_token = _set_cookie_map(response).get("serviceToken") or ""
            if hop_token:
                service_token = hop_token
            last_status = response.status_code
            if last_status not in (301, 302, 303, 307, 308):
                break
            current = _redirect_target(response, current)
        if not service_token:
            service_token = _cookie_value(self.cookies, "serviceToken")
        if not service_token:
            return "", f"跳转走了 {hops} 跳，最后 HTTP {last_status}，链上没出现 serviceToken"
        return service_token, ""

    def _service_login(self) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """用 passToken 换 serviceToken（serviceToken 会到期，passToken 不会）。"""
        pass_token = _cookie_value(self.cookies, "passToken")
        if not pass_token:
            return (LOGIN_FAILED, None,
                    "缺少 passToken，无法静默续期，请重新输入账密登录")
        url = f"{ACCOUNT_BASE_URL}/pass/serviceLogin?sid={MINA_SID}&_json=true"
        self.cookies.update({
            "passToken": pass_token,
            "deviceId": self.device_id,
            "sdkVersion": "3.8.6",
        })
        response = self._get(url)
        if response is None:
            return LOGIN_FAILED, None, "静默续期请求失败"
        _collect_cookies(response, self.cookies)
        data = _parse_json(response.text or "")
        if not data or int(data.get("code") or 0) != 0:
            return (LOGIN_FAILED, None,
                    f"静默续期失败：code={(data or {}).get('code')}, "
                    f"desc={(data or {}).get('desc') or '未知原因'}")
        location = data.get("location")
        if not location:
            return LOGIN_FAILED, None, "静默续期没有返回跳转地址"
        ssecurity = str(data.get("ssecurity") or "")
        if data.get("userId"):
            self._user_id = str(data.get("userId"))
        self._location = self._append_params(location, {
            "_userIdNeedEncrypt": "true",
            "clientSign": _client_sign(data.get("nonce"), ssecurity),
        })
        self._ssecurity = ssecurity or self._ssecurity
        service_token, detail = self._walk_location(self._location)
        if not service_token:
            return LOGIN_FAILED, None, f"没有取到 serviceToken：{detail}"
        return self._finish(service_token, "已用 passToken 静默续期")

    def _finish(self, service_token: str, ok_message: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
        if not service_token:
            return LOGIN_FAILED, None, "没有取到 serviceToken（登录被风控或未通过验证）"
        self._user_id = self._user_id or _cookie_value(self.cookies, "userId")
        token = {
            "username": self.username,
            "user_id": self._user_id,
            "device_id": self.device_id,
            "login_method": self.login_method,
            "services": {
                MINA_SID: {
                    "service_token": service_token,
                    "ssecurity": self._ssecurity,
                    "expires_at": time.time() + SERVICE_TOKEN_VALID_HOURS * 3600,
                }
            },
            "pass_token": _cookie_value(self.cookies, "passToken"),
            "c_user_id": _cookie_value(self.cookies, "cUserId"),
            "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        return LOGIN_SUCCESS, token, ok_message

    def _fetch_captcha(self, url: str) -> str:
        """取验证码图片，返回 data URI；同时收下 ick（提交验证码要带）。"""
        return _fetch_image(url, self.user_agent, self.cookies)

    def _prepare_identity(self) -> None:
        """authStart 形态的二次校验要先建立 identity 会话。

        小米把这类校验做成「先列可选方式、再验选中的那一种」两步：直接 POST
        verifyPhone/verifyEmail 会没有会话上下文，用户填的码是对的也判失败 ——
        这一步补不上，表现就是二次校验环节怎么试都过不去。
        """
        if IDENTITY_AUTH_START not in (self._verify_url or ""):
            return
        url = self._verify_url.replace(IDENTITY_AUTH_START, "/identity/list")
        try:
            response = self._get(url)
            if response is not None:
                _collect_cookies(response, self.cookies)
        except Exception as err:  # pragma: no cover - 网络层异常形状不固定
            logger.warning(f"{_LOG_TAG} 二次校验会话准备失败: {err}")

    def _verify_ticket(self, code: str) -> str:
        """提交短信/邮箱验证码，成功时返回后续跳转地址。"""
        self._prepare_identity()
        is_email = "email" in (self._verify_url or "")
        api = "/identity/auth/verifyEmail" if is_email else "/identity/auth/verifyPhone"
        flag = "8" if is_email else "4"
        url = f"{ACCOUNT_BASE_URL}{api}?_dc={int(time.time() * 1000)}"
        response = self._post(url, {
            "ticket": code,
            "trust": "true",
            "_json": "true",
            "_flag": flag,
        })
        if response is None:
            logger.warning(f"{_LOG_TAG} 二次校验提交没有拿到响应（网络或风控）: {api}")
            return ""
        _collect_cookies(response, self.cookies)
        data = _parse_json(response.text or "")
        if not data:
            logger.warning(f"{_LOG_TAG} 二次校验响应无法解析: HTTP="
                           f"{getattr(response, 'status_code', '?')}")
            return ""
        if int(data.get("code") or 0) != 0:
            # 「码填错了」和「会话已经不是那次会话」在界面上长得一样，只有这里分得开
            logger.info(f"{_LOG_TAG} 二次校验被拒: {api} code={data.get('code')} "
                        f"desc={str(data.get('description') or data.get('desc') or '')[:120]}")
            return ""
        location = data.get("location")
        return self._abs_url(location) if location else ""

    @staticmethod
    def _abs_url(value: str) -> str:
        value = (value or "").strip()
        if not value:
            return ""
        if value.startswith("http"):
            return value
        return f"{ACCOUNT_BASE_URL}{value}"

    @staticmethod
    def _append_params(url: str, params: Dict[str, str]) -> str:
        sep = "&" if "?" in url else "?"
        return url + sep + "&".join(
            f"{quote(k)}={quote(v)}" for k, v in params.items())


class XiaoaiQrLogin:
    """小米扫码登录：mijia 会话取二维码 → 长轮询等确认 → passToken 换 micoapi 票据。

    接口约定对齐 miot 的 src/qrcode/qrcode.ts。和账密相比，扫码不走「新设备登录」
    那套风控分支，也不用把密码放进配置：密码反复登不上时，这条通常是同一个账号
    走得通的路。
    """

    def __init__(self, device_id: str = "") -> None:
        self.device_id = device_id or generate_device_id()
        self.user_agent = USER_AGENT_TEMPLATE % self.device_id
        self.cookies: Dict[str, str] = {
            "sdkVersion": "3.8.6",
            "deviceId": self.device_id,
        }
        self.qr_image_url = ""
        self.tips = ""
        # loginUrl 是二维码的内容，等于一次性的登录凭证：只在服务端拿着，
        # 绝不回给前端 —— 谁拿到它都能替这个人完成登录
        self.login_url = ""
        self._sign = ""
        self._qs = ""
        self._callback = ""
        self._poll_url = ""
        self._expires_at = 0.0
        self._polls = 0
        self._non_zero = 0
        # 判过死刑的二维码要一直保持「已过期」：轮询方是个自动循环，
        # 一个终端状态回退成「继续等」会让界面永远停不下来
        self._dead = ""

    # ---------------- 状态 ----------------

    def ready(self) -> bool:
        return bool(self._poll_url)

    def expired(self) -> bool:
        return bool(self._expires_at) and time.time() >= self._expires_at

    def seconds_left(self) -> int:
        # 向上取整：刚取到的二维码要显示整 300 秒，而不是截断出来的 299
        return max(0, math.ceil(self._expires_at - time.time())) if self._expires_at else 0

    def reset(self) -> None:
        self.qr_image_url = ""
        self.tips = ""
        self.login_url = ""
        self._sign = self._qs = self._callback = self._poll_url = ""
        self._expires_at = 0.0
        self._polls = 0
        self._non_zero = 0
        self._dead = ""

    # ---------------- 请求 ----------------

    def _get(self, url: str, timeout: int = HTTP_TIMEOUT) -> Any:
        return RequestUtils(
            headers={"User-Agent": self.user_agent},
            cookies=dict(self.cookies),
            timeout=timeout,
        ).get_res(url, allow_redirects=False, raise_exception=False)

    # ---------------- 二维码 ----------------

    def start(self) -> Tuple[bool, str]:
        """取一张新二维码；成功才能取图并开始轮询。"""
        self.reset()
        step1 = self._get_json(f"{ACCOUNT_BASE_URL}/pass/serviceLogin"
                               f"?sid={QR_LOGIN_SID}&_json=true")
        if step1 is None:
            return False, "获取小米扫码签名失败（网络或接口变更）"
        self._sign = str(step1.get("_sign") or "")
        self._qs = str(step1.get("qs") or "")
        self._callback = str(step1.get("callback") or "")
        if not (self._sign and self._qs and self._callback):
            return False, f"扫码签名不完整：code={step1.get('code')}"

        query = _form_body({
            "_qrsize": "240",
            "qs": self._qs,
            "sid": QR_LOGIN_SID,
            "_sign": self._sign,
            "callback": self._callback,
            "_json": "true",
            "_dc": int(time.time() * 1000),
        })
        qr = self._get_json(f"{QR_LONG_POLLING_URL}?{query}")
        if qr is None:
            return False, "获取二维码失败（网络或接口变更）"
        if int(qr.get("code") or 0) != 0:
            return False, f"小米没有给出二维码：code={qr.get('code')}, " \
                          f"desc={qr.get('desc') or qr.get('description') or '未说明'}"
        self.qr_image_url = str(qr.get("qr") or "")
        self.login_url = str(qr.get("loginUrl") or "")
        self._poll_url = str(qr.get("lp") or "")
        self.tips = str(qr.get("qrTips") or "")
        if not self._poll_url:
            return False, "扫码接口没有返回轮询地址"
        if not self.qr_image_url:
            return False, "扫码接口没有返回二维码图片"
        self._expires_at = time.time() + (_as_int(qr.get("timeout"))
                                          or QR_DEFAULT_VALID_SECONDS)
        logger.info(f"{_LOG_TAG} 二维码已就绪，有效期 {self.seconds_left()} 秒")
        return True, ""

    def image(self) -> str:
        """二维码图片（data URI）；拿不到就返回空串，由调用方提示重新获取。"""
        if not self.qr_image_url:
            return ""
        return _fetch_image(self.qr_image_url, self.user_agent, self.cookies)

    def poll(self) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """问一次「扫了吗」。

        lp 是服务端长轮询：没人扫就一直挂着，所以客户端超时是常态而不是故障，
        一次超时都不能算登录失败。
        """
        if not self._poll_url:
            return LOGIN_FAILED, None, "还没有拿到二维码，先点「获取二维码」"
        if self._dead:
            return LOGIN_QR_EXPIRED, None, self._dead
        if self.expired():
            return self._expire("二维码已过期，重新获取一张再扫")
        self._polls += 1
        if self._polls > QR_MAX_POLLS:
            return self._expire("等扫码等得太久，重新获取一张二维码")
        response = self._get(self._poll_url, timeout=QR_POLL_TIMEOUT)
        if response is None:
            return LOGIN_QR_WAITING, None, ""
        if response.status_code == 403:
            return self._expire("这张二维码已经作废（过期或在别处扫过），重新获取一张")
        if response.status_code >= 400:
            return LOGIN_FAILED, None, f"查询扫码状态失败：HTTP {response.status_code}"
        data = _parse_json(response.text or "")
        if data is None:
            return LOGIN_QR_WAITING, None, ""
        code = int(data.get("code") or 0)
        if code != 0:
            self._non_zero += 1
            if self._non_zero >= 2:
                return self._expire(
                    f"二维码已过期：code={code}, desc={data.get('desc') or '小米未说明'}")
            # 单次非零码多是长轮询自己的重连抖动，一次就判过期会把正在扫码的人赶走
            return LOGIN_QR_WAITING, None, ""
        self._non_zero = 0
        pass_token = str(data.get("passToken") or "")
        user_id = str(data.get("userId") or "")
        if not pass_token or not user_id:
            # 正常回包但没有票据：还没确认，或这是一次空的重连响应
            return LOGIN_QR_WAITING, None, ""
        state, token, message = self._exchange(
            pass_token, user_id, str(data.get("cUserId") or ""))
        if state != LOGIN_SUCCESS:
            return LOGIN_FAILED, None, message
        logger.info(f"{_LOG_TAG} 扫码登录拿到票据，userId 已确认")
        return LOGIN_SUCCESS, token, "扫码登录成功"

    def _expire(self, message: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """把这次扫码会话钉死成「已过期」，并保留原因给界面照着说。"""
        self._dead = message
        return LOGIN_QR_EXPIRED, None, message

    def _exchange(self, pass_token: str, user_id: str,
                  c_user_id: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
        """把扫码换来的 passToken 换成小爱服务（micoapi）的 serviceToken。

        deviceId 沿用扫码这一套：小米把票据绑在设备标识上，中途换一个反而更像
        异常登录。
        """
        state, token, message = exchange_pass_token(
            pass_token, user_id, c_user_id, device_id=self.device_id,
            login_method="qrcode", extra_cookies=self.cookies)
        if state == LOGIN_SUCCESS and token:
            # 扫码不知道账号叫什么，留 userId 供界面标出这是哪个账号
            token.setdefault("qr_user_id", user_id)
        return state, token, message

    def _get_json(self, url: str) -> Optional[Dict[str, Any]]:
        response = self._get(url)
        if response is None:
            return None
        _collect_cookies(response, self.cookies)
        return _parse_json(response.text or "")


class XiaoaiClient:
    """小爱音箱 Mina 云端客户端：设备列表与播放控制。

    只认 userId + serviceToken 两个 cookie，其余走 form 编码的 /remote/ubus。
    401 一律抛 MinaAuthError 让上层决定是静默续期还是重新登录 —— 在这里
    悄悄返回空列表会让「token 过期」和「账号名下没有音箱」长得一模一样。
    """

    def __init__(self, token: Dict[str, Any], audio_id: str = "") -> None:
        self.token = token or {}
        self.audio_id = audio_id or DEFAULT_MUSIC_AUDIO_ID
        self.device_id = str(self.token.get("device_id") or "")
        self.user_agent = USER_AGENT_TEMPLATE % self.device_id

    # ---------------- 只读查询 ----------------

    @property
    def user_id(self) -> str:
        return str(self.token.get("user_id") or "")

    def service_token(self) -> str:
        services = self.token.get("services") or {}
        return str((services.get(MINA_SID) or {}).get("service_token") or "")

    def token_expired(self) -> bool:
        services = self.token.get("services") or {}
        expiry = float((services.get(MINA_SID) or {}).get("expires_at") or 0)
        return bool(expiry and time.time() >= expiry)

    def is_usable(self) -> bool:
        return bool(self.user_id and self.service_token() and not self.token_expired())

    def get_devices(self, master: bool = True) -> List[Dict[str, Any]]:
        """设备列表。

        master=1 只回主控设备（miot 用它 + 设备分组），master=0 回全部（miservice 用它）。
        两种取值在不同账号上各能多看见几台，所以主查询为空时再问一次另一档。
        """
        query = "master=1" if master else "master=0"
        data = self._get_json(f"{MINA_API_BASE_URL}/admin/v2/device_list?{query}")
        devices = self._normalize_devices(data)
        if not devices and master:
            data = self._get_json(f"{MINA_API_BASE_URL}/admin/v2/device_list?master=0")
            devices = self._normalize_devices(data)
        return devices

    @staticmethod
    def _normalize_devices(data: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not data or int(data.get("code") or 0) != 0:
            return []
        rows = data.get("data") or []
        devices: List[Dict[str, Any]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            device_id = str(row.get("deviceID") or "")
            if not device_id:
                continue
            devices.append({
                "device_id": device_id,
                "name": str(row.get("name") or ""),
                "alias": str(row.get("alias") or ""),
                "model": str(row.get("model") or ""),
                "hardware": str(row.get("hardware") or ""),
                "miot_did": str(row.get("miotDID") or ""),
                "presence": str(row.get("presence") or "offline"),
            })
        return devices

    def get_status(self, device_id: str) -> Dict[str, Any]:
        """播放状态：{code, status, volume, position}；读不到时 status=-1。"""
        result = self._ubus(device_id, "player_get_play_status", "mediaplayer", {})
        info = self._parse_info(result)
        return {
            "code": int(result.get("code") or 0) if isinstance(result, dict) else -1,
            "status": int(info.get("status") or 0) if isinstance(info.get("status"), (int, float)) else -1,
            "volume": info.get("volume"),
            "position": info.get("position"),
            "duration": info.get("duration"),
            "raw": info,
        }

    def get_volume(self, device_id: str) -> int:
        status = self.get_status(device_id)
        try:
            return int(status.get("volume"))
        except (TypeError, ValueError):
            return -1

    # ---------------- 播放控制 ----------------

    def play(self, device_id: str, url: str, hardware: str = "",
             play_mode: str = "auto", keep_light: bool = False) -> Dict[str, Any]:
        """推送一个音频 URL。

        返回 {"ok", "api", "error"}：api 说明实际走的接口，出问题时
        「型号没走对接口」和「云端没受理」是两种完全不同的排查方向。
        """
        use_music = (play_mode == "music") or (
            play_mode == "auto" and hardware in MUSIC_API_MODELS)
        if use_music:
            return self._play_music(device_id, url, keep_light)
        message = {"url": url, "type": 1 if keep_light else 2, "media": "app_ios"}
        result = self._ubus(device_id, "player_play_url", "mediaplayer", message)
        return self._device_result(result, "player_play_url")

    def _play_music(self, device_id: str, url: str, keep_light: bool) -> Dict[str, Any]:
        music = {
            "payload": {
                "audio_type": "MUSIC" if keep_light else "",
                "audio_items": [{
                    "item_id": {
                        "audio_id": self.audio_id,
                        "cp": {"album_id": "-1", "episode_index": 0,
                               "id": MUSIC_CP_ID, "name": "xiaowei"},
                    },
                    "stream": {"url": url},
                }],
                "list_params": {"listId": "-1", "loadmore_offset": 0,
                                "origin": "xiaowei", "type": "MUSIC"},
            },
            "play_behavior": "REPLACE_ALL",
        }
        message = {"startaudioid": self.audio_id, "music": json.dumps(music)}
        result = self._ubus(device_id, "player_play_music", "mediaplayer", message)
        return self._device_result(result, "player_play_music")

    def play_operation(self, device_id: str, action: str) -> Dict[str, Any]:
        """play / pause / stop。"""
        if action not in ("play", "pause", "stop"):
            return {"ok": False, "api": "player_play_operation", "error": f"不支持的操作 {action}"}
        result = self._ubus(device_id, "player_play_operation", "mediaplayer",
                            {"action": action, "media": "app_ios"})
        return self._device_result(result, "player_play_operation")

    def interrupt(self, device_id: str) -> Dict[str, Any]:
        """抢在小米自己的回答之前把声音停掉：先 pause 再 stop。

        只发 stop 不够 —— 部分型号忽略裸 stop，仍在放云端 TTS 的尾巴（上游同一条注释）。
        这一步是"用户体验"级的：不中断就会听到"我帮你找找看"和小提琴同时响。
        """
        paused = self.play_operation(device_id, "pause")
        stopped = self.play_operation(device_id, "stop")
        return {"ok": bool(stopped.get("ok")), "pause_ok": bool(paused.get("ok")),
                "api": "player_play_operation", "error": str(stopped.get("error") or "")}

    # ---------------- 语音识别记录 ----------------

    def get_latest_ask(self, device_id: str, hardware: str = "",
                       limit: int = CONVERSATION_LIMIT) -> Optional[List[Dict[str, Any]]]:
        """取最近的对话记录（用户对小爱说的话）。

        返回值区分三种情况，调用方的去重基线依赖这个区分：
        - None  = 这一轮没取到（网络/鉴权/解析失败）。基线**不能**被它推进，
          否则一次网络抖动会让那段时间说的话永久丢失。
        - []    = 取到了，确实没有记录。可以把基线推到服务端给的最新时间。
        - [...] = 记录，按时间升序，元素为 {request_id, timestamp_ms, question, answer}。
        """
        if hardware in MINA_ASK_HARDWARE:
            return self._get_ask_by_ubus(device_id)
        return self._get_ask_by_userprofile(device_id, hardware, limit)

    def _get_ask_by_userprofile(self, device_id: str, hardware: str,
                                limit: int) -> Optional[List[Dict[str, Any]]]:
        hardware = (hardware or "").strip()
        if not hardware:
            return None
        url = CONVERSATION_API % (hardware, int(time.time() * 1000), max(1, int(limit)))
        try:
            response = RequestUtils(headers={"User-Agent": self.user_agent},
                                    cookies={**self._cookies(), "deviceId": device_id},
                                    timeout=HTTP_TIMEOUT).get_res(
                url, allow_redirects=True, raise_exception=False)
        except Exception:
            return None
        if response is None:
            return None
        if response.status_code in (401, 403):
            raise MinaAuthError(f"读取对话记录鉴权失效（HTTP {response.status_code}）")
        if response.status_code != 200:
            return None
        try:
            payload = json.loads(response.text or "")
        except (TypeError, ValueError):
            return None
        raw = payload.get("data")
        if isinstance(raw, dict):
            records = raw.get("records") or []
        elif isinstance(raw, str) and raw:
            try:
                records = (json.loads(raw) or {}).get("records") or []
            except (TypeError, ValueError):
                return None
        else:
            return []
        rows: List[Dict[str, Any]] = []
        for record in records if isinstance(records, list) else []:
            if not isinstance(record, dict):
                continue
            timestamp = _as_int(record.get("time"))
            if not timestamp:
                # 时间戳拿不到就不能参与去重：兜底成 0 会让这条永远小于基线，
                # 被静默吞掉且没有任何日志（上游踩过并专门写了注释）
                continue
            answer = ""
            for item in record.get("answers") or []:
                if isinstance(item, dict) and item.get("type") == "TTS":
                    answer = str(((item.get("tts") or {}).get("text")) or "")
                    break
            rows.append({
                "request_id": "",
                "timestamp_ms": timestamp,
                "question": str(record.get("query") or ""),
                "answer": answer,
            })
        rows.sort(key=lambda row: row["timestamp_ms"])
        return rows

    def _get_ask_by_ubus(self, device_id: str) -> Optional[List[Dict[str, Any]]]:
        """M01 这类不认 userprofile 的型号：走 ubus nlp_result_get。"""
        try:
            result = self._ubus(device_id, "nlp_result_get", "mibrain", {})
        except MinaError:
            raise
        except Exception:
            return None
        if not isinstance(result, dict) or not result.get("data"):
            return None
        data = result.get("data")
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (TypeError, ValueError):
                return None
        if not isinstance(data, dict) or int(data.get("code") or 0) != 0:
            # 设备侧报错是"取不到"，不是"没有对话"
            return None
        info = data.get("info")
        if not info:
            return []
        try:
            parsed = json.loads(info) if isinstance(info, str) else info
        except (TypeError, ValueError):
            return None
        rows: List[Dict[str, Any]] = []
        for item in (parsed or {}).get("result") or []:
            nlp = item.get("nlp") if isinstance(item, dict) else None
            if not nlp:
                continue
            try:
                detail = json.loads(nlp) if isinstance(nlp, str) else nlp
            except (TypeError, ValueError):
                continue
            meta = detail.get("meta") or {}
            timestamp = _as_int(meta.get("timestamp"))
            if not timestamp:
                continue
            answers = ((detail.get("response") or {}).get("answer")) or []
            first = answers[0] if answers and isinstance(answers[0], dict) else {}
            intention = first.get("intention") or {}
            content = first.get("content") or {}
            rows.append({
                "request_id": str(meta.get("request_id") or ""),
                "timestamp_ms": timestamp,
                "question": str(intention.get("query") or ""),
                "answer": str(content.get("to_speak") or ""),
                "domain": str(first.get("domain") or ""),
                "action": str(first.get("action") or ""),
            })
        rows.sort(key=lambda row: row["timestamp_ms"])
        return rows

    def set_volume(self, device_id: str, volume: int) -> Dict[str, Any]:
        value = max(0, min(100, int(volume)))
        result = self._ubus(device_id, "player_set_volume", "mediaplayer",
                            {"volume": value, "media": "app_ios"})
        return self._device_result(result, "player_set_volume")

    def text_to_speech(self, device_id: str, text: str) -> Dict[str, Any]:
        """播报文本：先 mibrain/text_to_speech，失败回退 mediaplayer/player_play_tts。

        设备回执 3012 是「超时但很可能已经播出」，不能当失败重发，
        所以这里返回 ok=None + 明确文案，由调用方决定怎么显示。
        """
        text = (text or "").strip()
        if not text:
            return {"ok": False, "api": "text_to_speech", "error": "播报内容为空"}
        result = self._ubus(device_id, "text_to_speech", "mibrain", {"text": text})
        accepted = self._tts_acceptance(result)
        if accepted is True:
            return {"ok": True, "api": "text_to_speech", "error": ""}
        if accepted is None:
            return {"ok": None, "api": "text_to_speech", "error": "音箱回执超时，可能已经播报"}
        fallback = self._ubus(device_id, "player_play_tts", "mediaplayer", {"text": text})
        accepted = self._tts_acceptance(fallback)
        if accepted is True:
            return {"ok": True, "api": "player_play_tts", "error": ""}
        if accepted is None:
            return {"ok": None, "api": "player_play_tts", "error": "音箱回执超时，可能已经播报"}
        return {"ok": False, "api": "player_play_tts", "error": "两个播报接口都被设备拒绝"}

    @staticmethod
    def _tts_acceptance(result: Dict[str, Any]) -> Optional[bool]:
        if not isinstance(result, dict):
            return False
        code = int(result.get("code") or 0)
        device_code = XiaoaiClient._device_code(result)
        if code in (0, 101) and device_code == 3012:
            return None
        return code == 0 and (device_code is None or device_code == 0)

    # ---------------- 底层请求 ----------------

    def _headers(self) -> Dict[str, str]:
        return {"User-Agent": self.user_agent}

    def _cookies(self) -> Dict[str, str]:
        return {
            "userId": self.user_id,
            "serviceToken": self.service_token(),
            "channel": "MI_APP_STORE",
        }

    def _get_json(self, url: str) -> Optional[Dict[str, Any]]:
        response = RequestUtils(headers=self._headers(), cookies=self._cookies(),
                                timeout=HTTP_TIMEOUT).get_res(
            url, allow_redirects=False, raise_exception=False)
        return self._handle(response, url)

    def _post_form(self, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        response = RequestUtils(
            headers={**self._headers(),
                     "Content-Type": "application/x-www-form-urlencoded"},
            cookies=self._cookies(), timeout=HTTP_TIMEOUT).post_res(
            url, data=_form_body(params), allow_redirects=False, raise_exception=False)
        return self._handle(response, url)

    def _handle(self, response: Any, url: str) -> Optional[Dict[str, Any]]:
        if response is None:
            raise MinaError(f"请求小米云端失败: {url}")
        if response.status_code in (401, 403):
            raise MinaAuthError(
                f"小米云端鉴权失效（HTTP {response.status_code}）")
        if response.status_code >= 400:
            raise MinaError(f"小米云端返回 HTTP {response.status_code}: {url}")
        data = _parse_json(response.text or "")
        if data is None:
            raise MinaError(f"小米云端响应不是可识别的 JSON: {url}")
        # 有些接口把失败写在 body 的 code 里而不是 HTTP 状态
        code = int(data.get("code") or 0)
        message = str(data.get("message") or "")
        if code in (401, 403) or (code != 0 and re.search(r"auth", message, re.I)):
            raise MinaAuthError(f"小米云端业务鉴权失败: {message}")
        return data

    def _ubus(self, device_id: str, method: str, path: str,
              message: Dict[str, Any]) -> Dict[str, Any]:
        """一次 ubus 调用；非 0 code 原样返回（_device_result 负责判定与记录）。"""
        data = self._post_form(f"{MINA_API_BASE_URL}/remote/ubus", {
            "deviceId": device_id,
            "method": method,
            "path": path,
            "message": json.dumps(message, ensure_ascii=False),
            "requestId": _request_id(),
        })
        if data is None:  # pragma: no cover - _handle 已经抛错
            raise MinaError(f"ubus 无响应: {method}")
        return data

    def _device_result(self, result: Dict[str, Any], action: str) -> Dict[str, Any]:
        """云端受理 != 音箱执行：必须再看 data.code / device_data.code。"""
        code = int(result.get("code") or 0)
        device_code = self._device_code(result)
        if code != 0:
            return {"ok": False, "api": action,
                    "error": f"{action} 被云端拒绝 code={code} {result.get('message') or ''}".strip()}
        if device_code not in (None, 0):
            return {"ok": False, "api": action,
                    "error": f"{action} 云端受理但设备返回 code={device_code}"}
        return {"ok": True, "api": action, "error": ""}

    @staticmethod
    def _device_code(result: Dict[str, Any]) -> Optional[int]:
        data = result.get("data")
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (TypeError, ValueError):
                data = None
        if isinstance(data, dict):
            inner = data.get("device_data", data)
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except (TypeError, ValueError):
                    return None
            if isinstance(inner, dict) and "code" in inner:
                try:
                    return int(inner.get("code") or 0)
                except (TypeError, ValueError):
                    return None
        return None

    @staticmethod
    def _parse_info(result: Dict[str, Any]) -> Dict[str, Any]:
        data = result.get("data") if isinstance(result, dict) else None
        info = data.get("info") if isinstance(data, dict) else None
        if isinstance(info, str):
            try:
                info = json.loads(info)
            except (TypeError, ValueError):
                info = {}
        return info if isinstance(info, dict) else {}

    def validate(self) -> bool:
        """票据是否真的能用：device_list 返回 code=0 即有效（没有音箱也是 0）。

        不能复用 get_devices() 的结果判真伪 —— 它把失败兜底成空列表，
        空列表会让失效票据看起来像「账号名下没设备」。
        """
        try:
            data = self._get_json(f"{MINA_API_BASE_URL}/admin/v2/device_list?master=1")
        except MinaError:
            return False
        return bool(data) and int(data.get("code") or 0) == 0


def exchange_pass_token(pass_token: str, user_id: str, c_user_id: str = "",
                        device_id: str = "", login_method: str = "manual",
                        extra_cookies: Optional[Dict[str, str]] = None
                        ) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """拿一个 passToken 去换 micoapi 的 serviceToken，返回 (state, token, message)。

    扫码确认与手工填 cookie 都收敛到这一步：passToken 是长期票据，能换到
    serviceToken 就说明这串凭据真的有效 —— 比直接相信浏览器里那张多半已经躺了
    十几个小时的 serviceToken 可靠得多，也顺手把票据续上了。
    """
    if "%" in pass_token:
        # 从原始请求头里复制的值是百分号编码的；passToken 本身是 base64，不含 %，
        # 所以看见 % 就该解开 —— 小米对坏票据只回 401，不会说「你粘错了」
        pass_token = unquote(pass_token)
    auth = XiaoaiAuth(username="", password="", device_id=device_id,
                      login_method=login_method)
    for name, value in (extra_cookies or {}).items():
        # 旧的 serviceToken 不带：那是别的会话/别的服务的，混进去只会拿错票据
        if value and name != "serviceToken":
            auth.cookies[name] = value
    auth.cookies["passToken"] = pass_token
    if user_id:
        auth.cookies["userId"] = user_id
    if c_user_id:
        auth.cookies["cUserId"] = c_user_id
    auth._user_id = user_id or ""
    return auth._service_login()
