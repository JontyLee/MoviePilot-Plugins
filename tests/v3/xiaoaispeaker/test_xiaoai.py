"""小爱音箱插件单测：小米云端协议请求形状、库内路径边界与插件侧行为。

覆盖的是插件里「猜错就出事」的那几块，不打真实网络：
- mina：签名与登录请求体、ubus 请求形状、型号决定走哪个播放接口、
  设备列表 master 回落、TTS 回执 3012 不能当失败、401 要抛鉴权错。
- media：/stream 的越库判定（软链也算越库）、签名与有效期、Range 分片。
- 插件：配置夹逼与全量回写、票据过期与静默续期、设备取舍优先级、队列连播的
  「连续两次停止 + 宽限期」判据、命令参数从 arg_str 取、状态与配置回显不带密码。

运行（需要 pydantic；宿主虚拟环境或本地 venv 皆可）：
    python3 tests/v3/xiaoaispeaker/test_xiaoai.py
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import re
import sys
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "tests" / "v3" / "xiaoaispeaker"))

import _stub_env  # noqa: E402

# 所有替身实例的状态目录都挂在这个目录下：进程退出时整棵树由 TemporaryDirectory 收走
_STATE_ROOT = tempfile.TemporaryDirectory(prefix="xiaoaispeaker-state-")


class _FakePluginBase:
    """替身基类：配置写入与消息都记下来供断言，状态落到每实例独立的临时目录。

    状态目录必须按实例隔离 —— 插件的 save_data/get_data 是真的写文件，共用一个目录
    会让上一个用例留下的队列、分组、票据漏进下一个用例，断言就变成在演上一段的状态。
    """

    def __init__(self, *args, **kwargs) -> None:
        self._store: Dict[str, Any] = {}
        self._configs: List[Dict[str, Any]] = []
        self.messages: List[Dict[str, Any]] = []
        self._data_dir = Path(tempfile.mkdtemp(dir=_STATE_ROOT.name))

    def save_data(self, key, value, plugin_id=None) -> None:
        self._store[key] = value

    def get_data(self, key=None, plugin_id=None):
        if key is None:
            return dict(self._store)
        return self._store.get(key)

    def del_data(self, key=None, plugin_id=None):
        if key is None:
            self._store.clear()
        else:
            self._store.pop(key, None)

    def update_config(self, config, plugin_id=None) -> bool:
        self._configs.append(dict(config))
        return True

    def get_config(self, plugin_id=None):
        return {}

    def get_data_path(self, plugin_id=None) -> Path:
        return self._data_dir

    def post_message(self, *args, **kwargs) -> None:
        self.messages.append(kwargs)


_SETTINGS = _stub_env._Settings()
_stub_env.install(_FakePluginBase, settings_obj=_SETTINGS)

sys.path.insert(0, str(_ROOT / "plugins.v3"))
import xiaoaispeaker as x  # noqa: E402
from xiaoaispeaker import lyrics, media, mina  # noqa: E402


class _Resp:
    """requests.Response 的最小替身：status_code / text / content / headers / cookies。"""

    def __init__(self, status: int = 200, text: str = "", payload: Any = None,
                 prefixed: bool = False, headers: Optional[Dict[str, str]] = None,
                 cookies: Optional[Dict[str, str]] = None, content: bytes = b"") -> None:
        self.status_code = status
        body = text
        if not body and payload is not None:
            body = json.dumps(payload, ensure_ascii=False)
            if prefixed:
                body = mina.JSON_PREFIX + body
        self.text = body
        self.content = content
        self.headers = dict(headers or {})
        self.cookies = dict(cookies or {})


def _network(handler):
    """按 handler(method, url, body) -> _Resp|None 造一个 RequestUtils 替身类。"""

    class _Ru:
        def __init__(self, *args, **kwargs) -> None:
            self.init_kwargs = kwargs

        def get_res(self, url=None, params=None, **kwargs):
            return handler("GET", url, params)

        def post_res(self, url=None, data=None, **kwargs):
            return handler("POST", url, data)

    return _Ru


class _NetworkPatch:
    """把 mina 模块用的 RequestUtils 换成替身，退出即还原。"""

    def __init__(self, handler) -> None:
        self._orig = mina.RequestUtils
        self._handler = handler

    def __enter__(self):
        mina.RequestUtils = _network(self._handler)
        return self

    def __exit__(self, *exc) -> bool:
        mina.RequestUtils = self._orig
        return False


def _token(service_token: str = "ST", expired: bool = False) -> Dict[str, Any]:
    """构造一个可用的登录票据（过期时 expires_at 落在过去）。"""
    return {
        "username": "me@example.com",
        "user_id": "123456",
        "device_id": "deviceIdForTest",
        "services": {
            mina.MINA_SID: {
                "service_token": service_token,
                "ssecurity": "sss",
                "expires_at": (time.time() - 10) if expired else (time.time() + 3600),
            }
        },
        "pass_token": "PT",
        "saved_at": "2026-10-21 00:00:00",
    }


class _FakeClient:
    """XiaoaiClient 的替身：只记调用，返回插件要看的那几种形状。"""

    def __init__(self, devices=None, status=None, play_ok=True, tts_ok=True,
                 volume=30, asks=None) -> None:
        self._devices = devices if devices is not None else []
        self._status = status or {"status": 1, "volume": 30, "position": 1000, "duration": 60000}
        self._play_ok = play_ok
        self._tts_ok = tts_ok
        self._volume = volume
        self._asks = asks
        self.calls: List[tuple] = []
        self.expired = False

    def token_expired(self) -> bool:
        return self.expired

    def is_usable(self) -> bool:
        return True

    def get_devices(self, master: bool = True):
        self.calls.append(("get_devices", master))
        return list(self._devices)

    def get_status(self, device_id: str):
        self.calls.append(("get_status", device_id))
        return dict(self._status)

    def play(self, device_id, url, hardware="", play_mode="auto", keep_light=False):
        self.calls.append(("play", device_id, url, hardware, play_mode, keep_light))
        return {"ok": self._play_ok, "api": "player_play_url",
                "error": "" if self._play_ok else "被设备拒绝"}

    def play_operation(self, device_id, action):
        self.calls.append(("play_operation", device_id, action))
        return {"ok": True, "api": "player_play_operation", "error": ""}

    def set_volume(self, device_id, volume):
        self.calls.append(("set_volume", device_id, volume))
        return {"ok": True, "api": "player_set_volume", "error": ""}

    def text_to_speech(self, device_id, text):
        self.calls.append(("tts", device_id, text))
        return {"ok": self._tts_ok, "api": "text_to_speech",
                "error": "" if self._tts_ok else "设备拒绝"}

    def get_volume(self, device_id):
        self.calls.append(("get_volume", device_id))
        return self._volume

    def interrupt(self, device_id):
        self.calls.append(("interrupt", device_id))
        return {"ok": True, "pause_ok": True, "api": "player_play_operation", "error": ""}

    def get_latest_ask(self, device_id, hardware="", limit=5):
        self.calls.append(("get_latest_ask", device_id, hardware, limit))
        if self._asks is None:
            return None
        return list(self._asks)

    def validate(self) -> bool:
        self.calls.append(("validate",))
        return True


def _plugin(config: Optional[Dict[str, Any]] = None,
            library: Optional[Path] = None) -> x.XiaoaiSpeaker:
    """造一个已初始化、带库目录与票据的插件实例。"""
    plugin = x.XiaoaiSpeaker()
    data = dict(media_defaults)
    if library is not None:
        data["music_dirs"] = str(library)
    if config:
        data.update(config)
    plugin.init_plugin(data)
    plugin.save_data("token", _token())
    return plugin


media_defaults: Dict[str, Any] = {
    "enabled": True,
    "username": "me@example.com",
    "password": "secret",
    "base_url": "http://192.168.1.10:3000",
    "play_mode": "auto",
    "stream_ttl": 3600,
    "default_volume": 0,
    "auto_next": True,
}


def _device(device_id: str = "D1", name: str = "客厅", hardware: str = "L05B",
            presence: str = "online") -> Dict[str, Any]:
    return {"device_id": device_id, "name": name, "alias": name, "model": "xiaomi.wifispeaker." + hardware,
            "hardware": hardware, "presence": presence, "miot_did": ""}


# ---------------- mina：协议请求形状 ----------------

class MinaProtocolTest(unittest.TestCase):
    def test_client_sign_vector(self) -> None:
        self.assertEqual(
            mina._client_sign("1701", "abc123"),
            base64.b64encode(hashlib.sha1(b"nonce=1701&abc123").digest()).decode("ascii"),
        )

    def test_password_hash_is_upper_md5(self) -> None:
        self.assertEqual(mina._md5_upper("pass"), hashlib.md5(b"pass").hexdigest().upper())

    def test_parse_json_strips_prefix_and_rejects_non_object(self) -> None:
        self.assertEqual(mina._parse_json(mina.JSON_PREFIX + '{"code": 0}'), {"code": 0})
        self.assertIsNone(mina._parse_json(mina.JSON_PREFIX + "[1,2]"))
        self.assertIsNone(mina._parse_json("not json"))

    def test_form_body_quotes_values(self) -> None:
        body = mina._form_body({"a": "1 2", "b": "x&y"})
        self.assertEqual(body, "a=1%202&b=x%26y")

    def test_request_id_shape(self) -> None:
        value = mina._request_id()
        self.assertTrue(value.startswith("app_ios_"))
        self.assertEqual(len(value), len("app_ios_") + 30)

    def test_login_sends_md5_hash_and_captcha_on_retry(self) -> None:
        seen: List[str] = []

        def handler(method, url, body):
            if "serviceLogin?" in url:
                return _Resp(payload={"code": 0, "sid": "micoapi", "_sign": "SIGN",
                                      "qs": "http://qs", "callback": "http://cb"})
            if "serviceLoginAuth2" in url:
                seen.append(body or "")
                if "captCode" in (body or ""):
                    return _Resp(payload={"location": "/pass/rsn1", "ssecurity": "SS",
                                          "userId": "7", "nonce": "1"},
                                 prefixed=True)
                return _Resp(payload={"captchaUrl": "/captcha/get?x=1"}, prefixed=True)
            if "/captcha/get" in url:
                return _Resp(status=200, content=b"FAKEPNG")
            return _Resp(status=302, headers={"Location": "https://api2.mina.mi.com/redirect",
                                              "Set-Cookie": "serviceToken=NEWTOKEN; Path=/"})

        with _NetworkPatch(handler):
            auth = mina.XiaoaiAuth("me@example.com", "pass")
            state, payload, _ = auth.login()
            self.assertEqual(state, mina.LOGIN_NEED_CAPTCHA)
            self.assertTrue(payload["captcha_image"].startswith("data:image/png;base64,"))
            state, token, _ = auth.submit_captcha("1a2b")
        self.assertEqual(state, mina.LOGIN_SUCCESS)
        self.assertEqual(token["services"][mina.MINA_SID]["service_token"], "NEWTOKEN")
        self.assertEqual(token["user_id"], "7")
        # 第一次不带验证码，第二次必须带上，并且密码始终是 MD5 大写
        self.assertNotIn("captCode", seen[0])
        self.assertIn("captCode=1a2b", seen[1])
        self.assertIn(f"hash={mina._md5_upper('pass')}", seen[0])

    def test_silent_refresh_needs_no_password(self) -> None:
        def handler(method, url, body):
            if "serviceLogin?" in url:
                # 静默续期靠 cookie 里的 passToken，这一步不给账密也要给出跳转地址
                return _Resp(payload={"code": 0, "sid": "micoapi", "_sign": "SIGN",
                                      "qs": "http://qs", "callback": "http://cb",
                                      "location": "/pass/rs?sid=micoapi",
                                      "ssecurity": "SS2", "userId": "7", "nonce": "9"},
                             prefixed=True)
            return _Resp(status=302, headers={"Location": "https://api2.mina.mi.com/x",
                                              "Set-Cookie": "serviceToken=REFRESHED; Path=/"})

        with _NetworkPatch(handler):
            auth = mina.XiaoaiAuth.from_token(_token())
            self.assertEqual(auth.password, "")
            self.assertEqual(auth.cookies["passToken"], "PT")
            state, token, _ = auth.refresh()
        self.assertEqual(state, mina.LOGIN_SUCCESS)
        self.assertEqual(token["services"][mina.MINA_SID]["service_token"], "REFRESHED")

    def test_refresh_without_pass_token_fails_cleanly(self) -> None:
        auth = mina.XiaoaiAuth.from_token({"user_id": "1", "device_id": "d"})
        state, token, message = auth.refresh()
        self.assertEqual(state, mina.LOGIN_FAILED)
        self.assertIsNone(token)
        self.assertIn("passToken", message)

    def test_ubus_body_shape(self) -> None:
        captured: List[str] = []

        def handler(method, url, body):
            if "/remote/ubus" in url:
                captured.append(body or "")
                return _Resp(payload={"code": 0, "data": '{"code":0}'})
            return _Resp(payload={"code": 0, "data": []})

        with _NetworkPatch(handler):
            client = mina.XiaoaiClient(_token())
            client.play("D1", "http://x/a.mp3", hardware="L05B", play_mode="url")
        body = captured[0]
        self.assertIn("deviceId=D1", body)
        self.assertIn("method=player_play_url", body)
        self.assertIn("path=mediaplayer", body)
        self.assertIn("requestId=app_ios_", body)
        # message 字段是 _form_body 编码过的 JSON，取出来再解
        payload = json.loads(_form_field(body, "message"))
        self.assertEqual(payload["url"], "http://x/a.mp3")
        self.assertEqual(payload["media"], "app_ios")
        self.assertEqual(payload["type"], 2)

    def test_keep_light_switches_stream_type(self) -> None:
        bodies: List[str] = []

        def handler(method, url, body):
            bodies.append(body or "")
            return _Resp(payload={"code": 0, "data": '{"code":0}'})

        with _NetworkPatch(handler):
            client = mina.XiaoaiClient(_token())
            client.play("D1", "http://x/a.mp3", play_mode="url", keep_light=True)
        self.assertIn('"type": 1', json.dumps(json.loads(_form_field(bodies[0], "message"))))

    def test_whitelisted_model_uses_play_music(self) -> None:
        methods: List[str] = []

        def handler(method, url, body):
            methods.append(_form_field(body or "", "method"))
            return _Resp(payload={"code": 0, "data": '{"code":0}'})

        with _NetworkPatch(handler):
            client = mina.XiaoaiClient(_token())
            self.assertEqual(client.play("D1", "http://x/a.mp3", hardware="LX05",
                                         play_mode="auto")["api"], "player_play_music")
            self.assertEqual(client.play("D1", "http://x/a.mp3", hardware="LX99",
                                         play_mode="auto")["api"], "player_play_url")
            # 强制 url 模式时白名单型号也不走 play_music
            self.assertEqual(client.play("D1", "http://x/a.mp3", hardware="LX05",
                                         play_mode="url")["api"], "player_play_url")
        self.assertEqual(methods, ["player_play_music", "player_play_url", "player_play_url"])

    def test_device_list_falls_back_to_master0(self) -> None:
        urls: List[str] = []

        def handler(method, url, body):
            urls.append(url)
            if "master=1" in url:
                return _Resp(payload={"code": 0, "data": []})
            return _Resp(payload={"code": 0, "data": [{"deviceID": "D2", "name": "卧室",
                                                       "hardware": "L05B", "presence": "online"}]})

        with _NetworkPatch(handler):
            devices = mina.XiaoaiClient(_token()).get_devices()
        self.assertEqual([d["device_id"] for d in devices], ["D2"])
        self.assertTrue(any("master=1" in u for u in urls) and any("master=0" in u for u in urls))

    def test_http_401_raises_auth_error(self) -> None:
        with _NetworkPatch(lambda method, url, body: _Resp(status=401)):
            with self.assertRaises(mina.MinaAuthError):
                mina.XiaoaiClient(_token()).get_devices()

    def test_business_auth_code_raises_auth_error(self) -> None:
        with _NetworkPatch(lambda m, u, b: _Resp(payload={"code": 401, "message": "auth failure"})):
            with self.assertRaises(mina.MinaAuthError):
                mina.XiaoaiClient(_token()).get_devices()

    def test_tts_timeout_is_not_a_failure(self) -> None:
        def handler(method, url, body):
            return _Resp(payload={"code": 0, "data": json.dumps({"code": 3012})})

        with _NetworkPatch(handler):
            result = mina.XiaoaiClient(_token()).text_to_speech("D1", "你好")
        self.assertIsNone(result["ok"])
        self.assertIn("可能已经播报", result["error"])

    def test_tts_falls_back_to_second_api(self) -> None:
        calls: List[str] = []

        def handler(method, url, body):
            name = _form_field(body or "", "method")
            calls.append(name)
            if name == "text_to_speech":
                return _Resp(payload={"code": 0, "data": json.dumps({"code": 500})})
            return _Resp(payload={"code": 0, "data": json.dumps({"code": 0})})

        with _NetworkPatch(handler):
            result = mina.XiaoaiClient(_token()).text_to_speech("D1", "你好")
        self.assertTrue(result["ok"])
        self.assertEqual(calls, ["text_to_speech", "player_play_tts"])

    def test_device_code_failure_is_reported(self) -> None:
        with _NetworkPatch(lambda m, u, b: _Resp(payload={"code": 0, "data": {"code": 8}})):
            result = mina.XiaoaiClient(_token()).play_operation("D1", "play")
        self.assertFalse(result["ok"])
        self.assertIn("code=8", result["error"])


def _form_field(body: str, key: str) -> str:
    """从 _form_body 编出来的串里取一个字段（值做一次 unquote）。"""
    from urllib.parse import unquote
    for part in (body or "").split("&"):
        if "=" in part:
            name, _, value = part.partition("=")
            if name == key:
                return unquote(value)
    return ""


# ---------------- media：路径边界与直链 ----------------

class MediaBoundaryTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        # macOS 的 /var 是 /private/var 的软链，resolve_playable 给的是解析后的真身；
        # 夹具从一开始就用解析过的路径，断言才是「同一个字符串」比同一个字符串。
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        self.outside = base / "elsewhere"
        self.root.mkdir()
        self.outside.mkdir()
        (self.root / "01 - 晴天.mp3").write_bytes(b"A" * 1000)
        (self.root / "x.txt").write_bytes(b"T" * 4)
        (self.outside / "secret.flac").write_bytes(b"B" * 10)
        self.exts = media.parse_exts("")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_resolves_inside_library(self) -> None:
        real, reason = media.resolve_playable(str(self.root / "01 - 晴天.mp3"),
                                             [self.root], self.exts)
        self.assertEqual(real.name, "01 - 晴天.mp3")
        self.assertEqual(reason, "")

    def test_rejects_outside_relative_missing_and_non_audio(self) -> None:
        cases = {
            str(self.outside / "secret.flac"): "out_of_library",
            "01 - 晴天.mp3": "not_absolute",
            str(self.root / "nope.mp3"): "not_found",
            str(self.root / "x.txt"): "not_audio",
            "": "empty_path",
        }
        for text, reason in cases.items():
            real, got = media.resolve_playable(text, [self.root], self.exts)
            self.assertIsNone(real, text)
            self.assertEqual(got, reason, text)

    def test_rejects_symlink_pointing_out_of_library(self) -> None:
        link = self.root / "link.flac"
        try:
            link.symlink_to(self.outside / "secret.flac")
        except OSError:  # pragma: no cover - 平台不支持软链
            self.skipTest("文件系统不支持软链接")
        real, reason = media.resolve_playable(str(link), [self.root], self.exts)
        self.assertIsNone(real)
        self.assertEqual(reason, "out_of_library")

    def test_empty_roots_is_its_own_reason(self) -> None:
        real, reason = media.resolve_playable(str(self.root / "01 - 晴天.mp3"), [], self.exts)
        self.assertIsNone(real)
        self.assertEqual(reason, "no_roots")

    def test_signature_roundtrip_expiry_and_tamper(self) -> None:
        path = str(self.root / "01 - 晴天.mp3")
        expires = int(time.time()) + 600
        signature = media.sign_path("k" * 16, path, expires)
        self.assertTrue(media.verify_sign("k" * 16, path, expires, signature))
        self.assertFalse(media.verify_sign("k" * 16, path + "x", expires, signature))
        self.assertFalse(media.verify_sign("k" * 16, path, int(time.time()) - 1, signature))
        self.assertFalse(media.verify_sign("o" * 16, path, expires, signature))
        # 签名只取前 32 位，多余的尾巴不该放行
        self.assertFalse(media.verify_sign("k" * 16, path, expires, signature[:-1] + "z"))

    def test_stream_url_is_one_ascii_param(self) -> None:
        """整条直链只许有一个查询参数，且值里不含 & % + 空格。

        小米云端会把 URL 里的 & 换成空格（实测音箱发回来的是
        `path=…flac%20exp=…%20sig=…`），多参数链接到音箱手里必然散架。
        """
        url = media.stream_url("http://mp:3000", "XiaoaiSpeaker", "sec",
                               self.root / "01 - 晴天.mp3", 60)
        self.assertIn("http://mp:3000/api/v1/plugin/XiaoaiSpeaker/stream?k=", url)
        query = url.split("?", 1)[1]
        self.assertEqual(query.count("="), 1)
        self.assertNotIn("&", url)
        self.assertFalse(any(char in url for char in "%+ "))
        self.assertTrue(url.isascii())
        # 解开来还是原来那条路径，签名与有效期都在
        path, expires, signature = media.parse_query(query)
        self.assertEqual(path, str(self.root / "01 - 晴天.mp3"))
        self.assertGreater(expires, int(time.time()))
        self.assertEqual(len(signature), 32)

    def test_parse_range_cases(self) -> None:
        self.assertEqual(media.parse_range(None, 1000), (0, 999))
        self.assertEqual(media.parse_range("bytes=0-99", 1000), (0, 99))
        self.assertEqual(media.parse_range("bytes=200-", 1000), (200, 999))
        self.assertEqual(media.parse_range("bytes=-50", 1000), (950, 999))
        self.assertEqual(media.parse_range("bytes=1500-", 1000), (0, 999))
        self.assertEqual(media.parse_range("bytes=900-10", 1000), (0, 999))
        self.assertEqual(media.parse_range("items=0-1", 1000), (0, 999))
        self.assertEqual(media.parse_range("bytes=0-9999", 1000), (0, 999))

    def test_iter_file_returns_exact_slice(self) -> None:
        target = self.root / "01 - 晴天.mp3"
        chunks = list(media.iter_file(target, 10, 19))
        self.assertEqual(b"".join(chunks), b"A" * 10)

    def test_browse_stays_inside_roots(self) -> None:
        inside = media.browse(self.root, [self.root], self.exts)
        self.assertEqual(inside["reason"], "")
        self.assertEqual([f["name"] for f in inside["files"]], ["01 - 晴天.mp3"])
        self.assertEqual(inside["files"][0]["title"], "晴天")
        outside = media.browse(self.outside, [self.root], self.exts)
        self.assertEqual(outside["reason"], "out_of_library")
        self.assertEqual(outside["files"], [])

    def test_search_limits_and_skips_hidden(self) -> None:
        (self.root / ".hidden").mkdir()
        (self.root / ".hidden" / "晴天 hidden.mp3").write_bytes(b"C" * 5)
        hits = media.search("晴天", [self.root], self.exts)
        self.assertEqual([h["path"] for h in hits], [str(self.root / "01 - 晴天.mp3")])
        self.assertEqual(media.search("", [self.root], self.exts), [])
        self.assertEqual(media.search("查无此曲", [self.root], self.exts), [])

    def test_parse_dirs_and_exts(self) -> None:
        raw = f"{self.root}\n{self.outside}\r\n{self.root},,\" \""
        self.assertEqual([str(p) for p in media.parse_dirs(raw)],
                         [str(self.root), str(self.outside)])
        self.assertEqual(media.parse_exts(""), media.DEFAULT_AUDIO_EXTS)
        self.assertEqual(media.parse_exts("MP3, flac ,,../../etc"), (".mp3", ".flac"))

    def test_normalize_base_url(self) -> None:
        self.assertEqual(media.normalize_base_url("192.168.1.5:3000/"), "http://192.168.1.5:3000")
        self.assertEqual(media.normalize_base_url("https://mp.example.com/nas/"),
                         "https://mp.example.com/nas")
        self.assertEqual(media.normalize_base_url("not a url at all"), "")
        self.assertEqual(media.normalize_base_url("192.168.1.5 3000"), "")
        self.assertEqual(media.normalize_base_url("http://a:3000, http://b:3000"),
                         "http://a:3000")
        self.assertEqual(media.normalize_base_url(""), "")
        self.assertEqual(media.normalize_base_url(None), "")

    def test_strip_index_and_status_text(self) -> None:
        self.assertEqual(media.strip_index("03.轨迹 - 周杰伦.flac"), "轨迹 - 周杰伦")
        self.assertEqual(media.strip_index("晴天.mp3"), "晴天")
        self.assertEqual(media.status_text(0), "已停止")
        self.assertEqual(media.status_text(1), "播放中")
        self.assertEqual(media.status_text(None), "未知")

    def test_content_disposition_survives_latin1_headers(self) -> None:
        """中文文件名进 HTTP 头必须转义，否则整个取流请求变成 500。"""
        plain = media.content_disposition("01 - Sunny.mp3")
        self.assertIn('filename="01 - Sunny.mp3"', plain)
        self.assertIn("filename*=UTF-8''01%20-%20Sunny.mp3", plain)

        chinese = media.content_disposition("03 - 晴天.flac")
        # 真框架按 latin-1 编码头：这一句就是线上那个 500 的形状
        chinese.encode("latin-1")
        self.assertIn('filename="03 -.flac"', chinese)
        self.assertIn("filename*=UTF-8''03%20-%20%E6%99%B4%E5%A4%A9.flac", chinese)
        # 整个文件名都不是 ASCII 时兜底成 audio.flac，不能留个只有扩展名的空名
        self.assertIn('filename="audio.flac"', media.content_disposition("晴天.flac"))

    def test_parse_query_accepts_mapping_and_string(self) -> None:
        path = "/media/music/01 - 晴天.mp3"
        token = media.pack_link("sec", path, 123)
        self.assertEqual(media.parse_query({"k": token}),
                         (path, 123, media.sign_path("sec", path, 123)))
        self.assertEqual(media.parse_query(f"k={token}"),
                         (path, 123, media.sign_path("sec", path, 123)))
        # 音箱或中间层把链接弄歪时，宁可回「没有链接」也不要猜一个路径
        for broken in ("", "abc", "1.2", "1.2.3.4", "123.deadbeef.####"):
            self.assertEqual(media.parse_query(f"k={broken}"), ("", 0, ""))
        self.assertEqual(media.parse_query({"exp": "x"}), ("", 0, ""))
        self.assertEqual(media.parse_query(None), ("", 0, ""))


# ---------------- 插件：配置、设备、播放、队列、命令 ----------------

class PluginConfigTest(unittest.TestCase):
    def test_init_clamps_config(self) -> None:
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "play_mode": "banana",
                            "stream_ttl": "1", "default_volume": 999})
        self.assertEqual(plugin._play_mode, "auto")
        self.assertEqual(plugin._stream_ttl, 300)
        self.assertEqual(plugin._default_volume, 100)

    def test_init_keeps_runtime_when_resaved(self) -> None:
        plugin = _plugin()
        plugin.__dict__["_auth"] = "正在进行的登录会话"
        plugin.init_plugin(media_defaults)
        self.assertEqual(plugin.__dict__["_auth"], "正在进行的登录会话")

    def test_changing_username_drops_login_session(self) -> None:
        plugin = _plugin()
        plugin.__dict__["_auth"] = "旧账号的会话"
        plugin.init_plugin({**media_defaults, "username": "other@example.com"})
        self.assertIsNone(plugin.__dict__["_auth"])

    def test_base_url_falls_back_to_app_domain(self) -> None:
        plugin = _plugin(config={"base_url": ""})
        _SETTINGS.APP_DOMAIN = "http://10.0.0.5:3000/"
        try:
            base, source = plugin._effective_base_url()
            status = plugin._status()
        finally:
            _SETTINGS.APP_DOMAIN = ""
        self.assertEqual(base, "http://10.0.0.5:3000")
        self.assertEqual(source, "app_domain")
        self.assertEqual(status.config.base_url_effective, "http://10.0.0.5:3000")
        self.assertEqual(status.config.base_url_source, "app_domain")
        # 有回落地址就不要再说「请填写外部访问地址」
        self.assertNotIn("外部访问地址", " ".join(status.needs))
        none = _plugin(config={"base_url": ""})
        self.assertEqual(none._effective_base_url(), ("", ""))

    def test_config_api_writes_full_snapshot_and_keeps_password(self) -> None:
        plugin = _plugin()
        payload = x.schemas.ConfigIn(music_dirs="/tmp/other", stream_ttl=999999999)
        status = plugin.api_config(payload)
        saved = plugin._configs[-1]
        self.assertEqual(saved["music_dirs"], "/tmp/other")
        self.assertEqual(saved["stream_ttl"], 259200)
        # 没传的字段必须原样保留：update_config 是整体替换
        self.assertEqual(saved["password"], "secret")
        self.assertEqual(saved["username"], "me@example.com")
        self.assertEqual(set(saved.keys()), set(x._DEFAULT_CONFIG.keys()))
        self.assertEqual(status.config.password_set, True)

    def test_status_never_leaks_password_or_token(self) -> None:
        plugin = _plugin()
        plugin.save_data("token", {
            **_token(),
            "pass_token": "PASSSECRET9",
            "c_user_id": "CUSECRET9",
            "services": {mina.MINA_SID: {"service_token": "STSECRET9",
                                         "ssecurity": "SSECURITY9",
                                         "expires_at": time.time() + 3600}},
        })
        dumped = json.dumps(plugin._status().model_dump(), ensure_ascii=False)
        for secret in ("secret", "me@example.com", "PASSSECRET9", "CUSECRET9",
                       "STSECRET9", "SSECURITY9"):
            self.assertNotIn(secret, dumped)
        for key in ("service_token", "pass_token", "ssecurity", "password"):
            self.assertNotIn(f'"{key}"', dumped)

    def test_account_is_echoed_masked(self) -> None:
        """账号只回掩码：侧栏每 5 秒轮一次状态，明文等于把手机号挂在页面上。"""
        plugin = _plugin()
        cfg = plugin._status().config
        self.assertEqual(cfg.username, "me***@example.com")
        self.assertTrue(cfg.username_set)
        self.assertEqual(plugin._mask_account("13800138000"), "13****00")
        self.assertEqual(plugin._mask_account("123456789"), "12****89")
        self.assertEqual(plugin._mask_account(""), "")

    def test_masked_account_round_trip_does_not_overwrite_the_real_one(self) -> None:
        plugin = _plugin()
        plugin.api_config(x.schemas.ConfigIn(username="me***@example.com"))
        self.assertEqual(plugin._configs[-1]["username"], "me@example.com")
        plugin.api_config(x.schemas.ConfigIn(username="other@example.com"))
        self.assertEqual(plugin._configs[-1]["username"], "other@example.com")

    def test_login_flow_runs_through_the_login_lock(self) -> None:
        """整条登录链路的回归：`with self._login_lock():` 把锁当方法调用，每次登录 500。"""
        def handler(method, url, body):
            if "serviceLogin?" in url:
                return _Resp(payload={"code": 0, "sid": "micoapi", "_sign": "SIGN",
                                      "qs": "http://qs", "callback": "http://cb"})
            if "serviceLoginAuth2" in url:
                return _Resp(payload={"location": "/pass/rsn1", "ssecurity": "SS",
                                      "userId": "7", "nonce": "1"}, prefixed=True)
            return _Resp(status=302, headers={"Location": "https://api2.mina.mi.com/redirect",
                                              "Set-Cookie": "serviceToken=NEWTOKEN; Path=/"})

        plugin = _plugin()
        plugin._client = lambda: (_FakeClient(devices=[_device()]), "")
        with _NetworkPatch(handler):
            out = plugin.api_login(x.schemas.LoginIn(username="me@example.com", password="pass"))
        self.assertEqual(out.state, mina.LOGIN_SUCCESS, out.message)
        self.assertTrue(out.logged_in)
        self.assertEqual(out.device_count, 1)
        self.assertEqual(plugin.get_data("token")["user_id"], "7")

    def test_login_failure_comes_back_as_a_message_not_an_exception(self) -> None:
        plugin = _plugin()
        original = x.XiaoaiAuth

        class _Boom:
            username = "me@example.com"

            def __init__(self, username: str, password: str, device_id: str = "") -> None:
                pass

            def login(self):
                raise RuntimeError("小米返回了看不懂的页面")

        x.XiaoaiAuth = _Boom
        try:
            out = plugin.api_login(x.schemas.LoginIn(username="me@example.com", password="pass"))
        finally:
            x.XiaoaiAuth = original
        self.assertEqual(out.state, "failed")
        self.assertIn("看不懂的页面", out.message)

    def test_sidebar_nav_and_service_gating(self) -> None:
        plugin = _plugin()
        nav = plugin.get_sidebar_nav()
        self.assertEqual(len(nav), 1)
        self.assertEqual(nav[0]["nav_key"], "main")
        self.assertEqual([s["id"] for s in plugin.get_service()], ["XiaoaiSpeaker.Tick"])
        off = _plugin(config={"enabled": False})
        self.assertEqual(off.get_sidebar_nav(), [])
        self.assertEqual(off.get_service(), [])
        hidden = _plugin(config={"show_sidebar_nav": False})
        self.assertEqual(hidden.get_sidebar_nav(), [])
        # 关掉连播仍要巡检：睡眠定时到点与索引保鲜都挂在那条任务上
        no_next = _plugin(config={"auto_next": False})
        self.assertEqual([s["id"] for s in no_next.get_service()], ["XiaoaiSpeaker.Tick"])
        voice = _plugin(config={"voice_enabled": True, "voice_poll": 5})
        self.assertEqual([s["id"] for s in voice.get_service()],
                         ["XiaoaiSpeaker.Tick", "XiaoaiSpeaker.Voice"])
        trigger = voice.get_service()[1]["trigger"]
        seconds = getattr(trigger, "seconds", None)
        if seconds is None:  # 真 apscheduler 用 timedelta
            seconds = trigger.interval.total_seconds()
        self.assertEqual(seconds, 5.0)

    def test_config_page_can_enable_the_plugin(self) -> None:
        """侧栏出现与否卡在 get_state()，而 get_state() 读的是配置里的 enabled：
        插件管理页那个开关碰不到它，所以这一项必须在插件自己的配置页能改、能立刻生效。"""
        plugin = _plugin(config={"enabled": False})
        self.assertEqual(plugin.get_sidebar_nav(), [])
        saved = _stub_env.JOB_UPDATES
        del saved[:]
        out = plugin.api_config(x.schemas.ConfigIn(enabled=True))
        self.assertTrue(out.config.enabled)
        self.assertTrue(plugin.get_state())
        self.assertEqual(len(plugin.get_sidebar_nav()), 1, "打开后侧栏入口就该出现")
        self.assertEqual(len(plugin.get_service()), 1, "打开后周期任务就该登记")
        self.assertEqual(saved, [plugin._plugin_id()], "保存后要请宿主重建一次任务")
        # 关掉侧栏入口不影响任务，只收起导航；两项各自独立
        plugin.api_config(x.schemas.ConfigIn(show_sidebar_nav=False))
        self.assertEqual(plugin.get_sidebar_nav(), [])
        self.assertTrue(plugin.get_state())
        del saved[:]
        plugin.api_config(x.schemas.ConfigIn(default_volume=40))
        self.assertEqual(saved, [], "没动开关就别多喊一次重建任务")

    def test_render_mode_is_vue_federation(self) -> None:
        self.assertEqual(x.XiaoaiSpeaker.get_render_mode(), ("vue", "dist/assets"))
        self.assertEqual(_plugin().get_page(), [])

    def test_built_remote_entry_serves_app_page(self) -> None:
        """侧栏 nav_key=main 时宿主按 ./AppPage → ./Page 找组件：只暴露 ./Page 会报
        「无法加载插件全页组件」，而这个只有装到实例上才看得见 —— 拿产物先断言一次。"""
        entry = (_ROOT / "plugins.v3" / "xiaoaispeaker" / "dist" / "assets" / "remoteEntry.js")
        if not entry.exists():
            self.skipTest(f"{entry} 尚未构建")
        text = entry.read_text(encoding="utf-8")
        self.assertIn("'./AppPage'", text)
        chunk = re.search(r"__federation_expose_AppPage-[A-Za-z0-9_-]+\.js", text)
        self.assertIsNotNone(chunk, "./AppPage 暴露了但没有对应产物")
        self.assertTrue((entry.parent / chunk.group(0)).exists())

    def test_api_declarations(self) -> None:
        plugin = _plugin()
        apis = plugin.get_api()
        paths = [a["path"] for a in apis]
        keys = [(a["path"], tuple(a["methods"])) for a in apis]
        self.assertEqual(len(keys), len(set(keys)), "同一路径的同一方法不能重复")
        for path in ("/mode", "/timer", "/favorites", "/library", "/library/refresh",
                     "/playlists", "/playlist", "/voice", "/voice/test", "/commands",
                     "/login/qrcode"):
            self.assertIn(path, paths, path)
        stream = [a for a in apis if a["path"] == "/stream"][0]
        self.assertTrue(stream.get("allow_anonymous"))
        self.assertNotIn("auth", stream)
        self.assertIsNone(stream["response_model"])
        for api in apis:
            if api["path"] == "/stream":
                continue
            self.assertEqual(api["auth"], "bear", api["path"])
            self.assertIsNotNone(api.get("response_model"), api["path"])

    def test_logout_clears_token_and_queue(self) -> None:
        plugin = _plugin()
        plugin._set_queue_state([str(Path(__file__))], 0, "D1")
        result = plugin._logout()
        self.assertFalse(result.logged_in)
        self.assertIsNone(plugin.get_data("token"))
        self.assertEqual(plugin._queue_state()["items"], [])


class PluginDeviceTest(unittest.TestCase):
    def _with_client(self, plugin, client) -> x.XiaoaiSpeaker:
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {}
        return plugin

    def test_device_target_priority(self) -> None:
        online = _device("D1", "客厅", presence="online")
        offline = _device("D2", "书房", presence="offline")
        plugin = self._with_client(_plugin(), _FakeClient(devices=[offline, online]))
        picked, _ = plugin._target_device()
        self.assertEqual(picked["device_id"], "D1", "无默认时应优先在线设备")
        plugin._default_device_id = "D2"
        picked, _ = plugin._target_device()
        self.assertEqual(picked["device_id"], "D2", "配置默认要压过在线优先")
        picked, _ = plugin._target_device("D1")
        self.assertEqual(picked["device_id"], "D1", "显式指定压过默认")

    def test_missing_default_device_is_reported_not_silently_swapped(self) -> None:
        plugin = self._with_client(_plugin(config={"default_device_id": "GONE"}),
                                   _FakeClient(devices=[_device()]))
        picked, err = plugin._target_device()
        self.assertIsNone(picked)
        self.assertIn("不在当前账号的设备列表里", err)
        result = plugin._control("pause")
        self.assertFalse(result.ok)
        self.assertIn("不在当前账号的设备列表里", result.message)

    def test_empty_device_list_is_not_silently_ok(self) -> None:
        plugin = self._with_client(_plugin(), _FakeClient(devices=[]))
        _, err = plugin._target_device()
        self.assertIn("没有可用音箱", err)

    def test_device_select_writes_config_and_validates(self) -> None:
        plugin = self._with_client(_plugin(), _FakeClient(devices=[_device("D9", name="次卧")]))
        ok = plugin.api_device_select(x.schemas.DeviceSelectIn(device_id="D9"))
        self.assertTrue(ok.ok)
        self.assertEqual(plugin._configs[-1]["default_device_id"], "D9")
        bad = plugin.api_device_select(x.schemas.DeviceSelectIn(device_id="NOPE"))
        self.assertFalse(bad.ok)

    def test_devices_fallback_to_cache_when_token_dies(self) -> None:
        plugin = _plugin()
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": [_device("DC")]}
        plugin._client = lambda: (None, "小米登录票据已失效")
        devices, _ = plugin._devices_cached(force=True)
        self.assertEqual([d["device_id"] for d in devices], ["DC"])

    def test_needs_music_api_flag_follows_whitelist(self) -> None:
        plugin = self._with_client(_plugin(), _FakeClient(
            devices=[_device("D1", hardware="LX05"), _device("D2", hardware="LX99")]))
        rows = plugin._devices_cached()[0]
        flagged = {row.hardware: row for row in plugin._devices_out(rows)}
        self.assertTrue(flagged["LX05"].needs_music_api)
        self.assertFalse(flagged["LX99"].needs_music_api)
        # 默认标记只给配置里那台，不跟着「第一台」跑
        plugin._default_device_id = "D2"
        self.assertEqual({r.device_id: r.is_default for r in plugin._devices_out(rows)},
                         {"D1": False, "D2": True})


class PluginPlayTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        self.root.mkdir()
        self.track = self.root / "05 - 稻香.flac"
        self.track.write_bytes(b"Z" * 512)
        self.outside = base / "outside.mp3"
        self.outside.write_bytes(b"X" * 8)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plugin_with_client(self, client=None, **config):
        data = {**media_defaults, "music_dirs": str(self.root)}
        data.update(config)
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin(data)
        plugin.save_data("token", _token())
        client = client or _FakeClient(devices=[_device()])
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": [_device()]}
        return plugin, client

    def test_play_pushes_signed_stream_url(self) -> None:
        plugin, client = self._plugin_with_client()
        result = plugin.api_play(x.schemas.PlayIn(path=str(self.track)))
        self.assertTrue(result.ok, result.message)
        pushed = [c for c in client.calls if c[0] == "play"][0]
        url = pushed[2]
        self.assertTrue(url.startswith("http://192.168.1.10:3000/api/v1/plugin/XiaoaiSpeaker/stream?"))
        self.assertNotIn("&", url)
        # 推给音箱的必须是真实绝对路径，签名对它成立
        path, exp, sig = media.parse_query(url.split("?", 1)[1])
        self.assertEqual(path, str(self.track))
        self.assertTrue(media.verify_sign(plugin._secret(), path, exp, sig))

    def test_state_files_are_reused_by_the_next_instance(self) -> None:
        """重载与重启等于换一个实例：签名密钥和登录票据必须还在。

        换密钥会让已经推给音箱的直链在下一次取流时 403；丢票据会让小米把它当成
        又一台新设备，于是每回登录都要过验证码与二次校验。
        """
        first, _ = self._plugin_with_client()
        secret = first._secret()
        secret_file = first._data_dir / "state" / "secret.json"
        self.assertTrue(secret_file.is_file())
        # 密钥文件里是签名密钥、票据文件里是登录态，都不许让别人读走
        self.assertEqual(secret_file.stat().st_mode & 0o777, 0o600)

        second = x.XiaoaiSpeaker()
        second._data_dir = first._data_dir
        second.init_plugin({**media_defaults, "music_dirs": str(self.root)})
        self.assertEqual(second._secret(), secret)
        self.assertEqual(second.get_data("token").get("user_id"),
                         _token().get("user_id"))
        # 退出登录要把票据从盘上删干净，不能只当内存里没有
        second.del_data("token")
        self.assertIsNone(second.get_data("token"))

    def test_play_rejects_outside_library_without_touching_speaker(self) -> None:
        plugin, client = self._plugin_with_client()
        result = plugin.api_play(x.schemas.PlayIn(path=str(self.outside)))
        self.assertFalse(result.ok)
        self.assertIn("不在配置的音乐库目录内", result.message)
        self.assertEqual([c for c in client.calls if c[0] == "play"], [])
        self.assertIsNone(plugin.get_data("last"))

    def test_play_without_base_url_explains_why(self) -> None:
        _SETTINGS.APP_DOMAIN = ""
        plugin, _ = self._plugin_with_client(base_url="")
        result = plugin.api_play(x.schemas.PlayIn(path=str(self.track)))
        self.assertFalse(result.ok)
        self.assertIn("外部", result.message)

    def test_play_url_mode_passes_url_through(self) -> None:
        plugin, client = self._plugin_with_client()
        result = plugin.api_play(x.schemas.PlayIn(url="https://cdn.example.com/a.mp3"))
        self.assertTrue(result.ok)
        pushed = [c for c in client.calls if c[0] == "play"][0]
        self.assertEqual(pushed[2], "https://cdn.example.com/a.mp3")

    def test_play_requires_one_of_path_url_keyword(self) -> None:
        plugin, _ = self._plugin_with_client()
        self.assertFalse(plugin.api_play(x.schemas.PlayIn()).ok)

    def test_default_volume_is_applied_after_push(self) -> None:
        plugin, client = self._plugin_with_client(default_volume=42)
        plugin.api_play(x.schemas.PlayIn(path=str(self.track)))
        self.assertIn(("set_volume", "D1", 42), client.calls)

    def test_volume_zero_means_do_not_touch(self) -> None:
        plugin, client = self._plugin_with_client(default_volume=0)
        plugin.api_play(x.schemas.PlayIn(path=str(self.track)))
        self.assertEqual([c for c in client.calls if c[0] == "set_volume"], [])

    def test_expired_token_triggers_silent_refresh(self) -> None:
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root)})
        plugin.save_data("token", _token(expired=True))
        refreshed = _token(service_token="NEW")
        calls: List[tuple] = []

        def fake_refresh():
            calls.append(("refresh",))
            plugin.save_data("token", refreshed)
            return refreshed, ""

        plugin._silent_refresh = fake_refresh
        client, err = plugin._client()
        self.assertIsNotNone(client)
        self.assertEqual(client.service_token(), "NEW")
        self.assertEqual(calls, [("refresh",)])

    def test_silent_refresh_without_pass_token_asks_for_login(self) -> None:
        plugin = _plugin()
        stale = _token()
        stale["pass_token"] = ""
        plugin.save_data("token", stale)
        plugin.__dict__["_refresh_attempt_at"] = 0
        token, message = plugin._silent_refresh()
        self.assertIsNone(token)
        self.assertIn("重新登录", message)

    def test_refresh_cooldown_blocks_hammering(self) -> None:
        plugin = _plugin()
        plugin.save_data("token", _token(expired=True))
        plugin.__dict__["_refresh_attempt_at"] = time.time()
        token, message = plugin._silent_refresh()
        self.assertIsNone(token)
        self.assertIn("刚失败过", message)


class PluginQueueTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        self.root.mkdir()
        self.tracks = []
        for name in ("01 - 一.mp3", "02 - 二.mp3", "03 - 三.mp3"):
            path = self.root / name
            path.write_bytes(b"Q" * 64)
            self.tracks.append(str(path))
        self.foreign = base / "gone.mp3"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plugin(self, status=None, auto_next=True):
        client = _FakeClient(devices=[_device()], status=status)
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root),
                            "auto_next": auto_next})
        plugin.save_data("token", _token())
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": [_device()]}
        return plugin, client

    def test_queue_drops_paths_outside_library(self) -> None:
        plugin, _ = self._plugin()
        out = plugin._set_queue_state(self.tracks + [str(self.foreign)], 0, "D1")
        self.assertEqual(out.total, 3)
        self.assertEqual(out.index, 0)

    def test_queue_index_is_clamped(self) -> None:
        plugin, _ = self._plugin()
        out = plugin._set_queue_state(self.tracks, 99, "D1")
        self.assertEqual(out.index, 0)

    def test_next_and_prev_walk_the_queue(self) -> None:
        plugin, client = self._plugin()
        plugin._set_queue_state(self.tracks, 0, "D1")
        result = plugin._control("next")
        self.assertTrue(result.ok)
        self.assertEqual(result.queue.index, 1)
        pushed = [c for c in client.calls if c[0] == "play"][-1]
        path, exp, sig = media.parse_query(pushed[2].split("?", 1)[1])
        # 下一首推的必须是队列里第 2 首，且这条直链的签名对插件当前密钥成立
        self.assertEqual(path, self.tracks[1])
        self.assertGreater(exp, int(time.time()))
        self.assertTrue(media.verify_sign(plugin._secret(), path, exp, sig))
        self.assertEqual(plugin._control("prev").queue.index, 0)

    def test_next_at_tail_does_not_wrap(self) -> None:
        plugin, _ = self._plugin()
        plugin._set_queue_state(self.tracks, 2, "D1")
        result = plugin._control("next")
        self.assertTrue(result.ok)
        self.assertEqual(result.message, "队列已播到最后一首")
        self.assertEqual(plugin._queue_state()["index"], 2)

    def test_numeric_control_jumps_to_index(self) -> None:
        plugin, _ = self._plugin()
        plugin._set_queue_state(self.tracks, 0, "D1")
        self.assertEqual(plugin._control("2").queue.index, 2)
        self.assertEqual(plugin._control("99").queue.index, 2)

    def test_empty_queue_controls_say_so(self) -> None:
        plugin, _ = self._plugin()
        self.assertFalse(plugin._control("next").ok)
        self.assertFalse(plugin.api_queue(x.schemas.QueueIn(paths=[])).ok)

    def test_api_queue_plays_requested_index(self) -> None:
        plugin, client = self._plugin()
        result = plugin.api_queue(x.schemas.QueueIn(paths=self.tracks, index=1))
        self.assertTrue(result.ok)
        self.assertEqual(result.queue.index, 1)
        pushed = [c for c in client.calls if c[0] == "play"][-1]
        path, _, _ = media.parse_query(pushed[2].split("?", 1)[1])
        self.assertEqual(path, self.tracks[1])

    def test_tick_advances_only_after_grace_and_two_stops(self) -> None:
        stopped = {"status": 0, "volume": 10, "position": 0, "duration": 0}
        plugin, client = self._plugin(status=stopped)
        plugin._set_queue_state(self.tracks, 0, "D1")
        plugin.save_data("last", {"device_id": "D1", "path": self.tracks[0],
                                  "started_at": time.time(), "title": "一"})
        before = len([c for c in client.calls if c[0] == "play"])
        plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before,
                         "推流后宽限期内不该连播")
        # get_data 每次给的是新对象（宿主 JSON 列也一样），改了要写回去才作数
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 60
        plugin.save_data("last", last)
        plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before,
                         "第一次读到停止不该立刻切歌")
        plugin._tick()
        after = len([c for c in client.calls if c[0] == "play"])
        self.assertEqual(after, before + 1, "连续两次停止后应推进下一首")
        self.assertEqual(plugin._queue_state()["index"], 1)

    def test_tick_does_nothing_while_playing(self) -> None:
        plugin, client = self._plugin(status={"status": 1, "volume": 10,
                                             "position": 1, "duration": 2})
        plugin._set_queue_state(self.tracks, 0, "D1")
        plugin.save_data("last", {"device_id": "D1", "started_at": time.time() - 600})
        plugin.__dict__["_idle_ticks"] = 5
        before = len([c for c in client.calls if c[0] == "play"])
        plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before)
        # 播放中就清零，攒下的「停止次数」不能带进下一轮
        self.assertEqual(plugin.__dict__["_idle_ticks"], 0)

    def test_tick_is_inactive_when_disabled(self) -> None:
        plugin, client = self._plugin(auto_next=False)
        plugin._set_queue_state(self.tracks, 0, "D1")
        before = len(client.calls)
        plugin._tick()
        self.assertEqual(len(client.calls), before)

    def test_stop_then_next_tick_wont_start_new_track(self) -> None:
        stopped = {"status": 0, "volume": 0, "position": 0, "duration": 0}
        plugin, client = self._plugin(status=stopped)
        plugin._set_queue_state(self.tracks, 0, "D1")
        plugin.save_data("last", {"device_id": "D1", "started_at": time.time() - 600})
        plugin.__dict__["_idle_ticks"] = 5
        plugin.stop_service()
        before = len([c for c in client.calls if c[0] == "play"])
        plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before)


class PluginControlCommandTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        self.root.mkdir()
        self.track = self.root / "07 - 七里香.mp3"
        self.track.write_bytes(b"C" * 32)
        self.other = self.root / "08 - 明明就.mp3"
        self.other.write_bytes(b"C" * 32)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plugin(self, status=None):
        client = _FakeClient(devices=[_device()], status=status)
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root)})
        plugin.save_data("token", _token())
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": [_device()]}
        return plugin, client

    def test_toggle_follows_reported_status(self) -> None:
        playing, client = self._plugin(status={"status": 1, "volume": 5,
                                              "position": 1, "duration": 2})
        playing._control("toggle")
        self.assertIn(("play_operation", "D1", "pause"), client.calls)
        idle, client2 = self._plugin(status={"status": 0, "volume": 5,
                                            "position": 0, "duration": 0})
        idle._control("toggle")
        self.assertIn(("play_operation", "D1", "play"), client2.calls)

    def test_unknown_action_is_rejected(self) -> None:
        plugin, client = self._plugin()
        result = plugin._control("eject")
        self.assertFalse(result.ok)
        self.assertIn("不支持", result.message)
        self.assertEqual([c for c in client.calls if c[0] == "play_operation"], [])

    def test_keyword_play_uses_library_search(self) -> None:
        plugin, client = self._plugin()
        result = plugin._play_keyword("明明就")
        self.assertTrue(result.ok)
        pushed = [c for c in client.calls if c[0] == "play"][-1]
        path, _, _ = media.parse_query(pushed[2].split("?", 1)[1])
        self.assertEqual(path, str(self.other))
        self.assertEqual(plugin._queue_state()["items"][0]["path"], str(self.other))

    def test_keyword_play_reports_no_hit(self) -> None:
        plugin, _ = self._plugin()
        result = plugin._play_keyword("查无此曲")
        self.assertFalse(result.ok)
        self.assertIn("没找到", result.message)

    def test_tts_text_is_limited(self) -> None:
        plugin, client = self._plugin()
        result = plugin.api_tts(x.schemas.TtsIn(text="字" * 500))
        self.assertTrue(result.ok)
        text = [c for c in client.calls if c[0] == "tts"][-1][2]
        self.assertEqual(len(text), 200)
        self.assertFalse(plugin.api_tts(x.schemas.TtsIn(text="   ")).ok)

    def test_status_reads_volume_and_position(self) -> None:
        plugin, _ = self._plugin(status={"status": 2, "volume": 33,
                                        "position": 4000, "duration": 9000})
        playing = plugin._playing_out(force=True)
        self.assertEqual((playing.status, playing.status_text), (2, "已暂停"))
        self.assertEqual(playing.volume, 33)
        self.assertEqual(playing.position, 4000)

    def test_status_needs_tells_the_user_what_is_missing(self) -> None:
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({"enabled": True, "base_url": "", "music_dirs": "",
                            "username": "", "password": ""})
        status = plugin._status()
        joined = " ".join(status.needs)
        self.assertIn("登录", joined)
        self.assertIn("音乐库路径", joined)
        self.assertIn("外部访问地址", joined)
        self.assertFalse(status.logged_in)

    def test_command_uses_arg_str_and_filters_actions(self) -> None:
        plugin, client = self._plugin()
        event = _stub_env._Event(event_data={"action": "xiaoai_play",
                                            "arg_str": str(self.track),
                                            "channel": "Telegram", "user": "u1"})
        plugin.handle_command(event)
        self.assertEqual(len(plugin.messages), 1)
        self.assertEqual(plugin.messages[0]["channel"], "Telegram")
        self.assertEqual(plugin.messages[0]["userid"], "u1")
        self.assertEqual([c for c in client.calls if c[0] == "play"][-1][1], "D1")
        plugin.handle_command(_stub_env._Event(event_data={"action": "other_plugin_thing"}))
        self.assertEqual(len(plugin.messages), 1, "别的插件的动作不该由本插件响应")

    def test_command_status_and_usage(self) -> None:
        plugin, _ = self._plugin()
        plugin.handle_command(_stub_env._Event(event_data={"action": "xiaoai_status"}))
        self.assertIn("小爱音箱状态", plugin.messages[-1]["title"])
        plugin.handle_command(_stub_env._Event(event_data={"action": "xiaoai_play"}))
        self.assertIn("用法", plugin.messages[-1]["text"])
        plugin.handle_command(_stub_env._Event(event_data={"action": "xiaoai_volume",
                                                          "arg_str": "70"}))
        self.assertIn("音量 70", plugin.messages[-1]["text"])

    def test_command_failure_becomes_a_message_not_a_crash(self) -> None:
        plugin, _ = self._plugin()
        plugin._control = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("炸了"))
        plugin.handle_command(_stub_env._Event(event_data={"action": "xiaoai_control",
                                                           "arg_str": "pause"}))
        self.assertIn("小爱音箱命令执行失败", plugin.messages[-1]["title"])


class PluginStreamTranscodeTest(unittest.TestCase):
    """force_mp3：非 mp3 现场转码，长度未知所以不给 Range。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        self.root.mkdir()
        self.track = self.root / "song.m4a"
        self.track.write_bytes(b"FAKE" * 40)
        self.mp3 = self.root / "already.mp3"
        self.mp3.write_bytes(b"ID3" * 40)
        self.plugin = x.XiaoaiSpeaker()
        self.plugin.init_plugin({**media_defaults, "music_dirs": str(self.root),
                                 "force_mp3": True})

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _request(self, path: Path) -> Any:
        request_type = sys.modules["fastapi"].Request
        return request_type(query={"k": media.pack_link(
            self.plugin._secret(), str(path), int(time.time() + 600))},
            headers={"Range": "bytes=0-"})

    def test_switch_only_affects_non_mp3(self) -> None:
        self.assertTrue(media.needs_transcode(self.track, True))
        self.assertFalse(media.needs_transcode(self.mp3, True))
        self.assertFalse(media.needs_transcode(self.track, False))

    def test_config_endpoint_turns_the_switch_on_and_off(self) -> None:
        """配置页那个开关要真的落到属性上：漏一条赋值等于开关是画的。"""
        plugin = _plugin()
        self.assertFalse(plugin._force_mp3)
        out = plugin.api_config(x.schemas.ConfigIn(force_mp3=True))
        self.assertTrue(plugin._force_mp3, out.config.force_mp3)
        self.assertTrue(out.config.force_mp3)
        plugin.api_config(x.schemas.ConfigIn(force_mp3=False))
        self.assertFalse(plugin._force_mp3)

    def test_transcode_command_has_no_shell_and_no_secret(self) -> None:
        # argv 列表交给 exec：路径里的空格引号都不参与解析；直链的签名不进命令行
        argv = media.transcode_command(Path("/media/我 的 \"歌.m4a\""))
        self.assertIsInstance(argv, list)
        self.assertIn("-acodec", argv)
        self.assertEqual(argv[argv.index("-i") + 1], "/media/我 的 \"歌.m4a\"")
        self.assertNotIn("/stream", " ".join(argv))

    def test_missing_ffmpeg_falls_back_to_the_source_file(self) -> None:
        """开着开关但机器上没有 ffmpeg，不能把本来能放的弄成不能放。"""
        original = media.ffmpeg_available
        media.ffmpeg_available = lambda: False
        try:
            response = self.plugin.api_stream(self._request(self.track))
        finally:
            media.ffmpeg_available = original
        # 回的是源文件本身（上面那句 _request 带 bytes=0-，所以按新的对齐是 206）
        self.assertEqual(response.status_code, 206)
        self.assertEqual(b"".join(response.iterator), self.track.read_bytes())

    def test_transcoded_stream_is_mp3_without_range_support(self) -> None:
        started: List[Any] = []

        class _Proc:
            stdout = io.BytesIO(b"\xff\xfb" + b"x" * 500)
            returncode = None

            def poll(self): return None
            def kill(self): pass
            def wait(self): pass

        original_available, original_start = media.ffmpeg_available, media.start_transcode
        media.ffmpeg_available = lambda: True
        media.start_transcode = lambda path: started.append(path) or _Proc()
        try:
            response = self.plugin.api_stream(self._request(self.track))
        finally:
            media.ffmpeg_available = original_available
            media.start_transcode = original_start
        self.assertEqual(started, [self.track])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.media_type, "audio/mpeg")
        self.assertEqual(response.headers["Accept-Ranges"], "none")
        self.assertNotIn("Content-Length", response.headers)
        self.assertIn("song.mp3", response.headers["Content-Disposition"])

    def test_transcode_that_wont_start_is_a_clear_502(self) -> None:
        original_available, original_start = media.ffmpeg_available, media.start_transcode
        media.ffmpeg_available = lambda: True
        media.start_transcode = lambda path: (_ for _ in ()).throw(
            media.TranscodeError("起不了 ffmpeg：坏文件"))
        try:
            response = self.plugin.api_stream(self._request(self.track))
        finally:
            media.ffmpeg_available = original_available
            media.start_transcode = original_start
        self.assertEqual(response.status_code, 502)
        self.assertIn("坏文件", response.body["detail"])


class PluginStreamEndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        self.root.mkdir()
        self.track = self.root / "song.mp3"
        self.track.write_bytes(bytes(range(100)) * 3)
        self.outside = base / "outside.mp3"
        self.outside.write_bytes(b"N" * 8)
        self.plugin = x.XiaoaiSpeaker()
        self.plugin.init_plugin({**media_defaults, "music_dirs": str(self.root)})

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _request(self, path: str, expires: Optional[int] = None,
                 secret: Optional[str] = None, token: Optional[str] = None,
                 range_header: Optional[str] = None) -> Any:
        target = str(path)
        exp = int(expires if expires is not None else time.time() + 600)
        if token is None:
            token = media.pack_link(
                self.plugin._secret() if secret is None else secret, target, exp)
        headers = {"Range": range_header} if range_header else {}
        request_type = sys.modules["fastapi"].Request
        return request_type(query={"k": token}, headers=headers)

    def test_valid_signature_streams_file(self) -> None:
        response = self.plugin.api_stream(self._request(self.track))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response.iterator), self.track.read_bytes())
        self.assertEqual(response.headers["Accept-Ranges"], "bytes")

    def test_range_slice_returns_206(self) -> None:
        response = self.plugin.api_stream(self._request(self.track, range_header="bytes=10-19"))
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.headers["Content-Range"], f"bytes 10-19/{self.track.stat().st_size}")
        self.assertEqual(b"".join(response.iterator), self.track.read_bytes()[10:20])

    def test_open_ended_range_answers_206_like_upstream(self) -> None:
        """`bytes=0-` 也要按 206 回：音箱发的就是这一句。

        上游 songloft 的访问日志里，音箱带 range="bytes=0-" 来取，回的是
        status=206 + content_range="bytes 0-N/N"；我们原先回 200，有些固件就把这条
        当成不可分段的音频而不去解码。
        """
        size = self.track.stat().st_size
        response = self.plugin.api_stream(self._request(self.track, range_header="bytes=0-"))
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.headers["Content-Range"], f"bytes 0-{size - 1}/{size}")
        self.assertEqual(response.headers["Content-Length"], str(size))
        self.assertEqual(b"".join(response.iterator), self.track.read_bytes())

    def test_tampered_or_expired_or_foreign_is_403(self) -> None:
        # 换一把密钥：签名对不上
        bad_sig = self.plugin.api_stream(self._request(self.track, secret="0" * 32))
        self.assertEqual(bad_sig.status_code, 403)
        expired = self.plugin.api_stream(self._request(self.track, expires=int(time.time()) - 5))
        self.assertEqual(expired.status_code, 403)
        # 签名算得对，但目标是库外文件：越库判定必须仍然拦住
        foreign = self.plugin.api_stream(self._request(self.outside))
        self.assertEqual(foreign.status_code, 403)
        self.assertIn("不在配置的音乐库目录内", foreign.body["detail"])

    def test_missing_signature_and_missing_file_are_refused(self) -> None:
        # 链接被中间层弄坏（小米云端会把 & 换成空格，就是这样）与签名指向已删除
        # 的文件，都是 403
        mangled = self.plugin.api_stream(
            self._request(self.track, token="1792950245." + "0" * 32 + ".not-base64!!"))
        self.assertEqual(mangled.status_code, 403)
        gone = self.root / "deleted.mp3"
        response = self.plugin.api_stream(self._request(gone))
        self.assertEqual(response.status_code, 403)
        self.assertIn("文件不存在", response.body["detail"])

    def test_large_range_start_clamps_to_whole_file(self) -> None:
        # 越界的 Range 起点回整文件：音箱常见做法是拿它当「从头播」
        response = self.plugin.api_stream(self._request(self.track, range_header="bytes=99999-"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response.iterator), self.track.read_bytes())


class PluginVoiceTest(unittest.TestCase):
    """语音链路：会话轮询的去重、口令判定与执行、睡眠定时、收藏/歌单接口。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        (self.root / "周杰伦" / "范特西").mkdir(parents=True)
        self.tracks = []
        for name in ("01 - 周杰伦 - 稻香.mp3", "02 - 周杰伦 - 双截棍.mp3",
                     "03 - 周杰伦 - 朴素.mp3"):
            path = self.root / "周杰伦" / "范特西" / name
            path.write_bytes(b"V" * 64)
            self.tracks.append(str(path))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plugin(self, config=None, asks=None, **client_kwargs) -> tuple:
        client = _FakeClient(devices=[_device()], asks=asks, **client_kwargs)
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root),
                            "voice_enabled": True, **(config or {})})
        plugin.save_data("token", _token())
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": [_device()]}
        plugin._library().collect([self.root], plugin._exts(), 200)
        return plugin, client

    def _listener_tick(self, plugin, client, asks) -> int:
        """绕过监听器自己的节流，把一轮轮询当成"到点了"。"""
        client._asks = asks
        listener = plugin._listener()
        listener._next_at = 0.0
        return listener.tick()

    def test_listener_primes_before_handling(self) -> None:
        plugin, client = self._plugin(asks=[])
        old = {"request_id": "r1", "timestamp_ms": int(time.time() * 1000) - 60000,
               "question": "播放歌曲稻香", "answer": "好的"}
        handled = self._listener_tick(plugin, client, [old])
        self.assertEqual(handled, 0, "首轮只对齐时间基线，不能把登录前说的话当成命令")
        newer = {"request_id": "r2", "timestamp_ms": int(time.time() * 1000),
                 "question": "停止播放", "answer": "已停止"}
        handled = self._listener_tick(plugin, client, [old, newer])
        self.assertEqual(handled, 1)
        self.assertIn(("play_operation", "D1", "stop"), client.calls)
        handled = self._listener_tick(plugin, client, [old, newer])
        self.assertEqual(handled, 0, "同一条记录不能被反复执行")

    def test_listener_treats_fetch_failure_as_unknown(self) -> None:
        plugin, client = self._plugin(asks=None)
        handled = self._listener_tick(plugin, client, None)
        self.assertEqual(handled, 0)
        listener = plugin._listener()
        self.assertIn("读对话记录失败", listener.status()["error"])
        # 失败那轮不推进基线：恢复后这句还没处理过的话仍然要执行
        record = {"request_id": "r9", "timestamp_ms": int(time.time() * 1000),
                  "question": "音量调到30", "answer": "好"}
        plugin._listener()._primed["D1"] = True
        self._listener_tick(plugin, client, [record])
        self.assertIn(("set_volume", "D1", 30), client.calls)

    def test_voice_test_endpoint_judges_without_playing(self) -> None:
        plugin, client = self._plugin()
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="我想听稻香"))
        self.assertTrue(out.ok)
        self.assertEqual(out.kind, "play_song")
        self.assertIn("稻香", out.argument)
        # 判定的说法要说清「没执行」，页面上那行小字直接用它
        self.assertIn("未执行", out.message)
        self.assertEqual(out.matched, "我想听")
        self.assertFalse([call for call in client.calls if call[0] == "play"])

    def test_voice_test_execute_plays_and_queues_album(self) -> None:
        plugin, client = self._plugin()
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="我想听稻香", execute=True))
        self.assertTrue(out.ok)
        self.assertIn("播放", out.reply)
        self.assertIn("已执行", out.message)
        self.assertTrue([call for call in client.calls if call[0] == "play"])
        state = plugin._queue_state()
        self.assertEqual(len(state["items"]), 3, "同专辑连播：点一首要把这碟排进队列")
        self.assertIn("稻香", state["items"][state["index"]]["path"])

    def test_voice_unknown_is_not_executed(self) -> None:
        plugin, client = self._plugin()
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="今天天气怎么样", execute=True))
        self.assertFalse(out.ok)
        # 没命中时必须讲清是哪一级没接手：这个夹具没开 AI 兜底
        self.assertIn("AI 兜底没开", out.message)
        self.assertFalse([call for call in client.calls if call[0] == "play"])

    def test_voice_interrupt_and_hint_before_search(self) -> None:
        plugin, client = self._plugin(config={"voice_interrupt": True, "voice_hint_tts": True})
        handled = self._listener_tick(plugin, client, [])
        self.assertEqual(handled, 0)
        plugin._on_voice_text("播放歌曲稻香", "D1", "")
        kinds = [call[0] for call in client.calls]
        self.assertIn("interrupt", kinds)
        self.assertIn("tts", kinds)

    def test_homophone_is_not_played(self) -> None:
        plugin, _ = self._plugin()
        # 关键词本身被听错了就当没听见：不猜，免得把客厅变成随机点歌机
        missed = plugin.api_voice_test(x.schemas.VoiceIn(text="我想提到香", execute=True))
        self.assertFalse(missed.ok)
        # 关键词对了、歌名是同音错字：宁可说没找到，也不要放成另一首
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="播放歌曲到香", execute=True))
        self.assertEqual(out.kind, "play_song")
        self.assertIn("没找到", out.reply)
        self.assertEqual(plugin._queue_state()["items"], [])

    def test_volume_mode_and_index_intents(self) -> None:
        plugin, client = self._plugin()
        plugin.api_voice_test(x.schemas.VoiceIn(text="音量调到30", execute=True))
        self.assertIn(("set_volume", "D1", 30), client.calls)
        plugin.api_voice_test(x.schemas.VoiceIn(text="大声一点", execute=True))
        self.assertIn(("set_volume", "D1", 40), client.calls, "增量要基于当前音量")
        plugin.api_voice_test(x.schemas.VoiceIn(text="播放歌曲稻香", execute=True))
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="随机播放", execute=True))
        self.assertIn("随机", out.reply)
        self.assertEqual(plugin._queue_state()["mode"], "random")
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="播放第3首", execute=True))
        self.assertEqual(plugin._queue_state()["index"], 2)
        self.assertIn("第 3 首", out.reply)

    def test_queue_mode_survives_new_queue(self) -> None:
        plugin, _ = self._plugin()
        plugin._set_mode("loop")
        plugin._set_queue_state(self.tracks[:2], 0, "D1")
        self.assertEqual(plugin._queue_state()["mode"], "loop")
        self.assertEqual(plugin._queue_out().mode, "loop")
        plugin._advance("next")
        plugin._advance("next")
        self.assertEqual(plugin._queue_state()["index"], 0, "列表循环要在队尾回到第一首")

    def test_order_mode_end_of_queue_reports_once(self) -> None:
        plugin, _ = self._plugin()
        plugin._set_queue_state(self.tracks[:2], 1, "D1")
        result = plugin._advance("next")
        self.assertTrue(result.ok)
        self.assertEqual(plugin._queue_state()["index"], 1)
        reply = plugin._run_intent(x.voice.Match("next", "", "下一首", "", "下一首"), "D1")
        self.assertEqual(reply, x._QUEUE_END_MESSAGE)

    def test_sleep_timer_set_query_and_stop(self) -> None:
        plugin, client = self._plugin()
        out = plugin.api_timer(x.schemas.TimerIn(minutes=30))
        self.assertTrue(out.active)
        self.assertIn("30 分钟", out.remaining)
        query = plugin._run_intent(
            x.voice.Match("query_sleep_timer", "", "", "", "还有多久停"), "D1")
        self.assertIn("分钟", query)
        # 到点：巡检任务要真的把音箱停下（时间戳直接改旧，不真等 30 分钟）
        plugin.save_data("sleep_timer", {"stop_at": time.time() - 1, "device_id": "D1"})
        plugin._tick()
        self.assertIn(("play_operation", "D1", "stop"), client.calls)
        self.assertFalse(plugin._timer_state_out()["active"])

    def test_sleep_timer_by_songs_stops_on_finish(self) -> None:
        plugin, client = self._plugin(status={"status": 0, "volume": 10,
                                              "position": 0, "duration": 1})
        plugin._set_queue_state(self.tracks, 0, "D1")
        plugin.save_data("sleep_timer", {"stop_after_songs": 1, "device_id": "D1"})
        plugin.save_data("last", {"device_id": "D1", "path": self.tracks[0],
                                  "started_at": time.time() - 60})
        plugin._tick()
        plugin._tick()
        self.assertIn(("play_operation", "D1", "stop"), client.calls, "播完一首就该停")
        self.assertEqual(plugin._queue_state()["index"], 0, "不该再推进到下一首")

    def test_timer_cancel_by_songs_and_minutes(self) -> None:
        plugin, _ = self._plugin()
        plugin._run_intent(x.voice.Match("sleep_timer", "songs", "", "2首", "放2首歌后停止"), "D1")
        self.assertEqual(plugin._timer_state_out()["stop_after_songs"], 2)
        out = plugin.api_timer(x.schemas.TimerIn(cancel=True))
        self.assertFalse(out.active)

    def test_favorites_api_and_voice(self) -> None:
        plugin, _ = self._plugin()
        out = plugin.api_favorite(x.schemas.FavoriteIn(path=self.tracks[0]))
        self.assertTrue(out.ok)
        self.assertEqual(len(out.items), 1)
        self.assertIn("稻香", out.items[0].title)
        again = plugin.api_favorite(x.schemas.FavoriteIn(path=self.tracks[0]))
        self.assertTrue(again.ok)
        self.assertIn("已经在收藏里", again.message)
        # 播着一首时说"收藏这首歌"，收藏的是当前队列项
        plugin._set_queue_state(self.tracks, 1, "D1")
        reply = plugin._run_intent(
            x.voice.Match("favorite", "add", "收藏", "", "收藏这首歌"), "D1")
        self.assertIn("已收藏", reply)
        self.assertEqual(len(plugin._favorites()), 2)
        removed = plugin.api_favorite(x.schemas.FavoriteIn(path=self.tracks[0], remove=True))
        self.assertEqual(len(removed.items), 1)

    def test_playlists_api_and_favorite_playlist(self) -> None:
        plugin, client = self._plugin()
        rows = plugin.api_playlists()
        self.assertTrue(rows.ok)
        names = [item.name for item in rows.items]
        self.assertIn("范特西", names)
        target = [item for item in rows.items if item.name == "范特西"][0]
        songs = plugin.api_playlist(playlist_id=int(target.id))
        self.assertEqual(len(songs.paths), 3)
        reply = plugin._run_intent(
            x.voice.Match("play_playlist", "", "播放歌单", "范特西", "播放歌单范特西"), "D1")
        self.assertIn("播放歌单", reply)
        self.assertEqual(len(plugin._queue_state()["items"]), 3)
        plugin.api_favorite(x.schemas.FavoriteIn(path=self.tracks[1]))
        reply = plugin._run_intent(
            x.voice.Match("play_playlist", "", "播放歌单", "收藏", "播放我的收藏"), "D1")
        self.assertTrue(reply, "收藏当歌单播要有回执")
        self.assertEqual(plugin._queue_state()["items"][0]["path"], self.tracks[1])

    def test_commands_toggle_gates_matching(self) -> None:
        plugin, _ = self._plugin()
        saved = plugin.api_commands_save(x.schemas.CommandsIn(enabled=["next", "set_volume"]))
        self.assertEqual(sorted(plugin._voice_commands()), ["next", "set_volume"])
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="收藏这首歌", execute=True))
        self.assertFalse(out.ok, "关掉的口令类型不该再命中")
        flags = {item.kind: item.enabled for item in saved.items}
        self.assertTrue(flags["next"])
        self.assertFalse(flags["favorite"])
        # 停止是内置兜底，不跟着开关走：否则关掉口令表就再也停不下来了
        stopped = plugin.api_voice_test(x.schemas.VoiceIn(text="暂停播放", execute=True))
        self.assertEqual(stopped.kind, "stop")
        plugin.api_commands_save(x.schemas.CommandsIn(enabled=None))
        self.assertIn("favorite", plugin._voice_commands(), "enabled=null 要恢复全开")

    def _host_assistant(self, reply: str = "", **settings) -> "_stub_env._LLMHelper":
        """把宿主「设定 → AI 大模型」桩成可用，并排好下一次要回的话。"""
        _SETTINGS.LLM_PROVIDER = settings.get("provider", "deepseek")
        _SETTINGS.LLM_MODEL = settings.get("model", "test-model")
        _SETTINGS.LLM_API_KEY = settings.get("api_key", "sk-from-host-settings")
        _SETTINGS.LLM_BASE_URL = settings.get("base_url", "https://api.example.test")
        _stub_env._LLMHelper.reset()
        if reply:
            _stub_env._LLMHelper.script = [reply]
        return _stub_env._LLMHelper

    def _without_assistant(self) -> None:
        _SETTINGS.LLM_MODEL = ""
        _SETTINGS.LLM_API_KEY = ""
        _stub_env._LLMHelper.reset()

    def test_ai_fallback_uses_host_assistant(self) -> None:
        """AI 兜底只能借用主程序配好的那个助手：插件自己不留第二份 key/端点。"""
        helper = self._host_assistant(
            '{"action":"set_play_mode","params":{"mode":"random"},"confidence":"high"}')
        try:
            plugin, client = self._plugin(config={"ai_enabled": True, "ai_chat": True})
            # 口令表管不到的说法（"摇滚模式"）由模型兜底，动作仍走同一套执行代码
            out = plugin.api_voice_test(x.schemas.VoiceIn(text="来点摇滚那种乱序的", execute=True))
            self.assertTrue(out.ok)
            self.assertEqual(out.kind, "set_play_mode")
            self.assertEqual(plugin._queue_state()["mode"], "random")
            self.assertEqual(helper.kwargs.get("model"), "test-model")
            self.assertEqual(helper.kwargs.get("api_key"), "sk-from-host-settings")
            self.assertEqual(helper.kwargs.get("base_url"), "https://api.example.test")
            # 宿主 get_llm 没有 response_format/timeout 这两项：多传一个就是 TypeError，
            # JSON 靠提示词约束、超时靠我们自己 wait_for
            self.assertNotIn("response_format", helper.kwargs)
            self.assertNotIn("timeout", helper.kwargs)
            self.assertTrue(helper.instances[0].closed, "用完要把模型实例交还宿主")
            # 模型说"没把握"就当没听见，不许它猜着播
            helper.script = ['{"action":"play_song","params":{"name":"稻香"},"confidence":"low"}']
            self.assertFalse(plugin.api_voice_test(x.schemas.VoiceIn(text="随便来点")).ok)
            # 对话兜底：口令与记忆都没命中才轮到聊天，且回答不落盘
            # 排两句：第一句给意图判定（解析不出 JSON 就当没命中），第二句才是聊天回答
            helper.script = ["这句不像口令", "今天多云，二十度左右。"]
            reply = plugin._on_voice_text("今天天气咋样啊", "D1", "")
            self.assertIn("多云", reply)
            self.assertTrue(plugin._chat_active())
            self.assertEqual(len(plugin.__dict__["_chat_history"]), 2)
            self.assertNotIn("chat_history",
                             [p.stem for p in (plugin._data_dir / "state").glob("*.json")])
            self.assertFalse([call for call in client.calls if call[0] == "play"])
        finally:
            self._without_assistant()

    def test_ai_call_failure_is_not_fatal(self) -> None:
        """宿主模型超时或报错不许把语音链路拖下水：口令没命中就是没命中，异常不外抛。"""
        # asyncio.wait_for 在 3.11+ 抛的就是内置 TimeoutError，这里按同一种失败演
        self._host_assistant(TimeoutError("模型响应超时"))
        try:
            plugin, client = self._plugin(config={"ai_enabled": True, "ai_chat": True})
            self.assertFalse(plugin.api_voice_test(
                x.schemas.VoiceIn(text="来点摇滚那种乱序的", execute=True)).ok)
            self.assertEqual(plugin._chat_reply("今天天气咋样啊", "D1"), "")
            self.assertIn("调用失败", plugin._assistant().error)
            self.assertFalse([call for call in client.calls if call[0] == "play"])
        finally:
            self._without_assistant()

    def test_ai_timeout_is_applied_to_the_host_call(self) -> None:
        """ai_timeout 不是摆设：它真的当成 wait_for 的 timeout 用（宿主 get_llm 没这一项）。"""
        self._host_assistant("好，接着放。")
        seen: Dict[str, Any] = {}
        real = asyncio.wait_for

        async def spy(coro, *args, **kwargs):
            seen["timeout"] = kwargs.get("timeout", args[0] if args else None)
            return await real(coro, *args, **kwargs)

        assistant = x.intents.Assistant(7)
        asyncio.wait_for = spy
        try:
            self.assertEqual(assistant.ask("系统提示", "说一句"), "好，接着放。")
        finally:
            asyncio.wait_for = real
        self.assertEqual(seen.get("timeout"), 7)

    def test_ai_unavailable_without_host_settings(self) -> None:
        """宿主没配助手时不许假装可用：口令没命中就是没命中，状态里说清缺哪一项。"""
        self._without_assistant()
        plugin, _client = self._plugin(config={"ai_enabled": True, "ai_chat": True})
        self.assertFalse(plugin._ai_available())
        self.assertIsNone(plugin._match_voice("来点摇滚那种乱序的"))
        self.assertEqual(plugin._chat_reply("今天天气咋样啊", "D1"), "")
        status = plugin._status()
        self.assertFalse(status.config.ai_ready)
        self.assertIn("API Key", status.config.ai_message)
        off = self._plugin(config={"ai_enabled": False})[0]
        self.assertIsNone(off._match_voice("今天天气咋样啊"))

    def test_ai_history_keeps_assistant_turns(self) -> None:
        """对话历史里的助手那一轮要按 assistant 角色回给宿主模型，不能都算成用户说的。"""
        helper = self._host_assistant("好，接着放。")
        try:
            plugin, _client = self._plugin(config={"ai_enabled": True, "ai_chat": True})
            plugin.__dict__["_chat_history"] = [{"role": "user", "content": "上一句"},
                                                {"role": "assistant", "content": "上一答"}]
            plugin._chat_reply("再来一句", "D1")
            roles = [type(message).__name__ for message in helper.instances[0].messages]
        finally:
            self._without_assistant()
        self.assertEqual(roles, ["SystemMessage", "HumanMessage", "AIMessage", "HumanMessage"])

    def test_ai_parse_accepts_fenced_json(self) -> None:
        match = x.intents.AIAnalyzer._parse(
            '```json\n{"action":"play_index","params":{"index":"三"},"confidence":"medium"}\n```',
            "播第三首呗")
        self.assertIsNotNone(match)
        self.assertEqual(match.kind, "play_index")
        self.assertEqual(match.argument, "3")
        self.assertIsNone(x.intents.AIAnalyzer._parse("不是 JSON", "x"))
        self.assertIsNone(x.intents.AIAnalyzer._parse('{"action":"reboot"}', "x"))

    def test_config_api_applies_voice_fields_and_resets_listener(self) -> None:
        plugin, _client = self._plugin()
        listener = plugin._listener()
        listener._baseline["D1"] = 123
        out = plugin.api_config(x.schemas.ConfigIn(voice_poll=7, voice_devices="D9",
                                                   library_refresh=120))
        self.assertEqual(out.config.voice_poll, 7)
        self.assertEqual(out.config.voice_devices, "D9")
        self.assertEqual(out.config.library_refresh, 120)
        fresh = plugin._listener()
        self.assertEqual(fresh.status()["interval"], 7.0)
        self.assertEqual(fresh._baseline, {}, "改配置要丢掉上一批设备的时间基线")


class PluginGroupTest(unittest.TestCase):
    """设备分组：一条指令带动全组，成员互斥，掉队的只记一条告警。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        self.root.mkdir()
        self.track = self.root / "01 - 周杰伦 - 稻香.mp3"
        self.track.write_bytes(b"G" * 32)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plugin(self, devices=None, **client_kwargs):
        devices = devices or [_device("D1", "客厅"), _device("D2", "卧室"), _device("D3", "书房")]
        client = _FakeClient(devices=devices, **client_kwargs)
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root)})
        plugin.save_data("token", _token())
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": list(devices)}
        return plugin, client

    def test_group_save_validates_and_persists(self) -> None:
        plugin, _client = self._plugin()
        self.assertFalse(plugin.api_group_save(x.schemas.GroupIn(name="全屋")).ok)
        self.assertFalse(plugin.api_group_save(
            x.schemas.GroupIn(name="全屋", device_ids=["D1", "D9"])).ok,
            "设备不在账号列表里就不能入组")
        out = plugin.api_group_save(x.schemas.GroupIn(name="全屋", device_ids=["D1", "D2", "D2"]))
        self.assertTrue(out.ok)
        self.assertEqual(out.items[0].device_ids, ["D1", "D2"], "重复成员要去掉")
        self.assertEqual([row["name"] for row in plugin._groups()], ["全屋"])
        self.assertFalse(plugin.api_group_save(x.schemas.GroupIn(name="没有", remove=True)).ok)
        self.assertTrue(plugin.api_group_save(x.schemas.GroupIn(name="全屋", remove=True)).ok)
        self.assertEqual(plugin._groups(), [])

    def test_membership_is_exclusive(self) -> None:
        plugin, _client = self._plugin()
        plugin.api_group_save(x.schemas.GroupIn(name="A", device_ids=["D1", "D2", "D3"]))
        plugin.api_group_save(x.schemas.GroupIn(name="B", device_ids=["D2", "D3"]))
        rows = {row["name"]: row["device_ids"] for row in plugin._groups()}
        self.assertEqual(rows, {"B": ["D2", "D3"]},
                         "D2/D3 被 B 摘走后 A 只剩一台，已不成立要一并作废")

    def test_control_volume_tts_fan_out(self) -> None:
        plugin, client = self._plugin()
        plugin.api_group_save(x.schemas.GroupIn(name="全屋", device_ids=["D1", "D2", "D3"]))
        client.calls.clear()
        out = plugin._control("pause")
        self.assertTrue(out.ok)
        self.assertEqual(sorted(call[1] for call in client.calls if call[0] == "play_operation"),
                         ["D1", "D2", "D3"], "暂停要落到全组")
        self.assertIn("已同步 3 台", out.message)
        client.calls.clear()
        plugin._volume(40)
        self.assertEqual(sorted(call[1] for call in client.calls if call[0] == "set_volume"),
                         ["D1", "D2", "D3"])
        client.calls.clear()
        plugin._tts("吃饭了")
        self.assertEqual(sorted(call[1] for call in client.calls if call[0] == "tts"),
                         ["D1", "D2", "D3"])

    def test_play_push_fans_out_and_reports_stragglers(self) -> None:
        plugin, client = self._plugin()
        plugin.api_group_save(x.schemas.GroupIn(name="全屋", device_ids=["D1", "D2"]))
        out = plugin._play_file(str(self.track))
        self.assertTrue(out.ok)
        self.assertEqual(sorted(call[1] for call in client.calls if call[0] == "play"), ["D1", "D2"])
        self.assertIn("已同步 2 台", out.message)

    def test_stray_member_failure_is_reported_not_fatal(self) -> None:
        class _HalfClient(_FakeClient):
            def play(self, device_id, url, hardware="", play_mode="auto", keep_light=False):
                self.calls.append(("play", device_id))
                if device_id == "D2":
                    raise RuntimeError("设备离线")
                return {"ok": True, "api": "player_play_url", "error": ""}

        client = _HalfClient(devices=[_device("D1", "客厅"), _device("D2", "卧室")])
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root)})
        plugin.save_data("token", _token())
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": [_device("D1", "客厅"), _device("D2", "卧室")]}
        plugin._save_groups([{"name": "全屋", "device_ids": ["D1", "D2"]}])
        out = plugin._play_file(str(self.track))
        self.assertTrue(out.ok, "主音箱播上了就不能算失败")
        self.assertIn("卧室：设备离线", out.message)

    def test_ungrouped_device_is_single(self) -> None:
        plugin, client = self._plugin()
        out = plugin._control("stop")
        self.assertTrue(out.ok)
        self.assertNotIn("已同步", out.message)
        self.assertEqual([call for call in client.calls if call[0] == "play_operation"],
                         [("play_operation", "D1", "stop")])

    def test_status_exposes_groups(self) -> None:
        plugin, _client = self._plugin()
        plugin.api_group_save(x.schemas.GroupIn(name="全屋", device_ids=["D1", "D2"]))
        status = plugin._status()
        self.assertEqual(status.extra["groups"][0]["device_ids"], ["D1", "D2"])
        self.assertEqual(status.extra["default_group"], "全屋")


class LyricTest(unittest.TestCase):
    """LRC 解析与库内取词：侧车优先、编码不假定 UTF-8、库外路径一律拒绝。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve() / "music"
        self.root.mkdir()
        self.track = self.root / "01 - 稻香.mp3"
        self.track.write_bytes(b"L" * 32)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_parse_lrc_orders_and_drops_meta(self) -> None:
        rows = lyrics.parse_lrc(
            "[ti:稻香]\n[ar:周杰伦]\n[00:12.50]对一个甜蜜的地方\n"
            "[01:03.20]分析问题\n[00:05]空行前的\n\n[00:09.900]转折")
        self.assertEqual([row["time"] for row in rows], [5.0, 9.9, 12.5, 63.2])
        self.assertEqual(rows[0]["text"], "空行前的")
        self.assertNotIn("周杰伦", " ".join(row["text"] for row in rows))

    def test_parse_lrc_without_timestamps(self) -> None:
        rows = lyrics.parse_lrc("第一段\n第二段")
        self.assertEqual([row["time"] for row in rows], [-1.0, -1.0])
        self.assertEqual(rows[1]["text"], "第二段")

    def test_decode_gbk_sidecar(self) -> None:
        (self.root / "01 - 稻香.lrc").write_bytes(
            "[00:01.00]对一个甜蜜的地方".encode("gb18030"))
        data, reason = lyrics.load_lyric(self.track, [self.root])
        self.assertEqual(reason, "")
        self.assertEqual(data["source"], "sidecar")
        self.assertEqual(data["lines"][0]["text"], "对一个甜蜜的地方")
        self.assertTrue(data["has_time"])

    def test_plain_txt_has_no_time(self) -> None:
        (self.root / "01 - 稻香.txt").write_text("词：张三\n曲：李四", encoding="utf-8")
        data, _reason = lyrics.load_lyric(self.track, [self.root])
        self.assertFalse(data["has_time"])
        self.assertEqual(data["lines"][0]["text"], "词：张三")

    def test_sidecar_outside_library_is_ignored(self) -> None:
        outside = Path(self._tmp.name).resolve() / "evil.lrc"
        outside.write_text("[00:01.00]越界", encoding="utf-8")
        link = self.root / "01 - 稻香.lrc"
        try:
            link.symlink_to(outside)
        except OSError:  # pragma: no cover - 某些文件系统不允许软链
            self.skipTest("文件系统不支持符号链接")
        data, reason = lyrics.load_lyric(self.track, [self.root])
        self.assertEqual(data["lines"], [], "软链到库外的侧车不能读")
        self.assertEqual(reason, "not_found")

    def test_active_index_follows_position(self) -> None:
        rows = lyrics.parse_lrc("[00:05.00]一\n[00:10.00]二\n[01:00.00]三")
        self.assertEqual(lyrics.active_index(rows, 0), -1)
        self.assertEqual(lyrics.active_index(rows, 7), 0)
        self.assertEqual(lyrics.active_index(rows, 61), 2)
        self.assertEqual(lyrics.active_index(rows, None), -1)
        self.assertEqual(lyrics.active_index([{"time": -1.0, "text": "纯文本"}], 12), -1)

    def test_endpoint_rejects_outside_and_reports_missing(self) -> None:
        client = _FakeClient(devices=[_device()])
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root)})
        plugin.save_data("token", _token())
        plugin._client = lambda: (client, "")
        out = plugin.api_lyric(path=str(Path(self.track).parent.parent / "etc" / "x.mp3"))
        self.assertFalse(out.ok)
        out = plugin.api_lyric(path=str(self.track))
        self.assertFalse(out.ok)
        self.assertIn("没有歌词", out.message)
        (self.root / "01 - 稻香.lrc").write_text("[00:02.00]词在这里", encoding="utf-8")
        out = plugin.api_lyric(path=str(self.track))
        self.assertTrue(out.ok)
        self.assertEqual(out.source, "sidecar")
        self.assertEqual(out.lines[0].text, "词在这里")
        bad = plugin.api_lyric(path="")
        self.assertFalse(bad.ok)


class PluginTaskTest(unittest.TestCase):
    """定时任务：入参形状、到点判定（周日=0、分钟去重）、七种动作与分组带动的去重。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        (self.root / "周杰伦" / "范特西").mkdir(parents=True)
        for name in ("01 - 周杰伦 - 稻香.mp3", "02 - 周杰伦 - 双截棍.mp3",
                     "03 - 周杰伦 - 朴素.mp3"):
            (self.root / "周杰伦" / "范特西" / name).write_bytes(b"T" * 64)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _plugin(self, devices=None, **client_kwargs):
        devices = devices or [_device("D1", "客厅"), _device("D2", "卧室"), _device("D3", "书房")]
        client = _FakeClient(devices=devices, **client_kwargs)
        plugin = x.XiaoaiSpeaker()
        plugin.init_plugin({**media_defaults, "music_dirs": str(self.root)})
        plugin.save_data("token", _token())
        plugin._client = lambda: (client, "")
        plugin.__dict__["_devices_cache"] = {"at": time.time(), "fetched_at": "t",
                                             "devices": list(devices)}
        plugin._library().collect([self.root], plugin._exts(), 200)
        return plugin, client

    @staticmethod
    def _task(**over) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "name": "早上关机", "enabled": True, "action": "stop",
            "schedule": {"type": "weekly", "time": "07:00", "weekdays": [1, 2, 3]},
            "target": {}, "params": {}}
        row.update(over)
        return row

    def _saved(self, plugin, **over) -> str:
        out = plugin.api_task_save(x.schemas.TaskIn(**self._task(**over)))
        self.assertTrue(out.ok, out.message)
        return out.items[0]["id"]

    def _run(self, plugin, **over) -> List[Dict[str, Any]]:
        return plugin._run_task(plugin._clean_task(self._task(**over)))

    def test_clean_task_rejects_bad_shapes(self) -> None:
        plugin, _client = self._plugin()
        clean = plugin._clean_task
        self.assertIsNone(clean(self._task(schedule={"type": "weekly", "time": "7:00",
                                                      "weekdays": [1]})), "时间要写成补零的 HH:MM")
        self.assertIsNone(clean(self._task(action="eject")))
        self.assertIsNone(clean(self._task(name="  ")))
        self.assertIsNone(clean(self._task(schedule={"type": "weekly", "time": "07:00",
                                                      "weekdays": []})),
                          "一个周几都没勾的任务永远不会触发，按不合规处理")
        self.assertIsNone(clean(self._task(schedule={"type": "monthly", "time": "07:00",
                                                      "monthdays": [32]})))
        row = clean(self._task(schedule={"type": "monthly", "time": "07:00",
                                         "monthdays": [31, 1, 1, 99, "x"]}))
        self.assertEqual(row["schedule"]["monthdays"], [1, 31])
        self.assertEqual(row["schedule"]["type"], "monthly")

    def test_save_update_remove_and_rule_text(self) -> None:
        plugin, _client = self._plugin()
        out = plugin.api_task_save(x.schemas.TaskIn(**self._task()))
        self.assertTrue(out.ok)
        self.assertEqual(out.items[0]["rule"], "每周一、周二、周三 07:00")
        self.assertEqual(out.items[0]["action_text"], "停止播放")
        task_id = out.items[0]["id"]
        created = out.items[0]["created_at"]
        out = plugin.api_task_save(x.schemas.TaskIn(**self._task(id=task_id, name="改个名")))
        self.assertEqual([item["name"] for item in out.items], ["改个名"], "同 id 是改不是加")
        self.assertEqual(out.items[0]["created_at"], created)
        self.assertFalse(plugin.api_task_save(x.schemas.TaskIn(id="没有这条", remove=True)).ok)
        out = plugin.api_task_save(x.schemas.TaskIn(id=task_id, remove=True))
        self.assertTrue(out.ok)
        self.assertEqual(out.items, [])

    def test_play_task_needs_a_playlist(self) -> None:
        plugin, _client = self._plugin()
        out = plugin.api_task_save(x.schemas.TaskIn(**self._task(action="play_playlist")))
        self.assertFalse(out.ok)
        out = plugin.api_task_save(x.schemas.TaskIn(
            **self._task(action="play_playlist", params={"playlist_name": "范特西"})))
        self.assertTrue(out.ok)
        self.assertFalse(plugin.api_task_save(x.schemas.TaskIn(
            **self._task(action="play_playlist", params={"playlist_id": 0}))).ok,
            "playlist_id 为 0 等于没选歌单")
        self.assertFalse(plugin.api_task_save(x.schemas.TaskIn(
            **self._task(action="play_playlist", params={"playlist_id": "0",
                                                         "playlist_name": "  "}))).ok)

    def test_task_count_is_capped(self) -> None:
        plugin, _client = self._plugin()
        plugin._save_tasks([self._task(name=f"任务{i}", id=f"t{i}") for i in range(x._TASK_MAX)])
        self.assertEqual(len(plugin._tasks()), x._TASK_MAX)
        out = plugin.api_task_save(x.schemas.TaskIn(**self._task(name="第 51 条")))
        self.assertFalse(out.ok)
        self.assertIn("最多", out.message)
        # 改已有的条数不受上限影响
        self.assertTrue(plugin.api_task_save(x.schemas.TaskIn(**self._task(id="t0", name="还是这条"))).ok)

    def test_schedule_hit_uses_sunday_as_zero(self) -> None:
        hit = x.XiaoaiSpeaker._schedule_hit
        sunday = datetime(2026, 3, 1, 7, 0)
        self.assertEqual(sunday.isoweekday() % 7, 0, "周日要判成 0，与前端勾选的编号同源")
        self.assertTrue(hit({"type": "weekly", "time": "07:00", "weekdays": [0]}, sunday))
        self.assertFalse(hit({"type": "weekly", "time": "07:00", "weekdays": [1]}, sunday))
        self.assertFalse(hit({"type": "weekly", "time": "07:01", "weekdays": [0]}, sunday),
                         "分钟不对就不该触发")
        self.assertTrue(hit({"type": "monthly", "time": "07:00", "monthdays": [1]}, sunday))
        self.assertFalse(hit({"type": "monthly", "time": "07:00", "monthdays": [2]}, sunday))

    def test_tick_fires_once_per_minute(self) -> None:
        plugin, _client = self._plugin()
        now = datetime.now()
        self._saved(plugin, action="stop",
                    schedule={"type": "weekly", "time": now.strftime("%H:%M"),
                              "weekdays": [now.isoweekday() % 7]})
        plugin._tick_tasks()
        self.assertEqual(len(plugin._task_logs()), 1)
        minute = plugin.__dict__["_task_minute"]
        self.assertEqual(minute, now.strftime("%Y-%m-%d %H:%M"))
        plugin._tick_tasks()
        self.assertEqual(len(plugin._task_logs()), 1, "同一分钟再醒一次不能双发")
        plugin._save_tasks([{**row, "enabled": False} for row in plugin._tasks()])
        plugin.__dict__["_task_minute"] = ""
        plugin._tick_tasks()
        self.assertEqual(len(plugin._task_logs()), 1, "停用的任务到点也不跑")

    def test_device_actions(self) -> None:
        plugin, client = self._plugin()
        logs = self._run(plugin, action="stop")
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertIn(("play_operation", "D1", "stop"), client.calls)
        self.assertEqual(logs[0]["device"], "客厅")
        logs = self._run(plugin, action="set_volume", params={"volume": 45})
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertIn(("set_volume", "D1", 45), client.calls)
        self.assertFalse(self._run(plugin, action="set_volume")[0]["success"], "没有音量值要失败")
        logs = self._run(plugin, action="set_play_mode", params={"play_mode": "random"})
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertEqual(plugin._queue_state()["mode"], "random")
        self.assertFalse(self._run(plugin, action="set_play_mode")[0]["success"])

    def test_playlist_actions(self) -> None:
        plugin, client = self._plugin()
        logs = self._run(plugin, action="play_playlist", params={"playlist_name": "范特西"})
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertEqual(plugin._queue_state()["index"], 0)
        self.assertEqual(len(plugin._queue_state()["items"]), 3)
        self.assertEqual([call[1] for call in client.calls if call[0] == "play"], ["D1"])
        logs = self._run(plugin, action="play_playlist_from",
                         params={"playlist_name": "范特西", "song_name": "朴素"})
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertEqual(plugin._queue_state()["index"], 2, "从指定的那首开始")
        logs = self._run(plugin, action="play_playlist",
                         params={"playlist_name": "没有这个歌单"})
        self.assertFalse(logs[0]["success"], "歌单不在索引里要留下失败日志")
        self.assertIn("没有这个歌单", logs[0]["message"])

    def test_play_task_extras(self) -> None:
        plugin, client = self._plugin()
        logs = self._run(plugin, action="play_playlist",
                         params={"playlist_name": "范特西", "play_mode": "loop",
                                 "volume": 30, "stop_after_minutes": 25})
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertIn("音量 30", logs[0]["message"])
        self.assertIn("25 分钟后停", logs[0]["message"])
        self.assertEqual(plugin._queue_state()["mode"], "loop")
        self.assertIn(("set_volume", "D1", 30), client.calls)
        self.assertTrue(plugin._timer().state()["stop_at"])
        logs = self._run(plugin, action="play_playlist",
                         params={"playlist_name": "范特西", "start": "random"})
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertIn("随机起始", logs[0]["message"])

    def test_targets_all_and_explicit(self) -> None:
        plugin, client = self._plugin()
        logs = self._run(plugin, action="stop", target={"all": True})
        self.assertEqual(len(logs), 3, "没分组时三台各一条日志")
        self.assertEqual(sorted(call[1] for call in client.calls if call[0] == "play_operation"),
                         ["D1", "D2", "D3"])
        client.calls.clear()
        logs = self._run(plugin, action="stop", target={"device_ids": ["D2", "D9"]})
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["device"], "卧室", "认不出的设备直接跳过，不影响其余目标")

    def test_group_targets_do_not_double_fire(self) -> None:
        plugin, client = self._plugin()
        plugin.api_group_save(x.schemas.GroupIn(name="全屋", device_ids=["D1", "D2", "D3"]))
        logs = self._run(plugin, action="stop", target={"device_ids": ["D1", "D2", "D3"]})
        self.assertEqual(len(logs), 1, "D1 那一条已经带动全组，同组成员不再各自下发")
        self.assertEqual(sorted(call[1] for call in client.calls if call[0] == "play_operation"),
                         ["D1", "D2", "D3"])

    def test_monitor_actions(self) -> None:
        plugin, _client = self._plugin()
        logs = self._run(plugin, action="enable_monitor")
        self.assertTrue(logs[0]["success"], logs[0]["message"])
        self.assertTrue(plugin._voice_enabled)
        self.assertTrue(plugin._configs[-1]["voice_enabled"], "要写回配置，否则重启又变回关")
        logs = self._run(plugin, action="enable_monitor")
        self.assertIn("已经是开的", logs[0]["message"])
        self.assertTrue(self._run(plugin, action="disable_monitor")[0]["success"])
        self.assertFalse(plugin._voice_enabled)

    def test_run_endpoint_and_status(self) -> None:
        plugin, _client = self._plugin()
        task_id = self._saved(plugin, action="stop")
        run = plugin.api_task_run(x.schemas.TaskRunIn(id=task_id))
        self.assertTrue(run.ok)
        self.assertEqual(run.items[0]["task_id"], task_id)
        self.assertFalse(plugin.api_task_run(x.schemas.TaskRunIn(id="没有")).ok)
        plugin.api_task_run(x.schemas.TaskRunIn(id=task_id))
        logs = plugin.api_task_logs().items
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["message"], logs[1]["message"], "内容同两条，只是顺序新的在前")
        status = plugin._status()
        self.assertEqual(status.extra["tasks"][0]["name"], "早上关机")
        self.assertEqual(status.extra["task_logs"][0]["task_name"], "早上关机")

    def test_logs_are_trimmed(self) -> None:
        plugin, _client = self._plugin()
        plugin._append_task_logs([{"task_id": f"t{i}", "success": True} for i in range(300)])
        self.assertEqual(len(plugin._task_logs()), x._TASK_LOG_KEEP)
        self.assertEqual(plugin._task_logs()[-1]["task_id"], "t299", "留下的是最近的")
        self.assertEqual(len(plugin._task_logs(10)), 10)


class LoginMethodTest(unittest.TestCase):
    """三种登录方式（账密/扫码/Cookie 票据）里最容易做错的那几处。

    扫码的坑集中在「长轮询超时不是失败」「票据要沿用扫码那次的 deviceId」；
    Cookie 只收 userId 与 passToken 两项，坑集中在「拿 passToken 现换一张活票」，
    浏览器里那张 serviceToken 多半已经死了，所以不再接受它作为兜底。
    还有一种看不见的坑：deviceId 每次登录都换，小米就一直把你当新设备。
    """

    @staticmethod
    def _handler(lp: List[Optional[_Resp]], micoapi_ok: bool = True):
        """造一个扫码全流程的替身：lp 按顺序给响应，用完即「一直超时」。"""
        queue = list(lp)
        seen: List[str] = []

        def handler(method, url, body):
            seen.append(url or "")
            if "serviceLogin?" in url and "sid=mijia" in url:
                return _Resp(payload={"code": 0, "sid": "mijia", "_sign": "SIGN",
                                      "qs": "http://qs", "callback": "http://cb"},
                             prefixed=True)
            if "longPolling/loginUrl" in url:
                return _Resp(payload={"code": 0,
                                      "qr": "https://account.xiaomi.com/pass/qr/login?ticket=T",
                                      "loginUrl": "https://ak.account.xiaomi.com/longPolling/login?ticket=T",
                                      "lp": "https://ak.lp.account.xiaomi.com/lp/s?k=K",
                                      "qrTips": "可用小米手机扫码", "timeout": 300},
                             prefixed=True)
            if "/pass/qr/login" in url:
                return _Resp(status=200, content=b"QRPNG", headers={"Content-Type": "image/png"})
            if "/lp/s" in url:
                return queue.pop(0) if queue else None
            if "serviceLogin?" in url and "sid=micoapi" in url:
                if not micoapi_ok:
                    return _Resp(payload={"code": 7, "desc": "passToken 已失效"}, prefixed=True)
                return _Resp(payload={"code": 0, "sid": "micoapi", "location": "/pass/rs?sid=micoapi",
                                      "ssecurity": "SS", "userId": 77, "nonce": 9},
                             prefixed=True)
            return _Resp(status=302, headers={"Location": "https://api2.mina.mi.com/next",
                                              "Set-Cookie": "serviceToken=QRTOKEN; Path=/"})

        return handler, seen

    def test_qr_login_exchanges_pass_token_for_service_token(self) -> None:
        qr = mina.XiaoaiQrLogin(device_id="QRDEVICE000000")
        handler, _ = self._handler([
            None,                                                   # lp 超时 = 还没人扫
            _Resp(payload={"code": 0}, prefixed=True),               # 回了包但没票据 = 还没确认
            _Resp(payload={"code": 0, "passToken": "PT", "userId": 77,
                           "cUserId": "cu"}, prefixed=True),
        ])
        with _NetworkPatch(handler):
            ok, err = qr.start()
            self.assertTrue(ok, err)
            out = qr.poll()
            self.assertEqual(out[0], mina.LOGIN_QR_WAITING)
            self.assertEqual(qr.poll()[0], mina.LOGIN_QR_WAITING)
            state, token, message = qr.poll()
        self.assertEqual(state, mina.LOGIN_SUCCESS, message)
        self.assertEqual(token["services"][mina.MINA_SID]["service_token"], "QRTOKEN")
        self.assertEqual(token["user_id"], "77")
        self.assertEqual(token["login_method"], "qrcode")
        # deviceId 必须还是扫码那一次的：换掉等于又造出一台「新设备」
        self.assertEqual(token["device_id"], "QRDEVICE000000")

    def test_qr_start_returns_image_tips_and_countdown(self) -> None:
        qr = mina.XiaoaiQrLogin()
        with _NetworkPatch(self._handler([])[0]):
            ok, err = qr.start()
            # 图片也要在替身还在场时取：image() 走的是同一个网络入口
            image = qr.image()
        self.assertTrue(ok, err)
        self.assertTrue(image.startswith("data:image/png;base64,"), image[:32])
        self.assertEqual(qr.tips, "可用小米手机扫码")
        self.assertEqual(qr.seconds_left(), 300)

    def test_qr_poll_before_start_is_a_clear_failure(self) -> None:
        qr = mina.XiaoaiQrLogin()
        state, token, message = qr.poll()
        self.assertEqual(state, mina.LOGIN_FAILED)
        self.assertIsNone(token)
        self.assertIn("获取二维码", message)

    def test_qr_second_nonzero_code_expires_the_session(self) -> None:
        qr = mina.XiaoaiQrLogin()
        lp = [_Resp(payload={"code": -1, "desc": "reconnect"}, prefixed=True),
              _Resp(payload={"code": -1, "desc": "expired"}, prefixed=True)]
        with _NetworkPatch(self._handler(lp)[0]):
            qr.start()
            self.assertEqual(qr.poll()[0], mina.LOGIN_QR_WAITING, "单次非零码是长轮询的抖动")
            self.assertEqual(qr.poll()[0], mina.LOGIN_QR_EXPIRED)
            self.assertEqual(qr.poll()[0], mina.LOGIN_QR_EXPIRED)

    def test_qr_expired_before_poll_makes_no_request(self) -> None:
        qr = mina.XiaoaiQrLogin()
        called: List[str] = []

        def handler(method, url, body):
            called.append(url or "")
            return _Resp(payload={"code": 0}, prefixed=True)

        with _NetworkPatch(self._handler([])[0]):
            qr.start()
        qr._expires_at = time.time() - 1
        with _NetworkPatch(handler):
            state, _token, message = qr.poll()
        self.assertEqual(state, mina.LOGIN_QR_EXPIRED)
        self.assertIn("过期", message)
        self.assertEqual(called, [], "过期了就不该再去问小米")

    def test_qr_403_on_poll_is_expired_not_failed(self) -> None:
        qr = mina.XiaoaiQrLogin()
        with _NetworkPatch(self._handler([_Resp(status=403)])[0]):
            qr.start()
            self.assertEqual(qr.poll()[0], mina.LOGIN_QR_EXPIRED)

    def test_qr_start_without_login_url_params_fails_cleanly(self) -> None:
        def handler(method, url, body):
            if "serviceLogin?" in url:
                return _Resp(payload={"code": 0}, prefixed=True)
            return _Resp(payload={"code": 0}, prefixed=True)

        with _NetworkPatch(handler):
            ok, message = mina.XiaoaiQrLogin().start()
        self.assertFalse(ok)
        self.assertIn("签名", message)

    def test_exchange_pass_token_reports_micoapi_refusal(self) -> None:
        qr = mina.XiaoaiQrLogin()
        lp = [_Resp(payload={"code": 0, "passToken": "PT", "userId": 77}, prefixed=True)]
        with _NetworkPatch(self._handler(lp, micoapi_ok=False)[0]):
            qr.start()
            state, token, message = qr.poll()
        self.assertEqual(state, mina.LOGIN_FAILED)
        self.assertIsNone(token)
        self.assertIn("passToken 已失效", message)

    def test_manual_cookie_exchanges_pass_token_for_ticket(self) -> None:
        plugin = _plugin()
        handler, seen = self._handler([])
        with _NetworkPatch(handler):
            out = plugin._login_manual("77", "PT")
        self.assertEqual(out.state, mina.LOGIN_SUCCESS, out.message)
        saved = plugin.get_data("token")
        self.assertEqual(saved["services"][mina.MINA_SID]["service_token"], "QRTOKEN")
        self.assertEqual(saved["login_method"], "manual")
        self.assertTrue(any("sid=micoapi" in u for u in seen), "要用 passToken 现换一张活票")

    def test_pass_token_retry_switches_client_profile(self) -> None:
        """小米按请求自称的客户端决定认不认这张票：先按来路问，被拒再换另一种标识。

        浏览器 Cookie 里那张票来自 iOS passport 的网页会话，账密/扫码 mint 的来自
        Android 米家 —— 认错标识只回 70016，不重试就是「明明有票却换不到」。
        """
        original = mina.XiaoaiAuth._service_login
        calls: List[str] = []
        attempts = {"n": 0}

        def fake(self):
            calls.append(self.sdk_version)
            attempts["n"] += 1
            if attempts["n"] == 1:
                return (mina.LOGIN_FAILED, None,
                        "静默续期失败：code=70016, desc=登录验证失败")
            return mina.LOGIN_SUCCESS, {"services": {mina.MINA_SID: {}}}, "已换到"

        mina.XiaoaiAuth._service_login = fake
        try:
            state, _token, message = mina.exchange_pass_token("PT", "77")
            manual_calls = list(calls)
            calls.clear()
            attempts["n"] = 0
            mina.exchange_pass_token("PT", "77", login_method="qrcode")
            qr_calls = list(calls)
        finally:
            mina.XiaoaiAuth._service_login = original
        self.assertEqual(state, mina.LOGIN_SUCCESS, message)
        self.assertEqual(manual_calls,
                         [mina.PASSPORT_SDK_VERSION, mina.DEFAULT_SDK_VERSION],
                         "手工填的票先按网页来路问，被拒再补一次 Android 米家")
        self.assertEqual(qr_calls,
                         [mina.DEFAULT_SDK_VERSION, mina.PASSPORT_SDK_VERSION],
                         "扫码来的票来路是 Android 米家，先按它问")

    def test_manual_cookie_rejects_a_whole_cookie_blob(self) -> None:
        """只认两项：整串 cookie 粘进来要当场说清，而不是换票换出个莫名其妙的失败。"""
        plugin = _plugin()
        out = plugin._login_manual("77", "userId=77; passToken=PT; serviceToken=LIVE")
        self.assertEqual(out.state, "failed")
        self.assertIn("整串", out.message)

    def test_manual_cookie_missing_either_field_says_so(self) -> None:
        plugin = _plugin()
        for user_id, pass_token in (("77", ""), ("", "PT")):
            out = plugin._login_manual(user_id, pass_token)
            self.assertEqual(out.state, "failed")
            self.assertIn("userId", out.message)
            self.assertIn("passToken", out.message)

    def test_resend_restarts_the_login_session(self) -> None:
        """「重发」必须重新发起一次：小米那次会话的码只发一遍，续跑拿不到新码。"""
        plugin = _plugin()
        started: List[bool] = []

        class _Spy:
            username = "me@example.com"

            def __init__(self, *args, **kwargs):
                pass

            def login(self):
                started.append(True)
                return mina.LOGIN_NEED_VERIFY, None, "小米要求短信二次校验"

            def submit_verify_code(self, code):
                return mina.LOGIN_NEED_VERIFY, None, "验证码不对"

        original = x.XiaoaiAuth
        x.XiaoaiAuth = _Spy
        try:
            first = plugin.api_login(
                x.schemas.LoginIn(username="me@example.com", password="pass"))
            self.assertEqual(first.state, mina.LOGIN_NEED_VERIFY)
            # 原样带码提交是续跑那次会话，不会再发起一次登录
            plugin.api_login(x.schemas.LoginIn(code="123456"))
            self.assertEqual(len(started), 1)
            resent = plugin.api_login(x.schemas.LoginIn(resend=True))
            self.assertEqual(resent.state, mina.LOGIN_NEED_VERIFY)
            self.assertEqual(len(started), 2, "重发要重新发起一次登录才会拿到新码")
        finally:
            x.XiaoaiAuth = original

    def test_device_id_stays_the_same_across_logins(self) -> None:
        plugin = _plugin()
        first = plugin._device_id()
        self.assertEqual(plugin._device_id(), first)
        self.assertEqual(plugin.get_data("device_id"), first)
        self.assertEqual(len(first), 16)

    def test_password_login_uses_the_stored_device_id(self) -> None:
        plugin = _plugin()
        stored = plugin._device_id()
        captured: Dict[str, Any] = {}
        original = x.XiaoaiAuth

        class _Spy:
            username = ""

            def __init__(self, username, password, device_id="", login_method="password"):
                captured["device_id"] = device_id
                captured["login_method"] = login_method

            def login(self):
                return mina.LOGIN_FAILED, None, "不试了"

        x.XiaoaiAuth = _Spy
        try:
            plugin.api_login(x.schemas.LoginIn(username="me@example.com", password="pass"))
        finally:
            x.XiaoaiAuth = original
        self.assertEqual(captured["device_id"], stored)

    def test_walk_location_takes_the_last_hop_service_token(self) -> None:
        """跳转链末端自己下的那颗才算数：cookie 表里可能留着别的服务的同名票据。"""
        hops = [
            _Resp(status=302, headers={"Location": "https://api2.mina.mi.com/second",
                                       "Set-Cookie": "serviceToken=FRESH; Path=/"}),
            _Resp(status=200, text="done"),
        ]

        def handler(method, url, body):
            return hops.pop(0) if hops else _Resp(status=200, text="")

        auth = mina.XiaoaiAuth("me@example.com", "pass", device_id="D")
        auth.cookies["serviceToken"] = "STALE_OTHER_SID"
        with _NetworkPatch(handler):
            token, detail = auth._walk_location("https://account.xiaomi.com/pass/rs")
        self.assertEqual(token, "FRESH", detail)

    def test_second_factor_sends_the_code_instead_of_waiting_for_it(self) -> None:
        """notificationUrl 只是「这步校验在哪儿做」的地址，码要插件点名去发。

        少任何一步，页面就停在「请查收验证码」而用户永远等不到那条短信 ——
        这一步在真机上最容易漏，因为小米不报错。
        """
        seen: List[str] = []

        def handler(method, url, body):
            seen.append(f"{method} {url}")
            if "serviceLogin?" in url:
                return _Resp(payload={"code": 0, "sid": "micoapi", "_sign": "SIGN",
                                      "qs": "http://qs", "callback": "http://cb"})
            if "serviceLoginAuth2" in url:
                return _Resp(payload={"code": 0, "notificationUrl":
                                      "/fe/service/identity/authStart?context=CTX&sid=micoapi"},
                             prefixed=True)
            if "/identity/list" in url:
                return _Resp(payload={"code": 0, "flag": 4}, prefixed=True)
            if "/identity/auth/verifyPhone" in url:
                return _Resp(payload={"code": 0}, prefixed=True)
            if "/identity/auth/sendPhoneTicket" in url:
                return _Resp(payload={"code": 0}, prefixed=True)
            return _Resp(status=200, text="")

        with _NetworkPatch(handler):
            auth = mina.XiaoaiAuth("me@example.com", "pass")
            state, _, message = auth.login()
        self.assertEqual(state, mina.LOGIN_NEED_VERIFY)
        self.assertIn("已发往账号绑定的手机短信", message)
        self.assertTrue(auth._identity_open)
        self.assertEqual(auth._verify_flag, 4)
        joined = "\n".join(seen)
        for want in (
                "GET https://account.xiaomi.com/fe/service/identity/authStart?context=CTX&sid=micoapi",
                "GET https://account.xiaomi.com/identity/list?sid=micoapi"
                "&supportedMask=0&_locale=zh_CN&context=CTX",
                "GET https://account.xiaomi.com/identity/auth/verifyPhone?_flag=4&_json=true",
                "POST https://account.xiaomi.com/identity/auth/sendPhoneTicket"):
            self.assertIn(want, joined, f"缺了这一步就不会真的发码: {want}")

    def test_email_second_factor_skips_the_sms_endpoint(self) -> None:
        seen: List[str] = []

        def handler(method, url, body):
            seen.append(f"{method} {url}")
            if "serviceLogin?" in url:
                return _Resp(payload={"code": 0, "sid": "micoapi", "_sign": "SIGN",
                                      "qs": "http://qs", "callback": "http://cb"})
            if "serviceLoginAuth2" in url:
                return _Resp(payload={"code": 0, "notificationUrl":
                                      "/fe/service/identity/authStart?context=CTX"}, prefixed=True)
            if "/identity/list" in url:
                return _Resp(payload={"code": 0, "flag": 8}, prefixed=True)
            return _Resp(payload={"code": 0}, prefixed=True)

        with _NetworkPatch(handler):
            auth = mina.XiaoaiAuth("me@example.com", "pass")
            state, _, message = auth.login()
        self.assertEqual(state, mina.LOGIN_NEED_VERIFY)
        self.assertIn("邮箱", message)
        self.assertIn("GET https://account.xiaomi.com/identity/auth/verifyEmail?_flag=8&_json=true",
                      "\n".join(seen))
        self.assertNotIn("sendPhoneTicket", "\n".join(seen))

    def test_send_failure_says_the_code_was_not_dispatched(self) -> None:
        """发码被拒不能说「已发往手机」—— 那会让人一直盯着短信箱等。"""
        def handler(method, url, body):
            if "serviceLogin?" in url:
                return _Resp(payload={"code": 0, "sid": "micoapi", "_sign": "SIGN",
                                      "qs": "http://qs", "callback": "http://cb"})
            if "serviceLoginAuth2" in url:
                return _Resp(payload={"code": 0, "notificationUrl":
                                      "/fe/service/identity/authStart?context=CTX"}, prefixed=True)
            if "/identity/list" in url:
                return _Resp(payload={"code": 0, "flag": 4}, prefixed=True)
            if "/identity/auth/sendPhoneTicket" in url:
                return _Resp(payload={"code": 700011, "description": "请求过于频繁"}, prefixed=True)
            return _Resp(payload={"code": 0}, prefixed=True)

        with _NetworkPatch(handler):
            auth = mina.XiaoaiAuth("me@example.com", "pass")
            state, _, message = auth.login()
        self.assertEqual(state, mina.LOGIN_NEED_VERIFY)
        self.assertIn("没能发出去", message)
        self.assertIn("700011", message)

    def test_second_factor_resume_reopens_the_session(self) -> None:
        """插件重启后直接填码续跑：会话不在了要先补建立，否则填对码也过不去。"""
        seen: List[str] = []

        def handler(method, url, body):
            seen.append(f"{method} {url} {body or ''}")
            if "/identity/list" in url:
                return _Resp(payload={"code": 0, "flag": 8}, prefixed=True)
            return _Resp(payload={"code": 0, "location": "/pass/rs"}, prefixed=True)

        auth = mina.XiaoaiAuth("me@example.com", "pass", device_id="D")
        auth._verify_url = ("https://account.xiaomi.com/fe/service/identity/authStart"
                            "?targetUrl=x&_js_version=1")
        with _NetworkPatch(handler):
            location = auth._verify_ticket("123456")
        self.assertTrue(location)
        self.assertEqual(seen[0].split("?")[0],
                         "GET https://account.xiaomi.com/fe/service/identity/authStart")
        self.assertIn("GET https://account.xiaomi.com/identity/list?targetUrl=x", seen[1])
        # 方式以 list 给的为准，而不是从 URL 里猜：这里 URL 里没有 email 字样
        self.assertTrue(seen[2].startswith("POST https://account.xiaomi.com/identity/auth/verifyEmail"))
        self.assertIn("_flag=8", seen[2])
        self.assertIn("ticket=123456", seen[2])

    def test_verify_code_continues_past_the_confirm_page(self) -> None:
        """小米可能停在「确认手机号」页，它的 confirmUrl 才是发出 passToken 的那一跳。"""
        seen: List[str] = []

        def handler(method, url, body):
            seen.append(url or "")
            if "/identity/auth/verifyPhone" in url:
                return _Resp(payload={"code": 0, "location": "/pass/rs"}, prefixed=True)
            if url and url.endswith("/pass/end"):
                return _Resp(status=200, text="done")
            return _Resp(status=302,
                         headers={"Location": "/fe/service/verifyPhone?confirmUrl=%2Fpass%2Fend"})

        auth = mina.XiaoaiAuth("me@example.com", "pass", device_id="D")
        auth._verify_url = "https://account.xiaomi.com/identity/auth/verifyPhone"
        auth._verify_flag = 4
        auth._identity_open = True
        with _NetworkPatch(handler):
            auth.submit_verify_code("123456")
        self.assertIn("https://account.xiaomi.com/pass/end", seen,
                      "confirmUrl 没跟上，passToken 就永远拿不到")

    def test_qrcode_session_refresh_asks_for_scan_not_password(self) -> None:
        auth = mina.XiaoaiAuth.from_token({"user_id": "1", "device_id": "D",
                                           "login_method": "qrcode"})
        state, token, message = auth.refresh()
        self.assertEqual(state, mina.LOGIN_FAILED)
        self.assertIn("扫码", message)
        self.assertNotIn("密码", message)

    def test_login_out_point_uses_the_login_lock_and_reports_state(self) -> None:
        plugin = _plugin()
        plugin.del_data("token")
        handler, _ = self._handler([
            _Resp(payload={"code": 0, "passToken": "PT", "userId": 77}, prefixed=True)])
        with _NetworkPatch(handler):
            started = plugin.api_login_qrcode(x.schemas.QrIn(action="start"))
            self.assertEqual(started.state, mina.LOGIN_QR_WAITING)
            self.assertTrue(started.qr_image.startswith("data:image/png;base64,"))
            self.assertEqual(started.expires_in, 300)
            done = plugin.api_login_qrcode(x.schemas.QrIn(action="poll"))
        self.assertEqual(done.state, mina.LOGIN_SUCCESS, done.message)
        self.assertGreaterEqual(done.device_count, 0)
        self.assertTrue(done.logged_in)
        # 二维码的 loginUrl 是一次性凭证，绝不从接口出去
        self.assertNotIn("loginUrl", started.model_dump_json())
        self.assertNotIn("longPolling/login", started.model_dump_json())

    def test_status_exposes_login_method(self) -> None:
        plugin = _plugin()
        plugin.save_data("token", {**_token(), "login_method": "qrcode"})
        plugin._client = lambda: (_FakeClient(devices=[_device()]), "")
        self.assertEqual(plugin._status(force_devices=True).login_method, "qrcode")


if __name__ == "__main__":
    unittest.main(verbosity=2)
