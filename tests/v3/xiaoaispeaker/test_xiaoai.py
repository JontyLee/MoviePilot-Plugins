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
import threading
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

from unittest import mock  # noqa: E402

sys.path.insert(0, str(_ROOT / "plugins.v3"))
import xiaoaispeaker as x  # noqa: E402
from app.sdk.logging import logger as _logger  # noqa: E402
from xiaoaispeaker import library, lyrics, media, mina  # noqa: E402


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
                 volume=30, asks=None, play_api="player_play_url") -> None:
        self._devices = devices if devices is not None else []
        self._status = status or {"status": 1, "volume": 30, "position": 1000, "duration": 60000}
        self._play_ok = play_ok
        self._play_api = play_api
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
        return {"ok": self._play_ok, "api": self._play_api,
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
    def test_oh2_nested_playback_details_are_normalized(self) -> None:
        client = mina.XiaoaiClient(_token())
        client._ubus = lambda *args: {"code": 0, "data": {"info": json.dumps({
            "status": 2, "volume": 44,
            "play_song_detail": {"audio_id": "official-song", "position": 7863, "duration": 30041},
        })}}
        status = client.get_status("D1")
        self.assertEqual(status["position"], 7863)
        self.assertEqual(status["duration"], 30041)
        self.assertEqual(status.get("audio_id"), "official-song")
        self.assertEqual(status["status"], 2)

    def test_missing_playback_status_is_unknown(self) -> None:
        client = mina.XiaoaiClient(_token())
        client._ubus = lambda *args: {"code": 0, "data": {"info": "{}"}}
        self.assertEqual(client.get_status("D1")["status"], -1)

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

    def _plugin(self, status=None, auto_next=True, play_api="player_play_url", tts_ok=True):
        client = _FakeClient(devices=[_device()], status=status, play_api=play_api,
                             tts_ok=tts_ok)
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

    def test_single_play_replaces_the_previous_playlist(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks[:2], index=0))
        result = plugin.api_play(x.schemas.PlayIn(path=self.tracks[2]))
        self.assertTrue(result.ok)
        self.assertEqual([row.path for row in result.queue.items], [self.tracks[2]])
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        client._status["status"] = 0
        before = len([row for row in client.calls if row[0] == "play"])
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before)

    def test_external_play_disarms_the_library_queue(self) -> None:
        plugin, _ = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        result = plugin.api_play(x.schemas.PlayIn(url="https://audio.example/new.mp3"))
        self.assertTrue(result.ok)
        self.assertEqual(plugin._queue_state()["items"], [])
        self.assertEqual(result.playing.source, "url")

    def test_queue_index_tracks_the_played_file_after_filtering(self) -> None:
        plugin, _ = self._plugin()
        a, b, c = self.tracks
        result = plugin.api_queue(x.schemas.QueueIn(
            paths=[a, str(self.foreign), b, c], index=2))
        self.assertTrue(result.ok)
        self.assertEqual(result.queue.index, 1)
        self.assertEqual(result.queue.items[result.queue.index].path, b)
        self.assertEqual(plugin.get_data("last")["path"], b)

    def test_append_merges_server_queue_without_restarting(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks[:2], index=1))
        last = plugin.get_data("last")
        before = len([row for row in client.calls if row[0] == "play"])
        result = plugin.api_queue(x.schemas.QueueIn(paths=[self.tracks[2]], append=True))
        self.assertTrue(result.ok)
        self.assertEqual([row.path for row in result.queue.items], self.tracks)
        self.assertEqual(result.queue.index, 1)
        self.assertEqual(plugin.get_data("last"), last)
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before)

    def test_append_validates_new_paths_without_needing_a_default_device(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks[:1]))
        plugin._default_device_id = "missing-default"
        result = plugin.api_queue(x.schemas.QueueIn(paths=[self.tracks[1]], append=True))
        self.assertTrue(result.ok)
        self.assertEqual(result.queue.device_id, "D1")
        self.assertFalse(plugin.api_queue(x.schemas.QueueIn(paths=[str(self.foreign)], append=True)).ok)
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), 1)

    def test_append_to_an_empty_queue_starts_first_valid_path(self) -> None:
        plugin, _ = self._plugin()
        result = plugin.api_queue(x.schemas.QueueIn(paths=[str(self.foreign), self.tracks[1]], append=True))
        self.assertTrue(result.ok)
        self.assertEqual(result.playing.path, self.tracks[1])
        self.assertEqual(result.queue.index, 0)

    def test_old_advance_cannot_restore_a_replaced_playlist(self) -> None:
        plugin, _ = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks[:2]))
        reached = threading.Event()
        release = threading.Event()
        replaced = threading.Event()
        original = plugin._playing_out
        errors = []

        def block_old_result(*args, **kwargs):
            if threading.current_thread().name == "old-advance":
                reached.set()
                if not release.wait(2):
                    raise RuntimeError("advance test barrier timed out")
            return original(*args, **kwargs)

        def advance():
            try:
                plugin._advance("next")
            except Exception as exc:
                errors.append(exc)

        def replace():
            try:
                plugin.api_queue(x.schemas.QueueIn(paths=[self.tracks[2]]))
            except Exception as exc:
                errors.append(exc)
            finally:
                replaced.set()

        plugin._playing_out = block_old_result
        old = threading.Thread(target=advance, name="old-advance")
        new = threading.Thread(target=replace)
        old.start()
        self.assertTrue(reached.wait(2))
        new.start()
        try:
            # 在旧回调尚未提交时给新请求机会执行，结束后以实际状态而非线程时序断言。
            replaced.wait(0.1)
        finally:
            release.set()
            old.join(2)
            new.join(2)
        self.assertFalse(old.is_alive() or new.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual([row["path"] for row in plugin._queue_state()["items"]], [self.tracks[2]])
        self.assertEqual(plugin.get_data("last")["path"], self.tracks[2])

    def test_stale_tick_response_cannot_advance_a_new_session(self) -> None:
        plugin, _ = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks[:2]))
        plugin._tick_advance()
        plugin.__dict__["_idle_ticks"] = 1
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        original = plugin._device_status
        responses = 0

        def swapped_status(device_id, force=False):
            nonlocal responses
            responses += 1
            if responses == 1:
                plugin.api_queue(x.schemas.QueueIn(paths=[self.tracks[2], self.tracks[0]]))
                return {"status": 0}
            return original(device_id, force=force)

        plugin._device_status = swapped_status
        plugin._tick_advance()
        self.assertEqual(plugin.get_data("last")["path"], self.tracks[2])
        self.assertEqual(plugin._queue_state()["index"], 0)
        self.assertEqual(plugin.__dict__.get("_idle_ticks"), 0,
                         "旧响应连停止计数也不能带到新播放会话")

    def test_stale_playing_response_cannot_clear_a_new_pause(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        original = plugin._device_status
        responses = 0

        def paused_during_status(device_id, force=False):
            nonlocal responses
            responses += 1
            if responses == 1:
                plugin._control("pause")
                return {"status": 1}
            return original(device_id, force=force)

        plugin._device_status = paused_during_status
        plugin._tick_advance()
        self.assertEqual(plugin._queue_state().get("paused_token"),
                         plugin._queue_token(plugin._queue_state()))

    def test_same_track_replay_rejects_old_playing_response(self) -> None:
        plugin, _ = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        original = plugin._device_status
        responses = 0

        def replayed_during_status(device_id, force=False):
            nonlocal responses
            responses += 1
            if responses == 1:
                plugin._advance("0")
                return {"status": 1}
            return original(device_id, force=force)

        plugin._device_status = replayed_during_status
        plugin._tick_advance()
        self.assertIsNone(plugin.__dict__.get("_advance_seen"))

    def test_unknown_status_breaks_consecutive_stops(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        before = len([row for row in client.calls if row[0] == "play"])
        for status in (0, -1, 0):
            client._status = {"status": status}
            plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before)

    def test_queue_must_belong_to_the_last_pushed_track_and_device(self) -> None:
        for changed in ({"path": self.tracks[2]}, {"device_id": "D2"}):
            with self.subTest(changed=changed):
                plugin, client = self._plugin()
                plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
                last = plugin.get_data("last")
                last.update(started_at=time.time() - 120, **changed)
                plugin.save_data("last", last)
                plugin._tick_advance()
                before = len([row for row in client.calls if row[0] == "play"])
                client._status = {"status": 0}
                plugin._tick_advance()
                plugin._tick_advance()
                self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before)

    def test_announcement_precedes_auto_next_push(self) -> None:
        plugin, client = self._plugin()
        plugin._tts_announce = True
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        client.calls.clear()
        client._status = {"status": 0}
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual([row[0] for row in client.calls if row[0] in ("tts", "play")],
                         ["tts", "play"])

    def test_tts_suspends_auto_next_until_explicit_resume(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        self.assertTrue(plugin.api_tts(x.schemas.TtsIn(text="测试播报")).ok)
        client._status = {"status": 0}
        before = len([row for row in client.calls if row[0] == "play"])
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before)
        self.assertTrue(plugin._control("play").ok)
        self.assertEqual(plugin.get_data("last")["path"], self.tracks[0])
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before + 1)
        self.assertNotIn("suspended", plugin._queue_state())

    def test_rejected_tts_keeps_the_queue_alive(self) -> None:
        """播报被设备拒绝时声道没被占走，不该顺手停用连播。"""
        plugin, client = self._plugin(tts_ok=False)
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        self.assertFalse(plugin.api_tts(x.schemas.TtsIn(text="测试播报")).ok)
        self.assertNotIn("suspended", plugin._queue_state())
        client._status = {"status": 0}
        before = len([row for row in client.calls if row[0] == "play"])
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before + 1)

    def test_pause_and_unknown_do_not_establish_playback(self) -> None:
        for state in (2, -1):
            with self.subTest(state=state):
                plugin, client = self._plugin(status={"status": state})
                plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
                plugin._control("pause")
                token = plugin._queue_token(plugin._queue_state())
                plugin._tick_advance()
                self.assertEqual(plugin._queue_state().get("paused_token"), token)
                self.assertIsNone(plugin.__dict__.get("_advance_seen"))

    def test_official_audio_cannot_resume_the_previous_queue(self) -> None:
        # 现场 OH2 走 play_music：播官方试听时云端回的是另一个 audio_id
        plugin, client = self._plugin(play_api="player_play_music")
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        self.assertEqual(plugin.get_data("last")["audio_id"], mina.DEFAULT_MUSIC_AUDIO_ID)
        client._status.update(status=1, audio_id="455908638179983399")
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        client._status = {"status": 0}
        before = len([row for row in client.calls if row[0] == "play"])
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before)

    def test_play_music_entry_url_is_not_compared_as_foreign(self) -> None:
        """play_music 的条目由 audioID 认定：云端另报一个 url 不该被当成外部音频。"""
        plugin, client = self._plugin(play_api="player_play_music")
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        client._status.update(status=1, audio_id=mina.DEFAULT_MUSIC_AUDIO_ID,
                              stream_url="https://static.mis.example/cloud-item.mp3")
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        client._status = {"status": 0, "audio_id": mina.DEFAULT_MUSIC_AUDIO_ID}
        before = len([row for row in client.calls if row[0] == "play"])
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before + 1)
        self.assertEqual(plugin._queue_state()["index"], 1)

    def test_url_identity_uses_the_signed_link_not_its_punctuation(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        ours = plugin.get_data("last")["url"]
        # 小米会把 `&` 换成空格：签名参数一致就算同一条直链
        client._status.update(status=1, stream_url=ours.replace("?", "?x=1&"))
        plugin._tick_advance()
        self.assertEqual(plugin.__dict__.get("_advance_seen")[0],
                         plugin._queue_token(plugin._queue_state()))
        client._status.update(status=1, stream_url="https://stream.example/other.m4a")
        plugin._tick_advance()
        self.assertTrue(plugin._queue_state().get("suspended"))
        self.assertFalse([row for row in client.calls if row[0] == "play"][1:])

    def test_url_push_without_reported_identity_still_advances(self) -> None:
        """型号不报 audio_id 与 url 时不能凭空认定外部音频，否则连播再也不工作。"""
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        client._status = {"status": 0}
        before = len([row for row in client.calls if row[0] == "play"])
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before + 1)

    def test_external_url_suspends_an_observed_library_track(self) -> None:
        plugin, client = self._plugin()
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick_advance()
        client._status.update(status=1, stream_url="https://audio.example/official.mp3")
        plugin._tick_advance()
        self.assertTrue(plugin._queue_state().get("suspended"))
        self.assertTrue(plugin._control("next").ok)
        self.assertNotIn("suspended", plugin._queue_state())

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
        playing = {"status": 1, "volume": 10, "position": 1000, "duration": 60000}
        plugin, client = self._plugin(status=playing)
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        before = len([c for c in client.calls if c[0] == "play"])
        plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before)
        client._status = stopped
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

    def test_tick_silent_after_restart(self) -> None:
        """模拟 MP 重启后的第一件事：队列和 last 都还是旧状态、设备报 0。

        本进程没有观察到当前曲目在播时，不应把重启后的停止状态当作曲终推进。
        """
        stopped = {"status": 0, "volume": 10, "position": 0, "duration": 0}
        plugin, client = self._plugin(status=stopped)
        plugin._set_queue_state(self.tracks, 0, "D1")
        plugin.save_data("last", {"device_id": "D1", "path": self.tracks[0],
                                  "started_at": time.time() - 600})
        before = len([c for c in client.calls if c[0] == "play"])
        for _ in range(6):
            plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before,
                         "本进程没见过这一首在播，重启后不该盲推")

    def test_tick_skips_plugin_pause(self) -> None:
        """插件自己发的暂停要落标记，巡检见标记就不推进。"""
        playing = {"status": 1, "volume": 10, "position": 1000, "duration": 60000}
        stopped = {"status": 0, "volume": 10, "position": 0, "duration": 0}
        plugin, client = self._plugin(status=playing)
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick()
        self.assertTrue(plugin._control("pause").ok)
        self.assertEqual(plugin._queue_state().get("paused_token"),
                         plugin._queue_token(plugin._queue_state()))
        client._status = stopped
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 600
        plugin.save_data("last", last)
        before = len([c for c in client.calls if c[0] == "play"])
        for _ in range(4):
            plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before,
                         "暂停被当成播完，会把你没听完的那首切掉")
        # 口令恢复播放后连播要能回来
        plugin._control("play")
        self.assertNotIn("paused_token", plugin._queue_state())
        client._status = playing
        plugin._tick()
        client._status = stopped
        plugin._tick()
        plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before + 1,
                         "恢复播放后连播应恢复")

    def test_tick_pause_cleared_by_device_resume(self) -> None:
        """设备侧按键恢复播放插件看不见，巡检见到 status=1 时要把标记擦掉。"""
        playing = {"status": 1, "volume": 10, "position": 1000, "duration": 60000}
        stopped = {"status": 0, "volume": 10, "position": 0, "duration": 0}
        plugin, client = self._plugin(status=playing)
        plugin.api_queue(x.schemas.QueueIn(paths=self.tracks))
        plugin._tick()
        plugin._control("pause")
        client._status = stopped
        plugin._tick()
        client._status = playing
        plugin._tick()
        self.assertNotIn("paused_token", plugin._queue_state(),
                         "设备已经自己播起来了，标记得擦掉")
        client._status = stopped
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 600
        plugin.save_data("last", last)
        before = len([c for c in client.calls if c[0] == "play"])
        plugin._tick()
        plugin._tick()
        self.assertEqual(len([c for c in client.calls if c[0] == "play"]), before + 1,
                         "按键恢复后播完，照样该连播")

    def test_plugin_stop_marks_queue(self) -> None:
        playing = {"status": 1, "volume": 10, "position": 1000, "duration": 60000}
        plugin, _ = self._plugin(status=playing)
        plugin._set_queue_state(self.tracks, 1, "D1")
        token = plugin._queue_token(plugin._queue_state())
        self.assertTrue(plugin._control("stop").ok)
        self.assertEqual(plugin._queue_state().get("paused_token"), token)

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


class LibraryIndexTest(unittest.TestCase):
    """索引自己那两件事：重建要留下一行能 grep 的结果，落盘的缓存不该谁都能读。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name).resolve()
        self.root = base / "music"
        (self.root / "周杰伦" / "范特西").mkdir(parents=True)
        for name in ("01 - 稻香.mp3", "02 - 双截棍.mp3", "03 - 朴素.mp3"):
            (self.root / "周杰伦" / "范特西" / name).write_bytes(b"V" * 64)
        self.cache = base / "library-tags.json.gz"
        self.index = library.LibraryIndex()
        self.index.configure(self.cache)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_rebuild_leaves_one_greppable_line(self) -> None:
        mark = len(_logger.lines)
        # 上限 2 首：第三首没扫进来，页面上只会少一首，日志得说清是被上限截的
        self.assertEqual(self.index.collect([self.root], media.DEFAULT_AUDIO_EXTS, 2), 2)
        line = "\n".join(_logger.lines[mark:])
        self.assertIn("曲库索引重建", line)
        self.assertIn("耗时", line)
        self.assertIn("已到上限 2 首", line)

    def test_cache_file_is_owner_only(self) -> None:
        self.index.collect([self.root], media.DEFAULT_AUDIO_EXTS, 200)
        mode = self.cache.stat().st_mode & 0o777
        self.assertEqual(mode, 0o600, "整库路径清单不该给容器里别的进程读")


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

    def test_listener_primes_the_whole_first_batch(self) -> None:
        """首轮那批要整批对齐，一条都不执行。

        小米回的是按时间升序的最近若干条：旧写法只把第一条（最旧那条）当基线，剩下几条
        全当成刚说的新命令 —— 宿主一重启，音箱就把最近那句「播放…」重唱一遍，紧接着又
        被批里后面那句「暂停/停止」掐掉，界面上正是"重启后先播放、然后再停止"。
        """
        now = int(time.time() * 1000)
        backlog = [
            {"request_id": "b1", "timestamp_ms": now - 300000,
             "question": "我想听稻香", "answer": "好的"},
            {"request_id": "b2", "timestamp_ms": now - 240000,
             "question": "音量调到50", "answer": "好"},
            {"request_id": "b3", "timestamp_ms": now - 60000,
             "question": "暂停播放", "answer": "已暂停"},
        ]
        plugin, client = self._plugin(asks=[])
        self.assertEqual(self._listener_tick(plugin, client, backlog), 0,
                         "首轮那批被执行了，重启后音箱会自己唱起来")
        self.assertFalse([call for call in client.calls
                          if call[0] in ("play", "play_url", "set_volume", "play_operation")],
                         "对齐基线不该发出任何控制命令")
        # 页面仍然看得见这批对话（只是没执行）：诊断时靠它确认监听是活的
        self.assertEqual(len(plugin._listener().recent(20)), 3)
        # 基线对齐到批里最新的一条：之后真说的一句话仍然要执行
        fresher = dict(backlog[2], request_id="b4", timestamp_ms=now + 5000,
                       question="停止播放")
        self.assertEqual(self._listener_tick(plugin, client, backlog + [fresher]), 1)
        self.assertIn(("play_operation", "D1", "stop"), client.calls)

    def test_slow_round_still_waits_a_full_interval(self) -> None:
        """一轮跑得比间隔还久时，下一轮要等满间隔 —— 从本轮**结束**起算，不是从开始。

        从开始起算的话，慢轮跑完「下次时间」已经在过去，宿主下一次 tick 立刻又发一轮：
        配 2 秒实际是贴着打小米，两轮还会重叠成一天几十条 skipped 告警。
        """
        plugin, client = self._plugin(config={"voice_poll": 2}, asks=[])
        listener = plugin._listener()
        clock = {"t": time.time()}

        class _Clock:
            """只替掉 speech 模块里那个 time，其余调用原样转给真的 time。"""

            def time(self) -> float:
                return clock["t"]

            def __getattr__(self, name):
                return getattr(time, name)

        def slow_ask(*args, **kwargs) -> list:
            clock["t"] += 5.0  # 这轮问小米问掉 5 秒，比配置的 2 秒还久
            return []

        client.get_latest_ask = slow_ask
        with mock.patch.object(x._speech, "time", _Clock()):
            listener._next_at = 0.0
            self.assertEqual(listener.tick(), 0)
            self.assertGreaterEqual(listener.status()["next_in"], 2.0,
                                    "慢轮之后不该立刻再来一轮")

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

    def test_listener_success_clears_error_and_backoff(self) -> None:
        plugin, client = self._plugin(asks=None)
        self._listener_tick(plugin, client, None)
        listener = plugin._listener()
        self.assertTrue(listener.status()["error"])
        for _ in range(4):
            self._listener_tick(plugin, client, [])
            self.assertEqual(listener.status()["error"], "")
            self.assertLessEqual(listener.status()["next_in"], 2.1)
        listener._last_error = "stale"
        listener.reset()
        self.assertEqual(listener.status()["error"], "")

    def test_listener_reset_discards_late_responses_and_errors(self) -> None:
        for failure in (False, True):
            with self.subTest(failure=failure):
                plugin, client = self._plugin()
                listener = plugin._listener()
                listener._primed["D1"] = True

                def reset_during_fetch(*args):
                    listener.reset()
                    if failure:
                        raise RuntimeError("old request failed")
                    return [{"timestamp_ms": int(time.time() * 1000), "question": "播放歌曲稻香"}]

                client.get_latest_ask = reset_during_fetch
                self.assertEqual(listener.tick(), 0)
                self.assertEqual(listener._baseline, {})
                self.assertEqual(listener.status()["error"], "")
                self.assertFalse([row for row in client.calls if row[0] == "play"])

    def test_listener_does_not_overlap_cloud_reads(self) -> None:
        plugin, client = self._plugin()
        listener = plugin._listener()
        entered, release = threading.Event(), threading.Event()
        calls = []

        def blocked_fetch(*args):
            calls.append(args)
            entered.set()
            release.wait(2)
            return []

        client.get_latest_ask = blocked_fetch
        worker = threading.Thread(target=listener.tick)
        worker.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertEqual(listener.tick(), 0)
            self.assertEqual(len(calls), 1)
        finally:
            release.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())

    def test_child_song_command_does_not_require_ai(self) -> None:
        plugin, _ = self._plugin()
        match = plugin._match_voice("播放儿歌小跳蛙")
        self.assertIsNotNone(match)
        self.assertEqual((match.kind, match.argument), ("play_song", "小跳蛙"))

    def test_failed_voice_selection_neither_memorizes_nor_advances(self) -> None:
        plugin, client = self._plugin(config={"voice_hint_tts": False, "ai_enabled": False})
        plugin._play_file(self.tracks[0], device_id="D1", queue_paths=self.tracks)
        plugin._tick_advance()
        last = plugin.get_data("last")
        last["started_at"] = time.time() - 120
        plugin.save_data("last", last)
        plugin._library_rebuild = lambda **kwargs: False
        reply = plugin._on_voice_text("播放歌曲查无此曲", "D1", "OH2")
        self.assertIn("没找到", reply)
        self.assertFalse(plugin.get_data("voice_memory"))
        client._status["status"] = 0
        before = len([row for row in client.calls if row[0] == "play"])
        plugin._tick_advance()
        plugin._tick_advance()
        self.assertEqual(len([row for row in client.calls if row[0] == "play"]), before)

    def test_voice_test_execution_uses_takeover_and_failure_suspension(self) -> None:
        plugin, client = self._plugin(config={"voice_hint_tts": False})
        plugin._play_file(self.tracks[0], device_id="D1", queue_paths=self.tracks)
        plugin._library_rebuild = lambda **kwargs: False
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="播放儿歌查无此曲", execute=True))
        self.assertIn("没找到", out.reply)
        self.assertIn(("interrupt", "D1"), client.calls)
        self.assertTrue(plugin._queue_state().get("suspended"))
        self.assertFalse(plugin.get_data("voice_memory"))

    def test_unverified_legacy_memory_is_not_used(self) -> None:
        plugin, _ = self._plugin()
        plugin.save_data("voice_memory", [{"key": "play_song|来点旧歌", "path": self.tracks[0],
                                           "at": time.time()}])
        self.assertIsNone(plugin._match_voice("来点旧歌", use_ai=False))

    def test_failed_device_push_does_not_create_voice_memory(self) -> None:
        plugin, client = self._plugin(config={"voice_hint_tts": False})
        plugin._play_file(self.tracks[1], device_id="D1", queue_paths=self.tracks)
        client._play_ok = False
        reply = plugin._on_voice_text("播放歌曲稻香", "D1", "OH2")
        self.assertIn("拒绝", reply)
        self.assertFalse(plugin.get_data("voice_memory"))

    def test_repeated_voice_command_uses_its_successful_memory(self) -> None:
        plugin, _ = self._plugin()
        plugin._on_voice_text("播放歌曲稻香", "D1", "OH2")
        plugin._library().search_song = lambda query: [(100, plugin._library().get(self.tracks[1]).as_dict())]
        plugin._on_voice_text("播放歌曲稻香", "D1", "OH2")
        self.assertEqual(plugin.get_data("last")["path"], self.tracks[0])

    def test_config_changes_discard_pending_ai_result(self) -> None:
        for action in ("save", "reload", "stop", "disable_monitor", "select", "logout"):
            with self.subTest(action=action):
                plugin, client = self._plugin(config={"ai_enabled": True})
                entered, release = threading.Event(), threading.Event()

                class Analyzer:
                    def analyze(self, text):
                        entered.set()
                        release.wait(2)
                        return x._voice.Match("play_song", "", "ai", "稻香", text)

                plugin._ai_available = lambda: True
                plugin._analyzer = lambda: Analyzer()
                plugin._on_voice_text("来点我喜欢的", "D1", "OH2", background_ai=True)
                try:
                    self.assertTrue(entered.wait(1))
                    if action == "save":
                        plugin.api_config(x.schemas.ConfigIn(ai_enabled=False))
                    elif action == "reload":
                        plugin.init_plugin(plugin._config_snapshot())
                    elif action == "stop":
                        plugin.stop_service()
                    elif action == "disable_monitor":
                        plugin._task_monitor("disable_monitor")
                    elif action == "select":
                        plugin.api_device_select(x.schemas.DeviceSelectIn(device_id="D1"))
                    else:
                        plugin.api_logout()
                finally:
                    release.set()
                    plugin.__dict__["_voice_ai_thread"].join(2)
                self.assertFalse([row for row in client.calls if row[0] in ("play", "tts")])
                self.assertFalse(plugin.get_data("voice_memory"))

    def test_voice_test_cannot_execute_ai_result_after_a_stop(self) -> None:
        plugin, client = self._plugin(config={"ai_enabled": True})
        entered, release = threading.Event(), threading.Event()
        outputs = []

        class Analyzer:
            def analyze(self, text):
                entered.set()
                release.wait(2)
                return x._voice.Match("play_song", "", "ai", "稻香", text)

        plugin._ai_available = lambda: True
        plugin._analyzer = lambda: Analyzer()
        worker = threading.Thread(target=lambda: outputs.append(plugin.api_voice_test(
            x.schemas.VoiceIn(text="来点我喜欢的", execute=True))))
        worker.start()
        try:
            self.assertTrue(entered.wait(1))
            plugin._control("stop")
        finally:
            release.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertFalse(outputs[0].ok)
        self.assertFalse([row for row in client.calls if row[0] == "play"])

    def test_successful_voice_play_has_no_followup_tts(self) -> None:
        plugin, client = self._plugin(config={"voice_hint_tts": False, "voice_reply": True})
        listener = plugin._listener()
        listener._primed["D1"] = True
        self._listener_tick(plugin, client, [{"timestamp_ms": int(time.time() * 1000),
            "question": "播放歌曲稻香", "request_id": "play"}])
        self.assertTrue([row for row in client.calls if row[0] == "play"])
        self.assertFalse([row for row in client.calls if row[0] == "tts"])
        memory = plugin.get_data("voice_memory")
        self.assertEqual(memory[0]["path"], self.tracks[0])

    def test_rejected_interrupt_is_reported_in_logs(self) -> None:
        plugin, client = self._plugin(config={"voice_hint_tts": False})
        client.interrupt = lambda device_id: {"ok": False, "pause_ok": False, "error": "device rejected stop"}
        mark = len(_logger.lines)
        plugin._on_voice_text("播放歌曲稻香", "D1", "OH2")
        self.assertIn("device rejected stop", "\n".join(_logger.lines[mark:]))

    def test_ai_pending_does_not_block_stop_or_play_late(self) -> None:
        plugin, client = self._plugin(config={"ai_enabled": True, "ai_chat": False})
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        class Analyzer:
            def analyze(self, text):
                entered.set()
                release.wait(0.6)
                finished.set()
                return x._voice.Match("play_song", "", "ai", "稻香", text)
        plugin._ai_available = lambda: True
        plugin._analyzer = lambda: Analyzer()
        listener = plugin._listener()
        listener._primed["D1"] = True
        now = int(time.time() * 1000)
        started = time.monotonic()
        try:
            self._listener_tick(plugin, client, [{"timestamp_ms": now,
                "question": "来点我喜欢的", "request_id": "ai"}])
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 0.3, "监听在等待 AI，后续停止口令无从进入")
            self.assertTrue(entered.wait(1))
            self._listener_tick(plugin, client, [{"timestamp_ms": now + 1000,
                "question": "停止播放", "request_id": "stop"}])
            self.assertIn(("play_operation", "D1", "stop"), client.calls)
        finally:
            release.set()
            finished.wait(1)
            worker = plugin.__dict__.get("_voice_ai_thread")
            if worker:
                worker.join(1)
        self.assertFalse([row for row in client.calls if row[0] == "play"], "停止后的 AI 结果不应再起播")

    def test_artist_queue_includes_all_matches_in_natural_order(self) -> None:
        plugin, _ = self._plugin()
        index = plugin._library()
        index._songs = [library.Song(f"/music/歌手/专辑/{i} - 歌.mp3", f"{i} - 歌.mp3",
                                    f"歌{i}", "歌手", "专辑", 1, 1, 1)
                        for i in (12, 2, 10, 1, 11, 4, 3, 6, 5, 8, 7, 9)]
        paths, _ = x._intents.Selection(index, []).artist("歌手")
        self.assertEqual([Path(path).name for path in paths], [f"{i} - 歌.mp3" for i in range(1, 13)])

    def test_album_queue_accepts_equivalent_chinese_metadata(self) -> None:
        plugin, _ = self._plugin()
        index = plugin._library()
        index._songs = [library.Song(f"/music/歌手/专辑/{i}.mp3", f"{i}.mp3", str(i), "歌手", album, 1, 1, 1)
                        for i, album in [(2, "葉惠美"), (1, "叶惠美")]]
        paths = x._intents.Selection(index, [])._neighbours(index._songs[1])
        self.assertEqual(paths, ["/music/歌手/专辑/1.mp3", "/music/歌手/专辑/2.mp3"])

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

    def test_state_only_command_keeps_the_track_alive(self) -> None:
        """只改状态的口令不能把正在放的那首停掉：pause+stop 是不可逆的。

        音箱端那条签名直链一旦被 stop 就结束了，它不会自己回来；接着连播巡检还会
        把这声"停止"当成"这首播完了"，几十秒后推下一首 —— 说句"随机播放"的代价。
        """
        plugin, client = self._plugin(config={"voice_interrupt": True})
        plugin._set_queue_state(self.tracks[:3], 0, "D1")
        plugin._on_voice_text("随机播放", "D1", "")
        plugin._on_voice_text("音量调到50", "D1", "")
        plugin._on_voice_text("收藏", "D1", "")
        self.assertEqual(plugin._queue_state()["mode"], "random")
        self.assertNotIn("interrupt", [call[0] for call in client.calls])
        self.assertNotIn("stop", [call[2] for call in client.calls
                                 if call[0] == "play_operation"])
        # 点歌、切歌这类要占声道的，切掉小爱自己那句回答仍然是对的
        plugin._on_voice_text("播放歌曲稻香", "D1", "")
        self.assertIn("interrupt", [call[0] for call in client.calls])

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
        playing = {"status": 1, "volume": 10, "position": 1000, "duration": 60000}
        plugin, client = self._plugin(status=playing)
        plugin._set_queue_state(self.tracks, 0, "D1")
        plugin.save_data("sleep_timer", {"stop_after_songs": 1, "device_id": "D1"})
        plugin.save_data("last", {"device_id": "D1", "path": self.tracks[0],
                                  "started_at": time.time() - 60})
        plugin._tick()
        client._status = {"status": 0, "volume": 10, "position": 0, "duration": 1}
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
        self.assertEqual(sorted(plugin._voice_commands()), ["next", "set_volume"],
                         "enabled=null 只表示不动开关，别把用户改过的表清掉")
        plugin.api_commands_save(x.schemas.CommandsIn(reset=True))
        self.assertIn("favorite", plugin._voice_commands(), "reset 才恢复全开")

    def test_custom_commands_match_and_persist(self) -> None:
        """用户自己加的口令进表、参与判定，整句说完就能播。"""
        plugin, _ = self._plugin()
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="播放周杰伦", execute=False))
        self.assertFalse(out.ok, "没加自定义词前，这句话口令表判不出来")
        saved = plugin.api_commands_save(x.schemas.CommandsIn(
            add=[x.schemas.CommandWordIn(keyword="播放周杰伦", kind="play_artist",
                                         argument="周杰伦")]))
        self.assertTrue(saved.ok)
        self.assertEqual([item.keyword for item in saved.customs], ["播放周杰伦"])
        out = plugin.api_voice_test(x.schemas.VoiceIn(text="播放周杰伦", execute=True))
        self.assertTrue(out.ok)
        self.assertEqual(out.kind, "play_artist")
        self.assertEqual(out.matched, "播放周杰伦", "该报自定义词本身，而不是内置词")
        self.assertEqual(out.argument, "周杰伦")
        # 口令后面还带内容时，用说的话而不是用户配的默认内容
        self.assertEqual(plugin._match_voice("播放周杰伦 稻香").argument, "稻香")
        # 最长关键词优先：加了更长的口令后，那句话走新的语义
        plugin.api_commands_save(x.schemas.CommandsIn(
            add=[x.schemas.CommandWordIn(keyword="播放周杰伦的歌", kind="play_playlist",
                                         argument="范特西")]))
        self.assertEqual(plugin._match_voice("播放周杰伦的歌").kind, "play_playlist")
        stored = plugin._voice_words()
        self.assertEqual({item["keyword"] for item in stored},
                         {"播放周杰伦", "播放周杰伦的歌"}, "自定义口令要落盘并能读回")
        self.assertEqual([item["argument"] for item in stored], ["周杰伦", "范特西"])

    def test_custom_commands_are_validated(self) -> None:
        """自定义口令只认已有类型、限长度，内置本来就认的词与空话不重复收。"""
        plugin, _ = self._plugin()
        saved = plugin.api_commands_save(x.schemas.CommandsIn(add=[
            x.schemas.CommandWordIn(keyword="   ", kind="play_song"),
            x.schemas.CommandWordIn(keyword="我要听林俊杰", kind="no_such_kind"),
            x.schemas.CommandWordIn(keyword="？？", kind="play_song"),
            x.schemas.CommandWordIn(keyword="太一" * 40, kind="play_song"),
            # 整句本身就是内容，不填内容收进来也只会空跑一次搜索
            x.schemas.CommandWordIn(keyword="播放郭富城", kind="play_song"),
        ]))
        self.assertEqual(saved.customs, [], "五款不合规的都不该进表")
        self.assertIn("还得补上内容", saved.message)
        plugin.api_commands_save(x.schemas.CommandsIn(
            add=[x.schemas.CommandWordIn(keyword="来杯音乐", kind="set_volume",
                                         param="absolute", argument="20")]))
        again = plugin.api_commands_save(x.schemas.CommandsIn(
            add=[x.schemas.CommandWordIn(keyword="来杯音乐", kind="set_volume",
                                         param="absolute", argument="20")]))
        self.assertEqual(len(again.customs), 1, "重复添加不该堆两条")
        self.assertIn("已经加过了", again.message)
        removed = plugin.api_commands_save(x.schemas.CommandsIn(remove=["来杯音乐"]))
        self.assertEqual(removed.customs, [])

    def test_hidden_commands_gate_matching(self) -> None:
        """藏掉某个内置口令词后那句话不再被接手，还原后恢复。"""
        plugin, _ = self._plugin()
        self.assertEqual(plugin._match_voice("不喜欢的").kind, "favorite")
        saved = plugin.api_commands_save(x.schemas.CommandsIn(hide=["不喜欢的"]))
        row = [item for item in saved.items if item.kind == "favorite"][0]
        self.assertIn("不喜欢的", row.hidden)
        self.assertIn("不喜欢的", row.keywords, "藏起来的词仍要回给前端，不然没法还原")
        self.assertIsNone(plugin._match_voice("不喜欢的"),
                          "藏掉的词不该再命中，别的收藏口令仍要能命中")
        self.assertEqual(plugin._match_voice("取消收藏").kind, "favorite")
        plugin.api_commands_save(x.schemas.CommandsIn(unhide=["不喜欢的"]))
        self.assertEqual(plugin._match_voice("不喜欢的").kind, "favorite")

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
        """推理超时及时返回，同时仍把模型交还宿主。"""
        helper = self._host_assistant()
        closed = threading.Event()
        workers = []

        async def slow_invoke(model, messages):
            workers.append(threading.current_thread())
            await asyncio.sleep(1)

        async def close_model(model):
            model.closed = True
            closed.set()

        assistant = x.intents.Assistant()
        assistant.timeout = 0.03
        started = time.monotonic()
        try:
            with mock.patch.object(_stub_env._FakeLLM, "ainvoke", slow_invoke), \
                    mock.patch.object(helper, "close_llm", close_model):
                self.assertEqual(assistant.ask("系统提示", "说一句"), "")
                self.assertLess(time.monotonic() - started, 0.3)
                self.assertTrue(closed.wait(1))
                for worker in workers:
                    worker.join(1)
            self.assertIn("调用失败", assistant.error)
            self.assertTrue(helper.instances[0].closed)
        finally:
            self._without_assistant()

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

    def test_group_member_stop_suspends_the_shared_queue(self) -> None:
        plugin, _ = self._plugin()
        plugin.api_group_save(x.schemas.GroupIn(name="全屋", device_ids=["D1", "D2"]))
        plugin.api_queue(x.schemas.QueueIn(paths=[str(self.track)], device_id="D1"))
        self.assertTrue(plugin._control("stop", "D2").ok)
        self.assertEqual(plugin._queue_state().get("paused_token"),
                         plugin._queue_token(plugin._queue_state()))

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

    def test_tick_fired_marker_survives_a_restart(self) -> None:
        """「这一分钟已经触发过」要落盘：只记在进程里的话，宿主重启正好落在某条任务的
        那一分钟里就会把它补触发一次 —— 「播放歌单」被补触发，音箱就是在重启后自己唱起来。
        """
        plugin, _client = self._plugin()
        now = datetime.now()
        self._saved(plugin, action="stop",
                    schedule={"type": "weekly", "time": now.strftime("%H:%M"),
                              "weekdays": [now.isoweekday() % 7]})
        plugin._tick_tasks()
        self.assertEqual(len(plugin._task_logs()), 1)
        # 清掉内存标记 = 换了一个新进程，同一分钟内再判一次
        plugin.__dict__["_task_minute"] = ""
        plugin._tick_tasks()
        self.assertEqual(len(plugin._task_logs()), 1, "同一分钟重启不能把任务补触发一次")
        fired = plugin.get_data(x._KEY_TASK_FIRED)
        self.assertEqual(list(fired.values()), [now.strftime("%Y-%m-%d %H:%M")])

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


class AssistantDeadlineTest(unittest.TestCase):
    def test_result_waits_for_loop_and_child_cleanup(self) -> None:
        assistant = x._intents.Assistant()
        child_closed = threading.Event()
        loops = []

        async def child():
            try:
                await asyncio.sleep(10)
            finally:
                child_closed.set()

        async def invoke():
            loop = asyncio.get_running_loop()
            loops.append(loop)
            loop.create_task(child())
            await asyncio.sleep(0)
            return "ready"

        self.assertEqual(assistant.call(invoke()), "ready")
        self.assertTrue(child_closed.is_set(), "模型创建的子任务需要取消并等待清理")
        self.assertTrue(loops[0].is_closed(), "结果返回前要完成 worker 回收")
        self.assertEqual(assistant.call(asyncio.sleep(0, result="again")), "again")

    def test_cancel_unblocks_caller_and_keeps_one_underlying_worker(self) -> None:
        assistant = x._intents.Assistant()
        assistant.timeout = 2
        entered, release, caller_done = threading.Event(), threading.Event(), threading.Event()
        workers, outcomes = [], []

        async def blocked():
            workers.append(threading.current_thread())
            entered.set()
            release.wait(2)
            return "late"

        def call():
            try:
                outcomes.append(assistant.call(blocked()))
            except Exception as exc:
                outcomes.append(exc)
            finally:
                caller_done.set()

        caller = threading.Thread(target=call)
        caller.start()
        try:
            self.assertTrue(entered.wait(1))
            assistant.cancel()
            self.assertTrue(caller_done.wait(0.3), "取消不能等待不响应取消的第三方代码")
            self.assertIsInstance(outcomes[0], RuntimeError)
            for _ in range(3):
                with self.assertRaisesRegex(RuntimeError, "尚未结束"):
                    assistant.call(asyncio.sleep(0))
            self.assertEqual(len(workers), 1)
        finally:
            release.set()
            caller.join(2)
            for worker in workers:
                worker.join(2)
        self.assertEqual(assistant.call(asyncio.sleep(0, result="recovered")), "recovered")

    def test_timeout_covers_model_creation(self) -> None:
        class Helper:
            @staticmethod
            async def get_llm(**kwargs):
                await asyncio.sleep(0.2)
                return _stub_env._FakeLLM(["late"], {})
            @staticmethod
            async def close_llm(model):
                model.closed = True
        assistant = x._intents.Assistant()
        assistant.timeout = 0.03
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            assistant.call(assistant._invoke(Helper, [], {}))
        self.assertLess(time.monotonic() - started, 0.15)

    def test_timeout_covers_model_cleanup(self) -> None:
        class Helper:
            @staticmethod
            async def get_llm(**kwargs):
                return _stub_env._FakeLLM(["ready"], {})
            @staticmethod
            async def close_llm(model):
                await asyncio.sleep(0.2)
        assistant = x._intents.Assistant()
        assistant.timeout = 0.03
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            assistant.call(assistant._invoke(Helper, [], {}))
        self.assertLess(time.monotonic() - started, 0.15)


class ReleaseFieldsTest(unittest.TestCase):
    """版本号几处得一样：市场装的是索引里那一版，跑的是仓库里这一份代码。

    源码、市场索引、发布说明与 README 必须一致，保证安装入口对应实际插件版本。
    """

    def test_index_code_and_readme_agree(self) -> None:
        root = Path(x.__file__).resolve().parents[2]
        entry = json.loads((root / "package.v3.json").read_text(encoding="utf-8"))["XiaoaiSpeaker"]
        version = x.XiaoaiSpeaker.plugin_version
        self.assertEqual(entry["version"], version)
        self.assertIn(f"v{version}", entry["history"])
        self.assertTrue(entry["history"][f"v{version}"].strip())
        self.assertIn(f"| 版本 | {version} |", (root / "README.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
