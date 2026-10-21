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

import base64
import hashlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "tests" / "v3" / "xiaoaispeaker"))

import _stub_env  # noqa: E402


class _FakePluginBase:
    """替身基类：plugindata 用实例 dict 模拟，配置写入与消息都记下来供断言。"""

    def __init__(self, *args, **kwargs) -> None:
        self._store: Dict[str, Any] = {}
        self._configs: List[Dict[str, Any]] = []
        self.messages: List[Dict[str, Any]] = []

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
        return Path(tempfile.gettempdir())

    def post_message(self, *args, **kwargs) -> None:
        self.messages.append(kwargs)


_SETTINGS = _stub_env._Settings()
_stub_env.install(_FakePluginBase, settings_obj=_SETTINGS)

sys.path.insert(0, str(_ROOT / "plugins.v3"))
import xiaoaispeaker as x  # noqa: E402
from xiaoaispeaker import media, mina  # noqa: E402


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

    def __init__(self, devices=None, status=None, play_ok=True, tts_ok=True) -> None:
        self._devices = devices if devices is not None else []
        self._status = status or {"status": 1, "volume": 30, "position": 1000, "duration": 60000}
        self._play_ok = play_ok
        self._tts_ok = tts_ok
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

    def test_manual_token_parses_cookie_blob(self) -> None:
        token = mina.token_from_manual(
            "999", "userId=999; serviceToken=STVALUE; passToken=PTVALUE; cUserId=abc")
        self.assertEqual(token["user_id"], "999")
        self.assertEqual(token["services"][mina.MINA_SID]["service_token"], "STVALUE")
        self.assertEqual(token["pass_token"], "PTVALUE")
        self.assertTrue(token["manual"])

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

    def test_stream_url_shape(self) -> None:
        url = media.stream_url("http://mp:3000", "XiaoaiSpeaker", "sec",
                               self.root / "01 - 晴天.mp3", 60)
        self.assertIn("http://mp:3000/api/v1/plugin/XiaoaiSpeaker/stream?", url)
        self.assertIn("sig=", url)
        self.assertIn("exp=", url)

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

    def test_parse_query_accepts_mapping_and_string(self) -> None:
        path = "/media/music/01 - 晴天.mp3"
        query = {"path": path, "exp": "123", "sig": "abc"}
        self.assertEqual(media.parse_query(query), (path, 123, "abc"))
        text = "path=%2Fmedia%2Fmusic%2F01%20-%20%E6%99%B4%E5%A4%A9.mp3&exp=123&sig=abc"
        self.assertEqual(media.parse_query(text), (path, 123, "abc"))
        self.assertEqual(media.parse_query({"exp": "x"})[:2], ("", 0))
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
        for secret in ("secret", "PASSSECRET9", "CUSECRET9", "STSECRET9", "SSECURITY9"):
            self.assertNotIn(secret, dumped)
        for key in ("service_token", "pass_token", "ssecurity", "password"):
            self.assertNotIn(f'"{key}"', dumped)

    def test_sidebar_nav_and_service_gating(self) -> None:
        plugin = _plugin()
        nav = plugin.get_sidebar_nav()
        self.assertEqual(len(nav), 1)
        self.assertEqual(nav[0]["nav_key"], "main")
        self.assertEqual(plugin.get_service()[0]["id"], "XiaoaiSpeaker.AutoNext")
        off = _plugin(config={"enabled": False})
        self.assertEqual(off.get_sidebar_nav(), [])
        self.assertEqual(off.get_service(), [])
        hidden = _plugin(config={"show_sidebar_nav": False})
        self.assertEqual(hidden.get_sidebar_nav(), [])
        no_next = _plugin(config={"auto_next": False})
        self.assertEqual(no_next.get_service(), [])

    def test_render_mode_is_vue_federation(self) -> None:
        self.assertEqual(x.XiaoaiSpeaker.get_render_mode(), ("vue", "dist/assets"))
        self.assertEqual(_plugin().get_page(), [])

    def test_api_declarations(self) -> None:
        plugin = _plugin()
        apis = plugin.get_api()
        paths = [a["path"] for a in apis]
        self.assertEqual(len(paths), len(set(paths)), "API 路径不能重复")
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
        self.assertIn("sig=", url)
        # 推给音箱的必须是真实绝对路径，签名对它成立
        path, exp, sig = media.parse_query(url.split("?", 1)[1])
        self.assertEqual(path, str(self.track))
        self.assertTrue(media.verify_sign(plugin._secret(), path, exp, sig))

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
        plugin.get_data("last")["started_at"] = time.time() - 60
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
                 signature: Optional[str] = None,
                 range_header: Optional[str] = None) -> Any:
        target = str(path)
        exp = int(expires if expires is not None else time.time() + 600)
        sig = signature if signature is not None else media.sign_path(
            self.plugin._secret(), target, exp)
        query = {"path": target, "exp": str(exp), "sig": sig}
        headers = {"Range": range_header} if range_header else {}
        request_type = sys.modules["fastapi"].Request
        return request_type(query=query, headers=headers)

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

    def test_tampered_or_expired_or_foreign_is_403(self) -> None:
        bad_sig = self.plugin.api_stream(self._request(self.track, signature="0" * 32))
        self.assertEqual(bad_sig.status_code, 403)
        expired = self.plugin.api_stream(self._request(self.track, expires=int(time.time()) - 5))
        self.assertEqual(expired.status_code, 403)
        # 签名算得对，但目标是库外文件：越库判定必须仍然拦住
        foreign = self.plugin.api_stream(self._request(self.outside))
        self.assertEqual(foreign.status_code, 403)
        self.assertIn("不在配置的音乐库目录内", foreign.body["detail"])

    def test_missing_signature_and_missing_file_are_refused(self) -> None:
        # 不带签名（音箱把 URL 截断了也会这样）与签名指向已删除的文件，都是 403
        self.assertEqual(self.plugin.api_stream(self._request(self.track, signature="")).status_code, 403)
        gone = self.root / "deleted.mp3"
        response = self.plugin.api_stream(self._request(gone))
        self.assertEqual(response.status_code, 403)
        self.assertIn("文件不存在", response.body["detail"])

    def test_large_range_start_clamps_to_whole_file(self) -> None:
        # 越界的 Range 起点回整文件：音箱常见做法是拿它当「从头播」
        response = self.plugin.api_stream(self._request(self.track, range_header="bytes=99999-"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b"".join(response.iterator), self.track.read_bytes())


if __name__ == "__main__":
    unittest.main(verbosity=2)
