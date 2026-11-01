"""ANiStrmRelay 离线回归：上游路径、配置意图、内网代理及续期时序。

运行：.venv-test/bin/python -B tests/v3/anistrmrelay/test_anistrmrelay.py
宿主和调度器使用隔离替身，httpx/requests 使用真实库，不访问公网。
"""
from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import logging
import os
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

import httpx


ROOT = Path(__file__).resolve().parents[3]
TEMP_ROOT = Path(os.environ.get("TMPDIR") or tempfile.gettempdir())


class PluginBase:
    """记录宿主的整份配置替换和通知，不启动主程序。"""

    def __init__(self):
        self.saved_config = {}
        self.writes = []
        self.messages = []

    def update_config(self, config, plugin_id=None):
        self.saved_config = copy.deepcopy(config)
        self.writes.append(copy.deepcopy(config))
        return True

    def get_config(self, plugin_id=None):
        return copy.deepcopy(self.saved_config)

    def post_message(self, **kwargs):
        self.messages.append(kwargs)


class Scheduler:
    """仅记录 APScheduler 的任务声明，测试显式驱动任务。"""

    def __init__(self, **kwargs):
        self.jobs = {}
        self.running = False

    def add_job(self, func, **kwargs):
        job = types.SimpleNamespace(func=func, **kwargs)
        self.jobs[job.id] = job
        return job

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def get_jobs(self):
        return list(self.jobs.values())

    def remove_job(self, job_id):
        self.jobs.pop(job_id, None)

    def remove_all_jobs(self):
        self.jobs.clear()

    def start(self):
        self.running = True

    def shutdown(self, **kwargs):
        self.running = False

    def print_jobs(self):
        pass


class Trigger:
    """保留周期参数和 cron 声明。"""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @classmethod
    def from_crontab(cls, value):
        return cls(cron=value)


class JsonResponse:
    """保持 FastAPI 响应的状态码与 JSON 字节串契约。"""

    def __init__(self, content=None, status_code=200, **kwargs):
        self.status_code = status_code
        self.body = content if isinstance(content, bytes) else json.dumps(
            content or {}, ensure_ascii=False,
        ).encode()


class StreamResponse:
    """保留流式迭代器而不预读正文，用于校验逐块透传和提前关闭。"""

    def __init__(self, content, status_code=200, headers=None, **kwargs):
        self.body_iterator = content
        self.status_code = status_code
        self.headers = dict(headers or {})


class MediaByteStream(httpx.AsyncByteStream):
    """分两块返回媒体数据，记录消费者读到多少以及连接是否已关闭。"""

    def __init__(self):
        self.parts_read = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in (b"\x00\x00\x00\x20ftypisom", b"media-bytes"):
            self.parts_read += 1
            yield chunk

    async def aclose(self):
        self.closed = True


class RequestUtils:
    """保留同步 HTTP 适配器的 session/proxies 参数，默认禁止访问网络。"""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self._session = kwargs.get("session")

    def post_res(self, **kwargs):
        raise AssertionError("离线测试不允许真实目录请求")


SETTINGS = types.SimpleNamespace(
    TZ="Asia/Shanghai", USER_AGENT="MoviePilot-test", PROXY=None,
    FLARESOLVERR_URL="",
)


def load_plugin():
    """临时安装导入依赖，加载后还原 sys.modules，避免污染其他测试。"""
    definitions = {
        "app.core.config": {"settings": SETTINGS},
        "app.log": {"logger": logging.getLogger("anistrmrelay-test")},
        "app.plugins": {"_PluginBase": PluginBase},
        "app.utils.http": {"RequestUtils": RequestUtils},
        "apscheduler.schedulers.background": {"BackgroundScheduler": Scheduler},
        "apscheduler.triggers.cron": {"CronTrigger": Trigger},
        "apscheduler.triggers.interval": {"IntervalTrigger": Trigger},
        "fastapi": {"Request": object},
        "fastapi.responses": {
            "JSONResponse": JsonResponse, "Response": JsonResponse,
            "StreamingResponse": StreamResponse,
        },
    }
    modules = {}
    for name, attrs in definitions.items():
        pieces = name.split(".")
        for count in range(1, len(pieces) + 1):
            parent = ".".join(pieces[:count])
            if parent not in modules:
                modules[parent] = types.ModuleType(parent)
                modules[parent].__path__ = []
        modules[name].__dict__.update(attrs)
    spec = importlib.util.spec_from_file_location(
        "_anistrmrelay_test", ROOT / "plugins.v3/anistrmrelay/__init__.py",
    )
    plugin = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, modules):
        spec.loader.exec_module(plugin)
    return plugin


ani = load_plugin()


def response(payload=None, status=200, headers=None):
    """真实 httpx.Response，可验证 HTTP 状态及挑战响应。"""
    return httpx.Response(
        status, json=payload if payload is not None else {"files": []},
        headers=headers, request=httpx.Request("POST", ani.DEFAULT_UPSTREAM),
    )


def fs_response():
    """FlareSolverr 的 cookie 和 UA 成对响应，不使用真实凭据。"""
    return response({
        "status": "ok", "solution": {
            "cookies": [{"name": "cf_clearance", "value": "test-clearance"}],
            "userAgent": "FlareSolverr-test-UA",
        },
    })


class RelayTestCase(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        SETTINGS.PROXY = None
        SETTINGS.FLARESOLVERR_URL = ""
        ani._CLEARANCE_MEMORY.update(token="", ua="", issued_ts=0)
        ani._BACKFILL_GUARD.update(holder=0, ts=0)
        self.sleep = mock.patch.object(ani.time, "sleep")
        self.sleep.start()
        self.addCleanup(self.sleep.stop)
        self.tmp = tempfile.TemporaryDirectory(prefix="ani-test-", dir=TEMP_ROOT)
        self.addCleanup(self.tmp.cleanup)
        self.storage = Path(self.tmp.name)

    def config(self, **overrides):
        cfg = {
            "enabled": True, "relay_enabled": True, "use_proxy": True,
            "storageplace": str(self.storage), "selected_seasons": ["latest"],
            "cron": "0 4 * * *", "fs_url": "http://flaresolverr:8191",
            "proxy_base": ani.DEFAULT_UPSTREAM,
            "relay_public_base": "http://moviepilot:3000/api/v1/plugin/ANiStrmRelay/relay",
            "relay_proxy": "", "relay_token": "", "rewrite_existing": False,
            "cf_clearance": "existing-test-clearance", "cf_user_agent": "existing-test-UA",
        }
        cfg.update(overrides)
        return cfg

    def plugin(self, **overrides):
        p = ani.ANiStrmRelay()
        p.init_plugin(self.config(**overrides))
        self.addCleanup(p.stop_service)
        return p

    @staticmethod
    def jobs(p):
        return p._scheduler.jobs if p._scheduler else {}


class FolderPathTest(RelayTestCase):
    def client(self, payloads):
        class Listing:
            def post_res(self, url, **kwargs):
                path = httpx.URL(url).path
                item = payloads[path]
                if isinstance(item, Exception):
                    raise item
                return response(item)

        return ani.AniStrmClient(request_factory=Listing)

    def test_flat_quarter_keeps_quarter_directory(self):
        client = self.client({"/2026-10/": {"files": [
            {"name": "EP01.mp4", "mimeType": "video/mp4"},
        ]}})
        entries = client.get_season_entries("2026-10")
        self.assertEqual(entries[0]["relative_dir"], "2026-10")

    def test_nested_folders_keep_full_upstream_path(self):
        client = self.client({
            "/2026-10/": {"files": [{"name": "番剧 A", "mimeType": client_mime()}]},
            "/2026-10/番剧 A/": {"files": [{"name": "Extras", "mimeType": client_mime()}]},
            "/2026-10/番剧 A/Extras/": {"files": [{"name": "EP01.mp4"}]},
        })
        entry = client.get_season_entries("2026-10")[0]
        self.assertEqual(entry["relative_dir"], "2026-10/番剧 A/Extras")
        self.assertIn("/2026-10/%E7%95%AA%E5%89%A7%20A/Extras/EP01.mp4", entry["url"])

    def test_same_filename_in_two_quarters_creates_two_files(self):
        client = self.client({
            f"/{season}/": {"files": [{"name": "EP01.mp4"}]}
            for season in ("2026-7", "2026-10")
        })
        service = ani.StrmFileService()
        for season in ("2026-7", "2026-10"):
            entry = client.get_season_entries(season)[0]
            self.assertEqual(service.touch_strm_file(
                storage_path=str(self.storage), file_name=entry["name"],
                file_url=entry["url"], relative_dir=entry["relative_dir"],
            ), "created")
        self.assertEqual(len(list(self.storage.glob("*/EP01.mp4.strm"))), 2)
        self.assertFalse((self.storage / "EP01.mp4.strm").exists())

    def test_failed_child_does_not_drop_other_entries(self):
        client = self.client({
            "/2026-10/": {"files": [
                {"name": "broken", "mimeType": client_mime()}, {"name": "EP01.mp4"},
            ]},
            "/2026-10/broken/": httpx.ReadTimeout("listing timeout"),
        })
        self.assertEqual(len(client.get_season_entries("2026-10")), 1)
        self.assertEqual(client.pop_failed_series(), ["2026-10/broken"])

    def test_failed_quarter_is_reported_not_treated_as_empty(self):
        client = self.client({"/2026-10/": httpx.ReadTimeout("quarter timeout")})
        self.assertEqual(client.get_season_entries("2026-10"), [])
        self.assertEqual(client.pop_failed_series(), ["2026-10"])


def client_mime():
    return ani.AniStrmClient.FOLDER_MIME_TYPE


class ConfigLifecycleTest(RelayTestCase):
    def test_pause_preserves_user_switches_across_reload(self):
        p = self.plugin()
        p._ANiStrmRelay__relay_stop("test outage")
        saved = p.get_config()
        self.assertTrue(saved["relay_enabled"])
        self.assertTrue(saved["enabled"])
        self.assertTrue(saved["use_proxy"])
        self.assertFalse(p._relay_enabled)
        p.init_plugin(saved)
        self.assertTrue(p._relay_enabled)

    def test_missing_flaresolverr_pauses_without_erasing_intent(self):
        p = self.plugin(fs_url="")
        self.assertFalse(p._relay_enabled)
        self.assertTrue(p.get_config()["relay_enabled"])
        self.assertTrue(p.get_config()["enabled"])
        self.assertFalse(self.jobs(p))

    def test_missing_public_base_does_not_fall_back_to_direct_pull(self):
        p = self.plugin(relay_public_base="")
        self.assertTrue(p.get_config()["relay_enabled"])
        self.assertFalse(p._enabled)
        self.assertFalse(self.jobs(p))

    def test_form_models_and_persisted_keys_have_defaults(self):
        p = self.plugin()
        with mock.patch.object(p._client, "get_available_seasons", return_value=[]):
            form, defaults = p.get_form()

        def models(items):
            result = set()
            for item in items:
                if "model" in item.get("props", {}):
                    result.add(item["props"]["model"])
                result.update(models(item.get("content", [])))
            return result

        self.assertLessEqual(models(form), set(defaults))
        self.assertEqual(set(p.get_config()), set(defaults))

    def test_newer_cookie_keeps_its_paired_user_agent(self):
        ani._CLEARANCE_MEMORY.update(
            token="new-token", ua="new-UA", issued_ts=int(time.time()),
        )
        p = self.plugin(cf_clearance="old-token", cf_user_agent="old-UA")
        self.assertEqual(p._cf_clearance, "new-token")
        self.assertEqual(p._cf_user_agent, "new-UA")

    def test_stopped_instance_cannot_write_configuration(self):
        p = self.plugin()
        saved = p.get_config()
        p.stop_service()
        p._cf_clearance = "stale-token"
        p._ANiStrmRelay__update_config()
        self.assertEqual(p.get_config(), saved)

    def test_same_instance_cannot_start_a_second_backfill(self):
        p = self.plugin(relay_enabled=False)
        ani._BACKFILL_GUARD.update(holder=id(p), ts=time.time())
        with mock.patch.object(p, "_ANiStrmRelay__run_task") as run:
            p._ANiStrmRelay__task()
        run.assert_not_called()

    def test_old_response_cannot_rotate_cookie_after_reload(self):
        p = self.plugin()
        p._cf_clearance = "previous"
        p._relay_ready = True
        request = types.SimpleNamespace(
            method="POST", headers={}, query_params=types.SimpleNamespace(
                get=lambda key: None, multi_items=lambda: [],
            ), body=mock.AsyncMock(return_value=b"{}"),
        )

        async def check():
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: response(headers={"set-cookie": "cf_clearance=stale; Path=/"}),
            )) as client:
                send = client.send

                async def finish_old_request(*args, **kwargs):
                    p.init_plugin(self.config(cf_clearance="current", cf_user_agent="current-UA"))
                    return await send(*args, **kwargs)

                with mock.patch.object(ani, "_relay_client", return_value=client), \
                        mock.patch.object(client, "send", side_effect=finish_old_request):
                    await p.api_relay("2026-10/", request)

        asyncio.run(check())
        self.assertEqual(p.get_config()["cf_clearance"], "current")

    def test_refresh_finishing_after_reinit_cannot_overwrite_new_config(self):
        p = self.plugin()

        def finish_old_request(*args, **kwargs):
            p.init_plugin(self.config(enabled=False, relay_enabled=False,
                                      cf_clearance="replacement", cf_user_agent="replacement-UA"))
            return fs_response()

        with mock.patch.object(ani.httpx, "post", side_effect=finish_old_request):
            refreshed = p._ANiStrmRelay__fs_refresh()
        self.assertFalse(refreshed)
        self.assertEqual(p.get_config()["cf_clearance"], "replacement")

    def test_reload_during_retry_wait_does_not_make_an_old_request(self):
        p = self.plugin()
        with mock.patch.object(ani.httpx, "post", side_effect=httpx.ConnectError("old endpoint")) as post, \
                mock.patch.object(ani.time, "sleep", side_effect=lambda _: p.init_plugin(
                    self.config(relay_enabled=False, enabled=False),
                )):
            self.assertFalse(p._ANiStrmRelay__fs_refresh())
        self.assertEqual(post.call_count, 1)

    def test_all_selection_stays_when_no_quarter_was_obtained(self):
        p = self.plugin(relay_enabled=False, selected_seasons=["all"])
        with mock.patch.object(p._client, "get_available_seasons", return_value=["2026-10"]), \
                mock.patch.object(p._client, "get_season_entries", return_value=[]):
            p._ANiStrmRelay__task()
        self.assertEqual(p.get_config()["selected_seasons"], ["all"])

    def test_last_file_write_cannot_reset_new_quarter_selection(self):
        p = self.plugin(relay_enabled=False, selected_seasons=["all"])

        def finish_old_write(**kwargs):
            p.init_plugin(self.config(relay_enabled=False, selected_seasons=["all", "2026-7"]))
            return "created"

        with mock.patch.object(p._client, "get_available_seasons", return_value=["2026-10"]), \
                mock.patch.object(p._client, "get_season_entries", return_value=[{
                    "name": "EP01.mp4", "url": ani.DEFAULT_UPSTREAM + "/2026-10/EP01.mp4",
                    "relative_dir": "2026-10",
                }]), mock.patch.object(p._strm_service, "touch_strm_file", side_effect=finish_old_write):
            p._ANiStrmRelay__task()
        self.assertEqual(p.get_config()["selected_seasons"], ["all", "2026-7"])

    def test_startup_pull_retries_after_busy_guard_without_cron(self):
        p = self.plugin(cron="")
        p._relay_ready = True
        p._ANiStrmRelay__schedule_startup_pull()
        ani._BACKFILL_GUARD.update(holder=id(p) + 1, ts=time.time())
        with mock.patch.object(p, "_ANiStrmRelay__run_task") as run:
            p._ANiStrmRelay__task()
            run.assert_not_called()
            self.assertTrue(p._startup_pull_pending)
            self.assertIn("anistrm_relay_startup_pull", self.jobs(p))
            ani._BACKFILL_GUARD.update(holder=0, ts=0)
            p._ANiStrmRelay__task()
            self.assertEqual(run.call_count, 1)
        self.assertFalse(p._startup_pull_pending)

    def test_refresh_clears_ready_before_new_cookie_is_probed(self):
        p = self.plugin()
        p._relay_ready = True
        with mock.patch.object(ani.httpx, "post", return_value=fs_response()):
            self.assertTrue(p._ANiStrmRelay__fs_refresh())
        self.assertFalse(p._relay_ready)

    def test_reload_between_guard_acquisition_and_pending_commit_is_safe(self):
        p = self.plugin(cron="")
        p._relay_ready = True

        class GuardLock:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                p.init_plugin(self_config)

        self_config = self.config(cron="", relay_proxy="http://new.test:7890")
        with mock.patch.object(ani, "_BACKFILL_LOCK", GuardLock()), \
                mock.patch.object(p, "_ANiStrmRelay__run_task") as run:
            p._ANiStrmRelay__task()
        run.assert_not_called()
        self.assertTrue(p._startup_pull_pending)

    def test_old_probe_cannot_mark_new_proxy_ready_at_state_commit(self):
        p = self.plugin()
        real_lock = p._state_lock
        new_config = self.config(relay_proxy="http://new.test:7890")

        class ReloadAtCommit:
            fired = False

            def __enter__(self):
                if not self.fired:
                    self.fired = True
                    p._state_lock = real_lock
                    p.init_plugin(new_config)
                return real_lock.__enter__()

            def __exit__(self, *args):
                return real_lock.__exit__(*args)

        with mock.patch.object(p, "_ANiStrmRelay__fs_refresh", return_value=True), \
                mock.patch.object(p, "_ANiStrmRelay__relay_probe", return_value=response()):
            p._state_lock = ReloadAtCommit()
            p._ANiStrmRelay__relay_heartbeat()
        self.assertFalse(p._relay_ready)
        self.assertNotIn("anistrm_relay_startup_pull", self.jobs(p))

    def test_heartbeat_without_cookie_does_not_start_pull(self):
        p = self.plugin(cf_clearance="", cf_user_agent="")
        with mock.patch.object(p, "_ANiStrmRelay__fs_refresh", return_value=False), \
                mock.patch.object(p, "_ANiStrmRelay__relay_probe", return_value=response()):
            p._ANiStrmRelay__relay_heartbeat()
        self.assertFalse(p._relay_ready)
        self.assertNotIn("anistrm_relay_startup_pull", self.jobs(p))

    def test_startup_pull_waits_for_successful_credential_probe(self):
        p = self.plugin()
        self.assertNotIn("anistrm_relay_startup_pull", self.jobs(p))
        with mock.patch.object(p, "_ANiStrmRelay__fs_refresh", return_value=True), \
                mock.patch.object(p, "_ANiStrmRelay__relay_probe", return_value=response()):
            p._ANiStrmRelay__relay_heartbeat()
        self.assertIn("anistrm_relay_startup_pull", self.jobs(p))

    def test_failed_heartbeat_attempts_only_one_refresh(self):
        p = self.plugin()
        with mock.patch.object(p, "_ANiStrmRelay__fs_refresh", return_value=False) as refresh, \
                mock.patch.object(p, "_ANiStrmRelay__relay_probe", side_effect=httpx.ReadTimeout("test")):
            p._ANiStrmRelay__relay_heartbeat()
        self.assertEqual(refresh.call_count, 1)
        self.assertNotIn("anistrm_relay_startup_pull", self.jobs(p))

    def test_non_success_probe_does_not_start_pull(self):
        p = self.plugin()
        with mock.patch.object(p, "_ANiStrmRelay__fs_refresh", return_value=True), \
                mock.patch.object(p, "_ANiStrmRelay__relay_probe", return_value=response(status=500)):
            p._ANiStrmRelay__relay_heartbeat()
        self.assertNotIn("anistrm_relay_startup_pull", self.jobs(p))

    def test_successful_heartbeat_persists_rotated_cookie(self):
        p = self.plugin()
        with mock.patch.object(p, "_ANiStrmRelay__fs_refresh", return_value=True), \
                mock.patch.object(p, "_ANiStrmRelay__relay_probe", return_value=response(
                    headers={"set-cookie": "cf_clearance=rotated; Path=/"},
                )):
            p._ANiStrmRelay__relay_heartbeat()
        self.assertEqual(p.get_config()["cf_clearance"], "rotated")

    def test_selfcheck_reports_paused_intent_without_leaking_secrets(self):
        p = self.plugin(relay_token="test-secret", cf_clearance="cookie-secret")
        p._ANiStrmRelay__relay_stop("test outage")
        request = types.SimpleNamespace(url=p._relay_public_base)

        async def check():
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: httpx.Response(200, text="ip=192.0.2.1\n")
                if req.url.path.endswith("/trace") else response(),
            )) as client:
                with mock.patch.object(ani, "_relay_client", return_value=client):
                    result = await p.api_relay_selfcheck(request)
            data = json.loads(result.body)
            self.assertTrue(data["relay_requested"])
            self.assertFalse(data["relay_enabled"])
            self.assertEqual(data["pause_reason"], "test outage")
            self.assertEqual(data["mp_egress_ip"], "192.0.2.1")
            self.assertNotIn(b"test-secret", result.body)
            self.assertNotIn(b"cookie-secret", result.body)

        asyncio.run(check())


class ProxyBoundaryTest(RelayTestCase):
    def test_range_body_is_streamed_and_early_close_releases_response(self):
        p = self.plugin()
        p._relay_ready = True
        request = types.SimpleNamespace(
            method="GET", headers={"range": "bytes=0-"},
            query_params=types.SimpleNamespace(get=lambda key: None, multi_items=lambda: []),
            body=mock.AsyncMock(return_value=b""),
        )

        async def check():
            upstream = MediaByteStream()
            sent = []

            def serve(req):
                sent.append(req)
                return httpx.Response(206, headers={
                    "content-type": "video/mp4", "content-range": "bytes 0-22/23",
                }, stream=upstream)

            async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
                with mock.patch.object(ani, "_relay_client", return_value=client):
                    result = await p.api_relay("2025-1/EP01.mp4", request)
                    self.assertIsInstance(result, StreamResponse)
                    self.assertEqual(result.status_code, 206)
                    self.assertEqual(sent[0].headers["range"], "bytes=0-")
                    self.assertEqual(upstream.parts_read, 0)
                    self.assertEqual(await anext(result.body_iterator), b"\x00\x00\x00\x20ftypisom")
                    self.assertEqual(upstream.parts_read, 1)
                    await result.body_iterator.aclose()
                    self.assertTrue(upstream.closed)
                    self.assertEqual(upstream.parts_read, 1)

        asyncio.run(check())

    def test_media_client_uses_http1_without_multiplexing(self):
        with mock.patch.object(ani.httpx, "AsyncClient") as client:
            ani._new_relay_client(control=False)
        self.assertIs(client.call_args.kwargs["http2"], False)
        self.assertIs(client.call_args.kwargs["trust_env"], False)

    def test_control_client_keeps_http2_for_lists(self):
        with mock.patch.object(ani.httpx, "AsyncClient") as client:
            ani._new_relay_client(control=True)
        self.assertIs(client.call_args.kwargs["http2"], True)

    def test_listing_timeout_covers_both_upstream_attempts(self):
        p = self.plugin()
        request = p._client._build_request_utils()
        self.assertEqual(request.kwargs.get("timeout"), 480.0)

    def test_control_read_timeout_is_shorter_than_media_stream(self):
        with mock.patch.multiple(ani, _RELAY_CLIENT=None, _RELAY_CONTROL_CLIENT=None,
                                 _RELAY_CLIENT_PROXY=None, _RELAY_CONTROL_CLIENT_PROXY=None), \
                mock.patch.object(ani.httpx, "AsyncClient") as client:
            ani._relay_client(control=True)
            self.assertEqual(client.call_args.kwargs["timeout"].read, 180.0)
            ani._relay_client(control=False)
            self.assertEqual(client.call_args.kwargs["timeout"].read, 300.0)

    def test_rejected_proxy_selfcheck_sends_no_upstream_request(self):
        p = self.plugin(relay_proxy="socks5://rejected.test:7890")
        with mock.patch.object(ani, "_relay_client") as pool:
            result = asyncio.run(p.api_relay_selfcheck(types.SimpleNamespace(url=p._relay_public_base)))
        pool.assert_not_called()
        data = json.loads(result.body)
        self.assertFalse(data["probe"]["ok"])
        self.assertTrue(data["pause_reason"])

    def test_rejected_proxy_form_uses_local_quarters_only(self):
        p = self.plugin(relay_proxy="socks5://rejected.test:7890")
        with mock.patch.object(p._client, "get_available_seasons", return_value=[]) as seasons:
            p.get_form()
        seasons.assert_not_called()

    def test_old_response_cannot_replace_a_new_cookie_and_user_agent(self):
        p = self.plugin(cf_clearance="cookie-A", cf_user_agent="UA-A")
        p._relay_ready = True
        request = types.SimpleNamespace(
            method="POST", headers={}, query_params=types.SimpleNamespace(
                get=lambda key: None, multi_items=lambda: [],
            ), body=mock.AsyncMock(return_value=b"{}"),
        )

        async def check():
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: response(headers={"set-cookie": "cf_clearance=cookie-A-rotated; Path=/"}),
            )) as client:
                send = client.send

                async def finish_old_request(*args, **kwargs):
                    newer = response({"status": "ok", "solution": {
                        "cookies": [{"name": "cf_clearance", "value": "cookie-B"}],
                        "userAgent": "UA-B",
                    }})
                    with mock.patch.object(ani.httpx, "post", return_value=newer):
                        p._ANiStrmRelay__fs_refresh()
                    return await send(*args, **kwargs)

                with mock.patch.object(ani, "_relay_client", return_value=client), \
                        mock.patch.object(client, "send", side_effect=finish_old_request):
                    await p.api_relay("2026-10/", request)

        asyncio.run(check())
        self.assertEqual(p.get_config()["cf_clearance"], "cookie-B")
        self.assertEqual(p.get_config()["cf_user_agent"], "UA-B")

    def test_old_challenge_cannot_revoke_new_credentials_readiness(self):
        p = self.plugin(cf_clearance="cookie-A", cf_user_agent="UA-A")
        p._relay_ready = True
        request = types.SimpleNamespace(
            method="POST", headers={}, query_params=types.SimpleNamespace(
                get=lambda key: None, multi_items=lambda: [],
            ), body=mock.AsyncMock(return_value=b"{}"),
        )

        async def check():
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: response(status=403, headers={"cf-mitigated": "challenge"}),
            )) as client:
                send = client.send

                async def finish_old_request(*args, **kwargs):
                    p._cf_clearance, p._cf_user_agent = "cookie-B", "UA-B"
                    p._relay_ready = True
                    return await send(*args, **kwargs)

                with mock.patch.object(ani, "_relay_client", return_value=client), \
                        mock.patch.object(client, "send", side_effect=finish_old_request):
                    result = await p.api_relay("2026-10/", request)
                    self.assertEqual(result.status_code, 502)

        asyncio.run(check())
        self.assertTrue(p._relay_ready)
        self.assertFalse(p._urgent_refresh.is_set())

    def test_async_clients_are_isolated_by_proxy_and_control_pool(self):
        with mock.patch.multiple(ani, _RELAY_CLIENT=None, _RELAY_CONTROL_CLIENT=None,
                                 _RELAY_CLIENT_PROXY=None, _RELAY_CONTROL_CLIENT_PROXY=None), \
                mock.patch.object(ani, "_new_relay_client", side_effect=lambda proxy, control=False: types.SimpleNamespace(
                    proxy=proxy, control=control, is_closed=False,
                )):
            first = ani._relay_client(proxy="http://first.test:7890")
            self.assertIs(first, ani._relay_client(proxy="http://first.test:7890"))
            control = ani._relay_client(control=True, proxy="http://first.test:7890")
            self.assertIsNot(first, control)
            self.assertTrue(control.control)
            self.assertFalse(first.control)
            second = ani._relay_client(proxy="http://second.test:7890")
            self.assertIsNot(first, second)
            ani._drop_relay_client(proxy="http://first.test:7890")
            self.assertIs(second, ani._relay_client(proxy="http://second.test:7890"))

    def test_unsupported_proxy_pauses_instead_of_using_tun(self):
        p = self.plugin(relay_proxy="socks5://proxy.test:7890")
        self.assertFalse(p._relay_enabled)
        self.assertFalse(self.jobs(p))
        self.assertTrue(p.get_config()["relay_enabled"])

    def test_system_proxy_is_used_only_when_requested(self):
        SETTINGS.PROXY = {"https": "http://system.test:7890"}
        p = self.plugin()
        with mock.patch.object(ani.httpx, "post", return_value=fs_response()) as post:
            p._ANiStrmRelay__fs_refresh()
        self.assertEqual(post.call_args.kwargs["json"]["proxy"], {"url": "http://system.test:7890"})
        p.init_plugin(self.config(use_proxy=False))
        with mock.patch.object(ani.httpx, "post", return_value=fs_response()) as post:
            p._ANiStrmRelay__fs_refresh()
        self.assertNotIn("proxy", post.call_args.kwargs["json"])

    def test_flaresolverr_control_call_bypasses_environment_proxy(self):
        os.environ["HTTP_PROXY"] = "http://proxy.test:7890"
        p = self.plugin()
        with mock.patch.object(ani.httpx, "post", return_value=fs_response()) as post:
            self.assertTrue(p._ANiStrmRelay__fs_refresh())
        self.assertIs(post.call_args.kwargs.get("trust_env"), False)
        self.assertEqual(post.call_args.args[0], "http://flaresolverr:8191/v1")

    def test_browser_uses_same_proxy_as_upstream_clients(self):
        os.environ["HTTPS_PROXY"] = "http://proxy.test:7890"
        p = self.plugin()
        with mock.patch.object(ani.httpx, "post", return_value=fs_response()) as post:
            p._ANiStrmRelay__fs_refresh()
        self.assertEqual(post.call_args.kwargs["json"].get("proxy"),
                         {"url": "http://proxy.test:7890"})
        with mock.patch.object(ani.httpx, "Client") as client:
            client.return_value.__enter__.return_value.post.return_value = response()
            p._ANiStrmRelay__relay_probe()
        self.assertEqual(client.call_args.kwargs.get("proxy"), "http://proxy.test:7890")
        self.assertIs(client.call_args.kwargs.get("trust_env"), False)

    def test_explicit_relay_proxy_takes_priority_over_environment(self):
        os.environ["HTTPS_PROXY"] = "http://old.test:7890"
        p = self.plugin(relay_proxy="http://fixed.test:7890")
        with mock.patch.object(ani.httpx, "post", return_value=fs_response()) as post:
            p._ANiStrmRelay__fs_refresh()
        self.assertEqual(post.call_args.kwargs["json"].get("proxy"),
                         {"url": "http://fixed.test:7890"})

    def test_inner_listing_session_does_not_trust_proxy_environment(self):
        p = self.plugin()
        request = p._client._build_request_utils()
        self.assertIsNotNone(request._session)
        self.assertIs(request._session.trust_env, False)

    def test_async_relay_uses_the_resolved_proxy(self):
        p = self.plugin(relay_proxy="http://fixed.test:7890")
        p._cf_clearance = "test-clearance"
        p._relay_ready = True
        request = types.SimpleNamespace(
            method="POST", headers={}, query_params=types.SimpleNamespace(
                get=lambda key: None, multi_items=lambda: [],
            ), body=mock.AsyncMock(return_value=b'{}'),
        )

        async def check():
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: response(),
            )) as client:
                with mock.patch.object(ani, "_relay_client", return_value=client) as pool:
                    result = await p.api_relay("2026-10/", request)
                    self.assertEqual(result.status_code, 200)
                    self.assertEqual(pool.call_args.kwargs.get("proxy"), "http://fixed.test:7890")

        asyncio.run(check())

    def test_unverified_cookie_is_not_forwarded(self):
        p = self.plugin(cf_clearance="previous-cookie")
        request = types.SimpleNamespace(
            method="POST", query_params=types.SimpleNamespace(
                get=lambda key: None, multi_items=lambda: [],
            ), headers={},
            body=mock.AsyncMock(return_value=b"{}"),
        )
        with mock.patch.object(ani, "_relay_client") as pool:
            pool.return_value.send = mock.AsyncMock(return_value=response())
            result = asyncio.run(p.api_relay("2026-10/", request))
        self.assertEqual(result.status_code, 503)
        pool.assert_not_called()

    def test_failed_proxy_never_falls_back_to_another_egress(self):
        p = self.plugin(relay_proxy="http://fixed.test:7890", cf_clearance="test")
        p._relay_ready = True
        request = types.SimpleNamespace(
            method="POST", headers={}, query_params=types.SimpleNamespace(
                get=lambda key: None, multi_items=lambda: [],
            ), body=mock.AsyncMock(return_value=b"{}"),
        )
        client = types.SimpleNamespace(build_request=lambda **kwargs: httpx.Request(
            kwargs["method"], kwargs["url"], content=kwargs.get("content"),
        ), send=mock.AsyncMock(side_effect=httpx.ConnectError("proxy unavailable")))
        with mock.patch.object(ani, "_relay_client", return_value=client) as pool, \
                mock.patch.object(ani, "_drop_relay_client"):
            result = asyncio.run(p.api_relay("2026-10/", request))
        self.assertEqual(result.status_code, 502)
        self.assertTrue(all(call.kwargs["proxy"] == "http://fixed.test:7890"
                            for call in pool.call_args_list))


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    unittest.main(verbosity=2)
