"""MusicLibraryScraper v0.2.0 增量扫描 & 状态账 单测。

只验证插件自身逻辑（done/failed 记账、mtime+size 增量跳过、失败重试上限、定向重试、
prune、status 汇总不泄漏密码）。MTW 网络与 MP 写入通过替换私有方法 __scrape_file /
__has_core_tags 完全屏蔽。

运行：python3 tests/v3/musiclibraryscraper/test_incremental_scan.py
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "tests" / "v3" / "musiclibraryscraper"))

import _stub_env  # noqa: E402


class _FakePluginBase:
    """替身基类：plugindata 用实例 dict 模拟，save/get_data 直接读写同一引用。"""

    def __init__(self, *args, **kwargs):
        self._store: dict = {}
        self._configs: list = []
        self.messages: list = []

    def save_data(self, key, value, plugin_id=None):
        self._store[key] = value

    def get_data(self, key=None, plugin_id=None):
        return self._store.get(key)

    def del_data(self, key=None, plugin_id=None):
        if key is None:
            self._store.clear()
        else:
            self._store.pop(key, None)

    def update_config(self, config, plugin_id=None):
        self._configs.append(dict(config))
        return True

    def get_config(self, plugin_id=None):
        return {}

    def get_data_path(self, plugin_id=None):
        return Path(tempfile.gettempdir())

    def post_message(self, *args, **kwargs):
        self.messages.append(kwargs)
        return True


_stub_env.install(_FakePluginBase)

# 桩就绪后再导入插件（其顶层 import app.* / apscheduler / fastapi 才会成功）。
sys.path.insert(0, str(_ROOT / "plugins.v3"))
import musiclibraryscraper as mls  # noqa: E402

_SCAN = "_MusicLibraryScraper__scan"
_HAS_TAGS = "_MusicLibraryScraper__has_core_tags"
_SCRAPE = "_MusicLibraryScraper__scrape_file"


class IncrementalScanTest(unittest.TestCase):
    def _plugin(self, files: dict):
        """files: {name: 'ok'|'fail'}；返回 (plugin, tmpdir, counters)。"""
        tmp = Path(tempfile.mkdtemp())
        for name in files:
            fp = tmp / name
            fp.write_bytes(b"x" * 100)
        p = mls.MusicLibraryScraper()
        p._scraper_paths = str(tmp)
        p._exclude_paths = ""
        p._mtw_base_url = "http://mtw:8002"
        p._mtw_resource = "netease"
        p._force_all = False
        p._min_score = 3
        counters = {"scrape": 0, "tag": 0}

        def fake_scrape(fp):
            counters["scrape"] += 1
            return (True, "", 5) if files[Path(fp).name] == "ok" else (False, "无候选", 1)

        def fake_tag(fp):
            counters["tag"] += 1
            return False

        setattr(p, _SCRAPE, fake_scrape)
        setattr(p, _HAS_TAGS, fake_tag)
        return p, tmp, counters

    def _scan(self, p, **kw):
        getattr(p, _SCAN)(**kw)

    def test_first_run_classifies(self):
        p, tmp, c = self._plugin({"a.mp3": "ok", "b.mp3": "ok", "c.flac": "fail"})
        self._scan(p)
        done = p.get_data("done")
        failed = p.get_data("failed")
        runs = p.get_data("runs")
        self.assertEqual(len(done), 2)
        self.assertEqual(len(failed), 1)
        self.assertEqual(runs["ok"], 2)
        self.assertEqual(runs["failed"], 1)
        self.assertEqual(runs["from_cache"], 0)
        self.assertEqual(c["scrape"], 3)

    def test_second_run_uses_cache(self):
        p, tmp, c = self._plugin({"a.mp3": "ok", "b.mp3": "ok", "c.flac": "fail"})
        self._scan(p)
        c["scrape"] = c["tag"] = 0
        self._scan(p)
        runs = p.get_data("runs")
        # 两个 done 命中缓存：既不读 tag 也不调 MTW
        self.assertEqual(runs["from_cache"], 2)
        # 只有可重试的失败项（retries=1<MAX）再次触发 scrape
        self.assertEqual(c["scrape"], 1)
        self.assertEqual(p.get_data("failed")[str(tmp / "c.flac")]["retries"], 2)

    def test_changed_file_reprocessed(self):
        p, tmp, c = self._plugin({"a.mp3": "ok"})
        self._scan(p)
        c["scrape"] = 0
        # 改大小 → 缓存失效
        (tmp / "a.mp3").write_bytes(b"y" * 200)
        self._scan(p)
        self.assertEqual(c["scrape"], 1)
        self.assertEqual(p.get_data("runs")["from_cache"], 0)

    def test_exhausted_not_retried(self):
        p, tmp, c = self._plugin({"c.flac": "fail"})
        self._scan(p)
        key = str(tmp / "c.flac")
        # 手动抬到重试上限
        failed = p.get_data("failed")
        failed[key]["retries"] = p.MAX_RETRY
        c["scrape"] = c["tag"] = 0
        self._scan(p)
        self.assertEqual(p.get_data("runs")["exhausted"], 1)
        self.assertEqual(c["scrape"], 0)  # 超限后不再自动重试

    def test_targeted_retry_resets(self):
        p, tmp, c = self._plugin({"c.flac": "fail"})
        self._scan(p)
        key = str(tmp / "c.flac")
        p.get_data("failed")[key]["retries"] = p.MAX_RETRY
        # 定向重试：即便已达上限也应重跑；此处改成成功
        files = {"c.flac": "fail"}

        def always_ok(fp):
            c["scrape"] += 1
            return (True, "", 5)

        setattr(p, _SCRAPE, always_ok)
        c["scrape"] = 0
        self._scan(p, force=True, target_paths=[key])
        self.assertEqual(c["scrape"], 1)
        self.assertIn(key, p.get_data("done"))
        self.assertNotIn(key, p.get_data("failed"))

    def test_prune_removed_files(self):
        p, tmp, c = self._plugin({"a.mp3": "ok", "gone.mp3": "ok"})
        self._scan(p)
        self.assertEqual(len(p.get_data("done")), 2)
        (tmp / "gone.mp3").unlink()
        self._scan(p)
        self.assertNotIn(str(tmp / "gone.mp3"), p.get_data("done"))
        self.assertEqual(len(p.get_data("done")), 1)

    def test_status_hides_password(self):
        p, tmp, c = self._plugin({"a.mp3": "ok"})
        p._enabled = True
        p._mtw_username = "JontyLee"
        p._mtw_password = "SECRETPW123"
        self._scan(p)
        status = p._build_status()
        blob = str(status)
        self.assertNotIn("SECRETPW123", blob)
        self.assertNotIn("mtw_password", status["config"])
        self.assertTrue(status["config"]["has_username"])
        self.assertEqual(status["counts"]["done"], 1)

    def test_api_files_pagination_and_search(self):
        p, tmp, c = self._plugin({"a.mp3": "ok", "b.mp3": "ok", "z.mp3": "fail"})
        self._scan(p)

        class _Req:
            def __init__(self, q):
                self.query_params = q

        data = p.api_files(_Req({"bucket": "done", "limit": "1", "offset": "0"}))
        self.assertEqual(data["total"], 2)
        self.assertEqual(len(data["items"]), 1)
        searched = p.api_files(_Req({"bucket": "done", "search": "a.mp3"}))
        self.assertEqual(searched["total"], 1)


class ContractTest(unittest.TestCase):
    def _plugin(self):
        p = mls.MusicLibraryScraper()
        p._mtw_base_url = "http://mtw:8002"
        return p

    def test_bad_cron_falls_back(self):
        p = self._plugin()
        trig = p._build_cron_trigger("not a cron")
        self.assertEqual(trig, ("crontab", mls._DEFAULT_CRON))
        # 空值也回落默认
        self.assertEqual(p._build_cron_trigger(""), ("crontab", mls._DEFAULT_CRON))

    def test_service_requires_enabled(self):
        p = self._plugin()
        p._enabled = False
        self.assertEqual(p.get_service(), [])
        p._enabled = True
        p._cron = "0 3 * * 0"
        svc = p.get_service()
        self.assertEqual(len(svc), 1)
        self.assertEqual(svc[0]["id"], "MusicLibraryScraper.Scan")
        self.assertEqual(svc[0]["trigger"], ("crontab", "0 3 * * 0"))

    def test_render_mode_and_page(self):
        self.assertEqual(mls.MusicLibraryScraper.get_render_mode(), ("vue", "dist/assets"))
        self.assertEqual(self._plugin().get_page(), [])

    def test_api_declares_bear_auth(self):
        p = self._plugin()
        apis = p.get_api()
        self.assertEqual({a["path"] for a in apis}, {"/status", "/files", "/retry", "/scan"})
        self.assertTrue(all(a["auth"] == "bear" for a in apis))


if __name__ == "__main__":
    unittest.main(verbosity=2)
