"""MusicLibraryScraper 增量扫描 & 状态账 单测。

只验证插件自身逻辑（done/failed 记账、mtime+size 增量跳过、失败重试上限、定向重试、
prune、status 汇总不泄漏密码）。MTW 网络与 MP 写入通过替换私有方法 __scrape_file /
__is_complete 完全屏蔽。

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
_HAS_TAGS = "_MusicLibraryScraper__is_complete"
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
        p._mtw_resources = ["netease"]
        p._force_all = False
        p._min_score = 3
        counters = {"scrape": 0, "tag": 0}

        def fake_scrape(fp):
            counters["scrape"] += 1
            if files[Path(fp).name] == "ok":
                return (True, "", 3, "qmusic")
            return (False, "无候选", None, "")

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
        # 命中的源要落到账上，供数据页显示“这个文件是哪个源刮的”
        any_done = next(iter(done.values()))
        self.assertEqual(any_done["source"], "qmusic")

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
            return (True, "", 3, "netease")

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

    def test_config_page_explains_choices(self):
        """两个最容易误解的选项必须在配置页讲清楚：留空=全部源、两档写入模式的后果。"""
        plugin_dir = Path(mls.__file__).parent
        src = Path(mls.__file__).read_text(encoding="utf-8")
        cfg = (plugin_dir / "frontend" / "src" / "components" / "Config.vue").read_text(
            encoding="utf-8"
        )
        # 原生表单与 Vue 配置页都要有
        self.assertIn("留空=全部", src)
        self.assertIn("留空 = 全部源", cfg)
        for phrase in ("只补缺", "强制覆盖", "宿主"):
            self.assertIn(phrase, src)
            self.assertIn(phrase, cfg)
        # 覆盖后果不能只写"会覆盖"，要说清哪些字段会被替换、哪些不会
        self.assertIn("手工改过", cfg)
        self.assertIn("不会清空", cfg)

    def test_mtw_endpoints_use_apimt_prefix(self):
        """MTW 的 Django 路由挂在 /apimt/ 下，写成 /api/ 会一律 404。"""
        src = Path(mls.__file__).read_text(encoding="utf-8")
        self.assertIn("/apimt/token/", src)
        self.assertIn("/apimt/fetch_id3_by_title/", src)
        self.assertNotIn('/api/token/', src)
        self.assertNotIn('/api/fetch_id3_by_title/', src)


class MatchTest(unittest.TestCase):
    """候选比对这些坑：繁简之外的空格/标点差异、文件名当搜索词、tag 读不到时的目录兜底。"""

    def _plugin(self, min_score=3, resources="netease"):
        p = mls.MusicLibraryScraper()
        p._min_score = min_score
        # 故意传字符串：验证旧配置（单选）也能跑
        p._mtw_resources = resources
        p._mtw_base_url = "http://mtw:8002"
        p._mtw_username = ""
        p._mtw_password = ""
        return p

    def test_norm_text_strips_space_and_punctuation(self):
        self.assertEqual(mls._norm_text("你的陈奕迅 国语精选"), "你的陈奕迅国语精选")
        self.assertEqual(mls._norm_text("  CHIN UP! "), "chinup")
        # 全角转半角后比较
        self.assertEqual(mls._norm_text("Ｔｈｅ Ｌｉｎｅ-Ｕｐ"), "thelineup")
        self.assertEqual(mls._norm_text(None), "")

    def test_norm_text_prefers_host_music_text_key(self):
        """宿主有 music_text_key（含 rust zhconv 繁转简）时必须优先用它。"""
        calls = []

        def fake_key(value):
            calls.append(value)
            return "绵绵"

        orig = mls._music_text_key
        mls._music_text_key = staticmethod(fake_key)
        try:
            self.assertEqual(mls._norm_text("綿綿"), "绵绵")
        finally:
            mls._music_text_key = orig
        self.assertEqual(calls, ["綿綿"])

    def test_norm_text_survives_host_key_failure(self):
        """宿主函数抛异常也不能带崩整轮扫描，要退回本地实现。"""
        def boom(value):
            raise RuntimeError("rust zhconv unavailable")

        orig = mls._music_text_key
        mls._music_text_key = staticmethod(boom)
        try:
            self.assertEqual(mls._norm_text("你的陈奕迅 国语精选"), "你的陈奕迅国语精选")
        finally:
            mls._music_text_key = orig

    def test_read_music_tags_never_raises(self):
        out = mls._read_music_tags(Path("/nonexistent/dir/01 - x.flac"))
        self.assertEqual(
            out, {"title": "", "artist": "", "album": "", "duration": None, "has_cover": False}
        )

    def test_path_identity_strips_track_prefix_and_year(self):
        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "陈奕迅" / "黑白灰 (2003)" / "07 - 黑面.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        self.assertEqual(
            mls._path_identity(fp), {"title": "黑面", "artist": "陈奕迅", "album": "黑白灰"}
        )

    def test_score_candidate_counts_title_artist_album(self):
        want = {"title": "黑面", "artist": "陈奕迅", "album": "黑白灰"}
        score = mls.MusicLibraryScraper._score_candidate
        self.assertEqual(
            score({"name": "黑面", "artist": "陈奕迅", "album": "黑白灰"}, want), 3
        )
        # 只差空格：不该算失配
        self.assertEqual(
            score({"name": "黑面", "artist": "陈奕迅", "album": "黑白 灰"}, want), 3
        )
        # 专辑对不上 -> 2
        self.assertEqual(
            score({"name": "黑面", "artist": "陈奕迅", "album": "The Line-Up"}, want), 2
        )
        # 艺人是 dict 列表（qmusic 形状）
        self.assertEqual(
            score({"title": "黑面", "artists": [{"name": "陈奕迅"}],
                   "album": {"name": "黑白灰"}}, want), 3
        )
        self.assertEqual(score({"name": "完全无关", "artist": "别人"}, want), 0)

    def test_pick_best_threshold_and_fallback_to_path(self):
        # 未打 tag（测试环境无 mutagen）→ 用目录结构兜底，仍能匹配
        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "陈奕迅" / "黑白灰" / "07 - 黑面.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        p = self._plugin(min_score=3)
        cands = [{"name": "黑面", "artist": "陈奕迅", "album": "黑白灰"}]
        picked, sc = p._mtw_pick_best(fp, cands)
        self.assertEqual(sc, 3)
        self.assertIsNotNone(picked)
        # 阈值抬高到 4（满分 3）→ 不返回候选，但仍回报真实分数
        p2 = self._plugin(min_score=4)
        picked2, sc2 = p2._mtw_pick_best(fp, cands)
        self.assertIsNone(picked2)
        self.assertEqual(sc2, 3)

    def test_fetch_candidates_queries_clean_title_not_filename(self):
        """搜索词必须是清洗后的曲名；传 "07 - 黑面.flac" 只会召回噪音。"""
        captured = {"n": 0}

        class _Ru:
            def __init__(self, *a, **k):
                pass

            def post_json(self, url=None, json=None, **kw):
                captured["n"] += 1
                captured["url"] = url
                captured["body"] = json
                return {"result": True, "data": []}

        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "陈奕迅" / "黑白灰" / "07 - 黑面.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        p = self._plugin()
        orig = mls.RequestUtils
        mls.RequestUtils = _Ru
        try:
            self.assertEqual(p._mtw_fetch_candidates(fp), [])
        finally:
            mls.RequestUtils = orig
        self.assertIn("/apimt/fetch_id3_by_title/", captured["url"])
        self.assertEqual(captured["body"]["title"], "黑面")
        self.assertEqual(captured["body"]["artist"], "陈奕迅")
        self.assertEqual(captured["body"]["album"], "黑白灰")
        self.assertEqual(captured["body"]["resource"], "netease")
        # 旧的单字符串配置不能被当字符逐个遍历
        self.assertEqual(captured["n"], 1)

    def test_normalize_resources(self):
        """多选值要能吃下字符串、分隔串、数组三种历史形状，空值回落全源。"""
        self.assertEqual(mls._normalize_resources("netease"), ["netease"])
        self.assertEqual(mls._normalize_resources("netease;qmusic;netease"), ["netease", "qmusic"])
        self.assertEqual(mls._normalize_resources("netease, qmusic"), ["netease", "qmusic"])
        self.assertEqual(mls._normalize_resources(["migu", " ", None]), ["migu"])
        default = mls._normalize_resources(None)
        self.assertEqual(default, list(mls._DEFAULT_RESOURCES))
        # 实测这些 resource 恒返回空候选，不该进默认
        for dead in ("qq", "spotify", "acoustid", "all"):
            self.assertNotIn(dead, default)

    def test_empty_resources_mean_all(self):
        """留空 = 全部源：配置页的提示文案依赖这条契约。"""
        for empty in ([], "", None, [" "], ["  ", ""]):
            self.assertEqual(mls._normalize_resources(empty), list(mls._DEFAULT_RESOURCES))

    def _fake_mtw(self, by_resource, calls):
        class _Ru:
            def __init__(self, *a, **k):
                pass

            def post_json(self, url=None, json=None, **kw):
                resource = json["resource"]
                calls.append(resource)
                return {"result": True, "data": by_resource.get(resource, [])}

        return _Ru

    def _track(self):
        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "陈奕迅" / "黑白灰" / "07 - 黑面.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        return fp

    def test_fetch_candidates_merges_all_sources(self):
        """没有满分候选时逐源问完，并给每个候选标上命中的源。"""
        fp = self._track()
        by_resource = {
            r: [{"name": "黑面", "artist": "陈奕迅", "album": f"{r} 精选"}]
            for r in mls._DEFAULT_RESOURCES
        }
        calls = []
        p = self._plugin(min_score=3, resources=list(mls._DEFAULT_RESOURCES))
        orig = mls.RequestUtils
        mls.RequestUtils = self._fake_mtw(by_resource, calls)
        try:
            cands = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(calls, list(mls._DEFAULT_RESOURCES))
        self.assertEqual([c["resource"] for c in cands], list(mls._DEFAULT_RESOURCES))
        # 专辑名各源不同 → 全部只有 2 分，落到阈值以下
        picked, score = p._mtw_pick_best(fp, cands)
        self.assertIsNone(picked)
        self.assertEqual(score, 2)

    def test_fetch_candidates_stops_at_perfect_score(self):
        """某源已出 3/3 候选就停，避免每个文件都打满全部源。"""
        fp = self._track()
        by_resource = {
            "netease": [{"name": "无关曲", "artist": "无关人", "album": "无关碟"}],
            "qmusic": [{"name": "黑面", "artist": "陈奕迅", "album": "黑白灰"}],
        }
        calls = []
        p = self._plugin(min_score=3, resources=list(mls._DEFAULT_RESOURCES))
        orig = mls.RequestUtils
        mls.RequestUtils = self._fake_mtw(by_resource, calls)
        try:
            cands = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(calls, ["netease", "qmusic"])
        picked, score = p._mtw_pick_best(fp, cands)
        self.assertEqual((score, picked["resource"]), (3, "qmusic"))

    def test_musicinfo_has_no_type_or_artist_init_arg(self):
        """宿主 MusicInfo 的 type 是 init=False，artist 也不是参数，传了会 TypeError。"""
        import re
        src = Path(mls.__file__).read_text(encoding="utf-8")
        block = src.split("return MusicInfo(", 1)[1].split("\n        )\n", 1)[0]
        # 只认顶层参数名，避免把 music_type= / album_artist= 误判成 type= / artist=
        names = set(re.findall(r"^\s{12}(\w+)=", block, re.MULTILINE))
        self.assertNotIn("type", names)
        self.assertNotIn("artist", names)
        self.assertIn("music_type", names)
        self.assertIn("artists", names)


if __name__ == "__main__":
    unittest.main(verbosity=2)
