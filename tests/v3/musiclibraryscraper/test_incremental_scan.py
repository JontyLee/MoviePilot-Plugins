"""MusicLibraryScraper 增量扫描 & 状态账 单测。

只验证插件自身逻辑（done/failed 记账、mtime+size 增量跳过、失败重试上限、定向重试、
prune、status 汇总不泄漏密码）。MTW 网络与 MP 写入通过替换私有方法 __scrape_file /
__is_complete 完全屏蔽。

运行：python3 tests/v3/musiclibraryscraper/test_incremental_scan.py
"""
from __future__ import annotations

import contextlib
import inspect
import re
import sys
import tempfile
import time
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


class _Resp:
    """requests.Response 的最小替身：只带插件要看的那三样（status_code / json / close）。

    存在的理由就是本轮修掉的那个坑：宿主 RequestUtils.post_json 里那句 `if response:`
    判的是 requests 的 Response.__bool__（等价于 status_code < 400），所以 401、500 会
    连同错误体一起被折成 None。用 post_json 做桩的测试会把"JWT 过期"演成"MTW 挂了"，
    和线上行为完全不同 —— 客户端一律改用 post() + 这个假响应。
    """

    def __init__(self, status=200, payload=None, bad_json=False):
        self.status_code = status
        self._payload = payload
        self._bad_json = bad_json
        self.closed = False

    def json(self):
        if self._bad_json:
            raise ValueError("not json")
        return self._payload

    def close(self):
        self.closed = True


def _ru(handler):
    """按 handler(url, body) -> _Resp|None 造一个 RequestUtils 替身类（None = 连不上）。

    MTW 那条路是 POST JSON，内置源是 GET 带查询参数（QQ 音乐除外，它也 POST）——
    两条通道都交给同一个 handler，第二个参数原样给出请求体或查询参数，
    这样"内置源不许把库内路径发出去"这类断言能直接看请求内容。
    """

    class _Ru:
        def __init__(self, *a, **k):
            pass

        def post(self, url=None, json=None, **kw):
            return handler(url, json)

        def get_res(self, url=None, params=None, **kw):
            return handler(url, params)

    return _Ru


@contextlib.contextmanager
def _patch_ru(handler):
    """临时把插件用的 RequestUtils 换成替身，退出即还原。"""
    orig = mls.RequestUtils
    mls.RequestUtils = _ru(handler)
    try:
        yield
    finally:
        mls.RequestUtils = orig


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

        def fake_scrape(fp, tags=None, overwrite=False):
            counters["scrape"] += 1
            if files[Path(fp).name] == "ok":
                return (True, "", 3, "qmusic")
            return (False, "无候选", None, "")

        def fake_tag(fp, tags=None):
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

        def always_ok(fp, tags=None, overwrite=False):
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

    def _transport_outage(self, names, existing_failed=None, reason=None):
        """造一个"每次抓取都是 MTW 侧故障"的插件实例。"""
        files = {n: "ok" for n in names}
        p, tmp, counters = self._plugin(files)

        def outage(fp, tags=None, overwrite=False):
            counters["scrape"] += 1
            return (False, reason or mls._UNREACHABLE_REASON, None, "")

        setattr(p, _SCRAPE, outage)
        if existing_failed:
            p.save_data("failed", existing_failed)
        return p, tmp, counters

    def test_mtw_outage_does_not_burn_retry_budget(self):
        """MTW 整体故障不能记在文件头上：不写 failed 账、不涨 retries，否则连错
        MAX_RETRY 轮整库就永久跳过（mtime+size 不变，再也不刷）。"""
        p, tmp, c = self._transport_outage([f"{i}.mp3" for i in range(4)])
        self._scan(p)
        self.assertEqual(p.get_data("failed"), {})
        self.assertEqual(p.get_data("runs")["failed"], 0)
        self.assertEqual(p.get_data("runs")["transport"], 4)
        self.assertEqual(c["scrape"], 4)

    def test_pending_matches_bucket_when_files_have_stale_entries(self):
        """汇总的"待处理"必须和同一时刻数据页的待处理桶是同一个口径。

        桶是"这份文件列表里既不在 done 也不在 failed 的那些"。上一轮已经记过失败的
        文件在这一轮撞上 MTW 故障时，账本原样留着（不涨 retries 也不删），所以它属于
        失败桶、不属于待处理；用计数器加减很容易把它多算进待处理，两个数字就对不上了。
        """
        names = [f"{i}.mp3" for i in range(4)]
        p, tmp, c = self._transport_outage(names)
        p.save_data("failed", {str(tmp / "0.mp3"): {"retries": 1}})
        self._scan(p)
        runs = p.get_data("runs")
        self.assertEqual(c["scrape"], 4)
        self.assertEqual(runs["transport"], 4)
        # 4 个都撞上故障，其中 1 个还带着上一轮的失败账 → 待处理 3
        self.assertEqual(runs["pending"], 3)
        self.assertEqual(len(p._collect_rows("pending")), runs["pending"])
        self.assertIn(str(tmp / "0.mp3"), p.get_data("failed"))

    def test_mtw_outage_breaks_the_round_and_keeps_the_books(self):
        """连续故障踩满阈值就熔断：剩下的文件不再空打请求，且不 prune（账本只走了一半）。"""
        stale = {"/nonexistent/gone.mp3": {"mtime": 1, "size": 1, "retries": 1}}
        p, tmp, c = self._transport_outage(
            [f"{i}.mp3" for i in range(12)], existing_failed=stale
        )
        self._scan(p)
        self.assertEqual(c["scrape"], mls._ABORT_AFTER_TRANSPORT)
        runs = p.get_data("runs")
        self.assertTrue(runs["aborted"])
        self.assertEqual(runs["transport"], mls._ABORT_AFTER_TRANSPORT)
        # 熔断后没走到的文件必须还算"待处理"，否则那一轮看起来像库已经扫完；
        # 栽在 MTW 故障上的那 5 个也一样待处理 —— 它们既没进 done 也没进 failed，
        # 数据页的待处理桶会把它们列出来，汇总跟着减一份就对不上号了。
        self.assertEqual(runs["pending"], 12)
        self.assertEqual(len(p._collect_rows("pending")), runs["pending"])
        # 中止时账本只覆盖了部分目录，此时 prune 会误删没走到的条目
        self.assertIn("/nonexistent/gone.mp3", p.get_data("failed"))

    def test_small_targeted_batch_runs_through_outage(self):
        """小批次（<= 熔断阈值）值得整批跑完：手动重试点几个就试几个，别半途熔断。"""
        p, tmp, c = self._transport_outage([f"{i}.mp3" for i in range(3)])
        self._scan(p, force=True, target_paths=[str(tmp / f"{i}.mp3") for i in range(3)])
        self.assertEqual(c["scrape"], 3)
        self.assertFalse(p.get_data("last_targeted")["aborted"])

    def test_large_targeted_batch_also_breaks_on_outage(self):
        """/retry 全部失败项可能有几千个：MTW 真挂着时一样要熔断，否则一路打到底。"""
        p, tmp, c = self._transport_outage([f"{i}.mp3" for i in range(12)])
        self._scan(
            p, force=True, target_paths=[str(tmp / f"{i}.mp3") for i in range(12)]
        )
        self.assertEqual(c["scrape"], mls._ABORT_AFTER_TRANSPORT)
        self.assertTrue(p.get_data("last_targeted")["aborted"])

    def test_transport_outage_covers_every_documented_reason(self):
        """三种故障归因在批量循环里走同一条分支：鉴权失败、接口报错和不可达一样不记在
        文件头上、一样计入 transport、一样能熔断。批量侧此前只有"不可达"被验证过 ——
        另两种是 v0.3.4 新增的归因，之前只证明了它们存在于那个元组里。"""
        for reason in mls._TRANSPORT_REASONS:
            with self.subTest(reason=reason):
                p, tmp, c = self._transport_outage(
                    [f"{i}.mp3" for i in range(12)], reason=reason
                )
                self._scan(p)
                self.assertEqual(c["scrape"], mls._ABORT_AFTER_TRANSPORT)
                self.assertEqual(p.get_data("failed"), {})
                runs = p.get_data("runs")
                self.assertTrue(runs["aborted"])
                self.assertEqual(runs["transport"], mls._ABORT_AFTER_TRANSPORT)

    def test_non_numeric_retries_does_not_wedge_the_round(self):
        """账本里的 retries 不是数字（手工编辑、写到一半的 JSON）不能带走整轮。

        它是 int(prev.get("retries", 0)) 直接吃的值：抛在逐文件循环里等于整轮中止，
        而崩的位置由账本键序决定，下一轮枚举走到同一条路径还是同一个位置 —— 那之后的
        文件永远处理不到、汇总永远不落账，/status 也一起 500。
        """
        p, tmp, c = self._plugin({f"{i}.mp3": "ok" for i in range(4)})
        p.save_data("failed", {
            str(tmp / "0.mp3"): {"retries": "三次"},
            str(tmp / "1.mp3"): {"retries": None},
        })
        self._scan(p)
        self.assertEqual(c["scrape"], 4)
        self.assertEqual(len(p.get_data("done")), 4)
        self.assertEqual(p.get_data("failed"), {})
        self.assertEqual(p._build_status()["counts"]["failed"], 0)

    def test_raising_file_does_not_take_the_round_down(self):
        """宿主 ScrapingChain 内部异常只该影响那一个文件：一轮跑到第七千个文件崩掉，
        前面六千多个判断还没落账（每 60 秒才落一次），下一轮又崩在同一条路径上。"""
        p, tmp, c = self._plugin({f"{i}.mp3": "ok" for i in range(6)})

        def flaky(fp, tags=None, overwrite=False):
            c["scrape"] += 1
            if Path(fp).name == "2.mp3":
                raise RuntimeError("宿主内部异常")
            return (True, "", 3, "netease")

        setattr(p, _SCRAPE, flaky)
        self._scan(p)
        self.assertEqual(c["scrape"], 6)
        runs = p.get_data("runs")
        self.assertEqual(runs["error"], 1)
        self.assertEqual(runs["transport"], 0)
        self.assertEqual(len(p.get_data("done")), 5)
        # 插件对这个文件什么也没学到：不记失败、不涨 retries，下一轮照原样再来
        self.assertNotIn(str(tmp / "2.mp3"), p.get_data("failed"))
        self.assertEqual(runs["pending"], 1)
        self.assertEqual(len(p._collect_rows("pending")), runs["pending"])

    def test_raising_files_break_the_round_like_an_outage(self):
        """连着崩要按环境异常熔断，不能几千个文件一路崩到底。"""
        p, tmp, c = self._plugin({f"{i}.mp3": "ok" for i in range(12)})

        def always_raises(fp, tags=None, overwrite=False):
            c["scrape"] += 1
            raise RuntimeError("宿主内部异常")

        setattr(p, _SCRAPE, always_raises)
        self._scan(p)
        self.assertEqual(c["scrape"], mls._ABORT_AFTER_TRANSPORT)
        runs = p.get_data("runs")
        self.assertTrue(runs["aborted"])
        self.assertEqual(runs["error"], mls._ABORT_AFTER_TRANSPORT)
        self.assertEqual(p.get_data("failed"), {})
        self.assertEqual(runs["pending"], 12)

    def test_progress_flushed_mid_scan(self):
        """整轮动辄几小时：中途就得落账，容器重启不能把已刮好的部分全丢掉。"""
        p, tmp, c = self._plugin({f"{i}.mp3": "ok" for i in range(6)})
        saves = []
        real_save = p.save_data

        def counting_save(key, value, plugin_id=None):
            saves.append((key, len(value) if isinstance(value, dict) else value))
            return real_save(key, value, plugin_id)

        p.save_data = counting_save
        # plugindata 是单行 JSON，每写一次都要整本重新序列化：落账按时间而不是按条数
        orig = mls._FLUSH_INTERVAL
        mls._FLUSH_INTERVAL = 0.0
        try:
            self._scan(p)
        finally:
            mls._FLUSH_INTERVAL = orig
        done_saves = [n for k, n in saves if k == "done"]
        self.assertGreater(len(done_saves), 1)
        self.assertTrue(any(n > 0 for n in done_saves[:-1]), done_saves)

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
        self.assertIn("deficient", status["counts"])

    def test_status_groups_failures_by_reason(self):
        """失败按原因聚合：调阈值是"这一类有几个、这类里最高几分"的决定，逐行 reason
        得一页页翻，翻完还得自己数 —— 账本里本来就有 reason/score，界面上却凑不出结论。"""
        p, tmp, _c = self._plugin({"a.mp3": "ok"})
        p.save_data("failed", {
            str(tmp / "a.mp3"): {"retries": 1, "reason": "未达阈值（最低匹配分数 3）", "score": 2},
            str(tmp / "b.mp3"): {"retries": 1, "reason": "未达阈值（最低匹配分数 3）", "score": 1},
            str(tmp / "c.mp3"): {
                "retries": mls.MusicLibraryScraper.MAX_RETRY, "reason": "MTW 无候选",
            },
        })
        groups = p._build_status()["failure_reasons"]
        self.assertEqual([g["count"] for g in groups], [2, 1])
        self.assertEqual(groups[0]["best_score"], 2)
        self.assertEqual(groups[0]["retryable"], 2)
        # 全是超限的那一类要看得出来：定时扫描已经不会再碰它，只有手动重试还它一个机会
        self.assertEqual(groups[1]["retryable"], 0)
        self.assertIsNone(groups[1]["best_score"])

    def test_status_command_shows_the_reason_breakdown(self):
        p, tmp, _c = self._plugin({"a.mp3": "ok"})
        p._min_score = 3
        p.save_data("failed", {
            str(tmp / "a.mp3"): {"retries": 1, "reason": "未达阈值（最低匹配分数 3）", "score": 2},
        })
        p._cmd_status()
        text = p.messages[-1]["text"]
        self.assertIn("未达阈值（最低匹配分数 3） 1 个", text)
        self.assertIn("阈值降 1 分可命中", text)
        # runs 的 error 档与"下次触发在周几"也是同一类问题的两处出口
        self.assertIn("处理异常", text)
        self.assertIn("下次", text)

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

    def test_pending_bucket_is_cached(self):
        """待处理桶 = 全盘遍历做差集，是这几个接口里最贵的一档。

        数据页翻页/搜索/刷新会反复打它，短时间缓存一份、扫描开始与结束时作废。
        """
        p, tmp, c = self._plugin({"a.mp3": "ok", "b.mp3": "fail", "z.mp3": "fail"})
        self._scan(p)

        class _Req:
            def __init__(self, q):
                self.query_params = q

        walks = []
        orig = p._enumerate_files

        def counting_walk():
            walks.append(1)
            return orig()

        p._enumerate_files = counting_walk
        try:
            first = p.api_files(_Req({"bucket": "pending"}))
            second = p.api_files(_Req({"bucket": "pending", "offset": "1"}))
        finally:
            p._enumerate_files = orig
        self.assertEqual(len(walks), 1)
        self.assertEqual(first["total"], second["total"])
        # 扫描会改两本账，差集随之失效：下一轮开始前必须重新遍历
        self._scan(p)
        p._enumerate_files = counting_walk
        try:
            p.api_files(_Req({"bucket": "pending"}))
        finally:
            p._enumerate_files = orig
        self.assertEqual(len(walks), 2)

    def test_targeted_retry_summary_does_not_clobber_runs(self):
        """定向重试只覆盖几个文件：写进 runs 会把整盘汇总刷成 scanned=1、pending=0，
        数据页看起来就像整库已经扫完了。"""
        p, tmp, c = self._plugin({"a.mp3": "ok", "z.mp3": "fail"})
        self._scan(p)
        whole = dict(p.get_data("runs"))
        self.assertEqual(whole["scanned"], 2)
        self._scan(p, force=True, target_paths=[str(tmp / "z.mp3")])
        self.assertEqual(p.get_data("runs"), whole)
        targeted = p.get_data("last_targeted")
        self.assertTrue(targeted["targeted"])
        self.assertEqual(targeted["scanned"], 1)

    def test_retry_keeps_configured_write_mode(self):
        """定向重试的 force 只是绕过增量账和重试上限，不该顺带覆盖手工改过的标签。"""
        seen = []

        def record(fp, tags=None, overwrite=False):
            seen.append(overwrite)
            return (True, "", 3, "netease")

        p, tmp, c = self._plugin({"z.mp3": "ok"})
        setattr(p, _SCRAPE, record)
        p._force_all = False
        self._scan(p, force=True, target_paths=[str(tmp / "z.mp3")])
        self.assertEqual(seen, [False])
        # 界面上的"全量覆盖"才真的覆盖写
        seen.clear()
        self._scan(p, force=True, overwrite=True)
        self.assertEqual(seen, [True])
        # "强制覆盖"模式两样都开
        seen.clear()
        p._force_all = True
        self._scan(p)
        self.assertEqual(seen, [True])

    def test_tags_parsed_once_per_file(self):
        """mutagen 读一次不便宜：一轮里补空判定和匹配身份要用同一份解析结果。"""
        p, tmp, c = self._plugin({"a.mp3": "ok"})
        reads = []
        empty = {"title": "", "artist": "", "album": "", "duration": None,
                 "has_cover": False}

        def counting_read(fp):
            reads.append(1)
            return empty

        def with_tags(fp, tags=None, overwrite=False):
            # 补空判定已经把解析结果传进来了，身份匹配必须复用而不是重读
            assert tags is not None
            return (True, "", 3, "netease")

        def complete(fp, tags=None):
            assert tags is not None
            return False

        orig = mls._read_music_tags
        mls._read_music_tags = counting_read
        setattr(p, _SCRAPE, with_tags)
        setattr(p, _HAS_TAGS, complete)
        try:
            self._scan(p)
        finally:
            mls._read_music_tags = orig
        self.assertEqual(len(reads), 1)

    def test_enumerate_does_not_follow_symlinked_dirs(self):
        """rglob 会跟着目录软链走：库里放一个指向 / 或自身的软链就是无限遍历。"""
        tmp = Path(tempfile.mkdtemp())
        (tmp / "real.mp3").write_bytes(b"x")
        loop = tmp / "loop"
        try:
            loop.symlink_to(tmp, target_is_directory=True)
        except OSError:
            self.skipTest("当前环境不支持创建目录软链")
        p, _tmp, _c = self._plugin({"x.mp3": "ok"})
        p._scraper_paths = str(tmp)
        found = p._enumerate_files()
        self.assertEqual([f.name for f in found], ["real.mp3"])

    def test_enumerate_does_not_follow_symlinked_audio_files(self):
        """软链目录挡了，软链**文件**没挡：库内一个指向库外的 .flac 照样会被写标签。

        宿主 ScrapingChain 是按插件递过去的路径写文件的，所以"只处理库内"这条边界
        必须落在 realpath 上；/retry 已经用同一份基准拒收库外路径，扫描侧不能再放行。
        """
        out_dir = Path(tempfile.mkdtemp())
        (out_dir / "库外.mp3").write_bytes(b"x")
        lib2 = Path(tempfile.mkdtemp())
        (lib2 / "另一库.mp3").write_bytes(b"x")

        lib = Path(tempfile.mkdtemp())
        (lib / "普通.mp3").write_bytes(b"x")
        try:
            (lib / "越界软链.mp3").symlink_to(out_dir / "库外.mp3")
            (lib / "库内软链.mp3").symlink_to(lib2 / "另一库.mp3")
        except OSError:
            self.skipTest("当前环境不支持创建文件软链")
        p, _tmp, _c = self._plugin({"x.mp3": "ok"})
        p._scraper_paths = f"{lib}\n{lib2}"
        p._exclude_paths = ""
        names = sorted(f.name for f in p._enumerate_files())
        # 指向已配置库目录的软链保留（同一个库挂两个路径是常见做法）；指向库外的剔除
        self.assertEqual(
            names, ["另一库.mp3", "库内软链.mp3", "普通.mp3"])

        # 库外那个软链即使被人硬塞进重试列表，/retry 那侧也要拒收（两边口径一致）
        self.assertFalse(p._is_library_path(str(lib / "越界软链.mp3")))
        self.assertTrue(p._is_library_path(str(lib / "库内软链.mp3")))

    def test_enumerate_honours_excluded_dir_reached_by_alias(self):
        """排除目录常配成同一目录的别名/软链形式：逐文件比字符串会整片漏掉。

        漏掉之后扫描照刮照写，而 /retry 与数据页待处理桶（都走 realpath）拒收同一批路径，
        用户看到的是"我排除了这个目录，它一半在待处理、一半在失败里"。
        """
        lib = Path(tempfile.mkdtemp())
        real = lib / "classical"
        real.mkdir()
        (real / "a.mp3").write_bytes(b"x")
        (lib / "b.mp3").write_bytes(b"x")
        alias = lib / "排除别名"
        try:
            alias.symlink_to(real, target_is_directory=True)
        except OSError:
            self.skipTest("当前环境不支持创建目录软链")
        p, _tmp, _c = self._plugin({"x.mp3": "ok"})
        p._scraper_paths = str(lib)
        p._exclude_paths = str(alias)
        self.assertEqual([f.name for f in p._enumerate_files()], ["b.mp3"])
        # 两侧同一份口径
        self.assertFalse(p._is_library_path(str(real / "a.mp3")))

    def test_enumerate_reports_skipped_outside_links(self):
        """库外软链被跳过时必须报数量：不然用户只看到"发现 N 个"比目录里的少，查不出为什么。"""
        out = Path(tempfile.mkdtemp())
        (out / "t.flac").write_bytes(b"x")
        lib = Path(tempfile.mkdtemp())
        try:
            (lib / "l1.flac").symlink_to(out / "t.flac")
            (lib / "l2.flac").symlink_to(out / "t.flac")
        except OSError:
            self.skipTest("当前环境不支持创建文件软链")
        p, _tmp, _c = self._plugin({"x.mp3": "ok"})
        p._scraper_paths = str(lib)
        p._exclude_paths = ""
        warns = []
        orig = mls.logger.warning
        mls.logger.warning = lambda *a, **k: warns.append(str(a))
        try:
            self.assertEqual(p._enumerate_files(), [])
        finally:
            mls.logger.warning = orig
        self.assertTrue(any("跳过 2 个指向库外的音频软链" in w for w in warns), warns)

    def test_enumerate_skips_trickplay_dirs(self):
        """Jellyfin 的 .trickplay 预览缓存不是音乐，扫它只会白打请求、写坏账本。"""
        tmp = Path(tempfile.mkdtemp())
        (tmp / "a.mp3").write_bytes(b"x")
        cache = tmp / "专辑" / ".trickplay"
        cache.mkdir(parents=True)
        (cache / "cover.jpg").write_bytes(b"x")
        (cache / "fake.mp3").write_bytes(b"x")
        p, _tmp, _c = self._plugin({"x.mp3": "ok"})
        p._scraper_paths = str(tmp)
        self.assertEqual([f.name for f in p._enumerate_files()], ["a.mp3"])


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

    def test_dow_seven_is_sunday_not_a_whole_expression_fallback(self):
        """按 crontab 习惯写的星期日 7 就地换成 sun：调度器拒 7，但用户想说的就是周日。

        整条回落默认会连人配的时刻一起换掉，那是"我明明设了 04:30"的第二种丢法。
        """
        p = self._plugin()
        self.assertEqual(p._build_cron_trigger("0 3 * * 7"), ("crontab", "0 3 * * sun"))
        self.assertEqual(p._build_cron_trigger("30 4 * * 7"), ("crontab", "30 4 * * sun"))

    def test_numeric_dow_is_passed_through_untouched(self):
        """0-6 不做"猜意图"的位移：调度器把它读成周几是调度器的事，插件不偷偷改表达式。"""
        p = self._plugin()
        self.assertEqual(p._build_cron_trigger("0 3 * * 6"), ("crontab", "0 3 * * 6"))

    def test_default_cron_names_the_weekday(self):
        """默认值用星期名而不是数字：APScheduler 的 0 是周一，"0 3 * * 0" 这个"每周日"
        实际跑在周一 —— crontab 与调度器的星期编号差一位，名字没有歧义。"""
        self.assertEqual(mls._DEFAULT_CRON.split()[4], "sun")

    def test_cron_text_shows_which_weekday_the_scheduler_picked(self):
        """回显带上周几：光看表达式猜不出调度器读出来的日子。"""
        p = self._plugin()
        real = mls.CronTrigger

        class _Fire:
            def strftime(self, fmt):
                return "2026-10-18 03:03 (Sun)"

        class _Trigger:
            def get_next_fire_time(self, prev, now):
                return _Fire()

        class _Fake:
            @staticmethod
            def from_crontab(expr):
                return _Trigger()

        p._cron = "3 3 * * 6"
        mls.CronTrigger = _Fake
        try:
            self.assertEqual(p._cron_text(), ("3 3 * * 6", "2026-10-18 03:03 (Sun)"))
        finally:
            mls.CronTrigger = real

    def test_cron_text_does_not_collapse_without_the_next_fire_api(self):
        """拿不到下次时间就说"未知"：回显是给人看的，不该把定时任务本身挡掉。"""
        p = self._plugin()
        self.assertEqual(p._cron_text(), (mls._DEFAULT_CRON, "未知"))

    def test_cron_text_admits_the_fallback(self):
        p = self._plugin()
        p._cron = "not a cron"
        raw, text = p._cron_text()
        self.assertEqual(raw, mls._DEFAULT_CRON)
        self.assertIn("未生效", text)

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
        self.assertEqual({a["path"] for a in apis}, {"/status", "/files", "/retry", "/scan", "/edit"})
        self.assertTrue(all(a["auth"] == "bear" for a in apis))

    def test_data_page_translates_footer_and_status(self):
        """数据页不能漏 Vuetify 默认英文文案，且页脚文案必须走 Vuetify 3 的直传 props。

        footerProps 是 Vuetify 2 的 API：v3 把 makeVDataTableFooterProps 平铺进 VDataTable
        自己（VDataTableFooter.filterProps(props) 再挑出来），写成 :footer-props 会被当成
        未知属性丢掉，页脚一直是英文。
        """
        page = (Path(mls.__file__).parent / "frontend" / "src" / "components" / "Page.vue"
                ).read_text(encoding="utf-8")
        self.assertNotIn("footer-props", page)
        for phrase in ("items-per-page-text", "每页条数", "page-text", "items-per-page-options"):
            self.assertIn(phrase, page)

    def test_data_page_paging_and_bucket_bindings(self):
        """服务端分页的四处收口：越界页码、桶选中态、桶名真的传出去、重试提示。

        1) 桶变小（重试成功后失败项少了几十条）会让 offset 越界，拿回空页却显示
           "该桶暂无数据"，看着像数据没了；
        2) 选中态不能写死在组件内部（v-tabs 写死 model-value 时 bucket 变了高亮不动），
           侧栏同理要 :active="view === item.key" 由页面自己持有；
        3) 宿主注入的 api.get 只认第一个参数，第二个对象不会变成查询串而是被丢掉 ——
           写成 api.get(url, {bucket}) 时三个桶发出的都是裸 /files，后端按默认值回 done
           的第一页，界面表现就是"切换分桶数据一模一样"；
        4) doRetryAll 自己拼提示会把 out_of_library / no_paths 都说成"稍后再试"。
        """
        page = (Path(mls.__file__).parent / "frontend" / "src" / "components" / "Page.vue"
                ).read_text(encoding="utf-8")
        self.assertIn("Math.ceil(total.value / limit.value)", page)
        self.assertIn("loadFiles(false)", page)
        self.assertNotIn('model-value="failed"', page)
        self.assertIn(':active="view === item.key"', page)
        self.assertIn('@click="selectView(item.key)"', page)
        self.assertNotIn('@click="switchTab', page)
        self.assertEqual(page.count("已提交重试"), 1)
        self.assertIn("retryResult(res, '失败项')", page)
        # 定向重试不再覆盖 runs，界面要另起一行显示，否则看着像三天没扫
        self.assertIn("last_targeted", page)

        # 分桶条件必须拼进 URL：.get(url, params) 这种写法会被宿主静静丢掉
        self.assertIn("encodeURIComponent(key)", page)
        self.assertIn("bucket: bucket.value", page)
        for call in re.findall(r"\)\.get\(([^)]*)\)", page):
            self.assertNotIn(", {", call, f"api.get 不能带第二个参数: {call}")
        self.assertIn("/files${query({", page)

    def test_pages_share_one_flow_description(self):
        """流水线只能有一份：改了判据却只改一个页面，两个页面就会互相打脸。"""
        comp = Path(mls.__file__).parent / "frontend" / "src" / "components"
        flow = (comp / "flow.ts").read_text(encoding="utf-8")
        page = (comp / "Page.vue").read_text(encoding="utf-8")
        cfg = (comp / "Config.vue").read_text(encoding="utf-8")
        self.assertIn("FLOW_STEPS", flow)
        self.assertIn("from './flow'", page)
        self.assertIn("from './flow'", cfg)
        # 配置页先讲流程再讲具体项：这插件每个开关最后都改写音乐文件且不可回滚
        self.assertIn("pane === 'flow'", cfg)
        self.assertLess(cfg.index("流程总览"), cfg.index("运行与写入"))
        # 侧栏版式向宿主申请宽度（宿主对话框默认 60rem 装不下侧栏 + 分页表）
        self.assertIn("emit('layout'", page)
        self.assertIn("emit('layout'", cfg)

    def test_data_page_columns_are_not_sortable(self):
        """/files 恒按路径排序，表头留着箭头就是骗人：点一下只重排眼前这一页 50 条。

        真要排序得把 sort 参数透传给服务端并在 rows.sort 里按列取键；库里几千上万条时
        "按大小排序"只排当前页，用户看到的是一张既不全、又和总数对不上的表。
        """
        page = (Path(mls.__file__).parent / "frontend" / "src" / "components" / "Page.vue"
                ).read_text(encoding="utf-8")
        headers = page.split(":headers=\"[", 1)[1].split("]\"", 1)[0]
        cols = re.findall(r"\{[^{}]+\}", headers)
        self.assertGreaterEqual(len(cols), 6)
        for col in cols:
            self.assertIn("sortable: false", col, col)

        # 服务端确实只有路径这一种顺序（改动这里时，上面那条要一起重看）
        src = Path(mls.__file__).read_text(encoding="utf-8")
        self.assertIn('rows.sort(key=lambda r: r["path"])', src)

    def test_config_page_explains_choices(self):
        """易误解的选项必须在配置页讲清楚：留空=全部源、选取排序规则、两档写入模式的后果。"""
        plugin_dir = Path(mls.__file__).parent
        src = Path(mls.__file__).read_text(encoding="utf-8")
        cfg = (plugin_dir / "frontend" / "src" / "components" / "Config.vue").read_text(
            encoding="utf-8"
        )
        # 原生表单与 Vue 配置页都要有
        self.assertIn("留空=当前来源全部", src)
        self.assertIn("留空 = 全部源", cfg)
        for phrase in ("只补缺", "强制覆盖", "宿主", "同分时", "目录结构兜底"):
            self.assertIn(phrase, src)
            self.assertIn(phrase, cfg)
        # 覆盖后果不能只写"会覆盖"，要说清哪些字段会被替换、哪些不会
        self.assertIn("手工改过", cfg)
        self.assertIn("不会清空", cfg)
        # 阈值范围要与服务端夹紧一致，不能再写 0-3 诱导用户填出不可逆的 0
        for surface in (src, cfg):
            self.assertNotIn("（0-3）", surface)
            self.assertIn("（1-3）", surface)
        for phrase in ("1-3", "回滚"):
            self.assertIn(phrase, src)
            self.assertIn(phrase, cfg)
        # 宿主白名单这条要写在配置页：不写的话用户只会奇怪 .wv 怎么永远刮失败
        for phrase in ("音频白名单", ".trickplay", "软链"):
            self.assertIn(phrase, src)
            self.assertIn(phrase, cfg)
        # 识别来源两条路都要在两个表面说破：谁能用酷我/smart_tag、要不要 MTW，
        # 只写在一处会让人配完才发现内置源没有那个源
        for phrase in ("识别来源", "内置源", "免部署"):
            self.assertIn(phrase, src)
            self.assertIn(phrase, cfg)
        # 隐私边界只能写在配置页上，用户选内置源时看的就是这里
        self.assertIn("库内路径不出本机", cfg)

    def test_config_notes_are_consolidated(self):
        """说明集中在页面底部一个块里，不跟在单个配置项下面（persistent-hint 会撑坏排版）。"""
        cfg = (Path(mls.__file__).parent / "frontend" / "src" / "components" / "Config.vue"
               ).read_text(encoding="utf-8")
        self.assertNotIn("persistent-hint", cfg)
        self.assertNotIn('hint="', cfg)
        # 原生表单同理：说明只在 VAlert 里
        form = Path(mls.__file__).read_text(encoding="utf-8").split("def get_form", 1)[1]
        self.assertNotIn('"hint"', form)

    def test_reason_names_the_missing_field_in_ui(self):
        """原因得说得出差在哪一项：光看 2/3 分分不清该降阈值还是该把文件排出扫描范围。"""
        comp = Path(mls.__file__).parent / "frontend" / "src" / "components"
        flow = (comp / "flow.ts").read_text(encoding="utf-8")
        page = (comp / "Page.vue").read_text(encoding="utf-8")
        for phrase in ("曲名未对上", "本地无", "整张 CD"):
            self.assertIn(phrase, flow)
        # 概览那句提示不能只教人降阈值：本地缺这一项的文件降了也救不回来
        self.assertIn("本地无", page)
        self.assertIn("排出", page)

    def test_dead_select_mode_option_removed(self):
        """mtw_select_mode 从不进请求体、也不是 MTW 入参，属于死配置，不能复活。"""
        src = Path(mls.__file__).read_text(encoding="utf-8")
        cfg = (Path(mls.__file__).parent / "frontend" / "src" / "components" / "Config.vue"
               ).read_text(encoding="utf-8")
        # 文档里可以提这段历史，但属性、表单项和默认值都不能再有它
        self.assertNotIn("_mtw_select_mode", src)
        self.assertNotIn("select_mode", cfg)
        _, defaults = mls.MusicLibraryScraper().get_form()
        self.assertNotIn("mtw_select_mode", defaults)
        self.assertNotIn("候选排序", src)

    def test_icon_is_vendored_in_repo(self):
        """图标必须走本仓库 raw 地址，不能热链 MTW 实例（实例下线会连带市场图标一起裂）。"""
        self.assertTrue(
            mls.MusicLibraryScraper.plugin_icon.startswith(
                "https://raw.githubusercontent.com/JontyLee/MoviePilot-Plugins/main/icons/"
            )
        )

    def test_builtin_sources_credit_upstream_and_license(self):
        """内置源是把 Music Tag Web 的调用约定接过来用的：出处与许可要写在代码和 README 里。

        MTW 是 GPL-3.0、本仓库同为 GPL-3.0，引用与再分发本身没有障碍，缺的是说明；
        MusicBrainz 只取核心数据（CC0）这件事也必须写出来，否则读代码的人无从判断边界。
        """
        root = Path(mls.__file__).resolve().parents[2]
        sources = (Path(mls.__file__).parent / "sources.py").read_text(encoding="utf-8")
        readme = (root / "README.md").read_text(encoding="utf-8")
        for surface in (sources, readme):
            self.assertIn("music-tag-web", surface)
            self.assertIn("GPL-3.0", surface)
            self.assertIn("CC0", surface)
        # 仓库自己的许可要真是 GPL-3.0，上面那句"同等条款下可以引用"才成立
        self.assertIn("GNU GENERAL PUBLIC LICENSE", (root / "LICENSE").read_text(encoding="utf-8"))
        self.assertIn("Version 3", (root / "LICENSE").read_text(encoding="utf-8"))
        self.assertIn("上游、许可与引用", readme)

    def test_mtw_endpoints_use_apimt_prefix(self):
        """MTW 的 Django 路由挂在 /apimt/ 下，写成 /api/ 会一律 404。"""
        src = Path(mls.__file__).read_text(encoding="utf-8")
        self.assertIn("/apimt/token/", src)
        self.assertIn("/apimt/{endpoint}/", src)
        self.assertIn('self._mtw_request("fetch_id3_by_title"', src)
        self.assertNotIn('"/api/', src)
        self.assertNotIn('/api/token/', src)
        self.assertNotIn('/api/fetch_id3_by_title/', src)


class MatchTest(unittest.TestCase):
    """候选比对这些坑：繁简之外的空格/标点差异、文件名当搜索词、tag 读不到时的目录兜底。"""

    def _plugin(self, min_score=3, resources="netease", backend="mtw"):
        p = mls.MusicLibraryScraper()
        p._min_score = min_score
        # 故意传字符串：验证旧配置（单选）也能跑
        p._mtw_resources = resources
        p._mtw_base_url = "http://mtw:8002"
        p._mtw_username = ""
        p._mtw_password = ""
        # 后端与真正要问的源分开指定：类默认值是内置源，而这批用例描述的是 MTW 那条链路，
        # 不写清楚就会拿 MTW 形状的回答去喂内置源解析器，测出来的是形状不匹配而不是被测行为。
        p._backend = backend
        builtin = backend == "builtin"
        p._active_resources = mls._normalize_resources(
            resources,
            known=mls._sources.RESOURCES if builtin else None,
            default=mls._sources.RESOURCES if builtin else None,
        )
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

    def test_path_identity_ignores_library_root_and_above(self):
        """库根自己的名字不是专辑，它上面一层更不是艺人。

        不 clamp 时 /media/music/01 - 轨迹.flac 会得到 album="music"、artist=<库根的上一级>，
        这两个假身份既会当搜索词发给 MTW，比对又是双向包含，一个目录名就能白送一个匹配分。
        """
        roots = [Path("/media/music")]
        flat = Path("/media/music/01 - 轨迹.flac")
        self.assertEqual(
            mls._path_identity(flat, roots),
            {"title": "轨迹", "artist": "", "album": ""},
        )
        one = Path("/media/music/七里香 (2003)/01 - 七里香.flac")
        self.assertEqual(mls._path_identity(one, roots)["album"], "七里香")
        self.assertEqual(mls._path_identity(one, roots)["artist"], "")
        two = Path("/media/music/周杰伦/七里香 (2003)/01 - 七里香.flac")
        self.assertEqual(
            mls._path_identity(two, roots),
            {"title": "七里香", "artist": "周杰伦", "album": "七里香"},
        )
        # 不给 roots 保持老行为：调用方可以只看文件名兜底
        self.assertEqual(mls._path_identity(one)["artist"], "music")

    def test_root_dir_name_no_longer_buys_a_free_match_point(self):
        """上一条的实际后果：库根名凑出来的 album 能把 1 分抬成 3 分。"""
        roots = [Path("/media/music")]
        flat = Path("/media/music/01 - 轨迹.flac")
        blank = {"title": "", "artist": "", "album": "", "duration": None}
        want = mls._want_from(mls._local_identity(flat, blank, roots=roots))
        noisy = {"name": "轨迹", "artist": "Media", "album": "music"}
        self.assertEqual(mls.MusicLibraryScraper._score_candidate(noisy, want), 1)
        # 没有 clamp 时 artist="media"、album="music" 都是从库根及其上一级目录名凑出来的，
        # 同一份候选刚好凑满 3 分并被写进文件（写标签不可回滚）
        unchecked = mls._want_from(mls._local_identity(flat, blank))
        self.assertEqual(unchecked["artist"], "media")
        self.assertEqual(mls.MusicLibraryScraper._score_candidate(noisy, unchecked), 3)

    def test_identity_roots_are_the_configured_paths(self):
        """clamp 用的边界是配置原文：库根本身是软链时 realpath 会让每个文件都判成库外。"""
        p = self._plugin()
        p._scraper_paths = "/media/music\n/data/另一库/\n\n"
        self.assertEqual(
            p._identity_roots(), [Path("/media/music"), Path("/data/另一库")]
        )
        # 按配置原文缓存，改过配置立刻换一份，不需要 TTL
        again = p._identity_roots()
        self.assertIs(again, p._identity_roots())
        p._scraper_paths = "/srv/musik"
        self.assertEqual(p._identity_roots(), [Path("/srv/musik")])

    def test_read_music_tags_never_raises(self):
        out = mls._read_music_tags(Path("/nonexistent/dir/01 - x.flac"))
        self.assertEqual(
            out,
            {
                "title": "", "artist": "", "album": "", "year": "", "tracknumber": "",
                "duration": None, "has_cover": False, "has_lyrics": False,
            },
        )

    def test_cover_detected_through_id3_repeated_frame(self):
        """ID3 的 APIC 是可重复帧，mutagen 的取法是 getall("APIC")，按键直取会落空。

        判不出封面比判不出来更贵：标签已经刮齐、只差封面的库（真实曲库最常见的剩余缺口）
        会被补空模式永远判成"没齐"，每轮重打最多 7 次 MTW 请求，候选不达标还白涨 retries，
        几轮之后一个本来完整的文件被永久列成超限。
        """
        class Id3:
            def __init__(self, frames):
                self._frames = frames

            def get(self, key, default=None):
                return self._frames.get(key, default)

            def keys(self):
                return list(self._frames)

            def getall(self, frameid):
                return [v for k, v in self._frames.items()
                        if k.split(":", 1)[0] == frameid]

        covered = Id3({"APIC:Cover (front)": object(), "TALB": "黑白灰"})
        self.assertTrue(mls._has_embedded_cover(object(), covered))
        # 按帧名直取拿不到（正是插件此前唯一依赖的读法）
        self.assertIsNone(covered.get("APIC"))
        self.assertFalse(mls._has_embedded_cover(object(), Id3({"TALB": "黑白灰"})))

    def test_cover_detected_in_every_other_container(self):
        """其余容器各有各的读法：任何一档判错都会让补空模式反复重刮。"""
        class NoGetall(dict):
            """Vorbis/APE/MP4 的 tag 容器没有 getall，helper 不能因为它就整段放弃。"""

        flac_like = NoGetall({"metadata_block_picture": "ZgBsAG8AZw=="})
        self.assertTrue(mls._has_embedded_cover(object(), flac_like))
        self.assertTrue(mls._has_embedded_cover(
            object(), NoGetall({"Cover Art (Front)": (b"front", b"jpegdata")})))
        self.assertTrue(mls._has_embedded_cover(object(), NoGetall({"covr": [b"\xff\xd8"]})))
        # 通用图片接口挂在文件对象上（FLAC/MP4 这一类）
        class HasPictures:
            pictures = [object()]

        self.assertTrue(mls._has_embedded_cover(HasPictures(), NoGetall()))
        self.assertFalse(mls._has_embedded_cover(NoGetall({"title": "黑面"}), None))

    def test_read_music_tags_reports_embedded_cover(self):
        """整条 tag 读取链路此前没有测试：环境里没有 mutagen，_read_music_tags 一律返回
        空值、has_cover 恒为 False，补空判据实际上从没被验证过。"""
        import types

        class Tags(dict):
            def getall(self, frameid):
                return [object()] if frameid == "APIC" else []

        holder = types.SimpleNamespace(
            tags=Tags({"TIT2": "黑面", "TPE1": "陈奕迅", "TALB": "黑白灰"}),
            info=None,
        )
        fake = types.ModuleType("mutagen")
        fake.File = lambda path: holder
        orig = sys.modules.get("mutagen")
        sys.modules["mutagen"] = fake
        try:
            out = mls._read_music_tags(Path("/music/07 - 黑面.flac"))
        finally:
            if orig is None:
                sys.modules.pop("mutagen", None)
            else:
                sys.modules["mutagen"] = orig
        self.assertEqual(out["title"], "黑面")
        self.assertEqual(out["artist"], "陈奕迅")
        self.assertTrue(out["has_cover"])

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

    def test_field_hits_separates_miss_from_missing_local(self):
        """2/3 分必须能说出差在哪一项、是"没对上"还是"本地根本没这项"。

        只看分数做不出决定：曲名没对上要去改文件名或放宽阈值，本地读不出曲名
        （整张 CD 压成一个 wav）则该把这类文件排出扫描范围 —— 两种提示相反。
        """
        hits = mls.MusicLibraryScraper._field_hits
        want = {"title": "黑面", "artist": "陈奕迅", "album": "黑白灰"}
        self.assertEqual(
            hits({"name": "黑面", "artist": "陈奕迅", "album": "The Line-Up"}, want),
            {"title": mls._HIT, "artist": mls._HIT, "album": mls._MISS},
        )
        # 本地缺专辑：这不是候选的错，说成"未对上"会把人支去改文件名
        no_album = {"title": "黑面", "artist": "陈奕迅", "album": ""}
        self.assertEqual(
            hits({"name": "黑面", "artist": "陈奕迅", "album": "黑白灰"}, no_album)["album"],
            mls._NO_LOCAL,
        )
        self.assertEqual(
            mls.MusicLibraryScraper._miss_text({"title": mls._HIT, "artist": mls._HIT,
                                                "album": mls._MISS}),
            "专辑未对上",
        )
        self.assertEqual(
            mls.MusicLibraryScraper._miss_text({"title": mls._MISS, "artist": mls._NO_LOCAL,
                                                "album": mls._HIT}),
            "曲名未对上、本地无艺人",
        )

    def test_scrape_reason_names_the_missing_field(self):
        """失败原因连着逐项判定写进账本：数据页每一行、/status 的分布都读这一句。"""
        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "陈奕迅" / "黑白灰" / "07 - 黑面.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        p = self._plugin(min_score=3)

        def _handler(url, body):
            return _Resp(200, {"result": True, "data": {
                "data": [{"name": "黑面", "artist": "陈奕迅", "album": "The Line-Up"}]
            }})

        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            okf, reason, score, _ = p._MusicLibraryScraper__scrape_file(fp)
        finally:
            mls.RequestUtils = orig
        self.assertFalse(okf)
        self.assertEqual(score, 2)
        self.assertIn("未达阈值（最低匹配分数 3）", reason)
        self.assertIn("专辑未对上", reason)

    def test_scrape_reason_distinguishes_no_local_title(self):
        """本地读不出曲名时一个请求都不发，原因也不能写成"MTW 无候选"。"""
        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "07 - .flac"
        fp.write_bytes(b"x")
        p = self._plugin(min_score=3)
        asked = []

        def _handler(url, body):
            asked.append(body)
            return _Resp(200, {"result": True, "data": []})

        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            okf, reason, _, _ = p._MusicLibraryScraper__scrape_file(fp)
        finally:
            mls.RequestUtils = orig
        self.assertFalse(okf)
        self.assertIn("本地读不出曲名", reason)
        self.assertNotIn("MTW 无候选", reason)

    def test_failed_file_leaves_a_log_line(self):
        """失败不能只在账本里留痕：一轮问满 7 个源可能静默跑上百分钟，看着像卡死。"""
        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "陈奕迅" / "黑白灰" / "07 - 黑面.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        p = self._plugin(min_score=3)
        p._scraper_paths = str(tmp)
        p._exclude_paths = ""
        p._force_all = False

        def _handler(url, body):
            return _Resp(200, {"result": True, "data": {
                "data": [{"name": "乱码", "artist": "别人", "album": "别的"}]
            }})

        lines = []
        orig_ru, orig_log = mls.RequestUtils, mls.logger

        class _Log:
            def __getattr__(self, name):
                return lambda msg, *a, **k: lines.append(str(msg))

        mls.RequestUtils = _ru(_handler)
        mls.logger = _Log()
        try:
            p._MusicLibraryScraper__scan(target_paths=[str(fp)])
        finally:
            mls.RequestUtils = orig_ru
            mls.logger = orig_log
        # 原因和重试次数一起进日志，不看数据页也能知道这个文件烧到了第几次
        self.assertTrue(any("未通过" in l and "未达阈值" in l and "1/5" in l for l in lines),
                        [l for l in lines if "未通过" in l])

    def test_fetch_candidates_queries_clean_title_not_filename(self):
        """搜索词必须是清洗后的曲名；传 "07 - 黑面.flac" 只会召回噪音。"""
        captured = {"n": 0}

        def _handler(url, body):
            captured["n"] += 1
            captured["url"] = url
            captured["body"] = body
            return _Resp(200, {"result": True, "data": []})

        tmp = Path(tempfile.mkdtemp())
        fp = tmp / "陈奕迅" / "黑白灰" / "07 - 黑面.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        p = self._plugin()
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        # 空候选是"可用响应 + 查无此曲"，不是 MTW 故障
        self.assertEqual(cands, [])
        self.assertIsNone(failure)
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

    def test_normalize_resources_drops_unknown(self):
        """不认识的源必须丢掉，也不能整体退化成"没配置所以全源"。

        一个必然报错的源（旧配置残留的 qq/all、或 API 写坏的数组）会让每个文件多打一次
        失败请求，而"有源失败"若被当成 MTW 故障，整轮会在前几个文件就熔断。
        """
        self.assertEqual(mls._normalize_resources("netease;qq"), ["netease"])
        self.assertEqual(mls._normalize_resources(["netease", "nope"]), ["netease"])
        # 全是未知值：说明这份配置整体不可信，回落全源比空转好
        self.assertEqual(mls._normalize_resources(["nope"]), list(mls._DEFAULT_RESOURCES))

    def test_partial_source_failure_is_not_an_outage(self):
        """有源正常应答（哪怕是空候选）就是确定答案"曲库查不到"，不能报成 MTW 故障。

        musicbrainz 限流、咪咕 500 这类单源故障很常见；若据此记 transport 并熔断，
        全库会永远停在待处理，且永远烧不掉 retries。
        """
        fp = self._track()

        def _handler(url, body):
            if body.get("resource") == "netease":
                return _Resp(200, {"result": True, "data": []})
            return _Resp(503, {"detail": "migu upstream error"})  # 这个源挂了

        p = self._plugin(resources=["netease", "qmusic"])
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(cands, [])
        self.assertIsNone(failure)

    def test_fetch_candidates_unwraps_paged_data(self):
        """MTW 有时把结果再包一层 {"data": {"data": [...]}}：内层也要认。"""
        fp = self._track()

        def _handler(url, body):
            return _Resp(200, {"result": True,
                               "data": {"data": [{"name": "黑面", "artist": "陈奕迅",
                                                  "album": "黑白灰"}]}})

        p = self._plugin(resources=["netease"])
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertIsNone(failure)
        self.assertEqual(len(cands), 1)
        self.assertEqual(cands[0]["resource"], "netease")

    def test_supported_audio_ext_follows_host_whitelist(self):
        """宿主写入链按 RMT_AUDIOEXT 过滤：它不收的扩展名不该进来白打 7 次请求。"""
        orig = mls._host_audio_extensions
        orig_cache = dict(mls._AUDIO_EXT_CACHE)
        mls._host_audio_extensions = lambda: [".mp3", ".flac", ".wv"]
        mls._AUDIO_EXT_CACHE.update(ts=0.0, exts=None)  # 结果缓存 5 分钟，测试要强制重算
        try:
            exts = mls._supported_audio_ext()
        finally:
            mls._host_audio_extensions = orig
            mls._AUDIO_EXT_CACHE.clear()
            mls._AUDIO_EXT_CACHE.update(orig_cache)
        self.assertIn(".mp3", exts)
        self.assertIn(".flac", exts)
        self.assertIn(".wv", exts)
        self.assertNotIn(".ape", exts)
        # 宿主只收一两个时，顺序仍按本地列表，且不能退化成"全都要"
        self.assertEqual(exts, tuple(e for e in mls._MUSIC_EXT if e in
                                    {".mp3", ".flac", ".wv"}))

    def test_supported_audio_ext_is_memoized(self):
        """宿主配置快照按需构建，/retry 上千路径与每个文件都要问一次：不能每次都去问。"""
        calls = []
        orig = mls._host_audio_extensions
        orig_cache = dict(mls._AUDIO_EXT_CACHE)
        mls._host_audio_extensions = lambda: calls.append(1) or [".mp3"]
        mls._AUDIO_EXT_CACHE.update(ts=0.0, exts=None)
        try:
            for _ in range(50):
                mls._supported_audio_ext()
            self.assertEqual(len(calls), 1)
            # 过期后重新取：白名单改了最迟下一轮生效
            mls._AUDIO_EXT_CACHE["ts"] = time.time() - mls._AUDIO_EXT_TTL - 1
            mls._supported_audio_ext()
            self.assertEqual(len(calls), 2)
        finally:
            mls._host_audio_extensions = orig
            mls._AUDIO_EXT_CACHE.clear()
            mls._AUDIO_EXT_CACHE.update(orig_cache)

    def test_host_audio_extensions_normalizes_setting(self):
        """RMT_AUDIOEXT 是用户可在界面改的配置：不带点、大写、数组都要能吃得下。

        "读不到"（属性不存在 / 宿主改版）与"读到了但被清空"必须是两个答案：
        都返回空列表时，调用方要么把白名单清空当成读不到去硬扫全库、要么把可用列表
        误判成空库，两个方向都错。前者用 None 表示。
        """
        orig = getattr(mls.settings, "RMT_AUDIOEXT", None)
        orig_cache = dict(mls._AUDIO_EXT_CACHE)
        mls._AUDIO_EXT_CACHE.update(ts=0.0, exts=None)
        try:
            mls.settings.RMT_AUDIOEXT = "mp3, .FLAC, WV"
            self.assertEqual(mls._host_audio_extensions(), [".mp3", ".flac", ".wv"])
            mls.settings.RMT_AUDIOEXT = ["flac", "ape"]
            self.assertEqual(mls._host_audio_extensions(), [".flac", ".ape"])
            # 没配 / 读不到：None，交给下面的兜底
            mls.settings.RMT_AUDIOEXT = None
            self.assertIsNone(mls._host_audio_extensions())
            self.assertEqual(mls._supported_audio_ext(), mls._MUSIC_EXT)
            # 明确清空：宿主一个音频都不收，插件也必须一个都不碰（不能退回本地列表硬扫）
            mls.settings.RMT_AUDIOEXT = ""
            self.assertEqual(mls._host_audio_extensions(), [])
            mls._AUDIO_EXT_CACHE.update(ts=0.0, exts=None)
            self.assertEqual(mls._supported_audio_ext(), ())
        finally:
            mls.settings.RMT_AUDIOEXT = orig
            mls._AUDIO_EXT_CACHE.clear()
            mls._AUDIO_EXT_CACHE.update(orig_cache)

    def test_min_score_clamped_server_side(self):
        """配置页的 min/max 只是浏览器属性：原生表单和 /api 能塞任意值，两头都不可逆。

        0 或负数 => 只对上曲名就整库改写标签（写进文件回不来）；
        大于满分 => 永远达不到阈值，整库烧满 retries 后永久跳过。
        """
        clamp = mls.MusicLibraryScraper._clamp_min_score
        # 越界按最近的一界执行：0/负数与 1 本就等价（0 分选不出候选）
        for too_low in (0, -1, "-3"):
            self.assertEqual(clamp(too_low), 1)
        for too_high in (4, 99, "99"):
            self.assertEqual(clamp(too_high), mls._MAX_SCORE)
        self.assertEqual(clamp(""), mls._MAX_SCORE)
        self.assertEqual(clamp(None), mls._MAX_SCORE)
        self.assertEqual(clamp("abc"), mls._MAX_SCORE)
        self.assertEqual(clamp("2"), 2)
        self.assertEqual(clamp(1), 1)
        self.assertEqual(clamp(3), 3)

    def _fake_mtw(self, by_resource, calls):
        def _handler(url, body):
            resource = body["resource"]
            calls.append(resource)
            return _Resp(200, {"result": True, "data": by_resource.get(resource, [])})

        return _ru(_handler)

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
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertIsNone(failure)
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
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertIsNone(failure)
        self.assertEqual(calls, ["netease", "qmusic"])
        picked, score = p._mtw_pick_best(fp, cands)
        self.assertEqual((score, picked["resource"]), (3, "qmusic"))

    def test_fetch_candidates_reports_mtw_failure_not_empty_result(self):
        """MTW 挂了/令牌失效时不能报"无候选"：那会被记成文件失败并烧掉重试额度。"""
        fp = self._track()

        def _handler(url, body):
            if "token" in url:
                return _Resp(200, {"token": "t"})
            # 请求发出去了但连上了个寂寞：post 只在 RequestException 时返回 None
            return None

        p = self._plugin(resources=["netease"])
        p._mtw_username, p._mtw_password = "u", "p"
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(cands, [])
        self.assertEqual(failure, mls._UNREACHABLE_REASON)

    def test_fetch_candidates_reauth_once_on_token_expired(self):
        """JWT 提前失效（HTTP 401 + DRF 错误体）：判成鉴权失败要强制重登一次再重试。

        这一条同时钉住本轮修掉的那个坑：宿主 post_json 里 `if response:` 走的是
        Response.__bool__（status < 400），401 的响应连状态码带错误体全被折成 None，
        插件只会记成"MTW 不可达"，一路 401 到扫完、还顺带把熔断触发一次。
        """
        fp = self._track()
        state = {"logins": 0, "calls": 0}

        def _handler(url, body):
            if "token" in url:
                state["logins"] += 1
                return _Resp(200, {"token": "tok%d" % state["logins"]})
            state["calls"] += 1
            if state["calls"] == 1:
                # DRF SimpleJWT 的过期响应：真·401，不是 200 夹带错误体
                return _Resp(401, {"detail": "Given token not valid for any token type",
                                   "code": "token_not_valid"})
            return _Resp(200, {"result": True, "data": [
                {"name": "黑面", "artist": "陈奕迅", "album": "黑白灰"}]})

        p = self._plugin(resources=["netease"])
        p._mtw_username, p._mtw_password = "u", "p"
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertIsNone(failure)
        self.assertEqual(len(cands), 1)
        self.assertEqual(state["logins"], 2)

    def test_mtw_post_maps_status_code_not_only_body(self):
        """状态码才是故障归因的依据：401 是鉴权、5xx 是接口报错、连不上才是不可达。

        宿主 RequestUtils.post_json 里那句 `if response:` 判的是 requests 的
        Response.__bool__（等价 status_code < 400），非 2xx 连响应体都拿不到，所以
        插件必须绕过它自己收 Response —— 这条就是钉住那个绕过还在。
        """
        # 整本源码里不许再出现对宿主那层的调用（注释也要避开这个写法，别改成绕路断言）
        src = Path(mls.__file__).read_text(encoding="utf-8")
        self.assertNotIn(".post_json(", src)
        self.assertNotIn(".post_dict(", src)
        self.assertIn(".post(url=url, json=body)", inspect.getsource(mls._mtw_post))

        for status, want in ((401, mls._AUTH_REASON), (403, mls._AUTH_REASON),
                             (500, mls._API_ERROR_REASON), (404, mls._API_ERROR_REASON)):
            resp = _Resp(status, {"detail": "boom"})
            with _patch_ru(lambda url, body: resp):
                payload, reason = mls._mtw_post("http://mtw:8002/apimt/token/", {}, {})
            self.assertEqual(reason, want, f"HTTP {status}")
            self.assertEqual(payload, {"detail": "boom"}, "错误体要留着，别丢")
            self.assertTrue(resp.closed, f"HTTP {status} 也得把连接关掉")

        # 2xx 但解不出对象（网关/登录页截了）：不能当成"曲库说查不到"
        with _patch_ru(lambda url, body: _Resp(200, "<html/>", bad_json=True)):
            self.assertEqual(mls._mtw_post("u", {}, {})[1], mls._UNREACHABLE_REASON)
        # 连不上：post 只在 RequestException 时返回 None
        with _patch_ru(lambda url, body: None):
            self.assertEqual(mls._mtw_post("u", {}, {})[1], mls._UNREACHABLE_REASON)
        # 正常空候选是可用响应
        with _patch_ru(lambda url, body: _Resp(200, {"result": True, "data": []})):
            self.assertIsNone(mls._mtw_post("u", {}, {})[1])

    def test_login_success_shape_is_not_judged_as_fetch_result(self):
        """/apimt/token/ 的成功响应本来就没有 data 字段：拿检索结果的形状去兜底会把
        一次成功登录说成"MTW 不可达"，日志与后续任何按 reason 分支的调用方都会跟着错。"""
        with _patch_ru(lambda url, body: _Resp(200, {"token": "t", "expired_at": 0})):
            payload, reason = mls._mtw_post(
                "http://mtw:8002/apimt/token/", {}, {}, expect_data=False)
        self.assertIsNone(reason)
        self.assertEqual(payload["token"], "t")
        # 检索接口仍然按形状兜底（200 夹带 DRF 错误体那种）
        with _patch_ru(lambda url, body: _Resp(200, {"detail": "x", "code": "token_not_valid"})):
            self.assertEqual(mls._mtw_post("u", {}, {})[1], mls._AUTH_REASON)

    def test_api_error_does_not_relogin(self):
        """接口报错（5xx）不是鉴权失败：不该顺手把令牌清掉重登一次。"""
        fp = self._track()
        state = {"logins": 0, "calls": 0}

        def _handler(url, body):
            if "token" in url:
                state["logins"] += 1
                return _Resp(200, {"token": "t"})
            state["calls"] += 1
            return _Resp(500, {"detail": "internal"})

        p = self._plugin(resources=["netease"])
        p._mtw_username, p._mtw_password = "u", "p"
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            cands, failure = p._mtw_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(cands, [])
        self.assertEqual(failure, mls._API_ERROR_REASON)
        self.assertEqual(state["calls"], 1)
        self.assertEqual(state["logins"], 1)

    def test_login_failure_is_backed_off(self):
        """账密错 / MTW 关了 /apimt/token/：每个文件、每个源都会先登一次，
        那是成倍增长的必然失败请求加一屏同样的错误日志。失败后要静默。"""
        p = self._plugin(resources=["netease"])
        p._mtw_username, p._mtw_password = "u", "wrong"
        state = {"logins": 0, "errors": 0}
        orig_error = mls.logger.error

        def _handler(url, body):
            if "token" in url:
                state["logins"] += 1
                return _Resp(401, {"detail": "no"})
            return _Resp(200, {"result": True, "data": []})

        mls.logger.error = lambda *a, **k: state.__setitem__(
            "errors", state["errors"] + 1)
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            for _ in range(200):
                self.assertIsNone(p._mtw_token())
            # 只登了一次，也只记了一条错误
            self.assertEqual(state["logins"], 1)
            self.assertEqual(state["errors"], 1)
            # 静默期过了才再试一次（把时间戳往前挪，不用真的等 60 秒）
            p.__dict__["_login_retry_at"] = time.time() - 1
            self.assertIsNone(p._mtw_token())
            self.assertEqual(state["logins"], 2)
            # 登录成功后立刻作废静默期：下一次令牌真过期时能马上重登
            mls.RequestUtils = _ru(lambda url, body: _Resp(200, {"token": "t"}))
            p.__dict__["_login_retry_at"] = time.time() + 9999
            p._token, p._token_expiry = None, 0.0
            # 静默期没到就还是登不上（这是它的作用），到期后成功登录会把它清零
            self.assertIsNone(p._mtw_token())
            p.__dict__["_login_retry_at"] = time.time() - 1
            self.assertEqual(p._mtw_token(), "t")
            self.assertEqual(p.__dict__["_login_retry_at"], 0.0)
        finally:
            mls.logger.error = orig_error
            mls.RequestUtils = orig

    def test_configured_login_backoff_window_is_used(self):
        """静默期是常量，别写成"下一次请求就再试"：一轮几千个文件要能挡住。"""
        self.assertGreaterEqual(mls._LOGIN_BACKOFF, 30.0)
        self.assertIn(mls._API_ERROR_REASON, mls._TRANSPORT_REASONS)

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


class RobustnessTest(unittest.TestCase):
    """复盘后的并发/生命周期/入参边界回归。"""

    def _plugin(self):
        p = mls.MusicLibraryScraper()
        p._mtw_base_url = "http://mtw:8002"
        p._enabled = True
        p._cron = "0 3 * * 0"
        return p

    def test_every_scan_entry_is_mutually_exclusive(self):
        """周期任务、一次性任务、手动触发必须共用同一个槽位，否则两本账互相覆盖。"""
        p = self._plugin()
        p.init_plugin({"enabled": True, "cron": "0 3 * * 0"})
        svc = p.get_service()
        self.assertEqual(svc[0]["func"], p._run_scan_guarded)

        captured = {}
        orig_add = mls.scheduler_sdk.add_plugin_once_job

        def fake_add(plugin_id, job_id, func, name, **kw):
            captured["func"] = func
            return True

        mls.scheduler_sdk.add_plugin_once_job = fake_add
        try:
            self.assertTrue(p._kick_scan())
        finally:
            mls.scheduler_sdk.add_plugin_once_job = orig_add
        self.assertEqual(captured["func"], p._run_scan_guarded)

        # 槽位被占时：同步入口跳过、异步入口拒绝，都不许再起一轮
        self.assertTrue(p._begin_scan())
        self.assertFalse(p._run_scan_guarded())
        self.assertFalse(p._start_scan_async())
        p._end_scan()
        self.assertFalse(p._is_running())

    def test_once_job_refusal_falls_back_to_thread(self):
        """宿主调度器没在跑时 add_plugin_once_job 返回 False：不能对着界面报"已提交"。"""
        p = self._plugin()
        kicked = []
        orig_add = mls.scheduler_sdk.add_plugin_once_job
        mls.scheduler_sdk.add_plugin_once_job = lambda *a, **k: False
        p._start_scan_async = lambda **kw: kicked.append(kw) or True
        try:
            self.assertTrue(p._kick_scan())
        finally:
            mls.scheduler_sdk.add_plugin_once_job = orig_add
        self.assertEqual(kicked, [{"force": False, "paths": None, "overwrite": False}])

    def test_kick_scan_refuses_while_slot_busy(self):
        """一次性任务的回调是 3 秒后才跑：那时候抢不到槽会静默跳过。

        不在投递前先看一眼槽位，/scan 与"立即运行一次"就会对着界面回"已提交"，
        实际一轮都没跑 —— 用户只能从日志里那句"已有扫描在运行"反推真相。
        """
        p = self._plugin()
        orig_add = mls.scheduler_sdk.add_plugin_once_job
        added = []
        mls.scheduler_sdk.add_plugin_once_job = lambda *a, **k: added.append(a) or True
        try:
            self.assertTrue(p._begin_scan())
            self.assertFalse(p._kick_scan())
            self.assertEqual(added, [], "槽位被占时连一次性任务都不该投")
            p._end_scan()
            self.assertTrue(p._kick_scan())
            self.assertEqual(len(added), 1)
        finally:
            mls.scheduler_sdk.add_plugin_once_job = orig_add

    def test_config_reload_does_not_clear_running_flag(self):
        """保存配置会重跑 init_plugin：把在跑的扫描标成没在跑等于又开一条并发口子。"""
        p = self._plugin()
        p.init_plugin({"enabled": True})
        self.assertTrue(p._begin_scan())
        p.init_plugin({"enabled": True})
        self.assertTrue(p._is_running())
        p._end_scan()
        self.assertFalse(p._is_running())

    def test_stop_signal_survives_stop_service(self):
        """stop_service 里 set 完立刻 clear 等于从没停过；信号还得是每实例独立的。"""
        p = self._plugin()
        other = mls.MusicLibraryScraper()
        p.stop_service()
        self.assertTrue(p._stop_event().is_set())
        self.assertFalse(other._stop_event().is_set())
        # 下一轮扫描开始时才复位
        self.assertTrue(p._begin_scan())
        self.assertFalse(p._stop_event().is_set())
        p._end_scan()

    def test_retry_rejects_paths_outside_library(self):
        """/retry 不能调用方传什么就刮什么：越界路径与非音频文件一律拒绝。"""
        tmp = Path(tempfile.mkdtemp())
        (tmp / "in.mp3").write_bytes(b"x" * 10)
        (tmp / "notes.txt").write_bytes(b"x")
        excluded = tmp / "excluded"
        excluded.mkdir()
        (excluded / "hidden.flac").write_bytes(b"x")
        outside = tmp.parent / (tmp.name + "_outside")
        outside.mkdir()
        (outside / "escape.mp3").write_bytes(b"x")

        p = self._plugin()
        p._scraper_paths = str(tmp)
        p._exclude_paths = str(excluded)

        self.assertTrue(p._is_library_path(str(tmp / "in.mp3")))
        self.assertFalse(p._is_library_path("/etc/hosts"))
        self.assertFalse(p._is_library_path(str(tmp / "notes.txt")))
        self.assertFalse(p._is_library_path(str(excluded / "hidden.flac")))
        # 拼 ../ 绕前缀，realpath 之后照样在库外
        self.assertFalse(p._is_library_path(str(tmp / ".." / outside.name / "escape.mp3")))

        class _Req:
            def __init__(self, body):
                self._body = body

            async def json(self):
                return self._body

        kicked = []
        p._start_scan_async = lambda **kw: kicked.append(kw) or True
        import asyncio

        mixed = asyncio.run(p.api_retry(_Req({
            "paths": [str(tmp / "in.mp3"), "/etc/hosts"]
        })))
        self.assertTrue(mixed["accepted"])
        self.assertEqual(mixed["count"], 1)
        self.assertEqual(mixed["rejected"], 1)
        self.assertEqual(kicked[-1]["paths"], [str(tmp / "in.mp3")])

        only_outside = asyncio.run(p.api_retry(_Req({"paths": ["/etc/hosts"]})))
        self.assertFalse(only_outside["accepted"])
        self.assertEqual(only_outside["reason"], "out_of_library")

    def test_token_expiry_follows_the_jwt(self):
        """过期时间按令牌自己的 exp 算；拿不到就只缓存 1 小时，别再硬编 6 天。"""
        import base64
        import json as _json

        exp = int(time.time()) + 7200
        payload = base64.urlsafe_b64encode(
            _json.dumps({"exp": exp}).encode()
        ).decode().rstrip("=")
        token = f"hdr.{payload}.sig"
        self.assertEqual(mls._jwt_exp(token), float(exp))
        self.assertEqual(mls._token_expiry_at(token, {"token": token}), float(exp))
        # 响应体里给了过期时间就以它为准
        self.assertEqual(
            mls._token_expiry_at(token, {"token": token, "expired_at": exp + 60}),
            float(exp + 60),
        )
        # 解不出声明：宁可短缓存，多登一次比整轮带着废 token 白跑便宜
        self.assertLess(
            mls._token_expiry_at("opaque", {"token": "opaque"}) - time.time(), 3700
        )

    def test_mtw_failure_classification(self):
        """2xx 响应体仍要按形状兜底：空候选可用，认不出形状的不是"查无此曲"。

        状态码那层归因在 _mtw_post（401/403 鉴权、其它 4xx/5xx 接口报错、连不上不可达），
        这里只管 MTW 偶尔用 200 夹带 DRF 错误体的情况。
        """
        self.assertIsNone(mls._mtw_failure_reason({"result": True, "data": []}))
        self.assertEqual(mls._mtw_failure_reason(None), mls._UNREACHABLE_REASON)
        self.assertEqual(
            mls._mtw_failure_reason({"detail": "Given token not valid",
                                     "code": "token_not_valid"}),
            mls._AUTH_REASON,
        )
        # 认不出形状的响应同样不能当成"查无此曲"
        self.assertEqual(mls._mtw_failure_reason({"msg": "ok"}), mls._UNREACHABLE_REASON)

    def test_transport_reasons_are_the_documented_ones(self):
        """扫描侧靠前缀判断要不要记 retries，两边用同一组常量、不能各写各的。

        MTW 那三种故障（不可达 / 鉴权 / 接口报错）都不许算到文件头上：只有鉴权那一种
        会触发强制重登，另外两种只喂熔断。内置源的同一种故障单列一条，且前缀不能撞
        MTW —— 撞了 startswith 就把两条链路的账混成一类。
        """
        for reason in (mls._UNREACHABLE_REASON, mls._AUTH_REASON, mls._API_ERROR_REASON):
            self.assertIn(reason, mls._TRANSPORT_REASONS)
            self.assertTrue(reason.startswith("MTW"))
        builtin_reason = mls._BUILTIN_UNREACHABLE_REASON
        self.assertIn(builtin_reason, mls._TRANSPORT_REASONS)
        self.assertFalse(builtin_reason.startswith("MTW"))
        self.assertEqual(len(mls._TRANSPORT_REASONS), 4)

    def test_command_retry_uses_same_filter_as_api(self):
        """/music_scraper_retry 也要过一遍库内判定：否则照着账本里已删除、已挪出库的路径
        白跑一轮（stat 失败直接跳过、账本原样留着），看起来像"重试过了但还是失败"。"""
        tmp = Path(tempfile.mkdtemp())
        (tmp / "in.mp3").write_bytes(b"x")
        gone = str(tmp / "gone.mp3")
        p = self._plugin()
        p._scraper_paths = str(tmp)
        p.save_data("failed", {
            str(tmp / "in.mp3"): {"retries": 1},
            gone: {"retries": 1},
            "/etc/hosts": {"retries": 1},
        })
        kicked = []
        p._kick_scan = lambda **kw: kicked.append(kw) or True
        p._cmd_retry()
        self.assertEqual(kicked, [{"force": True, "paths": [str(tmp / "in.mp3")]}])
        self.assertIn("已不在库内", p.messages[-1]["text"])

    def test_config_change_drops_stale_token(self):
        """换 MTW 地址 / 换账号后手里那个令牌是上一个实例签发的：
        继续带着它既白吃 401，也算把凭据发到新地址上。"""
        p = self._plugin()
        base = {
            "enabled": True, "scraper_paths": "/music",
            "mtw_base_url": "http://mtw-a:8002",
            "mtw_username": "u", "mtw_password": "p",
        }
        p.init_plugin(base)
        p._token, p._token_expiry = "tok", time.time() + 3600
        p.init_plugin({**base, "mtw_base_url": "http://mtw-b:8002"})
        self.assertIsNone(p._token)
        self.assertEqual(p._token_expiry, 0.0)
        # 保存一次没改过 MTW 的配置不该把人家的登录状态清掉（否则每次改 cron 都要重登）
        p.init_plugin({**base, "mtw_base_url": "http://mtw-a:8002"})
        p._token, p._token_expiry = "tok", time.time() + 3600
        p.init_plugin({**base, "cron": "0 4 * * 0"})
        self.assertEqual(p._token, "tok")

    def test_min_score_clamped_on_config_load(self):
        """min_score 走 init_plugin 时同样要收敛：配置页的 min/max 拦不住原生表单。"""
        p = self._plugin()
        base = {"enabled": True, "scraper_paths": "/music", "mtw_base_url": "http://mtw:8002"}
        p.init_plugin({**base, "min_score": 0})
        self.assertEqual(p._min_score, 1)
        p.init_plugin({**base, "min_score": 9})
        self.assertEqual(p._min_score, mls._MAX_SCORE)
        p.init_plugin({**base, "min_score": 2})
        self.assertEqual(p._min_score, 2)

    def test_anonymous_mtw_warns_only_once(self):
        """没配 MTW 账密时每个请求都会走匿名分支：一轮几千个文件不能把日志刷满。"""
        p = self._plugin()
        p._mtw_username, p._mtw_password = "", ""
        warns = []
        orig_warning = mls.logger.warning
        orig_ru = mls.RequestUtils
        mls.logger.warning = lambda *a, **k: warns.append(str(a))

        try:
            for _ in range(50):
                self.assertIsNone(p._mtw_token())
            self.assertEqual(len(warns), 1)
            # 配上账密并登录成功后复位，之后又退回匿名时还要提示
            mls.RequestUtils = _ru(lambda url, body: _Resp(200, {"token": "t"}))
            p._mtw_username, p._mtw_password = "u", "p"
            self.assertIsNotNone(p._mtw_token())
            self.assertFalse(p.__dict__.get("_anon_warned"))
            p._token, p._token_expiry = None, 0.0
            p._mtw_password = ""
            for _ in range(10):
                self.assertIsNone(p._mtw_token())
            self.assertEqual(len(warns), 2)
        finally:
            mls.logger.warning = orig_warning
            mls.RequestUtils = orig_ru

    def test_corrupt_ledger_entry_does_not_crash_readers(self):
        """plugindata 可能被旧版本/手工编辑写成半对的样子：条目不是 dict 时
        读取侧不能一路 AttributeError，从 /status 到整轮扫描会一起崩。"""
        p = self._plugin()
        p._scraper_paths = "/music"
        p.save_data("failed", {"/music/a.mp3": "坏了", "/music/b.mp3": {"retries": 2}})
        p.save_data("done", {"/music/c.mp3": None})
        self.assertEqual(p._load_failed()["/music/a.mp3"], {})
        status = p._build_status()
        self.assertEqual(status["counts"]["failed"], 2)
        self.assertEqual(status["counts"]["retryable"], 2)
        # 记账侧同样要把坏条目当成"没有记录"，而不是在它上面 .get()
        self.assertEqual(int(p._load_failed()["/music/a.mp3"].get("retries", 0)), 0)

    def test_stop_signal_after_the_round_starts_is_never_cleared(self):
        """复位停止信号必须发生在占位之前。

        此前 clear() 落在锁外：置位 → 出锁 → clear 之间挤进来的 stop_service 会被抹掉，
        而它想停的正是紧接着这一轮 —— 界面按了"停止"、日志里一句"收到停止信号"都没有，
        只能等整轮跑完。
        """
        p = self._plugin()
        real = p._stop_event()
        seen = {}

        class Spy:
            def set(self):
                real.set()

            def clear(self):
                seen["running"] = p.__dict__.get("_scan_running")
                real.clear()

            def is_set(self):
                return real.is_set()

        p.__dict__["_stop_flag"] = Spy()
        self.assertTrue(p._begin_scan())
        self.assertIn("running", seen)
        self.assertFalse(seen["running"])
        # 真的跑起来之后到达的停止信号，下一轮占位时才复位
        real.set()
        p._end_scan()
        p._begin_scan()
        self.assertFalse(real.is_set())

    def test_enumerate_stops_before_the_next_root(self):
        """停止信号要让剩下的库也不再遍历：内层 break 只退掉当前库的 os.walk，
        外层还接着走下一个库，用户按了停止却还在扫全盘。"""
        first = Path(tempfile.mkdtemp())
        second = Path(tempfile.mkdtemp())
        (first / "a.mp3").write_bytes(b"x")
        (second / "b.mp3").write_bytes(b"x")
        p = self._plugin()
        p._scraper_paths = f"{first}\n{second}"
        visited = []
        real_walk = mls.os.walk

        def spy(top, *args, **kwargs):
            visited.append(str(top))
            p._stop_event().set()
            return iter(())

        mls.os.walk = spy
        try:
            files = p._enumerate_files()
        finally:
            mls.os.walk = real_walk
        self.assertEqual(files, [])
        self.assertEqual(visited, [str(first)])

    def test_files_api_rejects_unknown_bucket(self):
        """不认识的桶名不能悄悄落到 done 那一档：回显的还是请求的那个名字，
        调用方拿到一批"已处理"的行却以为是自己传的桶。"""
        p = self._plugin()
        p.save_data("done", {"/music/a.mp3": {"mtime": 1, "size": 1, "ts": "x"}})

        class _Req:
            def __init__(self, q):
                self.query_params = q

        data = p.api_files(_Req({"bucket": "deined"}))
        self.assertEqual(data["items"], [])
        self.assertEqual(data["total"], 0)
        self.assertEqual(data["bucket"], "deined")
        self.assertEqual(data["reason"], "unknown_bucket")
        # 不传桶名仍是原来的默认（done）
        self.assertEqual(p.api_files(_Req({}))["total"], 1)

    def test_form_defaults_keep_resources_empty(self):
        """原生表单不要把 7 个源预填上：留空=全部源才是配置页的口径。"""
        _content, defaults = self._plugin().get_form()
        self.assertEqual(defaults["mtw_resource"], [])
        self.assertIn("留空即全部源", mls._FORM_NOTES)

    def test_onlyonce_writeback_carries_every_config_key(self):
        """宿主 update_config 是整体替换不是合并：只回写 onlyonce 会清掉其余配置。"""
        p = mls.MusicLibraryScraper()
        _content, defaults = p.get_form()
        config = {
            **defaults,
            "scraper_paths": "/music",
            "mtw_base_url": "http://mtw:8002",
            "mtw_username": "u",
            "mtw_password": "p",
        }
        p.init_plugin(config)
        self.assertEqual(p._configs, [])  # 没勾选"立即运行一次"就不该回写
        p.init_plugin({**config, "onlyonce": True})
        written = p._configs[-1]
        self.assertEqual(set(written), set(defaults))
        self.assertIs(written["onlyonce"], False)


class BuiltinSourcesTest(unittest.TestCase):
    """内置识别源：不部署 Music Tag Web 也要能刮完整份库。

    这批用例守的是三件事：候选字段映射真的能喂给 MusicInfo（否则免部署就是空话）、
    平台改版/风控要说成"这个源没给答案"而不是"曲库里查不到"、以及内置源绝不把库内路径
    发给平台（MTW 模式会带 full_path，内置模式没有那个必要）。
    """

    _NETEASE = {
        "result": {"songs": [{
            "id": 186016, "name": "晴天", "no": 3, "cd": "01", "dt": 269000,
            "publishTime": 1059609600000,
            "ar": [{"name": "周杰伦"}, {"name": "杨瑞代"}],
            "al": {"name": "叶惠美", "picUrl": "http://p1.music.126.net/x/109.jpg"},
        }]}
    }
    _QMUSIC = {
        "req_0": {"code": 0, "data": {"body": {"song": {"list": [{
            "title": "晴天", "mid": "0039MnYb0qxYhV", "interval": 269,
            "time_public": "2003-07-31", "index_album": 3, "index_cd": 0,
            "singer": [{"name": "周杰伦"}],
            "album": {"mid": "000MkMni19ClKG", "name": "叶惠美"},
        }]}}}}
    }
    _KUGOU = {
        "status": 1, "data": {"info": [{
            "hash": "b3a52a7a", "songname": "晴天(现场版)", "songname_original": "晴天",
            "singername": "周杰伦", "album_name": "叶惠美", "duration": 269,
            "trans_param": {"union_cover": "http://imge.kugou.com/stdmusic/{size}/a.jpg"},
        }]}
    }
    _MIGU = {
        "code": "000000", "songResultData": {"result": [{
            "id": "3790007", "name": "晴天",
            "singers": [{"id": "112", "name": "周杰伦"}],
            "albums": [{"id": "8592", "name": "叶惠美"}],
            "imgItems": [{"imgSizeType": "01", "img": "http://s/1.webp"},
                         {"imgSizeType": "03", "img": "http://s/3.webp"}],
        }]}
    }
    _MUSICBRAINZ = {
        "recordings": [{
            "id": "1e6c9c56", "title": "晴天", "length": 269000,
            "first-release-date": "2003-07-31",
            "artist-credit": [{"name": "周杰倫", "artist": {"id": "a1", "name": "周杰倫"}}],
            "releases": [
                {"id": "r1", "title": "葉惠美", "status": "Promotion", "date": ""},
                {"id": "r2", "title": "葉惠美", "status": "Official", "date": "2003-07-31"},
            ],
        }]
    }
    _BY_URL = {
        "music.163.com": _NETEASE,
        "musicu.fcg": _QMUSIC,
        "kugou.com": _KUGOU,
        "migu.cn": _MIGU,
        "musicbrainz.org": _MUSICBRAINZ,
    }

    def _plugin(self, resources=None, backend="builtin", min_score=3):
        p = mls.MusicLibraryScraper()
        p._min_score = min_score
        p._backend = backend
        p._mtw_base_url = "http://mtw:8002" if backend == "mtw" else ""
        p._mtw_username = ""
        p._mtw_password = ""
        p._mtw_resources = list(resources or mls._DEFAULT_RESOURCES)
        builtin = backend == "builtin"
        p._active_resources = mls._normalize_resources(
            resources,
            known=mls._sources.RESOURCES if builtin else None,
            default=mls._sources.RESOURCES if builtin else None,
        )
        return p

    def _track(self, title="黑面", artist="陈奕迅", album="黑白灰"):
        tmp = Path(tempfile.mkdtemp())
        fp = tmp / artist / album / f"07 - {title}.flac"
        fp.parent.mkdir(parents=True)
        fp.write_bytes(b"x")
        return fp

    def _answer(self, asked, fail_urls=()):
        """按 URL 命中内置源的固定响应；asked 记录 (url, 查询参数或请求体)。"""
        def _handler(url, body):
            asked.append((url, body))
            for host, payload in self._BY_URL.items():
                if host in url:
                    if any(f in url for f in fail_urls):
                        return None
                    return _Resp(200, payload)
            return _Resp(200, {})
        return _ru(_handler)

    # ---------------- 检索词 ----------------

    def test_build_keyword_follows_mtw_convention(self):
        """曲名与艺人互不包含时用 "曲名-艺人" 提准，已包含就只发曲名。"""
        build = mls._sources.build_keyword
        self.assertEqual(build("黑面", "陈奕迅"), "黑面-陈奕迅")
        self.assertEqual(build("黑面 陈奕迅", "陈奕迅"), "黑面 陈奕迅")
        self.assertEqual(build("黑面", ""), "黑面")
        self.assertEqual(build(""), "")

    # ---------------- 各源字段映射 ----------------

    def test_netease_candidate_normalized(self):
        cands, err = mls._sources.parse_candidates("netease", self._NETEASE)
        self.assertIsNone(err)
        self.assertEqual(len(cands), 1)
        c = cands[0]
        self.assertEqual(c["name"], "晴天")
        self.assertEqual([a["name"] for a in c["artists"]], ["周杰伦", "杨瑞代"])
        self.assertEqual(c["album"], "叶惠美")
        self.assertEqual(c["album_img"], "http://p1.music.126.net/x/109.jpg")
        # cd 是 "01" 这种字符串，轨道号取自 no
        self.assertEqual((c["tracknumber"], c["discnumber"]), (3, 1))
        self.assertEqual(c["duration"], 269000)
        self.assertEqual(str(c["year"]), "1059609600000")

    def test_qmusic_cover_is_built_from_album_mid(self):
        """QQ 检索结果里没有封面地址，用专辑 mid 拼图片站 —— 不拼就等于每次都缺封套图。"""
        cands, err = mls._sources.parse_candidates("qmusic", self._QMUSIC)
        self.assertIsNone(err)
        c = cands[0]
        self.assertEqual(c["album_img"],
                         "https://y.gtimg.cn/music/photo_new/T002R500x500M000000MkMni19ClKG.jpg")
        self.assertEqual(c["year"], "2003-07-31")
        self.assertEqual([a["name"] for a in c["artists"]], ["周杰伦"])
        self.assertEqual(c["id"], "0039MnYb0qxYhV")

    def test_kugou_cover_template_filled_and_original_title_preferred(self):
        """优先 songname_original：现场版/翻唱版后缀会让曲名与本地身份差一层。"""
        cands, err = mls._sources.parse_candidates("kugou", self._KUGOU)
        self.assertIsNone(err)
        c = cands[0]
        self.assertEqual(c["name"], "晴天")
        self.assertNotIn("{size}", c["album_img"])
        self.assertTrue(c["album_img"].endswith(".jpg"))

    def test_migu_picks_largest_cover_and_requires_success_code(self):
        cands, err = mls._sources.parse_candidates("migu", self._MIGU)
        self.assertIsNone(err)
        self.assertEqual(cands[0]["album_img"], "http://s/3.webp")
        self.assertEqual(cands[0]["album"], "叶惠美")
        # 咪咕用字符串业务码表态，码不对时不能当成"查到了零条"
        bad = {"code": "000001", "songResultData": {"result": []}}
        self.assertEqual(mls._sources.parse_candidates("migu", bad), ([], mls._sources.API_ERROR))

    def test_musicbrainz_prefers_official_release(self):
        cands, err = mls._sources.parse_candidates("musicbrainz", self._MUSICBRAINZ)
        self.assertIsNone(err)
        c = cands[0]
        self.assertEqual(c["album"], "葉惠美")
        self.assertEqual(c["year"], "2003-07-31")
        self.assertEqual([a["name"] for a in c["artists"]], ["周杰倫"])
        # 繁体候选必须还能和本地身份对上，否则 MusicBrainz 就没有存在的理由。
        # 繁转简是宿主 music_text_key 的活（测试环境没有那层 rust zhconv），按同一契约给替身。
        to_simplified = {"葉": "叶", "倫": "伦"}
        orig_key = mls._music_text_key
        mls._music_text_key = staticmethod(
            lambda value: "".join(to_simplified.get(ch, ch) for ch in str(value)))
        try:
            hits = mls.MusicLibraryScraper._field_hits(c, {
                "title": "晴天", "artist": "周杰伦", "album": "叶惠美", "source": "local"})
        finally:
            mls._music_text_key = orig_key
        self.assertEqual(hits["album"], "hit")
        self.assertEqual(hits["artist"], "hit")

    # ---------------- 故障与形状的区分 ----------------

    def test_shape_change_is_a_fault_not_an_empty_answer(self):
        """平台改版或被网关截了：要说成"这个源没给答案"，不能说成"曲库里查不到"。"""
        for resource in mls._sources.RESOURCES:
            cands, err = mls._sources.parse_candidates(resource, {"unexpected": True})
            self.assertEqual(cands, [], resource)
            self.assertEqual(err, mls._sources.API_ERROR, resource)

    def test_empty_result_is_a_real_answer(self):
        """正常的零命中是结论，不是故障：该按文件记失败、该烧重试额度。"""
        empty = {"result": {"songs": []}}
        self.assertEqual(mls._sources.parse_candidates("netease", empty), ([], None))

    def test_unknown_source_builds_nothing(self):
        self.assertIsNone(mls._sources.build_request("kuwo", "黑面-陈奕迅"))
        self.assertEqual(
            mls._sources.parse_candidates("kuwo", {}), ([], mls._sources.UNREACHABLE))
        # 酷我与 smart_tag 只在 MTW 模式能问，这份边界要在源清单里说清楚
        self.assertNotIn("kuwo", mls._sources.RESOURCES)
        self.assertNotIn("smart_tag", mls._sources.RESOURCES)

    # ---------------- 拉候选与后端派发 ----------------

    def test_builtin_fetch_tags_resource_and_stops_at_perfect(self):
        asked = []
        p = self._plugin(resources=list(mls._sources.RESOURCES))
        fp = self._track("晴天", "周杰伦", "叶惠美")
        orig = mls.RequestUtils
        mls.RequestUtils = self._answer(asked)
        try:
            cands, failure = p._builtin_fetch_candidates(fp)
            # 满分候选（曲名+艺人+专辑全中）在第一个源就出现，后面的源不该再被问
            self.assertIsNone(failure)
            self.assertEqual([c["resource"] for c in cands], ["netease"])
            self.assertEqual(len(asked), 1)
        finally:
            mls.RequestUtils = orig

    def test_builtin_fetch_reports_transport_only_when_no_source_answered(self):
        """全部源都连不上才是服务侧故障；只要有一个源答了空，就是"查不到"。"""
        p = self._plugin()
        fp = self._track("晴天")
        orig = mls.RequestUtils
        mls.RequestUtils = self._answer([], fail_urls=tuple(self._BY_URL.keys()))
        try:
            cands, failure = p._builtin_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(cands, [])
        self.assertEqual(failure, mls._BUILTIN_UNREACHABLE_REASON)

    def test_builtin_shape_break_is_a_transport_fault_not_a_file_failure(self):
        """所有源都返回认不出的形状（平台一起改版）要说成服务侧故障：当成"查不到"会让
        整库烧满重试后永久跳过，而真正该做的是插件这边跟着改版把解析修好。"""
        p = self._plugin()
        fp = self._track("晴天")
        orig = mls.RequestUtils
        mls.RequestUtils = _ru(lambda url, body: _Resp(200, {"unexpected": True}))
        try:
            cands, failure = p._builtin_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(cands, [])
        self.assertEqual(failure, mls._BUILTIN_UNREACHABLE_REASON)

    def test_one_answered_source_makes_shape_breaks_a_file_failure(self):
        """有一个源正常答了空就是确定答案"查不到"：其余源改版不该拖成服务侧故障。"""
        p = self._plugin()
        fp = self._track("晴天")

        def _handler(url, body):
            if "music.163.com" in url:
                return _Resp(200, {"result": {"songs": []}})
            return _Resp(200, {"unexpected": True})

        orig = mls.RequestUtils
        mls.RequestUtils = _ru(_handler)
        try:
            cands, failure = p._builtin_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertEqual(cands, [])
        self.assertIsNone(failure)

    def test_builtin_request_never_carries_the_library_path(self):
        """MTW 模式会带 full_path；内置源只发检索词，库内目录结构不外传。"""
        asked = []
        p = self._plugin(resources=["netease"])
        fp = self._track("晴天")
        orig = mls.RequestUtils
        mls.RequestUtils = self._answer(asked)
        try:
            p._builtin_fetch_candidates(fp)
        finally:
            mls.RequestUtils = orig
        self.assertTrue(asked)
        for url, body in asked:
            self.assertNotIn(str(fp.parent), url)
            self.assertNotIn(str(fp.parent), str(body))
            self.assertNotIn("07 - ", str(body))
        # 检索词是清洗后的曲名与艺人
        self.assertIn("晴天", str(asked[0][1]))

    def test_dispatcher_uses_mtw_only_when_backend_is_mtw(self):
        p = self._plugin(backend="mtw")
        seen = []
        orig = p.__class__._mtw_fetch_candidates
        orig_builtin = p.__class__._builtin_fetch_candidates
        try:
            p.__class__._mtw_fetch_candidates = lambda self, path, identity=None: (
                seen.append("mtw") or ([], None))
            p.__class__._builtin_fetch_candidates = lambda self, path, identity=None: (
                seen.append("builtin") or ([], None))
            p._fetch_candidates(self._track("晴天"))
            p._backend = "builtin"
            p._fetch_candidates(self._track("晴天"))
        finally:
            p.__class__._mtw_fetch_candidates = orig
            p.__class__._builtin_fetch_candidates = orig_builtin
        self.assertEqual(seen, ["mtw", "builtin"])

    def test_auto_backend_follows_the_mtw_address(self):
        """auto 是升级安全值：配过 MTW 地址的装机继续走 MTW，没配的走内置源。"""
        _content, defaults = mls.MusicLibraryScraper().get_form()
        base = {**defaults, "scraper_paths": "/music"}
        with_mtw = mls.MusicLibraryScraper()
        with_mtw.init_plugin({**base, "mtw_base_url": "http://mtw:8002"})
        self.assertEqual(with_mtw._backend, "mtw")
        without = mls.MusicLibraryScraper()
        without.init_plugin(base)
        self.assertEqual(without._backend, "builtin")
        # 显式选择压过 auto 的判断：填着 MTW 地址也能强行用内置源
        forced = mls.MusicLibraryScraper()
        forced.init_plugin({**base, "mtw_base_url": "http://mtw:8002",
                            "source_backend": "builtin"})
        self.assertEqual(forced._backend, "builtin")
        bad = mls.MusicLibraryScraper()
        bad.init_plugin({**base, "source_backend": "nope"})
        self.assertEqual(bad._source_backend, "auto")

    def test_builtin_mode_never_touches_mtw_credentials(self):
        """免部署的意义所在：没配 MTW 地址账密也能跑完整一轮，不戳 /apimt/token/。"""
        asked = []
        fp = self._track("晴天", "周杰伦", "叶惠美")
        p = self._plugin(resources=["netease"], min_score=3)
        p._scraper_paths = str(fp.parents[2])
        p._exclude_paths = ""
        p._force_all = False
        orig_ru, orig_chain = mls.RequestUtils, mls._MusicScrapeChain
        written = []

        class _Chain:
            def scrape_metadata(self, fileitem, mediainfo, overwrite=False):
                written.append(mediainfo)
                return True, "ok"

        mls.RequestUtils = self._answer(asked)
        mls._MusicScrapeChain = _Chain
        try:
            ok, reason, score, source = p._MusicLibraryScraper__scrape_file(fp)
        finally:
            mls.RequestUtils = orig_ru
            mls._MusicScrapeChain = orig_chain
        self.assertTrue(ok, reason)
        self.assertEqual(source, "netease")
        self.assertEqual(score, 3)
        self.assertFalse(any("token" in url or "apimt" in url for url, _ in asked),
                         [url for url, _ in asked])
        # 候选真的变成了 MusicInfo：标题/艺人/专辑/封面都落进了写入链
        info = written[0]
        self.assertEqual((info.title, info.album), ("晴天", "叶惠美"))
        self.assertEqual(info.artists, ["周杰伦", "杨瑞代"])
        self.assertTrue(info.cover_url)

    def test_no_candidate_reason_names_the_backend(self):
        """两种后端的"查不到"要说成两句话，账本里的处置建议不一样。"""
        orig = mls.RequestUtils
        try:
            p = self._plugin(resources=["netease"], backend="builtin")
            mls.RequestUtils = _ru(lambda url, body: _Resp(200, {"result": {"songs": []}}))
            ok, reason, _, _ = p._MusicLibraryScraper__scrape_file(self._track("晴天"))
            self.assertFalse(ok)
            self.assertEqual(reason, mls._BUILTIN_NO_CANDIDATE_REASON)

            mtw = self._plugin(resources=["netease"], backend="mtw")
            mls.RequestUtils = _ru(lambda url, body: _Resp(200, {"result": True, "data": []}))
            ok, reason, _, _ = mtw._MusicLibraryScraper__scrape_file(self._track("晴天"))
            self.assertFalse(ok)
            # 存量账本里就是这句，升级不能把它改名改到统计分成两截
            self.assertEqual(reason, "MTW 无候选")
        finally:
            mls.RequestUtils = orig

    def test_builtin_scraper_keeps_only_supported_sources(self):
        """旧配置里的酷我/smart_tag 在内置源里要安静地摘掉，不能每个文件白打一次失败请求。"""
        p = self._plugin(resources=["kuwo", "netease", "smart_tag"])
        self.assertEqual(p._active_resources, ["netease"])
        # 整份配置只剩被忽略的那几个时，退回全部内置源而不是产出零候选
        only_dropped = self._plugin(resources=["kuwo"])
        self.assertEqual(only_dropped._active_resources, list(mls._sources.RESOURCES))

    def test_builtin_transport_failure_does_not_burn_retries(self):
        """内置源全挂时的分类与 MTW 故障一致：不写 failed 账、不涨 retries、能熔断。"""
        names = [f"{i}.mp3" for i in range(mls._ABORT_AFTER_TRANSPORT)]
        tmp = Path(tempfile.mkdtemp())
        for name in names:
            (tmp / name).write_bytes(b"x" * 100)
        p = self._plugin(resources=["netease"])
        p._scraper_paths = str(tmp)
        p._exclude_paths = ""
        p._force_all = False
        p._MusicLibraryScraper__done = {}
        orig_ru, orig_log = mls.RequestUtils, mls.logger
        mls.RequestUtils = _ru(lambda url, body: None)
        mls.logger = type("_L", (), {"__getattr__": lambda self, n: (lambda *a, **k: None)})()
        try:
            p._MusicLibraryScraper__scan()
        finally:
            mls.RequestUtils = orig_ru
            mls.logger = orig_log
        runs = p._load_runs()
        self.assertEqual(runs["transport"], mls._ABORT_AFTER_TRANSPORT)
        self.assertEqual(p._load_failed(), {})

    def test_status_echoes_the_effective_backend(self):
        """只回显 auto 等于什么都没回显：用户要看得见本轮到底在问平台还是问 MTW。"""
        _content, defaults = mls.MusicLibraryScraper().get_form()
        p = mls.MusicLibraryScraper()
        p.init_plugin({**defaults, "scraper_paths": "/music"})
        status = p._build_status()
        self.assertEqual(status["config"]["source_backend"], "auto")
        self.assertEqual(status["config"]["backend"], "builtin")
        self.assertEqual(status["config"]["backend_label"], "内置源")
        self.assertEqual(status["config"]["active_resource"], list(mls._sources.RESOURCES))
        self.assertNotIn("mtw_password", status["config"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
