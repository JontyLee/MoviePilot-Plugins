"""在导入插件前，向 sys.modules 注入宿主 app.* / 第三方依赖的最小桩。

本地没有 MoviePilot 运行环境与 apscheduler/fastapi/pydantic，插件模块顶层 import
会失败。这里提供足够让 `import musiclibraryscraper` 通过、且可驱动 __scan 增量逻辑
的替身；真实单测只验证插件自身的增量/记账逻辑，不触碰 MTW 网络与 MP 写入。
"""
from __future__ import annotations

import sys
import types


def _mod(name: str) -> types.ModuleType:
    m = sys.modules.get(name)
    if m is None:
        m = types.ModuleType(name)
        sys.modules[name] = m
    return m


class _Logger:
    def _noop(self, *a, **k):
        return None

    info = warning = error = debug = exception = _noop


class _Event:
    def __init__(self, event_type=None, event_data=None):
        self.event_type = event_type
        self.event_data = event_data or {}


class _EventManager:
    def register(self, _event_type):
        def _deco(fn):
            return fn

        return _deco

    def signal(self, *_a, **_k):
        return None


class _CronTrigger:
    @staticmethod
    def from_crontab(expr):
        # 粗校验 5 段；非法抛错，用于测试 cron 回落。
        parts = (expr or "").split()
        if len(parts) != 5:
            raise ValueError(f"bad cron: {expr!r}")
        # 照 APScheduler 的样子拒掉星期 7（它的上限是 6，0=周一），否则插件里
        # "7 换成 sun" 这一步在本机测试里永远不会被触发。
        if "7" in parts[4]:
            raise ValueError(f"the last value (7) is higher than the maximum value (6)")
        return ("crontab", expr)


class _ScrapingChain:
    def scrape_metadata(self, fileitem=None, mediainfo=None, **kwargs):
        return True, "stub"


class _MetaInfoPath:
    # 测试里 __is_complete / _mtw_pick_best 均被替换，这里只需可实例化。
    def __init__(self, path):
        self.title = ""
        self.artist = ""
        self.album = ""
        self.duration = None


class _MusicInfo:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FileItem:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _RequestUtils:
    """宿主 RequestUtils 的桩：插件只用同步 post()，它返回 Response（任何状态码）或
    连不上时的 None。桩一律"连不上"，要响应的用例自己替换 mls.RequestUtils。"""

    def __init__(self, *a, **k):
        pass

    def get(self, *a, **k):
        return None

    def post(self, *a, **k):
        return None


class _EventType:
    PluginAction = "PluginAction"
    ConfigChanged = "ConfigChanged"


class _Schemas:
    FileItem = _FileItem
    types = None


def install(_plugin_base_cls, settings_obj=None):
    """安装全部桩。_plugin_base_cls 作为 app.plugins._PluginBase。"""
    app = _mod("app")
    app.__path__ = []  # 标记为包
    app.schemas = _Schemas()
    _mod("app.schemas")
    sys.modules["app.schemas"] = app.schemas  # type: ignore
    # schemas.types 属性（部分代码用 app.schemas.types 已导入的名字，见下）

    # app.schemas.types：真实包路径，插件用 `from app.schemas.types import EventType`
    types_mod = _mod("app.schemas.types")
    types_mod.EventType = _EventType
    app.schemas.types = types_mod  # type: ignore

    chain = _mod("app.chain")
    chain.__path__ = []
    scraping = _mod("app.chain.scraping")
    scraping.ScrapingChain = _ScrapingChain
    app.chain = chain  # type: ignore
    chain.scraping = scraping  # type: ignore

    plugins_mod = _mod("app.plugins")
    plugins_mod._PluginBase = _plugin_base_cls
    app.plugins = plugins_mod  # type: ignore

    # app.sdk 及子模块
    sdk = _mod("app.sdk")
    sdk.__path__ = []
    app.sdk = sdk  # type: ignore

    cfg = _mod("app.sdk.config")
    cfg.settings = settings_obj or types.SimpleNamespace(PLUGIN_DATA_PATH=None)
    sdk.config = cfg  # type: ignore

    events = _mod("app.sdk.events")
    events.Event = _Event
    events.eventmanager = _EventManager()
    sdk.events = events  # type: ignore

    logging_mod = _mod("app.sdk.logging")
    logging_mod.logger = _Logger()
    sdk.logging = logging_mod  # type: ignore

    media = _mod("app.sdk.media")
    media.MetaInfoPath = _MetaInfoPath
    media.MusicInfo = _MusicInfo
    sdk.media = media  # type: ignore

    network = _mod("app.sdk.network")
    network.RequestUtils = _RequestUtils
    sdk.network = network  # type: ignore

    scheduler = _mod("app.sdk.scheduler")
    scheduler.add_plugin_once_job = lambda *a, **k: True
    scheduler.remove_plugin_once_job = lambda *a, **k: True
    sdk.scheduler = scheduler  # type: ignore

    # 第三方
    apsched = _mod("apscheduler")
    apsched.__path__ = []
    ap_triggers = _mod("apscheduler.triggers")
    ap_triggers.__path__ = []
    ap_cron = _mod("apscheduler.triggers.cron")
    ap_cron.CronTrigger = _CronTrigger
    apsched.triggers = ap_triggers  # type: ignore
    ap_triggers.cron = ap_cron  # type: ignore

    fastapi = _mod("fastapi")

    class _Request:  # noqa: D401 - 仅作类型占位
        pass

    fastapi.Request = _Request
    return app
