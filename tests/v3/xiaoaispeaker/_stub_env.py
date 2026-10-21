"""在导入插件前，向 sys.modules 注入宿主 app.* 与第三方依赖的最小桩。

插件顶层 import 的是宿主（app.plugins / app.schemas / app.sdk.*）与 apscheduler、
fastapi、pydantic。宿主桩一律在这里给齐；pydantic 是插件 API 模型的运行时，
桩不了（模型验证行为就是要测的东西），所以本套测试需要在有 pydantic 的环境里跑
（宿主虚拟环境，或 `python -m venv` 后装 pydantic）。

fastapi 与 apscheduler 无论环境里有没有真的都换成这里的替身：/stream 的断言要看的是
「插件给 Response 构造器传了什么」（状态码、头、迭代器），真的 StreamingResponse 把
内容包成 body_iterator 且要 async 驱动才能读出来，用它测就会变成在测 fastapi。替身
构造器的位置/关键字形状与真的一致，所以调用写错了这里同样会炸。
"""
from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any, Dict, Optional


def _mod(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    return module


def _force_mod(name: str) -> types.ModuleType:
    """无条件换成新桩：环境里装了真的 fastapi/apscheduler 也不用它。"""
    module = types.ModuleType(name)
    sys.modules[name] = module
    return module


def _try_real(name: str) -> Optional[Any]:
    try:
        return __import__(name)
    except Exception:
        return None


class _Logger:
    def __init__(self) -> None:
        self.lines: list = []

    def _record(self, level: str, message: str = "", *args, **kwargs) -> None:
        self.lines.append(f"{level}: {message}")

    def info(self, message="", *a, **k):
        self._record("info", message)

    def warning(self, message="", *a, **k):
        self._record("warning", message)

    def error(self, message="", *a, **k):
        self._record("error", message)

    def debug(self, message="", *a, **k):
        self._record("debug", message)

    def exception(self, message="", *a, **k):
        self._record("exception", message)


class _Event:
    def __init__(self, event_type=None, event_data=None):
        self.event_type = event_type
        self.event_data = event_data or {}


class _EventManager:
    def register(self, _event_type):
        def _decorator(fn):
            return fn

        return _decorator

    def send_event(self, *_a, **_k) -> None:
        return None


class _EventType:
    PluginAction = "plugin.action"
    ConfigChanged = "config.changed"


class _IntervalTrigger:
    """apscheduler IntervalTrigger 的替身：只记录秒数，够测试判服务声明。"""

    def __init__(self, seconds: int = 0, **kwargs) -> None:
        self.seconds = seconds

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"IntervalTrigger(seconds={self.seconds})"


class _Request:
    """fastapi.Request 的替身：插件只读 query_params 与 headers。"""

    def __init__(self, query: Optional[Dict[str, Any]] = None,
                 headers: Optional[Dict[str, Any]] = None) -> None:
        self.query_params = dict(query or {})
        self.headers = dict(headers or {})


class _JSONResponse:
    def __init__(self, status_code: int = 200, content: Any = None, **kwargs) -> None:
        self.status_code = status_code
        self.body = content

    def __repr__(self) -> str:  # pragma: no cover - 断言失败时好看些
        return f"JSONResponse({self.status_code}, {self.body!r})"


class _StreamingResponse:
    def __init__(self, iterator, status_code: int = 200,
                 media_type: str = "", headers: Optional[Dict[str, str]] = None,
                 **kwargs) -> None:
        self.iterator = iterator
        self.status_code = status_code
        self.media_type = media_type
        self.headers = dict(headers or {})


class _RequestUtils:
    """宿主 RequestUtils 的桩：默认「连不上」（返回 None），用例自己替换成有响应的那份。"""

    def __init__(self, *args, **kwargs) -> None:
        pass

    def get_res(self, url=None, **kwargs):
        return None

    def post_res(self, url=None, data=None, **kwargs):
        return None


class _Settings:
    """宿主 settings 的替身：只需要 APP_DOMAIN（外部访问地址回落到它）。"""

    def __init__(self) -> None:
        self.APP_DOMAIN = ""
        self.PLUGIN_DATA_PATH = Path("/tmp")


def install(plugin_base_cls, settings_obj: Optional[_Settings] = None) -> Any:
    """安装全部宿主桩，返回桩好的 app 包模块。"""
    settings_obj = settings_obj or _Settings()

    app = _mod("app")
    app.__path__ = []
    schemas = _mod("app.schemas")
    schemas.__path__ = []
    schemas.types = _EventType
    app.schemas = schemas
    types_mod = _mod("app.schemas.types")
    types_mod.EventType = _EventType
    schemas.types = types_mod

    plugins_mod = _mod("app.plugins")
    plugins_mod.__path__ = []
    plugins_mod._PluginBase = plugin_base_cls
    app.plugins = plugins_mod

    sdk = _mod("app.sdk")
    sdk.__path__ = []
    app.sdk = sdk

    config_mod = _mod("app.sdk.config")
    config_mod.settings = settings_obj
    sdk.config = config_mod

    events_mod = _mod("app.sdk.events")
    events_mod.Event = _Event
    events_mod.eventmanager = _EventManager()
    sdk.events = events_mod

    logging_mod = _mod("app.sdk.logging")
    logger = _Logger()
    logging_mod.logger = logger
    sdk.logging = logging_mod

    network_mod = _mod("app.sdk.network")
    network_mod.RequestUtils = _RequestUtils
    sdk.network = network_mod

    scheduler_mod = _mod("app.sdk.scheduler")
    scheduler_mod.add_plugin_once_job = lambda *a, **k: True
    scheduler_mod.remove_plugin_once_job = lambda *a, **k: True
    sdk.scheduler = scheduler_mod

    # 第三方：pydantic 必须是环境里的真货，fastapi/apscheduler 一律用上面的替身
    if _try_real("pydantic") is None:  # pragma: no cover - 缺它整套测试都没意义
        raise RuntimeError("这套测试需要 pydantic（插件 API 模型的运行时），"
                           "请在有 pydantic 的虚拟环境里运行")

    apsched = _force_mod("apscheduler")
    apsched.__path__ = []
    ap_triggers = _force_mod("apscheduler.triggers")
    ap_triggers.__path__ = []
    ap_interval = _force_mod("apscheduler.triggers.interval")
    ap_interval.IntervalTrigger = _IntervalTrigger
    apsched.triggers = ap_triggers
    ap_triggers.interval = ap_interval

    fastapi = _force_mod("fastapi")
    fastapi.__path__ = []
    fastapi.Request = _Request
    responses = _force_mod("fastapi.responses")
    responses.JSONResponse = _JSONResponse
    responses.StreamingResponse = _StreamingResponse
    fastapi.responses = responses

    return app
