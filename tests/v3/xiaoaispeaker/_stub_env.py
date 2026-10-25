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
        # 真框架在这里会把头按 latin-1 编码（starlette 的 init_headers），非 ASCII
        # 的头名/值当场抛 UnicodeEncodeError。桩不编码就等于把这类 bug 全放过，
        # 所以照做一遍：中文文件名塞进 Content-Disposition 才会被用例抓到。
        for key, value in self.headers.items():
            key.encode("latin-1")
            value.encode("latin-1")
        media_type.encode("latin-1")


class _RequestUtils:
    """宿主 RequestUtils 的桩：默认「连不上」（返回 None），用例自己替换成有响应的那份。"""

    def __init__(self, *args, **kwargs) -> None:
        pass

    def get_res(self, url=None, **kwargs):
        return None

    def post_res(self, url=None, data=None, **kwargs):
        return None


# 宿主调度器「按当前配置重建本插件周期任务」的调用记录：插件从侧栏页改完开关
# 必须喊这一声，否则打开语音监听也没有任务真的在跑。用例读它来断言。
JOB_UPDATES: list = []


def _update_plugin_job(plugin_id: str, *args, **kwargs) -> bool:
    JOB_UPDATES.append(plugin_id)
    return True


class _Settings:
    """宿主 settings 的替身：APP_DOMAIN 供外部地址回落，LLM_* 供智能助手兜底读取。

    默认没配 API Key —— 助手不可用是常态路径，测试要用就自己填上并塞回复脚本。
    """

    def __init__(self) -> None:
        self.APP_DOMAIN = ""
        self.PLUGIN_DATA_PATH = Path("/tmp")
        self.LLM_PROVIDER = "deepseek"
        self.LLM_MODEL = ""
        self.LLM_API_KEY = ""
        self.LLM_BASE_URL = ""
        self.LLM_BASE_URL_PRESET = None
        self.LLM_API_PROTOCOL = "auto"
        self.LLM_USER_AGENT = None
        self.LLM_USE_PROXY = True
        self.LLM_THINKING_LEVEL = None


class Message:
    """langchain 消息的替身：只保留 content，测试按角色序列断言。"""

    def __init__(self, content: str = "") -> None:
        self.content = content


# 类名就是这个替身要顶的部分契约：插件按角色把历史拼回消息列表，用例只能靠
# type(m).__name__ 分辨哪一轮是 user、哪一轮是 assistant，所以这里不加下划线前缀。
class SystemMessage(Message):
    pass


class HumanMessage(Message):
    pass


class AIMessage(Message):
    pass


class _FakeLLM:
    """按脚本吐回复的假模型：记下收到的消息与 get_llm 的参数。

    只给 `ainvoke`：宿主自己的写法是 `asyncio.wait_for(llm.ainvoke(...), timeout)`，插件
    要跟宿主一致；这里要是冒出了同步 `invoke`，用例就会直接 AttributeError。
    脚本元素是异常实例时先把它抛出去，用来演超时与调用失败。
    """

    def __init__(self, script: list, seen_kwargs: dict) -> None:
        self.script = script
        self.seen_kwargs = seen_kwargs
        self.messages: list = []
        self.closed = False

    async def ainvoke(self, messages):
        self.messages = list(messages)
        item = self.script.pop(0) if self.script else ""
        if isinstance(item, BaseException):
            raise item
        return AIMessage(item)


class _LLMHelper:
    """`app.agent.llm.LLMHelper` 的替身。

    真的那三个都是协程或纯函数 + 真网络调用，测试环境里没有 langchain 也没有 key，所以
    这里给一份可脚本化的：`get_llm`/`close_llm` 保持协程（插件必须自己跑掉它们，这条也要测），
    回复取自 `script`，`kwargs` 与 `instances` 让用例看见插件到底给宿主传了什么、
    有没有把模型实例关回去。
    """

    script: list = []
    kwargs: dict = {}
    instances: list = []

    @classmethod
    def reset(cls) -> None:
        cls.script = []
        cls.kwargs = {}
        cls.instances = []

    @classmethod
    async def get_llm(cls, **kwargs):
        cls.kwargs = dict(kwargs)
        llm = _FakeLLM(cls.script, cls.kwargs)
        cls.instances.append(llm)
        return llm

    @classmethod
    def extract_text_content(cls, content, fallback_to_string: bool = False):
        """与宿主同名方法同契约：进来的是响应的 content，不是响应对象本身。"""
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        return str(content) if fallback_to_string else ""

    @classmethod
    async def close_llm(cls, llm) -> None:
        llm.closed = True


def install(plugin_base_cls, settings_obj: Optional[_Settings] = None) -> Any:
    """安装全部宿主桩，返回桩好的 app 包模块。"""
    settings_obj = settings_obj or _Settings()
    _LLMHelper.reset()
    del JOB_UPDATES[:]

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
    scheduler_mod.update_plugin_job = _update_plugin_job
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

    # 宿主智能助手与 langchain 消息类：真货在测试环境里根本装不上（要 key 要网络），
    # 而助手通路是插件 AI 兜底的唯一入口，桩了它才测得到「协程有没有跑掉、消息顺序对不对」
    agent = _force_mod("app.agent")
    agent.__path__ = []
    app.agent = agent
    agent_llm = _force_mod("app.agent.llm")
    agent_llm.__path__ = []
    agent_llm.LLMHelper = _LLMHelper
    agent.llm = agent_llm
    langchain = _force_mod("langchain_core")
    langchain.__path__ = []
    lc_messages = _force_mod("langchain_core.messages")
    lc_messages.SystemMessage = SystemMessage
    lc_messages.HumanMessage = HumanMessage
    lc_messages.AIMessage = AIMessage
    langchain.messages = lc_messages

    return app
