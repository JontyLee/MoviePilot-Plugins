# 开发与验证指南

本指南用于 `JontyLee/MoviePilot-Plugins` 个人仓库，长期约定见 [AGENTS.md](../AGENTS.md)。文中命令在仓库根执行，路径相对仓库根。

[返回仓库首页](../README.md) · [确定改动范围](#确定改动范围) · [后端检查](#后端检查) · [前端检查](#前端检查) · [版本与发布](#版本与发布)

## 确定改动范围

先用 `git status --short` 与相关文件 diff 识别已有用户工作，按实际 remote 确认目标仓库。有 `.codegraph/` 索引时先查询相关符号，再读取需要修改的具体实现。

| 插件 | 实现 | 行为说明 | 测试入口 |
| --- | --- | --- | --- |
| TagFlow | `plugins.v3/tagflow/` | [TagFlow](tagflow.md) | `tests/v3/tagflow/test_tagflow.py` |
| XiaoaiSpeaker | `plugins.v3/xiaoaispeaker/` | [小爱音箱](xiaoaispeaker.md) | `tests/v3/xiaoaispeaker/test_xiaoai.py` |
| ANiStrmRelay | `plugins.v3/anistrmrelay/` | [ANi Strm](anistrm.md) | `tests/v3/anistrmrelay/test_anistrmrelay.py` |

README 维护本仓库身份、三个插件的概览与版本表。插件文档与当前版本冲突时，对照源码、相关测试及发布元数据确认现行行为；文档链接和示例路径必须指向实际存在的文件。

### 命名与文档联动

- 市场索引键与主类名一致，源码和测试目录取其小写形式；前端包名使用 `<plugin_id_lower>-remote`，联邦名称使用主类名。
- `AppPage.vue`、`Page.vue`、`Config.vue` 对应宿主的三种联邦入口；辅助文件按业务职责命名。
- TagFlow 整插件回归入口为 `test_tagflow.py`。三个测试文件均可直接运行，必须看到用例数与 `OK`，不能用无输出的退出成功替代测试结果。
- 改名后检查 `.gitignore`、文档与测试命令；修改忽略规则不会自动取消已跟踪文件。预览工装保留在本机，参考源码放 `.references/<上游>/<名称>-<版本>.py`，不与 `plugins.v3/` 中的实现混放。
- 使用说明保留稳定路径 `anistrm.md`、`tagflow.md`、`xiaoaispeaker.md`，开头注明当前插件 ID；内部链接、版本表和配置字段随对应实现同步维护。

## 配置与生命周期

新增配置项应贯通默认值、读取/归一化、完整快照、更新白名单、表单/前端类型及对应测试。缺省、空值和用户显式选择可能有不同语义。

- **TagFlow 管理页**：通过 `initialConfig` 与 `emit('save')` 交宿主保存。
- **TagFlow 侧栏**：`standalone` 从 GET `/config` 起步，通过 POST `/config` 局部更新。保存成功后重新读取配置原值，不用 `/status` 中展开后的生效值覆盖空数组等原始选择。
- **XiaoaiSpeaker**：从 GET `/status` 的 `config` 起步，POST `/config` 部分更新并返回新状态；当前没有 GET `/config`。不能用不含凭据的初始草稿覆盖完整配置，沿用所改组件现有的 `ConfigForm`、`useApi` 或 `query` 封装。

配置验证覆盖管理页和侧栏、缺省与空值、保存失败，以及新实例重载后的持久化值。完整快照还要覆盖 `onlyonce` 等自动回写路径，避免回写时遗漏新键。

修改启用状态或周期时核对宿主任务刷新；停止/重载要清理定时任务、线程、订阅与前端定时器，避免重复启动或重放旧命令。保存和重载应保留既有身份、配置前缀、任务执行标记及状态迁移语义。

## API 与页面联动

- 沿用 `get_api`、SDK 和明确的响应类型。普通 HTTP API、事件命令、语音 AI 兜底与助手/MCP 工具注册是不同入口。
- 新增助手能力时，先核实实际宿主工具注册和权限契约，再验证工具目录与完整调用；不添加未经宿主支持的假定元数据。
- 状态查询只返回调用方需要的字段，设备票据、访问令牌及签名密钥不进入响应或日志；测试同时确认查询不触发播放等控制动作。
- 宿主注入的 `api.get/post` 直接返回响应体。直接调用宿主 `api.get` 时，将 GET 条件编码进 URL，不假设支持 Axios 第二参数或再次读取 `.data`。
- 已有 `useApi.get(path, params)` 会在内部编码查询串，应保留其合法参数，不能与宿主原始接口混为一谈。

页面复用宿主 Vue/Vuetify，保留 `./AppPage`、`./Page`、`./Config` 联邦导出。局部样式不能依赖会被构建门禁过滤的 `.v-*` / `.mdi-*` 全局选择器。

## 后端检查

优先使用已有 `.venv-test/bin/python`；若不存在，先确认项目依赖与替代解释器，不假设系统 Python 已装全依赖。

```bash
.venv-test/bin/python -B tests/v3/anistrmrelay/test_anistrmrelay.py
.venv-test/bin/python -B tests/v3/tagflow/test_tagflow.py
.venv-test/bin/python -B tests/v3/xiaoaispeaker/test_xiaoai.py
git diff --check
```

按涉及插件分别选择入口。桩在导入插件前安装，必须读现有 `_stub_env` 和测试初始化；将两个插件测试硬塞进同一解释器可能造成假失败。

需要按名称筛选用例时，可用独立的发现入口，例如：

```bash
.venv-test/bin/python -B -m unittest discover -s tests/v3/tagflow -p test_tagflow.py
```

修改行为时先检查对应测试目录的当前运行入口，针对失败场景补合适回归；不能直接宣称执行了不存在的测试或通用 CI。

## 前端检查

```bash
npm --prefix plugins.v3/tagflow/frontend run typecheck
npm --prefix plugins.v3/tagflow/frontend run build
npm --prefix plugins.v3/xiaoaispeaker/frontend run typecheck
npm --prefix plugins.v3/xiaoaispeaker/frontend run build
```

只运行涉及插件的命令。若缺少依赖，先看该前端的 package 文件与现有锁文件，再选择安装方式；不能删除用户未提交的锁文件来让构建通过。

当前 Vite 构建输出在 `frontend/dist/`，运行时使用插件根的 `dist/assets/`。交付前核对：

1. 当前 `vite.config.ts` 的真实输出路径；
2. 三个联邦导出仍存在；
3. `remoteEntry.js` 与引用的哈希分片来自同一构建；
4. 已跟踪的发布资源完整更新，旧资源的移除有明确依据；
5. Vue/Vuetify 继续复用宿主，不将全局 `.v-*` / `.mdi-*` 样式带入宿主。

不把“Vite build 成功”当成发布目录已同步；也不手工改生成的分片内容。

## 版本与发布

发布时核对插件类的 `plugin_version`、`package.v3.json` 的 `version` 和置顶 `history` 键。同步 README、插件文档、前端 `package.json` 及锁文件根包的版本。`history` 从 `v1.0.0` 首发说明开始，按实际发布版本维护；使用文档只描述现行行为，不写开发过程或现场变更流水账。

本仓库通过 GitHub Release 发布，使用以下命名：

| 用途 | 格式 | 示例 |
| --- | --- | --- |
| 仓库版本标签 | `v<版本>` | `v1.0.0` |
| 插件市场 Release 标签 | `<插件ID>_v<版本>` | `TagFlow_v1.0.0` |
| 插件安装包 | `<目录名>_v<版本>.zip` | `tagflow_v1.0.0.zip` |

ZIP 根目录必须是插件目录，例如 `tagflow/__init__.py`，包含运行需要的 Python、静态数据和 `dist/assets/`。从 Git 跟踪的源码树打包，避免加入 `node_modules`、缓存、本机预览与参考副本。发布前核对 ZIP 中的版本和联邦资源引用；不能只创建仓库标签而遗漏市场安装包。

市场索引以当前个人仓库为准，保留类名/目录、已有配置前缀、数据迁移和来源信息。提交、推送、发布与宿主重启均需用户明确要求。

文档与实现不一致时，先按源码和相关测试确认当前契约，再更新说明；兼容读取分支应按实际数据来源记录，避免将其误写为当前配置入口。

## 验证结论分层

- **离线测试**：证明确定性逻辑和模拟接口行为。
- **浏览器预览**：证明布局与已模拟状态。
- **宿主集成**：证明运行中的 API 注册、权限、保存和资源加载。
- **真实设备或媒体**：证明外部服务与设备链路，需有对应任务授权。

手工替换 Python 文件后，实例重载未必重绑宿主 API。不要仅凭 `/status` 版本变化断言接口已经执行新实现；宿主重启属于运行环境操作，不由一般代码检查顺带执行。

## 常见遗漏

| 现象 | 重点核对 |
| --- | --- |
| 保存后字段消失 | 完整配置快照、局部更新、空值语义 |
| 保存一次启动多轮任务 | 旧任务收尾、停止标记、宿主任务刷新 |
| 页面筛选无效 | GET 查询串是否真实发送 |
| API 有了但助手看不到 | 实际工具注册与目录，不能只测 HTTP |
| 页面仍旧或空白 | 发布 `dist/assets/` 与 `remoteEntry.js` 引用分片是否同一批 |
| 换源码后接口仍旧 | 宿主可能仍绑定旧路由，按实际部署机制验证 |
| 点「更新」后仍是旧版本 | 宿主市场索引有 30 分钟缓存且安装链路不刷新它；改走「版本历史」的强制读取，或核对宿主日志「加载插件：<ID> 版本：<版本>」，不能只信弹窗提示的「更新成功」 |
