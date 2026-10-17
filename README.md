# MoviePilot-Plugins

MoviePilot 官方插件仓库，也是默认插件市场的源码与索引仓库：
<https://github.com/jxxghp/MoviePilot-Plugins>

当前开发目标是 MoviePilot V3。新插件开发者不需要先阅读 V2 文档，也不要在多份
“适配指南”之间自行拼接流程。

## 从这里开始

### 开发一个新的 V3 插件

阅读 [MoviePilot 插件开发指南（V3）](./docs/Plugin_Development.md)。这是当前唯一的
完整主指南，覆盖目录、最小骨架、生命周期、稳定 SDK、配置与数据、V3 数据库事务、
页面、事件、API、服务、测试和发布。

### 把旧插件迁移到 V3

先阅读主指南，再查看
[V2 插件迁移到 V3](./docs/V3_Plugin_Adaptation.md)。迁移专题只讲旧导入兼容、
数据库事务、媒体身份、音乐链、数据迁移和 V3 合同差异，不再承担从零开发说明。

### 维护官方仓库或发布版本

查看 [仓库与发布指南](./docs/Repository_Guide.md)，了解索引、版本选择、元数据、
CI、Release 和跨仓协作边界。

### 按具体功能查示例

查看 [常见问题](./docs/FAQ.md)。API 返回与前端调用另见
[插件 API 专题](./docs/V3_API_Response_Adaptation.md)。

### 仍然维护 V2 插件

[V2 插件开发指南](./docs/V2_Plugin_Development.md) 仅作为历史版本参考。新插件和
V3 专用实现不要从该文档开始。

## 仓库负责什么

本仓库不是独立运行时：

- `MoviePilot` 负责插件加载、事件分发、API、服务、数据、工作流和 Agent 运行时。
- `MoviePilot-Frontend` 负责配置页、详情页、仪表板和 Vue 联邦组件渲染。
- `MoviePilot-Plugins` 负责插件源码、市场索引、图标、测试、文档和发布流程。

## 当前目录

```text
MoviePilot-Plugins/
├── plugins.v3/              # 当前 V3 专用插件，新插件放这里
├── tests/v3/                # V3 插件测试
├── package.v3.json          # V3 插件市场索引
├── plugins.v2/              # V2 历史专用实现
├── package.v2.json          # V2 历史索引
├── plugins/                 # 更早或跨版本的存量实现
├── package.json             # 默认历史索引
├── icons/                   # 插件图标
├── docs/                    # 开发、迁移、FAQ 和发布文档
└── .github/                 # CI 与 Release 工作流
```

V3 新插件使用 `plugins.v3/<plugin_id_lower>/`、`tests/v3/<plugin_id_lower>/` 和
`package.v3.json`。V3 对旧插件的回退加载只用于兼容存量实现，不是新插件继续写入
旧目录的理由。

## 本仓库插件

### MusicTagWeb音乐库刮削

| 项 | 值 |
| --- | --- |
| 类名 / 目录 | `MusicLibraryScraper` / `plugins.v3/musiclibraryscraper/` |
| 配置前缀 | `musiclibraryscraper_` |
| 版本 | 0.3.0 |
| 元数据来源 | Music Tag Web（MTW），插件不直连任何音乐平台 API |
| 图标 | `icons/music-tag.png`（取自 MTW 应用自带图标） |

定时扫描音乐目录，把每个文件按配置顺序交给 MTW 的 `/apimt/fetch_id3_by_title/`
识别，再沿用 MP 原生音乐刮削链路写入标签与封面。

- 识别源为多选，**留空 = 全部源**：`netease`、`qmusic`、`kuwo`、`kugou`、`migu`、
  `musicbrainz`、`smart_tag`。实测 `qq`、`spotify`、`acoustid`、`all` 恒返回空候选，
  因此不列入默认。某源已出满分（曲名+艺人+专辑 3/3）候选即停止后续查询。
- 写入模式两档，都只作用于 MP 认识的字段（曲名、艺人、专辑、专辑艺人、年份、轨道号、
  碟号、封面、歌词）：
  - 只补缺（默认）：曲名+艺人+专辑+内嵌封面 四项齐全的文件按 mtime+size 记账直接跳过，
    不再请求 MTW；缺哪项补哪项，已有非空标签保持不动。
  - 强制覆盖：忽略增量账、全库重扫，候选与现有标签不同就重写，封面重新下载内嵌，
    歌词强制重写。手工改过的字段会被替换；候选里没有的字段不会清空，
    插件不认识的自定义注释字段也不会删。
- 最终是否覆盖还要叠加宿主「设置 → 媒体刮削」的音乐三项策略，取两者中更强的一个：
  `tag_overwrite = 插件覆盖模式 or 宿主标签策略==覆盖`，`write_tags = 宿主标签策略!=跳过`。
  宿主默认是 标签仅缺失、封面仅缺失、歌词更优才换；把宿主标签策略设成「跳过」时，
  本插件两种模式都只写封面与歌词。
- 状态账（done/failed/runs）写入 plugindata，按 mtime+size 增量跳过；
  数据页、`/status` `/files` `/retry` `/scan` API 与 `/music_scraper_status`
  `/music_scraper_retry` 命令都读同一份账。
- 前置条件：MTW 与 MP 挂载同一份音乐库路径，且配置 MTW 地址与账密。
- 目录名、类名与配置前缀自 v0.2.x 起保持不变：MP 的 `config_data` 以配置前缀为键、
  `plugindata` 以类名为键，改名会让已配置的 MTW 地址账密和已积累的刮削状态账一起失效。

## 最重要的提交规则

- 插件目录名必须是插件主类名的小写形式，主类定义在目录的 `__init__.py`。
- 新增类和方法需要补充说明职责的注释。
- `plugin_version`、索引 `version` 和最新 `history` 必须一致。
- 当前版本历史置顶，所有历史按语义版本降序排列。
- V3 新代码优先使用 `app.sdk`；不要新增对宿主内部目录布局的无必要依赖。
- 宿主数据通过 Oper、Chain 或稳定 SDK 访问；不要直接操作宿主 Model，也不要自行持有
  `SessionFactory` 等裸会话工厂。数据库事务装饰器只用于插件自有表。
- 插件运行数据写入插件数据目录，不要写回源码目录。
- V3 第三方依赖写入插件 `pyproject.toml`，不提交插件 `uv.lock`；V1/V2 保留 `requirements.txt`。
- 第三方依赖安装在宿主共享环境，不能降级或覆盖 MoviePilot 核心依赖，也不要由插件直接执行包管理器。
- 测试放在仓库根 `tests/v3/<plugin_id>/`，不要放进插件源码目录。
- 提交前运行 Python 编译、版本门禁、相关测试和 `git diff --check`。

## 第三方插件仓库

第三方仓库建议 fork 本项目并保留相同目录和索引结构。MoviePilot 插件市场只读取
GitHub 仓库的 `main` 分支；仓库地址通过 `PLUGIN_MARKET` 配置，多个地址用逗号
分隔。

请勿开发用于破解 MoviePilot 用户认证，或提供色情、赌博等违法违规内容的插件。

## 常用链接

- [完整 V3 开发指南](./docs/Plugin_Development.md)
- [V2 插件迁移到 V3](./docs/V3_Plugin_Adaptation.md)
- [插件 API 专题](./docs/V3_API_Response_Adaptation.md)
- [仓库与发布指南](./docs/Repository_Guide.md)
- [FAQ 索引](./docs/FAQ.md)
- [插件仓测试说明](./tests/README.md)
- [MoviePilot-Frontend V3 模块联邦指南](https://github.com/jxxghp/MoviePilot-Frontend/blob/v3/docs/module-federation-guide.md)
