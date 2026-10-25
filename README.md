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

这里只概述功能。判据为什么这么定、每个配置项的含义、失败怎么归因、接口与排查清单，
都在各自的使用说明里（`docs/`），改配置前先读那一份。

### TagFlow音乐库刮削

| 项 | 值 |
| --- | --- |
| 类名 / 目录 | `MusicLibraryScraper` / `plugins.v3/musiclibraryscraper/` |
| 配置前缀 | `musiclibraryscraper_` |
| 版本 | 0.9.0 |
| 元数据来源 | 插件直连的各平台公开检索接口（网易云 / QQ音乐 / 酷狗 / 咪咕 / MusicBrainz），不必部署任何服务 |
| 图标 | `icons/tagflow.svg`（仓库自绘，市场走 raw 地址） |
| 使用说明 | [docs/musiclibraryscraper.md](./docs/musiclibraryscraper.md) |

定时扫描音乐目录，为每个文件检索元数据，再沿用 MP 原生音乐刮削链路写入标签与封面。
只写入 MP 认识的字段：曲名、艺人、专辑、专辑艺人、年份、轨道号、碟号、封面、歌词。

- 五个平台的公开检索接口由插件直连，只发清洗后的「曲名-艺人」检索词，库内路径不出本机。
- 曲名、艺人、专辑各 1 分，低于最低匹配分数（默认 3）判为失败不写入；失败按原因聚合，
  支持只重试失败文件。
- 写入模式两档：只补缺（默认）或强制覆盖；最终是否覆盖取插件与宿主设置中更强的一方。
- 改写可回溯：记下每个文件真正落到盘的改动，点一行看改前 → 改后与文件现状。
- 有缺项可补：编辑弹层先读文件现值填进去，保存只提交真正改动过的字段。
- 主界面「整理」分区有全页入口，配置从页内「配置」按钮弹出的对话框里改。

### 小爱音箱

| 项 | 值 |
| --- | --- |
| 类名 / 目录 | `XiaoaiSpeaker` / `plugins.v3/xiaoaispeaker/` |
| 配置前缀 | `xiaoaispeaker_` |
| 版本 | 0.5.1 |
| 元数据来源 | 无（不刮削，只播放）；设备与播放状态来自小米云端 Mina/ubus |
| 图标 | `icons/xiaoai.svg`（仓库自绘，市场走 raw 地址） |
| 使用说明 | [docs/xiaoaispeaker.md](./docs/xiaoaispeaker.md) |

把 MP 音乐库里的音频推送到小爱音箱上听。播放地址由插件本机签发限时直链，不经过媒体
服务器转码；库内路径与音频内容不出本机。

- 登录支持账号密码、扫码、Cookie 票据三种方式（Cookie 只填 userId 与 passToken 两项），
  小米要图形验证码或手机/邮箱二次校验时插件会点名向小米索取那条验证码（不是干等
  `notificationUrl`）、等不到码可重发；票据只落本机插件数据，账号只回掩码、密码与
  凭据永不回显、也不写日志。
- 曲库索引与文本匹配：一次扫盘把目录与 m3u 收成「歌曲 / 歌单」两张表，「播放周杰伦的
  稻香」这类说法能落到具体文件。
- 侧栏运行视图六栏：运行总览 / 播放台 / 音乐库 / 歌单与收藏 / 定时任务 / 语音控制；
  配置全部在独立的插件配置页里改。
- 设备分组多房间同步、睡眠定时（按分钟或按首数）、五种播放方式、页面侧歌词。
- 语音口令：读小爱对话记录把说的话变成命令，未命中时可选交给主程序已配好的智能助手判定。
- 远程命令五个：`/xiaoai_status`、`/xiaoai_play`、`/xiaoai_control`、`/xiaoai_volume`、
  `/xiaoai_tts`。

## 致谢

本仓库的插件在协议约定与实现上受益于这些开源项目，出处、许可正文与修改声明随仓库一起
公开（详见各插件使用说明的「上游与许可」一节与源码头部注释）：

- [Music Tag Web](https://github.com/xhongc/music-tag-web) —— `GPL-3.0`。TagFlow音乐库刮削
  的逐源查询、候选归一与打分约定以它为上游。
- [songloft-plugin-miot](https://github.com/songloft-org/songloft-plugin-miot) —— `Apache-2.0`。
  小爱音箱插件的小米云端接入约定按它的契约移植。
- [MiService](https://github.com/Yonsm/MiService) —— `MIT`。用于交叉核对小米登录与 ubus 的取值。
- [MusicBrainz](https://musicbrainz.org) —— 只取 `CC0` 核心数据，按官方要求自报身份。
- [MoviePilot](https://github.com/jxxghp/MoviePilot) 与
  [MoviePilot-Frontend](https://github.com/jxxghp/MoviePilot-Frontend) —— 插件 SDK、
  事件与页面联邦契约；侧栏页面的观感参考了主程序的豆瓣中心与 PT 监控室。

小米账号与 Mina 接口、各音乐平台的公开检索接口都是平台自己的在线服务，不属于任何开源
许可范围，也不承诺稳定；长期或商用依赖前请自行确认平台服务条款。本仓库不下载音频、
不绕过会员与版权限制。

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
- [TagFlow音乐库刮削 使用说明](./docs/musiclibraryscraper.md)
- [小爱音箱 使用说明](./docs/xiaoaispeaker.md)
- [插件仓测试说明](./tests/README.md)
- [MoviePilot-Frontend V3 模块联邦指南](https://github.com/jxxghp/MoviePilot-Frontend/blob/v3/docs/module-federation-guide.md)
