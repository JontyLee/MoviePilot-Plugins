# MoviePilot-Plugins

**JontyLee 的 MoviePilot V3 个人插件仓库。** 提供 ANi 番剧 STRM 中转、音乐库刮削与小爱音箱播放三个插件，源码与市场索引在本仓库维护。

**当前版本：[v1.0.0](https://github.com/JontyLee/MoviePilot-Plugins/releases/tag/v1.0.0)**

[插件一览](#插件一览) · [安装与更新](#安装与更新) · [目录结构](#目录结构) · [开发与验证](#开发与验证) · [致谢](#致谢)

## 插件一览

| 插件 | 主要用途 | 常用入口 | MoviePilot 最低版本 |
| --- | --- | --- | --- |
| [ANi Strm 自用版（CF中转）](docs/anistrm.md) | 按季度生成番剧 STRM，自动维护 CF 凭证并中转播放 | 插件配置、中转自检 API | 3.1.6 |
| [TagFlow音乐库刮削](docs/tagflow.md) | 检索音乐元数据，补齐标签、封面与歌词 | 侧栏「整理」 | 3.1.6 |
| [小爱音箱](docs/xiaoaispeaker.md) | 将本地音乐推送到音箱，支持语音点歌与定时播放 | 侧栏「发现」 | 3.1.8 |

### ANi Strm 自用版（CF中转）

| 项 | 值 |
| --- | --- |
| 插件 ID / 主类 | `ANiStrmRelay` |
| 源码 | [`plugins.v3/anistrmrelay/`](plugins.v3/anistrmrelay/) |
| 配置前缀 | `anistrm_` |
| 版本 | 1.0.0 |
| Release | [ANiStrmRelay v1.0.0](https://github.com/JontyLee/MoviePilot-Plugins/releases/tag/ANiStrmRelay_v1.0.0) |

- 从 open ANi 获取季度与番剧目录，保留相对路径生成 STRM，支持最新季、多季及历史全季补齐。
- 内置 Cloudflare 透传中转，FlareSolverr 自动取得并续期 `cf_clearance` 与配套浏览器 UA。
- 目录读取和媒体播放共用出站代理策略；凭证验证成功后才启动首轮目录补齐。
- 支持将当前目标路径下的旧 STRM 内容改写为中转地址；缺依赖或验证失败时保留用户开关。

需要可访问的 FlareSolverr 服务、可写的 STRM 目录，以及媒体服务器能访问的 MoviePilot 中转地址。

**[配置与使用说明 →](docs/anistrm.md)**

### TagFlow音乐库刮削

| 项 | 值 |
| --- | --- |
| 插件 ID / 主类 | `TagFlow` |
| 源码 | [`plugins.v3/tagflow/`](plugins.v3/tagflow/) |
| 配置前缀 | `musiclibraryscraper_` |
| 版本 | 1.0.0 |
| Release | [TagFlow v1.0.0](https://github.com/JontyLee/MoviePilot-Plugins/releases/tag/TagFlow_v1.0.0) |

- 直连网易云、QQ 音乐、酷狗、咪咕、MusicBrainz 的公开检索接口，无需部署 Music Tag Web。
- 曲名、艺人、专辑各计 1 分；优先取高分候选，同分时比较年份、音轨、碟号和封面的完整度。
- 自动扫描支持只补缺与强制覆盖，沿用宿主音乐刮削链写入；记录实际改写结果与失败原因。
- 数据页提供已处理、有缺项、失败待重试、待处理四个视图，可逐字段编辑与补封面。
- 已处理但仍缺项的文件按 30 天退避重新尝试补齐，避免每轮向识别源重复请求。

首次建议选择小范围音乐目录，保持「只补缺」和最低匹配分数 `3`。标签会直接写入文件，插件没有撤销功能。

**[配置、评分与补齐说明 →](docs/tagflow.md)**

### 小爱音箱

| 项 | 值 |
| --- | --- |
| 插件 ID / 主类 | `XiaoaiSpeaker` |
| 源码 | [`plugins.v3/xiaoaispeaker/`](plugins.v3/xiaoaispeaker/) |
| 配置前缀 | `xiaoaispeaker_` |
| 版本 | 1.0.0 |
| Release | [XiaoaiSpeaker v1.0.0](https://github.com/JontyLee/MoviePilot-Plugins/releases/tag/XiaoaiSpeaker_v1.0.0) |

- 支持小米账号密码、扫码与 Cookie 票据登录，设备控制经小米 Mina 云端接口完成。
- 本机签发限时播放直链，音箱通过 HTTP 从插件取流；非 MP3 可选由本机 ffmpeg 现场转码。
- 提供目录浏览、曲库索引、歌单、收藏、五种播放方式、睡眠定时与页面歌词。
- 支持设备分组、定时任务、对话记录驱动的语音口令，以及主程序大模型的可选兜底。
- 侧栏查看运行与控制播放；账号、曲库、播放偏好、口令表及任务定义在插件配置页维护。

需要账号下的小爱音箱、MoviePilot 可读的音乐目录，以及音箱能访问的 MoviePilot 地址。播放 URL 会经小米云端下发，详细数据流见使用说明。

**[登录、播放与语音说明 →](docs/xiaoaispeaker.md)**

## 安装与更新

1. 在 MoviePilot 的插件市场仓库列表中添加本仓库地址：

   ```text
   https://github.com/JontyLee/MoviePilot-Plugins
   ```

   通过 `PLUGIN_MARKET` 管理仓库时，将该地址追加到已有列表，多个地址用逗号分隔。
2. 刷新市场，按上表名称安装插件。V3 插件由 `main` 分支的 [`package.v3.json`](package.v3.json) 提供索引。
3. 打开插件配置，按对应使用说明设置目录、连接信息与启用开关，再进行首次运行。
4. Release 页面提供三个插件各自的 ZIP 包；市场安装使用 `插件ID_v版本号` 对应的 Release，仓库标签 `v1.0.0` 标记共同的源码快照。

目录配置均指 **MoviePilot 运行环境内的路径**。Docker 部署应填写容器挂载路径；音箱和媒体服务器使用的 HTTP 地址则必须从它们各自的网络可达。

## 目录结构

```text
MoviePilot-Plugins/
├── plugins.v3/
│   ├── anistrmrelay/       # STRM 生成与 CF 中转
│   ├── tagflow/            # 音乐标签刮削、Vue 前端及发布资源
│   └── xiaoaispeaker/      # 小米接入、播放与语音、Vue 前端及发布资源
├── tests/v3/              # 按插件隔离的离线回归
├── docs/
│   ├── Development.md     # 本仓库开发与验证流程
│   ├── anistrm.md         # ANiStrmRelay 使用说明
│   ├── tagflow.md         # TagFlow 使用说明
│   └── xiaoaispeaker.md   # 小爱音箱使用说明
├── icons/                 # 当前插件图标与历史素材
├── package.v3.json        # V3 市场索引与发布说明
├── AGENTS.md              # 仓库维护约定
└── LICENSE                # GPL-3.0
```

Vue 源码位于插件的 `frontend/`，构建输出为 `frontend/dist/`，宿主加载的发布资源在插件根目录 `dist/assets/`。本机预览、`.codegraph/` 索引与 `.references/` 参考副本由 Git 忽略。

## 开发与验证

先阅读 [开发与验证指南](docs/Development.md) 和 [仓库约定](AGENTS.md)。各插件文档末尾提供源码文件与职责索引。

使用已准备好依赖的 `.venv-test`，在仓库根目录分别执行：

```bash
.venv-test/bin/python -B tests/v3/anistrmrelay/test_anistrmrelay.py
.venv-test/bin/python -B tests/v3/tagflow/test_tagflow.py
.venv-test/bin/python -B tests/v3/xiaoaispeaker/test_xiaoai.py
```

每个命令应输出实际执行的用例数与结果。测试桩按插件隔离，运行入口及前端检查命令见开发指南。

- 插件源码目录名是主类名的小写形式，市场索引键与主类名一致。
- 修改名称时同步核对文档、测试、忽略规则、联邦导出与历史迁移；已发布的配置键和命令兼容性需单独评估。
- 行为发布时同步源码版本、市场版本、置顶历史、README 版本表与相关使用说明。
- 配置保存、API、调度和前端资源均遵循宿主约定，运行数据写入插件数据目录。
- 离线测试、前端构建、浏览器与宿主集成分别验证，按实际结果记录。

## 致谢

本仓库以 [GPL-3.0](LICENSE) 发布。各插件的来源、改写范围和第三方许可说明见对应使用文档与源码注释。

| 上游项目 | 用途与许可 |
| --- | --- |
| [honue/MoviePilot-Plugins](https://github.com/honue/MoviePilot-Plugins) | ANi-Strm 2.5.1 是 ANiStrmRelay 的基础，GPL-3.0 |
| open ANi 字幕组 | 提供番剧源站与季度目录接口 |
| [FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) | ANi 中转的浏览器验证接入 |
| [Music Tag Web](https://github.com/xhongc/music-tag-web) | TagFlow 的平台接口调用、候选归一与匹配约定参考，GPL-3.0 |
| [MusicBrainz](https://musicbrainz.org) | TagFlow 使用的核心元数据，CC0 |
| [songloft-plugin-miot](https://github.com/songloft-org/songloft-plugin-miot) | 小爱音箱的云端接入与语音匹配约定参考，Apache-2.0 |
| [MiService](https://github.com/Yonsm/MiService) | 小米登录与 ubus 协议交叉核对，MIT |
| [MoviePilot](https://github.com/jxxghp/MoviePilot) / [MoviePilot-Frontend](https://github.com/jxxghp/MoviePilot-Frontend) | 插件宿主、SDK 与 Vue 模块联邦契约 |

小米云端、音乐平台与番剧源站属于外部在线服务，其可用性和接口行为由各自服务决定。
