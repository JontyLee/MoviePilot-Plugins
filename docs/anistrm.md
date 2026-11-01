# ANi Strm 自用版（CF中转）

按 open ANi 的季度和番剧目录生成 STRM，通过插件内置中转传递目录与媒体请求，并由 FlareSolverr 自动维护 Cloudflare 凭证。

| 项目 | 当前值 |
| --- | --- |
| 插件 ID / 主类 | `ANiStrmRelay` |
| 版本 / 宿主要求 | `1.0.0` / MoviePilot `≥ 3.1.6` |
| Release | [ANiStrmRelay v1.0.0](https://github.com/JontyLee/MoviePilot-Plugins/releases/tag/ANiStrmRelay_v1.0.0) |
| 源码 | [`plugins.v3/anistrmrelay/`](../plugins.v3/anistrmrelay/) |
| 配置前缀 | `anistrm_` |
| 上游基础 | honue 的 ANi-Strm `2.5.1`，GPL-3.0 |
| 操作入口 | 插件配置、中转自检 API；当前没有独立 Vue 侧栏页 |

[仓库首页](../README.md) · [快速开始](#快速开始) · [配置项](#配置项) · [目录与旧文件](#目录与旧文件) · [代理与网络](#代理与网络) · [接口与状态](#接口与状态) · [排查](#排查)

## 快速开始

### 准备条件

- MoviePilot 能访问 FlareSolverr 的 HTTP API，以及 open ANi 源站。
- MoviePilot 对 STRM 存储目录有写权限；媒体服务器能读取生成的 STRM，并通过 HTTP 访问 MoviePilot。
- FlareSolverr 解题浏览器与插件回源使用一致的出口和浏览器 UA。需要代理时使用可达的无认证 HTTP(S) 入口。

### 首次配置

1. 安装插件，在 **FlareSolverr 地址** 填写 MoviePilot 可达的服务地址，例如同一 Docker 网络内的 `http://flaresolverr:8191`。宿主已配置 `FLARESOLVERR_URL` 时可留空。
2. **反代/源站地址** 保持默认 `https://openani.an-i.workers.dev`。
3. **中转访问地址** 填写完整前缀，例如：

   ```text
   http://192.168.1.10:3000/api/v1/plugin/ANiStrmRelay/relay
   ```

   将 IP 和端口替换为部署实际值；该地址必须同时能从媒体服务器和 MoviePilot 自身访问。
4. 需要代理时填写 **中转出站代理**，例如 `http://192.168.1.10:7890`。同一个入口用于浏览器解题、探活与回源。
5. 设置 **Strm 存储地址**，如 `/media/strm`，首次选择 **最新季**。
6. 打开 **启用插件** 和 **内置CF中转**，保存。约 15 秒后启动首次心跳，解题可能超过 100 秒；凭证验证成功后才开始补齐。
7. 查看插件日志或调用自检接口，确认目录已生成，再从媒体服务器播放一集。

插件没有「立即运行一次」开关：启用后保存或重载会自动安排一轮补齐。后续默认每天 `04:00` 更新。

> **现有 STRM：**普通补齐跳过已存在文件。需要修改其中的地址时打开「改写已存在strm」；目标路径外的文件按[目录与旧文件](#目录与旧文件)单独校对。

## 配置项

### STRM 生成

| 界面项 | 配置键 | 默认值 | 含义 |
| --- | --- | --- | --- |
| 启用插件 | `enabled` | 关 | 启用 STRM 补齐与周期任务 |
| 执行周期 | `cron` | `0 4 * * *` | 每天 04:00；显式留空关闭周期更新，保存后的首轮补齐仍有效 |
| 拉取季度 | `selected_seasons` | `["latest"]` | `latest` 为最新季，`all` 为全部季度，也可多选指定季度；空列表不拉取 |
| Strm 存储地址 | `storageplace` | `/downloads/strm` | MoviePilot 环境内的输出目录 |
| 反代/源站地址 | `proxy_base` | open ANi 默认源站 | 上游目录与媒体来源，不填写本插件的中转地址 |
| 使用代理 | `use_proxy` | 开 | 直接目录请求使用宿主代理；中转模式访问本机入口时强制直连 |
| 改写已存在strm | `rewrite_existing` | 关 | 更新当前目标路径下内容不同的 STRM，相同内容跳过 |

`cron` 按 APScheduler 的五段表达式解析；星期使用 `mon`、`sun` 等名字可避免数字含义混淆。

### 中转与凭证

| 界面项 | 配置键 | 默认值 | 含义 |
| --- | --- | --- | --- |
| 内置CF中转 | `relay_enabled` | 关 | 注册保活并将生成的链接指向中转 |
| FlareSolverr 地址 | `fs_url` | 空 | 空值沿用宿主 `FLARESOLVERR_URL` |
| 中转访问地址 | `relay_public_base` | 空 | 外部可达的完整 `/api/v1/plugin/<实例ID>/relay` 前缀 |
| 中转出站代理 | `relay_proxy` | 空 | 解题、探活、媒体与目录回源共用的 HTTP(S) 代理 |
| 中转访问令牌 | `relay_token` | 空 | 中转请求的 `rt` 参数；留空允许匿名访问 |

`cf_clearance` 与 `cf_user_agent` 是插件自动维护的持久化字段，不再提供手工输入框。没有可用 FlareSolverr 地址时会暂停拉取和中转，不能靠旧 cookie 持续运行。

### 三类地址不要混用

| 地址 | 谁访问它 | 示例 |
| --- | --- | --- |
| FlareSolverr 地址 | MoviePilot 调用浏览器控制 API | `http://flaresolverr:8191` |
| 反代/源站地址 | 解题浏览器与插件出站客户端 | `https://openani.an-i.workers.dev` |
| 中转访问地址 | MoviePilot 目录客户端、媒体服务器 | `http://192.168.1.10:3000/api/v1/plugin/ANiStrmRelay/relay` |

例如 `http://moviepilot:3000` 只有在调用方能解析该服务名时才可用；媒体服务器在另一网络时应使用双方可达的地址。映射端口也不一定等于容器内的 `3000`。

## 目录与旧文件

### 输出结构

配置 `/media/strm` 后，保留上游完整相对目录：

```text
/media/strm/
├── 2026-10/
│   ├── 季度根文件.mp4.strm
│   └── 番剧名/
│       └── 第01集.mp4.strm
└── 2026-7/
    └── 番剧名/
        └── 第01集.mp4.strm
```

- 不增加固定的 `openani` 目录。需要该层时直接将存储目录设为 `/media/strm/openani`。
- 上游平铺的季度文件仍保持平铺，不依据文件名猜测番剧子目录。
- STRM 保存播放 URL；CF cookie 与浏览器 UA 只在中转回源时注入，不写进 STRM。
- 本插件负责生成 STRM。媒体元数据整理/刮削可接 MoviePilot 目录监控或媒体服务器扫描流程。

### 补历史季度与改写链接

**补历史：**将拉取季度改为全部后保存。取得文件且所有目录、写入均成功时自动切回最新季；未完整成功时保留全部选择，修复后可再次补齐。

**更换中转地址或令牌：**保存新配置时勾选「改写已存在strm」，并选中需要更新的全部季度。确认目标文件中的 URL 已更新后取消改写开关。

CF 凭证续期不需要改写文件；访问地址或 `relay_token` 改变则需要更新存量 URL。已通过目录监控移动到其它位置的 STRM 不在当前输出目标中，应同步检查最终媒体库中的文件。

### 已有文件的目录校对

插件不会自动移动或删除目标路径之外的文件。已有 STRM 的目录结构若不包含季度前缀，重新补齐可能在目标目录再生成一份。

1. 备份现有 STRM，查看内容中的上游季度和子目录。
2. 对照新目录结构映射目标路径，检查重名与内容冲突。
3. 在确认媒体库使用位置后迁移旧文件，避免直接清空存储根目录。

本插件身份为 `ANiStrmRelay`，与原版 `ANiStrm` 不同，可分别安装。若同时运行，应配置独立输出范围，避免两边写入同一批文件。

## 代理与网络

### 请求链路

```text
MoviePilot ── 内网直连 ──> FlareSolverr /v1
                              └─ 浏览器 + 出站代理 ──> open ANi（取 cookie + UA）

MoviePilot 目录客户端 ──> 本插件 /relay/ ── HTTP/2 + 同一出站代理 ──> open ANi
媒体服务器 ── STRM ──> 本插件 /relay/… ── HTTP/1.1 + 同一出站代理 ──> open ANi
```

出站代理优先级：**显式 `relay_proxy` → 环境中对应协议代理或 `ALL_PROXY` → 已开启 `use_proxy` 的宿主代理 → TUN/直连**。仅支持无认证 HTTP(S) 代理；不支持的配置会暂停，代理失败不会静默切换到其它出口。

FlareSolverr 控制 API 与本机目录入口独立直连，不使用上述出站代理。

### FlareSolverr 浏览器代理

插件通过 FlareSolverr 的 `request.get` 参数指定浏览器代理：

```json
{
  "cmd": "request.get",
  "url": "https://openani.an-i.workers.dev/",
  "maxTimeout": 140000,
  "proxy": {"url": "http://192.168.1.10:7890"}
}
```

不要用 FlareSolverr 容器的 `HTTP_PROXY/HTTPS_PROXY` 代替浏览器参数，这些变量还可能让 Selenium 到本机 WebDriver 的请求绕出。FlareSolverr 的其它部署参数见[官方说明](https://github.com/FlareSolverr/FlareSolverr#commands)。

### 固定出口与内网排除

同一个代理入口不保证同一个公网出口。open ANi 域名规则应使用固定节点，避免自动测速组跨服务器切换；`cf_clearance` 与解题时的出口和浏览器身份相关。

核对出口时比较 **open ANi 同域** `/cdn-cgi/trace` 的 `ip=`，仅比较其它 IP 查询网站不能证明该域走同一路由。同一出口与 UA 是必要条件，也不能保证所有 CF 策略均可通过。

<details>
<summary>配置全局环境代理时的 NO_PROXY 示例</summary>

其它内网服务也应绕过环境代理，按实际服务名和地址调整：

```yaml
environment:
  HTTP_PROXY: http://192.168.1.10:7890
  HTTPS_PROXY: http://192.168.1.10:7890
  NO_PROXY: localhost,127.0.0.1,::1,moviepilot,flaresolverr,postgres,redis,emby,192.168.1.10
  no_proxy: localhost,127.0.0.1,::1,moviepilot,flaresolverr,postgres,redis,emby,192.168.1.10
```

不同 HTTP 库对 CIDR 的支持不同，关键服务优先列出明确名称和 IP。修改 Compose 环境变量需要在容器重建后生效；本插件对 FlareSolverr API 与本机目录入口已有独立直连处理。

</details>

## 接口与状态

默认前缀为 `/api/v1/plugin/ANiStrmRelay`；独立实例使用宿主实际注册的 ID。自检返回的建议地址来自当前请求，可用于核对前缀。

| 方法与路径 | 用途 | 认证 |
| --- | --- | --- |
| GET `/relay` | 自检、上游探测与中转地址提示 | 宿主 `bear` 认证 |
| POST `/relay/` 或 `/relay/<目录>/` | 转发上游目录列表 | 匿名路由；配置令牌时必须带 `rt` |
| GET / HEAD `/relay/<文件路径>` | 媒体流，支持 Range 透传 | 同上 |

**`/relay` 与 `/relay/` 不同：**无尾斜杠的是自检，带尾斜杠的是中转根目录。没有 `/relay/selfcheck` 端点。当前未注册远程事件命令或助手/MCP 工具。

中转 URL 会按配置携带 `?rt=…`。令牌属于访问凭据，建议使用 URL 安全的随机字母数字串，并按部署网络限制匿名入口。

### 自检字段

| 字段 | 含义 |
| --- | --- |
| `plugin_version` | 当前执行自检接口的插件版本 |
| `relay_requested` | 用户保存的中转开启意图 |
| `relay_enabled` | 中转当前是否处于启用状态 |
| `relay_ready` | 后台心跳是否已验证当前凭证 |
| `pause_reason` | 暂停或验证失败原因 |
| `cookie_configured` / `token_configured` | 是否已有 CF 凭证/中转令牌，不返回令牌值 |
| `proxy_source` / `proxy_configured` | 出站代理来源与是否配置 |
| `mp_egress_ip` | 同域 trace 看到的出口 IP 或探测错误 |
| `probe` | 本次目录探测状态、CF 挑战判定或错误 |
| `suggested_public_base` | 根据当前请求得到的中转地址建议值 |

自检会发起网络请求。它的目录探测等待为 20 秒，短于实际批量目录读取的预算；一次自检超时不等于后台目录一定失败。`probe.ok` 是本次探测结果，`relay_ready` 是后台验证状态，两者也不是播放成功证明。

### 续期与暂停

- 首次心跳约在保存后 15 秒触发，之后通常每 5 分钟检查，12 分钟换证阈值在心跳时判断。
- 命中挑战页后标记急需换证；同一拍刚换新证却再次被拦时，先短间隔重探，避免连续重复解题。
- 缺 FlareSolverr、中转地址或不兼容代理时暂停；连续验证失败也会暂停任务，但不会把用户配置开关永久改为关闭。
- 修复后保存配置或重载插件重新验证，确认需要的开关处于开启状态。
- 上游实际滚动下发的新 cookie 会被保存，但源站不保证滚动续期，持续运行依靠 FlareSolverr。

## 排查

先检查插件日志（常见位置 `/config/logs/plugins/anistrmrelay.log`）和 FlareSolverr 日志。

| 现象 | 检查与处理 |
| --- | --- |
| 保存后暂时没有 STRM | 首次解题可能超过 100 秒，先确认凭证已验证，再看季度选择与输出权限 |
| `Connection refused` | 服务未运行或地址/端口过期，Docker 内优先使用稳定服务名 |
| 内网控制 API 返回空 502 | 比较直连与环境代理请求，检查网络连通性及 `NO_PROXY` |
| 中转返回 503 | 查看暂停原因、FlareSolverr 地址与 `relay_ready`，可能还在验证 |
| 中转返回 403 | 检查 `rt` 令牌是否与当前配置一致 |
| 中转返回 502 / CF 挑战 | 查看解题结果、同域出口与配套 UA，避免高并发反复重试 |
| 某季度文件数异常或为 0 | 查看目录请求真实状态与未取得的目录名；502 错误体不应当作空目录 |
| 一部分番剧缺失 | 子目录失败会隔离，其余条目继续补齐；修复网络后选择相应季度重跑 |
| Emby 收到 206 仍无法播放 | 继续验证 MP4 正文和 ffprobe 识别结果，206 只证明响应头已返回 |
| 改了中转地址但播放仍用旧链接 | 开启改写并覆盖对应季度，检查已被移动到最终媒体库的 STRM |
| 换源码后接口版本或行为未更新 | 按宿主实际插件安装/路由重绑流程验证；只看到配置变化不能证明接口已换成新实现 |

<details>
<summary>超时与协议选择</summary>

- 目录控制面单次读取等待 180 秒，网络异常最多尝试两次；外层目录客户端等待 480 秒，媒体流独立等待 300 秒。这些是超时预算，不保证请求在预算内成功。
- 媒体 GET/HEAD 使用 HTTP/1.1，隔离播放器和 ffprobe 提前关闭响应的影响；目录和凭证探活使用 HTTP/2。
- 播放检查应读取足够的 MP4 数据并通过媒体服务器的 ffprobe 确认视频、音轨与时长；仅验证状态码不够。
- FlareSolverr 的解题结果取决于当前网络、浏览器与源站策略，应以本次凭证回放和媒体正文验证为准。

</details>

## 源码职责

| 文件或类 | 职责 |
| --- | --- |
| [`__init__.py`](../plugins.v3/anistrmrelay/__init__.py) 中 `ANiStrmRelay` | 配置、任务、CF 凭证、中转 API 与自检 |
| 同文件 `AniStrmClient` | open ANi 目录访问、季度解析、递归与失败隔离 |
| 同文件 `StrmFileService` | 播放 URL 规范化、STRM 创建与按需改写 |
| [`test_anistrmrelay.py`](../tests/v3/anistrmrelay/test_anistrmrelay.py) | 路径、生命周期、代理、协议与流式响应离线回归 |

## 上游与许可

- 基于 [honue/MoviePilot-Plugins](https://github.com/honue/MoviePilot-Plugins) 的 ANi-Strm 2.5.1（GPL-3.0），以独立 ID `ANiStrmRelay` 维护。CF 失效排查参考上游 #147/#142。
- open ANi 字幕组提供番剧源站和目录接口。
- [FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) 提供自动浏览器接入，是否能通过挑战取决于源站、网络与浏览器环境。

[返回仓库首页](../README.md) · [开发与验证指南](Development.md)
