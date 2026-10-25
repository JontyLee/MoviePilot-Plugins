# ANi Strm 自用版（CF 中转 v0.1.0）

基于 [honue/MoviePilot-Plugins](https://github.com/honue/MoviePilot-Plugins) 的 ANi-Strm 2.5.1 迭代，目录接口、季度选择与刮削链路行为不变，新增内置 Cloudflare 透传中转。

## 背景

openani 公共反代（`openani.an-i.workers.dev`）因超出 CF 每日 10 万请求配额，被 Cloudflare 对整个域名强制**交互式人机挑战**（上游 issue #147，2026-08 起社区性失效）。strm 文件里写的直链由 Emby/Jellyfin 在播放时直接请求，不经过 MoviePilot，任何宿主侧网络配置都管不到这一段，因此播放全部 403。

## 原理

`cf_clearance` cookie 重放是目前唯一可无人值守维护的路径。本迭代把透传代理内置进插件自身的 HTTP API：

```
Emby/Jellyfin ── strm 内容 ──> http://<MP>:3000/api/v1/plugin/ANiStrmRelay/relay/<路径>?rt=<令牌>
                                        │  插件匿名中转端点
                                        │  注入 cf_clearance + 解题浏览器 UA，HTTP/2 出站
                                        └──> https://openani.an-i.workers.dev/<路径>
```

- **凭据只存在于插件设置**，不进 strm 文件：cookie 过期时播放返回 502 并带更新提示，只需在插件设置里换新 cookie，**存量 strm 一律不用重新生成**。
- **跟随 CF 滚动续期**：会话保持活跃期间，Cloudflare 会随放行响应重新下发 `cf_clearance`（Set-Cookie）。中转自动捕获并热替换内存凭证（按 5 分钟节流写回插件设置，重启不丢），**一次手贴可覆盖整段连续观看期**；播放日志出现"已捕获滚动刷新的cf_clearance"即在续命。撤销类空值（`DELETED`）忽略不覆盖；长期空闲（超过 challenge TTL，默认约 30 分钟无请求）后 cookie 仍会彻底过期，重复第 1-2 步重新贴一次即可。
- 开启中转后，插件的季度目录拉取（Alist 风格 POST）也走同一条中转链路，目录与播放共用一份 cookie。
- `cf_clearance` 与**出口 IP + User-Agent 绑定**（详见 Cloudflare Clearance 文档），换节点/换 UA 都会重新触发挑战。

## 配置步骤

1. 浏览器（**出口 IP 必须与 MP 容器一致**，见下节）访问"反代/源站地址"，过一次人机验证，从开发者工具（F12 → Network → 任一对源站域的请求 → Request Headers）同时复制 `cf_clearance` 的值和该请求的 `User-Agent`——一次复制里两个都拿到了，保证成对来自同一会话。
2. 插件设置：
   - `cf_clearance Cookie`：粘贴上一步的值；
   - `验证时的浏览器UA`：与第 1 步复制的 `User-Agent` 完全一致（就是同一条 Network 请求头里的那串，别手敲、别用别的浏览器/别的标签页的 UA）；`cf_clearance` 与 UA 绑定，UA 不匹配会重新触发挑战；
   - `中转访问令牌`：建议填一串随机字符（中转端点匿名可达，令牌防滥用；生成的 strm 会自动附带 `?rt=`）；
   - `中转访问地址`：媒体服务器可达的本插件中转前缀，形如 `http://<MP局域网地址>:3000/api/v1/plugin/ANiStrmRelay/relay`。不确定插件 ID 大小写时，登录后调用 `GET /api/v1/plugin/ANiStrmRelay/relay`（bearer 鉴权）自检，返回中的 `suggested_public_base` 即应填值，`probe.ok=true` 表示 cookie 当前可用；
   - 打开 `内置CF中转`。
3. 首次迁移存量文件：勾选 `改写已存在strm` + `立即运行一次`；跑完后日志给出"新增/跳过/改写/失败"计数。此开关按内容比对工作，**幂等、可重复执行**，迁移完成后建议取消勾选（保留则每次任务都会重写内容有差异的文件）。
4. 之后不必盯着 30 分钟续贴：只要持续有播放/目录请求，中转会跟随 CF 的滚动下发自动换 cookie；等播放重新返回 502（长期空闲后彻底过期）再重复第 1-2 步即可恢复播放。

## 出口 IP 对齐（关键）

`cf_clearance` 绑定 **出口 IP + User-Agent**，二者任一与解题那次会话不一致，Cloudflare 都会重新弹挑战。现在中转运行在 MP 容器内、由 MP 的出口访问 openani，所以**解题浏览器的出口必须和 MP 容器的出口是同一个 IP**，否则贴进去的 cookie 对中转的出口无效。

MP 服务器（NAS）的出口和你 Mac/手机浏览器的出口通常是不同 IP，务必用下面任一方式对齐：

- **MP 直连宽带（无代理）**：让解题浏览器也走同一条宽带出口即可。先查 MP 出口：登录后调用自检 `GET /api/v1/plugin/ANiStrmRelay/relay`，返回中的 `mp_egress_ip` 就是 MP 容器的实际公网出口 IP；再让浏览器所在设备用**同一网络出口**打开，去 `https://ip.sb` 核对显示的是同一个 IP，然后再去解题。
- **MP 走固定代理节点（`use_proxy`/`PROXY`）**：把解题浏览器的代理设成**同一个节点**（同出口 IP 的服务器 + 端口），核对 `ip.sb` 显示的 IP 与 MP 一致后再解题。⚠️ 会轮换出口的节点不行——IP 一变 cookie 立即失效，必须锁固定节点。
- **核对通过再贴**：浏览器地址栏先开 `https://ip.sb`，页面顶部 IP == MP 容器出口 IP，才继续；这一步省了，就是白贴。

> 提示：把"出口对齐 + UA 成对 + 滚动续期"三条放一起看——这套方案的"手贴续命"本质是"用与 MP 同出口、同 UA 的浏览器，隔一段时间过一次人机验证再成对回填"，回填一次后靠中转跟随滚动覆盖连续观看期，只有彻底闲置过期才需要再贴。真正全自动依赖 CF 把该挑战降级为 managed（自动）型，本插件不保证。

## 注意事项

- 开启中转后 `使用代理` 会被自动置为 `否`（中转入口是宿主机地址，走外部代理反而会断）；Emby/Jellyfin 容器也**不再需要**为 openani 配置 `http_proxy`。
- 交互式挑战无法用无头浏览器/TLS 伪装自动通过，本方案不做自动解题；若后续源站降级为普通 managed challenge，可再考虑接入 FlareSolverr 自动续期。
- 若不想在 MP 内开启匿名端点，可将 `中转访问地址` 指向独立的反代进程（如仓库外的小转发服务），只把令牌留在内网使用；插件源码不依赖外部进程。
- 插件 ID 独立（`ANiStrmRelay`），与原版 `ANiStrm` 可共存、互不覆盖；配置各自独立，首次安装需重新填写各项。若原版已生成过 strm，切换时记得用本插件的 `改写已存在strm` 迁移。

## 与外部迁移脚本的关系

`migrate_strm.sh`（独立局域网代理方案）仍是批量改写存量文件的一种手段；本插件的 `改写已存在strm` 开关在插件内提供了等价能力，二者取其一即可。

## 致谢

- 本插件是 [honue/MoviePilot-Plugins](https://github.com/honue/MoviePilot-Plugins) **ANi-Strm 2.5.1**（GPL-3.0）的自用迭代：目录接口、季度选择与刮削链路全部沿用原版实现，中转与热替换为新增部分；失效根因与方案取向依据上游 issue #147、#142 的社区排查讨论。
- 源站 `openani.an-i.workers.dev`（worker 反代与 Alist 风格目录接口）由 **open ANi 字幕组**提供，本项目仅做携带凭证的透传，不与任何官方站点分发内容产生关联。
