"""
musiclibraryscraper — MoviePilot v3 插件（插件名：MusicTagWeb音乐库刮削）

用 Music Tag Web（MTW）作为唯一音乐元数据来源，MP 侧走 ScrapingChain 原生 MUSIC 写入。
识别源、字段、封面都来自 MTW 的 /apimt/fetch_id3_by_title/，插件不直连任何音乐平台 API。

v0.2.0：
- 增量扫描：done/failed 状态账（存 plugindata），mtime+size 命中即跳过，不再重复解析 tag。
- 数据页：Vue 联邦交互页（Page.vue），展示 已处理/失败/待重试，支持逐行与批量重试。
- 调度：改用宿主 get_service() 周期任务 + 一次性任务，插件不再自建 BackgroundScheduler。
- 对外：get_api() 提供 /status /files /retry /scan；get_command() 提供状态与重试命令。

v0.2.1（修好 MTW 调用契约，之前一次都没真的刮成功过）：
- 路径前缀 /api/ → /apimt/（MTW 的 Django 路由挂在 /apimt/ 下，旧写法一律 404）。
- 检索关键词用清洗后的曲名（tag 优先、目录结构兜底），不再传 "01 - 轨迹.flac" 这种原始文件名 ——
  MTW 的 fetch_id3_by_title 是拿 title 当搜索词用的，传文件名只会召回噪音。
- 请求体补齐 title/artist/album/full_path/resource，与 MTW 前端调用完全一致。
- 请求 helper 从 post_dict(jsondata=) 换成 post_json(json=)（宿主没有 post_dict 这个方法）。
- 本地 tag 改由 mutagen 读取：MP 的 MetaInfoPath 对音频返回 MetaVideo，根本没有
  artist/album，导致“已有 tag 跳过”永远判 False、匹配也无从下手。
- 比对归一化改为直接复用宿主 app.domain.music.music_text_key（NFKD + casefold +
  rust zhconv 繁转简），本地 NFKC 实现仅作宿主缺该函数时的兜底。注：MTW 的搜索侧
  本身已做繁简归一（綿綿/绵绵 召回同一批），需要归一的只有插件的比对侧。
- 补空模式的完成判据从 title+artist 收紧为 title+artist+album+内嵌封面：实测整库
  1039 个文件的文字标签早已刮齐，只剩约 36% 缺封套图，旧判据会让插件永远空转。

v0.3.0：
- 识别源改为多选（mtw_resource 存数组，仍兼容旧的单字符串），留空 = 全部源：
  netease / qmusic / kuwo / kugou / migu / musicbrainz / smart_tag。实测 qq、spotify、
  acoustid、all 恒返回空候选，没列进默认。
- 逐源按配置顺序查询并合并候选，候选上标记命中的源，写入与状态账都记真实来源；
  某源已出满分（3/3）候选即停止后续查询，避免每个文件都打满 7 次请求。
- 配置页把最容易误解的几件事讲清楚：元数据来源、识别源留空 = 全部源、只补缺 / 强制覆盖
  分别会动到哪些字段，以及最终是否覆盖还要叠加宿主「媒体刮削」的音乐三项策略。
- 更名 MusicTagWeb音乐库刮削、换 MTW 官方图标、说明里点明元数据来源。
  目录名/类名/配置前缀保持不变，否则 config_data 与 plugindata 会整体失效。

v0.3.1：
- 配置说明改为集中式：全部收进页面底部一个说明块，选项本身只留标签，不再用 persistent-hint
  跟在控件下面（排版被长提示撑坏）。
- 补上「查询与选取」规则说明，回答"排序默认是什么规则"：按源顺序逐源查询，某源出 3/3 满分候选
  即停；都没满分则合并全部候选取最高分；曲名/艺人/专辑 各 1 分（繁简与大小写归一后互相包含
  即命中）；同分时按源顺序、同源内按 MTW 返回顺序取第一个；最高分低于 min_score 判失败。
  匹配的本地身份优先读 tag，缺字段按 艺人/专辑 (年)/NN - 曲名 目录结构兜底。
- 移除 mtw_select_mode：它只被解析和回显、从不进任何请求体，也不是 MTW 的入参（MTW 前端源码里
  查不到这个概念），候选取舍一直由上面的打分规则决定，属于死配置。
- 数据页页脚仍是英文的真因：Vuetify 3 的 VDataTable 没有 footerProps（那是 v2 的 API），
  :footer-props 会被当成未知属性丢掉，页脚一直读 locale 默认值。v3 把 makeVDataTableFooterProps
  平铺进 VDataTable 自身，所以 items-per-page-text / items-per-page-options / page-text
  必须直接写在 <v-data-table> 上。
- 图标改用 MTW 界面自带的 avator.webp，已下载进仓库 icons/，市场引用仓库 raw 地址而不是热链实例。

v0.3.2（整体复盘后的健壮性修复）：
- 扫描只有一个入口：宿主周期任务与一次性任务都改走带互斥的 _run_scan_guarded()。此前
  get_service() 把 __scan 直接交给调度器，绕开了 _scan_running 标志，周 cron 与手动
  /scan、/retry、"立即运行一次" 可以并发跑；两本账各自在开头读、结尾写，后写完的会把
  先写的整本盖掉，已刮成功的记录退回未处理。
- MTW 侧故障不再烧重试额度：响应异常/401/接口错误归类成"MTW 不可达"或"MTW 接口或鉴权失败"，
  既不计入 MAX_RETRY、也不写 failed 账。此前 MTW 重启一次或 JWT 过期，一整轮会把整库每个文件
  各记一次失败，连错 5 轮就全体永久跳过（mtime+size 不变就再也不碰）。
- 连续 5 个文件栽在 MTW 故障上就中止本轮（熔断），不再往下白打请求；中止时不 prune 账本。
- JWT 过期按令牌自身的 exp 声明（或 MTW 返回的 expired_at）计算，不再硬编 6 天；请求被判
  鉴权失败时强制重登一次再重试。
- 停止信号：stop_service() 不再 set() 完立刻 clear()（等于从没停过），扫描开始时才 clear；
  信号量由类属性改为每实例独立。
- 扫描每处理 25 个文件落一次账，长时间运行中途容器重启不再丢掉整轮进度。
- /retry 只接受落在 scraper_paths 内、且不在 exclude_paths 内的路径，客户端传什么都改不了库外文件。
- add_plugin_once_job 返回 False（调度器没在跑）时回落后台线程，不再假装"已提交"。
- 数据页翻页：v-data-table 的 pageCount 由内存里的 items 长度算出，而 items 只是服务端返回的
  当页（≤ limit 条），pageCount 恒为 1 → 下一页按钮永久禁用，这才是 "1 of 1" 的真因
  （v0.3.1 只把文案翻成中文，功能仍是坏的）。改用 v-data-table-server，总条数交给 items-length。

v0.3.3（第二轮复盘）：
- MTW 故障的判据从"任一源失败"收紧为"没有任何源成功应答"：某个上游挂了（musicbrainz 限流、
  咪咕 500）而其余源正常返回空候选时，那是"查无此曲"，要按文件失败记账；否则一轮里前 5 个文件
  就熔断，整库永远停在"待处理"。
- mtw_resource 只接受已知 7 源，未知值（旧配置残留的 qq/all、或 API 写坏的）直接丢弃并告警：
  一个必然失败的源会放大成上面那条误判。
- 枚举与 /retry 的扩展名改取「宿主 audio_extensions ∩ 本地列表」：宿主写入链自己会按可配置的
  白名单过滤，.wv/.aif 不在其中，此前每个这类文件要先白打最多 7 次 MTW 请求、再收到
  "刮削路径中没有支持的音频文件"，烧满 5 轮 retries 才永久跳过。交集结果缓存 5 分钟，
  免得一次上千路径的重试要构建上千遍宿主配置快照。
- min_score 在服务端 clamp 到 1-3：填 0/负数会让只对上曲名的候选整库重写标签（写进文件不可逆），
  填 >3 则永远不达阈值、整库烧满 retries 永久跳过。配置页的 min/max 只是浏览器提示，挡不住 API。
- 熔断不再豁免大批量定向重试：批次长度超过阈值时同样中止，否则 /retry 全部失败项在 MTW 真挂时
  会把几千个文件 × 全部源打到底。
- 定向重试的汇总改写 last_targeted，不再覆盖 runs：点一次行内"重试"就把整盘汇总刷成
  scanned=1、pending=0，看起来像整库已经扫完。
- 落账阈值从"每 25 次变更"改成"有变更且距上次 ≥60s"：plugindata 是单行 JSON，整本重序列化是
  O(库大小)，5 万条目一轮能打上几千次全量写。
- 每个文件的 tag 只解析一次（此前补空判定、拉候选、挑选各读一遍）。
- "待处理"桶加 30s 短期缓存：它是全库遍历做差集，数据页翻页、搜索、刷新每次都重算一遍；
  扫描开始与结束时作废，不会让界面上的数字滞后于账本。
- force（绕过增量账）与 overwrite（覆盖已有标签）拆开传递：此前"全量覆盖"按钮只发 force，
  标签仍按只补缺写入，用户以为在覆盖其实没有；定向重试则只绕过账本，不顺带替换手工改过的标签。
- 换 MTW 地址或账密时丢弃缓存 JWT：那是上一个实例签发的，继续带着只会白吃 401。
- /retry 命令入口（/music_scraper_retry）与 API 走同一套库内路径判据，不再照着账本里已删除、
  已挪出库的路径白跑一轮（stat 失败直接跳过、账本原样留着，看着像"重试过了还是失败"）。
- MTW 把结果再包一层 {"data": {"data": [...]}} 时不再被认成"拿不到结论"。
- 未配置 MTW 账密时"将匿名请求"只告警一次：这是每个请求都会走的分支，此前一轮几千个
  文件能把日志刷满几千行。
- 读账本时把不是 dict 的条目归一成空 dict（旧版本/手工编辑/写坏一半的 plugindata）：
  此前 prev.get() 会抛 AttributeError，从 /status 到整轮扫描一起崩。
- 数据页：offset 越界（桶变小）时收敛到最后一页再取，不再显示"该桶暂无数据"；v-tabs 选中态
  改绑 bucket（写死 model-value 时高亮停在旧标签）；批量重试与逐行重试统一走同一套提示。
- 前端 tsconfig 不再把 vite.config.ts 同时列进主程序与引用工程：TS6305 会让 vue-tsc 门禁整体失败。
- _enumerate_files 改用 os.walk(followlinks=False) 并在目录级剪枝 .trickplay：pathlib 的 **
  会递归进软链目录，库内一个回环软链能让枚举永不返回、互斥槽被永久占住。
  /files?bucket=pending 的 30s TTL 缓存同时覆盖数据页搜索框每停顿一次的重算。

v0.3.8（真机 12 个失败项只说"差一分"，说不出差在哪一项）：
- 失败原因要落到逐项判定：账本里 12 条失败全是"未达阈值（最低匹配分数 3）"+ 分数 2，看不出
  到底是候选对不上，还是本地根本没这项可比 —— 而这两种要做的决定相反：前者放宽阈值或改文件名，
  后者（整张 CD 压成一个 wav，目录里也没有曲名）降阈值救不回来，只能把它排出扫描范围。
  _score_candidate 一直逐字段判过，只是把结果折成一个整数就丢了。现在拆成 hit / miss /
  本地缺这项 三态（_field_hits），原因写成"未达阈值（最低匹配分数 3）：曲名未对上、本地无专辑"，
  /status 的 failure_reasons 与数据页按它自动分类。
- 本地读不出曲名的文件不再记成"MTW 无候选"：一个请求都没发过，写"无候选"会让人以为曲库里
  查不到，其实是插件连该问什么都不知道。
- 失败要逐行进日志：未达阈值与无候选此前完全静默（只有写标签失败才有一行），19 个文件的定向
  重试里 12 个失败跑了 100 分钟没有一行输出，看着就是卡死。现在每个失败文件一行，带原因、
  分数和第几次重试（共 MAX_RETRY 次）。

v0.3.7（切换分桶看到的是同一批数据）：
- 切换"已处理/失败待重试/待处理"看到同样一批行，根因不在后端的分桶 —— _collect_rows
  三分支各走各的账，而是请求根本没带上桶名：宿主注入给插件的 api.get 只认第一个参数
  （豆瓣中心的插件都是把 ?page=… 直接写进 URL 的），第二个对象被静静丢掉。于是三次点击
  发出的都是裸 GET /files，后端按默认值回答 done 的第一页。参数改由前端自己编码拼进
  URL，宿主的访问日志里现在能看到 /files?bucket=pending&limit=50&offset=0。
- 数据页与配置页改成侧栏版式（参考豆瓣中心/PT监控室）：数据页左侧是 概览 + 三个桶，
  概览先给整轮结果（四个计数、最近一轮、本轮生效的配置、失败原因分布、流水线）再进某一桶；
  配置页左侧是 流程总览 / 运行与写入 / 识别与匹配 / 音乐库范围，先把"一个文件从被扫到写标签
  经过哪些判断、各由哪个开关决定"讲完，再摆具体配置项。两页的流水线读同一份 flow.ts，
  改了判据不会只改一个页面。
- 两页都按 layout 事件向宿主申请宽度（配置 74rem、数据 86rem）：宿主对话框默认 60rem，
  侧栏 + 表格会被挤成一团。

v0.3.6（真机跑过一轮之后）：
- 星期编号按宿主的调度器算，不是按 crontab：宿主把这段 cron 交给 APScheduler
  （CronTrigger.from_crontab），它的星期是 0=周一…6=周日、上限 6；crontab 的 0/7=周日 在这里
  一个是周一、一个直接校验失败。默认值 "0 3 * * 0" 因此实际跑在周一，而它写的是"每周日"；
  用户按习惯写 "… * 7" 又被整条回落成默认，等于同时丢了日子和时刻。现在默认用名字
  "0 3 * * sun"，单独的星期 7 就地换成 sun（保留其余字段），0-6 原样交给调度器不做猜测。
- 生效的 cron 要连着"下次触发在周几"一起回显：光看表达式猜不出调度器读出来的日子，
  注册时打一行、/status 与状态命令各带一条。数据页与命令的输出同时补 runs 的 error 档。
- 失败原因要按原因聚合，逐行看 reason 做不出决定：调「最低匹配分数」、加识别源靠的是
  "这一类有几个、这类里最高几分"，而账本一行一个文件、翻页翻不出这个数。/status 新增
  failure_reasons（原因、个数、最高分、还有没有可重试的），数据页与状态命令直接给出
  "阈值降 N 分可命中"这类结论。
- 保存配置不再弹两条通知：配置页走插件自己的 Vue 组件而不是宿主内置表单，页面 emit('save')
  后宿主收到配置、自己 PUT /plugin/{id}，成功再弹一条"已保存"。配置页此前也弹一条
  "配置已保存"，一次保存就是两条；更糟的是这条发生在请求发出之前，保存失败时照样说"已保存"。
  现在配置页只把配置交出去，成败与插件名由宿主一起报。
- 真机验证：1039 个文件里 21 个的内嵌封面是 v0.3.5 判得出、此前判不出的（ID3 的 APIC 实际
  键名带描述，如 "APIC:0"，而 DSF 这类容器连通用 pictures 接口都没有），反向没有一个是
  新判据丢掉封面的。

v0.3.5（第四轮复盘）：
- 内嵌封面终于判得出来：补空判据是 title+artist+album+内嵌封面，而封面只按帧名
  tags.get("APIC") 直取 —— APIC 是可重复帧，mutagen 的取法是 getall("APIC")（键还带描述），
  APEv2 的封面 key 更是 "Cover Art (Front)"，压根不在那份表里。后果比判不出来更贵：
  已经刮齐标签、只差封面的 MP3/APE 会被永远判成"没齐"，每轮重打最多 7 次 MTW 请求，
  候选不达标还白涨 retries，五轮之后一个本来完整的文件被永久列成超限。改成一整套读法
  （通用 pictures 接口 + getall + 键名前缀/全名匹配）。这条链路此前一条测试都没有：
  测试环境没有 mutagen，_read_music_tags 恒返回空值、has_cover 恒为 False。
- 目录兜底的身份不再拿库根的名字当专辑、拿库根的上一级当艺人：/media/music/01 - 轨迹.flac
  会得到 album="music"、artist="media"，这两个假身份既被当搜索词发给 MTW，比对又是双向
  包含（want in got 也算命中），一个目录名就能白送两分 —— min_score 放宽到 1-2 时足以把
  错的艺人写进文件，而写标签不可回滚。现在只认严格落在 scraper_paths 之下的目录，
  猜不出来就留空判"未达阈值"。
- 账本里的 retries 要归一成整数：它是 int(prev.get("retries", 0)) 直接吃的值，写成 null、
  "三次" 或旧版本的 dict 都会抛 TypeError，而抛在逐文件循环里等于整轮中止 —— 崩的位置由
  账本键序决定，下一轮枚举走到同一条路径还是同一个位置，那之后的文件永远处理不到、
  汇总永远不落账、/status 一起 500。v0.3.3 说的"读账本时把坏条目归一"只归一了外层不是
  dict 的条目，字段级坏值仍然会崩。
- 单个文件把插件或宿主打崩不再带走整轮：__scrape_file 直接调用宿主 ScrapingChain，外面
  一层保护都没有。落账每 60 秒一次，一轮跑到第七千个文件崩掉等于前面六千多个判断白做，
  槽位释放、下一轮从头再跑。现在一个文件的异常只影响它自己（不记成功也不记失败，
  下一轮照原样再来），连着出现按环境异常熔断，汇总新增 error 一档。
- 复位停止信号挪进锁内、占位之前：此前 clear() 落在锁外，置位 → 出锁 → clear 之间挤进来
  的 stop_service 会被抹掉，而它想停的正是紧接着这一轮 —— 界面按了"停止"、日志里一句
  "收到停止信号"都没有。
- 枚举的停止信号要挡住外层库循环：内层 break 只退掉当前这个库的 os.walk，下一个库照走，
  按了停止还在扫全盘，走完后还被当成"这一轮扫过的文件"写进汇总。
- /files 不认识的桶名不再悄悄落到 done 那一档：回显仍是请求的那个名字，调用方拿到一批
  "已处理"的行却以为是自己传的桶。现在明确回 unknown_bucket、空列表。
- 熔断与待处理口径不变：MTW 故障与处理异常都不记在文件头上，汇总的"待处理"仍按账本算，
  和数据页的桶一致（顺带删掉计数器时代留下的 max(0, pending)）。

v0.3.4（第三轮复盘）：
- MTW 的 HTTP 状态码重新可见：宿主 RequestUtils.post_json 里那句 `if response:` 判的是 requests
  的 Response.__bool__（等价 status_code < 400），401/403/5xx 会连状态码带错误体一起被折成 None。
  后果是"JWT 过期"和"MTW 重启了"在插件眼里长得一模一样 —— v0.3.2 写的那条"鉴权失败强制重登一次
  再重试"实际永远走不到，过期令牌会被一路当成 MTW 不可达，还把熔断触发一次。改由插件自己
  拿住 Response 归因：401/403 → 鉴权失败（重登重试），其它 4xx/5xx → 接口报错，连不上 → 不可达；
  三者都不记到文件头上，但只有鉴权那一种会去重登。
- 登录失败负缓存：账密错或 MTW 关掉了 /apimt/token/ 时，此前每个文件、每个源都要先登一次，
  放大成几十万次的必然失败请求 + 一屏同样的错误日志。失败后 60 秒内直接按未登录处理；
  登录成功或人改过账密时立刻作废这段静默期。
- 宿主音频白名单区分"读不到"和"被清空"：此前两种都是空列表，于是 docstring 里承诺的
  "宿主什么都不收就返回空集、不退回本地列表硬扫"根本没实现。现在读不到 → None → 按本地列表扫；
  明确清空 → 交集为空 → 一个文件都不碰，并告警说明原因。
- 软链文件也要收敛在库内：followlinks=False 只挡目录，库内一个指向库外的 .flac 软链照样会被
  写标签（宿主按插件递过去的路径写文件），而 /retry 用 realpath 判定拒收的正是同一条路径。
  枚举与 /retry 改用同一份 realpath 基准（_library_bounds + _target_in_library）；排除目录改按
  realpath 整棵剪枝：把它配成别名或软链形式时，逐文件比字符串会整片漏掉，扫描照刮照写、
  /retry 与待处理桶却拒收。被跳过的库外软链按数量报一条 warning 并写进配置页说明，
  否则用户只看到"发现 N 个文件"比目录里少、查不出为什么。
- 汇总里的"待处理"改为直接按账本算（这份文件列表里既不在 done 也不在 failed 的那些），
  和数据页的待处理桶是同一个口径。此前它是计数器加减：MTW 故障的文件不落账所以要留着，
  而上一轮记过失败、这一轮撞上故障的文件账本原样还在失败桶里，多算一份就两边对不上号。
- /apimt/token/ 的成功响应不再按检索结果的形状兜底（它本来就没有 data 字段）：
  一次成功登录会被说成"MTW 不可达"，日志和任何按 reason 分支的调用方都跟着错。
- 投递一次性任务前先看一眼槽位：回调 3 秒后才跑、那时抢不到槽会静默跳过，此前 /scan 与
  "立即运行一次"会对着界面报"已提交"、实际一轮都没跑。
- 数据页所有列改 sortable:false：/files 恒按路径排序，留着表头箭头点一下只重排眼前这一页，
  用户看到的是一张既不全、又和总条数对不上的表。
"""
from __future__ import annotations

import base64
import json
import os
import re
import threading
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from apscheduler.triggers.cron import CronTrigger
from fastapi import Request

from app import schemas
from app.chain.scraping import ScrapingChain
from app.plugins import _PluginBase
from app.schemas.types import EventType
from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.media import MusicInfo
from app.sdk.network import RequestUtils

try:  # 宿主较新版本才有 SDK 调度门面；缺失时回退到后台线程
    from app.sdk import scheduler as scheduler_sdk
except Exception:  # pragma: no cover - 视宿主版本而定
    scheduler_sdk = None


# MP 支持的音乐扩展名，独立于视频扩展名白名单。
_MUSIC_EXT = (".flac", ".mp3", ".m4a", ".ape", ".wav", ".wv", ".tta",
              ".aif", ".aiff", ".opus", ".ogg", ".mpc", ".dsf", ".dff", ".mka")
# 与宿主音频白名单求交后的结果缓存（见 _supported_audio_ext）。
_AUDIO_EXT_TTL = 300.0
_AUDIO_EXT_CACHE: Dict[str, Any] = {"ts": 0.0, "exts": None}

# done/failed/runs 状态账的 plugindata key。
_KEY_DONE = "done"
_KEY_FAILED = "failed"
_KEY_RUNS = "runs"
# 定向重试（手动点某一行/批量重试）的汇总单独存这里：它只覆盖几个文件，混进 runs
# 会把整盘汇总刷成 scanned=1、pending=0，看起来像库已经扫完了。
_KEY_TARGETED_RUNS = "last_targeted"
# 默认 cron：每周日 03:00。星期用名字而不是数字：宿主把这段字符串交给 APScheduler
# （CronTrigger.from_crontab），它的编号是 0=周一…6=周日，和 crontab 的 0/7=周日 差一位，
# 而且不认 7。"0 3 * * 0" 在这个调度器上是周一，写名字才没有歧义。
_DEFAULT_CRON = "0 3 * * sun"
# _score_candidate 的满分：曲名 / 艺人 / 专辑 各 1 分。
_MAX_SCORE = 3
# 打分的三项，顺序即失败原因里的表述顺序。
_MATCH_FIELDS = (("title", "曲名"), ("artist", "艺人"), ("album", "专辑"))
# 逐项判定的三种状态：对上 / 有得比但没对上 / 本地缺这项（无从比起）。
_HIT = "hit"
_MISS = "miss"
_NO_LOCAL = "no_local"
# MTW 实测能返回候选的识别源（顺序即查询顺序）：qq / spotify / acoustid / all 恒返回空，
# 不要列进来。migu 单源给 20 条，musicbrainz 给繁体字形，其余各 10 条。
_DEFAULT_RESOURCES = ("netease", "qmusic", "kuwo", "kugou", "migu", "musicbrainz", "smart_tag")

# MTW 侧故障的原因（连不上 / 鉴权失效 / 接口报错）。这类失败说明插件没学到任何关于该文件的信息，
# 因此不计入 MAX_RETRY、也不写 failed 账：否则 MTW 重启一次就给整库每个文件记一笔失败，
# 连错满 MAX_RETRY 轮后全体永久跳过，mtime+size 不变就再也刷不动。
_UNREACHABLE_REASON = "MTW 不可达"
_AUTH_REASON = "MTW 鉴权失败"
_API_ERROR_REASON = "MTW 接口报错"
_TRANSPORT_REASONS = (_UNREACHABLE_REASON, _AUTH_REASON, _API_ERROR_REASON)
# 熔断：连续多少个文件栽在 MTW 故障上就中止本轮，别在 MTW 挂着的时段空打上千次请求。
_ABORT_AFTER_TRANSPORT = 5
# 登录失败后的静默期（秒）：账密错了或 MTW 关掉了 /apimt/token/ 时，每个源、每个文件都会
# 再登一次 —— 一轮下来是几十万次的必然失败，还顺带把错误日志刷满。
_LOGIN_BACKOFF = 60.0
# 扫描中途落账的最小间隔（秒）：整轮动辄数小时，中途容器重启不该把进度全丢掉。
# 按时间而不是按变更次数：plugindata 是单行 JSON，每次 save 都要把整本账重新序列化，
# 固定"每 25 次落一次"在 5 万条目的库里会打出上千次全量写。
_FLUSH_INTERVAL = 60.0
# /files?bucket=pending 的结果缓存时长（秒）：该桶要全库枚举，搜索框每停顿一下就会发一次请求。
_PENDING_TTL = 30.0

# 配置页底部的一段集中说明：来源、查询与选取规则、两档写入模式、宿主策略叠加。
# 不跟在单个配置项下面，选项本身只留最短的标签，细节全部收在这里。
_FORM_NOTES = (
    "【元数据来源】全部取自 Music Tag Web（MTW）的识别结果，插件不直连任何音乐平台。"
    "识别源可多选，留空即全部源（网易云 / QQ音乐 / 酷我 / 酷狗 / 咪咕 / MusicBrainz / 智能）；"
    "qq、spotify、acoustid、all 实测恒返回空候选，没有列进来；不在上面这份清单里的源会被直接忽略。\n"
    "【查询与选取】每个文件按这里的源顺序逐源查询，某源出现 3/3 满分候选就立刻停止，不再问后面的源；"
    "都没满分则合并所有候选取最高分。分数为曲名 / 艺人 / 专辑 各 1 分（先做繁简与大小写归一，"
    "互相包含即算命中）；同分时按源顺序、同源内按 MTW 返回顺序取第一个。"
    "最高分仍低于「最低匹配分数」（默认 3，即要求三项全中）就记为失败、不写入。"
    "该项只接受 1-3，越界会被收回到边界值：调到 1-2 表示只对上曲名或艺人也照样改写标签，"
    "标签是写进文件的、无法回滚；调到 3 以上则永远不会达标，整库都会被记成失败。"
    "用于匹配的本地身份优先读文件已有的 tag，缺哪项就按 艺人/专辑 (年)/NN - 曲名 的目录结构兜底；"
    "只有严格位于「音乐库路径」之下的目录才算艺人/专辑，库根本身的名字不参与猜测，"
    "猜不出来就留空（宁可判未达阈值，也不拿目录名凑一个假身份去改写标签）。\n"
    "【写入模式】只补缺：曲名+艺人+专辑+内嵌封面 四项齐全的文件按 mtime+size 记账直接跳过"
    "（不再请求 MTW），缺哪项补哪项，已有非空标签保持不动，手工修改不会被覆盖。"
    "强制覆盖：每轮忽略增量账、全库重扫重问，候选与现有标签不同就重写，封面重新下载内嵌、"
    "歌词强制重写，手工改过的曲名/艺人/专辑/年份/轨道号会被替换；候选里没有的字段不会清空，"
    "插件不认识的自定义注释字段也不会删。\n"
    "【叠加宿主策略】最终是否覆盖还取宿主「设置 → 媒体刮削」音乐三项策略与这里的较强者："
    "宿主标签策略=覆盖时，即使这里选只补缺也会重写；=跳过时，两种模式都不写该项。"
    "宿主默认 标签仅缺失、封面仅缺失、歌词更优才换。\n"
    "【会处理的文件】只扫描宿主音频白名单（设置 → 媒体刮削 → 音频扩展名，默认 RMT_AUDIOEXT）"
    "与本插件支持列表的交集：宿主不收的扩展名写标签时会被原生刮削链直接拒成"
    "「刮削路径中没有支持的音频文件」，先排除掉才不会白打请求、又把文件记成失败并烧满重试次数。"
    "Jellyfin 的 .trickplay 预览缓存目录整棵跳过；目录软链不跟随，避免回环目录把这一轮卡死。"
    "指向库外的音频软链也会跳过并在日志里报数量 —— 写标签是按路径落到真实文件的，"
    "让扫描改到音乐库之外的文件不可接受；确有需要请把它指向的目录也加进库路径。\n"
    "【同一份库】MTW 与 MP 需挂载同一份音乐库路径，否则写标签会落到不同文件。"
)

# 各容器格式的 tag key：ID3 帧名 / MP4 atom / Vorbis 注释 / APEv2。
_TAG_KEYS = {
    "title": ("TIT2", "\u00a9nam", "title", "TITLE"),
    "artist": ("TPE1", "\u00a9ART", "artist", "ARTIST"),
    "album": ("TALB", "\u00a9alb", "album", "ALBUM"),
}
# 内嵌封面的 key 形状（各容器读法不同，见 _has_embedded_cover）：
# MP4 atom / Vorbis 注释按全名匹配，ID3 的 APIC 帧与 APEv2 的 Cover Art 按前缀匹配。
_COVER_KEYS = ("covr", "picture", "metadata_block_picture")
_COVER_KEY_PREFIXES = ("apic", "cover art")
# 常见 “NN - 曲名” / “NN. 曲名” 前缀。
_TRACK_PREFIX_RE = re.compile(r"^\s*\d{1,3}\s*[-._)]\s*")
# 目录名尾部的 “ (2017)” / “ [2017]” 年份。
_YEAR_SUFFIX_RE = re.compile(r"\s*[\[(]\s*(?:19|20)\d{2}\s*[\])]\s*$")
# 比较用归一化：去掉所有空白与非字母数字符号（宿主归一化不可用时的兜底）。
_NORM_STRIP_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")

try:  # 宿主自带的音乐文本归一化：NFKD + casefold + 繁转简（rust zhconv）
    from app.domain.music import music_text_key as _music_text_key
except Exception:  # pragma: no cover - 视宿主版本而定
    _music_text_key = None


def _norm_text(value: Any) -> str:
    """归一化标题/艺人/专辑，供候选比对。

    优先复用宿主 app.domain.music.music_text_key —— 它已经统一了繁简、全半角、
    变音符并忽略排版符号，本插件的失败样本（綿綿/陳奕迅/這所有的一切）全部命中。
    宿主没这个函数时退回 NFKC + 去符号的本地实现。
    """
    if value is None:
        return ""
    text = str(value)
    if _music_text_key is not None:
        try:
            key = _music_text_key(text)
            if key:
                return str(key)
        except Exception:
            pass
    return _NORM_STRIP_RE.sub("", unicodedata.normalize("NFKC", text).casefold())


def _normalize_resources(value: Any, warn: bool = False) -> List[str]:
    """把 mtw_resource 归一成去重保序、且只含已知源的列表。

    配置项从单选改成了多选，但库里可能还存着旧的单字符串（"netease"），
    也可能被前端写成逗号/分号分隔的串。直接对字符串迭代拿到的是字符，
    所以任何入口都必须先过这里。空值回落到"全部源"。

    未知值必须在这里就丢掉：一个必然报错的源（旧配置残留的 qq/all、或 API 写坏的数组）
    会让每个文件都多打一次失败请求，而"有源失败"会被拉候选那边当成 MTW 故障 ——
    结果是整轮在前几个文件就熔断，全库永远停在待处理。
    """
    if isinstance(value, str):
        raw: List[Any] = value.replace(",", ";").split(";")
    elif isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        raw = []
    known = set(_DEFAULT_RESOURCES)
    out: List[str] = []
    unknown: List[str] = []
    for item in raw:
        if item is None:
            continue
        name = str(item).strip()
        if not name or name in out:
            continue
        if name in known:
            out.append(name)
        elif name not in unknown:
            unknown.append(name)
    if unknown and warn:
        logger.warning(
            f"[music-scraper] 忽略不认识的识别源 {'、'.join(unknown)}"
            f"（可用：{'、'.join(_DEFAULT_RESOURCES)}）"
        )
    return out or list(_DEFAULT_RESOURCES)


def _host_audio_extensions() -> Optional[List[str]]:
    """宿主写入链实际接受的音频扩展名（设置里的 RMT_AUDIOEXT，可在界面上改）。

    ScrapingChain 在写标签前会按这份白名单过滤文件，不在里面的直接返回
    "刮削路径中没有支持的音频文件"。插件要先看它一眼，否则每个不收的扩展名都要
    先白打最多 7 次 MTW 请求、再被记一次刮削失败，烧满 retries 才永久跳过。

    读不到（宿主改版、配置还没加载）与"读到了但一个都不收"必须分开：前者只能按插件
    自己的列表扫，后者是真的什么都不该扫。两者都返回空列表时，调用方无法区分，
    要么把白名单清空变成硬扫全库、要么把可用列表误判成空。None 表示读不到。
    """
    exts: Any = None
    try:
        from app.application.configuration import get_chain_runtime_config_snapshot

        exts = get_chain_runtime_config_snapshot().audio_extensions
    except Exception:
        try:
            exts = getattr(settings, "RMT_AUDIOEXT", None)
        except Exception:
            exts = None
    if exts is None:
        return None
    out: List[str] = []
    if isinstance(exts, str):
        exts = exts.split(",")
    for item in exts or []:
        name = str(item).lower().strip()
        if not name:
            continue
        out.append(name if name.startswith(".") else f".{name}")
    return out


def _supported_audio_ext() -> Tuple[str, ...]:
    """本插件真正会处理的扩展名：本地候选列表 ∩ 宿主白名单。

    读不到宿主配置时按本地列表走（此时可能包含宿主不收的扩展名，功能不受影响、
    只是那几个文件会记成失败）。宿主明确一个都不收时返回空集，不能退回去按本地列表硬扫。

    结果缓存 5 分钟：宿主那份配置快照是按需构建的，而 /retry 上千个路径、
    _enumerate_files 的每个文件都要问一次"这个扩展名收不收"；白名单不是每秒钟都在
    变的东西，改了最迟下一轮生效就够。测试把 _AUDIO_EXT_CACHE["ts"] 置 0 即可强制重算。
    """
    now = time.time()
    cached = _AUDIO_EXT_CACHE["exts"]
    if cached is not None and now - _AUDIO_EXT_CACHE["ts"] < _AUDIO_EXT_TTL:
        return cached
    host = _host_audio_extensions()
    if host is None:
        exts = _MUSIC_EXT
    else:
        host_set = set(host)
        exts = tuple(ext for ext in _MUSIC_EXT if ext in host_set)
        if not exts:
            logger.warning(
                "[music-scraper] 宿主音频白名单里没有本插件认识的格式，本轮不会处理任何文件"
                f"（宿主收：{'、'.join(host) or '（空）'}）"
            )
    _AUDIO_EXT_CACHE["exts"] = exts
    _AUDIO_EXT_CACHE["ts"] = now
    return exts


def _epoch_of(value: Any) -> Optional[float]:
    """把秒/毫秒时间戳或 ISO 日期串换算成 epoch 秒，认不出来返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        # 秒级与毫秒级都可能：1e11 秒是公元 5138 年，超过就按毫秒处理
        return float(value / 1000) if value > 1e11 else float(value)
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{10,13}", text):
        return _epoch_of(int(text))
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _jwt_exp(token: str) -> Optional[float]:
    """只解 JWT 载荷里的 exp，不验签：令牌是本机从 MTW 拿的，验签要服务端密钥、没必要。"""
    try:
        segment = str(token).split(".")[1]
        segment += "=" * (-len(segment) % 4)
        claims = json.loads(base64.urlsafe_b64decode(segment.encode()))
    except Exception:
        return None
    return _epoch_of(claims.get("exp") if isinstance(claims, dict) else None)


def _token_expiry_at(token: str, payload: Dict[str, Any]) -> float:
    """令牌什么时候过期按它自己的声明算，不再硬编 6 天。

    MTW 的 JWT 生命周期是服务端配置：换密钥、改过期设置、管理员踢人都可能让它提前失效。
    猜长了就会长期带着一个废 token 跑完整轮，每个文件都"无候选"，还一路记成刮削失败；
    取不到声明时宁可只缓存 1 小时，多登一次比整轮白跑便宜。
    """
    for key in ("expired_at", "expires_at", "expiry", "expire", "exp"):
        epoch = _epoch_of(payload.get(key)) if isinstance(payload, dict) else None
        if epoch:
            return epoch
    return _jwt_exp(token) or (time.time() + 3600)


def _mtw_failure_reason(resp: Any) -> Optional[str]:
    """判断这份 2xx 响应体是不是"没给出结论"，返回故障原因；None 表示可用（含空候选）。

    状态码那层由 _mtw_post 先看，这里只按响应体形状兜底：MTW 偶尔用 200 夹带 DRF 错误体
    （带 detail/code，JWT 失效就是 {"detail": "...", "code": "token_not_valid"}），
    正常检索结果一定带 data 数组。空候选是 {"data": []}，属于可用响应。
    """
    if resp is None:
        return _UNREACHABLE_REASON
    if not isinstance(resp, dict):
        return _UNREACHABLE_REASON
    # data 是 list（检索结果）或 dict（分页包一层）都算拿到结论；拉候选那边两种形状都会解。
    if isinstance(resp.get("data"), (list, dict)):
        return None
    if any(key in resp for key in ("detail", "error", "exception", "code")):
        return _AUTH_REASON
    return _UNREACHABLE_REASON


def _mtw_post(url: str, body: Dict[str, Any], headers: Dict[str, str],
              expect_data: bool = True) -> Tuple[Any, Optional[str]]:
    """POST 一次 MTW 接口，返回 (响应体, 故障原因)。

    不能直接用 RequestUtils.post_json：宿主那句 `if response:` 判的是 requests 的
    Response.__bool__（等价于 status_code < 400），于是 401、500 的响应连同一个错误体
    一起被折成 None。插件分不清"MTW 重启了"（该熔断、不该给文件记账）和"JWT 过期了"
    （该重登一次再重试），只能把两者一律记成不可达，强制重登那条路永远走不到。
    这里自己拿住 Response 看状态码，非 2xx 也尽量把 JSON 错误体解出来。

    expect_data=False 给 /apimt/token/ 用：那份响应本来就没有 data 字段
    （{"token": "...", ...}），按检索结果的形状去兜底判会把一次成功登录说成"不可达"。
    """
    resp = RequestUtils(headers=headers).post(url=url, json=body)
    if resp is None:
        return None, _UNREACHABLE_REASON
    try:
        status = int(getattr(resp, "status_code", 0) or 0)
        try:
            payload = resp.json()
        except Exception:
            payload = None
    finally:
        # post_json 会自己 close，改用 post 之后连接得我们自己收。
        try:
            resp.close()
        except Exception:
            pass
    if status in (401, 403):
        return payload, _AUTH_REASON
    if status >= 400:
        return payload, _API_ERROR_REASON
    if not isinstance(payload, dict):
        # 2xx 但解不出对象：多半是被网关或登录页截了，不是曲库给的答案
        return payload, _UNREACHABLE_REASON
    if not expect_data:
        return payload, None
    return payload, _mtw_failure_reason(payload)


def _first_tag_value(tags, keys) -> str:
    """从 mutagen 标签容器里按候选 key 取第一个非空文本。"""
    for key in keys:
        try:
            value = tags.get(key)
        except Exception:
            continue
        if value is None:
            continue
        # ID3 Frame 把文本放在 .text 列表里
        text = getattr(value, "text", value)
        if isinstance(text, (list, tuple)):
            text = text[0] if text else None
        if isinstance(text, bytes):
            try:
                text = text.decode("utf-8", "ignore")
            except Exception:
                continue
        if isinstance(text, str) and text.strip():
            return text.strip()
    return ""


def _has_embedded_cover(audio: Any, tags: Any) -> bool:
    """容器里是否已经内嵌封面。

    不能只按帧名 `tags.get("APIC")` 判 ID3：APIC 是可重复帧，mutagen 取这类帧的口径是
    `getall("APIC")`（键还可能带描述）；APEv2 的封面 key 是 "Cover Art (Front)"，
    根本不在按帧名罗列的那份表里。判错的代价不对称 —— 已经有封面的文件被判成没封面，
    补空模式就永远认为它"没刮齐"：每轮重打最多 7 次 MTW 请求，还因为候选不达标白涨
    retries，几轮之后一个本来完整的文件被永久列成超限。
    """
    # mutagen 的通用图片接口：文件对象（FLAC/OGG/MP4/APE…）和 tag 容器（ID3）两处都可能暴露
    for holder in (audio, tags):
        try:
            if getattr(holder, "pictures", None):
                return True
        except Exception:
            continue
    try:
        if tags.getall("APIC"):
            return True
    except Exception:
        pass
    try:
        keys = list(tags.keys())
    except Exception:
        return False
    for key in keys:
        name = str(key).lower()
        if not (name.startswith(_COVER_KEY_PREFIXES) or name in _COVER_KEYS):
            continue
        try:
            if tags.get(key):
                return True
        except Exception:
            continue
    return False


def _read_music_tags(path: Path) -> Dict[str, Any]:
    """读取本地音乐 tag。

    MP 的 MetaInfoPath 对音频返回 MetaVideo（没有 artist/album），不能用于音乐判定，
    因此这里直接用 mutagen。失败时返回空值，由调用方按“缺 tag”处理。
    """
    out: Dict[str, Any] = {
        "title": "", "artist": "", "album": "", "duration": None, "has_cover": False,
    }
    try:
        from mutagen import File as MutagenFile

        audio = MutagenFile(str(path))
    except Exception:
        return out
    if audio is None:
        return out
    tags = getattr(audio, "tags", None)
    if tags is not None:
        for field, keys in _TAG_KEYS.items():
            out[field] = _first_tag_value(tags, keys)
            if not out[field] and field in ("title", "artist"):
                # Vorbis/APE 的 key 大小写不敏感，mutagen 已归一，这里兜底遍历一次
                try:
                    for key in list(tags.keys()):
                        if key.lower() == field:
                            out[field] = _first_tag_value(tags, (key,))
                            if out[field]:
                                break
                except Exception:
                    pass
        out["has_cover"] = _has_embedded_cover(audio, tags)
    try:
        info = getattr(audio, "info", None)
        length = getattr(info, "length", None) if info else None
        if length:
            out["duration"] = int(length)
    except Exception:
        pass
    return out


def _path_identity(path: Path, roots: Sequence[Path] = ()) -> Dict[str, str]:
    """按 “艺人/专辑 (年)/NN - 曲名” 目录结构推断身份，供未打 tag 的文件参与匹配。

    roots 给定时只认严格落在库根之下的目录：库根自己以及它上面的层级不是专辑/艺人目录。
    不 clamp 的话 /media/music/01 - 轨迹.flac 会得到 album="music"、
    /media/music/周杰伦/01 - x.flac 会得到 artist="media" —— 这种凭空造出来的名字
    既会被当搜索词发给 MTW，比对又是双向包含（want in got 也算命中），一个目录名
    就可能白送一个匹配分；把 min_score 放宽到 1-2 时，够格的文件会被按错艺人改标签，
    而写进文件的标签没法回滚。宁可留空判「未达阈值」，也不要假身份。
    """
    parents = list(path.parents)

    def _name(index: int) -> str:
        if len(parents) <= index:
            return ""
        dirpath = parents[index]
        if roots and not any(
            dirpath != root and dirpath.is_relative_to(root) for root in roots
        ):
            return ""
        return dirpath.name

    return {
        "title": _TRACK_PREFIX_RE.sub("", path.stem).strip(),
        "artist": _name(1).strip(),
        "album": _YEAR_SUFFIX_RE.sub("", _name(0)).strip(),
    }


def _local_identity(path: Path, tags: Optional[Dict[str, Any]] = None,
                    roots: Sequence[Path] = ()) -> Dict[str, Any]:
    """本地身份：tag 优先，缺失字段用目录结构兜底。

    MTW 的 fetch_id3_by_title 是把 title 当搜索关键词用的，传原始文件名
    （"01 - 轨迹.flac"）只会召回一堆噪音，必须先清洗成干净曲名。

    tags 由调用方传入时不再重复解析文件：mutagen 读一次不便宜，一轮里补空判定、
    拉候选、挑选三处都要用同一份结果。
    """
    if tags is None:
        tags = _read_music_tags(path)
    from_path = _path_identity(path, roots)
    return {
        "title": (tags["title"] or from_path["title"]).strip(),
        "artist": (tags["artist"] or from_path["artist"]).strip(),
        "album": (tags["album"] or from_path["album"]).strip(),
        "duration": tags["duration"],
    }


def _want_from(identity: Dict[str, Any]) -> Dict[str, Any]:
    """把本地身份收敛成匹配用的期望值，拉候选和挑最佳必须用同一份口径。"""
    return {
        "title": identity["title"],
        "artist": identity["artist"],
        "album": identity["album"],
        "duration": identity.get("duration"),
    }


class _MusicScrapeChain(ScrapingChain):
    """复用 libraryscraper 的 trickplay 排除策略，避免 Jellyfin 预览目录被递归。"""

    def scrape_metadata(self, fileitem, *args, **kwargs):
        if fileitem and MusicLibraryScraper._is_trickplay_path(Path(fileitem.path)):
            return False, "跳过 Jellyfin 预览缓存目录"
        return super().scrape_metadata(fileitem, *args, **kwargs)


class MusicLibraryScraper(_PluginBase):
    """扫描音乐库，向 Music Tag Web 拉候选，交由 MP ScrapingChain 完成写入。

    目录名 / 类名 / 配置前缀沿用 v0.2.x：MP 的 config_data 以 plugin_config_prefix 为键、
    plugindata 以类名为键，改名会让已配置的 MTW 地址账密和已积累的刮削状态账一起失效。
    """

    plugin_name = "MusicTagWeb音乐库刮削"
    plugin_desc = (
        "定时扫描音乐目录，元数据统一取自 Music Tag Web（MTW）的多源识别"
        "（网易云 / QQ / 酷我 / 酷狗 / 咪咕 / MusicBrainz / smart_tag），"
        "再沿用 MP 原生音乐刮削链路写入标签与封面。"
    )
    plugin_icon = "https://raw.githubusercontent.com/JontyLee/MoviePilot-Plugins/main/icons/avator.webp"
    plugin_version = "0.3.8"
    plugin_author = "JontyLee"
    author_url = "https://github.com/JontyLee"
    plugin_config_prefix = "musiclibraryscraper_"
    plugin_order = 8
    user_level = 1

    # 配置项
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: Optional[str] = None
    _scraper_paths: str = ""
    _exclude_paths: str = ""
    _force_all: bool = False
    _mtw_base_url: str = ""
    _mtw_username: str = ""
    _mtw_password: str = ""
    _mtw_resources: List[str] = list(_DEFAULT_RESOURCES)
    _min_score: int = 3

    # MTW 令牌缓存
    _token: Optional[str] = None
    _token_expiry: float = 0.0

    # 失败自动重试上限：retries 达到该值后，定时扫描不再自动重试（仅手动重试）。
    MAX_RETRY: int = 5

    def init_plugin(self, config: Dict[str, Any] = None):
        # 每实例独立的扫描锁、运行标志与停止信号（都放 __dict__，避免类属性被多实例共享）。
        # 运行标志用 setdefault 而不是直接置 False：保存配置会重跑 init_plugin，
        # 那时把在跑的扫描标记成"没在跑"，就等于又开了一条并发写账本的口子。
        self.__dict__.setdefault("_scan_lock", threading.Lock())
        self.__dict__.setdefault("_scan_running", False)
        self.__dict__.setdefault("_stop_flag", threading.Event())
        self.__dict__.setdefault("_pending_cache", None)

        if config:
            previous_mtw = (self._mtw_base_url, self._mtw_username, self._mtw_password)
            self._enabled = bool(config.get("enabled"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._cron = config.get("cron") or None
            self._scraper_paths = config.get("scraper_paths") or ""
            self._exclude_paths = config.get("exclude_paths") or ""
            self._force_all = config.get("mode") == "force_all"
            self._mtw_base_url = (config.get("mtw_base_url") or "").rstrip("/")
            self._mtw_username = config.get("mtw_username") or ""
            self._mtw_password = config.get("mtw_password") or ""
            # warn=True：只在配置入口告警一次，不要每个文件拉候选时都刷一遍日志
            self._mtw_resources = _normalize_resources(config.get("mtw_resource"), warn=True)
            self._min_score = self._clamp_min_score(config.get("min_score"))
            if (self._mtw_base_url, self._mtw_username, self._mtw_password) != previous_mtw:
                # 换地址/换账号后手里那个令牌是上一个实例签发的：
                # 继续带它请求既会白吃一次 401，也算把凭据发到新的地址上。
                self._mtw_drop_token()
                # 人刚改过账密，登录静默期里那条新凭据永远试不到：一并作废。
                self.__dict__["_login_retry_at"] = 0.0

        # 勾选“立即运行一次”：回写配置关闭该开关，并投递一次性扫描。
        if self._onlyonce:
            self._onlyonce = False
            # update_config 是整体替换而不是合并（宿主 PluginInstanceOper.save_config_data
            # 直接 record.config_data = deepcopy(config_data)），所以必须把全量配置写回去，
            # 只写 {"onlyonce": False} 会把其余配置一起清掉。密码本来就在这份配置里，
            # 明文落 systemconfig 是 MP 存储插件配置的方式，插件侧无法在不丢配置的前提下回避。
            self.update_config({
                "enabled": self._enabled,
                "onlyonce": False,
                "cron": self._cron,
                "scraper_paths": self._scraper_paths,
                "exclude_paths": self._exclude_paths,
                "mode": "force_all" if self._force_all else "",
                "mtw_base_url": self._mtw_base_url,
                "mtw_username": self._mtw_username,
                "mtw_password": self._mtw_password,
                # 配置键保持单数，值自 v0.3.0 起是数组（读侧兼容旧的单字符串）
                "mtw_resource": self._mtw_resources,
                "min_score": self._min_score,
            })
            self._kick_scan()

    @staticmethod
    def _clamp_min_score(raw: Any) -> int:
        """最低匹配分数只接受 1.._MAX_SCORE，越界按最近的那一界执行并告警。

        配置页的 min/max 只是浏览器属性，原生表单和 /api 都能塞进任意值，而两头都不可逆：
        0 或负数会让"只对上曲名"的候选整库改写标签（写进文件就回不来）；
        大于满分则永远达不到阈值，整库都会被记成失败、烧满 retries 后永久跳过。
        0 与 1 行为本就等价（一分不中的候选根本选不出来），所以降到 1 不改变意图。
        """
        text = "" if raw is None else str(raw).strip()
        if not text:
            return _MAX_SCORE
        try:
            value = int(float(text))
        except (TypeError, ValueError):
            logger.warning(f"[music-scraper] min_score {raw!r} 不是数字，按默认 {_MAX_SCORE} 执行")
            return _MAX_SCORE
        if 1 <= value <= _MAX_SCORE:
            return value
        clamped = min(max(value, 1), _MAX_SCORE)
        logger.warning(
            f"[music-scraper] min_score {value} 超出 1-{_MAX_SCORE}，按 {clamped} 执行"
            f"（{_MAX_SCORE} = 曲名/艺人/专辑 全中）"
        )
        return clamped

    def get_state(self) -> bool:
        return self._enabled

    # ---------------- 命令 ----------------

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程命令：查询刮削状态 / 重试失败项。"""
        return [
            {
                "cmd": "/music_scraper_status",
                "event": EventType.PluginAction,
                "desc": "MTW 音乐库刮削：查看运行状态与统计",
                "category": "音乐刮削",
                "data": {"action": "music_scraper_status"},
            },
            {
                "cmd": "/music_scraper_retry",
                "event": EventType.PluginAction,
                "desc": "MTW 音乐库刮削：重试全部失败项",
                "category": "音乐刮削",
                "data": {"action": "music_scraper_retry"},
            },
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None):
        """处理属于本插件的 PluginAction 事件。"""
        event_data = (event.event_data if event else None) or {}
        action = event_data.get("action")
        if action == "music_scraper_status":
            self._cmd_status()
        elif action == "music_scraper_retry":
            self._cmd_retry()

    def _reason_lines(self, stats: Dict[str, Any]) -> str:
        """失败原因分布的文本行（命令输出用）：最多列 3 类，其余并成一条计数。

        没有这段聚合的话，"阈值降一档能救回多少"只能靠一页页翻表格估。
        """
        groups = stats.get("failure_reasons") or []
        if not groups:
            return ""
        lines = [f"失败原因（最低匹配分数 {stats['config']['min_score']}）："]
        min_score = stats['config']['min_score']
        for item in groups[:3]:
            score = item.get("best_score")
            line = f"· {item['reason']} {item['count']} 个"
            if isinstance(score, int):
                line += f"，最高 {score} 分"
                if min_score - score > 0:
                    line += f"，阈值降 {min_score - score} 分可命中"
            if not item["retryable"]:
                line += "，均已超限不再自动重试"
            lines.append(line)
        rest = groups[3:]
        if rest:
            lines.append(f"· 其他 {len(rest)} 类共 {sum(g['count'] for g in rest)} 个")
        return "\n".join(lines) + "\n"

    def _cmd_status(self):
        """命令：打印最近一次运行汇总与分桶计数。"""
        stats = self._build_status()
        self.post_message(
            title=f"{self.plugin_name} 状态",
            text=(
                f"最近扫描：{stats['runs'].get('ts') or '从未运行'}\n"
                f"发现 {stats['runs'].get('scanned', 0)}｜命中缓存跳过 "
                f"{stats['runs'].get('from_cache', 0)}｜已有tag跳过 "
                f"{stats['runs'].get('skipped_tagged', 0)}\n"
                f"成功 {stats['runs'].get('ok', 0)}｜失败 "
                f"{stats['runs'].get('failed', 0)}｜MTW 故障 "
                f"{stats['runs'].get('transport', 0)}｜处理异常 "
                f"{stats['runs'].get('error', 0)}\n"
                f"已处理 {stats['counts']['done']}｜待重试 "
                f"{stats['counts']['retryable']}｜超限 "
                f"{stats['counts']['exhausted']}｜待处理 "
                f"{stats['counts']['pending_unknown']}\n"
                + self._reason_lines(stats)
                + f"定时 {stats['config']['cron']}｜下次 {stats['config']['cron_next']}"
                + (
                    f"\n最近定向重试：{stats['last_targeted'].get('ts')}"
                    f"，成功 {stats['last_targeted'].get('ok', 0)}"
                    f"｜失败 {stats['last_targeted'].get('failed', 0)}"
                    if stats.get("last_targeted", {}).get("ts") else ""
                )
            ),
        )

    def _filter_retry_paths(self, paths: List[str]) -> Tuple[List[str], int]:
        """把外部给来的重试路径收敛到库内音频文件，返回 (可用路径, 被拒数量)。

        命令入口和 /retry 必须走同一套判据：否则 /music_scraper_retry 会照着账本里
        已经不存在、或已经挪出库的路径白跑一轮（stat 失败直接跳过，账本原样留着，
        看起来像"重试过了但还是失败"）。
        """
        accepted = [str(p) for p in paths if self._is_library_path(str(p))]
        return accepted, len(paths) - len(accepted)

    def _cmd_retry(self):
        """命令：投递一次“重试全部失败项”的扫描。"""
        paths = list(self._load_failed().keys())
        if not paths:
            self.post_message(title=f"{self.plugin_name} 重试", text="没有失败项可重试。")
            return
        accepted, rejected = self._filter_retry_paths(paths)
        if not accepted:
            self.post_message(
                title=f"{self.plugin_name} 重试",
                text=f"{len(paths)} 个失败项都已不在库内（文件被删或移走），账本留到下一轮清理。",
            )
            return
        started = self._kick_scan(force=True, paths=accepted)
        tail = f"（{rejected} 个已不在库内，跳过）" if rejected else ""
        self.post_message(
            title=f"{self.plugin_name} 重试",
            text=(f"已提交 {len(accepted)} 个失败项的重试{tail}。"
                  if started else "已有扫描在运行，请稍后再试。"),
        )

    # ---------------- API ----------------

    def get_api(self) -> List[Dict[str, Any]]:
        """插件对外接口，供 Vue 数据页与 Agent 调用（bear 认证）。"""
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "扫描状态与统计汇总",
            },
            {
                "path": "/files",
                "endpoint": self.api_files,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "按桶分页查询文件（done/failed/pending）",
            },
            {
                "path": "/retry",
                "endpoint": self.api_retry,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "重试指定路径或全部失败项",
            },
            {
                "path": "/scan",
                "endpoint": self.api_scan,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "触发一次扫描",
            },
        ]

    def api_status(self, request: Request) -> Dict[str, Any]:
        """GET /status —— 返回汇总、计数、配置回显（不含密码）与最近失败。"""
        return self._build_status()

    def api_files(self, request: Request) -> Dict[str, Any]:
        """GET /files —— bucket=done|failed|pending&limit&offset&search 分页查询。"""
        qp = request.query_params
        try:
            limit = max(1, min(int(qp.get("limit") or 50), 500))
        except (TypeError, ValueError):
            limit = 50
        try:
            offset = max(0, int(qp.get("offset") or 0))
        except (TypeError, ValueError):
            offset = 0
        bucket = (qp.get("bucket") or "done").lower()
        if bucket not in ("done", "failed", "pending"):
            # 不认识的桶名不能悄悄落到 done 那一档：回显的还是请求的那个名字，
            # 调用方拿到一批"已处理"的行却以为是自己传的桶，看着像筛选坏了。
            return {
                "bucket": bucket, "total": 0, "offset": offset,
                "limit": limit, "items": [], "reason": "unknown_bucket",
            }
        search = (qp.get("search") or "").strip().lower()

        rows = self._collect_rows(bucket)
        if search:
            rows = [r for r in rows if search in r["path"].lower() or search in (r["name"] or "").lower()]
        rows.sort(key=lambda r: r["path"])
        return {
            "bucket": bucket,
            "total": len(rows),
            "offset": offset,
            "limit": limit,
            "items": rows[offset:offset + limit],
        }

    async def api_retry(self, request: Request) -> Dict[str, Any]:
        """POST /retry —— body {paths:[...]} 或 {bucket:"failed", all:true}。"""
        try:
            body = await request.json()
        except Exception:
            body = {}
        paths = body.get("paths") if isinstance(body, dict) else None
        all_failed = bool(isinstance(body, dict) and body.get("all"))
        if (not paths) and all_failed:
            paths = list(self._load_failed().keys())
        if not paths:
            return {"accepted": False, "reason": "no_paths", "count": 0}
        # 只接受库内的音频文件：调用方传什么就刮什么是把插件当成了任意文件写接口，
        # 换个 token 就能改到音乐库之外的文件。
        accepted, rejected = self._filter_retry_paths(paths)
        if rejected:
            logger.warning(f"[music-scraper] /retry 拒绝 {rejected} 个库外或非音频路径")
        if not accepted:
            return {"accepted": False, "reason": "out_of_library", "count": 0}
        started = self._kick_scan(force=True, paths=accepted)
        return {
            "accepted": started,
            "count": len(accepted),
            "rejected": rejected,
            "running": self._is_running(),
            "reason": None if started else "busy",
        }

    async def api_scan(self, request: Request) -> Dict[str, Any]:
        """POST /scan —— body {force?:bool}，触发一次（增量或全量）扫描。"""
        try:
            body = await request.json()
        except Exception:
            body = {}
        force = bool(isinstance(body, dict) and body.get("force")) or self._force_all
        # 界面上的"全量覆盖"既要绕过增量账、也要真的覆盖写：只给 force 的话
        # 标签还是会按"只补缺"写，按钮名字和实际行为对不上。
        started = self._kick_scan(force=force, overwrite=force)
        return {"accepted": started, "running": self._is_running(),
                "reason": None if started else "busy"}

    # ---------------- 页面 / 渲染 ----------------

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """声明 Vue 联邦渲染，产物目录 dist/assets。"""
        return "vue", "dist/assets"

    def get_page(self) -> List[dict]:
        """Vue 模式下详情页由远程 ./Page 渲染，这里返回空。"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """周期扫描服务，由宿主调度器托管（插件自建 scheduler 已移除）。"""
        if not self._enabled:
            return []
        return [
            {
                "id": "MusicLibraryScraper.Scan",
                "name": "MTW 音乐库刮削",
                "trigger": self._build_cron_trigger(self._cron),
                # 必须走带互斥的入口：直接给 __scan 会绕开运行标志，cron 与手动触发
                # 就能并发跑，两本账各自读到结尾再写回，后写的把先写的整本盖掉。
                "func": self._run_scan_guarded,
                "kwargs": {},
            }
        ]

    def stop_service(self):
        """插件停用：投递停止信号并取消宿主一次性任务（可重复调用）。"""
        # 只置位、不在这里 clear：clear 会把刚投递的停止信号吞掉，正在跑的扫描
        # 永远观察不到，等于停不下来。标志改由下一轮扫描开始时复位。
        self._stop_event().set()
        try:
            remove = getattr(scheduler_sdk, "remove_plugin_once_job", None) if scheduler_sdk else None
            if remove:
                remove(self.__class__.__name__, "scan_once")
        except Exception as err:  # pragma: no cover
            logger.debug(f"[music-scraper] 取消一次性任务异常: {err}")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "mode", "label": "写入模式",
                                "items": [
                                    {"title": "只补缺（推荐）", "value": ""},
                                    {"title": "强制覆盖已有标签", "value": "force_all"},
                                ]
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VCronField", "props": {
                                "model": "cron", "label": "执行周期", "placeholder":
                                "5 位 cron；星期 0=周一…6=周日、7=周日，留空默认 0 3 * * sun"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "mtw_base_url",
                                "label": "Music Tag Web 地址",
                                "placeholder": "http://192.168.x.x:8001"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "min_score", "label": "最低匹配分数（1-3）", "type": "number",
                                "min": 1, "max": 3, "placeholder": "默认 3"
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "mtw_resource", "label": "MTW 识别源（可多选，留空=全部）",
                                "multiple": True, "chips": True, "closable": True,
                                "items": [
                                    {"title": "网易云 netease", "value": "netease"},
                                    {"title": "QQ 音乐 qmusic", "value": "qmusic"},
                                    {"title": "酷我 kuwo", "value": "kuwo"},
                                    {"title": "酷狗 kugou", "value": "kugou"},
                                    {"title": "咪咕 migu", "value": "migu"},
                                    {"title": "MusicBrainz", "value": "musicbrainz"},
                                    {"title": "智能 smart_tag", "value": "smart_tag"},
                                ]
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "mtw_username", "label": "MTW 用户名", "type": "text"
                            }}
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "mtw_password", "label": "MTW 密码",
                                "type": "password", "placeholder": "仅本机保存，不入 git"
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VTextarea", "props": {
                                "model": "scraper_paths", "rows": 5,
                                "label": "音乐库路径（一行一个目录）",
                                "placeholder": "/media/music/周杰伦\n/media/music"
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VTextarea", "props": {
                                "model": "exclude_paths", "rows": 2,
                                "label": "排除路径",
                            }}
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VAlert", "props": {
                                "type": "info", "variant": "tonal",
                                "text": _FORM_NOTES
                            }}
                        ]},
                    ]},
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "mode": "",
            "cron": _DEFAULT_CRON,
            "scraper_paths": "",
            "exclude_paths": "",
            "mtw_base_url": "",
            "mtw_username": "",
            "mtw_password": "",
            # 留空 = 全部源（读侧 _normalize_resources 保证），不要把 7 个源预填进去：
            # 预填会让两个配置页长得不一样，也和说明文案对不上。
            "mtw_resource": [],
            "min_score": 3,
        }

    # ---------------- 状态账 ----------------

    def _load_book(self, key: str) -> Dict[str, dict]:
        """读一本账：外层和条目都必须是 dict，retries 必须是整数。

        plugindata 可能来自旧版本、手工编辑或写到一半的 JSON。条目不是 dict 时，
        后面每一处 prev.get() 都会抛 AttributeError，从 /status 到整轮扫描一起崩；
        归一成空 dict 只表示"这条记录什么都不知道"，那个文件会被重新刮一次。

        retries 也要归一：它是 `int(prev.get("retries", 0))` 直接吃的那个值，写成非数字
        （null、"三次"、旧版本的 dict）都会抛 TypeError，而抛在逐文件循环里等于整轮中止 ——
        崩的位置由账本里的键序决定，下一轮枚举走到同一个键还是同一个位置，于是那之后的
        文件永远处理不到、汇总也永远不落账。
        """
        data = self.get_data(key)
        if not isinstance(data, dict):
            return {}
        out: Dict[str, dict] = {}
        for k, v in data.items():
            if not isinstance(v, dict):
                out[k] = {}
                continue
            # 只在坏值上动手：done 条目本来没有 retries，不该为此多写一个键
            if "retries" in v and not isinstance(v["retries"], int):
                try:
                    v["retries"] = int(v["retries"] or 0)
                except (TypeError, ValueError):
                    v["retries"] = 0
            out[k] = v
        return out

    def _load_done(self) -> Dict[str, dict]:
        return self._load_book(_KEY_DONE)

    def _load_failed(self) -> Dict[str, dict]:
        return self._load_book(_KEY_FAILED)

    def _load_runs(self, key: str = _KEY_RUNS) -> dict:
        data = self.get_data(key)
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------------- 主体流程 ----------------

    @staticmethod
    def _normalize_dow(raw_cron: str) -> str:
        """把 crontab 习惯里表示周日的星期 7 换成 sun。

        APScheduler 的星期上限是 6，直接写 7 会让整条表达式校验失败；用户写 7 想说的
        就是周日，所以只换这一个值、保留其余字段，而不是把整条表达式扔掉回落默认。
        0-6 原样交给调度器（它按 0=周一 解析），不做任何"猜意图"的位移。
        """
        parts = raw_cron.split()
        if len(parts) == 5 and parts[4] == "7":
            parts[4] = "sun"
            return " ".join(parts)
        return raw_cron

    @staticmethod
    def _next_fire_text(trigger: Any) -> str:
        """触发器的下次触发时间文本；拿不到就说"未知"，不拿它当校验用。"""
        try:
            nxt = trigger.get_next_fire_time(None, datetime.now())
        except Exception:
            return "未知"
        return nxt.strftime("%Y-%m-%d %H:%M (%a)") if nxt else "未知"

    def _cron_text(self) -> Tuple[str, str]:
        """(生效的 cron, 下次触发时间)。界面要看得见日子实际落在周几。"""
        raw = self._normalize_dow((self._cron or "").strip() or _DEFAULT_CRON)
        try:
            trigger = CronTrigger.from_crontab(raw)
        except (ValueError, KeyError) as err:
            return _DEFAULT_CRON, f"未生效（{err}，已回落默认）"
        return raw, self._next_fire_text(trigger)

    def _build_cron_trigger(self, raw_cron: Optional[str]) -> CronTrigger:
        """非法/空 cron 回落默认，避免插件整体 load_failed。"""
        raw = self._normalize_dow((raw_cron or "").strip() or _DEFAULT_CRON)
        try:
            trigger = CronTrigger.from_crontab(raw)
        except (ValueError, KeyError) as err:
            logger.warning(f"[music-scraper] cron 表达式非法：{raw!r} → {err}，回落默认 {_DEFAULT_CRON}")
            raw = _DEFAULT_CRON
            trigger = CronTrigger.from_crontab(raw)
        # 把解析结果写进日志：星期编号按调度器语义（0=周一…6=周日）而不是 crontab
        # （0/7=周日），只回显表达式的话，"每周日"实际跑在周一这种事在日志里看不出来。
        logger.info(f"[music-scraper] 定时扫描 cron={raw} → 下次 {self._next_fire_text(trigger)}")
        return trigger

    def _is_running(self) -> bool:
        return bool(self.__dict__.get("_scan_running"))

    def _stop_event(self) -> threading.Event:
        """每实例独立的扫描停止信号（init_plugin 之前也可用）。"""
        return self.__dict__.setdefault("_stop_flag", threading.Event())

    def _begin_scan(self) -> bool:
        """占用扫描槽位；已有扫描在跑时返回 False。顺带复位上一轮的停止信号。"""
        lock = self.__dict__.setdefault("_scan_lock", threading.Lock())
        with lock:
            if self.__dict__.get("_scan_running"):
                return False
            # 复位停止信号必须在锁内、占位之前：放在锁外时，恰好挤在这个窗口里到达的
            # stop_service 会被抹掉，而它想停的正是紧接着要开始的这一轮 —— 界面按了
            # "停止"、日志里却一句"收到停止信号"都没有，只能等整轮跑完。
            self._stop_event().clear()
            self.__dict__["_scan_running"] = True
        return True

    def _end_scan(self):
        lock = self.__dict__.setdefault("_scan_lock", threading.Lock())
        with lock:
            self.__dict__["_scan_running"] = False

    def _execute_scan(self, force: bool = False,
                      target_paths: Optional[List[str]] = None,
                      overwrite: bool = False) -> None:
        """执行一轮扫描并释放槽位；调用方必须已经 _begin_scan() 占到槽。"""
        try:
            self.__scan(force=force, target_paths=target_paths, overwrite=overwrite)
        except Exception as err:  # pragma: no cover
            logger.error(f"[music-scraper] 扫描异常: {err}")
        finally:
            self._end_scan()

    def _run_scan_guarded(self, force: bool = False,
                          target_paths: Optional[List[str]] = None,
                          overwrite: bool = False) -> bool:
        """宿主调度器用的同步入口：抢不到槽位就跳过本轮，不与已 in-flight 的扫描并发。"""
        if not self._begin_scan():
            logger.info("[music-scraper] 已有扫描在运行，本次触发跳过")
            return False
        self._execute_scan(force=force, target_paths=target_paths, overwrite=overwrite)
        return True

    def _kick_scan(self, force: bool = False, paths: Optional[List[str]] = None,
                   overwrite: bool = False) -> bool:
        """投递一次扫描：无参优先交宿主一次性任务，带参/强制走后台线程。"""
        if not force and not paths:
            if self.__dict__.get("_scan_running"):
                # 一次性任务的回调 3 秒后才会跑，那时候抢不到槽位就静默跳过。
                # 这里不先挡一下，/scan 与"立即运行一次"会对着界面报"已提交"，
                # 实际一轮都没跑，用户只能等下一句"已有扫描在运行"出现在日志里。
                logger.info("[music-scraper] 已有扫描在运行，本次触发跳过")
                return False
            add = getattr(scheduler_sdk, "add_plugin_once_job", None) if scheduler_sdk else None
            if add:
                try:
                    # 宿主只在调度器运行且注册成功时返回 True；返回 False 时任务其实没排上，
                    # 不回落后台线程就会对着界面报"已提交"、实际什么都没跑。
                    if add(
                        self.__class__.__name__,
                        "scan_once",
                        self._run_scan_guarded,
                        "MTW 音乐库刮削（一次性）",
                        delay_seconds=3,
                    ):
                        return True
                    logger.debug("[music-scraper] 一次性任务未注册（调度器没在跑），回落后台线程")
                except Exception as err:  # pragma: no cover
                    logger.debug(f"[music-scraper] 一次性任务投递失败，回落后台线程: {err}")
        return self._start_scan_async(force=force, paths=paths, overwrite=overwrite)

    def _start_scan_async(self, force: bool = False, paths: Optional[List[str]] = None,
                          overwrite: bool = False) -> bool:
        """在后台线程执行扫描；若已有扫描在跑则拒绝，避免并发重复处理。"""
        if not self._begin_scan():
            return False
        threading.Thread(
            target=self._execute_scan,
            kwargs={"force": force, "target_paths": paths, "overwrite": overwrite},
            name="music-scraper-scan",
            daemon=True,
        ).start()
        return True

    def _is_library_path(self, path: str) -> bool:
        """外部传入的重试路径是否真的是库内的音频文件。

        与 _enumerate_files 的软链收敛用同一份 realpath 基准：两边口径不一致时，
        /retry 拒收的路径扫描侧却照样写，界面看起来就是"筛选没生效"。
        """
        try:
            target = Path(path).resolve()
        except (OSError, RuntimeError):
            return False
        roots_real, excludes_real = self._library_bounds()
        if not self._target_in_library(target, roots_real, excludes_real):
            return False
        return (target.suffix.lower() in _supported_audio_ext() and target.is_file())

    def _enumerate_files(self) -> List[Path]:
        """按配置枚举音频文件（宿主白名单、扩展名、trickplay、排除路径过滤）。

        用 os.walk 而不是 Path.rglob：rglob 的 ** 会递归进软链目录，库内一个回环软链
        就能让枚举永远不返回，而扫描槽位已经被互斥占住 —— 之后每一轮 cron 与手动触发
        都只会记"已有扫描在运行"。顺带在目录级剪掉 .trickplay，不用再对里面每个
        缩略图文件做一次 stat。

        followlinks=False 只挡住了软链**目录**，软链**文件**照样出现在 filenames 里，
        而宿主写标签是按我们递给它的路径写的：库内一个指向 /etc 或库外曲库的 .flac 软链
        会被写穿出去。/retry 用 realpath 判定拒收同一批路径，扫描侧必须用同一套口径
        （两边都拿 realpath 比 _library_bounds 那两份基准）。
        """
        allowed_ext = set(_supported_audio_ext())
        excludes = [Path(e) for e in (self._exclude_paths or "").split("\n") if e.strip()]
        roots_real, excludes_real = self._library_bounds()
        files: List[Path] = []
        links_out = 0
        for root in (p for p in (self._scraper_paths or "").split("\n") if p.strip()):
            if self._stop_event().is_set():
                # 内层 break 只退出了当前这个库的 os.walk；不在这儿也挡一次，
                # 停止之后剩下的库照样被遍历，只是最后被整轮当成"扫过的文件"记账。
                logger.info("[music-scraper] 收到停止信号")
                break
            root_path = Path(root)
            if not root_path.exists():
                logger.warning(f"[music-scraper] 路径不存在: {root}")
                continue
            for dirpath, dirnames, filenames in os.walk(root_path, followlinks=False):
                if self._stop_event().is_set():
                    logger.info("[music-scraper] 收到停止信号")
                    break
                current = Path(dirpath)
                # 排除目录可能配的是同一目录的别名或软链形式：整棵目录按 realpath 判一次，
                # 一个目录一次系统调用，比逐文件判定便宜、也不会把别名形式的排除漏掉。
                try:
                    current_real = current.resolve()
                except (OSError, RuntimeError):
                    current_real = current
                if any(current_real == e or current_real.is_relative_to(e)
                       for e in excludes_real):
                    dirnames[:] = []
                    continue
                # Jellyfin 预览缓存目录整棵剪掉：里面全是 .jpg/.mp4，逐个 stat 纯属浪费
                dirnames[:] = [d for d in dirnames if not d.lower().endswith(".trickplay")]
                for name in filenames:
                    if Path(name).suffix.lower() not in allowed_ext:
                        continue
                    fp = current / name
                    if not fp.is_file():
                        continue
                    if self._is_trickplay_path(fp):
                        continue
                    if any(fp.is_relative_to(e) for e in excludes):
                        continue
                    if fp.is_symlink():
                        # 只对软链做 realpath：库里九成九不是软链，没必要为每个文件多一次系统调用
                        try:
                            real = fp.resolve()
                        except (OSError, RuntimeError):
                            continue
                        if not self._target_in_library(real, roots_real, excludes_real):
                            links_out += 1
                            continue
                    files.append(fp)
        if links_out:
            # 不写日志用户只会看到"发现 N 个文件"比目录里的少，且查不出为什么
            logger.warning(
                f"[music-scraper] 已跳过 {links_out} 个指向库外的音频软链"
                "（写标签会落到 scraper_paths 之外；确有需要请把目标目录也加进库路径）"
            )
        return files

    def _identity_roots(self) -> List[Path]:
        """配置里写的库根（不做 realpath）：给目录兜底的身份当边界用。

        不 realpath 是有意的 —— 待判定的路径都是 os.walk 从这份配置拼出来的，字符串
        形式一致才比得对；库根本身是软链时，realpath 反而会让每一个文件都判成"在库根之外"。
        按配置原文缓存，改配置立刻换 key，不需要 TTL。
        """
        raw = self._scraper_paths or ""
        cached = self.__dict__.get("_roots_cache")
        if cached and cached[0] == raw:
            return cached[1]
        roots = [Path(item).expanduser() for item in
                 (p for p in raw.split("\n") if p.strip())]
        self.__dict__["_roots_cache"] = (raw, roots)
        return roots

    def _library_bounds(self) -> Tuple[List[Path], List[Path]]:
        """配置库路径与排除路径的 realpath 列表：软链收敛判定和 /retry 库内判定共用同一份基准。

        两侧都要 realpath：scraper_paths 本身可能是软链，客户端传的路径也可能带 ../，
        只做字符串前缀比较两种都能绕过。
        """
        roots: List[Path] = []
        for raw in (p for p in (self._scraper_paths or "").split("\n") if p.strip()):
            try:
                roots.append(Path(raw).resolve())
            except (OSError, RuntimeError):
                continue
        excludes: List[Path] = []
        for raw in (p for p in (self._exclude_paths or "").split("\n") if p.strip()):
            try:
                excludes.append(Path(raw).resolve())
            except (OSError, RuntimeError):
                continue
        return roots, excludes

    @staticmethod
    def _target_in_library(target: Path, roots_real: List[Path],
                           excludes_real: List[Path]) -> bool:
        """已经 resolve 过的路径是否落在库内且不在排除目录里。"""
        if not roots_real:
            return False
        if not any(target == r or target.is_relative_to(r) for r in roots_real):
            return False
        return not any(target == e or target.is_relative_to(e) for e in excludes_real)

    def __scan(self, force: bool = False, target_paths: Optional[List[str]] = None,
               overwrite: bool = False):
        """增量扫描主流程。

        force：忽略 done 缓存并强制重打。
        overwrite：是否覆盖已有标签（"强制覆盖"模式或界面上的"全量覆盖"）。
        target_paths：仅处理这批路径（供手动重试），此时跳过缓存与重试上限。
        """
        if not self._scraper_paths or not self._mtw_base_url:
            logger.warning("[music-scraper] 未配置 scraper_paths 或 mtw_base_url，跳过")
            return

        started = time.time()
        # 账本要变了，pending 桶的缓存立刻作废（它靠 done/failed 做差集）
        self.__dict__["_pending_cache"] = None
        done = self._load_done()
        failed = self._load_failed()
        targeted = bool(target_paths)

        if targeted:
            files = [Path(p) for p in target_paths if p]
        else:
            files = self._enumerate_files()

        scanned = from_cache = skipped_tagged = ok = failed_n = exhausted = 0
        transport = errors = aborted = 0
        now_iso = self._now_iso()
        effective_force = force or (self._force_all and not targeted)
        # 覆盖写只由"强制覆盖"模式或界面上的"全量覆盖"决定：定向重试的 force 只是要绕过
        # 增量账和重试上限，不该顺带把手工改过的标签替换掉。
        effective_overwrite = bool(overwrite) or self._force_all
        logger.info(
            f"[music-scraper] 开始处理 {len(files)} 个文件"
            f"（mode={'覆盖' if effective_overwrite else '补空'}"
            f"{'，定向重试' if targeted else '，增量'}）"
        )
        live_keys = set()  # 整盘扫描时实际存在（stat 成功）的文件，供 prune 用
        dirty = False  # 自上次落账以来两本账有没有变更
        last_flush = time.time()
        # 连续"插件没学到任何东西"的文件数：MTW 故障或单文件异常都算
        transport_streak = 0
        # 定向重试的批次也可能很大（/retry 全部失败项）：MTW 真挂着时一样要熔断，
        # 否则几千个文件 × 全部源会一路打到底。只有小批次才值得整批跑完。
        may_abort = len(files) > _ABORT_AFTER_TRANSPORT

        for fp in files:
            if self._stop_event().is_set():
                logger.info("[music-scraper] 收到停止信号，提前结束")
                break
            scanned += 1
            key = str(fp)
            try:
                st = fp.stat()
                mtime, size = int(st.st_mtime), int(st.st_size)
            except OSError as err:
                logger.debug(f"[music-scraper] 无法 stat：{key} — {err}")
                continue
            live_keys.add(key)

            try:
                # 增量：命中 done 缓存直接跳过（不读 tag）。
                if not effective_force and not targeted and key in done:
                    prev = done[key]
                    if isinstance(prev, dict) and prev.get("mtime") == mtime and prev.get("size") == size:
                        from_cache += 1
                        continue

                # 失败上限：非强制、非定向时，超限且文件未变则不再自动重试。
                if not effective_force and not targeted and key in failed:
                    prev = failed[key]
                    if isinstance(prev, dict) \
                            and int(prev.get("retries", 0)) >= self.MAX_RETRY \
                            and prev.get("mtime") == mtime and prev.get("size") == size:
                        exhausted += 1
                        continue

                # 每个文件的 tag 只解析一次：补空判定与匹配身份共用这份读取结果
                tags = _read_music_tags(fp)

                # 补空模式：tag 与封面都齐了才跳过。
                if not effective_force and self.__is_complete(fp, tags):
                    prev = done.get(key)
                    source = prev.get("source") if isinstance(prev, dict) else None
                    done[key] = {"mtime": mtime, "size": size, "ts": now_iso, "source": source}
                    failed.pop(key, None)
                    skipped_tagged += 1
                    dirty = True
                    continue

                okf, reason, score, source = self.__scrape_file(
                    fp, tags=tags, overwrite=effective_overwrite)
            except Exception as err:
                # 单个文件把插件或宿主打崩不能带走整轮：崩在第七千个文件上，前面六千多个
                # 判断还没落账（落账每 60 秒一次），槽位释放、下一轮从头又崩在同一条路径上
                # —— 账本里的坏值、宿主 ScrapingChain 的内部异常都是确定性的，于是全库
                # 永远停在待处理。这个文件既不记成功也不记失败，下一轮照原样再来；
                # 连着出现就按环境异常熔断，不烧这个文件的 retries。
                errors += 1
                transport_streak += 1
                logger.error(f"[music-scraper] 处理异常：{key} — {err}")
                if may_abort and transport_streak >= _ABORT_AFTER_TRANSPORT:
                    logger.warning(
                        f"[music-scraper] 连续 {transport_streak} 个文件处理异常，"
                        f"判为环境异常，本轮中止"
                    )
                    aborted = True
                    break
                continue

            if reason.startswith(_TRANSPORT_REASONS):
                # MTW 侧故障：插件没学到任何关于这个文件的信息，所以既不写 failed 账、
                # 也不涨 retries，原样留着下一轮再处理。连续踩满就熔断中止本轮。
                transport += 1
                transport_streak += 1
                if may_abort and transport_streak >= _ABORT_AFTER_TRANSPORT:
                    logger.warning(
                        f"[music-scraper] 连续 {transport_streak} 个文件是 MTW 故障，"
                        f"判为 MTW 整体不可用，本轮中止"
                    )
                    aborted = True
                    break
            else:
                transport_streak = 0
                if okf:
                    done[key] = {"mtime": mtime, "size": size, "ts": now_iso, "source": source}
                    failed.pop(key, None)
                    ok += 1
                    dirty = True
                else:
                    prev = failed.get(key)
                    prev_retries = int(prev.get("retries", 0)) if isinstance(prev, dict) else 0
                    failed[key] = {
                        "mtime": mtime, "size": size,
                        "retries": prev_retries + 1,
                        "last_ts": now_iso, "reason": reason, "score": score,
                    }
                    done.pop(key, None)
                    failed_n += 1
                    dirty = True
                    # 失败也要一行一行留痕：一轮里几十个文件逐个问满 7 个源可能静默跑上百分钟，
                    # 日志一片空白看着就像卡死，而账本里的原因又要刷开数据页才看得到。
                    logger.info(
                        f"[music-scraper] 未通过：{key} — {reason}"
                        f"{'，分数 ' + str(score) if score is not None else ''}"
                        f"（第 {prev_retries + 1}/{self.MAX_RETRY} 次）"
                    )

            # 定期落账：整轮动辄几小时，中途容器重启不该把已刮好的部分全丢掉。
            # 按时间而不是次数：plugindata 是单行 JSON，每次写都要整本重新序列化。
            if dirty and (time.time() - last_flush) >= _FLUSH_INTERVAL:
                self.save_data(_KEY_DONE, done)
                self.save_data(_KEY_FAILED, failed)
                dirty = False
                last_flush = time.time()

        # 清理：仅整盘扫描且未中断/未熔断时剔除磁盘上已消失的记账项，避免状态无限膨胀。
        # 熔断或提前退出时 live_keys 只覆盖了一部分目录，这时候 prune 会误删未走到的条目。
        if not targeted and not aborted and not self._stop_event().is_set():
            for k in list(done.keys()):
                if k not in live_keys:
                    done.pop(k, None)
            for k in list(failed.keys()):
                if k not in live_keys:
                    failed.pop(k, None)

        # "待处理"要和同一时刻数据页的待处理桶是同一个口径，否则两边对不上号、
        # 看着像有文件凭空消失。桶是"这份文件列表里既不在 done 也不在 failed 的那些"，
        # 这里就直接照它算：MTW 故障、处理异常的文件都不落账所以仍然待处理，熔断/停止
        # 没走到的也一样，上一轮已经记过失败的则回到失败桶（用计数器加减时很容易漏一种）。
        pending = sum(
            1 for fp in files
            if str(fp) not in done and str(fp) not in failed
        )
        summary = {
            "ts": now_iso,
            "scanned": scanned,
            "from_cache": from_cache,
            "skipped_tagged": skipped_tagged,
            "ok": ok,
            "failed": failed_n,
            "exhausted": exhausted,
            "transport": transport,
            "error": errors,
            "aborted": aborted,
            "pending": pending,
            "force": effective_force,
            "targeted": targeted,
            "cron": self._normalize_dow((self._cron or "").strip()) or _DEFAULT_CRON,
            "duration_s": round(time.time() - started, 1),
        }
        self.save_data(_KEY_DONE, done)
        self.save_data(_KEY_FAILED, failed)
        # 定向重试只覆盖几个文件：写进 runs 会把整盘汇总刷成 scanned=1、pending=0，
        # 数据页看起来就像整库已经扫完了。它单独记在 last_targeted。
        self.save_data(_KEY_TARGETED_RUNS if targeted else _KEY_RUNS, summary)
        self.__dict__["_pending_cache"] = None
        logger.info(
            f"[music-scraper] {'定向重试' if targeted else '扫描'}完成："
            f"命中缓存 {from_cache}，已有tag {skipped_tagged}，"
            f"成功 {ok}，失败 {failed_n}，超限跳过 {exhausted}，MTW 故障 {transport}"
            f"{'，处理异常 ' + str(errors) if errors else ''}"
            f"{'（本轮熔断中止，未处理的下轮再来）' if aborted else ''}"
            f"（{summary['duration_s']}s）"
        )

    def __is_complete(self, path: Path, tags: Optional[Dict[str, Any]] = None) -> bool:
        """补空模式的“够格”判据：title + artist + album + 内嵌封面。

        只判 title/artist 会让已刮齐标签、但缺封面的文件永远被跳过，而那恰恰是
        真实音乐库最常见剩下的缺口（MP 写入链路会把 cover_url 下载后内嵌）。
        """
        if tags is None:
            tags = _read_music_tags(path)
        return bool(
            tags["title"] and tags["artist"] and tags["album"] and tags["has_cover"]
        )

    def __scrape_file(self, path: Path, tags: Optional[Dict[str, Any]] = None,
                      overwrite: bool = False) -> Tuple[bool, str, Optional[int], str]:
        """处理单个文件，返回 (是否成功, 失败原因, 命中分数, 命中来源)。"""
        identity = _local_identity(path, tags, roots=self._identity_roots())
        if not identity["title"]:
            # 和"MTW 无候选"分开：那种是问过了、曲库里确实查不到，这种是一次都没问 ——
            # 该做的是把这类文件（整张 CD 压成一个 wav）排出扫描范围，不是放宽阈值。
            return False, "本地读不出曲名（tag 与目录结构都没给出）", None, ""
        candidates, failure = self._mtw_fetch_candidates(path, identity=identity)
        if failure:
            # MTW 侧故障：不能算这个文件的锅，调用方按原因前缀区分后不记 retries。
            return False, failure, None, ""
        if not candidates:
            return False, "MTW 无候选", None, ""
        picked, score, hits = self._mtw_best_candidate(path, candidates, identity=identity)
        if not picked or score < self._min_score:
            # 分数不够时把"差在哪一项"一起写进原因：2/3 分本身不够用来看该改文件名、
            # 该放宽阈值，还是该把这类文件排出扫描范围 —— 曲名没对上和本地根本没有曲名，
            # 要做的决定完全相反。
            miss = self._miss_text(hits)
            detail = f"未达阈值（最低匹配分数 {self._min_score}）"
            return False, f"{detail}：{miss}" if miss else detail, score, ""
        source = str(picked.get("resource") or "").strip()
        info = self._candidate_to_musicinfo(picked, path)
        if not info:
            return False, "候选信息不完整，无法转换", score, source
        try:
            fileitem = schemas.FileItem(
                type="file",
                name=path.name,
                path=str(path),
                extension=path.suffix.lstrip("."),
                size=path.stat().st_size if path.exists() else 0,
            )
        except OSError:
            fileitem = schemas.FileItem(
                type="file", name=path.name, path=str(path),
                extension=path.suffix.lstrip("."), size=0,
            )
        ok, msg = _MusicScrapeChain().scrape_metadata(
            fileitem=fileitem,
            mediainfo=info,
            overwrite=overwrite,
        )
        if ok:
            logger.info(f"[music-scraper] 刮削成功: {path.name} → {info.title}（{source or 'MTW'}）")
            return True, "", score, source
        logger.warning(f"[music-scraper] 刮削失败: {path.name} — {msg}")
        return False, f"写入失败：{msg}", score, source

    # ---------------- 查询辅助（供 status/files） ----------------

    @staticmethod
    def __aggregate_reasons(failed: Dict[str, dict]) -> List[Dict[str, Any]]:
        """把失败账本按原因聚合成 [{reason, count, best_score, retryable}]，按数量降序。"""
        grouped: Dict[str, Dict[str, Any]] = {}
        for entry in failed.values():
            reason = str(entry.get("reason") or "").strip() or "未记录原因"
            item = grouped.setdefault(reason, {
                "reason": reason, "count": 0, "best_score": None, "retryable": 0,
            })
            item["count"] += 1
            score = entry.get("score")
            if isinstance(score, int) and not isinstance(score, bool):
                if item["best_score"] is None or score > item["best_score"]:
                    item["best_score"] = score
            if int(entry.get("retries") or 0) < MusicLibraryScraper.MAX_RETRY:
                item["retryable"] += 1
        return sorted(grouped.values(), key=lambda r: -r["count"])

    def _build_status(self) -> Dict[str, Any]:
        """组装状态汇总：runs + 分桶计数 + 配置回显（不含密码）+ 最近失败。"""
        cron_now = self._cron_text()
        done = self._load_done()
        failed = self._load_failed()
        runs = self._load_runs()
        retryable = sum(1 for v in failed.values() if int(v.get("retries", 0)) < self.MAX_RETRY)
        exhausted = len(failed) - retryable
        recent_failed = sorted(
            ({"path": k, **(v if isinstance(v, dict) else {})} for k, v in failed.items()),
            key=lambda r: r.get("last_ts") or "",
            reverse=True,
        )[:10]
        # 失败原因分布：逐行看 reason 要一页页翻，而"阈值要不要降一档、要不要多开一个源"
        # 是按原因聚合出来的一个决定。best_score 是这一类里最高的分数，它和 min_score 只差
        # 1 时，降一档阈值就能救回这一整类 —— 没有这个数，用户只能瞎猜。
        failure_reasons = self.__aggregate_reasons(failed)
        return {
            "enabled": self._enabled,
            "running": self._is_running(),
            "max_retry": self.MAX_RETRY,
            "runs": runs,
            # 定向重试的汇总单独一处：混进 runs 会让整盘统计被一个文件的重试盖掉。
            "last_targeted": self._load_runs(_KEY_TARGETED_RUNS),
            "counts": {
                "done": len(done),
                "failed": len(failed),
                "retryable": retryable,
                "exhausted": exhausted,
                "pending_unknown": runs.get("pending", 0),
            },
            "failure_reasons": failure_reasons,
            "config": {
                "cron": cron_now[0],
                "cron_next": cron_now[1],
                "mode": "force_all" if self._force_all else "fill_empty",
                "scraper_paths": self._scraper_paths,
                "exclude_paths": self._exclude_paths,
                "mtw_base_url": self._mtw_base_url,
                "mtw_resource": self._mtw_resources,
                "min_score": self._min_score,
                # 实际会处理的扩展名（本地候选列表 ∩ 宿主白名单）：宿主把某个格式移出
                # 白名单时，这里能立刻看出来插件已经不再碰它。
                "audio_extensions": list(_supported_audio_ext()),
                "has_username": bool(self._mtw_username),
                # 绝不回传密码。
            },
            "recent_failed": recent_failed,
        }

    def _collect_rows(self, bucket: str) -> List[Dict[str, Any]]:
        """把某桶整理成表格行 {path,name,mtime,size,status,reason,retries,score,ts}。"""
        rows: List[Dict[str, Any]] = []
        if bucket == "failed":
            failed = self._load_failed()
            for path, v in failed.items():
                v = v if isinstance(v, dict) else {}
                rows.append({
                    "path": path, "name": Path(path).name,
                    "mtime": v.get("mtime"), "size": v.get("size"),
                    "status": "failed", "reason": v.get("reason"),
                    "retries": v.get("retries"), "score": v.get("score"),
                    "ts": v.get("last_ts"),
                })
        elif bucket == "pending":
            cached = self.__dict__.get("_pending_cache")
            if cached and time.time() - cached["ts"] < _PENDING_TTL:
                return list(cached["rows"])
            done = self._load_done()
            failed = self._load_failed()
            known = set(done) | set(failed)
            for fp in self._enumerate_files():
                key = str(fp)
                if key in known:
                    continue
                try:
                    st = fp.stat()
                    mtime, size = int(st.st_mtime), st.st_size
                except OSError:
                    mtime, size = None, None
                rows.append({
                    "path": key, "name": fp.name,
                    "mtime": mtime, "size": size,
                    "status": "pending", "reason": None,
                    "retries": None, "score": None, "ts": None,
                })
            # 待处理 = 全盘遍历做差集，是这几个接口里最贵的一档。数据页翻页、搜索、
            # 刷新都会再打一次，短时间缓存一份，扫描开始/结束时主动作废。
            self.__dict__["_pending_cache"] = {"ts": time.time(), "rows": list(rows)}
        else:  # done
            done = self._load_done()
            for path, v in done.items():
                v = v if isinstance(v, dict) else {}
                rows.append({
                    "path": path, "name": Path(path).name,
                    "mtime": v.get("mtime"), "size": v.get("size"),
                    "status": "done", "reason": None,
                    "retries": None, "score": None,
                    "ts": v.get("ts"), "source": v.get("source"),
                })
        return rows

    # ---------------- MTW HTTP 客户端 ----------------

    def _mtw_headers(self) -> Dict[str, str]:
        token = self._mtw_token()
        return {"Authorization": f"JWT {token}"} if token else {}

    def _mtw_token(self) -> Optional[str]:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token
        if not (self._mtw_username and self._mtw_password):
            # 每个请求都会走这个分支，一轮几千个文件能把日志刷满：只说一次
            if not self.__dict__.get("_anon_warned"):
                self.__dict__["_anon_warned"] = True
                logger.warning("[music-scraper] 未配置 MTW 账密，将匿名请求（可能失败）")
            return None
        self.__dict__["_anon_warned"] = False
        # 每个文件、每个源都要先问一次令牌。账密错 / MTW 把 /apimt/token/ 关了的时候，
        # 这就是成倍增长的必然失败请求外加一屏一样的错误日志：静默期内直接按未登录处理。
        if time.time() < float(self.__dict__.get("_login_retry_at") or 0.0):
            return None
        resp, reason = _mtw_post(
            f"{self._mtw_base_url}/apimt/token/",
            {"username": self._mtw_username, "password": self._mtw_password},
            {"Content-Type": "application/json"},
            expect_data=False,
        )
        token = resp.get("token") if isinstance(resp, dict) else None
        if not token:
            self.__dict__["_login_retry_at"] = time.time() + _LOGIN_BACKOFF
            # 不回显响应体，避免 MTW 的错误信息里夹带的敏感内容进日志。
            logger.error(
                f"[music-scraper] MTW JWT 获取失败（{reason or '响应里没有 token'}），"
                f"{int(_LOGIN_BACKOFF)} 秒内不再尝试登录；检查 MTW 地址与账密"
            )
            return None
        self.__dict__["_login_retry_at"] = 0.0
        self._token = token
        self._token_expiry = _token_expiry_at(token, resp)
        return self._token

    def _mtw_drop_token(self):
        """丢掉缓存的令牌，下次请求重新登录。

        只清令牌、不清登录静默期：登录本身还在失败时，401 重试路径再去戳一次
        /apimt/token/ 只会把失败请求放大成每个源一次。
        """
        self._token = None
        self._token_expiry = 0.0

    def _mtw_request(self, endpoint: str, body: Dict[str, Any],
                     auth_retry: bool = True) -> Tuple[Any, Optional[str]]:
        """POST MTW 的 /apimt/<endpoint>/，返回 (响应体, 故障原因)。

        故障原因非空表示这次请求没能给出结论（不可达 / 鉴权 / 接口报错），调用方不能把它
        当成"这个文件查不到"。鉴权失败（HTTP 401/403）会强制重登一次再重试，覆盖 MTW 换密钥、
        轮换 JWT、管理员踢人这些让缓存令牌提前失效的情况。
        """
        resp, reason = _mtw_post(
            f"{self._mtw_base_url}/apimt/{endpoint}/",
            body,
            {"Content-Type": "application/json", **self._mtw_headers()},
        )
        if reason == _AUTH_REASON and auth_retry:
            self._mtw_drop_token()
            if self._mtw_token():
                return self._mtw_request(endpoint, body, auth_retry=False)
        return resp, reason

    def _mtw_fetch_candidates(self, path: Path,
                              identity: Optional[Dict[str, Any]] = None
                              ) -> Tuple[List[dict], Optional[str]]:
        """按配置的源依次向 MTW 拉候选并合并，返回 (候选, MTW 侧故障原因)。

        默认全源：单源 top10 经常整批是翻唱/钢琴版（实测 min_score=3 只能过 57%），
        换源比放宽阈值安全。某源已经出过满分候选就不再往下问，控制请求量。

        故障原因只在**没有任何源成功应答**时才回报。只要有一个源正常返回（哪怕是空列表），
        那就是"这份曲库查不到"的确定答案，要按文件失败记账：否则某一个上游挂了
        （musicbrainz 限流、咪咕 500）就会让整轮在前几个文件就熔断，全库永远停在待处理。
        """
        if identity is None:
            identity = _local_identity(path, roots=self._identity_roots())
        if not identity["title"]:
            return [], None
        want = _want_from(identity)
        merged: List[dict] = []
        failure: Optional[str] = None
        answered = 0
        best_score = 0
        for resource in _normalize_resources(self._mtw_resources):
            # MTW 前端契约：title/artist/album 取自本地 tag，full_path 仅作参考
            body = {
                "resource": resource,
                "title": identity["title"],
                "artist": identity["artist"],
                "album": identity["album"],
                "full_path": str(path),
            }
            resp, reason = self._mtw_request("fetch_id3_by_title", body)
            if reason:
                failure = reason
                continue
            answered += 1
            data = resp.get("data") if isinstance(resp, dict) else None
            if isinstance(data, dict) and "data" in data:
                data = data["data"]
            if not isinstance(data, list):
                continue
            for c in data:
                if not isinstance(c, dict):
                    continue
                # 以本插件的请求参数为准标记来源，MTW 回什么都不信
                c["resource"] = resource
                merged.append(c)
                if best_score < _MAX_SCORE:
                    best_score = max(best_score, self._score_candidate(c, want))
            if best_score >= _MAX_SCORE:
                break
        if not merged and failure and not answered:
            return [], failure
        return merged, None

    # ---------------- 候选匹配 & 字段映射 ----------------

    def _mtw_pick_best(self, path: Path, candidates: List[dict],
                       identity: Optional[Dict[str, Any]] = None
                       ) -> Tuple[Optional[dict], int]:
        best, best_score, _ = self._mtw_best_candidate(path, candidates, identity=identity)
        return (best if best_score >= self._min_score else None), best_score

    def _mtw_best_candidate(self, path: Path, candidates: List[dict],
                            identity: Optional[Dict[str, Any]] = None
                            ) -> Tuple[Optional[dict], int, Optional[Dict[str, str]]]:
        """返回 (分最高的候选, 分数, 该候选的逐项判定)。

        分数不够时调用方要把"差在哪一项"写进失败原因：只知道 2/3 分，就分不清是
        候选本身对不上，还是本地压根没有这项可以去对（整张 CD 压成一个 wav 的文件
        就是后者），两种情况要做的决定完全相反。
        """
        if identity is None:
            identity = _local_identity(path, roots=self._identity_roots())
        want = _want_from(identity)
        best, best_score, best_hits = None, 0, None
        for c in candidates:
            hits = self._field_hits(c, want)
            score = sum(1 for state in hits.values() if state == _HIT)
            if best is None or score > best_score:
                best, best_score, best_hits = c, score, hits
        return best, max(best_score, 0), best_hits

    @staticmethod
    def _field_hits(c: dict, want: dict) -> Dict[str, str]:
        """MTW utils.match_song 简化版：曲名 / 艺人 / 专辑 逐项判定。

        每项是 hit（对上）/ miss（有得比但没对上）/ no_local（本地缺这项，永远比不了）。
        三者必须分开：no_local 记 0 分是"信息不全"，把它说成"没对上"会让人去改文件名，
        而真正该做的是把这类文件排除出扫描范围。
        """
        hits: Dict[str, str] = {}

        want_title = _norm_text(want.get("title"))
        got_title = _norm_text(c.get("name") or c.get("title"))
        if not want_title:
            hits["title"] = _NO_LOCAL
        else:
            hits["title"] = _HIT if (
                got_title and (want_title in got_title or got_title in want_title)
            ) else _MISS

        artists_raw = c.get("artists") or c.get("artist") or c.get("singer") or []
        if isinstance(artists_raw, (list, tuple)):
            got_artist = _norm_text(" ".join(
                str(a.get("name") if isinstance(a, dict) else a) for a in artists_raw
            ))
        else:
            got_artist = _norm_text(artists_raw)
        want_artist = _norm_text(want.get("artist"))
        if not want_artist:
            hits["artist"] = _NO_LOCAL
        else:
            hits["artist"] = _HIT if (
                got_artist and (want_artist in got_artist or got_artist in want_artist)
            ) else _MISS

        album_obj = c.get("album") or {}
        if isinstance(album_obj, dict):
            got_album = _norm_text(album_obj.get("name") or album_obj.get("title"))
        else:
            got_album = _norm_text(album_obj)
        want_album = _norm_text(want.get("album"))
        if not want_album:
            hits["album"] = _NO_LOCAL
        else:
            hits["album"] = _HIT if (
                got_album and (want_album in got_album or got_album in want_album)
            ) else _MISS

        return hits

    @staticmethod
    def _miss_text(hits: Optional[Dict[str, str]]) -> str:
        """把逐项判定写成一句人话：差哪几项、分别是因为什么。"""
        if not hits:
            return ""
        parts: List[str] = []
        for key, label in _MATCH_FIELDS:
            state = hits.get(key)
            if state == _HIT:
                continue
            if state == _NO_LOCAL:
                parts.append(f"本地无{label}")
            elif state == _MISS:
                parts.append(f"{label}未对上")
        return "、".join(parts)

    @staticmethod
    def _score_candidate(c: dict, want: dict) -> int:
        """MTW utils.match_song 简化版：title / artist / album 各 0/1。"""
        hits = MusicLibraryScraper._field_hits(c, want)
        return sum(1 for state in hits.values() if state == _HIT)

    def _candidate_to_musicinfo(self, c: dict, path: Path) -> Optional[MusicInfo]:
        """把 MTW 候选折叠成 MP MusicInfo。

        MTW 各源（netease / qmusic / kuwo / kugou / migu）在候选里字段命名不完全一致，
        常见差异：artist 是字符串而非 list、album 是字符串而非 dict、cover 字段位于
        顶层 album_img / pic / album.picUrl；year 常见是 "YYYY-MM-DD" 完整日期串。
        lyrics 已经在候选里返回，不再二次调 fetch_lyric。
        """
        title = str(c.get("name") or c.get("title") or "").strip()
        if not title:
            return None

        artist_list = self._normalize_artists(c.get("artists") or c.get("artist"))

        album_obj = c.get("album")
        if isinstance(album_obj, dict):
            album = str(album_obj.get("name") or album_obj.get("title") or "").strip() or None
        elif isinstance(album_obj, str):
            album = album_obj.strip() or None
        else:
            album = None

        cover_url = None
        for key in ("album_img", "pic", "cover", "img", "album_pic", "picture"):
            value = c.get(key)
            if isinstance(value, str) and value:
                cover_url = value
                break
        if not cover_url and isinstance(album_obj, dict):
            for key in ("picUrl", "pic", "cover", "img"):
                value = album_obj.get(key)
                if isinstance(value, str) and value:
                    cover_url = value
                    break

        album_artist = None
        for key in ("albumartist", "album_artist"):
            value = c.get(key)
            if isinstance(value, str) and value.strip():
                album_artist = value.strip()
                break

        year: Optional[int] = None
        for key in ("year", "publish_time", "publishTime", "pub_time", "ptime"):
            publish = c.get(key)
            year = self._normalize_year(publish)
            if year:
                break

        duration_s = self._normalize_duration(c.get("duration") or c.get("dt"))

        track_number = c.get("tracknumber") or c.get("track_number") or c.get("index") or c.get("no")
        try:
            track_number = int(track_number) if track_number is not None and track_number != 0 else None
        except (TypeError, ValueError):
            track_number = None

        total_tracks = c.get("totaltracks") or c.get("total_tracks")
        try:
            total_tracks = int(total_tracks) if total_tracks else None
        except (TypeError, ValueError):
            total_tracks = None

        disc_number = c.get("discnumber") or c.get("disc_number")
        try:
            disc_number = int(disc_number) if disc_number else None
        except (TypeError, ValueError):
            disc_number = None

        total_discs = c.get("totaldiscs") or c.get("total_discs")
        try:
            total_discs = int(total_discs) if total_discs else None
        except (TypeError, ValueError):
            total_discs = None

        lyrics: Optional[str] = None
        raw_lyrics = c.get("lyrics") or c.get("lyric") or c.get("lrc")
        if isinstance(raw_lyrics, str) and raw_lyrics.strip() and not raw_lyrics.startswith("未找到"):
            lyrics = raw_lyrics

        composers = self._normalize_artists(c.get("composer"))
        genres: List[str] = []
        for key in ("genre", "language"):
            value = c.get(key)
            if isinstance(value, str) and value.strip():
                genres.append(value.strip())

        song_id = str(c.get("id") or c.get("song_id") or c.get("mid") or "").strip() or None
        source = str(c.get("resource") or "").strip()

        # MusicInfo 的 type / artist 不是 init 参数（type 是 init=False 派生字段，
        # 艺人统一走 artists: list[str]）；media_source 枚举里没有 netease/qmusic，
        # 留着不填，来源与歌曲 id 记在 raw_data 里。
        return MusicInfo(
            music_type="recording",
            title=title,
            artists=artist_list,
            composers=composers,
            album=album,
            album_artist=album_artist or (artist_list[0] if artist_list else None),
            year=year,
            track_number=track_number,
            total_tracks=total_tracks,
            disc_number=disc_number,
            total_discs=total_discs,
            duration=duration_s,
            cover_url=cover_url,
            lyrics=lyrics,
            genres=genres,
            raw_data={
                "mtw_resource": source,
                "mtw_song_id": song_id,
                "mtw_candidate": c,
            },
            field_sources={
                "title": "remote",
                "artists": "remote",
                "album": "remote",
                "year": "remote",
                "lyrics": "remote",
                "cover_url": "remote",
            },
        )

    @staticmethod
    def _normalize_artists(value: Any) -> List[str]:
        """把 artist/artists/composer 三种常见结构统一为 list[str]。"""
        if isinstance(value, str):
            parts = [p.strip() for p in value.replace(";", "/").split("/") if p.strip()]
            return parts or [value.strip()]
        if isinstance(value, list):
            return [
                str(a.get("name") if isinstance(a, dict) else a).strip()
                for a in value
                if (isinstance(a, dict) and a.get("name")) or (isinstance(a, str) and a.strip())
            ]
        return []

    @staticmethod
    def _normalize_year(value: Any) -> Optional[int]:
        if isinstance(value, int) and 1900 <= value <= 2100:
            return value
        if isinstance(value, str):
            digits = "".join(ch for ch in value[:10] if ch.isdigit())
            if len(digits) >= 4:
                try:
                    year = int(digits[:4])
                    if 1900 <= year <= 2100:
                        return year
                except ValueError:
                    return None
        if isinstance(value, (int, float)) and value > 1e9:
            try:
                ts = float(value)
                if ts > 1e12:
                    ts /= 1000
                return datetime.utcfromtimestamp(ts).year
            except Exception:
                return None
        return None

    @staticmethod
    def _normalize_duration(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)):
            seconds = int(value)
            if seconds > 10000:
                seconds //= 1000
            return seconds if seconds > 0 else None
        return None

    # ---------------- 工具 ----------------

    @staticmethod
    def _is_trickplay_path(path: Path) -> bool:
        return any(parent.name.lower().endswith(".trickplay") for parent in path.parents) or (
            path.name.lower().endswith(".trickplay") and path.is_dir()
        )
