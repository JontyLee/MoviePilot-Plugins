// 数据流转的同一份描述：配置页用它讲"这一环由哪个开关决定"，数据页用它讲"这一轮走到哪了"。
// 两处必须同源，否则改了流水线却只改了一个页面。
export interface FlowStep {
  key: string
  icon: string
  title: string
  desc: string
  // 决定这一环的配置项（配置页用得上，数据页只是浏览）
  knobs?: string
}

export const FLOW_STEPS: FlowStep[] = [
  {
    key: 'enumerate',
    icon: 'mdi-folder-search-outline',
    title: '枚举库内音频',
    desc: '只处理「宿主音频白名单 ∩ 插件支持表」的交集；Jellyfin 的 .trickplay 预览缓存整棵跳过，'
      + '目录软链不跟随，指向库外的音频软链也跳过 —— 写标签是按路径落到真实文件的。',
    knobs: '音乐库路径 / 排除路径'
  },
  {
    key: 'identity',
    icon: 'mdi-file-document-edit-outline',
    title: '读本地身份',
    desc: '用 mutagen 读文件已有的曲名/艺人/专辑/年份/轨道号；缺哪项就按 艺人/专辑 (年)/NN - 曲名 '
      + '的目录结构兜底。只有严格在库路径之下的目录才算艺人/专辑，库根本身的名字不参与猜测。',
    knobs: '音乐库路径'
  },
  {
    key: 'incremental',
    icon: 'mdi-skip-forward-outline',
    title: '增量判据',
    desc: '只补缺模式下，曲名+艺人+专辑+内嵌封面 四项齐全的文件按 mtime+size 直接记账跳过，'
      + '一个请求都不发；缺哪项补哪项，已有非空标签不动。强制覆盖则每轮全库重扫重问。',
    knobs: '写入模式'
  },
  {
    key: 'query',
    icon: 'mdi-magnify',
    title: '逐源检索',
    desc: '按「识别来源」逐个问：内置源直接请求各音乐平台的公开检索接口（只发清洗后的「曲名-艺人」检索词，'
      + '库内路径不出本机），MTW 模式转给 /apimt/fetch_id3_by_title（会带完整路径作参考）。'
      + '某个源已经出到 3/3 满分候选就立刻停止，不再问后面的源 —— 否则每个文件都要打满整个源列表。',
    knobs: '识别来源 / 识别源'
  },
  {
    key: 'score',
    icon: 'mdi-scale-balance',
    title: '打分与阈值',
    desc: '曲名 / 艺人 / 专辑 各 1 分（繁简与大小写归一后互相包含即算命中）；都没满分就合并全部候选取最高分。'
      + '最高分仍低于「最低匹配分数」判失败、不写入，也不碰文件。失败原因会连着写清差在哪一项：'
      + '「曲名未对上」是候选不对，可以放宽阈值或改文件名；「本地无曲名」是文件本身没给出曲名'
      + '（整张 CD 压成一个 wav 就是这种），放宽阈值也救不回来，该把它排出扫描范围。',
    knobs: '最低匹配分数'
  },
  {
    key: 'write',
    icon: 'mdi-content-save-move-outline',
    title: '宿主原生写入',
    desc: '命中后沿用 MP 原生音乐刮削链写标签、下载内嵌封面、写歌词；最终是否覆盖还要叠加宿主'
      + '「设置 → 媒体刮削」的音乐三项策略，取两者里较强者。',
    knobs: '写入模式 + 宿主刮削策略'
  },
  {
    key: 'ledger',
    icon: 'mdi-bookcheck-outline',
    title: '记账',
    desc: '成功进「已处理」，未达阈值进「失败待重试」，同一个文件最多重试 5 次后进「超限放弃」。'
      + '服务侧故障不算文件失败、不烧额度：内置源全部不可达、MTW 不可达/鉴权失败都一样，'
      + '连续 5 次故障本轮直接熔断，剩下的留给下一轮。'
  }
]
