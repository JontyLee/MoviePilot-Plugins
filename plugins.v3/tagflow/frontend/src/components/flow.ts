// 流水线只有一份描述：配置页拿它画「一个文件要过几道判断」，数据页拿它画「这一轮走到哪、
// 各环落了多少文件」。两处同源，改了环节不会只改一个页面。
// 这里只留界面要显示的最短文字 —— 逐项判据、失败归因、各源现状都在仓库的
// docs/tagflow.md，配置页与概览不该长成一篇说明文。
export interface FlowStep {
  key: string
  icon: string
  // 两个字左右的环节名，画在节点上
  title: string
  // 一行小字：这一环凭什么放行
  caption: string
  // 决定这一环的配置项（配置页标出来，数据页不显示）
  knobs?: string
}

export const FLOW_STEPS: FlowStep[] = [
  {
    key: 'enumerate',
    icon: 'mdi-folder-search-outline',
    title: '枚举',
    caption: '宿主白名单交集',
    knobs: '音乐库路径 / 排除路径'
  },
  {
    key: 'identity',
    icon: 'mdi-tag-text-outline',
    title: '身份',
    caption: '标签优先，目录兜底',
    knobs: '音乐库路径'
  },
  {
    key: 'incremental',
    icon: 'mdi-skip-forward-outline',
    title: '增量',
    caption: '四项齐全就免问',
    knobs: '写入模式'
  },
  {
    key: 'query',
    icon: 'mdi-magnify',
    title: '检索',
    caption: '全部源并行问',
    knobs: '识别源'
  },
  {
    key: 'score',
    icon: 'mdi-scale-balance',
    title: '打分',
    caption: '分数够，再挑给得全的',
    knobs: '最低匹配分数'
  },
  {
    key: 'write',
    icon: 'mdi-content-save-move-outline',
    title: '写入',
    caption: '宿主原生刮削链',
    knobs: '写入模式 + 宿主策略'
  },
  {
    key: 'ledger',
    icon: 'mdi-book-check-outline',
    title: '记账',
    caption: '故障不算文件失败'
  }
]

// 各环节的数字来自 /status 的 runs（本轮汇总）：数据页把流水线画成"这一轮走到哪了"。
// 键与环节同名，取不到的按 0。
export interface StageCounts {
  enumerate?: number
  identity?: number
  incremental?: number
  query?: number
  score?: number
  write?: number
  ledger?: number
}

export function stageCounts(runs: any): StageCounts {
  const n = (v: any) => Number(v ?? 0) || 0
  const scanned = n(runs?.scanned)
  // 免问的那部分：命中增量账 + 已有标签判齐 + 超限跳过
  const free = n(runs?.from_cache) + n(runs?.skipped_tagged) + n(runs?.exhausted)
  const queried = Math.max(scanned - free, 0)
  return {
    enumerate: scanned,
    identity: scanned,
    incremental: free,
    query: queried,
    score: n(runs?.ok) + n(runs?.failed),
    write: n(runs?.ok),
    ledger: n(runs?.failed) + n(runs?.transport) + n(runs?.error)
  }
}
