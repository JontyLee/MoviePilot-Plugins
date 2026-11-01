<script setup lang="ts">
import { ref, onMounted, computed, inject } from 'vue'
import FlowPipeline from './FlowPipeline.vue'
import { FLOW_STEPS, stageCounts } from './flow'

interface HostToast {
  error(msg: string): unknown
  info(msg: string): unknown
  success(msg: string): unknown
  warning(msg: string): unknown
}

const emit = defineEmits(['action', 'switch', 'close', 'layout'])

const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function as any, default: null },
  showSwitch: { type: Boolean, default: true }
})

const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`)
const toast = inject<HostToast | null>('moviepilot:toast', null)

const loading = ref(false)
const busy = ref(false)
const snackbar = ref({ show: false, text: '', color: 'info' })

const status = ref<any>({ runs: {}, counts: {}, config: {}, recent_failed: [] })
const rows = ref<any[]>([])
const total = ref(0)
type Bucket = 'done' | 'failed' | 'pending' | 'deficient'
type View = 'overview' | Bucket
const view = ref<View>('overview')
const bucket = ref<Bucket>('failed')
const search = ref('')
const page = ref(1)
const limit = ref(50)
const offset = ref(0)
// 已处理桶里"只看本插件改写过的"：整库跑完后这一桶绝大多数是宿主或往轮写的，
// 不筛就找不到插件到底动了谁
const writtenOnly = ref(false)
let searchTimer: ReturnType<typeof setTimeout> | null = null

const buckets: { key: Bucket; label: string; icon: string; hint: string }[] = [
  { key: 'done', label: '已处理', icon: 'mdi-check-circle-outline', hint: '已经写好标签的文件（本轮与往轮累计）' },
  { key: 'failed', label: '失败待重试', icon: 'mdi-alert-circle-outline', hint: '没查到或没达阈值，下一轮还会再试' },
  { key: 'pending', label: '待处理', icon: 'mdi-progress-clock', hint: '账本里没有、下一轮要问的文件；等自动处理就行，急着要就点行上的「立即刮削」' },
  { key: 'deficient', label: '有缺项', icon: 'mdi-file-document-alert-outline', hint: '已处理但封面/歌词等仍缺失，可单项编辑补齐' }
]

// 表格是分页取回的（items 只有当前页），所以：
// 1) 用 v-data-table-server 并把 total 交给 items-length —— v-data-table 的 pageCount 是拿
//    内存里的 items 长度算的，一页 50 条时 pageCount 恒等于 1，下一页按钮直接永久禁用；
// 2) 页码文案也不能让 Vuetify 按当前页条数算，直接用 offset/limit/total 生成
//    （顺带替掉默认的英文 "1 of 1"）。
const pageText = computed(() => {
  if (!total.value) return '共 0 条'
  const from = offset.value + 1
  const to = Math.min(offset.value + limit.value, total.value)
  return `第 ${from}–${to} 条 / 共 ${total.value} 条`
})

const SOURCE_TEXT: Record<string, string> = {
  netease: '网易云',
  qmusic: 'QQ 音乐',
  kugou: '酷狗',
  migu: '咪咕',
  musicbrainz: 'MusicBrainz'
}

function sourceText(value: string) {
  return SOURCE_TEXT[value] || value
}

// 后端存的是 tag 的字段名（title/artist/cover…），界面上要说人话
const FIELD_TEXT: Record<string, string> = {
  title: '曲名',
  artist: '艺人',
  album: '专辑',
  year: '年份',
  tracknumber: '音轨号',
  cover: '封面',
  lyrics: '歌词'
}

function fieldText(value: string) {
  return FIELD_TEXT[value] || value
}

// 弹层里的五个文本字段：宽度按"曲名艺人最常要读全名"给，音轨号/年份短的多
const EDIT_FIELDS = [
  { key: 'title', cols: '12 md-6' },
  { key: 'artist', cols: '12 md-6' },
  { key: 'album', cols: '12 md-4' },
  { key: 'year', cols: '6 md-4' },
  { key: 'tracknumber', cols: '6 md-4' }
] as const

function changedFieldNames(set: Record<string, string[]> | undefined): string {
  return Object.keys(set || {}).map(fieldText).join('·')
}

function notify(msg: string, color = 'info') {
  if (toast) {
    const fn = (toast as any)[color] || toast.info
    fn.call(toast, msg)
    return
  }
  snackbar.value = { show: true, text: msg, color }
}

// 宿主注入的 api.get 只认第一个参数：第二个对象不会变成查询串，而是静静丢掉。写成
// api.get(url, {bucket,limit}) 时三个桶发出去的都是裸 /files，后端只能按默认值回 done
// 的第一页 —— 表现就是"切换侧栏，数据一模一样"。条件必须自己拼进 url。
function query(params: Record<string, any>): string {
  const pairs: string[] = []
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`)
  }
  return pairs.length ? `?${pairs.join('&')}` : ''
}

async function loadStatus() {
  try {
    status.value = await (props.api as MoviePilotApi).get(`${base.value}/status`)
  } catch (e: any) {
    notify(`状态加载失败：${e?.message || e}`, 'error')
  }
}

async function loadFiles(retryOutOfRange = true) {
  loading.value = true
  try {
    const data = await (props.api as MoviePilotApi).get(
      `${base.value}/files${query({
        bucket: bucket.value,
        limit: limit.value,
        offset: offset.value,
        search: search.value,
        // 空值由 query() 丢掉，等于不筛；只对 done 桶出现在界面上，后端也只在 done 应用
        written: writtenOnly.value ? '1' : ''
      })}`
    )
    rows.value = data?.items || []
    total.value = data?.total || 0
    // 徽标和列表现在都是账本口径，但旧账本要等打开一次这一桶才补全核对结果：
    // 列表总数是当场数出来的，比 status 里的旧值新，拿它修正徽标，两边才不会再各说各话
    if (bucket.value === 'deficient' && !search.value && status.value?.counts) {
      status.value.counts.deficient = total.value
    }
    // 桶变小时页码会越界（重试成功后失败项就没了几十条）：越界的 offset 拿回空页，
    // 界面看着像"该桶暂无数据"，其实后面还有内容。收敛到最后一页再取一次。
    if (retryOutOfRange && total.value > 0 && !rows.value.length) {
      const lastPage = Math.max(1, Math.ceil(total.value / limit.value))
      if (page.value > lastPage) {
        page.value = lastPage
        offset.value = (lastPage - 1) * limit.value
        await loadFiles(false)
        return
      }
    }
  } catch (e: any) {
    rows.value = []
    total.value = 0
    notify(`列表加载失败：${e?.message || e}`, 'error')
  } finally {
    loading.value = false
  }
}

async function refresh() {
  await Promise.all([loadStatus(), view.value === 'overview' ? Promise.resolve() : loadFiles()])
  emit('action')
}

// 侧栏全页（./AppPage）把这一页嵌进去用：配置对话框关掉后要能刷新，
// 又不必重挂组件（重挂会把选中的那一桶、页码、搜索一起丢掉）。
defineExpose({ refresh })

function selectView(key: View) {
  view.value = key
  if (key === 'overview') return
  // 回到同一个桶不重置页码与搜索，只有换桶才清：否则点一下概览再点回来就回到第一页。
  if (bucket.value !== key) {
    bucket.value = key
    search.value = ''
    page.value = 1
    offset.value = 0
    // 「只看改写」是 done 桶专属的筛法，换桶留着它会让别的桶带着一个失效开关
    writtenOnly.value = false
  }
  loadFiles()
}

// /retry 的三种拒绝要分开说：全部路径在库外（out_of_library）和被扫描占着（busy）
// 是完全不同的原因，混成"稍后再试"会让人反复点。label 只用于成功文案，
// 失败提示带上文件名反而读不通。verb 是同一接口在两个桶里的不同叫法：失败桶是
// 重试一次，待处理桶是提前刮一轮。
function retryResult(res: any, label = '', verb = '重试') {
  if (res?.accepted) {
    const rejected = res?.rejected ? `，忽略 ${res.rejected} 个库外路径` : ''
    notify(`已提交${verb} ${res.count} 个${label}${rejected}`)
  } else if (res?.reason === 'out_of_library') {
    notify('这些路径不在配置的音乐库范围内，已拒绝', 'warning')
  } else if (res?.reason === 'no_paths') {
    notify(`没有可${verb}的路径`, 'warning')
  } else {
    notify('已有扫描在运行，请稍后再试', 'warning')
  }
}

async function retryPaths(paths: string[], label: string, verb = '重试') {
  if (!paths.length) return
  busy.value = true
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}/retry`, { paths })
    retryResult(res, label, verb)
    await refresh()
  } catch (e: any) {
    notify(`${verb}失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = false
  }
}

function retryRow(row: any) {
  retryPaths([row.path], `（${row.name}）`, bucket.value === 'pending' ? '立即刮削' : '重试')
}

function retryAllFailed() {
  if (!counts.value.failed) {
    notify('没有失败项可重试', 'warning')
    return
  }
  doRetryAll()
}

async function doRetryAll() {
  busy.value = true
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}/retry`, {
      bucket: 'failed',
      all: true
    })
    retryResult(res, '失败项')
    await refresh()
  } catch (e: any) {
    notify(`重试失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = false
  }
}

async function scanNow(force: boolean) {
  busy.value = true
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}/scan`, { force })
    notify(res?.accepted ? (force ? '已开始全量扫描' : '已开始增量扫描') : '已有扫描在运行', res?.accepted ? 'info' : 'warning')
    await refresh()
  } catch (e: any) {
    notify(`触发扫描失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = false
  }
}

// 点开谁才读谁：/files 的行里没有 tag 值（一页 50 条每条都读一遍 tag 太贵，多数行根本
// 不会被点开），而"已有的先填进去、缺的才要人补"必须知道文件里现在写着什么。
async function loadFile(path: string): Promise<any | null> {
  try {
    const res = await (props.api as MoviePilotApi).get(`${base.value}/file${query({ path })}`)
    if (res?.accepted) return res
    if (res?.reason === 'not_found') notify('这个文件已经不在了，账本里的记录已过期', 'warning')
    else if (res?.reason === 'unreadable') notify('读不出这个文件的标签，可能不是音频或容器不认识', 'warning')
    else if (res?.reason === 'out_of_library') notify('该文件不在配置的音乐库范围内', 'warning')
    else notify('读取文件详情失败', 'error')
  } catch (e: any) {
    notify(`读取文件详情失败：${e?.message || e}`, 'error')
  }
  return null
}

const editDialog = ref(false)
const editLoading = ref(false)
const editRow = ref<any>(null)
const editForm = ref({ title: '', artist: '', album: '', year: '', tracknumber: '', lyrics: '' })
// 打开弹层时文件里的原值：判"已有/缺项"用它，判"这一项改没改"也用它
const editBase = ref<Record<string, string>>({})
const editMissing = ref<string[]>([])
const editHasCover = ref(true)
const editLyricsFrom = ref('')
// 封面不进 editForm：editForm 的每一项都会被当成文本标签提交，而封面是「取图 + 内嵌」，
// 后端走的是另一个键。混在一起的话，粘贴一张图就等于往 TIT2 里写一条 URL。
const editCoverUrl = ref('')

// 只提交真正动过的项：把整表原样发回去会写一堆和文件里一模一样的值，看着像保存成功，
// 实际是把人自己填的内容又盖了一遍（宿主还会因此认为这个文件被改过）。
const editChanged = computed(() =>
  Object.entries(editForm.value)
    .filter(([key, value]) => String(value ?? '').trim()
      && String(value ?? '').trim() !== (editBase.value[key] || ''))
    .map(([key]) => key)
)

async function openEdit(row: any) {
  editRow.value = row
  editForm.value = { title: '', artist: '', album: '', year: '', tracknumber: '', lyrics: '' }
  editBase.value = {}
  editMissing.value = (row.missing || []) as string[]
  editHasCover.value = !editMissing.value.includes('cover')
  editLyricsFrom.value = ''
  editCoverUrl.value = ''
  editCandidates.value = []
  editHistory.value = null
  editCandidateError.value = ''
  editDialog.value = true
  editLoading.value = true
  try {
    const detail = await loadFile(row.path)
    if (!detail) return
    const tags = detail.tags || {}
    editForm.value = {
      title: tags.title || '',
      artist: tags.artist || '',
      album: tags.album || '',
      year: tags.year || '',
      tracknumber: tags.tracknumber || '',
      lyrics: tags.lyrics || ''
    }
    editBase.value = { ...editForm.value }
    editMissing.value = detail.missing || []
    editHasCover.value = !!detail.has_cover
    editLyricsFrom.value = detail.lyrics_from || ''
  } finally {
    editLoading.value = false
  }
}

// 「获取候选」：把内置源各问一遍摆在一起（自动刮削是「满足阈值的候选里挑给得最全的那一条」，
// 这里不合并也不选胜者，每个源只留它自己分最高的一条），由人比较对得准的是谁；
// 历史最佳从失败账直读、不联网 —— 自动刮削差半分时它常常就是
// 正确答案，只是没到自动写入那条线，值得摆出来让人决定采不采纳。
const editCandidates = ref<any[]>([])
const editHistory = ref<any>(null)
const editCandidateLoading = ref(false)
const editCandidateError = ref('')

async function fetchCandidates() {
  if (!editRow.value) return
  editCandidateLoading.value = true
  editCandidateError.value = ''
  try {
    const res = await (props.api as MoviePilotApi).get(`${base.value}/candidates${query({ path: editRow.value.path })}`)
    if (res?.accepted) {
      editCandidates.value = res.candidates || []
      editHistory.value = res.history || null
      if (!editCandidates.value.length) editCandidateError.value = '各源都没有答这一条'
    } else if (res?.reason === 'no_title') {
      editCandidateError.value = '读不出本地曲名，无法检索（文件名和 tag 都没给出曲名）'
    } else if (res?.reason === 'sources_unreachable') {
      editCandidateError.value = `识别源没答上：${res.detail || '网络/风控'}`
    } else if (res?.reason === 'out_of_library') {
      editCandidateError.value = '该文件不在配置的音乐库范围内'
    } else {
      editCandidateError.value = res?.reason || '获取失败'
    }
  } catch (e: any) {
    editCandidateError.value = `获取失败：${e?.message || e}`
  } finally {
    editCandidateLoading.value = false
  }
}

// 采纳只填非空值：候选没有年份时拿空串盖进表格，会把人已经填好的年份清掉
// （而候选的"缺项"和文件里的"缺项"根本不是一回事）。
// 歌词只有源真给出可落盘的文本时才在（后端与写文件用同一判据筛过）。
const ADOPT_FIELDS = ['title', 'artist', 'album', 'year', 'tracknumber', 'lyrics'] as const

function candidateFields(cand: any): Record<string, string> {
  const raw = cand?.fields || {}
  const out: Record<string, string> = {}
  for (const key of ADOPT_FIELDS) {
    // 历史最佳来自失败账的摘要（brief），短字段在顶层、没有完整 fields 一层：
    // 顶层值截断过，但对曲名/艺人/专辑/年份/音轨这种短值够用；歌词只认真值，不拿摘要凑
    const value = String(raw[key] ?? (key === 'lyrics' ? '' : cand?.[key] ?? '')).trim()
    if (value) out[key] = value
  }
  return out
}

function adoptCount(cand: any): number {
  const fields = candidateFields(cand)
  return Object.entries(fields)
    .filter(([key, value]) => value !== (editBase.value[key] || ''))
    .length
}

// 封面不进 ADOPT_FIELDS：那是文本标签的名单，采纳封面只是把一个地址填进表单，
// 真把图嵌进文件要等点「保存」。历史最佳那条只有摘要、没有地址，所以它没有这个按钮。
function candidateCover(cand: any): string {
  return String(cand?.cover_url || '').trim()
}

function adoptCover(cand: any) {
  const url = candidateCover(cand)
  if (!url) {
    notify('这条候选没给出封面地址', 'warning')
    return
  }
  if (url === editCoverUrl.value) {
    notify('封面地址已经填过了', 'warning')
    return
  }
  editCoverUrl.value = url
  notify('封面地址已填入，点「保存」才下载到文件里', 'success')
}

// keys 省略 = 全部非空项；候选详情弹窗按勾选传，写哪些由人点名，不自动决定
function adoptCandidate(cand: any, keys?: string[]) {
  const fields = candidateFields(cand)
  const picked = keys ? Object.fromEntries(
    Object.entries(fields).filter(([key]) => keys.includes(key))) : fields
  const before = Object.entries(picked)
    .filter(([key, value]) => value !== (editBase.value[key] || '')).length
  for (const [key, value] of Object.entries(picked)) {
    (editForm.value as any)[key] = value
  }
  if (!before) notify('候选给出的值文件里已经有了', 'warning')
  return before
}

// 候选详情弹窗：一条候选占一行看不清全部字段，"采纳"又是一次性全盖 ——
// 这里逐字段列出候选值与文件现值，勾选要覆盖哪些。
const candDialog = ref(false)
const candDetail = ref<any>(null)
const candPicked = ref<string[]>([])

function openCandidate(cand: any) {
  if (!cand) return
  candDetail.value = cand
  const fields = candidateFields(cand)
  // 默认只勾"与文件现值不同"的项：把一堆一样的值勾上再点应用，等于没做决定
  candPicked.value = Object.entries(fields)
    .filter(([key, value]) => value !== (editBase.value[key] || ''))
    .map(([key]) => key)
  candDialog.value = true
}

function applyCandidatePicked() {
  if (!candDetail.value || !candPicked.value.length) return
  const n = adoptCandidate(candDetail.value, candPicked.value)
  candDialog.value = false
  notify(n ? `已填入 ${n} 项，点「保存」才写入文件` : '选的值和文件里的一样', n ? 'success' : 'warning')
}

// 逐项判定要说人话：hit 是"这一项对上了"，no_local 是本地根本没这项、没法比 ——
// 后两种在挑选时含义完全不同（改文件名 / 改阈值 / 换源）。
const HIT_TEXT: Record<string, string> = { hit: '对', miss: '不对', no_local: '无' }

function hitMark(state: string) {
  return HIT_TEXT[state] || state
}

async function submitEdit() {
  if (!editRow.value) return
  const fields: Record<string, string> = {}
  for (const key of editChanged.value) {
    fields[key] = String((editForm.value as any)[key] ?? '').trim()
  }
  const cover = String(editCoverUrl.value || '').trim()
  if (!Object.keys(fields).length && !cover) {
    notify('没有需要写入的改动', 'warning')
    return
  }
  busy.value = true
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}/edit`, {
      path: editRow.value.path,
      fields,
      ...(cover ? { cover_url: cover } : {})
    })
    if (res?.accepted) {
      const n = Object.keys(fields).length
      // 封面单独说一句：文本写成功、图没嵌进去是半件事，混成「已写入 N 项」就是谎报
      const coverText = res.cover === 'ok' ? '，封面已内嵌'
          : res.cover === 'failed' ? '，但封面内嵌失败（检查文件是否可写，或换一个地址）' : ''
      notify(`${n ? `已写入 ${n} 项` : '封面已提交'}${coverText}`,
             res.cover === 'failed' ? 'warning' : 'success')
      editDialog.value = false
      await refresh()
    } else if (res?.reason === 'out_of_library') {
      notify('该文件不在媒体库内，无法写入', 'warning')
    } else if (res?.reason === 'invalid_fields') {
      notify('字段不合法或为空', 'warning')
    } else if (res?.reason === 'write_failed') {
      notify('写标签失败，检查文件权限', 'error')
    } else if (res?.reason === 'no_path') {
      notify('缺少文件路径', 'warning')
    } else if (res?.reason === 'cover_unavailable') {
      notify('取不到这张图：地址可能失效，或被平台挡了 —— 什么都没写入', 'warning')
    } else if (res?.reason === 'cover_unsupported') {
      notify('当前主程序没有封面下载接口，补不了封面 —— 什么都没写入', 'warning')
    } else {
      notify('保存失败', 'error')
    }
  } catch (e: any) {
    notify(`保存失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = false
  }
}

// 详情弹层：点开一行看"这个文件现在写着什么、本插件把它从什么改成了什么"。
const detailDialog = ref(false)
const detailLoading = ref(false)
const detail = ref<any>(null)
const detailRow = ref<any>(null)

async function openDetail(row: any) {
  detailRow.value = row
  detail.value = null
  detailDialog.value = true
  detailLoading.value = true
  try {
    detail.value = await loadFile(row.path)
  } finally {
    detailLoading.value = false
  }
}

// 整行可点，但操作列里的按钮不能被带着一起开弹层（点"重试"会顺手弹出个详情框）
function onRowClick(event: Event, args: any) {
  const target = event.target as HTMLElement | null
  if (target?.closest?.('button')) return
  const item = args?.item ?? args
  if (item?.path) openDetail(item)
}

function onWrittenFilterChange() {
  page.value = 1
  offset.value = 0
  loadFiles()
}

function detailSets(): Record<string, string[]> {
  return detail.value?.rewrote || detailRow.value?.set || {}
}

function onPage(nextPage: number) {
  page.value = Math.max(1, nextPage)
  offset.value = (page.value - 1) * limit.value
  loadFiles()
}

function onItemsPerPage(next: number) {
  limit.value = next > 0 ? next : 50
  page.value = 1
  offset.value = 0
  loadFiles()
}

// 每敲一个字就发一次 /files 的话，后端每次都要把整桶排序、pending 桶还要全库 stat。
function onSearch() {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(() => {
    page.value = 1
    offset.value = 0
    loadFiles()
  }, 300)
}

function fmtTime(unix?: number | string) {
  if (!unix && unix !== 0) return '-'
  if (typeof unix === 'string') return unix
  try {
    return new Date(unix * 1000).toLocaleString()
  } catch {
    return String(unix)
  }
}

function fmtSize(bytes?: number) {
  if (!bytes) return '-'
  const mb = bytes / 1048576
  return mb >= 1 ? `${mb.toFixed(1)} MB` : `${(bytes / 1024).toFixed(0)} KB`
}

const counts = computed(() => status.value?.counts || {})
const runs = computed(() => status.value?.runs || {})
const cfg = computed(() => status.value?.config || {})
// 定向重试不再覆盖 runs（否则 scanned=1 会让整库看着像刚扫完），它单独记在
// last_targeted；概览补一行，界面才不会停在"上次扫描是三天前"。
const lastTargeted = computed(() => status.value?.last_targeted || {})
const running = computed(() => !!status.value?.running)
// 失败原因分布：调「最低匹配分数」、加识别源都是按原因做的决定，逐行翻表估不出来。
const failureReasons = computed<any[]>(() => status.value?.failure_reasons || [])
const minScore = computed(() => cfg.value?.min_score ?? 3)

function reasonLabel(group: any): string {
  const parts: string[] = [`${group.count} 个`]
  const best = group.best_score
  if (typeof best === 'number') {
    parts.push(`最高 ${best} 分`)
    const gap = minScore.value - best
    if (gap > 0) parts.push(`阈值降 ${gap} 分可命中`)
  }
  if (!group.retryable) parts.push('均已超限')
  return parts.join(' · ')
}

const navItems = computed(() => [
  { key: 'overview' as View, title: '概览', icon: 'mdi-view-dashboard-outline', badge: 0 },
  ...buckets.map(b => ({
    key: b.key as View,
    title: b.label,
    icon: b.icon,
    // 有缺项也挂数字：这一桶要人逐个进来补，不标数量就看不出还剩多少没处理。
    badge: b.key === 'failed'
      ? Number(counts.value.failed ?? 0)
      : (b.key === 'deficient' ? Number(counts.value.deficient ?? 0) : (b.key === 'done' ? Number(counts.value.done ?? 0) : 0))
  }))
])

// 生效的源清单以服务端回显为准：不认识的源后端已经丢掉了，回显配置原值会让人
// 以为那几个源也在被问。
const sourceNames = computed(() =>
  (cfg.value?.resources || []).map((s: string) => sourceText(s)).join(' → ') || '全部源'
)
// 流水线上的数字来自本轮汇总折算（见 flow.ts 的 stageCounts）
const stages = computed(() => stageCounts(runs.value))

const activeBucket = computed(() => buckets.find(b => b.key === bucket.value))

// 表头随桶走，三条理由各是一条踩过的坑：
// 1) 分桶就是按状态分的，一个桶里每行的状态都相同 —— 「状态」列等于把桶名重印一遍；
// 2) retries 只有失败账里有值，其余桶留着就是一列空白，看着像坏了的列；
// 3) 中间那一栏在失败桶是失败原因加分数、在缺项桶是还缺哪几项、在已处理桶是写入来源，
//    列名跟着内容改；一路叫「原因/分数」就是有缺项那页在骗人。
const REASON_TITLE: Record<Bucket, string> = {
  done: '写入来源',
  failed: '原因/分数',
  pending: '',
  deficient: '还缺哪几项'
}

// 每列都给宽度，配合 .mls-table 的 table-layout: fixed：不给宽度的话表格按内容摊，
// 文件名列里那条不换行的完整路径会把宽度撑到自己满意为止，中间的「写入来源 /
// 还缺哪几项」被挤成一列竖排，反而是要看的内容最窄。
const tableHeaders = computed(() => {
  const cols: any[] = [{ title: '文件名', key: 'name', width: '36%', sortable: false }]
  if (REASON_TITLE[bucket.value]) {
    cols.push({ title: REASON_TITLE[bucket.value], key: 'reason', width: '28%', sortable: false })
  }
  if (bucket.value === 'failed') {
    cols.push({ title: '重试', key: 'retries', width: 76, sortable: false })
  }
  cols.push({ title: '大小', key: 'size', width: 76, sortable: false })
  cols.push({ title: '修改时间', key: 'mtime', width: 145, sortable: false })
  cols.push({ title: '操作', key: 'actions', width: bucket.value === 'pending' ? 100 : 150, sortable: false })
  return cols
})

// 改写项数：账本里的 set 是插件真往文件里写过的字段差集，空的就是这条没动过手
function rewrittenCount(item: any) {
  return Object.keys(item?.set || {}).length
}

onMounted(() => {
  // 侧栏版式比宿主默认的 60rem 宽得多，宽度由宿主的数据框按这个事件设置
  emit('layout', { maxWidth: '86rem' })
  refresh()
})
</script>

<template>
  <v-card flat class="mls-page">
    <div class="mls-head">
      <v-avatar color="primary" variant="tonal" rounded="lg" size="34" class="mr-3">
        <v-icon icon="mdi-music-circle-outline" size="22" />
      </v-avatar>
      <div class="mls-head-text">
        <div class="text-h6">TagFlow音乐库刮削</div>
        <div class="text-caption text-medium-emphasis">
          直连平台检索补标签 · 逐文件核对与补齐在下方分桶里
        </div>
      </div>
      <v-spacer />
      <v-chip v-if="running" color="info" size="small" variant="tonal" prepend-icon="mdi-progress-clock" class="mr-2">
        扫描进行中
      </v-chip>
      <v-btn v-if="props.showSwitch" size="small" variant="text" class="text-none"
             prepend-icon="mdi-cog-outline" @click="emit('switch')">配置</v-btn>
      <v-btn size="small" variant="text" class="text-none" :loading="loading || busy"
             prepend-icon="mdi-refresh" @click="refresh">刷新</v-btn>
    </div>
    <v-divider />

    <div class="mls-body">
      <nav class="mls-nav">
        <v-list density="comfortable" nav class="py-2">
          <v-list-item
            v-for="item in navItems"
            :key="item.key"
            :active="view === item.key"
            color="primary"
            rounded="lg"
            class="mls-nav-item"
            @click="selectView(item.key)"
          >
            <template #prepend><v-icon :icon="item.icon" class="mr-2" /></template>
            <v-list-item-title>{{ item.title }}</v-list-item-title>
            <template #append>
              <v-chip v-if="item.badge" size="x-small" variant="tonal"
                      :color="item.key === 'failed' ? 'warning' : item.key === 'deficient' ? 'info' : 'grey'">{{ item.badge }}</v-chip>
            </template>
          </v-list-item>
        </v-list>
      </nav>

      <section class="mls-content">
        <!-- 概览：整体流转与结论先看，再进具体某一桶 -->
        <div v-if="view === 'overview'" class="mls-pane">
          <!-- 流转动画占第一屏：在不在跑、跑到哪一环、各环落下多少，一眼看完。
               逐项判据不在这里摊开，仓库 docs/tagflow.md 才有地方写那些。 -->
          <div class="mls-section">
            <div class="mls-flow-head">
              <div class="mls-section-title">本轮流转</div>
              <div class="text-caption text-grey">
                <template v-if="running">扫描进行中 · 光点走到哪一环就问到了哪一环</template>
                <template v-else-if="runs.ts">
                  静止的是上一轮：{{ fmtTime(runs.ts) }}，{{ (runs.force === undefined ? '只补缺' : (runs.force ? '强制覆盖' : '只补缺')) }}
                  · 用时 {{ runs.duration_s ?? 0 }}s
                </template>
                <template v-else>还没跑过一轮 —— 下面勾一个按钮开跑</template>
              </div>
              <v-spacer />
              <v-btn size="small" variant="tonal" color="info" :loading="busy"
                     @click="scanNow(false)">增量扫描</v-btn>
              <v-btn size="small" variant="tonal" color="warning" :loading="busy"
                     @click="retryAllFailed">重试失败</v-btn>
            </div>
            <FlowPipeline :steps="FLOW_STEPS" :counts="stages" :running="running" variant="live" />
          </div>

          <v-row dense>
            <v-col cols="6" md="3">
              <v-card variant="tonal" color="success">
                <v-card-text class="py-2">
                  <div class="text-caption">已处理</div>
                  <div class="text-h5">{{ counts.done ?? 0 }}</div>
                </v-card-text>
              </v-card>
            </v-col>
            <v-col cols="6" md="3">
              <v-card variant="tonal" color="warning">
                <v-card-text class="py-2">
                  <div class="text-caption">待重试</div>
                  <div class="text-h5">{{ counts.retryable ?? 0 }}</div>
                </v-card-text>
              </v-card>
            </v-col>
            <v-col cols="6" md="3">
              <v-card variant="tonal" color="error">
                <v-card-text class="py-2">
                  <div class="text-caption">超限放弃</div>
                  <div class="text-h5">{{ counts.exhausted ?? 0 }}</div>
                </v-card-text>
              </v-card>
            </v-col>
            <v-col cols="6" md="3">
              <v-card variant="tonal" color="grey">
                <v-card-text class="py-2">
                  <div class="text-caption">待处理</div>
                  <div class="text-h5">{{ counts.pending_unknown ?? 0 }}</div>
                </v-card-text>
              </v-card>
            </v-col>
          </v-row>

          <div class="mls-section">
            <div class="mls-section-title">最近一轮</div>
            <v-row dense>
              <v-col cols="12" md="6">
                <v-card variant="outlined" class="pa-3 mls-fill">
                  <div class="text-caption text-medium-emphasis mb-1">
                    扫描时间 {{ fmtTime(runs.ts) }}｜用时 {{ runs.duration_s ?? 0 }}s
                  </div>
                  <div v-if="cfg.cron" class="text-caption text-grey">
                    定时 {{ cfg.cron }}｜下次 {{ cfg.cron_next }}
                  </div>
                  <div v-if="runs.aborted" class="text-caption text-warning mt-1">
                    识别源连续故障，本轮已中止，未处理的留到下一轮
                  </div>
                  <div v-if="lastTargeted.ts" class="text-caption text-grey mt-1">
                    最近定向重试 {{ fmtTime(lastTargeted.ts) }}：成功 {{ lastTargeted.ok ?? 0 }}｜失败
                    {{ lastTargeted.failed ?? 0 }}
                  </div>
                </v-card>
              </v-col>
              <v-col cols="12" md="6">
                <v-card variant="outlined" class="pa-3 mls-fill">
                  <div class="text-caption text-medium-emphasis mb-1">本轮生效的配置</div>
                  <div class="text-body-2">
                    {{ cfg.mode === 'force_all' ? '强制覆盖' : '只补缺' }}｜最低匹配分数
                    {{ cfg.min_score }}｜{{ sourceNames }}
                  </div>
                  <div class="text-caption text-grey">
                    库路径 {{ (cfg.scraper_paths || []).length }} 个｜排除
                    {{ (cfg.exclude_paths || []).length }} 个｜歌词{{ cfg.fill_lyrics ? '主动抓取' : '交给宿主' }}
                  </div>
                  <div class="text-caption text-grey">
                    实际处理的扩展名：{{ (cfg.audio_extensions || []).join(' / ') }}
                  </div>
                </v-card>
              </v-col>
            </v-row>
          </div>

          <div v-if="failureReasons.length" class="mls-section">
            <div class="mls-section-title">失败原因分布</div>
            <v-card variant="outlined" class="mls-reason">
              <div v-for="group in failureReasons" :key="group.reason" class="mls-reason-row">
                <span class="mls-reason-text">{{ group.reason }}</span>
                <span class="text-caption text-no-wrap">{{ reasonLabel(group) }}</span>
              </div>
            </v-card>
            <div class="text-caption text-grey mt-1">
              原因里写了差哪一项：「未对上」可以调分数或改文件名；「本地无某项」是文件本身没给出
              这个信息，降分数也救不回来，该把它排出扫描范围。
            </div>
          </div>

          <div class="d-flex align-center gap-2 mt-4">
            <v-spacer />
            <v-btn size="small" variant="tonal" color="warning" :loading="busy" @click="retryAllFailed">重试全部失败</v-btn>
            <v-btn size="small" variant="tonal" color="info" :loading="busy" @click="scanNow(false)">增量扫描</v-btn>
            <v-btn size="small" variant="tonal" color="error" :loading="busy" @click="scanNow(true)">全量覆盖</v-btn>
          </div>
        </div>

        <!-- 某一桶：搜索 + 分页表 -->
        <div v-else class="mls-pane">
          <div class="d-flex align-center flex-wrap gap-2 mb-2">
            <div>
              <div class="text-subtitle-2">{{ activeBucket?.label }}</div>
              <div class="text-caption text-grey">{{ activeBucket?.hint }}｜点任意一行看详情</div>
            </div>
            <v-spacer />
            <v-text-field
              v-model="search"
              density="compact"
              hide-details
              placeholder="搜索路径/文件名"
              prepend-inner-icon="mdi-magnify"
              style="max-width: 260px"
              clearable
              @update:model-value="onSearch"
            />
            <v-btn v-if="bucket === 'done'" size="small" variant="tonal"
                   :color="writtenOnly ? 'primary' : 'default'" class="text-none"
                   :prepend-icon="writtenOnly ? 'mdi-filter' : 'mdi-filter-outline'"
                   @click="writtenOnly = !writtenOnly; onWrittenFilterChange()">
              只看本插件改写{{ writtenOnly ? '' : `（${counts.rewritten ?? 0}）` }}
            </v-btn>
            <v-btn v-if="bucket === 'failed'" size="small" variant="tonal" color="warning"
                   :loading="busy" @click="retryAllFailed">重试全部失败</v-btn>
            <v-btn size="small" variant="tonal" color="info" :loading="busy" @click="scanNow(false)">增量扫描</v-btn>
            <v-btn size="small" variant="tonal" color="error" :loading="busy" @click="scanNow(true)">全量覆盖</v-btn>
          </div>

          <!-- 数据表：items 是服务端分页好的一页，必须用 v-data-table-server 并把总条数交给
               items-length，否则 pageCount 按当前页条数算、翻页永远停在第一页。
               列一律 sortable:false（见 tableHeaders）：顺序只有服务端那一种（done 按"本插件
               改写优先 + 最近改动在上"，其余桶按路径），留着表头箭头会让人以为排的是整个桶，
               实际只重排了眼前这一页。
               表头随桶走：一个桶里每一行的状态都相同，留着「状态」列等于把桶名重印一遍；
               「重试」次数只有失败桶有值（其余桶这个字段恒空）；缺项桶那一栏给的是还缺哪几项，
               跟着失败桶叫「原因/分数」就是列名和内容各说各话。 -->
          <v-data-table-server
            :headers="tableHeaders"
            :items="rows"
            :items-length="total"
            :loading="loading"
            items-per-page-text="每页条数"
            :items-per-page-options="[20, 50, 100]"
            :page-text="pageText"
            :page="page"
            :items-per-page="limit"
            density="compact"
            class="elevation-1 mls-table"
            @click:row="onRowClick"
            @update:page="onPage"
            @update:items-per-page="onItemsPerPage"
          >
            <template #item.name="{ item }">
              <div class="text-truncate" :title="item.name">{{ item.name }}</div>
              <div class="text-caption text-grey text-truncate" :title="item.path">{{ item.path }}</div>
              <!-- 改写了哪几项直接摊在行上：这一桶要一眼看出插件动了谁，点开详情才知道就没人点 -->
              <div v-if="rewrittenCount(item)" class="mt-1">
                <v-chip size="x-small" variant="tonal" color="primary" prepend-icon="mdi-pen-plus">
                  改写 {{ rewrittenCount(item) }} 项
                </v-chip>
                <span class="text-caption text-primary ml-1">{{ changedFieldNames(item.set) }}</span>
              </div>
            </template>
            <template #item.retries="{ item }">
              <!-- retries 是「定时扫描已经为这个文件试过几轮」，满了上限就不再自动重试
                   （只剩手动），所以这一列只在失败桶出现 -->
              <span v-if="item.retries" class="text-caption">第 {{ item.retries }} 次</span>
              <span v-else class="text-caption text-grey">还没重试</span>
            </template>
            <template #item.reason="{ item }">
              <!-- 缺项桶给的是字段名，直接印出来是 year,tracknumber,cover 这种半英文串 -->
              <span v-if="item.missing?.length" class="text-caption">
                {{ item.missing.map(fieldText).join('、') }}
              </span>
              <span v-else-if="item.reason" class="text-caption">{{ item.reason }}</span>
              <span v-if="item.score != null" class="text-caption text-grey"> · 分数 {{ item.score }}</span>
              <!-- 「写入来源」只在插件真往文件里写过字时才成立。账本对每条已处理都记着本轮的
                   扫描来源，判定已齐跳过的、值是宿主自己写的那几条 set 是空的 —— 照印来源就成了
                   「本插件改写 0 条」的徽标旁边一排网易云，自己打自己的脸。 -->
              <span v-else-if="item.source && rewrittenCount(item)" class="text-caption text-grey">
                · {{ sourceText(item.source) }}
              </span>
              <span v-else-if="bucket === 'done'" class="text-caption text-grey">未改写</span>
              <span v-else-if="!item.reason && !item.missing?.length">-</span>
            </template>
            <template #item.mtime="{ item }">{{ fmtTime(item.mtime) }}</template>
            <template #item.size="{ item }">{{ fmtSize(item.size) }}</template>
            <template #item.actions="{ item }">
              <!-- 待处理不给编辑：那一桶是"还没轮到"，不是"写坏了要修"，文件里现在写着什么
                   插件根本还没判过，手工填进去等于把没核对过的现值固化成结论。要动手早晚，
                   正确动作是让插件现在就问一轮 —— 所以这一桶只给「立即刮削」。
                   编辑对已处理/失败/有缺项开放；重试只在失败桶（这一列本来也只在那儿出现）。
                   详情不给按钮：整行本来就是点开详情的入口（@click:row），按钮留着只是占宽 -->
              <v-btn v-if="bucket !== 'pending'" size="x-small" variant="text" color="primary"
                     :loading="busy" @click="openEdit(item)">编辑</v-btn>
              <v-btn v-if="bucket === 'failed'" size="x-small" variant="text" color="primary"
                     :loading="busy" @click="retryRow(item)">重试</v-btn>
              <v-btn v-if="bucket === 'pending'" size="x-small" variant="text" color="primary"
                     :loading="busy" @click="retryRow(item)">立即刮削</v-btn>
            </template>
            <template #no-data>
              <div class="text-center pa-6 text-grey">该桶暂无数据</div>
            </template>
          </v-data-table-server>
        </div>
      </section>
    </div>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3000" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>

    <!-- 详情：这个文件现在写着什么、本插件把它从什么改成了什么 -->
    <v-dialog v-model="detailDialog" max-width="680px">
      <v-card variant="flat">
        <div class="mls-dlg-head">
          <v-avatar color="primary" variant="tonal" rounded="lg" size="36" class="mr-3">
            <v-icon icon="mdi-file-music-outline" size="20" />
          </v-avatar>
          <div class="mls-dlg-head-text">
            <div class="text-subtitle-1">{{ detailRow?.name }}</div>
            <div class="text-caption text-grey text-truncate">{{ detailRow?.path }}</div>
          </div>
          <v-spacer />
          <v-btn icon="mdi-close" variant="text" size="small" @click="detailDialog = false" />
        </div>
        <v-divider />
        <v-card-text class="pt-4">
          <v-progress-linear v-if="detailLoading" indeterminate color="primary" height="2" />
          <template v-else-if="detail">
            <div class="mls-section-title">本插件的改写</div>
            <v-table v-if="Object.keys(detailSets()).length" density="compact" class="mls-diff mb-4">
              <thead>
                <tr>
                  <th class="text-left">字段</th>
                  <th class="text-left">改前</th>
                  <th class="text-left">改后</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(pair, field) in detailSets()" :key="field">
                  <td class="text-no-wrap">{{ fieldText(String(field)) }}</td>
                  <td class="mls-diff-old">{{ pair[0] || '（空）' }}</td>
                  <td class="mls-diff-new">{{ pair[1] || '（空）' }}</td>
                </tr>
              </tbody>
            </v-table>
            <div v-else class="text-caption text-grey mb-4">
              这一条没有改写记录：要么本轮判定已齐直接跳过，要么是宿主自己写入的
            </div>
            <div v-if="detail.set_ts" class="text-caption text-grey mt-n3 mb-4">
              最近一次改写 {{ detail.set_ts }}｜来源 {{ sourceText(detail.source || '') }}
            </div>

            <div class="mls-section-title">文件里现在写着</div>
            <v-row dense>
              <v-col v-for="field in ['title', 'artist', 'album', 'year', 'tracknumber']"
                     :key="field" cols="6" md="4">
                <div class="mls-kv">
                  <div class="text-caption text-grey">{{ fieldText(field) }}</div>
                  <div class="mls-kv-value">{{ (detail.tags || {})[field] || '—' }}</div>
                </div>
              </v-col>
              <v-col cols="6" md="4">
                <div class="mls-kv">
                  <div class="text-caption text-grey">封面</div>
                  <div class="mls-kv-value">{{ detail.has_cover ? '已内嵌' : '无' }}</div>
                </div>
              </v-col>
              <v-col cols="12" md="4">
                <div class="mls-kv">
                  <div class="text-caption text-grey">歌词</div>
                  <div class="mls-kv-value">
                    {{ detail.lyrics_from === 'sibling' ? '同名 .lrc 里有'
                       : (detail.lyrics_from === 'embedded' ? '已内嵌' : '无') }}
                  </div>
                </div>
              </v-col>
            </v-row>
            <div v-if="(detail.missing || []).length" class="text-caption text-grey mt-2">
              仍缺：{{ detail.missing.map(fieldText).join('、') }}
            </div>
          </template>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" class="text-none" @click="detailDialog = false">关闭</v-btn>
          <v-btn v-if="detailRow?.status === 'deficient'" variant="flat" color="primary"
                 class="text-none" @click="detailDialog = false; openEdit(detailRow)">补齐缺项</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- 补齐缺项：已有的先填进去，人只补还缺的那几项 -->
    <v-dialog v-model="editDialog" max-width="800px">
      <v-card variant="flat">
        <div class="mls-dlg-head">
          <v-avatar color="primary" variant="tonal" rounded="lg" size="36" class="mr-3">
            <v-icon icon="mdi-music-box-outline" size="20" />
          </v-avatar>
          <div class="mls-dlg-head-text">
            <div class="text-subtitle-1">补齐缺项</div>
            <div class="text-caption text-grey text-truncate">{{ editRow?.path }}</div>
          </div>
          <v-spacer />
          <v-btn icon="mdi-close" variant="text" size="small" @click="editDialog = false" />
        </div>
        <v-divider />
        <v-card-text class="pt-4">
          <v-progress-linear v-if="editLoading" indeterminate color="primary" height="2" />
          <div v-if="editMissing.length" class="mls-chips mb-3">
            <v-chip v-for="field in editMissing" :key="field" size="x-small" variant="tonal"
                    color="warning" prepend-icon="mdi-alert-circle-outline">{{ fieldText(field) }}</v-chip>
            <span class="text-caption text-grey ml-1">以上几项文件里没有，其余都是已填好的现值</span>
          </div>
          <div v-else-if="!editLoading" class="text-caption text-grey mb-3">
            按后端判定这一条已经齐了，下面的值就是文件里的现值
          </div>

          <v-form @submit.prevent="submitEdit">
            <v-row dense>
              <v-col v-for="field in EDIT_FIELDS" :key="field.key" :cols="field.cols">
                <v-text-field
                  v-model="editForm[field.key]"
                  :label="fieldText(field.key)"
                  :loading="editLoading"
                  density="comfortable"
                  hide-details="auto"
                  variant="outlined"
                >
                  <template #append-inner>
                    <v-chip v-if="!editBase[field.key]" size="x-small" variant="text"
                            color="warning">缺</v-chip>
                    <v-icon v-else-if="editChanged.includes(field.key)" icon="mdi-pencil-circle"
                            size="x-small" color="primary" />
                    <v-icon v-else icon="mdi-check-circle-outline" size="x-small" color="grey" />
                  </template>
                </v-text-field>
              </v-col>
            </v-row>

            <div class="mls-lyrics mt-2">
              <div class="d-flex align-center">
                <div class="mls-section-title mb-0">歌词</div>
                <v-spacer />
                <v-chip v-if="editLyricsFrom === 'sibling'" size="x-small" variant="tonal" color="info">
                  取自同名 .lrc
                </v-chip>
                <v-chip v-else-if="editLyricsFrom === 'embedded'" size="x-small" variant="text" color="grey">
                  文件里已有
                </v-chip>
              </div>
              <v-textarea v-model="editForm.lyrics" density="compact" rows="5" auto-grow
                          hide-details variant="outlined" :loading="editLoading"
                          placeholder="留空则不改动" />
            </div>

            <div class="mls-lyrics mt-3">
              <div class="d-flex align-center">
                <div class="mls-section-title mb-0">封面</div>
                <v-spacer />
                <v-chip size="x-small" :variant="editHasCover ? 'text' : 'tonal'"
                        :color="editHasCover ? 'grey' : 'warning'">
                  {{ editHasCover ? '文件里已有' : '缺' }}
                </v-chip>
              </div>
              <div class="d-flex align-center gap-2 mt-1">
                <v-text-field v-model="editCoverUrl" density="compact" hide-details
                              variant="outlined" clearable
                              placeholder="图片地址 https://…，留空则不动封面" />
                <v-avatar v-if="editCoverUrl" size="52" rounded="lg" variant="flat">
                  <v-img :src="editCoverUrl" cover />
                </v-avatar>
              </div>
              <div class="text-caption text-grey mt-1">
                填了地址就会下载并内嵌这一张；文件里已有封面会被换掉。图取自地址由插件下载，
                不是浏览器里那张预览 —— 预览打不开通常就意味着地址失效或被平台挡了
              </div>
            </div>
          </v-form>

          <!-- 内置源候选：自动刮削只在满足阈值的候选里挑给得最全的一条，这里恰恰相反 ——
               各源都问一遍摆出来，由人比较谁对得准；采纳只填非空项，不覆盖已填好的值 -->
          <div class="mls-section mt-5">
            <div class="d-flex align-center">
              <div class="mls-section-title mb-0">内置源候选</div>
              <v-spacer />
              <v-btn size="small" variant="tonal" color="info" :loading="editCandidateLoading"
                     prepend-icon="mdi-magnify" @click="fetchCandidates">获取全部源候选</v-btn>
            </div>
            <div v-if="editCandidateError" class="text-caption text-warning mt-2">{{ editCandidateError }}</div>
            <v-card v-if="editHistory" variant="outlined" class="mt-2 pa-2 mls-cand-click"
                    @click="openCandidate(editHistory)">
              <div class="d-flex align-center flex-wrap gap-1">
                <v-chip size="x-small" variant="tonal" color="warning" prepend-icon="mdi-history">
                  历史最佳 · {{ sourceText(editHistory.source || '') }} · {{ editHistory.score }} 分
                </v-chip>
                <span class="text-caption text-grey text-truncate" :title="editHistory.reason || ''">
                  {{ editHistory.reason || '未达阈值' }}
                </span>
                <v-spacer />
                <v-chip v-for="(state, field) in (editHistory.hits || {})" :key="field"
                        size="x-small" variant="text"
                        :color="state === 'hit' ? 'success' : state === 'no_local' ? 'grey' : 'warning'">
                  {{ fieldText(String(field)) }} {{ hitMark(state) }}
                </v-chip>
                <v-btn size="x-small" variant="text" color="primary" @click.stop="openCandidate(editHistory)">详情</v-btn>
                <v-btn size="x-small" variant="text" :color="adoptCount(editHistory) ? 'primary' : 'grey'"
                       :disabled="!adoptCount(editHistory)" @click.stop="adoptCandidate(editHistory)">
                  采纳{{ adoptCount(editHistory) ? `（${adoptCount(editHistory)} 项）` : '（无新内容）' }}
                </v-btn>
              </div>
              <div class="text-caption mt-1">
                曲名：{{ editHistory.title || '—' }}｜艺人：{{ editHistory.artist || '—' }}｜
                专辑：{{ editHistory.album || '—' }}｜{{ editHistory.year || '—' }} · 第 {{ editHistory.tracknumber || '—' }} 轨
              </div>
            </v-card>
            <div v-for="cand in editCandidates" :key="`${cand.source}-${cand.title}-${cand.album}`"
                 class="mls-cand mls-cand-click" @click="openCandidate(cand)">
              <div class="d-flex align-center flex-wrap gap-1">
                <v-chip size="x-small" variant="tonal" color="primary">{{ sourceText(cand.source || '') }}</v-chip>
                <v-chip size="x-small" variant="text" :color="cand.score >= 3 ? 'success' : 'warning'">
                  {{ cand.score }} 分
                </v-chip>
                <v-chip v-if="cand.has_lyric" size="x-small" variant="text" color="grey">源上有歌词</v-chip>
                <v-chip v-if="candidateCover(cand)" size="x-small" variant="text" color="grey">源上有封面</v-chip>
                <v-spacer />
                <v-chip v-for="(state, field) in (cand.hits || {})" :key="field" size="x-small" variant="text"
                        :color="state === 'hit' ? 'success' : state === 'no_local' ? 'grey' : 'warning'">
                  {{ fieldText(String(field)) }} {{ hitMark(state) }}
                </v-chip>
                <v-btn size="x-small" variant="text" color="primary" @click.stop="openCandidate(cand)">详情</v-btn>
                <v-btn v-if="candidateCover(cand)" size="x-small" variant="text" color="primary"
                       @click.stop="adoptCover(cand)">采纳封面</v-btn>
                <v-btn size="x-small" variant="text" :color="adoptCount(cand) ? 'primary' : 'grey'"
                       :disabled="!adoptCount(cand)" @click.stop="adoptCandidate(cand)">
                  采纳{{ adoptCount(cand) ? `（${adoptCount(cand)} 项）` : '（无新内容）' }}
                </v-btn>
              </div>
              <div class="text-caption mt-1">
                曲名：{{ cand.title || '—' }}｜艺人：{{ cand.artist || '—' }}｜
                专辑：{{ cand.album || '—' }}｜{{ cand.year || '—' }} · 第 {{ cand.tracknumber || '—' }} 轨
              </div>
            </div>
            <div v-if="editCandidateLoading" class="text-caption text-grey mt-2">
              正在并行问内置源（最慢的一个答完就齐），慢的源最多等 6 秒
            </div>
            <div v-else-if="editCandidates.length" class="text-caption text-grey mt-2">
              每个源只列分最高的一条；点「详情」逐字段勾选要覆盖什么，点「采纳」一把填全部非空项，
              带封面的源另有「采纳封面」—— 都只填进表单，仍要点「保存」才写入文件
            </div>
          </div>
        </v-card-text>
        <v-divider />
        <v-card-actions>
          <div class="text-caption text-grey">
            {{ editChanged.length ? `本次写入 ${editChanged.length} 项：${editChanged.map(fieldText).join('、')}`
               : (editCoverUrl.trim() ? '' : '没有改动可写入') }}
            <template v-if="editCoverUrl.trim()">｜{{ editChanged.length ? '并且' : '' }}内嵌封面</template>
          </div>
          <v-spacer />
          <v-btn variant="text" class="text-none" @click="editDialog = false">取消</v-btn>
          <v-btn variant="flat" color="primary" class="text-none"
                 :loading="busy"
                 :disabled="editLoading || (!editChanged.length && !editCoverUrl.trim())"
                 @click="submitEdit">
            保存
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- 候选详情：逐字段对照候选值与文件现值，勾选要覆盖哪些 ——
         点「采纳」一把全盖是替人做决定，这里让写进去的每一项都经过点名 -->
    <v-dialog v-model="candDialog" max-width="640px">
      <v-card variant="flat" border>
        <v-card-title class="text-subtitle-1 d-flex align-center py-3">
          候选详情 · {{ sourceText(candDetail?.source || '') }}
          <v-chip v-if="candDetail" size="x-small" variant="text" class="ml-2"
                  :color="candDetail.score >= 3 ? 'success' : 'warning'">{{ candDetail.score }} 分</v-chip>
          <v-spacer />
          <v-btn icon="mdi-close" size="x-small" variant="text" @click="candDialog = false" />
        </v-card-title>
        <v-divider />
        <v-card-text v-if="candDetail" class="pa-4">
          <div v-for="key in ADOPT_FIELDS" :key="key">
            <v-checkbox v-if="candidateFields(candDetail)[key]" hide-details density="compact"
                        :value="key" :label="fieldText(key)" v-model="candPicked">
              <template #append>
                <div class="text-caption" style="max-width: 380px; white-space: pre-wrap; word-break: break-all">
                  <span class="text-grey">文件：</span>{{ editBase[key] || '（空）' }}
                  <span class="ml-2 text-grey">候选：</span>{{ candidateFields(candDetail)[key] }}
                </div>
              </template>
            </v-checkbox>
          </div>
          <div v-if="candidateCover(candDetail)" class="d-flex align-center gap-2 mt-2">
            <v-avatar size="44" rounded="lg" variant="flat">
              <v-img :src="candidateCover(candDetail)" cover />
            </v-avatar>
            <div class="text-caption" style="max-width: 330px; word-break: break-all">
              <span class="text-grey">封面：</span>{{ candidateCover(candDetail) }}
            </div>
            <v-spacer />
            <v-btn size="x-small" variant="text" color="primary" @click="adoptCover(candDetail)">填入封面地址</v-btn>
          </div>
          <div v-else-if="candDetail?.has_cover" class="text-caption text-grey mt-2">
            这条候选带封面，但账本里的摘要没存地址 —— 要补封面就重新「获取全部源候选」
          </div>
          <div v-if="!Object.keys(candidateFields(candDetail)).length" class="text-caption text-grey">
            这条候选没有给出任何可采纳的值
          </div>
        </v-card-text>
        <v-divider />
        <v-card-actions>
          <div class="text-caption text-grey">应用只填入编辑表格，点「保存」才写入文件</div>
          <v-spacer />
          <v-btn variant="text" class="text-none" @click="candDialog = false">取消</v-btn>
          <v-btn variant="flat" color="primary" class="text-none"
                 :disabled="!candPicked.length" @click="applyCandidatePicked">
            应用所选{{ candPicked.length ? `（${candPicked.length} 项）` : '' }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.mls-page { width: 100%; }
.mls-head { display: flex; align-items: center; gap: 8px; padding: 12px 16px; }
.mls-head-text { min-width: 0; }
.mls-body { display: flex; min-height: 440px; }
.mls-nav {
  /* 208px：默认 176px 时"失败待重试"+图标+数字徽标三段挤在一行，Vuetify 的 nav 标题
     是 nowrap+ellipsis，最长的两个桶名都被切成"失败待重…"。徽标只在部分桶出现，
     按最长的一项给宽度。 */
  flex: 0 0 208px;
  border-right: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  background: rgba(var(--v-theme-on-surface), .02);
}
.mls-nav-item { margin: 2px 8px; }
.mls-content { flex: 1 1 auto; min-width: 0; overflow-y: auto; }
.mls-pane { padding: 14px 16px; }
.mls-section { margin-top: 14px; }
.mls-section-title { font-size: 13px; font-weight: 600; margin-bottom: 6px; opacity: .75; }
.mls-fill { height: 100%; }
.mls-reason { padding: 6px 10px; }
.mls-reason-row { display: flex; align-items: center; gap: 10px; padding: 3px 0; }
.mls-reason-text { flex: 1 1 auto; min-width: 0; font-size: 13px; }
.mls-flow-head { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 6px; }
.mls-page .text-truncate {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
/* 整行点开详情，指针形状要跟着变；表头不在 tbody 里，不受影响 */
.mls-table :deep(tbody tr) { cursor: pointer; }
/* 列宽按表头给的百分比和像素摊死。默认是按内容摊，文件名列里那条不换行的完整路径
   会把这一列撑到自己满意为止，中间的「写入来源 / 还缺哪几项」被压成窄窄一条竖排 ——
   最该看的内容反而最窄。fixed 之后溢出交给单元格裁，行内元素自己截断。 */
.mls-table :deep(table) { table-layout: fixed; }
.mls-table :deep(th),
.mls-table :deep(td) { overflow: hidden; }
.mls-dlg-head { display: flex; align-items: center; gap: 8px; padding: 14px 18px; min-width: 0; }
.mls-dlg-head-text { min-width: 0; flex: 1 1 auto; }
.mls-chips { display: flex; align-items: center; flex-wrap: wrap; gap: 6px; }
.mls-cand {
  padding: 6px 10px;
  border-top: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
}
.mls-cand-click { cursor: pointer; }
.mls-cand-click:hover { background: rgba(var(--v-theme-on-surface), .04); }
.mls-diff { border: 1px solid rgba(var(--v-border-color), var(--v-border-opacity)); }
.mls-diff-old { color: rgba(var(--v-theme-on-surface), .6); text-decoration: line-through; }
.mls-diff-new { font-weight: 600; }
.mls-kv {
  padding: 7px 10px;
  border-radius: 8px;
  background: rgba(var(--v-theme-on-surface), .04);
  min-width: 0;
}
.mls-kv-value {
  font-size: 13px;
  overflow-wrap: anywhere;
  min-height: 18px;
}
.mls-lyrics :deep(textarea) { font-family: inherit; font-size: 13px; line-height: 1.6; }
@media (max-width: 760px) {
  .mls-body { flex-direction: column; }
  .mls-nav {
    flex: 0 0 auto;
    border-right: none;
    border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  }
}
</style>
