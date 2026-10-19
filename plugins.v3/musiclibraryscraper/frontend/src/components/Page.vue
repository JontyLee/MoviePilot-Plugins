<script setup lang="ts">
import { ref, onMounted, computed, inject } from 'vue'
import { FLOW_STEPS } from './flow'

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
type Bucket = 'done' | 'failed' | 'pending'
type View = 'overview' | Bucket
const view = ref<View>('overview')
const bucket = ref<Bucket>('failed')
const search = ref('')
const page = ref(1)
const limit = ref(50)
const offset = ref(0)
let searchTimer: ReturnType<typeof setTimeout> | null = null

const buckets: { key: Bucket; label: string; icon: string; hint: string }[] = [
  { key: 'done', label: '已处理', icon: 'mdi-check-circle-outline', hint: '已经写好标签的文件（本轮与往轮累计）' },
  { key: 'failed', label: '失败待重试', icon: 'mdi-alert-circle-outline', hint: '没查到或没达阈值，下一轮还会再试' },
  { key: 'pending', label: '待处理', icon: 'mdi-progress-clock', hint: '账本里没有、下一轮要问的文件' }
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

const STATUS_TEXT: Record<string, string> = {
  done: '已处理',
  failed: '失败',
  pending: '待处理'
}

const SOURCE_TEXT: Record<string, string> = {
  netease: '网易云',
  qmusic: 'QQ 音乐',
  kuwo: '酷我',
  kugou: '酷狗',
  migu: '咪咕',
  musicbrainz: 'MusicBrainz',
  smart_tag: '智能'
}

function statusText(value: string) {
  return STATUS_TEXT[value] || value
}

function sourceText(value: string) {
  return SOURCE_TEXT[value] || value
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
        search: search.value
      })}`
    )
    rows.value = data?.items || []
    total.value = data?.total || 0
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

function selectView(key: View) {
  view.value = key
  if (key === 'overview') return
  // 回到同一个桶不重置页码与搜索，只有换桶才清：否则点一下概览再点回来就回到第一页。
  if (bucket.value !== key) {
    bucket.value = key
    search.value = ''
    page.value = 1
    offset.value = 0
  }
  loadFiles()
}

// /retry 的三种拒绝要分开说：全部路径在库外（out_of_library）和被扫描占着（busy）
// 是完全不同的原因，混成"稍后再试"会让人反复点。label 只用于成功文案，
// 失败提示带上文件名反而读不通。
function retryResult(res: any, label = '') {
  if (res?.accepted) {
    const rejected = res?.rejected ? `，忽略 ${res.rejected} 个库外路径` : ''
    notify(`已提交重试 ${res.count} 个${label}${rejected}`)
  } else if (res?.reason === 'out_of_library') {
    notify('这些路径不在配置的音乐库范围内，已拒绝', 'warning')
  } else if (res?.reason === 'no_paths') {
    notify('没有可重试的路径', 'warning')
  } else {
    notify('已有扫描在运行，请稍后再试', 'warning')
  }
}

async function retryPaths(paths: string[], label: string) {
  if (!paths.length) return
  busy.value = true
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}/retry`, { paths })
    retryResult(res, label)
    await refresh()
  } catch (e: any) {
    notify(`重试失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = false
  }
}

function retryRow(row: any) {
  retryPaths([row.path], `（${row.name}）`)
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
    badge: b.key === 'failed'
      ? Number(counts.value.failed ?? 0)
      : (b.key === 'done' ? Number(counts.value.done ?? 0) : 0)
  }))
])

// 生效的源清单以服务端给的 active_resource 为准：内置源不认酷我/smart_tag，
// 回显配置原值会让人以为那两个源也在问。
const sourceNames = computed(() =>
  (cfg.value?.active_resource || []).map((s: string) => sourceText(s)).join(' → ') || '全部源'
)

const activeBucket = computed(() => buckets.find(b => b.key === bucket.value))

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
        <div class="text-h6">MusicTagWeb音乐库刮削</div>
        <div class="text-caption text-medium-emphasis">
          枚举库内音频 → 读本地身份 → 逐源检索 → 打分定阈值 → 宿主原生写入 → 记账
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
                      :color="item.key === 'failed' ? 'warning' : 'grey'">{{ item.badge }}</v-chip>
            </template>
          </v-list-item>
        </v-list>
      </nav>

      <section class="mls-content">
        <!-- 概览：整体流转与结论先看，再进具体某一桶 -->
        <div v-if="view === 'overview'" class="mls-pane">
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
                    扫描时间 {{ fmtTime(runs.ts) }}
                    <span v-if="cfg.cron">｜定时 {{ cfg.cron }}（下次 {{ cfg.cron_next }}）</span>
                  </div>
                  <div class="text-body-2">
                    发现 {{ runs.scanned ?? 0 }}｜缓存跳过 {{ runs.from_cache ?? 0 }}｜已有tag跳过
                    {{ runs.skipped_tagged ?? 0 }}
                  </div>
                  <div class="text-body-2">
                    成功 {{ runs.ok ?? 0 }}｜失败 {{ runs.failed ?? 0 }}｜服务侧故障
                    {{ runs.transport ?? 0 }}｜处理异常 {{ runs.error ?? 0 }}
                  </div>
                  <div v-if="runs.aborted" class="text-caption text-warning mt-1">
                    识别来源连续故障，本轮已中止，未处理的留到下一轮
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
                    识别来源 {{ cfg.backend_label || '自动' }}
                    <template v-if="cfg.backend === 'builtin'">
                      ｜直连平台检索，库内路径不外传
                    </template>
                    <template v-else>
                      ｜MTW {{ cfg.mtw_base_url || '未配置' }}｜账密 {{ cfg.has_username ? '已配' : '未配' }}
                    </template>
                  </div>
                  <div class="text-caption text-grey">
                    库路径 {{ (cfg.scraper_paths || []).length }} 个｜排除
                    {{ (cfg.exclude_paths || []).length }} 个
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
              原因里写了差哪一项：「曲名/艺人/专辑未对上」可以调「最低匹配分数」或改文件名；
              「本地无某项」是文件本身没给出这个信息（整张 CD 压成一个 wav），降阈值也救不回来，
              应该把它排出扫描范围。超限的用「重试全部失败」再试。
            </div>
          </div>

          <div class="mls-section">
            <div class="mls-section-title">流水线</div>
            <div class="mls-flow">
              <div v-for="(step, i) in FLOW_STEPS" :key="step.key" class="mls-flow-step">
                <div class="mls-flow-index">{{ i + 1 }}</div>
                <div class="mls-flow-body">
                  <div class="text-body-2 font-weight-medium">
                    <v-icon :icon="step.icon" size="16" class="mr-1" />{{ step.title }}
                  </div>
                  <div class="text-caption text-grey">{{ step.desc }}</div>
                </div>
              </div>
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
              <div class="text-caption text-grey">{{ activeBucket?.hint }}</div>
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
            <v-btn v-if="bucket === 'failed'" size="small" variant="tonal" color="warning"
                   :loading="busy" @click="retryAllFailed">重试全部失败</v-btn>
            <v-btn size="small" variant="tonal" color="info" :loading="busy" @click="scanNow(false)">增量扫描</v-btn>
            <v-btn size="small" variant="tonal" color="error" :loading="busy" @click="scanNow(true)">全量覆盖</v-btn>
          </div>

          <!-- 数据表：items 是服务端分页好的一页，必须用 v-data-table-server 并把总条数交给
               items-length，否则 pageCount 按当前页条数算、翻页永远停在第一页。
               列一律 sortable:false：/files 恒按路径排序（服务端只知道这一种顺序），留着表头
               箭头会让人以为排的是整个桶，实际只重排了眼前这一页 -->
          <v-data-table-server
            :headers="[
              { title: '文件名', key: 'name', sortable: false },
              { title: '状态', key: 'status', width: 130, sortable: false },
              { title: '原因/分数', key: 'reason', sortable: false },
              { title: '重试', key: 'retries', width: 70, sortable: false },
              { title: '大小', key: 'size', width: 90, sortable: false },
              { title: '修改时间', key: 'mtime', width: 180, sortable: false },
              { title: '操作', key: 'actions', sortable: false, width: 90 }
            ]"
            :items="rows"
            :items-length="total"
            :loading="loading"
            items-per-page-text="每页条数"
            :items-per-page-options="[20, 50, 100]"
            :page-text="pageText"
            :page="page"
            :items-per-page="limit"
            density="compact"
            class="elevation-1"
            @update:page="onPage"
            @update:items-per-page="onItemsPerPage"
          >
            <template #item.name="{ item }">
              <div :title="item.path">{{ item.name }}</div>
              <div class="text-caption text-grey text-truncate" style="max-width: 480px">{{ item.path }}</div>
            </template>
            <template #item.status="{ item }">
              <v-chip
                size="x-small"
                variant="tonal"
                :color="item.status === 'done' ? 'success' : item.status === 'failed' ? 'error' : 'grey'"
              >{{ statusText(item.status) }}</v-chip>
            </template>
            <template #item.reason="{ item }">
              <span v-if="item.reason" class="text-caption">{{ item.reason }}</span>
              <span v-if="item.score != null" class="text-caption text-grey"> · 分数 {{ item.score }}</span>
              <span v-else-if="item.source" class="text-caption text-grey"> · {{ sourceText(item.source) }}</span>
              <span v-else-if="!item.reason">-</span>
            </template>
            <template #item.mtime="{ item }">{{ fmtTime(item.mtime) }}</template>
            <template #item.size="{ item }">{{ fmtSize(item.size) }}</template>
            <template #item.actions="{ item }">
              <v-btn size="x-small" variant="text" color="primary" :loading="busy" @click="retryRow(item)">重试</v-btn>
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
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.mls-page { width: 100%; }
.mls-head { display: flex; align-items: center; gap: 8px; padding: 12px 16px; }
.mls-head-text { min-width: 0; }
.mls-body { display: flex; min-height: 440px; }
.mls-nav {
  flex: 0 0 176px;
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
.mls-flow { display: flex; flex-direction: column; gap: 8px; }
.mls-flow-step { display: flex; gap: 10px; align-items: flex-start; }
.mls-flow-index {
  flex: 0 0 22px; height: 22px; border-radius: 50%; font-size: 12px;
  display: flex; align-items: center; justify-content: center; margin-top: 3px;
  background: rgba(var(--v-theme-primary), .14); color: rgb(var(--v-theme-primary)); font-weight: 700;
}
.mls-flow-body { min-width: 0; }
.mls-page .text-truncate {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
@media (max-width: 760px) {
  .mls-body { flex-direction: column; }
  .mls-nav {
    flex: 0 0 auto;
    border-right: none;
    border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  }
}
</style>
