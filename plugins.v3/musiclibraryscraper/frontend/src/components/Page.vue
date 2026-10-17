<script setup lang="ts">
import { ref, onMounted, computed, inject } from 'vue'

interface HostToast {
  error(msg: string): unknown
  info(msg: string): unknown
  success(msg: string): unknown
  warning(msg: string): unknown
}

const emit = defineEmits(['action', 'switch', 'close'])

const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function as any, default: null }
})

const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`)
const toast = inject<HostToast | null>('moviepilot:toast', null)

const loading = ref(false)
const busy = ref(false)
const snackbar = ref({ show: false, text: '', color: 'info' })

const status = ref<any>({ runs: {}, counts: {}, config: {}, recent_failed: [] })
const rows = ref<any[]>([])
const total = ref(0)
const bucket = ref<'done' | 'failed' | 'pending'>('failed')
const search = ref('')
const limit = ref(50)
const offset = ref(0)

const tabs: { key: 'done' | 'failed' | 'pending'; label: string }[] = [
  { key: 'done', label: '已处理' },
  { key: 'failed', label: '失败待重试' },
  { key: 'pending', label: '待处理' }
]

// 表格是分页取回的（items 只有当前页），Vuetify 自己算的页码文案会按当前页条数错报，
// 所以直接用 offset/limit/total 生成，顺带把默认的英文 "1 of 1" 换掉。
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

async function loadStatus() {
  try {
    status.value = await (props.api as MoviePilotApi).get(`${base.value}/status`)
  } catch (e: any) {
    notify(`状态加载失败：${e?.message || e}`, 'error')
  }
}

async function loadFiles() {
  loading.value = true
  try {
    const data = await (props.api as MoviePilotApi).get(`${base.value}/files`, {
      bucket: bucket.value,
      limit: limit.value,
      offset: offset.value,
      search: search.value
    })
    rows.value = data?.items || []
    total.value = data?.total || 0
  } catch (e: any) {
    rows.value = []
    total.value = 0
    notify(`列表加载失败：${e?.message || e}`, 'error')
  } finally {
    loading.value = false
  }
}

async function refresh() {
  await Promise.all([loadStatus(), loadFiles()])
  emit('action')
}

async function retryPaths(paths: string[], label: string) {
  if (!paths.length) return
  busy.value = true
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}/retry`, { paths })
    if (res?.accepted) notify(`已提交重试 ${res.count} 个${label}`)
    else notify(`已有扫描在运行，稍后再试${label}`, 'warning')
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
    if (res?.accepted) notify(`已提交重试 ${res.count} 个失败项`)
    else notify('已有扫描在运行，稍后再试', 'warning')
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

function switchTab(key: 'done' | 'failed' | 'pending') {
  bucket.value = key
  offset.value = 0
  loadFiles()
}

function onPage(nextOffset: number) {
  offset.value = Math.max(0, nextOffset)
  loadFiles()
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
const running = computed(() => !!status.value?.running)

onMounted(refresh)
</script>

<template>
  <div class="plugin-root pa-4">
    <div class="d-flex align-center mb-3 flex-wrap gap-2">
      <div class="text-h6">MusicTagWeb音乐库刮削 · 数据页</div>
      <v-spacer />
      <v-chip v-if="running" color="info" size="small" variant="tonal" prepend-icon="mdi-progress-clock">
        扫描进行中
      </v-chip>
      <v-btn size="small" variant="text" prepend-icon="mdi-cog" @click="emit('switch')">配置</v-btn>
      <v-btn size="small" color="primary" :loading="loading || busy" prepend-icon="mdi-refresh" @click="refresh">
        刷新
      </v-btn>
    </div>

    <!-- 汇总卡片 -->
    <v-row dense class="mb-2">
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
            <div class="text-caption">上次扫描</div>
            <div class="text-caption text-no-wrap">{{ fmtTime(runs.ts) }}</div>
            <div class="text-caption">
              缓存{{ runs.from_cache ?? 0 }}｜成功{{ runs.ok ?? 0 }}｜失败{{ runs.failed ?? 0 }}
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- 操作条 -->
    <div class="d-flex align-center flex-wrap gap-2 mb-2">
      <v-tabs density="compact" model-value="failed" bg-color="transparent" show-arrows>
        <v-tab v-for="t in tabs" :key="t.key" :value="t.key" @click="switchTab(t.key)">
          {{ t.label }}
          <v-badge v-if="t.key === 'failed'" :content="counts.failed || 0" inline class="ml-1" color="warning" />
        </v-tab>
      </v-tabs>
      <v-spacer />
      <v-text-field
        v-model="search"
        density="compact"
        hide-details
        placeholder="搜索路径/文件名"
        prepend-inner-icon="mdi-magnify"
        style="max-width: 260px"
        clearable
        @update:model-value="onPage(0)"
      />
      <v-btn size="small" variant="tonal" color="warning" :loading="busy" @click="retryAllFailed">重试全部失败</v-btn>
      <v-btn size="small" variant="tonal" color="info" :loading="busy" @click="scanNow(false)">增量扫描</v-btn>
      <v-btn size="small" variant="tonal" color="error" :loading="busy" @click="scanNow(true)">全量覆盖</v-btn>
    </div>

    <!-- 数据表 -->
    <v-data-table
      :headers="[
        { title: '文件名', key: 'name', sortable: false },
        { title: '状态', key: 'status', width: 130 },
        { title: '原因/分数', key: 'reason', sortable: false },
        { title: '重试', key: 'retries', width: 70 },
        { title: '大小', key: 'size', width: 90 },
        { title: '修改时间', key: 'mtime', width: 180 },
        { title: '操作', key: 'actions', sortable: false, width: 90 }
      ]"
      :items="rows"
      :loading="loading"
      items-per-page-text="每页条数"
      :items-per-page-options="[20, 50, 100]"
      :page-text="pageText"
      :items-per-page="limit"
      density="compact"
      class="elevation-1"
      @update:page="onPage(($event - 1) * limit)"
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
        <v-btn
          size="x-small"
          variant="text"
          color="primary"
          :loading="busy"
          @click="retryRow(item)"
        >重试</v-btn>
      </template>
      <template #no-data>
        <div class="text-center pa-6 text-grey">该桶暂无数据</div>
      </template>
    </v-data-table>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3000" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </div>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.plugin-root .text-truncate {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
