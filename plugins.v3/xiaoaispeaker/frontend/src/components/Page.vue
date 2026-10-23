<script setup lang="ts">
import { ref, computed, inject, onMounted, onUnmounted } from 'vue'
import {
  type Status,
  type Device,
  fmtSeconds,
  fmtSize,
  query
} from './types'

interface HostToast {
  error(msg: string): unknown
  info(msg: string): unknown
  success(msg: string): unknown
  warning(msg: string): unknown
}

const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  nativeSubscribe: { type: Function as any, default: null },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  showSwitch: { type: Boolean, default: true }
})

const emit = defineEmits(['action', 'switch', 'close', 'layout'])

const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`)
const toast = inject<HostToast | null>('moviepilot:toast', null)

const status = ref<Status | null>(null)
const busy = ref<string>('')
const snackbar = ref({ show: false, text: '', color: 'info' })
const volume = ref(50)

const playing = computed(() => status.value?.playing)
const queue = computed(() => status.value?.queue)
const devices = computed<Device[]>(() => status.value?.devices || [])
const cfg = computed(() => status.value?.config)

function notify(msg: string, color = 'info') {
  if (toast) {
    const fn = (toast as any)[color] || toast.info
    fn.call(toast, msg)
    return
  }
  snackbar.value = { show: true, text: msg, color }
}

async function loadStatus(showError = true) {
  try {
    status.value = await (props.api as MoviePilotApi).get(`${base.value}/status`)
    if (playing.value?.volume !== null && playing.value?.volume !== undefined) {
      volume.value = playing.value.volume
    }
  } catch (e: any) {
    if (showError) notify(`状态加载失败：${e?.message || e}`, 'error')
  }
}

async function post(path: string, body: Record<string, any>, label: string) {
  busy.value = label
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}${path}`, body)
    if (res && res.ok === false) notify(res.message || res.error || `${label}失败`, 'warning')
    else if (res?.message) notify(res.message)
    await loadStatus(false)
    emit('action')
  } catch (e: any) {
    notify(`${label}失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

function control(action: string) {
  return post('/control', { action }, '播放控制')
}

function setVolume() {
  return post('/volume', { volume: Math.round(volume.value) }, '设置音量')
}

function selectDevice(device: Device) {
  return post('/device/select', { device_id: device.device_id }, '设为默认')
}

async function playKeyword() {
  const text = keyword.value.trim()
  if (!text) {
    notify('输入曲名关键词（库内文件名搜索，命中第一个直接播）', 'warning')
    return
  }
  // /play 的 keyword 分支后端自己搜库内第一个命中；这里先探一次，把命中的文件名报给用户
  try {
    const hits = await (props.api as MoviePilotApi).get(`${base.value}/search${query({ keyword: text })}`)
    if (!hits?.items?.length) {
      notify(`库里没找到「${text}」`, 'warning')
      return
    }
  } catch {
    // 探测失败不挡住播放：/play 自己还会再搜一遍
  }
  await post('/play', { keyword: text }, '播放')
  keyword.value = ''
}

const keyword = ref('')

let timer: ReturnType<typeof setInterval> | null = null

onMounted(async () => {
  // 详情页是宿主对话框里的窄容器，宽度交给宿主按这个事件设置
  emit('layout', { maxWidth: '46rem' })
  await loadStatus()
  timer = setInterval(() => {
    if (!document.hidden) loadStatus(false)
  }, 6000)
})

onUnmounted(() => {
  if (timer) clearInterval(timer)
})
</script>

<template>
  <v-card flat class="xai-detail">
    <div class="xai-head">
      <span class="xai-mark">
        <v-icon :icon="playing?.status === 1 ? 'mdi-speaker-wireless' : 'mdi-speaker'" size="18" />
      </span>
      <div class="xai-head-text">
        <div class="xai-head-title">小爱音箱</div>
        <div class="xai-head-sub">
          <template v-if="status?.logged_in">{{ cfg?.username || '已登录' }}｜{{ devices.length }} 台设备</template>
          <template v-else>未登录：到侧栏「账号与设备」登录小米账号</template>
        </div>
      </div>
      <v-spacer />
      <span class="xai-pill" :class="playing?.status === 1 ? 'xai-pill-ok' : 'xai-pill-idle'">
        {{ playing?.status_text || '未知' }}
      </span>
      <v-btn v-if="status?.token_expired" size="x-small" variant="text" color="error"
             class="ml-1 text-none">票据过期</v-btn>
      <v-btn v-if="props.showSwitch" icon="mdi-cog-outline" size="x-small" variant="text"
             @click="emit('switch')" />
      <v-btn icon="mdi-refresh" size="x-small" variant="text" :loading="!!busy"
             @click="loadStatus()" />
    </div>

    <div class="xai-body">
      <v-alert v-for="item in (status?.needs || [])" :key="item" type="warning" variant="tonal"
               density="compact" class="mb-2">
        {{ item }}
      </v-alert>

      <v-card variant="flat" class="xai-hero pa-4">
        <div class="xai-hero-title text-truncate" :title="playing?.title || playing?.path">
          {{ playing?.title || playing?.path || playing?.url || '当前没有在播放' }}
        </div>
        <div class="text-caption text-grey">
          {{ playing?.device_name || '无设备' }}｜{{ playing?.status_text || '未知' }}
          <template v-if="playing?.volume !== null && playing?.volume !== undefined">
            ｜音量 {{ playing?.volume }}
          </template>
          <template v-if="playing?.position !== null && playing?.position !== undefined">
            ｜{{ fmtSeconds(playing?.position) }} / {{ fmtSeconds(playing?.duration) }}
          </template>
        </div>
        <div v-if="playing?.error" class="text-caption text-error">{{ playing.error }}</div>
        <div class="d-flex align-center mt-3" style="gap: 10px; flex-wrap: wrap">
          <div class="xai-transport">
            <v-btn size="x-small" variant="text" icon="mdi-previous-variant"
                   :loading="busy === '播放控制'" @click="control('prev')" />
            <v-btn class="xai-transport-main" color="primary" variant="flat" rounded="circle"
                   :icon="playing?.status === 1 ? 'mdi-pause' : 'mdi-play'"
                   :loading="busy === '播放控制'" @click="control(playing?.status === 1 ? 'pause' : 'play')" />
            <v-btn size="x-small" variant="text" icon="mdi-skip-next-circle-outline"
                   :loading="busy === '播放控制'" @click="control('next')" />
            <v-btn size="x-small" variant="text" color="error" icon="mdi-stop-circle-outline"
                   :loading="busy === '播放控制'" @click="control('stop')" />
          </div>
          <div class="xai-volume">
            <v-icon icon="mdi-volume-low" size="15" class="mr-1" />
            <v-slider v-model="volume" min="0" max="100" step="1" hide-details density="compact" />
            <v-btn size="x-small" variant="text" :loading="busy === '设置音量'" @click="setVolume">设音量</v-btn>
          </div>
        </div>
      </v-card>

      <div class="xai-row">
        <v-text-field v-model="keyword" density="compact" hide-details clearable
                      prepend-inner-icon="mdi-magnify" placeholder="按曲名播库内第一首"
                      style="min-width: 200px; flex: 1 1 200px" @keyup.enter="playKeyword" />
        <v-btn size="small" variant="tonal" :loading="busy === '播放'" @click="playKeyword">播放</v-btn>
      </div>

      <div class="xai-title">
        队列
        <span class="text-caption text-grey">{{ queue?.total || 0 }} 首</span>
        <v-chip size="x-small" variant="tonal" :color="queue?.auto_next ? 'success' : 'grey'">
          {{ queue?.auto_next ? '连播开' : '连播关' }}
        </v-chip>
      </div>
      <v-card v-if="queue?.total" variant="outlined" class="xai-queue">
        <div v-for="(item, i) in (queue?.items || [])" :key="item.path"
             class="xai-queue-row" :class="{ 'xai-queue-current': i === queue?.index }"
             @click="control(String(i))">
          <span class="xai-queue-index">{{ i + 1 }}</span>
          <span class="xai-queue-name" :title="item.path">{{ item.title || item.name }}</span>
          <span class="text-caption text-grey">{{ fmtSize(item.size) }}</span>
        </div>
      </v-card>
      <div v-else class="text-caption text-grey">队列为空（侧栏页可以从音乐库目录或搜索结果建队列）。</div>

      <div class="xai-title">设备</div>
      <div v-if="!devices.length" class="text-caption text-grey">
        {{ status?.logged_in ? '没有取到设备，可在侧栏页刷新设备列表。' : '登录后才会列出音箱。' }}
      </div>
      <div v-for="device in devices" :key="device.device_id" class="xai-row">
        <v-icon :icon="device.presence === 'online' ? 'mdi-speaker' : 'mdi-speaker-off'" size="18"
                :color="device.presence === 'online' ? 'success' : 'grey'" />
        <span class="text-body-2 text-truncate" style="flex: 1 1 auto">{{ device.name || device.alias || device.device_id }}</span>
        <v-chip v-if="device.is_default" size="x-small" variant="flat" color="primary">默认</v-chip>
        <v-chip size="x-small" variant="tonal" :color="device.presence === 'online' ? 'success' : 'grey'">
          {{ device.presence === 'online' ? '在线' : '离线' }}
        </v-chip>
        <v-btn size="x-small" variant="text" :disabled="device.is_default"
               :loading="busy === '设为默认'" @click="selectDevice(device)">设为默认</v-btn>
      </div>

      <div class="text-caption text-grey mt-3">
        库内浏览、整目录入队与播报在侧栏「小爱音箱」页面；外部直链推送同样在那里。
      </div>
    </div>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；含 .v- 的选择器会被构建的 postcss 门禁整条删掉。配色取宿主变量。 */
.xai-detail {
  width: 100%;
  --xai-ink: rgba(var(--v-theme-on-surface), .92);
  --xai-ink-faint: rgba(var(--v-theme-on-surface), .5);
  --xai-line: rgba(var(--v-border-color), var(--v-border-opacity));
}
.xai-head {
  display: flex; align-items: center; gap: 10px; padding: 12px 16px;
  border-bottom: 1px solid var(--xai-line);
  background: linear-gradient(180deg, rgba(var(--v-theme-on-surface), .045), transparent);
}
.xai-mark {
  display: inline-flex; align-items: center; justify-content: center;
  width: 32px; height: 32px; border-radius: 10px; flex: 0 0 auto;
  color: rgb(var(--v-theme-primary)); background: rgba(var(--v-theme-primary), .14);
}
.xai-head-text { min-width: 0; line-height: 1.25; }
.xai-head-title { font-size: 14px; font-weight: 650; color: var(--xai-ink); }
.xai-head-sub { font-size: 11.5px; color: var(--xai-ink-faint); }
.xai-pill {
  display: inline-flex; align-items: center; height: 21px; padding: 0 9px;
  border-radius: 999px; font-size: 11.5px; font-weight: 500; white-space: nowrap;
  border: 1px solid transparent;
}
.xai-pill-ok { color: rgb(var(--v-theme-success)); background: rgba(var(--v-theme-success), .12); border-color: rgba(var(--v-theme-success), .2); }
.xai-pill-idle { color: var(--xai-ink-faint); background: rgba(var(--v-theme-on-surface), .06); border-color: var(--xai-line); }
.xai-body { padding: 14px 16px 16px; }
.xai-hero {
  border-radius: 14px; color: var(--xai-ink);
  background:
    radial-gradient(120% 140% at 0% 0%, rgba(var(--v-theme-primary), .15), transparent 62%),
    rgba(var(--v-theme-on-surface), .04);
  box-shadow: inset 0 0 0 1px var(--xai-line);
}
.xai-hero-title { font-size: 15px; font-weight: 650; }
.xai-transport {
  display: inline-flex; align-items: center; gap: 2px; padding: 2px;
  border-radius: 999px; background: rgba(var(--v-theme-on-surface), .06);
}
.xai-transport-main { width: 34px; height: 34px; box-shadow: 0 4px 12px rgba(var(--v-theme-primary), .28); }
.xai-volume {
  display: flex; align-items: center; flex: 1 1 180px; min-width: 160px; max-width: 320px;
  color: var(--xai-ink-faint);
}
.xai-row { display: flex; align-items: center; gap: 8px; margin-top: 8px; min-width: 0; }
.xai-title {
  display: flex; align-items: center; gap: 8px; margin: 18px 0 6px;
  font-size: 12px; font-weight: 650; letter-spacing: .3px; color: var(--xai-ink-faint);
}
.xai-queue {
  padding: 4px 6px; max-height: 220px; overflow-y: auto;
  border-radius: 14px; border: 1px solid var(--xai-line);
}
.xai-queue-row {
  display: flex; align-items: center; gap: 10px; padding: 6px 9px;
  border-radius: 9px; cursor: pointer; transition: background .14s ease;
}
.xai-queue-row:hover { background: rgba(var(--v-theme-on-surface), .06); }
.xai-queue-current {
  background: rgba(var(--v-theme-primary), .1);
  box-shadow: inset 2px 0 0 rgb(var(--v-theme-primary));
}
.xai-queue-index { flex: 0 0 20px; font-size: 11.5px; color: var(--xai-ink-faint); font-variant-numeric: tabular-nums; }
.xai-queue-name {
  flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  font-size: 13px; color: var(--xai-ink);
}
.xai-detail .text-truncate { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
@media (max-width: 760px) { .xai-volume { max-width: none; } }
</style>
