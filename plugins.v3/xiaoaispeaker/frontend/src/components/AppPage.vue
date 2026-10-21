<script setup lang="ts">
import { ref, computed, inject, onMounted, onUnmounted } from 'vue'
import ConfigForm from './ConfigForm.vue'
import {
  type Status,
  type LoginResult,
  type Browse,
  type Search,
  type Device,
  type FileRow,
  statusColor,
  fmtSeconds,
  fmtSize,
  fmtTime,
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
  navKey: { type: String, default: 'main' },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' }
})

// 侧栏全页宿主只接 action 这一个事件（见联邦组件规范 5.4），配置就在本页的
// 「配置」栏里改，不需要向宿主请求切换。
const emit = defineEmits(['action'])

const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`)
const toast = inject<HostToast | null>('moviepilot:toast', null)

const EMPTY_STATUS: Status = {
  enabled: false,
  version: '',
  logged_in: false,
  token_saved_at: '',
  token_expired: false,
  manual_token: false,
  needs: [],
  notices: [],
  devices: [],
  default_device: null,
  playing: {
    device_id: '', device_name: '', status: -1, status_text: '未知', volume: null,
    position: null, duration: null, title: '', path: '', url: '', source: '',
    updated_at: '', error: ''
  },
  queue: { device_id: '', index: -1, total: 0, items: [], auto_next: false, source: '' },
  config: {
    enabled: false, show_sidebar_nav: true, username: '', password_set: false,
    default_device_id: '', music_dirs: '', dir_count: 0, missing_dirs: [],
    base_url: '', base_url_effective: '', base_url_source: '', play_mode: 'auto',
    audio_exts: '', audio_id: '', stream_ttl: 3600, default_volume: 0,
    auto_next: true, keep_light: false, tts_announce: false
  },
  extra: {}
}

const status = ref<Status>({ ...EMPTY_STATUS })
const loading = ref(false)
const busy = ref<string>('')
const snackbar = ref({ show: false, text: '', color: 'info' })

type Pane = 'play' | 'library' | 'account' | 'config'
const pane = ref<Pane>('play')

// ---------------- 读取 ----------------

const cfg = computed(() => status.value.config)
const playing = computed(() => status.value.playing)
const queue = computed(() => status.value.queue)
const devices = computed<Device[]>(() => status.value.devices || [])
const needs = computed(() => status.value.needs || [])
const notices = computed(() => status.value.notices || [])

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
    if (!volumeTouched.value) {
      volume.value = playing.value.volume ?? cfg.value.default_volume ?? 50
    }
  } catch (e: any) {
    if (showError) notify(`状态加载失败：${e?.message || e}`, 'error')
  }
}

// 后端对播放状态有 3 秒缓存、对设备列表有 5 分钟缓存，前端轮询只要不藏页面时打就行：
// 音箱是局域网里的共享设备，没人看的时候没必要一直替它轮询云端。
let timer: ReturnType<typeof setInterval> | null = null

function startPolling() {
  stopPolling()
  timer = setInterval(() => {
    if (document.hidden) return
    loadStatus(false)
  }, 5000)
}

function stopPolling() {
  if (timer) {
    clearInterval(timer)
    timer = null
  }
}

// ---------------- 播放 ----------------

const volume = ref(50)
const volumeTouched = ref(false)
const ttsText = ref('')

const progress = computed(() => {
  const position = playing.value.position
  const duration = playing.value.duration
  if (!position || !duration || duration <= 0) return 0
  return Math.min(100, (position / duration) * 100)
})

async function post(path: string, body: Record<string, any>, label: string) {
  busy.value = label
  try {
    const res = await (props.api as MoviePilotApi).post(`${base.value}${path}`, body)
    if (res && res.ok === false) {
      notify(res.message || res.error || `${label}失败`, 'warning')
    } else if (res && (res.message || res.ok === true)) {
      notify(res.message || `${label}已执行`)
    }
    await loadStatus(false)
    emit('action')
    return res
  } catch (e: any) {
    notify(`${label}失败：${e?.message || e}`, 'error')
    return null
  } finally {
    busy.value = ''
  }
}

function control(action: string) {
  return post('/control', { action }, '播放控制')
}

function setVolume() {
  volumeTouched.value = true
  return post('/volume', { volume: Math.round(volume.value) }, '设置音量')
}

function speak() {
  const text = ttsText.value.trim()
  if (!text) {
    notify('先写上要播报的内容', 'warning')
    return
  }
  return post('/tts', { text: text.slice(0, 200) }, '播报').then((res) => {
    if (res) ttsText.value = ''
  })
}

function playFile(row: FileRow) {
  return post('/play', { path: row.path }, '播放')
}

function playUrl() {
  const url = externalUrl.value.trim()
  if (!url) {
    notify('先粘贴一条音频直链', 'warning')
    return
  }
  return post('/play', { url }, '播放直链').then(() => { externalUrl.value = '' })
}

const externalUrl = ref('')

// 「加入队列」不另开接口：/queue 是整本替换，所以把现有队列取回来加上这一条再提交，
// 当前位置保持不变（队列为空时从 0 开始播）。
async function enqueue(path: string) {
  const items = (queue.value.items || []).map(i => i.path)
  if (items.includes(path)) {
    notify('这一首已经在队列里了', 'warning')
    return
  }
  await post('/queue', { paths: [...items, path], index: items.length ? queue.value.index : 0 }, '加入队列')
}

function queueDirectory() {
  const paths = browseFiles.value.map(f => f.path)
  if (!paths.length) {
    notify('这个目录里没有可播放的音频', 'warning')
    return
  }
  return post('/queue', { paths, index: 0 }, '整目录入队')
}

function jumpTo(index: number) {
  return post('/control', { action: String(index) }, '切曲')
}

function selectDevice(device: Device) {
  return post('/device/select', { device_id: device.device_id }, '设为默认')
}

function clearDefault() {
  return post('/device/select', { device_id: '' }, '取消默认音箱')
}

// ---------------- 登录 ----------------

const loginForm = ref({ username: '', password: '' })
const loginResult = ref<LoginResult | null>(null)
const verifyCode = ref('')
const manualToken = ref('')
const showManual = ref(false)

const loginPending = computed(() => {
  const state = loginResult.value?.state
  return state === 'need_captcha' || state === 'need_verify'
})

async function startLogin() {
  busy.value = '登录'
  try {
    const res: LoginResult = await (props.api as MoviePilotApi).post(`${base.value}/login`, {
      username: loginForm.value.username.trim(),
      password: loginForm.value.password
    })
    afterLogin(res)
  } catch (e: any) {
    notify(`登录请求失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

async function submitVerify() {
  const code = verifyCode.value.trim()
  if (!code) {
    notify('填写验证码', 'warning')
    return
  }
  busy.value = '提交验证码'
  try {
    const res: LoginResult = await (props.api as MoviePilotApi).post(`${base.value}/login`, { code })
    afterLogin(res)
  } catch (e: any) {
    notify(`验证码提交失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

async function submitManual() {
  const raw = manualToken.value.trim()
  if (!raw) {
    notify('把整串 cookie 粘进来（要包含 userId 与 serviceToken）', 'warning')
    return
  }
  busy.value = '校验票据'
  try {
    const res: LoginResult = await (props.api as MoviePilotApi).post(
      `${base.value}/login`, { manual_token: raw }
    )
    afterLogin(res)
  } catch (e: any) {
    notify(`票据校验失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

function afterLogin(res: LoginResult) {
  loginResult.value = res || null
  verifyCode.value = ''
  if (res?.state === 'success') {
    notify(res.message || '登录成功', 'success')
    loginForm.value.password = ''
    showManual.value = false
    manualToken.value = ''
    loadStatus(false)
    return
  }
  if (res?.state === 'need_captcha' || res?.state === 'need_verify') {
    notify(res.message || '需要验证码', 'warning')
    return
  }
  notify(res?.message || '登录失败', 'error')
}

async function doLogout() {
  const res = await post('/logout', {}, '退出登录')
  if (res) {
    loginResult.value = null
    loginForm.value = { username: loginForm.value.username, password: '' }
  }
}

async function refreshDevices() {
  busy.value = '刷新设备'
  try {
    const res = await (props.api as MoviePilotApi).get(`${base.value}/devices`)
    if (res?.message && !res?.devices?.length) notify(`设备刷新失败：${res.message}`, 'warning')
    else notify(`设备列表已刷新，共 ${(res?.devices || []).length} 台`)
    await loadStatus(false)
  } catch (e: any) {
    notify(`设备刷新失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

// ---------------- 音乐库 ----------------

const browse = ref<Browse>({ cwd: '', parent: '', roots: [], dirs: [], files: [], reason: '', truncated: false })
const search = ref<Search>({ keyword: '', items: [], reason: '' })
const mode = ref<'browse' | 'search'>('browse')
const keyword = ref('')
let searchTimer: ReturnType<typeof setTimeout> | null = null

const browseFiles = computed(() => (mode.value === 'search' ? search.value.items : browse.value.files))
const queueAllLabel = computed(() => (mode.value === 'search' ? '搜索结果入队并播放' : '整目录入队并播放'))

const BROWSE_REASON: Record<string, string> = {
  no_dirs: '还没有配置「音乐库路径」，先在配置里填上目录',
  not_dir: '这不是一个目录，或者它已经不存在了',
  unresolvable: '路径解析不了（软链或权限问题）',
  out_of_library: '这个目录不在配置的音乐库范围内'
}

// "truncated" 不是错误：条目多到配额上限时后端把它同时写进 reason 与 truncated，
// 这里只留「只列出了前面一部分」那条提示，不再重复显示一个英文码。
const browseReasonText = computed(() => {
  const reason = browse.value.reason || ''
  if (!reason || reason === 'truncated') return ''
  return BROWSE_REASON[reason] || reason
})

const SEARCH_REASON: Record<string, string> = {
  no_dirs: '还没有配置「音乐库路径」',
  no_hits: '库里没有文件名匹配这个关键词的音频'
}

async function loadBrowse(path: string) {
  try {
    browse.value = await (props.api as MoviePilotApi).get(`${base.value}/browse${query({ path })}`)
  } catch (e: any) {
    notify(`目录加载失败：${e?.message || e}`, 'error')
  }
}

async function runSearch() {
  const text = keyword.value.trim()
  if (!text) {
    search.value = { keyword: '', items: [], reason: '' }
    return
  }
  try {
    search.value = await (props.api as MoviePilotApi).get(`${base.value}/search${query({ keyword: text })}`)
  } catch (e: any) {
    notify(`搜索失败：${e?.message || e}`, 'error')
  }
}

function onKeyword() {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(runSearch, 350)
}

async function switchMode(next: 'browse' | 'search') {
  mode.value = next
  if (next === 'search' && keyword.value.trim()) await runSearch()
  if (next === 'browse' && !browse.value.cwd) await loadBrowse('')
}

onMounted(async () => {
  await loadStatus()
  await loadBrowse('')
  startPolling()
})

onUnmounted(stopPolling)

const paneItems: { key: Pane; title: string; icon: string; desc: string }[] = [
  { key: 'play', title: '播放台', icon: 'mdi-speaker', desc: '当前播放、队列连播、音量与播报' },
  { key: 'library', title: '音乐库', icon: 'mdi-folder-music-outline', desc: '浏览库内目录与搜索文件名，点一首直接推到音箱' },
  { key: 'account', title: '账号与设备', icon: 'mdi-account-circle-outline', desc: '小米账号登录、音箱列表与默认音箱' },
  { key: 'config', title: '配置', icon: 'mdi-cog-outline', desc: '音乐库范围、外部访问地址与播放接口' }
]

const currentPane = computed(() => paneItems.find(p => p.key === pane.value) || paneItems[0])
</script>

<template>
  <v-card flat class="xai-page">
    <div class="xai-head">
      <v-avatar color="primary" variant="tonal" rounded="lg" size="34" class="mr-3">
        <v-icon icon="mdi-speaker" size="22" />
      </v-avatar>
      <div class="xai-head-text">
        <div class="text-h6">小爱音箱</div>
        <div class="text-caption text-medium-emphasis">
          库内音频直链推送到小爱音箱 · v{{ status.version || '-' }}
        </div>
      </div>
      <v-spacer />
      <v-chip size="small" variant="tonal" class="mr-2"
              :color="status.token_expired ? 'error' : status.logged_in ? 'success' : 'warning'"
              :prepend-icon="status.logged_in ? 'mdi-check-circle-outline' : 'mdi-alert-circle-outline'">
        {{ status.token_expired ? '票据已过期' : status.logged_in ? '已登录' : '未登录' }}
      </v-chip>
      <v-chip size="small" variant="tonal" class="mr-2"
              :color="statusColor(playing.status)"
              :prepend-icon="playing.status === 1 ? 'mdi-play-circle-outline' : 'mdi-pause-circle-outline'">
        {{ playing.device_name || '无设备' }} · {{ playing.status_text }}
      </v-chip>
      <v-btn size="small" variant="text" class="text-none" :loading="loading || !!busy"
             prepend-icon="mdi-refresh" @click="loadStatus()">刷新</v-btn>
    </div>
    <v-divider />

    <v-alert v-if="needs.length || notices.length" type="info" variant="tonal"
             density="compact" class="ma-3">
      <div v-for="item in needs" :key="'n' + item" class="text-body-2">
        <v-icon icon="mdi-form-select" size="14" class="mr-1" />{{ item }}
      </div>
      <div v-for="item in notices" :key="'m' + item" class="text-caption text-grey-darken-1">
        {{ item }}
      </div>
    </v-alert>

    <div class="xai-body">
      <nav class="xai-nav">
        <v-list density="comfortable" nav class="py-2">
          <v-list-item
            v-for="item in paneItems"
            :key="item.key"
            :active="pane === item.key"
            color="primary"
            rounded="lg"
            class="xai-nav-item"
            @click="pane = item.key"
          >
            <template #prepend><v-icon :icon="item.icon" class="mr-2" /></template>
            <v-list-item-title>{{ item.title }}</v-list-item-title>
          </v-list-item>
        </v-list>
      </nav>

      <section class="xai-content">
        <div class="xai-pane-title">{{ currentPane.desc }}</div>

        <!-- 播放台 -->
        <div v-show="pane === 'play'" class="xai-pane">
          <v-card variant="outlined" class="pa-4">
            <div class="d-flex align-center" style="gap: 12px; flex-wrap: wrap">
              <v-icon :icon="playing.status === 1 ? 'mdi-volume-high' : 'mdi-volume-variant-off'"
                      size="34" :color="statusColor(playing.status)" />
              <div style="min-width: 0; flex: 1 1 240px">
                <div class="text-subtitle-1 text-truncate" :title="playing.title || playing.path">
                  {{ playing.title || playing.path || playing.url || '音箱上没有在放的东西' }}
                </div>
                <div class="text-caption text-grey">
                  {{ playing.device_name || '未选择设备' }}
                  <template v-if="playing.source">｜来源 {{ playing.source }}</template>
                  <template v-if="playing.updated_at">｜{{ playing.updated_at }}</template>
                </div>
                <div v-if="playing.error" class="text-caption text-error">{{ playing.error }}</div>
              </div>
              <div class="text-right">
                <div class="text-caption text-grey">{{ fmtSeconds(playing.position) }} / {{ fmtSeconds(playing.duration) }}</div>
                <v-chip size="x-small" variant="tonal" :color="statusColor(playing.status)">{{ playing.status_text }}</v-chip>
              </div>
            </div>
            <v-progress-linear :model-value="progress" height="4" color="primary" class="mt-3" rounded />
            <div class="d-flex align-center mt-3" style="gap: 6px; flex-wrap: wrap">
              <v-btn size="small" variant="tonal" prepend-icon="mdi-previous" :loading="busy === '播放控制'"
                     @click="control('prev')">上一首</v-btn>
              <v-btn size="small" color="primary" variant="flat" :icon="playing.status === 1 ? 'mdi-pause' : 'mdi-play'"
                     :loading="busy === '播放控制'" @click="control(playing.status === 1 ? 'pause' : 'play')" />
              <v-btn size="small" variant="tonal" append-icon="mdi-skip-next" :loading="busy === '播放控制'"
                     @click="control('next')">下一首</v-btn>
              <v-btn size="small" variant="tonal" color="error" prepend-icon="mdi-stop"
                     :loading="busy === '播放控制'" @click="control('stop')">停止</v-btn>
              <v-spacer />
              <span class="text-caption text-grey">音量</span>
              <v-slider v-model="volume" min="0" max="100" step="1" hide-details density="compact"
                        style="max-width: 160px" @update:model-value="volumeTouched = true" />
              <v-btn size="small" variant="tonal" :loading="busy === '设置音量'" @click="setVolume">设为
                {{ Math.round(volume) }}</v-btn>
            </div>
          </v-card>

          <div class="xai-block">
            <div class="xai-block-title">
              播放队列
              <v-chip size="x-small" variant="tonal" class="ml-2"
                      :color="queue.auto_next ? 'success' : 'grey'">
                {{ queue.auto_next ? '自动连播开' : '自动连播关' }}
              </v-chip>
              <span class="text-caption text-grey ml-2">{{ queue.total }} 首</span>
            </div>
            <v-alert v-if="!cfg.auto_next" type="warning" variant="tonal" density="compact" class="mb-2">
              配置里的「队列自动连播」是关的，音箱播完当前这首不会自己接下一首（这里的手动「下一首」仍然有效）。
            </v-alert>
            <v-card v-if="queue.total" variant="outlined" class="xai-queue">
              <div v-for="(item, i) in queue.items" :key="item.path" class="xai-queue-row"
                   :class="{ 'xai-queue-current': i === queue.index }" @click="jumpTo(i)">
                <span class="xai-queue-index">{{ i + 1 }}</span>
                <span class="xai-queue-name" :title="item.path">{{ item.title || item.name }}</span>
                <span class="text-caption text-grey">{{ fmtSize(item.size) }}</span>
                <v-icon v-if="i === queue.index" icon="mdi-volume-high" size="16" color="primary" />
              </div>
            </v-card>
            <div v-else class="text-caption text-grey">
              队列为空。到「音乐库」里点一首的「播放」或「加入队列」，也可以整目录入队。
            </div>
          </div>

          <div class="xai-block">
            <div class="xai-block-title">播报与外部直链</div>
            <div class="d-flex" style="gap: 8px; flex-wrap: wrap">
              <v-text-field v-model="ttsText" density="compact" hide-details clearable
                            placeholder="让音箱说一句话（≤200 字）" style="min-width: 240px; flex: 1 1 260px"
                            @keyup.enter="speak" />
              <v-btn size="small" variant="tonal" color="info" :loading="busy === '播报'" @click="speak">播报</v-btn>
            </div>
            <div class="d-flex mt-2" style="gap: 8px; flex-wrap: wrap">
              <v-text-field v-model="externalUrl" density="compact" hide-details clearable
                            placeholder="http(s)://…/song.mp3（音箱能访问到的直链）"
                            style="min-width: 240px; flex: 1 1 260px" @keyup.enter="playUrl" />
              <v-btn size="small" variant="tonal" :loading="busy === '播放直链'" @click="playUrl">放这条</v-btn>
            </div>
            <div class="text-caption text-grey mt-1">
              库内文件走的是插件签发的限时直链；外部直链由音箱自己访问，插件不转发内容。
              播报失败时音箱可能已经播了（小米的 TTS 接口对同一句会返回 3012）。
            </div>
          </div>
        </div>

        <!-- 音乐库 -->
        <div v-show="pane === 'library'" class="xai-pane">
          <div class="d-flex align-center mb-2" style="gap: 8px; flex-wrap: wrap">
            <v-btn-toggle v-model="mode" density="compact" mandatory
                          @update:model-value="switchMode($event)">
              <v-btn value="browse" size="small" variant="text" prepend-icon="mdi-folder-outline">浏览</v-btn>
              <v-btn value="search" size="small" variant="text" prepend-icon="mdi-magnify">搜索</v-btn>
            </v-btn-toggle>
            <v-text-field v-if="mode === 'search'" v-model="keyword" density="compact" hide-details clearable
                          prepend-inner-icon="mdi-magnify" placeholder="按文件名搜索库内音频"
                          style="max-width: 320px" @update:model-value="onKeyword" />
            <v-spacer />
            <v-btn size="small" variant="tonal" color="primary" :disabled="!browseFiles.length"
                   :loading="busy === '整目录入队'" @click="queueDirectory">{{ queueAllLabel }}</v-btn>
          </div>

          <template v-if="mode === 'browse'">
            <div class="d-flex align-center flex-wrap gap-1 mb-2">
              <v-chip v-for="root in browse.roots" :key="root" size="x-small" variant="outlined"
                      class="cursor-pointer" prepend-icon="mdi-harddisk" @click="loadBrowse(root)">
                {{ root }}
              </v-chip>
              <v-btn v-if="browse.parent" size="x-small" variant="text" prepend-icon="mdi-arrow-up"
                     @click="loadBrowse(browse.parent)">上级</v-btn>
              <span class="text-caption text-grey">{{ browse.cwd }}</span>
            </div>

            <div v-if="browseReasonText" class="text-caption text-grey">{{ browseReasonText }}</div>

            <v-card v-if="browse.dirs.length" variant="outlined" class="pa-2 mb-2">
              <div v-for="dir in browse.dirs" :key="dir.path" class="xai-dir" @click="loadBrowse(dir.path)">
                <v-icon icon="mdi-folder-outline" size="16" class="mr-2" />{{ dir.name }}
              </div>
            </v-card>

            <v-alert v-if="browse.truncated" type="info" variant="tonal" density="compact" class="mb-2">
              这个目录里的条目太多，只列出了前面一部分。
            </v-alert>
          </template>

          <v-table v-if="browseFiles.length" density="compact" class="xai-table">
              <thead>
                <tr>
                  <th style="width: 38%">文件名</th>
                  <th>大小</th>
                  <th>修改时间</th>
                  <th style="width: 190px">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in browseFiles" :key="row.path">
                  <td>
                    <div class="text-truncate" :title="row.path">{{ row.name }}</div>
                    <div v-if="row.title" class="text-caption text-grey text-truncate">{{ row.title }}</div>
                  </td>
                  <td class="text-caption">{{ fmtSize(row.size) }}</td>
                  <td class="text-caption">{{ fmtTime(row.mtime) }}</td>
                  <td>
                    <v-btn size="x-small" variant="text" color="primary" :loading="busy === '播放'"
                           @click="playFile(row)">播放</v-btn>
                    <v-btn size="x-small" variant="text" :loading="busy === '加入队列'"
                           @click="enqueue(row.path)">加入队列</v-btn>
                  </td>
                </tr>
              </tbody>
            </v-table>
          <div v-else class="text-caption text-grey">
            {{ mode === 'search'
               ? (search.reason ? (SEARCH_REASON[search.reason] || search.reason) : '输入关键词后，在库内文件名里搜索。')
               : (browseReasonText || '这个目录里没有可播放的音频。') }}
          </div>
          <div class="text-caption text-grey mt-2">
            只有配置在「音乐库路径」里的目录会被列出；播放走插件本机读文件 + 签名限时直链，
            不需要音箱登录任何音乐服务。
          </div>
        </div>

        <!-- 账号与设备 -->
        <div v-show="pane === 'account'" class="xai-pane">
          <v-card variant="outlined" class="pa-4 mb-3">
            <div class="xai-block-title">小米账号</div>
            <div v-if="status.logged_in" class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
              <div class="text-body-2" style="flex: 1 1 220px">
                {{ cfg.username || '（未记录账号名）' }}
                <div class="text-caption text-grey">
                  票据保存于 {{ status.token_saved_at || '-' }}
                  <template v-if="status.manual_token">｜手工粘贴的 cookie</template>
                  <template v-if="status.token_expired">｜已过期，需要重新登录</template>
                </div>
              </div>
              <v-btn size="small" variant="tonal" :loading="busy === '刷新设备'"
                    prepend-icon="mdi-refresh" @click="refreshDevices">刷新设备</v-btn>
              <v-btn size="small" variant="tonal" color="error" :loading="busy === '退出登录'"
                     prepend-icon="mdi-logout" @click="doLogout">退出登录</v-btn>
            </div>

            <template v-else>
              <div class="d-flex" style="gap: 8px; flex-wrap: wrap">
                <v-text-field v-model="loginForm.username" density="compact" hide-details clearable
                              label="小米账号（手机号 / 邮箱 / 账号ID）" :placeholder="cfg.username"
                              style="min-width: 220px; flex: 1 1 220px" />
                <v-text-field v-model="loginForm.password" density="compact" hide-details
                              :type="loginForm.password ? 'text' : 'password'"
                              :label="cfg.password_set ? '密码（留空则用已保存的）' : '密码'"
                              style="min-width: 180px; flex: 1 1 180px" @keyup.enter="startLogin" />
                <v-btn color="primary" variant="flat" class="text-none" :loading="busy === '登录'"
                       prepend-icon="mdi-login" @click="startLogin">登录</v-btn>
              </div>

              <div v-if="loginPending" class="mt-3">
                <v-img v-if="loginResult?.captcha_image" :src="loginResult.captcha_image" max-height="120"
                       class="rounded mb-2" style="max-width: 240px" contain />
                <div class="d-flex" style="gap: 8px; flex-wrap: wrap">
                  <v-text-field v-model="verifyCode" density="compact" hide-details
                                :label="loginResult?.state === 'need_verify' ? '邮箱/短信验证码' : '图形验证码'"
                                style="max-width: 220px" @keyup.enter="submitVerify" />
                  <v-btn size="small" variant="tonal" color="info" :loading="busy === '提交验证码'"
                         @click="submitVerify">提交</v-btn>
                  <v-btn size="small" variant="text" @click="startLogin">重新发起登录</v-btn>
                </div>
              </div>
              <div v-else-if="loginResult" class="text-caption text-grey mt-2">{{ loginResult.message }}</div>

              <v-expand-transition>
                <div v-if="showManual" class="mt-3">
                  <v-textarea v-model="manualToken" :rows="3" density="compact" hide-details
                              placeholder="userId=…; serviceToken=…; 把浏览器里 i.api.xiaomi.com 登录后的整串 cookie 粘进来" />
                  <div class="d-flex mt-1" style="gap: 8px">
                    <v-btn size="small" variant="tonal" :loading="busy === '校验票据'"
                           @click="submitManual">校验并使用</v-btn>
                    <v-btn size="small" variant="text" @click="showManual = false">取消</v-btn>
                  </div>
                </div>
              </v-expand-transition>
              <div class="text-caption text-grey mt-2">
                账密登不上（小米风控、二次校验过不去）时可以在高级里直接粘贴 cookie 票据；
                票据只存在本机插件数据里，配置页不回显明文。
                <v-btn size="x-small" variant="text" class="text-none ml-1"
                       :prepend-icon="showManual ? 'mdi-chevron-up' : 'mdi-chevron-down'"
                       @click="showManual = !showManual">高级</v-btn>
              </div>
            </template>
          </v-card>

          <div class="xai-block-title">
            音箱设备
            <span class="text-caption text-grey ml-2">{{ devices.length }} 台</span>
          </div>
          <v-alert v-if="!devices.length" type="warning" variant="tonal" density="compact">
            {{ status.logged_in
               ? '设备列表是空的：可能票据已失效，点上面的「刷新设备」重试。'
               : '登录小米账号后才能看到音箱。' }}
          </v-alert>
          <v-card v-for="device in devices" :key="device.device_id" variant="outlined" class="pa-3 mb-2">
            <div class="d-flex align-center" style="gap: 10px; flex-wrap: wrap">
              <v-icon :icon="device.presence === 'online' ? 'mdi-speaker' : 'mdi-speaker-off'"
                      :color="device.presence === 'online' ? 'success' : 'grey'" />
              <div style="min-width: 0; flex: 1 1 200px">
                <div class="text-body-2">
                  {{ device.name || device.alias || device.device_id }}
                  <v-chip v-if="device.is_default" size="x-small" variant="flat" color="primary" class="ml-1">默认</v-chip>
                  <v-chip v-if="device.alias && device.name" size="x-small" variant="tonal" class="ml-1">{{ device.alias }}</v-chip>
                </div>
                <div class="text-caption text-grey">
                  {{ device.model || device.hardware || '未知型号' }}｜{{ device.presence === 'online' ? '在线' : '离线' }}
                  <template v-if="device.needs_music_api">｜该型号需走 play_music，直链可能被当成电台播放</template>
                </div>
              </div>
              <v-btn size="x-small" variant="text" color="primary" :disabled="device.is_default"
                     :loading="busy === '设为默认'" @click="selectDevice(device)">设为默认</v-btn>
              <v-btn size="x-small" variant="text" :disabled="!cfg.default_device_id"
                     :loading="busy === '取消默认'" @click="clearDefault">取消默认</v-btn>
            </div>
          </v-card>
          <div class="text-caption text-grey">
            没设默认音箱时，操作落在第一台在线设备上；都不在线就落在列表第一台。
            设备列表由插件缓存 5 分钟，改动账号或新增音箱后点「刷新设备」。
          </div>
        </div>

        <!-- 配置：v-if 而非 v-show，这一栏不在时才不发那次 /status -->
        <div v-if="pane === 'config'" class="xai-pane xai-pane-config">
          <ConfigForm :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                      @saved="loadStatus()" />
        </div>
      </section>
    </div>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.xai-page { width: 100%; }
.xai-head { display: flex; align-items: center; gap: 8px; padding: 12px 16px; }
.xai-head-text { min-width: 0; }
.xai-body { display: flex; min-height: 440px; }
.xai-nav {
  flex: 0 0 168px;
  border-right: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  background: rgba(var(--v-theme-on-surface), .02);
}
.xai-nav-item { margin: 2px 8px; }
.xai-content { flex: 1 1 auto; min-width: 0; overflow-y: auto; }
.xai-pane { padding: 12px 16px; }
.xai-pane-config { padding: 0; }
.xai-pane-title { font-size: 12px; color: rgba(var(--v-theme-on-surface), .6); margin-bottom: 10px; }
.xai-block { margin-top: 16px; }
.xai-block-title { font-size: 13px; font-weight: 600; margin-bottom: 6px; opacity: .8; }
.xai-queue { padding: 4px 6px; max-height: 320px; overflow-y: auto; }
.xai-queue-row {
  display: flex; align-items: center; gap: 8px; padding: 4px 8px; border-radius: 6px; cursor: pointer;
}
.xai-queue-row:hover { background: rgba(var(--v-theme-on-surface), .05); }
.xai-queue-current { background: rgba(var(--v-theme-primary), .12); }
.xai-queue-index { flex: 0 0 22px; font-size: 12px; color: rgba(var(--v-theme-on-surface), .6); }
.xai-queue-name { flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 13px; }
.xai-dir { display: flex; align-items: center; padding: 3px 6px; border-radius: 6px; cursor: pointer; font-size: 13px; }
.xai-dir:hover { background: rgba(var(--v-theme-on-surface), .05); }
.xai-table { font-size: 13px; }
.xai-table .text-truncate { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cursor-pointer { cursor: pointer; }
@media (max-width: 760px) {
  .xai-body { flex-direction: column; }
  .xai-nav {
    flex: 0 0 auto;
    border-right: none;
    border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  }
}
</style>
