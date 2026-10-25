<script setup lang="ts">
import { ref, computed, inject, onMounted, onUnmounted, watch } from 'vue'
import Config from './Config.vue'
import {
  type Status,
  type Browse,
  type Search,
  type Device,
  type FileRow,
  type TimerState,
  type LibraryState,
  type VoiceState,
  type FavoriteResult,
  type FavoriteRow,
  type PlaylistResult,
  type PlaylistRow,
  type SongResult,
  type VoiceRecord,
  type GroupRow,
  type LyricResult,
  type TaskRow,
  type TaskLogRow,
  statusColor,
  fmtSeconds,
  fmtSize,
  fmtTime,
  modeText,
  MODE_ITEMS,
  activeLine,
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

// 侧栏全页只报动作给宿主（见联邦组件规范 5.4）：这一页只看运行情况，
// 配置与登录都在本页自己开的「插件配置」对话框里改。
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
  login_method: '',
  needs: [],
  notices: [],
  devices: [],
  default_device: null,
  playing: {
    device_id: '', device_name: '', status: -1, status_text: '未知', volume: null,
    position: null, duration: null, title: '', path: '', url: '', source: '',
    updated_at: '', error: ''
  },
  queue: {
    device_id: '', index: -1, total: 0, items: [], auto_next: false, source: '',
    mode: 'order'
  },
  config: {
    enabled: false, show_sidebar_nav: true, username: '', username_set: false, password_set: false,
    default_device_id: '', music_dirs: '', dir_count: 0, missing_dirs: [],
    base_url: '', base_url_effective: '', base_url_source: '', play_mode: 'auto',
    audio_exts: '', audio_id: '', stream_ttl: 3600, default_volume: 0,
    auto_next: true, keep_light: false, tts_announce: false,
    voice_enabled: false, voice_devices: '', voice_poll: 2, voice_interrupt: true,
    voice_hint_tts: false, voice_hint_text: '正在搜索，请稍候', voice_reply: true,
    voice_memory: true, ai_enabled: false, ai_chat: false, ai_timeout: 6,
    ai_ready: false, ai_provider: '', ai_model: '', ai_message: '',
    library_limit: 10000, library_refresh: 60
  },
  extra: {}
}

const status = ref<Status>({ ...EMPTY_STATUS })
const loading = ref(false)
const busy = ref<string>('')
const snackbar = ref({ show: false, text: '', color: 'info' })

type Pane = 'overview' | 'play' | 'library' | 'songs' | 'tasks' | 'voice'
const pane = ref<Pane>('overview')

// ---------------- 读取 ----------------

const cfg = computed(() => status.value.config)
const playing = computed(() => status.value.playing)
const queue = computed(() => status.value.queue)
const devices = computed<Device[]>(() => status.value.devices || [])
const onlineCount = computed(() => devices.value.filter(d => d.presence === 'online').length)
const needsMusicApiCount = computed(() => devices.value.filter(d => d.needs_music_api).length)
// 配置里那台可能已经不在本账号的设备列表里（换账号、音箱解绑），那时只剩裸 ID 可读，
// 总览里宁可写成「不在列表里」也别让人以为它还在
const defaultDeviceName = computed(() => {
  const id = cfg.value.default_device_id
  if (!id) return devices.value.find(d => d.presence === 'online')?.name
    || devices.value.find(d => d.presence === 'online')?.alias
    || '（没有在线音箱）'
  const hit = devices.value.find(d => d.device_id === id)
  return hit ? (hit.name || hit.alias || hit.device_id) : `${id}（不在当前设备列表里）`
})
const needs = computed(() => status.value.needs || [])
const notices = computed(() => status.value.notices || [])

// 顶栏那颗「最后同步」的相对时间：轮询每 5 秒动一次 syncedAt，读数就跟着走
const syncedAt = ref(0)
const syncText = computed(() => {
  if (!syncedAt.value) return '尚未同步'
  const diff = Math.max(0, Math.round((Date.now() - syncedAt.value) / 1000))
  if (diff < 5) return '刚刚同步'
  if (diff < 60) return `${diff} 秒前同步`
  return `${Math.round(diff / 60)} 分钟前同步`
})

// 睡眠定时、索引概况、监听概况都搭在 /status.extra 里一起回来：首屏与轮询都能看到，
// 不必为这三块各发一次请求。单独取详情时才走各自的端点。
// 兜底值只在 extra 里那一项缺席时用（后端还没升上来、或插件被关掉），不做二次合并。
const TIMER_FALLBACK: TimerState = {
  active: false, stop_at: '', stop_after_songs: 0, remaining: ''
}
const LIBRARY_FALLBACK: LibraryState = {
  ready: false, songs: 0, playlists: 0, built_at: '', scanned_at: '', seconds: 0,
  skipped: 0, truncated: false, error: '', roots: [], limit: 0, refresh_minutes: 0
}
const VOICE_FALLBACK: VoiceState = {
  ok: true, message: '', enabled: false, chat: false, chat_active: false,
  interval: 0, handled: 0, devices: [], commands: [], error: '', last_poll_at: '',
  records: [], matched: '', kind: '', argument: '', reply: ''
}

const timerState = computed<TimerState>(() => ({
  ...TIMER_FALLBACK, ...((status.value.extra?.timer as TimerState) || {})
}))
const library = computed<LibraryState>(() => ({
  ...LIBRARY_FALLBACK, ...((status.value.extra?.library as LibraryState) || {})
}))
const voice = computed<VoiceState>(() => ({
  ...VOICE_FALLBACK, ...((status.value.extra?.voice as VoiceState) || {})
}))

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
    syncedAt.value = Date.now()
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

// ---------------- 插件配置 ----------------

// 配置与登录都在这一个对话框里改（照豆瓣中心把「看运行」和「改配置」分开的做法）。
// 打开时停掉本页的 5 秒轮询：两处同时刷同一批状态没有意义，关掉时再补一次。
const configDialog = ref(false)

watch(configDialog, (open) => {
  if (open) {
    stopPolling()
    return
  }
  void loadStatus(false)
  startPolling()
})

function onConfigUpdated() {
  void loadStatus(false)
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

function playPath(path: string) {
  return post('/play', { path }, '播放')
}

function playFile(row: FileRow) {
  return playPath(row.path)
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

// ---------------- 歌词 ----------------

// /lyric 读的是库内文件，一次换歌读一遍就够，不跟着 5 秒一次的状态轮询反复读盘；
// 高亮行由前端按播放进度算（activeLine 与后端 lyrics.active_index 同一条规则）。
const lyric = ref<LyricResult | null>(null)
const lyricBusy = ref(false)
const showLyric = ref(true)
const lyricBox = ref<HTMLElement | null>(null)

const lyricTrack = computed(() => {
  const item = queue.value.items?.[queue.value.index]
  return item?.path || playing.value.path || ''
})

const lyricActive = computed(() => {
  const lines = lyric.value?.lines || []
  if (!lines.length || !lyric.value?.has_time) return -1
  return activeLine(lines, playing.value.position)
})

// 进度每 5 秒随状态轮询回来一次，行变了就把那行滚回中间。
watch(lyricActive, (index) => {
  if (index < 0) return
  const box = lyricBox.value
  const node = box?.children?.[index] as HTMLElement | undefined
  node?.scrollIntoView({ block: 'center', behavior: 'smooth' })
})

async function loadLyric(path: string) {
  if (!path) {
    lyric.value = null
    return
  }
  lyricBusy.value = true
  try {
    lyric.value = await (props.api as MoviePilotApi).get(`${base.value}/lyric${query({ path })}`)
  } catch (e: any) {
    notify(`歌词读取失败：${e?.message || e}`, 'error')
  } finally {
    lyricBusy.value = false
  }
}

watch(lyricTrack, (next) => {
  if (showLyric.value) void loadLyric(next)
})

function toggleLyric() {
  showLyric.value = !showLyric.value
  if (showLyric.value) void loadLyric(lyricTrack.value)
  else lyric.value = null
}

// ---------------- 设备分组（只读） ----------------

// 分组怎么建、哪台音箱进哪一组都是配置，在「插件配置」里改；这里只把它当成
// 「这一组会一起响」的运行情况列出来给人看，别在两个地方都能编辑。
const groups = computed<GroupRow[]>(() => (status.value.extra?.groups as GroupRow[]) || [])
const defaultGroup = computed<string>(() => String(status.value.extra?.default_group || ''))

// ---------------- 定时任务（运行情况） ----------------

// 任务怎么定义（做什么、哪几天、哪台音箱、播什么歌单）在「插件配置」里改；
// 这一页只回答「它跑没跑、下一次是什么时候」，外加启停与立即执行这两个动作。
const tasks = computed<TaskRow[]>(() => (status.value.extra?.tasks as TaskRow[]) || [])
const taskLogs = computed<TaskLogRow[]>(() => (status.value.extra?.task_logs as TaskLogRow[]) || [])
const enabledTasks = computed(() => tasks.value.filter(row => row.enabled).length)

function toggleTask(row: TaskRow) {
  return post('/tasks', { ...row, enabled: !row.enabled }, row.enabled ? '停用任务' : '启用任务')
}

function runTask(row: TaskRow) {
  return post('/tasks/run', { id: row.id }, '立即执行')
}

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

// ---------------- 播放方式、定时、收藏 ----------------

const modeItems = MODE_ITEMS
const currentMode = computed(() => queue.value.mode || 'order')

function setMode(mode: string) {
  return post('/mode', { mode }, '设置播放方式')
}

const timerMinutes = ref(30)
const timerSongs = ref(3)

function applyTimer(kind: 'minutes' | 'songs') {
  const raw = kind === 'minutes' ? timerMinutes.value : timerSongs.value
  const value = Math.floor(Number(raw) || 0)
  if (value <= 0) {
    notify(kind === 'minutes' ? '定时分钟数要大于 0' : '定时首数要大于 0', 'warning')
    return
  }
  return post('/timer', { [kind]: value }, '设置定时停止')
}

function cancelTimer() {
  return post('/timer', { cancel: true }, '取消定时停止')
}

// 收藏列表单独一次 GET 就够：它只在打开「歌单与收藏」时才用得上，
// /status.extra 里只带条数，避免每次轮询都翻一遍索引。
const favorites = ref<FavoriteRow[]>([])
const favoriteReason = ref('')

async function loadFavorites() {
  try {
    const res: FavoriteResult = await (props.api as MoviePilotApi).get(`${base.value}/favorites`)
    favorites.value = res?.items || []
    favoriteReason.value = res?.message || ''
  } catch (e: any) {
    notify(`收藏读取失败：${e?.message || e}`, 'error')
  }
}

const favoritePaths = computed(() => new Set(favorites.value.map(item => item.path)))
const currentFavorite = computed(() => {
  const path = queue.value.items?.[queue.value.index]?.path || playing.value.path
  return !!path && favoritePaths.value.has(path)
})

function toggleFavorite(remove: boolean) {
  return post('/favorites', { remove }, remove ? '取消收藏' : '收藏').then(() => loadFavorites())
}

function removeFavorite(row: FavoriteRow) {
  return post('/favorites', { path: row.path, remove: true }, '取消收藏').then(() => loadFavorites())
}

function playFavorites() {
  if (!favorites.value.length) {
    notify('收藏是空的，先在播放台或音乐库里点「收藏」', 'warning')
    return
  }
  return post('/queue', { paths: favorites.value.map(item => item.path), index: 0 }, '播放收藏')
}

// ---------------- 歌单 ----------------

const playlists = ref<PlaylistRow[]>([])
const playlistReason = ref('')
const openPlaylist = ref<PlaylistRow | null>(null)
const playlistSongs = ref<SongResult | null>(null)

async function loadPlaylists() {
  try {
    const res: PlaylistResult = await (props.api as MoviePilotApi).get(`${base.value}/playlists`)
    playlists.value = res?.items || []
    playlistReason.value = res?.message || ''
  } catch (e: any) {
    notify(`歌单读取失败：${e?.message || e}`, 'error')
  }
}

async function openPlaylistRows(row: PlaylistRow) {
  openPlaylist.value = row
  playlistSongs.value = null
  busy.value = '展开歌单'
  try {
    // 后端按函数签名收查询参数，名字就是 playlist_id（写成 id 会被当成没传）
    playlistSongs.value = await (props.api as MoviePilotApi)
      .get(`${base.value}/playlist${query({ playlist_id: row.id })}`)
  } catch (e: any) {
    notify(`歌单展开失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

function playPlaylist(row: PlaylistRow) {
  return openPlaylistRows(row).then(() => {
    const paths = playlistSongs.value?.paths || []
    if (!paths.length) {
      notify('这个歌单展开后没有可播放的音频', 'warning')
      return
    }
    return post('/queue', { paths, index: 0 }, '播放歌单')
  })
}

function queuePlaylistSongs() {
  const paths = playlistSongs.value?.paths || []
  if (!paths.length) {
    notify('这个歌单里没有可播放的音频', 'warning')
    return
  }
  return post('/queue', { paths, index: 0 }, '歌单入队')
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

async function refreshLibrary() {
  return post('/library/refresh', {}, '重建曲库索引').then((res) => {
    if (res?.message) notify(res.message, res.ok === false ? 'warning' : 'info')
    if (!res) return
    setTimeout(() => loadStatus(false), 6000)
  })
}

// ---------------- 语音 ----------------

const voiceRecords = ref<VoiceRecord[]>([])
const voiceCommandsOn = ref<string[]>([])
const testText = ref('')
const testExecute = ref(false)
const testResult = ref<VoiceState | null>(null)

const KIND_TEXT: Record<string, string> = {
  play_song: '点歌', play_artist: '歌手', play_album: '专辑', play_playlist: '歌单',
  play_favorites: '收藏', play_random: '随机', play_history: '刚才那首',
  search: '搜索', pause: '暂停', resume: '继续', stop: '停止', next: '下一首',
  prev: '上一首', play_index: '按序号切曲', volume: '音量', volume_up: '音量加',
  volume_down: '音量减', mute: '静音', set_mode: '播放方式', sleep_timer: '定时停止',
  cancel_sleep_timer: '取消定时', query_sleep_timer: '查定时', speak: '播报',
  unknown: '未识别'
}

function kindText(kind: string) {
  return KIND_TEXT[kind] || kind
}

async function loadVoiceDetail() {
  try {
    const res: VoiceState = await (props.api as MoviePilotApi).get(`${base.value}/voice`)
    voiceRecords.value = res?.records || []
    voiceCommandsOn.value = res?.commands || []
    testResult.value = res ? { ...res, records: [] } : null
  } catch (e: any) {
    notify(`语音状态读取失败：${e?.message || e}`, 'error')
  }
}

async function runVoiceTest() {
  const text = testText.value.trim()
  if (!text) {
    notify('先写一句要试的话', 'warning')
    return
  }
  busy.value = '试口令'
  try {
    const res: VoiceState = await (props.api as MoviePilotApi).post(`${base.value}/voice/test`, {
      text, execute: testExecute.value
    })
    testResult.value = res || null
    if (res && !res.ok) notify(res.message || '没有命中口令', 'warning')
    else if (testExecute.value) await loadStatus(false)
  } catch (e: any) {
    notify(`测试失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

function setVoiceEnabled(next: boolean) {
  return post('/config', { voice_enabled: next }, next ? '开启语音监听' : '关闭语音监听')
    .then(() => loadVoiceDetail())
}

// ---------------- 分栏切换时补数据 ----------------

async function switchPane(next: Pane) {
  pane.value = next
  if (next === 'songs') {
    await Promise.all([loadPlaylists(), loadFavorites()])
  } else if (next === 'voice') {
    await loadVoiceDetail()
  }
}

onMounted(async () => {
  await loadStatus()
  await loadBrowse('')
  startPolling()
})

onUnmounted(stopPolling)

// 侧栏只讲「现在跑得怎么样」：登录、默认音箱、口令表、任务定义这些都归配置页
const paneItems: { key: Pane; title: string; icon: string; desc: string }[] = [
  { key: 'overview', title: '运行总览', icon: 'mdi-view-dashboard-outline', desc: '登录、音箱、监听、曲库索引与直链地址现在是什么状态' },
  { key: 'play', title: '播放台', icon: 'mdi-speaker', desc: '当前播放、队列连播、播放方式、歌词、定时停止、音量与播报' },
  { key: 'library', title: '音乐库', icon: 'mdi-folder-music-outline', desc: '浏览库内目录与搜索文件名，点一首直接推到音箱' },
  { key: 'songs', title: '歌单与收藏', icon: 'mdi-playlist-play', desc: '目录歌单与 m3u 歌单、收藏列表，整本入队播放' },
  { key: 'tasks', title: '定时任务', icon: 'mdi-clock-outline', desc: '哪几条在等触发、上一次跑成什么样，可启停与立即执行' },
  { key: 'voice', title: '语音控制', icon: 'mdi-microphone-message', desc: '监听开没开、听到了什么、试一句话' }
]

const currentPane = computed(() => paneItems.find(p => p.key === pane.value) || paneItems[0])

// 侧栏条目右边的小数字：一眼能看出「队列里堆了多少首」「监听抓了几句」，
// 0 就不显示，免得整列都是灰色的零
function paneBadge(key: Pane): number {
  if (key === 'play') return queue.value.total || 0
  if (key === 'voice') return voice.value.handled || 0
  if (key === 'tasks') return enabledTasks.value
  return 0
}
</script>

<template>
  <v-card flat class="xai-page">
    <div class="xai-head">
      <div class="xai-brand">
        <span class="xai-brand-mark">
          <v-icon :icon="playing.status === 1 ? 'mdi-speaker-wireless' : 'mdi-speaker'" size="20" />
        </span>
        <div class="xai-brand-text">
          <div class="xai-brand-title">小爱音箱</div>
          <div class="xai-brand-sub">
            库内音频直链推送 · v{{ status.version || '-' }} · {{ syncText }}
          </div>
        </div>
      </div>
      <v-spacer />
      <div class="xai-stats">
        <span class="xai-pill" :class="`xai-pill-${status.token_expired ? 'error' : status.logged_in ? 'ok' : 'warn'}`">
          <v-icon size="13"
                  :icon="status.logged_in ? 'mdi-check-circle-outline' : 'mdi-alert-circle-outline'" />
          {{ status.token_expired ? '票据过期' : status.logged_in ? '已登录' : '未登录' }}
        </span>
        <span class="xai-pill" :class="`xai-pill-${playing.status === 1 ? 'ok' : 'idle'}`">
          <v-icon size="13"
                  :icon="playing.status === 1 ? 'mdi-play' : 'mdi-pause'" />
          {{ playing.device_name || '无设备' }} · {{ playing.status_text }}
        </span>
      </div>
      <v-btn icon="mdi-refresh" size="small" variant="text" :loading="loading || !!busy"
             @click="loadStatus()" />
      <v-btn size="small" variant="tonal" class="text-none ml-1" prepend-icon="mdi-cog-outline"
             @click="configDialog = true">插件配置
      </v-btn>
    </div>

    <v-alert v-if="needs.length || notices.length" type="info" variant="tonal"
             density="compact" class="xai-todo">
      <div v-for="item in needs" :key="'n' + item" class="text-body-2">
        <v-icon icon="mdi-form-select" size="14" class="mr-1" />{{ item }}
      </div>
      <div v-for="item in notices" :key="'m' + item" class="text-caption text-grey-darken-1">
        {{ item }}
      </div>
      <div class="mt-1">
        <v-btn size="x-small" variant="text" class="text-none" prepend-icon="mdi-cog-outline"
               @click="configDialog = true">到插件配置里处理
        </v-btn>
      </div>
    </v-alert>

    <div class="xai-body">
      <nav class="xai-nav">
        <v-list density="compact" nav class="py-2">
          <v-list-item
            v-for="item in paneItems"
            :key="item.key"
            :active="pane === item.key"
            color="primary"
            rounded="lg"
            class="xai-nav-item"
            @click="switchPane(item.key)"
          >
            <template #prepend><v-icon :icon="item.icon" class="mr-2" size="18" /></template>
            <v-list-item-title>{{ item.title }}</v-list-item-title>
            <template #append>
              <span v-if="paneBadge(item.key)" class="xai-nav-badge">{{ paneBadge(item.key) }}</span>
            </template>
          </v-list-item>
        </v-list>
      </nav>

      <section class="xai-content">
        <div class="xai-pane-title">
          <span class="xai-pane-name">{{ currentPane.title }}</span>
          <span class="xai-pane-desc">{{ currentPane.desc }}</span>
        </div>

        <!-- 运行总览：只看跑得怎么样，一律不给输入框 -->
        <div v-show="pane === 'overview'" class="xai-pane">
          <div class="xai-ov">
            <div class="xai-ov-card">
              <div class="xai-ov-head">
                <v-icon size="17" :icon="status.logged_in ? 'mdi-account-check-outline' : 'mdi-account-alert-outline'"
                        :color="status.token_expired ? 'error' : status.logged_in ? 'success' : 'warning'" />
                小米账号
                <v-spacer />
                <span class="xai-pill"
                      :class="`xai-pill-${status.token_expired ? 'error' : status.logged_in ? 'ok' : 'warn'}`">
                  {{ status.token_expired ? '票据过期' : status.logged_in ? '已登录' : '未登录' }}
                </span>
              </div>
              <div class="xai-ov-line">
                {{ status.logged_in ? (cfg.username_set ? cfg.username : '（未记录账号名）') : '还没有可用的登录票据' }}
                <span v-if="status.manual_token" class="text-caption text-grey">｜手工粘贴的 cookie</span>
              </div>
              <div class="xai-ov-sub">
                <template v-if="status.logged_in">票据保存于 {{ status.token_saved_at || '-' }}</template>
                <template v-else>登录、账号密码与设备分组只在「插件配置」里做，这里不重复一遍</template>
              </div>
            </div>

            <div class="xai-ov-card">
              <div class="xai-ov-head">
                <v-icon size="17" icon="mdi-speaker-multiple" :color="devices.length ? 'success' : 'grey'" />
                音箱
                <v-spacer />
                <span class="xai-pill" :class="`xai-pill-${devices.length ? 'ok' : 'warn'}`">
                  {{ devices.length ? `${devices.length} 台 / ${onlineCount} 台在线` : '没有设备' }}
                </span>
              </div>
              <div class="xai-ov-line">
                默认：{{ defaultDeviceName }}
                <span v-if="!cfg.default_device_id" class="text-caption text-grey">（未指定，落第一台在线）</span>
              </div>
              <div v-if="groups.length" class="xai-ov-sub">
                分组 {{ groups.length }} 组：{{ groups.map(g => `${g.name}(${g.devices.length || g.device_ids.length})`).join('、') }}
              </div>
              <div v-if="needsMusicApiCount" class="xai-ov-sub text-warning">
                {{ needsMusicApiCount }} 台只支持 play_music，直链可能被当成电台播放
              </div>
            </div>

            <div class="xai-ov-card">
              <div class="xai-ov-head">
                <v-icon size="17" :icon="playing.status === 1 ? 'mdi-play-circle-outline' : 'mdi-pause-circle-outline'"
                        :color="statusColor(playing.status) === 'success' ? 'success' : 'grey'" />
                正在播
                <v-spacer />
                <span class="xai-pill" :class="`xai-pill-${playing.status === 1 ? 'ok' : 'idle'}`">
                  {{ playing.status_text }}
                </span>
              </div>
              <div class="xai-ov-line text-truncate" :title="playing.title || playing.path">
                {{ playing.title || playing.path || playing.url || '音箱上没有在放的东西' }}
              </div>
              <div class="xai-ov-sub">
                {{ playing.device_name || '无设备' }}｜队列 {{ queue.total }} 首<template v-if="queue.total">
                  （第 {{ queue.index + 1 }} 首）</template>｜{{ modeText(queue.mode || 'order') }}｜
                连播{{ queue.auto_next ? '开' : '关' }}
                <template v-if="timerState.active">｜定时停止 {{ timerState.remaining || '已登记' }}</template>
              </div>
              <div v-if="playing.error" class="xai-ov-sub text-error">{{ playing.error }}</div>
            </div>

            <div class="xai-ov-card">
              <div class="xai-ov-head">
                <v-icon size="17" icon="mdi-microphone-message" :color="voice.enabled ? 'success' : 'grey'" />
                语音监听
                <v-spacer />
                <span class="xai-pill" :class="`xai-pill-${voice.enabled ? 'ok' : 'idle'}`">
                  {{ voice.enabled ? '监听中' : '未开启' }}
                </span>
              </div>
              <div class="xai-ov-line">
                {{ voice.enabled
                   ? `每 ${voice.interval} 秒读一轮 · 已接手 ${voice.handled} 句 · 上次轮询 ${voice.last_poll_at || '-'}`
                   : '插件不读小米的对话记录接口' }}
              </div>
              <div class="xai-ov-sub">
                生效口令 {{ voiceCommandsOn.length }} 类
                <template v-if="voice.chat">｜AI 对话{{ voice.chat_active ? '窗口开着' : '已配（窗口未开）' }}</template>
                <template v-if="voice.devices.length">｜监听 {{ voice.devices.join('、') }}</template>
              </div>
              <div v-if="voice.error" class="xai-ov-sub text-error">{{ voice.error }}</div>
            </div>

            <div class="xai-ov-card">
              <div class="xai-ov-head">
                <v-icon size="17"
                        :icon="library.ready ? 'mdi-database-marker-outline' : 'mdi-database-refresh-outline'"
                        :color="library.ready ? 'success' : 'grey'" />
                曲库索引
                <v-spacer />
                <span class="xai-pill" :class="`xai-pill-${library.ready ? 'ok' : 'warn'}`">
                  {{ library.ready ? `${library.songs} 首` : '尚未建立' }}
                </span>
              </div>
              <div class="xai-ov-line">
                {{ library.ready ? `${library.playlists} 个歌单｜语音点歌可用` : '语音点歌还点不到歌' }}
              </div>
              <div class="xai-ov-sub">
                <template v-if="library.ready">上次扫描 {{ library.scanned_at || library.built_at || '-' }}</template>
                <template v-else>第一次扫描由语音轮询带着跑，也可以去「音乐库」那栏手动重建</template>
                <template v-if="library.truncated">｜条目超上限被截断</template>
                <template v-if="library.skipped">｜跳过 {{ library.skipped }} 个</template>
              </div>
              <div v-if="library.error" class="xai-ov-sub text-error">{{ library.error }}</div>
            </div>

            <div class="xai-ov-card">
              <div class="xai-ov-head">
                <v-icon size="17" icon="mdi-link-variant"
                        :color="cfg.base_url_effective ? 'success' : 'error'" />
                音箱取流地址
                <v-spacer />
                <span class="xai-pill" :class="`xai-pill-${cfg.base_url_effective ? 'ok' : 'error'}`">
                  {{ cfg.base_url_effective ? '可用' : '拿不到' }}
                </span>
              </div>
              <div class="xai-ov-line text-truncate" :title="cfg.base_url_effective">
                {{ cfg.base_url_effective || '没有可用地址，音箱必然拉不到流' }}
              </div>
              <div class="xai-ov-sub">
                <template v-if="cfg.base_url_source">来自 {{ cfg.base_url_source }}</template>
                ｜库内 {{ cfg.dir_count }} 个目录
                <template v-if="cfg.missing_dirs?.length">｜读不到：{{ cfg.missing_dirs.join('、') }}</template>
              </div>
            </div>

            <div class="xai-ov-card">
              <div class="xai-ov-head">
                <v-icon size="17" icon="mdi-clock-outline" :color="enabledTasks ? 'success' : 'grey'" />
                定时任务
                <v-spacer />
                <span class="xai-pill" :class="`xai-pill-${enabledTasks ? 'ok' : 'idle'}`">
                  {{ enabledTasks ? `${enabledTasks}/${tasks.length} 条待触发` : '没有启用的任务' }}
                </span>
              </div>
              <div class="xai-ov-line text-truncate">
                <template v-if="tasks.length">
                  {{ tasks.slice(0, 3).map(t => `${t.name}（${t.rule}）`).join('、') }}
                  <template v-if="tasks.length > 3">｜另 {{ tasks.length - 3 }} 条</template>
                </template>
                <template v-else>还没建任务，去「插件配置」里建</template>
              </div>
              <div class="xai-ov-sub">
                <template v-if="taskLogs.length">
                  最近一次：{{ taskLogs[0].at }} {{ taskLogs[0].task_name }}
                  —— {{ taskLogs[0].success ? '成功' : '失败' }}
                </template>
                <template v-else>还没有执行记录</template>
              </div>
            </div>
          </div>

          <div class="xai-block">
            <div class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
              <v-btn size="small" variant="tonal" color="primary" prepend-icon="mdi-cog-outline"
                     class="text-none" @click="configDialog = true">打开插件配置
              </v-btn>
              <span class="text-caption text-grey">
                这一页只读运行情况和做播放动作；账号、默认音箱、口令表、任务定义与其他配置都在配置页改。
              </span>
            </div>
          </div>
        </div>

        <!-- 播放台 -->
        <div v-show="pane === 'play'" class="xai-pane">
          <v-card variant="flat" class="xai-hero pa-5">
            <div class="d-flex align-center" style="gap: 14px; flex-wrap: wrap">
              <span class="xai-hero-mark" :class="{ 'xai-hero-live': playing.status === 1 }">
                <v-icon :icon="playing.status === 1 ? 'mdi-volume-high' : 'mdi-volume-variant-off'"
                        size="26" />
              </span>
              <div style="min-width: 0; flex: 1 1 260px">
                <div class="xai-hero-title text-truncate" :title="playing.title || playing.path">
                  {{ playing.title || playing.path || playing.url || '音箱上没有在放的东西' }}
                </div>
                <div class="xai-hero-sub">
                  {{ playing.device_name || '未选择设备' }}
                  <template v-if="playing.source">｜来源 {{ playing.source }}</template>
                  <template v-if="playing.updated_at">｜{{ playing.updated_at }}</template>
                </div>
                <div v-if="playing.error" class="text-caption text-error">{{ playing.error }}</div>
              </div>
              <div class="text-right xai-hero-time">
                <div>{{ fmtSeconds(playing.position) }} <span>/ {{ fmtSeconds(playing.duration) }}</span></div>
                <span class="xai-pill" :class="`xai-pill-${statusColor(playing.status) === 'success' ? 'ok' : 'idle'}`">
                  {{ playing.status_text }}
                </span>
              </div>
            </div>
            <v-progress-linear :model-value="progress" height="5" color="primary"
                               class="mt-4 xai-hero-bar" rounded />
            <div class="d-flex align-center mt-4" style="gap: 10px; flex-wrap: wrap">
              <div class="xai-transport">
                <v-btn icon="mdi-skip-previous-circle-outline" size="small" variant="text"
                       :loading="busy === '播放控制'" @click="control('prev')" />
                <v-btn class="xai-transport-main" color="primary" variant="flat" rounded="circle"
                       :icon="playing.status === 1 ? 'mdi-pause' : 'mdi-play'"
                       :loading="busy === '播放控制'"
                       @click="control(playing.status === 1 ? 'pause' : 'play')" />
                <v-btn icon="mdi-skip-next-circle-outline" size="small" variant="text"
                       :loading="busy === '播放控制'" @click="control('next')" />
                <v-btn icon="mdi-stop-circle-outline" size="small" variant="text" color="error"
                       :loading="busy === '播放控制'" @click="control('stop')" />
              </div>
              <div class="xai-volume">
                <v-icon icon="mdi-volume-low" size="16" class="mr-1" />
                <v-slider v-model="volume" min="0" max="100" step="1" hide-details density="compact"
                          @update:model-value="volumeTouched = true" />
                <v-btn size="x-small" variant="tonal" :loading="busy === '设置音量'"
                       @click="setVolume">设为 {{ Math.round(volume) }}</v-btn>
              </div>
            </div>
            <div class="d-flex align-center mt-2 xai-modes">
              <span class="text-caption text-grey mr-1">播放方式</span>
              <v-btn v-for="item in modeItems" :key="item.value" size="x-small" variant="text"
                     class="text-none" :color="currentMode === item.value ? 'primary' : 'grey-darken-1'"
                     :prepend-icon="item.icon" :loading="busy === '设置播放方式'"
                     @click="setMode(item.value)">{{ item.title }}</v-btn>
              <v-spacer />
              <v-btn size="x-small" variant="text" class="text-none"
                     :color="currentFavorite ? 'error' : 'grey-darken-1'"
                     :prepend-icon="currentFavorite ? 'mdi-heart' : 'mdi-heart-outline'"
                     :loading="busy === '收藏' || busy === '取消收藏'"
                     @click="toggleFavorite(currentFavorite)">
                {{ currentFavorite ? '取消收藏' : '收藏' }}
              </v-btn>
            </div>
            <div class="text-caption text-grey mt-1">
              播放方式作用于插件队列：顺序/列表循环/随机/单曲循环决定「下一首」从哪来，
              「只播这一首」不接下一首。音箱本机的小米播放方式改不了。
            </div>
          </v-card>

          <div class="xai-block">
            <div class="xai-block-title">
              播放队列
              <v-chip size="x-small" variant="tonal" class="ml-2"
                      :color="queue.auto_next ? 'success' : 'grey'">
                {{ queue.auto_next ? '自动连播开' : '自动连播关' }}
              </v-chip>
              <v-chip size="x-small" variant="tonal" class="ml-2" color="primary"
                      prepend-icon="mdi-repeat">
                {{ modeText(currentMode) }}
              </v-chip>
              <span class="text-caption text-grey ml-2">{{ queue.total }} 首</span>
            </div>
            <v-alert v-if="!cfg.auto_next" type="warning" variant="tonal" density="compact" class="mb-2">
              配置里的「队列自动连播」是关的，音箱播完当前这首不会自己接下一首（这里的手动「下一首」仍然有效）。
            </v-alert>
            <v-card v-if="queue.total" variant="outlined" class="xai-card xai-queue">
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

          <div class="xai-block">
            <div class="xai-block-title">
              歌词
              <v-chip size="x-small" variant="text" class="ml-2" :color="showLyric ? 'primary' : 'grey'"
                     :prepend-icon="showLyric ? 'mdi-chevron-up' : 'mdi-chevron-down'"
                     @click="toggleLyric">{{ showLyric ? '收起' : '展开' }}</v-chip>
              <v-btn v-if="showLyric" size="x-small" variant="text" prepend-icon="mdi-refresh"
                     :loading="lyricBusy" @click="loadLyric(lyricTrack)">重新读取</v-btn>
            </div>
            <template v-if="showLyric">
              <div v-if="!lyricTrack" class="text-caption text-grey">
                音箱上现在没有在播的库内歌曲，歌词要等放起来才读得到。
              </div>
              <div v-else-if="lyricBusy && !lyric" class="text-caption text-grey">正在读取歌词…</div>
              <div v-else-if="lyric && !lyric.ok" class="text-caption text-grey">
                {{ lyric.message || '这首歌没有歌词。' }}
              </div>
              <div v-else-if="!lyric?.lines?.length" class="text-caption text-grey">没有歌词。</div>
              <template v-else>
                <div class="text-caption text-grey mb-1">
                  {{ lyric?.name }}｜{{ lyric?.source === 'sidecar' ? '同目录歌词文件' : '音频内嵌标签' }}
                  <template v-if="!lyric?.has_time">｜没有时间戳，只能整篇看</template>
                </div>
                <v-card variant="outlined" class="xai-card">
                  <div ref="lyricBox" class="xai-lyric">
                    <div v-for="(line, i) in lyric?.lines" :key="i" class="xai-lyric-line"
                         :class="{ 'xai-lyric-active': i === lyricActive }">
                      <span v-if="lyric?.has_time" class="xai-lyric-time">{{ fmtSeconds(line.time) }}</span>
                      <span>{{ line.text }}</span>
                    </div>
                  </div>
                </v-card>
              </template>
            </template>
          </div>

          <div class="xai-block">
            <div class="xai-block-title">
              定时停止
              <v-chip v-if="timerState.active" size="x-small" variant="tonal" color="success" class="ml-2">
                {{ timerState.stop_after_songs ? `还剩 ${timerState.stop_after_songs} 首` : `到 ${timerState.stop_at} 停` }}
              </v-chip>
              <v-chip v-else size="x-small" variant="tonal" color="grey" class="ml-2">未设置</v-chip>
            </div>
            <div class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
              <v-text-field v-model.number="timerMinutes" type="number" min="1" max="600"
                            density="compact" hide-details label="多少分钟后停" style="max-width: 150px" />
              <v-btn size="small" variant="tonal" :loading="busy === '设置定时停止'"
                     @click="applyTimer('minutes')">按时间</v-btn>
              <v-text-field v-model.number="timerSongs" type="number" min="1" max="500"
                            density="compact" hide-details label="再播多少首停" style="max-width: 150px" />
              <v-btn size="small" variant="tonal" :loading="busy === '设置定时停止'"
                     @click="applyTimer('songs')">按首数</v-btn>
              <v-btn size="small" variant="text" color="error" :disabled="!timerState.active"
                     :loading="busy === '取消定时停止'" @click="cancelTimer">取消定时</v-btn>
            </div>
            <div class="text-caption text-grey mt-1">
              {{ timerState.active ? (timerState.remaining || '定时已登记') : '按时间停由巡检任务到点下停止；按首数停在每首播完时计数（都要开着「自动连播」或手动切曲才会走到）。' }}
              <template v-if="timerState.stop_at">｜到点 {{ timerState.stop_at }}</template>
            </div>
          </div>
        </div>

        <!-- 音乐库 -->
        <div v-show="pane === 'library'" class="xai-pane">
          <v-card variant="outlined" class="xai-card pa-3 mb-3">
            <div class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
              <v-icon :icon="library.ready ? 'mdi-database-marker-outline' : 'mdi-database-refresh-outline'"
                      :color="library.ready ? 'success' : 'grey'" size="20" />
              <span class="text-body-2">语音点歌用的曲库索引</span>
              <v-chip size="x-small" variant="tonal" :color="library.ready ? 'success' : 'grey'">
                {{ library.ready ? `${library.songs} 首 / ${library.playlists} 个歌单` : '尚未建立' }}
              </v-chip>
              <v-spacer />
              <v-btn size="small" variant="tonal" color="primary" prepend-icon="mdi-database-refresh-outline"
                     :loading="busy === '重建曲库索引'" @click="refreshLibrary">重建索引</v-btn>
            </div>
            <div class="text-caption text-grey mt-1">
              <template v-if="library.built_at">上次重建 {{ library.built_at }}｜</template>
              <template v-if="library.refresh_minutes">每 {{ library.refresh_minutes }} 分钟自动保鲜</template>
              <template v-else>自动保鲜已关（周期 0），只能手动重建</template>
              <template v-if="library.skipped">｜跳过 {{ library.skipped }} 个读不动的文件</template>
              <template v-if="library.truncated">｜已到上限 {{ library.limit }} 首，库没扫全</template>
            </div>
            <div v-if="library.error" class="text-caption text-error mt-1">{{ library.error }}</div>
            <div class="text-caption text-grey mt-1">
              这个索引是给「语音控制」选曲用的（读标签与目录名，不搬运文件）。下面这个浏览列表是实时扫目录，两者互不影响。
            </div>
          </v-card>

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

            <v-card v-if="browse.dirs.length" variant="outlined" class="xai-card pa-2 mb-2">
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

        <!-- 歌单与收藏 -->
        <div v-show="pane === 'songs'" class="xai-pane">
          <div class="xai-block-title">
            收藏
            <span class="text-caption text-grey ml-2">{{ favorites.length }} 首</span>
            <v-btn size="x-small" variant="text" class="text-none ml-2" prepend-icon="mdi-refresh"
                   @click="loadFavorites">重新读取</v-btn>
          </div>
          <div class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
            <v-btn size="small" variant="tonal" color="primary" :disabled="!favorites.length"
                   :loading="busy === '播放收藏'" prepend-icon="mdi-heart" @click="playFavorites">
              播放我的收藏
            </v-btn>
            <span class="text-caption text-grey">
              收藏按加入顺序整本入队；语音里说「播放我的收藏」走的是同一份列表。
            </span>
          </div>
          <v-card v-if="favorites.length" variant="outlined" class="xai-card pa-2 mt-2 xai-queue">
            <div v-for="row in favorites" :key="row.path" class="xai-queue-row">
              <v-icon :icon="row.exists ? 'mdi-music-note' : 'mdi-file-question-outline'" size="16"
                      :color="row.exists ? 'grey' : 'warning'" />
              <span class="xai-queue-name" :title="row.path">{{ row.title || row.path }}</span>
              <v-chip v-if="!row.exists" size="x-small" variant="tonal" color="warning">文件已不在</v-chip>
              <v-btn size="x-small" variant="text" color="primary" :loading="busy === '播放'"
                     @click="playPath(row.path)">播放</v-btn>
              <v-btn size="x-small" variant="text" :loading="busy === '取消收藏'"
                     @click="removeFavorite(row)">移除</v-btn>
            </div>
          </v-card>
          <div v-else class="text-caption text-grey mt-1">
            {{ favoriteReason || '还没有收藏。在播放台点「收藏」，或对正在播的这首说「收藏这首歌」。' }}
          </div>

          <div class="xai-block">
            <div class="xai-block-title">
              歌单
              <span class="text-caption text-grey ml-2">{{ playlists.length }} 个</span>
              <v-btn size="x-small" variant="text" class="text-none ml-2" prepend-icon="mdi-refresh"
                     @click="loadPlaylists">重新读取</v-btn>
            </div>
            <v-alert v-if="!playlists.length" type="info" variant="tonal" density="compact">
              {{ playlistReason || '歌单来自曲库索引：库里的一层目录（含音频）与 .m3u 文件各算一个歌单，先在「音乐库」那栏重建索引。' }}
            </v-alert>
            <v-card v-else variant="outlined" class="xai-card pa-2 xai-queue">
              <div v-for="row in playlists" :key="row.id" class="xai-queue-row"
                   :class="{ 'xai-queue-current': openPlaylist?.id === row.id }" @click="openPlaylistRows(row)">
                <v-icon :icon="row.kind === 'm3u' ? 'mdi-file-music-outline' : 'mdi-folder-music-outline'"
                        size="16" />
                <span class="xai-queue-name" :title="row.path">{{ row.name }}</span>
                <span class="text-caption text-grey">{{ row.count }} 首</span>
                <v-btn size="x-small" variant="text" color="primary" :loading="busy === '播放歌单'"
                       @click.stop="playPlaylist(row)">播放</v-btn>
              </div>
            </v-card>
            <div v-if="openPlaylist" class="xai-block">
              <div class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
                <span class="text-body-2">{{ openPlaylist.name }}</span>
                <v-chip size="x-small" variant="tonal">{{ playlistSongs?.paths?.length || 0 }} 首可播</v-chip>
                <v-spacer />
                <v-btn size="small" variant="tonal" color="primary" :disabled="!playlistSongs?.paths?.length"
                       :loading="busy === '歌单入队'" prepend-icon="mdi-plus-circle-outline"
                       @click="queuePlaylistSongs">整本入队并播放</v-btn>
              </div>
              <div v-if="playlistSongs && !playlistSongs.ok" class="text-caption text-warning mt-1">
                {{ playlistSongs.message }}
              </div>
              <v-table v-else-if="playlistSongs?.items?.length" density="compact" class="xai-table mt-2">
                <thead>
                  <tr>
                    <th style="width: 45%">曲目</th>
                    <th>大小</th>
                    <th style="width: 120px">操作</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in playlistSongs.items" :key="row.path">
                    <td>
                      <div class="text-truncate" :title="row.path">{{ row.title || row.name }}</div>
                    </td>
                    <td class="text-caption">{{ fmtSize(row.size) }}</td>
                    <td>
                      <v-btn size="x-small" variant="text" color="primary" :loading="busy === '播放'"
                             @click="playFile(row)">播放</v-btn>
                      <v-btn size="x-small" variant="text" :loading="busy === '加入队列'"
                             @click="enqueue(row.path)">队列</v-btn>
                    </td>
                  </tr>
                </tbody>
              </v-table>
              <div class="text-caption text-grey mt-1">
                曲目多于 500 首时只列出前面一部分，「整本入队并播放」仍然是整本。
              </div>
            </div>
          </div>
        </div>

        <!-- 定时任务 -->
        <div v-show="pane === 'tasks'" class="xai-pane">
          <div class="xai-block-title">
            已有任务
            <span class="text-caption text-grey ml-2">{{ tasks.length }} 条</span>
          </div>
          <div v-if="!tasks.length" class="text-caption text-grey mb-2">
            还没有任务：到「插件配置」的定时任务那一节建一条，到点由插件的周期巡检触发（20 秒一次，同一分钟只跑一次）。
          </div>
          <v-card v-for="row in tasks" :key="row.id" variant="outlined" class="xai-card pa-3 mb-2">
            <div class="d-flex align-center" style="gap: 10px; flex-wrap: wrap">
              <v-switch :model-value="row.enabled" hide-details density="compact" color="primary"
                        :loading="busy === '启用任务' || busy === '停用任务'"
                        @update:model-value="toggleTask(row)" />
              <div style="min-width: 0; flex: 1 1 220px">
                <div class="text-body-2">{{ row.name }}
                  <v-chip size="x-small" variant="tonal" class="ml-1">{{ row.action_text }}</v-chip>
                </div>
                <div class="text-caption text-grey">
                  {{ row.rule }}｜{{ row.target.all ? '全部在线音箱'
                     : (row.target.device_ids.length ? `${row.target.device_ids.length} 台指定音箱` : '默认音箱') }}
                  <template v-if="row.params?.playlist_name || row.params?.playlist_id">
                    ｜歌单 {{ row.params.playlist_name || row.params.playlist_id }}
                  </template>
                </div>
              </div>
              <v-btn size="x-small" variant="text" color="primary" :loading="busy === '立即执行'"
                     @click="runTask(row)">立即执行</v-btn>
            </div>
          </v-card>

          <div class="text-caption text-grey mt-2">
            任务怎么定义（做什么、哪几天、哪台音箱、播哪个歌单）在「插件配置」的定时任务那一节改，
            这一栏只看跑得怎么样、做启停与立即执行。
            <v-btn size="x-small" variant="text" class="text-none ml-1" prepend-icon="mdi-cog-outline"
                   @click="configDialog = true">去插件配置
            </v-btn>
          </div>

          <div class="xai-block">
            <div class="xai-block-title">
              执行日志
              <span class="text-caption text-grey ml-2">最近 {{ taskLogs.length }} 条</span>
            </div>
            <div v-if="!taskLogs.length" class="text-caption text-grey">还没有执行记录。</div>
            <v-card v-else variant="outlined" class="xai-card xai-logs">
              <div v-for="(row, i) in taskLogs" :key="i" class="xai-log">
                <v-icon :icon="row.success ? 'mdi-check-circle-outline' : 'mdi-alert-circle-outline'"
                        size="15" :color="row.success ? 'success' : 'error'" />
                <span class="text-caption text-grey">{{ row.at }}</span>
                <span class="text-caption">{{ row.task_name }}</span>
                <span v-if="row.device" class="text-caption text-grey">{{ row.device }}</span>
                <span class="text-caption xai-log-message">{{ row.message }}</span>
              </div>
            </v-card>
          </div>
        </div>

        <!-- 语音控制 -->
        <div v-show="pane === 'voice'" class="xai-pane">
          <v-card variant="outlined" class="xai-card pa-4">
            <div class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
              <v-icon icon="mdi-microphone-message" size="26"
                      :color="voice.enabled ? 'success' : 'grey'" />
              <div style="min-width: 0; flex: 1 1 220px">
                <div class="text-subtitle-1">对话监听</div>
                <div class="text-caption text-grey">
                  {{ voice.enabled
                     ? `每 ${voice.interval} 秒读一轮 · 已处理 ${voice.handled} 句 · 上次轮询 ${voice.last_poll_at || '-'}`
                     : '未开启：插件不读小米的对话记录接口' }}
                </div>
              </div>
              <v-chip v-if="voice.chat" size="x-small" variant="tonal"
                      :color="voice.chat_active ? 'info' : 'grey'">
                {{ voice.chat_active ? '对话窗口开着' : 'AI 对话已配（窗口未开）' }}
              </v-chip>
              <v-btn size="small" variant="tonal" :color="voice.enabled ? 'grey' : 'success'"
                     :loading="busy === '开启语音监听' || busy === '关闭语音监听'"
                     @click="setVoiceEnabled(!voice.enabled)">
                {{ voice.enabled ? '关闭监听' : '开启监听' }}
              </v-btn>
            </div>
            <div v-if="voice.error" class="text-caption text-error mt-2">{{ voice.error }}</div>
            <div class="text-caption text-grey mt-2">
              开关只影响监听这一项动作；轮询间隔、监听哪些音箱、打断与回复等细节在「插件配置」里改。
              对音箱说「{{ cfg.voice_hint_text || '正在搜索，请稍候' }}」以外的普通指令时，小爱自己会先答一句，
              插件只在命中口令表时才接手。
            </div>
          </v-card>

          <div class="xai-block">
            <div class="xai-block-title">试一句话</div>
            <div class="d-flex" style="gap: 8px; flex-wrap: wrap">
              <v-text-field v-model="testText" density="compact" hide-details clearable
                            placeholder="例：播放 周杰伦 稻香 / 声音大一点 / 随机播放 / 收藏这首歌"
                            style="min-width: 240px; flex: 1 1 280px" @keyup.enter="runVoiceTest" />
              <v-switch v-model="testExecute" color="success" hide-details inset density="compact"
                        label="直接执行" />
              <v-btn size="small" variant="tonal" color="info" :loading="busy === '试口令'"
                     @click="runVoiceTest">判定</v-btn>
            </div>
            <v-alert v-if="testResult && (testResult.kind || testResult.message)" class="mt-2"
                     :type="testResult.ok ? 'success' : 'warning'" variant="tonal" density="compact">
              <div class="text-body-2">
                {{ testResult.ok
                   ? `命中「${kindText(testResult.kind)}」${testResult.argument ? `，参数：${testResult.argument}` : ''}`
                   : (testResult.message || '没有命中口令') }}
              </div>
              <div v-if="testResult.reply" class="text-caption">音箱回复：{{ testResult.reply }}</div>
              <div v-if="testResult.ok && !testExecute" class="text-caption text-grey">
                只判定、没执行；勾上「直接执行」才会真的推到音箱。
              </div>
            </v-alert>
          </div>

          <div class="xai-block">
            <div class="xai-block-title">
              生效口令
              <span class="text-caption text-grey ml-2">{{ voiceCommandsOn.length }} 类</span>
            </div>
            <div class="xai-chips">
              <v-chip v-for="kind in voiceCommandsOn" :key="kind" size="small" variant="tonal"
                      color="primary">{{ kindText(kind) }}
              </v-chip>
              <span v-if="!voiceCommandsOn.length" class="text-caption text-grey">
                口令表整表关着，小爱说什么都不会被接手
              </span>
            </div>
            <div class="text-caption text-grey mt-1">
              逐类开哪几类是配置，在「插件配置 → 语音口令表」里改；这里只列此刻真正生效的。
              「停止播放」这类保底口令不受那个开关影响。
              <v-btn size="x-small" variant="text" class="text-none ml-1" prepend-icon="mdi-cog-outline"
                     @click="configDialog = true">去插件配置
              </v-btn>
            </div>
          </div>

          <div class="xai-block">
            <div class="d-flex align-center">
              <div class="xai-block-title">小爱最近听到的话</div>
              <v-spacer />
              <v-btn size="x-small" variant="text" class="text-none" prepend-icon="mdi-refresh"
                     @click="loadVoiceDetail">重新读取</v-btn>
            </div>
            <v-alert v-if="!voiceRecords.length" type="info" variant="tonal" density="compact">
              {{ voice.enabled
                 ? '还没抓到新的对话。对音箱说一句话，等下一次轮询（最多十几秒）再刷新这里。'
                 : '开启监听后，这里会列出插件从对话记录里读到的每一句。' }}
            </v-alert>
            <v-card v-else variant="outlined" class="xai-card pa-2">
              <div v-for="(row, i) in voiceRecords" :key="`${row.at}-${i}`" class="xai-record">
                <span class="text-caption text-grey xai-record-at">{{ row.at }}</span>
                <span class="xai-record-text">{{ row.text }}</span>
                <span v-if="row.answer" class="text-caption text-grey text-truncate"
                      :title="row.answer">→ {{ row.answer }}</span>
              </div>
            </v-card>
            <div class="text-caption text-grey mt-1">
              这些内容来自小米云端的对话记录接口，只在插件本机内存里留最近 {{ voiceRecords.length }} 条供排障，
              不落盘、不上传；关掉监听即清空。
            </div>
          </div>
        </div>

      </section>
    </div>

    <!-- 配置页与侧栏分开：这里只把它当对话框端上来，登录、账号、默认音箱、口令表、
         任务定义都只在这一个地方能改（照豆瓣中心侧栏/配置分离的做法） -->
    <v-dialog v-model="configDialog" max-width="1080px" scrollable persistent>
      <v-card variant="flat" class="xai-config-dialog">
        <Config :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                show-close :show-switch="false" @close="configDialog = false"
                @updated="onConfigUpdated" />
      </v-card>
    </v-dialog>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁，含 .v- 的选择器会被整条删掉）。
   颜色一律取主程序的 --v-theme-* 变量，深色浅色都跟着宿主走。 */
.xai-page {
  width: 100%;
  --xai-radius: 14px;
  --xai-ink: rgba(var(--v-theme-on-surface), .92);
  --xai-ink-soft: rgba(var(--v-theme-on-surface), .62);
  --xai-ink-faint: rgba(var(--v-theme-on-surface), .42);
  --xai-line: rgba(var(--v-border-color), var(--v-border-opacity));
  --xai-veil: rgba(var(--v-theme-on-surface), .045);
}

/* ---------- 顶栏 ---------- */
.xai-head {
  display: flex; align-items: center; gap: 12px;
  padding: 14px 18px;
  border-bottom: 1px solid var(--xai-line);
  background: linear-gradient(180deg, var(--xai-veil), transparent);
}
.xai-brand { display: flex; align-items: center; gap: 11px; min-width: 0; }
.xai-brand-mark {
  display: inline-flex; align-items: center; justify-content: center;
  width: 38px; height: 38px; border-radius: 12px; flex: 0 0 auto;
  color: rgb(var(--v-theme-primary));
  background: rgba(var(--v-theme-primary), .14);
  box-shadow: inset 0 0 0 1px rgba(var(--v-theme-primary), .18);
}
.xai-brand-text { min-width: 0; line-height: 1.25; }
.xai-brand-title { font-size: 15px; font-weight: 650; letter-spacing: .2px; color: var(--xai-ink); }
.xai-brand-sub { font-size: 11.5px; color: var(--xai-ink-faint); white-space: nowrap; }
.xai-stats { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }

/* 状态药丸：顶栏、播放台、详情页共用一套形状 */
.xai-pill {
  display: inline-flex; align-items: center; gap: 5px;
  height: 22px; padding: 0 9px; border-radius: 999px;
  font-size: 11.5px; font-weight: 500; white-space: nowrap;
  border: 1px solid transparent;
}
.xai-pill-ok { color: rgb(var(--v-theme-success)); background: rgba(var(--v-theme-success), .12); border-color: rgba(var(--v-theme-success), .2); }
.xai-pill-warn { color: rgb(var(--v-theme-warning)); background: rgba(var(--v-theme-warning), .12); border-color: rgba(var(--v-theme-warning), .2); }
.xai-pill-error { color: rgb(var(--v-theme-error)); background: rgba(var(--v-theme-error), .12); border-color: rgba(var(--v-theme-error), .2); }
.xai-pill-idle { color: var(--xai-ink-soft); background: var(--xai-veil); border-color: var(--xai-line); }

.xai-todo { margin: 14px 18px 0; border-radius: var(--xai-radius); }

/* 列表面板的容器：v-card 自带的圆角比这套设计小一圈，边框也偏重，
   统一收成 14px + 一条淡线。用自有类名写，绕开构建的 .v- 门禁。 */
.xai-card {
  border-radius: var(--xai-radius);
  border-color: var(--xai-line);
}

/* ---------- 骨架 ---------- */
.xai-body { display: flex; min-height: 460px; }
.xai-nav {
  flex: 0 0 186px;
  padding: 10px 0;
  border-right: 1px solid var(--xai-line);
  background: var(--xai-veil);
}
.xai-nav-item { margin: 1px 8px; }
.xai-nav-badge {
  min-width: 20px; padding: 0 6px; height: 18px; line-height: 18px; text-align: center;
  border-radius: 999px; font-size: 11px; font-weight: 600;
  color: var(--xai-ink-soft); background: rgba(var(--v-theme-on-surface), .08);
}
.xai-content { flex: 1 1 auto; min-width: 0; overflow-y: auto; }
.xai-pane { padding: 14px 18px 22px; }
.xai-pane-title {
  display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap;
  padding: 14px 18px 0;
}
.xai-pane-name { font-size: 13px; font-weight: 650; color: var(--xai-ink); }
.xai-pane-desc { font-size: 11.5px; color: var(--xai-ink-faint); }

/* 每个内容块之间靠留白分段，不再靠一条条横线 */
.xai-block { margin-top: 18px; }
.xai-block-title {
  display: flex; align-items: center; gap: 6px;
  font-size: 12px; font-weight: 600; letter-spacing: .3px;
  color: var(--xai-ink-soft); margin-bottom: 8px;
}

/* ---------- 运行总览 ---------- */
/* 每张卡只讲一件事：标题行带状态药丸，正文一行结论、一行细节 */
.xai-ov { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); }
.xai-ov-card {
  padding: 13px 15px; border-radius: var(--xai-radius);
  border: 1px solid var(--xai-line); background: var(--xai-veil);
}
.xai-ov-head {
  display: flex; align-items: center; gap: 7px;
  font-size: 12.5px; font-weight: 650; color: var(--xai-ink);
}
.xai-ov-line { margin-top: 8px; font-size: 13px; color: var(--xai-ink); }
.xai-ov-sub { margin-top: 3px; font-size: 11.5px; color: var(--xai-ink-faint); line-height: 1.6; }
.xai-chips { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }

.xai-config-dialog {
  background: rgb(var(--v-theme-surface));
  border-radius: var(--xai-radius);
}

/* ---------- 播放台 ---------- */
.xai-hero {
  border-radius: var(--xai-radius);
  color: var(--xai-ink);
  background:
    radial-gradient(120% 140% at 0% 0%, rgba(var(--v-theme-primary), .16), transparent 60%),
    linear-gradient(180deg, rgba(var(--v-theme-surface, 255, 255, 255), .06), transparent),
    var(--xai-veil);
  box-shadow: inset 0 0 0 1px var(--xai-line);
}
.xai-hero-mark {
  display: inline-flex; align-items: center; justify-content: center;
  width: 54px; height: 54px; border-radius: 16px; flex: 0 0 auto;
  color: rgb(var(--v-theme-primary)); background: rgba(var(--v-theme-primary), .14);
}
/* 正在播的时候给一圈呼吸的光环，页面不动也知道在放 */
.xai-hero-live { animation: xai-pulse 2.4s ease-in-out infinite; }
@keyframes xai-pulse {
  0%, 100% { box-shadow: 0 0 0 0 rgba(var(--v-theme-primary), .22); }
  50% { box-shadow: 0 0 0 9px rgba(var(--v-theme-primary), 0); }
}
@media (prefers-reduced-motion: reduce) { .xai-hero-live { animation: none; } }
.xai-hero-title { font-size: 17px; font-weight: 650; letter-spacing: .2px; }
.xai-hero-sub { font-size: 11.5px; color: var(--xai-ink-faint); margin-top: 2px; }
.xai-hero-time { font-size: 13px; font-variant-numeric: tabular-nums; color: var(--xai-ink); }
.xai-hero-time span { font-size: 11.5px; color: var(--xai-ink-faint); }
.xai-hero-bar { background: rgba(var(--v-theme-on-surface), .08); }
.xai-transport {
  display: inline-flex; align-items: center; gap: 2px; padding: 3px;
  border-radius: 999px; background: rgba(var(--v-theme-on-surface), .06);
}
.xai-transport-main { width: 42px; height: 42px; box-shadow: 0 4px 14px rgba(var(--v-theme-primary), .3); }
.xai-volume {
  display: flex; align-items: center; flex: 1 1 240px; min-width: 200px; max-width: 380px;
  color: var(--xai-ink-soft);
}
.xai-modes { gap: 2px; flex-wrap: wrap; }
.xai-mode { cursor: pointer; user-select: none; transition: transform .12s ease; }
.xai-mode:active { transform: scale(.95); }
@media (prefers-reduced-motion: reduce) { .xai-mode:active { transform: none; } }

/* ---------- 队列 / 目录 / 歌词 / 日志 ---------- */
.xai-queue { padding: 4px 6px; max-height: 340px; overflow-y: auto; }
.xai-queue-row {
  display: flex; align-items: center; gap: 10px; padding: 7px 10px;
  border-radius: 10px; cursor: pointer; transition: background .14s ease;
}
.xai-queue-row:hover { background: rgba(var(--v-theme-on-surface), .06); }
.xai-queue-current {
  background: rgba(var(--v-theme-primary), .1);
  box-shadow: inset 2px 0 0 rgb(var(--v-theme-primary));
}
.xai-queue-index { flex: 0 0 22px; font-size: 11.5px; color: var(--xai-ink-faint); font-variant-numeric: tabular-nums; }
.xai-queue-name {
  flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  font-size: 13px; color: var(--xai-ink);
}
.xai-dir {
  display: flex; align-items: center; gap: 6px; padding: 6px 9px;
  border-radius: 9px; cursor: pointer; font-size: 13px; color: var(--xai-ink);
  transition: background .14s ease;
}
.xai-dir:hover { background: rgba(var(--v-theme-on-surface), .06); }
.xai-lyric { max-height: 280px; overflow-y: auto; padding: 4px 8px; }
.xai-lyric-line {
  display: flex; gap: 10px; padding: 5px 8px; border-radius: 8px; font-size: 13px;
  color: var(--xai-ink-soft);
}
.xai-lyric-active {
  background: rgba(var(--v-theme-primary), .12); color: var(--xai-ink);
  font-weight: 600; box-shadow: inset 2px 0 0 rgb(var(--v-theme-primary));
}
.xai-lyric-time { flex: 0 0 42px; font-size: 11px; color: var(--xai-ink-faint); font-variant-numeric: tabular-nums; }
.xai-logs { padding: 4px 8px; max-height: 280px; overflow-y: auto; }
.xai-log { display: flex; align-items: center; gap: 8px; padding: 5px 0; flex-wrap: wrap; }
.xai-log-message { flex: 1 1 200px; min-width: 0; color: var(--xai-ink-soft); }

.xai-table { font-size: 13px; }
.xai-table .text-truncate { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.xai-record { display: flex; align-items: baseline; gap: 8px; padding: 4px 6px; font-size: 13px; }
.xai-record-at { flex: 0 0 74px; color: var(--xai-ink-faint); font-variant-numeric: tabular-nums; }
.xai-record-text { flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cursor-pointer { cursor: pointer; }

/* 长列表滚动条细一点，别抢内容 */
.xai-content::-webkit-scrollbar, .xai-queue::-webkit-scrollbar,
.xai-lyric::-webkit-scrollbar, .xai-logs::-webkit-scrollbar { width: 7px; height: 7px; }
.xai-content::-webkit-scrollbar-thumb, .xai-queue::-webkit-scrollbar-thumb,
.xai-lyric::-webkit-scrollbar-thumb, .xai-logs::-webkit-scrollbar-thumb {
  border-radius: 999px; background: rgba(var(--v-theme-on-surface), .16);
}

@media (max-width: 900px) {
  .xai-brand-sub { display: none; }
}
@media (max-width: 760px) {
  .xai-body { flex-direction: column; }
  .xai-nav {
    flex: 0 0 auto;
    padding: 8px 10px;
    border-right: none;
    border-bottom: 1px solid var(--xai-line);
  }
  /* 窄屏下七行竖排导航会把内容推到屏幕外，换成一行胶囊横排 */
  .xai-nav .xai-nav-item {
    display: inline-flex; width: auto; margin: 2px 5px 2px 0;
    border-radius: 999px; border: 1px solid var(--xai-line);
  }
  .xai-head { padding: 12px 14px; flex-wrap: wrap; }
  .xai-pane, .xai-pane-title { padding-left: 14px; padding-right: 14px; }
  .xai-volume { max-width: none; }
}
</style>
