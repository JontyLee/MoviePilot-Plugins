/**
 * types.ts — 后端 schemas.py 的镜像形状 + 两个页面共用的极小工具。
 *
 * 只列前端真正读取的字段；后端加字段不需要这里同步，缺字段才会编译期报错。
 */

export interface Device {
  device_id: string
  name: string
  alias: string
  model: string
  hardware: string
  presence: string
  is_default: boolean
  needs_music_api: boolean
}

export interface Playing {
  device_id: string
  device_name: string
  status: number
  status_text: string
  volume: number | null
  position: number | null
  duration: number | null
  title: string
  path: string
  url: string
  source: string
  updated_at: string
  error: string
}

export interface QueueItem {
  path: string
  name: string
  title: string
  size: number
}

export interface Queue {
  device_id: string
  index: number
  total: number
  items: QueueItem[]
  auto_next: boolean
  source: string
  mode: string
}

export interface FileRow {
  name: string
  path: string
  title: string
  size: number
  mtime: number
  stream_url: string
}

export interface DirRow {
  name: string
  path: string
}

export interface Browse {
  cwd: string
  parent: string
  roots: string[]
  dirs: DirRow[]
  files: FileRow[]
  reason: string
  truncated: boolean
}

export interface Search {
  keyword: string
  items: FileRow[]
  reason: string
}

export interface PluginConfig {
  enabled: boolean
  show_sidebar_nav: boolean
  username: string
  password_set: boolean
  default_device_id: string
  music_dirs: string
  dir_count: number
  missing_dirs: string[]
  base_url: string
  base_url_effective: string
  base_url_source: string
  play_mode: string
  audio_exts: string
  audio_id: string
  stream_ttl: number
  default_volume: number
  auto_next: boolean
  keep_light: boolean
  tts_announce: boolean
  voice_enabled: boolean
  voice_devices: string
  voice_poll: number
  voice_interrupt: boolean
  voice_hint_tts: boolean
  voice_hint_text: string
  voice_reply: boolean
  voice_memory: boolean
  ai_enabled: boolean
  ai_chat: boolean
  ai_api_url: string
  ai_key_set: boolean
  ai_model: string
  ai_timeout: number
  library_limit: number
  library_refresh: number
}

/** /status.extra.timer 与 GET /timer 的睡眠定时状态。 */
export interface TimerState {
  active: boolean
  stop_at: string
  stop_after_songs: number
  remaining: string
  message?: string
}

/** /status.extra.library 与 GET /library 的曲库索引状态。 */
export interface LibraryState {
  ready: boolean
  songs: number
  playlists: number
  built_at: string
  scanned_at: string
  seconds: number
  skipped: number
  truncated: boolean
  error: string
  cached_tags?: number
  roots: string[]
  limit: number
  refresh_minutes: number
}

/** GET /voice 的监听概况 + 一次口令判定结果。 */
export interface VoiceState {
  ok: boolean
  message: string
  enabled: boolean
  chat: boolean
  chat_active: boolean
  interval: number
  handled: number
  devices: string[]
  commands: string[]
  error: string
  last_poll_at: string
  records: VoiceRecord[]
  matched: string
  kind: string
  argument: string
  reply: string
}

export interface VoiceRecord {
  device_id: string
  at: string
  text: string
  answer: string
}

export interface CommandRow {
  kind: string
  label: string
  enabled: boolean
  keywords: string[]
}

export interface FavoriteRow {
  path: string
  title: string
  exists: boolean
}

export interface FavoriteResult {
  ok: boolean
  message: string
  items: FavoriteRow[]
  playing?: Playing | null
  queue?: Queue | null
}

export interface PlaylistRow {
  id: string
  name: string
  kind: string
  path: string
  count: number
}

export interface PlaylistResult {
  ok: boolean
  message: string
  items: PlaylistRow[]
}

export interface SongResult {
  ok: boolean
  message: string
  name: string
  paths: string[]
  items: FileRow[]
}

/** /status.extra.groups 与 GET/POST /groups 的设备分组。 */
export interface GroupRow {
  name: string
  device_ids: string[]
  devices: string[]
}

export interface GroupsResult {
  ok: boolean
  message: string
  items: GroupRow[]
}

/** GET /lyric 的单行歌词；time 为 -1 表示该行没有时间戳。 */
export interface LyricLine {
  time: number
  text: string
}

export interface LyricResult {
  ok: boolean
  message: string
  path: string
  name: string
  source: string
  has_time: boolean
  lines: LyricLine[]
}

/** 定时任务的触发规则：weekly 勾星期几（0=周日），monthly 勾几号。 */
export interface TaskSchedule {
  type: string
  time: string
  weekdays: number[]
  monthdays: number[]
}

export interface TaskTarget {
  all: boolean
  device_ids: string[]
}

export interface TaskParams {
  playlist_id?: number | string
  playlist_name?: string
  song_name?: string
  start?: string
  play_mode?: string
  volume?: number | null
  stop_after_minutes?: number | null
}

/** /status.extra.tasks 与 GET/POST /tasks 的一条定时任务。 */
export interface TaskRow {
  id: string
  name: string
  enabled: boolean
  action: string
  action_text: string
  rule: string
  schedule: TaskSchedule
  target: TaskTarget
  params: TaskParams
  created_at: string
  updated_at: string
}

export interface TaskLogRow {
  task_id: string
  task_name: string
  action: string
  device: string
  success: boolean
  message: string
  at: string
}

/** 定时任务能做的动作，与后端 _TASK_ACTIONS 同一套。 */
export const TASK_ACTIONS = [
  { title: '播放歌单', value: 'play_playlist' },
  { title: '歌单从某首播', value: 'play_playlist_from' },
  { title: '停止播放', value: 'stop' },
  { title: '设置音量', value: 'set_volume' },
  { title: '设置播放方式', value: 'set_play_mode' },
  { title: '开启语音监听', value: 'enable_monitor' },
  { title: '关闭语音监听', value: 'disable_monitor' }
]

export const WEEK_DAYS = ['周日', '周一', '周二', '周三', '周四', '周五', '周六']

/** 这条任务要填哪些额外参数：表单和后端 _task_play 判的是同一件事。 */
export function taskNeedsPlaylist(action: string): boolean {
  return action === 'play_playlist' || action === 'play_playlist_from'
}

export interface Status {
  enabled: boolean
  version: string
  logged_in: boolean
  token_saved_at: string
  token_expired: boolean
  manual_token: boolean
  needs: string[]
  notices: string[]
  devices: Device[]
  default_device: Device | null
  playing: Playing
  queue: Queue
  config: PluginConfig
  extra: Record<string, any>
}

export interface LoginResult {
  state: string
  message: string
  captcha_image: string
  logged_in: boolean
  device_count: number
}

/** 后端 status 里的播放态：0 停止、1 播放中、2 暂停（mina.PLAY_STATUS_*）。 */
export function statusColor(status: number): string {
  if (status === 1) return 'success'
  if (status === 2) return 'warning'
  if (status === 0) return 'grey'
  return 'grey-darken-1'
}

export function fmtSeconds(value?: number | null): string {
  if (value === null || value === undefined) return '-'
  const total = Math.max(0, Math.floor(value))
  const minutes = Math.floor(total / 60)
  const seconds = total % 60
  return `${minutes}:${String(seconds).padStart(2, '0')}`
}

export function fmtSize(bytes?: number): string {
  if (!bytes) return '-'
  const mb = bytes / 1048576
  return mb >= 1 ? `${mb.toFixed(1)} MB` : `${(bytes / 1024).toFixed(0)} KB`
}

export function fmtTime(value?: number | string): string {
  if (!value && value !== 0) return '-'
  if (typeof value === 'string') return value
  try {
    return new Date(value * 1000).toLocaleString()
  } catch {
    return String(value)
  }
}

/** 播放方式：与后端 _MODE_TEXT / intents.normalize_mode 的取值一一对应。 */
export const MODE_ITEMS = [
  { title: '顺序播放', value: 'order', icon: 'mdi-sort' },
  { title: '列表循环', value: 'loop', icon: 'mdi-repeat' },
  { title: '随机播放', value: 'random', icon: 'mdi-shuffle-variant' },
  { title: '单曲循环', value: 'single', icon: 'mdi-repeat-once' },
  { title: '只播这一首', value: 'single_play', icon: 'mdi-music-note-minus' }
]

export function modeText(mode: string): string {
  return MODE_ITEMS.find(m => m.value === mode)?.title || mode || '顺序播放'
}

/**
 * 当前该高亮哪一行，和后端 lyrics.active_index 是同一条规则：取最后一条
 * 时间戳 <= 播放进度 + 0.5 的行。0.5 秒的余量是给音箱上报进度粗糙用的。
 */
export function activeLine(lines: LyricLine[], position?: number | null): number {
  if (!lines.length || position === null || position === undefined) return -1
  let hit = -1
  for (const [i, line] of lines.entries()) {
    if (line.time < 0) continue
    if (line.time <= position + 0.5) hit = i
    else break
  }
  return hit
}

/**
 * 宿主注入的 api.get 只认第一个参数：第二个对象不会变成查询串，而是静静丢掉。
 * 条件必须自己编码进 URL，否则 /browse 与 /search 发出去的都是裸路径，
 * 后端只能按默认值（库根、空关键词）回答。
 */
export function query(params: Record<string, any>): string {
  const pairs: string[] = []
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`)
  }
  return pairs.length ? `?${pairs.join('&')}` : ''
}
