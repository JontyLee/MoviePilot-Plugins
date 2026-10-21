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
