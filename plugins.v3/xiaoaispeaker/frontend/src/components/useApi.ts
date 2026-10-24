/**
 * useApi.ts — 配置页那几个组件共用的极小宿主接口封装。
 *
 * 宿主注入的 api.get 只认第一个参数（见 types.query 的说明），条件必须自己编码进 URL；
 * api.post 的返回形状各端点不一样，这里只按 {ok, message} 这一套通用约定播报。
 */
import { computed, inject, ref } from 'vue'
import { query } from './types'

interface HostToast {
  error(msg: string): unknown
  info(msg: string): unknown
  success(msg: string): unknown
  warning(msg: string): unknown
}

export interface ApiProps {
  api?: MoviePilotApi
  pluginId: string
  sourcePluginId: string
}

export function useApi(props: ApiProps, after?: () => void) {
  const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`)
  const toast = inject<HostToast | null>('moviepilot:toast', null)
  const busy = ref('')
  const snackbar = ref({ show: false, text: '', color: 'info' })

  function notify(msg: string, color = 'info') {
    if (toast) {
      const fn = (toast as any)[color] || toast.info
      fn.call(toast, msg)
      return
    }
    snackbar.value = { show: true, text: msg, color }
  }

  async function get(path: string, params: Record<string, any> = {}) {
    return (props.api as MoviePilotApi).get(`${base.value}${path}${query(params)}`)
  }

  /** 通用 POST：按 ok/message 播报，成功后叫一次 after（让外层重取状态）。 */
  async function post(path: string, body: Record<string, any>, label: string) {
    busy.value = label
    try {
      const res = await (props.api as MoviePilotApi).post(`${base.value}${path}`, body)
      if (res && res.ok === false) notify(res.message || res.error || `${label}失败`, 'warning')
      else if (res && (res.message || res.ok === true)) notify(res.message || `${label}已执行`)
      if (res) after?.()
      return res
    } catch (e: any) {
      notify(`${label}失败：${e?.message || e}`, 'error')
      return null
    } finally {
      busy.value = ''
    }
  }

  /** 原样发一次请求，播报交给调用方（登录那种带状态机的返回形状不在 ok/message 约定里）。 */
  async function raw(path: string, body: Record<string, any>, label: string) {
    busy.value = label
    try {
      return await (props.api as MoviePilotApi).post(`${base.value}${path}`, body)
    } catch (e: any) {
      notify(`${label}失败：${e?.message || e}`, 'error')
      return null
    } finally {
      busy.value = ''
    }
  }

  return { base, busy, snackbar, notify, get, post, raw }
}
