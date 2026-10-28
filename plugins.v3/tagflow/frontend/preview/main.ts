import { createApp, h, reactive } from 'vue'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'
import 'vuetify/styles'
import Config from '../src/components/Config.vue'

// 仅本地预览：把 standalone 配置页当成宿主对话框里那张卡来看排版，
// 接口全部本地假数据，不参与任何构建或发布。
const saved = reactive({
  enabled: true,
  show_sidebar_nav: true,
  onlyonce: false,
  mode: '',
  cron: '0 3 * * sun',
  scraper_paths: '/media/music\n/media/music/周杰伦',
  exclude_paths: '/media/music/未完成',
  resources: [],
  min_score: 3,
  fill_lyrics: false,
  audio_extensions: ['flac', 'mp3', 'm4a', 'wav', 'ape', 'ogg', 'dsf']
})

const missed: string[] = []

const api = {
  async get(url: string) {
    if (url.includes('/config')) return { ...saved }
    missed.push(`GET ${url}`)
    return {}
  },
  async post(url: string, body?: any) {
    if (url.includes('/config')) return { ok: true, message: '已保存（本地预览）', ...saved, ...body }
    missed.push(`POST ${url}`)
    return { ok: true }
  }
} as unknown as MoviePilotApi

const params = new URLSearchParams(location.search)
const app = createApp({
  render() {
    return h('div', { class: 'preview-shell' }, [
      missed.length ? h('div', {}, `未预置的接口：${missed.join(' | ')}`) : null,
      h(Config, {
        api,
        pluginId: 'TagFlow',
        sourcePluginId: 'TagFlow',
        standalone: params.get('inline') !== '1'
      } as any)
    ])
  }
})

app.use(createVuetify({ components, directives, theme: { defaultTheme: params.get('dark') === '1' ? 'dark' : 'light' } }))
app.mount('#app')
