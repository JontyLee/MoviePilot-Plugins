<script setup lang="ts">
import { ref, inject } from 'vue'

interface HostToast {
  error(msg: string): unknown
  info(msg: string): unknown
  success(msg: string): unknown
  warning(msg: string): unknown
}

const props = defineProps({
  initialConfig: { type: Object as any, default: () => ({}) },
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function as any, default: null }
})

const emit = defineEmits(['save', 'close', 'switch'])
const toast = inject<HostToast | null>('moviepilot:toast', null)

// 实测能返回候选的 MTW 源；qq / spotify / acoustid / all 恒返回空，不要列进来。
const DEFAULT_RESOURCES = ['netease', 'qmusic', 'kuwo', 'kugou', 'migu', 'musicbrainz', 'smart_tag']

const DEFAULTS = {
  enabled: false,
  onlyonce: false,
  mode: '',
  cron: '0 3 * * 0',
  scraper_paths: '',
  exclude_paths: '',
  mtw_base_url: '',
  mtw_username: '',
  mtw_password: '',
  mtw_resource: DEFAULT_RESOURCES.slice(),
  mtw_select_mode: 0,
  min_score: 3
}

// v0.2.x 的库里存的是单字符串，多选控件需要数组
function toResourceList(value: any): string[] {
  const raw = Array.isArray(value) ? value : String(value ?? '').split(/[;,]/)
  const out: string[] = []
  for (const item of raw) {
    const name = String(item ?? '').trim()
    if (name && !out.includes(name)) out.push(name)
  }
  return out.length ? out : DEFAULT_RESOURCES.slice()
}

const config = ref<Record<string, any>>({
  ...DEFAULTS,
  ...(props.initialConfig || {}),
  mtw_resource: toResourceList((props.initialConfig || {}).mtw_resource)
})

const modeItems = [
  { title: '只补缺（标签/封面）', value: '' },
  { title: '强制覆盖已有标签', value: 'force_all' }
]

const resourceItems = [
  { title: '网易云 netease', value: 'netease' },
  { title: 'QQ 音乐 qmusic', value: 'qmusic' },
  { title: '酷我 kuwo', value: 'kuwo' },
  { title: '酷狗 kugou', value: 'kugou' },
  { title: '咪咕 migu', value: 'migu' },
  { title: 'MusicBrainz', value: 'musicbrainz' },
  { title: '智能 smart_tag', value: 'smart_tag' }
]

const selectModeItems = [
  { title: '0 默认', value: 0 },
  { title: '1 精准匹配（时长）', value: 1 }
]

function saveConfig() {
  emit('save', config.value)
  toast?.success('配置已保存')
}
</script>

<template>
  <div class="plugin-config pa-4">
    <div class="text-h6 mb-3">MusicTagWeb音乐库刮削 · 配置</div>

    <v-row dense>
      <v-col cols="12" md="4">
        <v-switch v-model="config.enabled" label="启用插件" hide-details color="primary" inset />
      </v-col>
      <v-col cols="12" md="4">
        <v-switch v-model="config.onlyonce" label="立即运行一次" hide-details color="primary" inset />
      </v-col>
      <v-col cols="12" md="4">
        <v-select v-model="config.mode" :items="modeItems" label="覆盖模式" hide-details />
      </v-col>
    </v-row>

    <v-row dense>
      <v-col cols="12" md="6">
        <v-text-field
          v-model="config.cron"
          label="执行周期"
          placeholder="5 位 cron，留空默认 0 3 * * 0"
          hide-details
        />
      </v-col>
      <v-col cols="12" md="6">
        <v-select
          v-model="config.mtw_resource"
          :items="resourceItems"
          label="MTW 识别源（可多选）"
          multiple
          chips
          closable-chips
          density="compact"
          hint="按顺序逐源查询，出现满分候选即停"
          persistent-hint
        />
      </v-col>
    </v-row>

    <v-row dense>
      <v-col cols="12" md="8">
        <v-text-field
          v-model="config.mtw_base_url"
          label="Music Tag Web 地址"
          placeholder="http://192.168.x.x:8001"
          hide-details
        />
      </v-col>
      <v-col cols="12" md="4">
        <v-select
          v-model="config.mtw_select_mode"
          :items="selectModeItems"
          label="MTW 候选排序"
          hide-details
        />
      </v-col>
    </v-row>

    <v-row dense>
      <v-col cols="12" md="4">
        <v-text-field v-model="config.mtw_username" label="MTW 用户名" hide-details />
      </v-col>
      <v-col cols="12" md="4">
        <v-text-field
          v-model="config.mtw_password"
          label="MTW 密码"
          type="password"
          placeholder="仅本机保存，不入 git"
          hide-details
        />
      </v-col>
      <v-col cols="12" md="4">
        <v-text-field
          v-model.number="config.min_score"
          label="最低匹配分数"
          type="number"
          min="0"
          max="3"
          hint="曲名/艺人/专辑 各 1 分，满分 3。3 最稳但会漏掉繁简字形与合辑专辑名差异；2 会接受“同名 Live 版”"
          persistent-hint
        />
      </v-col>
    </v-row>

    <v-row dense>
      <v-col cols="12">
        <v-textarea
          v-model="config.scraper_paths"
          :rows="5"
          label="音乐库路径（一行一个目录）"
          placeholder="/media/music/周杰伦&#10;/media/music"
          hide-details
        />
      </v-col>
    </v-row>

    <v-row dense>
      <v-col cols="12">
        <v-textarea v-model="config.exclude_paths" :rows="2" label="排除路径" hide-details />
      </v-col>
    </v-row>

    <v-alert type="info" variant="tonal" class="mt-2" density="compact">
      元数据全部来自 Music Tag Web（MTW），插件不直连音乐平台。多选源按顺序逐源查询，某源已出满分候选即停止，
      单源 top10 常整批是翻唱/钢琴版，换源比放宽分数安全。
      「补空」判定为 曲名+艺人+专辑+内嵌封面 齐全，缺一即会重新刮削；封面由 MP
      下载后内嵌写入。覆盖模式会重写所有非空 tag。MTW 与 MP 需挂载同一份音乐库路径，否则写标签会落到不同文件。
    </v-alert>

    <div class="d-flex align-center gap-2 mt-4">
      <v-spacer />
      <v-btn variant="text" @click="emit('switch')">查看数据页</v-btn>
      <v-btn color="primary" prepend-icon="mdi-content-save" @click="saveConfig">保存配置</v-btn>
    </div>
  </div>
</template>

<style scoped>
/* 仅局部样式；构建有 vuetify/mdi 全局规则剥离门禁。 */
</style>
