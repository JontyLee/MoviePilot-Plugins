<script setup lang="ts">
import { ref, computed, inject } from 'vue'

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
  mtw_resource: [] as string[],
  mtw_select_mode: 0,
  min_score: 3
}

// v0.2.x 的库里存的是单字符串，多选控件需要数组。
// 留空表示"不指定"，由后端回落到全部源，这里不替用户把选项填满。
function toResourceList(value: any): string[] {
  const raw = Array.isArray(value) ? value : String(value ?? '').split(/[;,]/)
  const out: string[] = []
  for (const item of raw) {
    const name = String(item ?? '').trim()
    if (name && !out.includes(name)) out.push(name)
  }
  return out
}

const config = ref<Record<string, any>>({
  ...DEFAULTS,
  ...(props.initialConfig || {}),
  mtw_resource: toResourceList((props.initialConfig || {}).mtw_resource)
})

const modeItems = [
  { title: '只补缺（推荐）', value: '' },
  { title: '强制覆盖已有标签', value: 'force_all' }
]

// 实测能返回候选的 MTW 源；qq / spotify / acoustid / all 恒返回空，不要列进来。
const resourceItems = [
  { title: '网易云 netease', value: 'netease' },
  { title: 'QQ 音乐 qmusic', value: 'qmusic' },
  { title: '酷我 kuwo', value: 'kuwo' },
  { title: '酷狗 kugou', value: 'kugou' },
  { title: '咪咕 migu', value: 'migu' },
  { title: 'MusicBrainz', value: 'musicbrainz' },
  { title: '智能 smart_tag', value: 'smart_tag' }
]

const ALL_SOURCES_TEXT = resourceItems.map(i => i.title.split(' ')[0]).join(' / ')

const resourceHint = computed(() =>
  config.value.mtw_resource?.length
    ? `已选 ${config.value.mtw_resource.length} 个源，按顺序逐源查询，出现满分候选即停`
    : `留空 = 全部源（${ALL_SOURCES_TEXT}）；按顺序逐源查询，出现满分候选即停`
)

// 两种模式的实际差别，避免"补空/覆盖"被理解成只是重跑一次
const modeHint = computed(() =>
  config.value.mode === 'force_all'
    ? '强制覆盖：每轮忽略增量账、全库重扫重问 MTW；凡候选与现有标签不同就重写，封面重新下载并内嵌，歌词强制重写。手工改过的曲名、艺人、专辑、年份、轨道号会被替换（候选里没有的字段不会清空，插件不认识的自定义注释字段也不会删）。'
    : '只补缺（推荐）：曲名+艺人+专辑+内嵌封面 四项齐全的文件直接跳过（按 mtime+size 记账，之后不再请求）；缺哪项补哪项，已有的非空标签保持不动，手工修改不会被覆盖。'
)

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
        <v-select
          v-model="config.mode"
          :items="modeItems"
          label="写入模式"
          hint="决定已有标签是保留还是重写，下方有逐项差别说明"
          persistent-hint
        />
      </v-col>
    </v-row>

    <v-alert
      :type="config.mode === 'force_all' ? 'warning' : 'success'"
      variant="tonal"
      density="compact"
      class="mb-2"
      :text="modeHint"
    />

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
          :hint="resourceHint"
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
          hint="曲名/艺人/专辑 各 1 分，满分 3。保持 3 即可：单源召不回的歌由多源轮询补上；降到 2 会接受同名 Live 版/翻唱，写错标签的代价比漏刮高。"
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
      <div class="mb-1">
        元数据全部来自 Music Tag Web（MTW），插件不直连音乐平台。单源 top10 常整批是翻唱/钢琴版，
        所以默认逐源查询，而不是放宽匹配分数。
      </div>
      <div class="mb-1">
        最终是否覆盖还叠加宿主「设置 → 媒体刮削」里的音乐三项策略（标签 / 封面 / 歌词），取两者中更强的一个：
        宿主设为「覆盖」时，即使这里选只补缺也会重写已有标签；宿主设为「跳过」时，两种模式都不写该项。
        默认策略是 标签仅缺失、封面仅缺失、歌词更优才换。
      </div>
      <div>MTW 与 MP 需挂载同一份音乐库路径，否则写标签会落到不同文件。</div>
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
