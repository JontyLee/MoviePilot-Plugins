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

// 说明统一收在页面底部一个块里，配置项本身只留标签。
const sourceNote = computed(() =>
  config.value.mtw_resource?.length
    ? `已选 ${config.value.mtw_resource.length} 个源，就按这个顺序问`
    : `留空 = 全部源（${ALL_SOURCES_TEXT}）`
)

const modeNote = computed(() =>
  config.value.mode === 'force_all' ? '强制覆盖（当前）' : '只补缺（当前，推荐）'
)

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
        <v-select v-model="config.mode" :items="modeItems" label="写入模式" hide-details />
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
          label="MTW 识别源（可多选，留空=全部）"
          multiple
          chips
          closable-chips
          density="compact"
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
        <v-text-field
          v-model.number="config.min_score"
          label="最低匹配分数（0-3）"
          type="number"
          min="0"
          max="3"
          hide-details
        />
      </v-col>
    </v-row>

    <v-row dense>
      <v-col cols="12" md="6">
        <v-text-field v-model="config.mtw_username" label="MTW 用户名" hide-details />
      </v-col>
      <v-col cols="12" md="6">
        <v-text-field
          v-model="config.mtw_password"
          label="MTW 密码"
          type="password"
          placeholder="仅本机保存，不入 git"
          hide-details
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

    <v-alert type="info" variant="tonal" class="mt-2">
      <div class="mb-2">
        <div class="font-weight-medium">元数据来源</div>
        全部取自 Music Tag Web（MTW）的识别结果，插件不直连任何音乐平台。{{ sourceNote }}。
        qq / spotify / acoustid / all 实测恒返回空候选，没有列进来。
      </div>
      <div class="mb-2">
        <div class="font-weight-medium">查询与选取</div>
        每个文件按源顺序逐源查询，某源出现 3/3 满分候选就立刻停止，不再问后面的源；都没满分则合并所有候选取最高分。
        分数为曲名 / 艺人 / 专辑 各 1 分（先做繁简与大小写归一，互相包含即算命中）；同分时按源顺序、同源内按 MTW
        返回顺序取第一个。最高分仍低于「最低匹配分数」（默认 3，即要求三项全中）就记为失败、不写入。
        用于匹配的本地身份优先读文件已有的 tag，缺哪项就按 艺人/专辑 (年)/NN - 曲名 的目录结构兜底。
      </div>
      <div class="mb-2">
        <div class="font-weight-medium">写入模式 · {{ modeNote }}</div>
        <div>只补缺：曲名+艺人+专辑+内嵌封面 四项齐全的文件按 mtime+size 记账直接跳过（不再请求 MTW），
          缺哪项补哪项，已有非空标签保持不动，手工修改不会被覆盖。</div>
        <div>强制覆盖：每轮忽略增量账、全库重扫重问，候选与现有标签不同就重写，封面重新下载内嵌、歌词强制重写，
          手工改过的曲名/艺人/专辑/年份/轨道号会被替换；候选里没有的字段不会清空，插件不认识的自定义注释字段也不会删。</div>
      </div>
      <div class="mb-2">
        <div class="font-weight-medium">叠加宿主策略</div>
        最终是否覆盖还取宿主「设置 → 媒体刮削」音乐三项策略与这里的较强者：宿主标签策略=覆盖时，即使这里选只补缺也会重写；
        =跳过时，两种模式都不写该项。宿主默认 标签仅缺失、封面仅缺失、歌词更优才换。
      </div>
      <div>
        <div class="font-weight-medium">同一份库</div>
        MTW 与 MP 需挂载同一份音乐库路径，否则写标签会落到不同文件。
      </div>
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
