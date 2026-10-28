<script setup lang="ts">
import { ref, computed, inject, onMounted } from 'vue'
import FlowPipeline from './FlowPipeline.vue'
import { FLOW_STEPS } from './flow'

const props = defineProps({
  initialConfig: { type: Object as any, default: () => ({}) },
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function as any, default: null },
  // 侧栏全页里自己开的这个配置框：宿主不给 AppPage 传 initialConfig，
  // 起头和落盘都得走插件自己的 /config，而不是 emit('save') 交给宿主整体替换。
  standalone: { type: Boolean, default: false }
})

const emit = defineEmits(['save', 'close', 'switch', 'layout', 'updated'])

const DEFAULTS = {
  enabled: false,
  // 缺键按开：升级过来的装机没有这一项，默认关会让入口"升级完就消失"
  show_sidebar_nav: true,
  onlyonce: false,
  mode: '',
  cron: '0 3 * * sun',
  scraper_paths: '',
  exclude_paths: '',
  // 识别源留空 = 全部源：预填五个等于把这条规则从界面上抹掉
  resources: [] as string[],
  min_score: 3,
  // 关掉时歌词仍由宿主 lrclib / AMLL 取
  fill_lyrics: false
}

const DOCS_URL = 'https://github.com/JontyLee/MoviePilot-Plugins/blob/main/docs/tagflow.md'

// 老配置里的多选可能是分号/逗号分隔的字符串，控件要数组。这里只做拆分与去重，
// 不替用户把选项补全 —— 留空就是"全部源"，交给后端解释。
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
  resources: toResourceList((props.initialConfig || {}).resources)
})

const saving = ref(false)
const audioExtensions = ref<string[]>([])
const snackbar = ref({ show: false, text: '', color: 'info' })

interface HostToast {
  error(msg: string): unknown
  info(msg: string): unknown
  success(msg: string): unknown
  warning(msg: string): unknown
}
const toast = inject<HostToast | null>('moviepilot:toast', null)
const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`)

function notify(msg: string, color = 'info') {
  if (toast) {
    const fn = (toast as any)[color] || toast.info
    fn.call(toast, msg)
    return
  }
  snackbar.value = { show: true, text: msg, color }
}

const modeItems = [
  { title: '只补缺（推荐）', value: '' },
  { title: '强制覆盖已有标签', value: 'force_all' }
]

const resourceItems = [
  { title: '网易云 netease', value: 'netease' },
  { title: 'QQ 音乐 qmusic', value: 'qmusic' },
  { title: '酷狗 kugou', value: 'kugou' },
  { title: '咪咕 migu', value: 'migu' },
  { title: 'MusicBrainz', value: 'musicbrainz' }
]

const picked = computed(() => toResourceList(config.value.resources))
const sourceNote = computed(() =>
  picked.value.length
    ? `全部源一次问完（发出时随机错峰，防风控）；分数与完整度都并列时，按这个顺序取先`
    : `留空 = 全部源（${resourceItems.map(i => i.title.split(' ')[0]).join(' / ')}）`
)

function saveConfig() {
  if (props.standalone) return saveStandalone()
  // 只负责把配置交给宿主：宿主自己 PUT /plugin/{id} 并弹「已保存 / 保存失败」。
  // 这里再弹一条会出现两条通知，而且它在请求发出前就报成功，失败了也照样说已保存。
  emit('save', config.value)
}

/** GET /config 回的是存下来的配置原值：resources 留空就是留空，不展开成全部源。
 *  保存成功后也重新取一次 —— POST 回的是整份 /status，那里的 resources 是本轮生效值，
 *  拿它起头会把"留空 = 全部源"这条规则在界面上写成显式选中的五个源。 */
async function loadStandalone() {
  try {
    const res = await (props.api as MoviePilotApi).get(`${base.value}/config`)
    if (!res) return
    config.value = {
      ...DEFAULTS,
      ...res,
      onlyonce: false,
      resources: toResourceList(res.resources)
    }
    // 实际会处理的扩展名（宿主白名单 ∩ 插件支持表）：宿主收走某个格式时这里立刻看得出
    audioExtensions.value = Array.isArray(res.audio_extensions) ? res.audio_extensions : []
  } catch (e: any) {
    notify(`配置读取失败：${e?.message || e}`, 'error')
  }
}

async function saveStandalone() {
  saving.value = true
  try {
    const body: Record<string, any> = { ...config.value, onlyonce: !!config.value.onlyonce }
    const res = await (props.api as MoviePilotApi).post(`${base.value}/config`, body)
    if (res && res.ok === false) {
      notify(res.message || '保存失败', 'warning')
      return
    }
    await loadStandalone()
    notify('配置已保存', 'success')
    emit('updated')
  } catch (e: any) {
    notify(`保存失败：${e?.message || e}`, 'error')
  } finally {
    saving.value = false
  }
}

onMounted(() => {
  // 一页摊开后没有左侧导航占宽，但两条路径输入框与流水线仍要比宿主默认 60rem 松一点
  emit('layout', { maxWidth: '64rem' })
  if (props.standalone) loadStandalone()
})
</script>

<template>
  <v-card flat :class="['mls-card', standalone ? 'mls-card-fit' : '']">
    <div class="mls-head">
      <v-avatar color="primary" variant="tonal" rounded="lg" size="40" class="mr-3">
        <v-icon icon="mdi-music-circle-outline" size="24" />
      </v-avatar>
      <div class="mls-head-text">
        <div class="text-h6">TagFlow音乐库刮削 · 配置</div>
        <div class="text-caption text-medium-emphasis">
          每个开关最后都落到音乐文件上且无法回滚，先看它管流水线哪一环
        </div>
      </div>
      <v-spacer />
      <v-switch v-model="config.enabled" color="success" hide-details inset
                :label="config.enabled ? '已启用' : '已停用'" />
    </div>
    <v-divider />

    <div class="mls-body">
      <!-- 流程：动画讲环节，细节去 docs；一页摊开后这里只留一句提醒，
           逐项判据与警告的完整版都在下面的具体项里说，不重复占一整屏 -->
      <section class="mls-group">
        <div class="mls-group-title">流程</div>
        <FlowPipeline :steps="FLOW_STEPS" variant="config" :running="true" />

        <div class="mls-docs">
          <div class="text-caption mls-warn">
            分数调到 1 会把翻唱、钢琴版当成原曲；强制覆盖会替换手工改过的标签 ——
            两者都写进文件、无法回滚，先小范围试一轮再全库跑。
          </div>
          <div class="text-caption text-grey mt-1">
            目录结构兜底、同分时取哪一份、各源现状与没接进来的源，都写在仓库的使用说明里。
          </div>
          <v-btn :href="DOCS_URL" target="_blank" rel="noopener" variant="text" size="small"
                 class="text-none mt-1" prepend-icon="mdi-open-in-new">
            docs/tagflow.md
          </v-btn>
        </div>
      </section>

      <!-- 运行与写入 -->
      <section class="mls-group">
        <div class="mls-group-title">运行与写入</div>

        <v-row dense>
          <v-col cols="12" md="6">
            <v-select v-model="config.mode" :items="modeItems" label="写入模式" hide-details />
          </v-col>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="config.cron"
              label="执行周期"
              placeholder="5 位 cron；星期 0=周一…6=周日、7=周日，留空默认 0 3 * * sun"
              hide-details
            />
          </v-col>
        </v-row>

        <div class="text-caption text-grey mt-2">
          {{ config.mode === 'force_all' ? '强制覆盖：全库重扫重问，手工改过的标签会被替换（候选里没有的字段不会清空）。'
            : '只补缺：四项齐全的文件直接跳过，缺哪项补哪项，手工改过的标签不动。' }}
          最终是否覆盖取这里与宿主「设置 → 媒体刮削」的较强者。
        </div>

        <v-switch v-model="config.onlyonce" label="保存后立即运行一次" hide-details
                  color="primary" inset class="mt-2" />

        <div class="mt-2">
          <v-switch v-model="config.show_sidebar_nav" label="在主界面「整理」显示侧栏入口"
                    hide-details color="primary" inset />
          <div class="text-caption text-grey mt-1">
            数据页（逐文件核对与补齐）在这个入口里；关掉不影响周期扫描，也不影响宿主「插件管理 → 配置」。
          </div>
        </div>
      </section>

      <!-- 识别与匹配 -->
      <section class="mls-group">
        <div class="mls-group-title">识别与匹配</div>

        <v-row dense>
          <v-col cols="12" md="4">
            <v-text-field
              v-model.number="config.min_score"
              label="最低匹配分数（1-3）"
              type="number"
              min="1"
              max="3"
              hide-details
            />
          </v-col>
          <v-col cols="12" md="8">
            <v-select
              v-model="config.resources"
              :items="resourceItems"
              label="识别源（可多选，留空=全部源）"
              multiple
              chips
              closable-chips
              density="compact"
            />
          </v-col>
        </v-row>
        <div class="text-caption text-grey mt-2">{{ sourceNote }}。</div>

        <v-switch
          v-model="config.fill_lyrics"
          label="缺歌词时主动去识别源抓（默认交给宿主）"
          hide-details
          color="primary"
          inset
          class="mt-2"
        />

        <div class="text-caption text-grey mt-2">
          曲名 / 艺人 / 专辑 各 1 分，默认要 3 分全中才写；分数只是门槛，低于阈值只记失败、不碰文件。
          过了门槛的候选不止一个时，比谁给得全（年份 / 音轨 / 碟号 / 封面）。
          调低到 1-2 就可能把翻唱、纯音乐版当成原曲，标签写进文件无法回滚。
        </div>
        <div class="text-caption text-grey mt-1">
          五个源都是插件直连的公开检索接口，不必部署任何服务；只发清洗后的「曲名-艺人」，
          库内路径与目录结构不出本机。
        </div>
        <div class="text-caption text-grey mt-1">
          比对用的本地身份优先读文件已有标签，缺哪项就按目录结构兜底；猜不出来宁可判未达阈值，
          也不拿目录名凑假身份改写标签。
        </div>
      </section>

      <!-- 音乐库范围 -->
      <section class="mls-group">
        <div class="mls-group-title">音乐库范围</div>

        <v-textarea
          v-model="config.scraper_paths"
          :rows="4"
          auto-grow
          label="音乐库路径（一行一个目录）"
          placeholder="/media/music/周杰伦&#10;/media/music"
          hide-details
        />
        <v-textarea v-model="config.exclude_paths" :rows="1" auto-grow
                    label="排除路径" hide-details class="mt-2" />

        <div class="text-caption text-grey mt-2">
          只处理宿主音频白名单与本插件支持列表的交集；.trickplay 缓存目录整棵跳过，
          指向库外的软链不碰 —— 写标签是按路径落到真实文件的。
        </div>
        <div v-if="audioExtensions.length" class="mls-chips mt-2">
          <v-chip v-for="ext in audioExtensions" :key="ext" size="x-small" variant="tonal"
                  color="grey">{{ ext }}</v-chip>
        </div>
      </section>
    </div>

    <v-divider />
    <div class="mls-actions">
      <v-btn v-if="!standalone" variant="text" class="text-none" prepend-icon="mdi-database-eye-outline"
             @click="emit('switch')">
        查看数据页
      </v-btn>
      <v-btn v-else variant="text" class="text-none" prepend-icon="mdi-close" @click="emit('close')">
        关闭
      </v-btn>
      <v-spacer />
      <v-btn color="primary" variant="flat" prepend-icon="mdi-content-save" class="text-none"
             :loading="saving" @click="saveConfig">
        保存配置
      </v-btn>
    </div>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.mls-card { width: 100%; display: flex; flex-direction: column; }
.mls-head { display: flex; align-items: center; gap: 8px; padding: 14px 18px; }
.mls-head-text { min-width: 0; }
.mls-body { flex: 1 1 auto; min-height: 0; overflow-y: auto; padding: 4px 18px 16px; }
/* 侧栏对话框里整张卡要被限高：限在卡片上、让正文自己滚，
   底部的保存条才不会顶出可视区被裁掉（宿主那层滚动交给 v-dialog 做不了这件事）。 */
.mls-card-fit { max-height: 86vh; }
.mls-group { padding: 16px 0; }
.mls-group + .mls-group { border-top: 1px solid rgba(var(--v-border-color), var(--v-border-opacity)); }
.mls-group-title { font-size: 13px; font-weight: 600; margin-bottom: 10px; opacity: .8; }
.mls-docs { margin-top: 14px; padding-top: 10px; border-top: 1px solid rgba(var(--v-border-color), var(--v-border-opacity)); }
.mls-warn { color: rgb(var(--v-theme-warning)); }
.mls-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.mls-actions { display: flex; align-items: center; gap: 8px; padding: 10px 16px; }
</style>
