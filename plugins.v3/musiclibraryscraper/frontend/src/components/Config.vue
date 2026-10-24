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

const DOCS_URL = 'https://github.com/JontyLee/MoviePilot-Plugins/blob/main/docs/musiclibraryscraper.md'

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

type Pane = 'flow' | 'run' | 'match' | 'library'
const pane = ref<Pane>('flow')

const panes: { key: Pane; title: string; icon: string; desc: string }[] = [
  { key: 'flow', title: '流程', icon: 'mdi-source-branch', desc: '一个文件从被扫到写标签，要过的几道判断' },
  { key: 'run', title: '运行与写入', icon: 'mdi-play-circle-outline', desc: '什么时候跑、跑到已有标签的文件动不动它' },
  { key: 'match', title: '识别与匹配', icon: 'mdi-magnify', desc: '问哪几个源、几分算命中' },
  { key: 'library', title: '音乐库范围', icon: 'mdi-folder-multiple-outline', desc: '扫哪些目录、跳过哪些' }
]

const currentPane = computed(() => panes.find(p => p.key === pane.value) || panes[0])

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
    ? `按这个顺序逐源问，某源出到满分就停`
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
  // 侧栏版式需要比宿主默认的 60rem 更宽，宽度由宿主的配置框按这个事件设置
  emit('layout', { maxWidth: '74rem' })
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
        <div class="text-caption text-medium-emphasis">{{ currentPane.desc }}</div>
      </div>
      <v-spacer />
      <v-switch v-model="config.enabled" color="success" hide-details inset
                :label="config.enabled ? '已启用' : '已停用'" />
    </div>
    <v-divider />

    <div class="mls-body">
      <nav class="mls-nav">
        <v-list density="comfortable" nav class="py-2">
          <v-list-item
            v-for="item in panes"
            :key="item.key"
            :active="pane === item.key"
            color="primary"
            rounded="lg"
            class="mls-nav-item"
            @click="pane = item.key"
          >
            <template #prepend><v-icon :icon="item.icon" class="mr-2" /></template>
            <v-list-item-title>{{ item.title }}</v-list-item-title>
          </v-list-item>
        </v-list>
      </nav>

      <section class="mls-content">
        <!-- 流程：动画讲环节，细节去 docs；这里不再摊开说明文 -->
        <div v-show="pane === 'flow'" class="mls-pane">
          <FlowPipeline :steps="FLOW_STEPS" variant="config" :running="true" />

          <v-alert type="warning" variant="tonal" density="compact" class="mt-3">
            <div class="text-body-2 font-weight-medium">写进文件就回不去</div>
            <div class="text-caption mt-1">
              「最低匹配分数」调到 1：曲名对上就写，翻唱、钢琴版也会被当成原曲。<br />
              「强制覆盖」：手工改过的标签会被替换，封面重新下载内嵌。<br />
              两者都取这里与宿主刮削策略里较强者；先小范围试一轮再全库跑。
            </div>
          </v-alert>

          <div class="mls-docs">
            <div class="mls-block-title">想看逐项判据与失败归因</div>
            <div class="text-caption text-grey">
              配置项下面的说明只留最短的一句；目录结构兜底、同分时取哪一份、各源现状与
              没接进来的源，都写在仓库的使用说明里。
            </div>
            <v-btn :href="DOCS_URL" target="_blank" rel="noopener" variant="text" size="small"
                   class="text-none mt-1" prepend-icon="mdi-open-in-new">
              docs/musiclibraryscraper.md
            </v-btn>
          </div>
        </div>

        <!-- 运行与写入 -->
        <div v-show="pane === 'run'" class="mls-pane">
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

          <div class="mls-block">
            <div class="text-caption text-grey">
              {{ config.mode === 'force_all' ? '强制覆盖：全库重扫重问，手工改过的标签会被替换（候选里没有的字段不会清空）。'
                : '只补缺：四项齐全的文件直接跳过，缺哪项补哪项，手工改过的标签不动。' }}
              最终是否覆盖取这里与宿主「设置 → 媒体刮削」的较强者。
            </div>
          </div>

          <v-switch v-model="config.onlyonce" label="保存后立即运行一次" hide-details
                    color="primary" inset class="mt-1" />

          <div class="mls-block">
            <v-switch v-model="config.show_sidebar_nav" label="在主界面「整理」显示侧栏入口"
                      hide-details color="primary" inset />
            <div class="text-caption text-grey mt-1">
              数据页（逐文件核对与补齐）在这个入口里；关掉不影响周期扫描，也不影响宿主「插件管理 → 配置」。
            </div>
          </div>
        </div>

        <!-- 识别与匹配 -->
        <div v-show="pane === 'match'" class="mls-pane">
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
          <div class="text-caption text-grey">{{ sourceNote }}。</div>

          <v-switch
            v-model="config.fill_lyrics"
            label="缺歌词时主动去识别源抓（默认交给宿主）"
            hide-details
            color="primary"
            inset
            class="mt-1"
          />

          <div class="mls-block">
            <div class="text-caption text-grey">
              曲名 / 艺人 / 专辑 各 1 分，默认要 3 分全中才写；分数低于阈值只记失败、不碰文件。
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
          </div>
        </div>

        <!-- 音乐库范围 -->
        <div v-show="pane === 'library'" class="mls-pane">
          <v-textarea
            v-model="config.scraper_paths"
            :rows="6"
            label="音乐库路径（一行一个目录）"
            placeholder="/media/music/周杰伦&#10;/media/music"
            hide-details
          />
          <v-textarea v-model="config.exclude_paths" :rows="2" label="排除路径" hide-details class="mt-2" />

          <div class="mls-block">
            <div class="text-caption text-grey">
              只处理宿主音频白名单与本插件支持列表的交集；.trickplay 缓存目录整棵跳过，
              指向库外的软链不碰 —— 写标签是按路径落到真实文件的。
            </div>
            <div v-if="audioExtensions.length" class="mls-chips mt-2">
              <v-chip v-for="ext in audioExtensions" :key="ext" size="x-small" variant="tonal"
                      color="grey">{{ ext }}</v-chip>
            </div>
          </div>
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
.mls-body { display: flex; min-height: 400px; }
/* 侧栏对话框里整张卡要被限高：限在卡片上、让内容区自己滚，
   底部的保存条才不会顶出可视区被裁掉（宿主那层滚动交给 v-dialog 做不了这件事）。 */
.mls-card-fit { max-height: 86vh; }
.mls-card-fit .mls-body { flex: 1 1 auto; min-height: 0; }
.mls-nav {
  flex: 0 0 176px;
  border-right: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  background: rgba(var(--v-theme-on-surface), .02);
}
.mls-nav-item { margin: 2px 8px; }
.mls-content { flex: 1 1 auto; min-width: 0; overflow-y: auto; }
.mls-pane { padding: 16px 18px; }
.mls-block { margin-top: 14px; }
.mls-block-title { font-size: 13px; font-weight: 600; margin-bottom: 4px; opacity: .8; }
.mls-docs { margin-top: 18px; padding-top: 12px; border-top: 1px solid rgba(var(--v-border-color), var(--v-border-opacity)); }
.mls-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.mls-actions { display: flex; align-items: center; gap: 8px; padding: 10px 16px; }
@media (max-width: 760px) {
  .mls-body { flex-direction: column; }
  .mls-nav {
    flex: 0 0 auto;
    border-right: none;
    border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  }
}
</style>
