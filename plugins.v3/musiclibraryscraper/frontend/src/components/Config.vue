<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { FLOW_STEPS } from './flow'

const props = defineProps({
  initialConfig: { type: Object as any, default: () => ({}) },
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function as any, default: null }
})

const emit = defineEmits(['save', 'close', 'switch', 'layout'])

const DEFAULTS = {
  enabled: false,
  onlyonce: false,
  mode: '',
  cron: '0 3 * * sun',
  scraper_paths: '',
  exclude_paths: '',
  mtw_base_url: '',
  // 识别来源默认 auto（与后端一致）：填过 MTW 地址的装机继续用 MTW，没填的用内置源
  source_backend: 'auto',
  mtw_username: '',
  mtw_password: '',
  mtw_resource: [] as string[],
  min_score: 3,
  // 缺失时主动去内置识别源拉歌词回填，默认关闭；关闭时歌词仍由宿主 lrclib / AMLL 取
  fill_lyrics: false
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

type Pane = 'flow' | 'run' | 'match' | 'library'
const pane = ref<Pane>('flow')

const panes: { key: Pane; title: string; icon: string; desc: string }[] = [
  { key: 'flow', title: '流程总览', icon: 'mdi-source-branch', desc: '一个文件从被扫到写到标签，中间经过哪些判断、各由哪个配置决定' },
  { key: 'run', title: '运行与写入', icon: 'mdi-play-circle-outline', desc: '什么时候跑、跑到已有标签的文件时动不动它' },
  { key: 'match', title: '识别与匹配', icon: 'mdi-magnify', desc: '问谁、问几个源、几分才算命中' },
  { key: 'library', title: '音乐库范围', icon: 'mdi-folder-multiple-outline', desc: '扫哪些目录、跳过哪些目录' }
]

const currentPane = computed(() => panes.find(p => p.key === pane.value) || panes[0])

const modeItems = [
  { title: '只补缺（推荐）', value: '' },
  { title: '强制覆盖已有标签', value: 'force_all' }
]

const backendItems = [
  { title: '自动（填了 MTW 地址走 MTW，否则内置源）', value: 'auto' },
  { title: '内置源（免部署 Music Tag Web）', value: 'builtin' },
  { title: 'Music Tag Web', value: 'mtw' }
]

// 插件自己直连的内置源。酷我实测检索接口恒不返回命中、www 接口要签名；
// smart_tag 是 MTW 侧的 AI 归因，插件这层没有对应的公开接口 —— 这两个只在 MTW 模式列出。
const builtinResourceItems = [
  { title: '网易云 netease', value: 'netease' },
  { title: 'QQ 音乐 qmusic', value: 'qmusic' },
  { title: '酷狗 kugou', value: 'kugou' },
  { title: '咪咕 migu', value: 'migu' },
  { title: 'MusicBrainz', value: 'musicbrainz' }
]

// 实测能返回候选的 MTW 源；qq / spotify / acoustid / all 恒返回空，不要列进来。
const mtwResourceItems = [
  { title: '网易云 netease', value: 'netease' },
  { title: 'QQ 音乐 qmusic', value: 'qmusic' },
  { title: '酷我 kuwo', value: 'kuwo' },
  { title: '酷狗 kugou', value: 'kugou' },
  { title: '咪咕 migu', value: 'migu' },
  { title: 'MusicBrainz', value: 'musicbrainz' },
  { title: '智能 smart_tag', value: 'smart_tag' }
]

// 与后端 _effective_backend 同一套判断：auto 只是"看有没有填 MTW 地址"，不是第四种来源
const backend = computed(() => {
  const raw = String(config.value.source_backend || 'auto')
  if (raw === 'builtin' || raw === 'mtw') return raw
  return String(config.value.mtw_base_url || '').trim() ? 'mtw' : 'builtin'
})
const isMtw = computed(() => backend.value === 'mtw')

const resourceItems = computed(() => (isMtw.value ? mtwResourceItems : builtinResourceItems))

// 选中但当前来源不认识的源：后端会安静丢掉，这里先说破，否则用户只会觉得"怎么少了几个源"
const ignoredResources = computed(() => {
  const known = resourceItems.value.map(i => i.value)
  return toResourceList(config.value.mtw_resource).filter(v => !known.includes(v))
})

const ALL_SOURCES_TEXT = computed(
  () => resourceItems.value.map(i => i.title.split(' ')[0]).join(' / ')
)

const sourceNote = computed(() => {
  const picked = toResourceList(config.value.mtw_resource)
  if (picked.length) {
    const dropped = ignoredResources.value
    return `已选 ${picked.length} 个源，就按这个顺序问`
      + (dropped.length ? `；${dropped.join(' / ')} 只在另一侧来源有效，本轮会被忽略` : '')
  }
  return isMtw.value
    ? `留空 = 全部源（${ALL_SOURCES_TEXT.value}）`
    : `留空 = 内置源全部（${ALL_SOURCES_TEXT.value}）`
})

const modeNote = computed(() =>
  config.value.mode === 'force_all' ? '强制覆盖（当前）' : '只补缺（当前，推荐）'
)

function saveConfig() {
  // 只负责把配置交给宿主：宿主自己 PUT /plugin/{id} 并弹「已保存 / 保存失败」。
  // 这里再弹一条会出现两条通知，而且它在请求发出前就报成功，失败了也照样说已保存。
  emit('save', config.value)
}

onMounted(() => {
  // 侧栏版式需要比宿主默认的 60rem 更宽，宽度由宿主的配置框按这个事件设置
  emit('layout', { maxWidth: '74rem' })
})
</script>

<template>
  <v-card flat class="mls-card">
    <div class="mls-head">
      <v-avatar color="primary" variant="tonal" rounded="lg" size="40" class="mr-3">
        <v-icon icon="mdi-music-circle-outline" size="24" />
      </v-avatar>
      <div class="mls-head-text">
        <div class="text-h6">MusicTagWeb音乐库刮削 · 配置</div>
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
        <!-- 先讲整条流水线：知道某一环存在，才知道下面那几个开关在调什么 -->
        <div v-show="pane === 'flow'" class="mls-pane">
          <div class="mls-flow">
            <div v-for="(step, i) in FLOW_STEPS" :key="step.key" class="mls-flow-step">
              <div class="mls-flow-index">{{ i + 1 }}</div>
              <div class="mls-flow-body">
                <div class="text-body-2 font-weight-medium">
                  <v-icon :icon="step.icon" size="16" class="mr-1" />{{ step.title }}
                  <span v-if="step.knobs" class="mls-flow-knob">由「{{ step.knobs }}」决定</span>
                </div>
                <div class="text-caption text-grey">{{ step.desc }}</div>
              </div>
            </div>
          </div>

          <v-alert type="warning" variant="tonal" density="compact" class="mt-3">
            <div class="text-body-2">写进文件就回不去的三件事</div>
            <ul class="mls-list">
              <li>「最低匹配分数」调到 1 表示曲名对上就写入，翻唱、钢琴版、纯音乐同名都会被当成
                原曲写进文件，且无法回滚。</li>
              <li>「强制覆盖」会替换手工改过的曲名/艺人/专辑/年份/轨道号，也会重新下载并内嵌封面。</li>
              <li>识别来源选 MTW 时，MTW 与 MP 必须挂载同一份音乐库路径，否则查到的元数据会写到另一个文件上；
                内置源不发库内路径，没有这个约束。</li>
            </ul>
          </v-alert>
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
            <div class="mls-block-title">写入模式 · {{ modeNote }}</div>
            <div class="text-caption text-grey">
              只补缺：曲名+艺人+专辑+内嵌封面 四项齐全的文件按 mtime+size 记账直接跳过（一个检索请求都不发），
              缺哪项补哪项，已有非空标签保持不动，手工修改不会被覆盖。
            </div>
            <div class="text-caption text-grey">
              强制覆盖：每轮忽略增量账、全库重扫重问，候选与现有标签不同就重写，封面重新下载内嵌、歌词强制重写。
              候选里没有的字段不会清空，插件不认识的自定义注释字段也不会删。
            </div>
            <div class="text-caption text-grey">
              最终是否覆盖还取宿主「设置 → 媒体刮削」音乐三项策略与这里的较强者：宿主标签策略=覆盖时，
              即使这里选只补缺也会重写；=跳过时，两种模式都不写该项。宿主默认 标签仅缺失、封面仅缺失、歌词更优才换。
            </div>
          </div>

          <div class="mls-block">
            <div class="mls-block-title">执行周期</div>
            <div class="text-caption text-grey">
              留空按默认 0 3 * * sun（每周日 03:00）。星期编号按宿主的调度器算：0=周一…6=周日、7 视作周日，
              和 crontab 的 0/7=周日 差一位；保存后数据页概览会回显「下次触发在周几」，以它为准。
            </div>
            <div class="text-caption text-grey">
              非法表达式不会让插件挂掉：会记一条日志并回落默认值，但你自己配的时刻会一起丢掉。
            </div>
          </div>

          <v-switch v-model="config.onlyonce" label="保存后立即运行一次" hide-details color="primary" inset class="mt-1" />
        </div>

        <!-- 识别与匹配 -->
        <div v-show="pane === 'match'" class="mls-pane">
          <v-row dense>
            <v-col cols="12" md="4">
              <v-select v-model="config.source_backend" :items="backendItems"
                        label="识别来源" hide-details />
            </v-col>
            <v-col cols="12" md="8">
              <v-text-field
                v-model="config.mtw_base_url"
                :label="isMtw ? 'Music Tag Web 地址（MTW 模式必填）' : 'Music Tag Web 地址（内置源不需要）'"
                placeholder="http://192.168.x.x:8001"
                hide-details
              />
            </v-col>
          </v-row>

          <v-row dense class="mt-1" v-if="isMtw">
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

          <v-row dense class="mt-1">
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
          </v-row>

          <v-switch
            v-model="config.fill_lyrics"
            label="缺失时主动抓取歌词回填（仅内置源 netease / qmusic）"
            hide-details
            color="primary"
            inset
            class="mt-1"
          />

          <v-select
            v-model="config.mtw_resource"
            :items="resourceItems"
            :label="isMtw ? 'MTW 识别源（可多选，留空=全部）' : '内置识别源（可多选，留空=全部）'"
            multiple
            chips
            closable-chips
            density="compact"
            class="mt-1"
          />
          <div class="text-caption text-grey">{{ sourceNote }}。</div>

          <div class="mls-block">
            <div class="mls-block-title">元数据来源</div>
            <div class="text-caption text-grey">
              内置源：插件自己直连 {{ builtinResourceItems.length }} 个音乐平台的公开检索接口
              （{{ builtinResourceItems.map(i => i.value).join(' / ') }}），不需要为刮削单独部署 Music Tag Web；
              发给平台的只有清洗后的「曲名-艺人」检索词，库内路径不出本机。
            </div>
            <div class="text-caption text-grey">
              Music Tag Web：沿用 MTW 的识别结果，除了内置那几个源，还能用酷我与 smart_tag。
              已经部署了 MTW 的装机选这条（或保持「自动」）行为与之前完全一致；
              这条会把库内完整路径作为参考一起发给 MTW，所以 MTW 与 MP 必须挂同一份库。
            </div>
            <div class="text-caption text-grey">
              歌词回填开关默认关闭：歌词仍由宿主刮削链自己取（lrclib / AMLL）。
              打开后只在内置源模式下生效 —— 选中候选缺歌词时，插件主动从 netease / qmusic 抓歌词回填（MTW 模式无此能力）。
              qq / spotify / acoustid / all 实测恒返回空候选，两种来源都没有列进来。
            </div>
          </div>

          <div class="mls-block">
            <div class="mls-block-title">查询与选取</div>
            <div class="text-caption text-grey">
              每个文件按源顺序逐源查询，某源出现 3/3 满分候选就立刻停止，不再问后面的源；都没满分则合并所有候选取最高分。
              分数为曲名 / 艺人 / 专辑 各 1 分（先做繁简与大小写归一，互相包含即算命中）；同分时按源顺序、同源内按来源
              返回顺序取第一个。最高分仍低于「最低匹配分数」（默认 3，即要求三项全中）就记为失败、不写入。
            </div>
            <div class="text-caption text-grey">
              这个阈值只在 1-3 之间生效，越界会按最近的一界执行。用于匹配的本地身份优先读文件已有的 tag，
              缺哪项就按 艺人/专辑 (年)/NN - 曲名 的目录结构兜底；只有严格位于「音乐库路径」之下的目录才算艺人/专辑，
              库根本身的名字不参与猜测 —— 猜不出来就留空，宁可判「未达阈值」也不拿目录名凑一个假身份去改写标签。
            </div>
          </div>
        </div>

        <!-- 音乐库范围 -->
        <div v-show="pane === 'library'" class="mls-pane">
          <v-textarea
            v-model="config.scraper_paths"
            :rows="7"
            label="音乐库路径（一行一个目录）"
            placeholder="/media/music/周杰伦&#10;/media/music"
            hide-details
          />
          <v-textarea v-model="config.exclude_paths" :rows="3" label="排除路径" hide-details class="mt-2" />

          <div class="mls-block">
            <div class="mls-block-title">会处理的文件</div>
            <div class="text-caption text-grey">
              只扫描宿主音频白名单（设置 → 媒体刮削 → 音频扩展名）与本插件支持列表的交集：宿主不收的扩展名写标签时
              会被原生刮削链直接拒成「刮削路径中没有支持的音频文件」，先排除掉才不会白打请求、又把文件记成失败并烧满重试次数。
            </div>
            <div class="text-caption text-grey">
              Jellyfin 的 .trickplay 预览缓存目录整棵跳过；目录软链不跟随，避免回环目录把这一轮卡死。
              指向库外的音频软链同样会跳过并在日志里报数量 —— 写标签是按路径落到真实文件的，
              让扫描改到音乐库之外的文件不可接受；确有需要请把它指向的目录也加进库路径。
            </div>
            <div class="text-caption text-grey">
              路径必须写到实际挂载的目录：只有严格位于「音乐库路径」之下的目录才按 艺人/专辑 解释，
              库根本身的名字不算专辑名。识别来源用 MTW 时，同一份库还需要 MTW 与 MP 挂载到相同路径
              （MTW 会收到完整路径作参考）；内置源不受这条约束。
            </div>
          </div>
        </div>
      </section>
    </div>

    <v-divider />
    <div class="mls-actions">
      <v-btn variant="text" class="text-none" prepend-icon="mdi-database-eye-outline" @click="emit('switch')">
        查看数据页
      </v-btn>
      <v-spacer />
      <v-btn color="primary" variant="flat" prepend-icon="mdi-content-save" class="text-none" @click="saveConfig">
        保存配置
      </v-btn>
    </div>
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.mls-card { width: 100%; display: flex; flex-direction: column; }
.mls-head { display: flex; align-items: center; gap: 8px; padding: 14px 18px; }
.mls-head-text { min-width: 0; }
.mls-body { display: flex; min-height: 400px; }
.mls-nav {
  flex: 0 0 176px;
  border-right: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  background: rgba(var(--v-theme-on-surface), .02);
}
.mls-nav-item { margin: 2px 8px; }
.mls-content { flex: 1 1 auto; min-width: 0; overflow-y: auto; }
.mls-pane { padding: 16px 18px; }
.mls-block { margin-top: 16px; }
.mls-block-title { font-size: 13px; font-weight: 600; margin-bottom: 4px; opacity: .8; }
.mls-list { margin: 0; padding-left: 18px; }
.mls-list li { margin: 2px 0; font-size: 13px; }
.mls-flow { display: flex; flex-direction: column; gap: 10px; }
.mls-flow-step { display: flex; gap: 10px; align-items: flex-start; }
.mls-flow-index {
  flex: 0 0 22px; height: 22px; border-radius: 50%; font-size: 12px;
  display: flex; align-items: center; justify-content: center; margin-top: 3px;
  background: rgba(var(--v-theme-primary), .14); color: rgb(var(--v-theme-primary)); font-weight: 700;
}
.mls-flow-body { min-width: 0; }
.mls-flow-knob { font-size: 11px; color: rgb(var(--v-theme-primary)); margin-left: 8px; opacity: .85; }
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
