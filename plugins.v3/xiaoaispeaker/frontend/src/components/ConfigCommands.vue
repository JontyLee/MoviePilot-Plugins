<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useApi } from './useApi'
import {
  type CommandKindOption,
  type CommandRow,
  type CommandWord,
  why
} from './types'

/**
 * 配置页的口令表面板：类型开关 + 用户自己编辑的口令。
 *
 * 放在配置页而不是侧栏，因为它是「改完才生效的设置」，不是运行情况。
 * 编辑只开放内置引擎本来就支持的两件事：加自己的说法、把内置的说法藏起来 ——
 * 匹配按最长关键词优先，自定义词和内置词抢同一句时更长的那条赢，不需要额外规则。
 */
const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' }
})

const emit = defineEmits(['changed'])

const { busy, snackbar, notify, post, get, raw } = useApi(props, () => emit('changed'))
const rows = ref<CommandRow[]>([])
const customs = ref<CommandWord[]>([])
const kinds = ref<CommandKindOption[]>([])
const loaded = ref(false)

// 新加的那条：类型换了，可选子参数与「内容」是不是必填都要跟着换
const draft = ref({ keyword: '', kind: '', param: '', argument: '' })

const draftKind = computed(() => kinds.value.find(item => item.kind === draft.value.kind))
const draftParams = computed(() => draftKind.value?.params || [])
const draftSingleParam = computed(() => draftParams.value.length <= 1)

// 整句说完还播不出东西的类型，与后端 _WORD_NEEDS_ARGUMENT 同一套
const NEEDS_ARGUMENT = ['play_song', 'play_artist', 'play_playlist', 'play_index', 'tts']
const draftNeedsArgument = computed(() => {
  const kind = draftKind.value
  if (!kind) return false
  return NEEDS_ARGUMENT.includes(kind.kind)
    || (kind.kind === 'set_volume' && draft.value.param === 'absolute')
})

const enabledCount = computed(() => rows.value.filter(row => row.enabled).length)
const hiddenCount = computed(() => rows.value.reduce((sum, row) => sum + row.hidden.length, 0))

function paramsOf(kind: string): CommandKindOption['params'] {
  return kinds.value.find(item => item.kind === kind)?.params || []
}

/** 判定回执里别露 play_playlist 这种内部值：类型中文名后端已经随口令表一起给了。 */
function kindLabel(kind: string) {
  return kinds.value.find(item => item.kind === kind)?.label || kind || '未知'
}

function resetDraft(keepKind = false) {
  const kind = keepKind ? draft.value.kind : (kinds.value[0]?.kind || '')
  draft.value = { keyword: '', kind, param: paramsOf(kind)[0]?.value || '', argument: '' }
}

async function load() {
  try {
    const res = await get('/commands')
    rows.value = res?.items || []
    customs.value = res?.customs || []
    kinds.value = res?.kinds || []
    if (!draft.value.kind && kinds.value.length) resetDraft()
    loaded.value = true
  } catch (e: any) {
    notify(`口令表读取失败：${why(e)}`, 'error')
  }
}

/** 后端每改一次都回全表：直接用它刷新，省一次往返也不会读到改之前的版本。 */
function apply(res: any) {
  if (!res) return
  if (res.items) rows.value = res.items
  if (res.customs) customs.value = res.customs
  if (res.kinds) kinds.value = res.kinds
}

async function save() {
  apply(await post('/commands', {
    enabled: rows.value.filter(row => row.enabled).map(row => row.kind)
  }, '保存口令表'))
}

async function addWord() {
  const keyword = draft.value.keyword.trim()
  if (!keyword || !draft.value.kind) {
    notify('把「要说的话」和它的类型填上', 'warning')
    return
  }
  if (draftNeedsArgument.value && !draft.value.argument.trim()) {
    notify('这类口令说完整句还得补上内容（歌名、歌手、歌单…），不然播不出东西', 'warning')
    return
  }
  const res = await post('/commands', {
    add: [{
      keyword,
      kind: draft.value.kind,
      param: draft.value.param,
      argument: draft.value.argument.trim()
    }]
  }, '添加口令')
  if (res && res.ok !== false) {
    resetDraft(true)
    void probe(keyword)
  }
  apply(res)
}

async function dropWord(keyword: string) {
  apply(await post('/commands', { remove: [keyword] }, '删口令'))
}

async function toggleKeyword(row: CommandRow, keyword: string) {
  const hiding = !row.hidden.includes(keyword)
  apply(await post('/commands',
    hiding ? { hide: [keyword] } : { unhide: [keyword] },
    hiding ? '藏起口令' : '还原口令'))
}

async function restoreAll() {
  apply(await post('/commands', { reset: true }, '恢复默认口令表'))
  resetDraft()
}

// 加完立刻本地判一次：和音箱那句话走同一条判定链，用户当场看得到它接不接手
const lastProbe = ref<{ text: string; ok: boolean; kind: string; message: string } | null>(null)

async function probe(text: string) {
  const res = await raw('/voice/test', { text, execute: false }, '试口令')
  lastProbe.value = res ? {
    text,
    ok: res.ok !== false,
    kind: res.kind || '',
    message: res.message || ''
  } : null
}

onMounted(load)

defineExpose({ load })
</script>

<template>
  <div class="xcc">
    <div class="xcc-title">
      <v-icon icon="mdi-microphone-message" size="20" class="xcc-mark" />
      语音口令表
      <span v-if="loaded" class="text-caption text-grey ml-2">
        启用 {{ enabledCount }}/{{ rows.length }} 类｜自定义 {{ customs.length }} 条
        <template v-if="hiddenCount">｜已藏 {{ hiddenCount }} 个内置词</template>
      </span>
      <v-spacer />
      <v-btn size="x-small" variant="text" class="text-none" prepend-icon="mdi-refresh"
             @click="load">重新读取
      </v-btn>
      <v-btn size="small" variant="tonal" color="primary" :loading="busy === '保存口令表'"
             prepend-icon="mdi-content-save" @click="save">保存开关
      </v-btn>
    </div>
    <div class="text-caption text-grey mb-2">
      关掉某一类，那句话就不会被接手（会留给小爱自己的回答）。「停止播放」这类保底口令不受这里影响。
      监听开关、轮询间隔与 AI 兜底在左边「语音与 AI」那组里改。
    </div>

    <!-- 一、自定义口令：内置表里没有的说法在这里补 -->
    <div class="xcc-block-title">自定义口令</div>
    <v-card variant="outlined" class="xcc-card pa-3">
      <div class="d-flex" style="gap: 8px; flex-wrap: wrap; align-items: center">
        <v-text-field v-model="draft.keyword" density="compact" hide-details clearable
                      label="要说的话" placeholder="例：播放周杰伦"
                      style="min-width: 170px; flex: 1 1 190px" @keyup.enter="addWord" />
        <v-select v-model="draft.kind" :items="kinds" item-title="label" item-value="kind"
                  density="compact" hide-details label="判成哪种" variant="outlined"
                  style="min-width: 140px; flex: 0 1 150px"
                  @update:model-value="draft.param = draftParams[0]?.value || ''" />
        <v-select v-if="!draftSingleParam" v-model="draft.param" :items="draftParams"
                  item-title="label" item-value="value" density="compact" hide-details
                  label="子参数" variant="outlined" style="min-width: 130px; flex: 0 1 140px" />
        <v-text-field v-model="draft.argument" density="compact" hide-details clearable
                      :label="draftNeedsArgument ? '内容（必填）' : '内容（可留空）'"
                      placeholder="例：周杰伦"
                      style="min-width: 150px; flex: 1 1 170px" @keyup.enter="addWord" />
        <v-btn size="small" variant="flat" color="primary" :loading="busy === '添加口令'"
               prepend-icon="mdi-plus" @click="addWord">加这条
        </v-btn>
      </div>
      <div class="text-caption text-grey mt-2">
        「内容」是<b>整句说完时</b>要播的东西：口令「播放周杰伦」+ 内容「周杰伦」，说这句就播周杰伦；
        同一句后面再带词（「播放周杰伦 稻香」）就播带的那个「稻香」。
        自定义词和内置词一起按<b>最长关键词</b>抢，更长的那条赢。
      </div>

      <div v-if="customs.length" class="xcc-words mt-3">
        <div v-for="word in customs" :key="word.keyword" class="xcc-word">
          <span class="xcc-word-key">{{ word.keyword }}</span>
          <v-chip size="x-small" variant="tonal" color="primary" class="text-none">
            {{ word.label }}<template v-if="word.param && word.param_label">
              · {{ word.param_label }}</template>
          </v-chip>
          <span v-if="word.argument" class="text-caption text-grey">内容 {{ word.argument }}</span>
          <v-spacer />
          <v-btn size="x-small" variant="text" class="text-none xcc-del" icon="mdi-close"
                 @click="dropWord(word.keyword)" />
        </div>
      </div>
      <div v-else class="text-caption text-grey mt-3">
        {{ loaded ? '还没有自定义口令。内置的说法不够用时，在上面加一条。' : '正在读取口令表…' }}
      </div>

      <div v-if="lastProbe" class="xcc-probe">
        「{{ lastProbe.text }}」{{ lastProbe.ok ? '现在会被接手' : '还是没被接手' }}
        <template v-if="lastProbe.kind">｜判成 {{ kindLabel(lastProbe.kind) }}</template>
        <span class="text-grey">{{ lastProbe.message ? `｜${lastProbe.message}` : '' }}</span>
      </div>
    </v-card>

    <!-- 二、内置口令：整类开关，外加逐个词藏起来 -->
    <div class="xcc-block-title">
      内置口令
      <span class="text-caption text-grey font-weight-regular">
        点一个词就把它藏起来（那句话不再由这个口令接手），再点一下还原
      </span>
    </div>
    <v-card variant="outlined" class="xcc-card pa-2">
      <div v-for="row in rows" :key="row.kind" class="xcc-row">
        <v-switch v-model="row.enabled" color="primary" hide-details inset density="compact"
                  :label="row.label" style="flex: 0 0 168px" />
        <div class="xcc-keywords">
          <button v-for="keyword in row.keywords" :key="keyword" type="button"
                  class="xcc-chip" :class="{ 'xcc-chip-off': row.hidden.includes(keyword) }"
                  :title="row.hidden.includes(keyword) ? '点这里还原' : '点这里藏起来'"
                  @click="toggleKeyword(row, keyword)">
            {{ keyword }}
            <v-icon v-if="row.hidden.includes(keyword)" icon="mdi-eye-off-outline" size="12" />
          </button>
        </div>
      </div>
      <div v-if="!rows.length" class="text-caption text-grey pa-2">
        {{ loaded ? '后端没有给出口令表。' : '正在读取口令表…' }}
      </div>
      <div v-if="loaded && (hiddenCount || customs.length)" class="xcc-foot">
        <v-spacer />
        <v-btn size="x-small" variant="text" class="text-none" color="error"
               prepend-icon="mdi-restore" :loading="busy === '恢复默认口令表'"
               @click="restoreAll">整套恢复默认
        </v-btn>
      </div>
    </v-card>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </div>
</template>

<style scoped>
/* 仅插件内局部样式；含 .v- 的选择器会被构建的 postcss 门禁整条删掉。配色取宿主变量。 */
.xcc {
  display: flex; flex-direction: column;
  --xcc-ink: rgba(var(--v-theme-on-surface), .92);
  --xcc-ink-faint: rgba(var(--v-theme-on-surface), .5);
  --xcc-line: rgba(var(--v-border-color), var(--v-border-opacity));
}
.xcc-title {
  display: flex; align-items: center; gap: 8px; margin-bottom: 6px;
  font-size: 13px; font-weight: 650; letter-spacing: .3px; color: var(--xcc-ink);
}
.xcc-mark { color: rgb(var(--v-theme-primary)); opacity: .85; }
.xcc-block-title {
  display: flex; align-items: baseline; gap: 8px; flex-wrap: wrap;
  margin: 14px 0 6px; font-size: 12px; font-weight: 650; letter-spacing: .6px;
  color: var(--xcc-ink);
}
.xcc-block-title::before {
  content: ''; width: 3px; height: 12px; border-radius: 2px; align-self: center;
  background: linear-gradient(180deg, rgb(var(--v-theme-primary)), rgba(var(--v-theme-primary), .35));
}
.xcc-card { border-radius: 14px; }
.xcc-row {
  display: flex; align-items: flex-start; gap: 10px; flex-wrap: wrap;
  padding: 4px 8px; border-bottom: 1px dashed var(--xcc-line);
}
.xcc-row:last-of-type { border-bottom: none; }
.xcc-keywords { display: flex; flex-wrap: wrap; gap: 4px; flex: 1 1 220px; min-width: 0; }
.xcc-chip {
  border: 1px solid var(--xcc-line); background: transparent; cursor: pointer;
  border-radius: 999px; padding: 1px 9px; font-size: 12px; line-height: 18px;
  color: var(--xcc-ink); transition: border-color .16s, color .16s, background .16s;
}
.xcc-chip:hover { border-color: rgb(var(--v-theme-primary)); color: rgb(var(--v-theme-primary)); }
.xcc-chip-off {
  text-decoration: line-through; color: var(--xcc-ink-faint);
  border-style: dashed; background: rgba(var(--v-theme-on-surface), .04);
}
.xcc-words { display: flex; flex-direction: column; }
.xcc-word {
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  padding: 5px 2px; border-bottom: 1px dashed var(--xcc-line);
}
.xcc-word:last-child { border-bottom: none; }
.xcc-word-key {
  font-size: 13px; font-weight: 600; color: var(--xcc-ink);
  padding: 1px 8px; border-radius: 8px; background: rgba(var(--v-theme-primary), .1);
}
.xcc-del { color: var(--xcc-ink-faint); }
.xcc-probe {
  margin-top: 8px; padding: 6px 10px; border-radius: 10px; font-size: 12px;
  background: rgba(var(--v-theme-on-surface), .05); color: var(--xcc-ink);
  border: 1px solid var(--xcc-line);
}
.xcc-foot { display: flex; align-items: center; padding-top: 6px; }
@media (prefers-reduced-motion: reduce) {
  .xcc-chip { transition: none; }
}
</style>
