<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useApi } from './useApi'
import { type CommandRow } from './types'

/**
 * 配置页的口令表面板：逐类开关决定哪句话会被插件接手。
 *
 * 放在配置页而不是侧栏，因为它是「改完才生效的设置」，不是运行情况。
 */
const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' }
})

const emit = defineEmits(['changed'])

const { busy, snackbar, notify, post, get } = useApi(props, () => emit('changed'))
const rows = ref<CommandRow[]>([])
const loaded = ref(false)

async function load() {
  try {
    const res = await get('/commands')
    rows.value = res?.items || []
    loaded.value = true
  } catch (e: any) {
    notify(`口令表读取失败：${e?.message || e}`, 'error')
  }
}

async function save() {
  const res = await post('/commands', {
    enabled: rows.value.filter(row => row.enabled).map(row => row.kind)
  }, '保存口令表')
  if (res?.items) rows.value = res.items
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
        启用 {{ rows.filter(r => r.enabled).length }}/{{ rows.length }} 类
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
      监听开关、轮询间隔与 AI 兜底在上面「语音控制」那组里改。
    </div>
    <v-card variant="outlined" class="xcc-card pa-2">
      <div v-for="row in rows" :key="row.kind" class="xcc-row">
        <v-switch v-model="row.enabled" color="primary" hide-details inset density="compact"
                  :label="row.label" style="flex: 0 0 168px" />
        <span class="text-caption text-grey xcc-keywords">{{ row.keywords.join('、') }}</span>
      </div>
      <div v-if="!rows.length" class="text-caption text-grey pa-2">
        {{ loaded ? '后端没有给出口令表。' : '正在读取口令表…' }}
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
.xcc-card { border-radius: 14px; }
.xcc-row {
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  padding: 2px 8px; border-bottom: 1px dashed var(--xcc-line);
}
.xcc-row:last-child { border-bottom: none; }
.xcc-keywords { flex: 1 1 220px; min-width: 0; }
</style>
