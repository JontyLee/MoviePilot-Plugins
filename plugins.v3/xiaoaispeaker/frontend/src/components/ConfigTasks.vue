<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useApi } from './useApi'
import {
  type Device,
  type PlaylistRow,
  type TaskRow,
  MODE_ITEMS,
  TASK_ACTIONS,
  WEEK_DAYS,
  taskNeedsPlaylist
} from './types'

/**
 * 配置页的定时任务面板：任务怎么定义（做什么、哪几天、哪台音箱、播什么）属于配置，
 * 到点跑成什么样、日志、立即执行都留在侧栏的运行视图里看。
 */
const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  devices: { type: Array as () => Device[], default: () => [] }
})

const emit = defineEmits(['changed'])

const { busy, snackbar, notify, post, get } = useApi(props, () => emit('changed'))

const tasks = ref<TaskRow[]>([])
const playlists = ref<PlaylistRow[]>([])
const taskActions = TASK_ACTIONS
const weekDays = WEEK_DAYS
const monthDays = Array.from({ length: 31 }, (_unused, i) => i + 1)
const deviceItems = computed(() => props.devices.map(item => ({
  title: item.name || item.alias || item.device_id,
  value: item.device_id
})))

function emptyTask() {
  return {
    name: '',
    enabled: true,
    action: 'play_playlist',
    schedule: { type: 'weekly', time: '07:00', weekdays: [1, 2, 3, 4, 5] as number[], monthdays: [1] as number[] },
    target: { all: false, device_ids: [] as string[] },
    params: { playlist_name: '', song_name: '', start: 'first', play_mode: '',
              volume: null as number | null, stop_after_minutes: null as number | null }
  }
}

const form = ref(emptyTask())
const editingTask = ref('')

function newTask() {
  editingTask.value = ''
  form.value = emptyTask()
}

function editTask(row: TaskRow) {
  editingTask.value = row.id
  form.value = {
    name: row.name,
    enabled: row.enabled,
    action: row.action,
    schedule: {
      type: row.schedule?.type || 'weekly',
      time: row.schedule?.time || '07:00',
      weekdays: [...(row.schedule?.weekdays || [])],
      monthdays: [...(row.schedule?.monthdays || [])]
    },
    target: { all: !!row.target?.all, device_ids: [...(row.target?.device_ids || [])] },
    params: {
      playlist_name: row.params?.playlist_name || '',
      song_name: row.params?.song_name || '',
      start: row.params?.start || 'first',
      play_mode: row.params?.play_mode || '',
      volume: row.params?.volume ?? null,
      stop_after_minutes: row.params?.stop_after_minutes ?? null
    }
  }
}

function toggleIn(list: number[], value: number) {
  const at = list.indexOf(value)
  if (at >= 0) list.splice(at, 1)
  else list.push(value)
}

async function load() {
  try {
    const [taskRes, playlistRes] = await Promise.all([get('/tasks'), get('/playlists')])
    tasks.value = taskRes?.items || []
    playlists.value = playlistRes?.items || []
  } catch (e: any) {
    notify(`定时任务读取失败：${e?.message || e}`, 'error')
  }
}

async function saveTask() {
  const draft = form.value
  if (!draft.name.trim()) {
    notify('先给任务起个名字', 'warning')
    return
  }
  if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(draft.schedule.time.trim())) {
    notify('时间要写成 HH:MM，比如 07:00', 'warning')
    return
  }
  const key = draft.schedule.type === 'monthly' ? 'monthdays' : 'weekdays'
  if (!draft.schedule[key].length) {
    notify(draft.schedule.type === 'monthly' ? '勾出每月哪几天' : '勾出每周哪几天', 'warning')
    return
  }
  if (taskNeedsPlaylist(draft.action) && !draft.params.playlist_name.trim()) {
    notify('播放歌单的任务要先选中歌单（歌单来自曲库索引）', 'warning')
    return
  }
  const res = await post('/tasks', {
    id: editingTask.value || undefined,
    name: draft.name.trim(),
    enabled: draft.enabled,
    action: draft.action,
    schedule: { type: draft.schedule.type, time: draft.schedule.time.trim(),
                weekdays: [...draft.schedule.weekdays].sort((a, b) => a - b),
                monthdays: [...draft.schedule.monthdays].sort((a, b) => a - b) },
    target: { all: draft.target.all, device_ids: draft.target.all ? [] : [...draft.target.device_ids] },
    params: { ...draft.params }
  }, '保存定时任务')
  if (res && res.ok !== false) {
    editingTask.value = ''
    form.value = emptyTask()
    await load()
  }
}

function removeTask(row: TaskRow) {
  return post('/tasks', { id: row.id, remove: true }, '删除定时任务').then(async () => {
    if (editingTask.value === row.id) newTask()
    await load()
  })
}

function toggleTask(row: TaskRow) {
  return post('/tasks', { ...row, enabled: !row.enabled }, row.enabled ? '停用任务' : '启用任务')
}

onMounted(load)

defineExpose({ load })
</script>

<template>
  <div class="xct">
    <div class="xct-title">
      <v-icon icon="mdi-clock-outline" size="20" class="xct-mark" />
      定时任务
      <span class="text-caption text-grey ml-2">{{ tasks.length }} 条</span>
      <v-spacer />
      <v-btn size="x-small" variant="text" class="text-none" prepend-icon="mdi-refresh"
             @click="load">重新读取
      </v-btn>
    </div>
    <div class="text-caption text-grey mb-2">
      到点由插件的周期巡检触发（20 秒一次，同一分钟只跑一次）。执行情况、日志和「立即执行」在侧栏的
      「定时任务」栏看；这里只管怎么定义。播放类任务要音箱在线、且配好了「外部访问地址」。
    </div>

    <v-card v-for="row in tasks" :key="row.id" variant="outlined" class="xct-card pa-3 mb-2">
      <div class="d-flex align-center" style="gap: 10px; flex-wrap: wrap">
        <v-switch :model-value="row.enabled" hide-details density="compact" color="primary"
                  :loading="busy === '启用任务' || busy === '停用任务'"
                  @update:model-value="toggleTask(row)" />
        <div style="min-width: 0; flex: 1 1 220px">
          <div class="text-body-2">{{ row.name }}
            <v-chip size="x-small" variant="tonal" class="ml-1">{{ row.action_text }}</v-chip>
          </div>
          <div class="text-caption text-grey">
            {{ row.rule }}｜{{ row.target.all ? '全部在线音箱'
               : (row.target.device_ids.length ? `${row.target.device_ids.length} 台指定音箱` : '默认音箱') }}
            <template v-if="taskNeedsPlaylist(row.action)">
              ｜歌单 {{ row.params?.playlist_name || row.params?.playlist_id || '（未选）' }}
            </template>
          </div>
        </div>
        <v-btn size="x-small" variant="text" @click="editTask(row)">编辑</v-btn>
        <v-btn size="x-small" variant="text" color="error" :loading="busy === '删除定时任务'"
               @click="removeTask(row)">删除
        </v-btn>
      </div>
    </v-card>
    <div v-if="!tasks.length" class="text-caption text-grey mb-2">还没有任务，下面建一条。</div>

    <v-card variant="outlined" class="xct-card pa-3">
      <div class="xct-sub">{{ editingTask ? `编辑任务「${editingTask}」` : '新建任务' }}</div>
      <div class="d-flex" style="gap: 8px; flex-wrap: wrap">
        <v-text-field v-model="form.name" density="compact" hide-details clearable label="任务名"
                      placeholder="早上起床放歌" style="min-width: 180px; flex: 1 1 180px" />
        <v-select v-model="form.action" :items="taskActions" density="compact" hide-details
                  label="做什么" style="min-width: 160px" />
        <v-text-field v-model="form.schedule.time" density="compact" hide-details
                      label="时间 HH:MM" placeholder="07:00" style="width: 110px" />
        <v-btn-toggle v-model="form.schedule.type" density="compact" mandatory>
          <v-btn value="weekly" size="small" variant="text">按星期</v-btn>
          <v-btn value="monthly" size="small" variant="text">按几号</v-btn>
        </v-btn-toggle>
        <v-checkbox v-model="form.enabled" label="启用" density="compact" hide-details />
      </div>

      <div class="xct-days mt-2">
        <template v-if="form.schedule.type === 'weekly'">
          <v-chip v-for="(label, day) in weekDays" :key="day" size="small"
                  :variant="form.schedule.weekdays.includes(day) ? 'flat' : 'outlined'"
                  :color="form.schedule.weekdays.includes(day) ? 'primary' : 'default'"
                  @click="toggleIn(form.schedule.weekdays, day)">{{ label }}
          </v-chip>
        </template>
        <template v-else>
          <v-chip v-for="day in monthDays" :key="day" size="small"
                  :variant="form.schedule.monthdays.includes(day) ? 'flat' : 'outlined'"
                  :color="form.schedule.monthdays.includes(day) ? 'primary' : 'default'"
                  @click="toggleIn(form.schedule.monthdays, day)">{{ day }}
          </v-chip>
        </template>
      </div>

      <div class="d-flex align-center mt-2" style="gap: 8px; flex-wrap: wrap">
        <v-checkbox v-model="form.target.all" label="全部在线音箱" density="compact" hide-details />
        <v-combobox v-if="!form.target.all" v-model="form.target.device_ids" :items="deviceItems"
                    item-title="title" item-value="value" density="compact" hide-details multiple chips
                    closable-chips label="指定音箱（留空=默认音箱）"
                    style="min-width: 240px; flex: 1 1 240px" />
      </div>

      <div v-if="taskNeedsPlaylist(form.action)" class="d-flex mt-2" style="gap: 8px; flex-wrap: wrap">
        <v-combobox v-model="form.params.playlist_name" :items="playlists.map(p => p.name)"
                    density="compact" hide-details clearable label="歌单（来自曲库索引）"
                    placeholder="范特西" style="min-width: 200px; flex: 1 1 200px" />
        <v-text-field v-if="form.action === 'play_playlist_from'" v-model="form.params.song_name"
                      density="compact" hide-details clearable label="从这首歌开始" placeholder="稻香"
                      style="min-width: 160px; flex: 1 1 160px" />
        <v-select v-if="form.action === 'play_playlist'" v-model="form.params.start"
                  :items="[{ title: '从第一首', value: 'first' }, { title: '随机起点', value: 'random' }]"
                  density="compact" hide-details label="起点" style="width: 130px" />
      </div>

      <div class="d-flex mt-2" style="gap: 8px; flex-wrap: wrap">
        <v-select v-model="form.params.play_mode"
                  :items="[{ title: '跟随上次', value: '' }, ...MODE_ITEMS]" density="compact"
                  hide-details label="播放方式" style="min-width: 150px" />
        <v-text-field v-if="form.action === 'set_volume' || taskNeedsPlaylist(form.action)"
                      v-model.number="form.params.volume" type="number" min="0" max="100"
                      density="compact" hide-details label="音量 0-100" style="width: 120px" />
        <v-text-field v-if="taskNeedsPlaylist(form.action)" v-model.number="form.params.stop_after_minutes"
                      type="number" min="0" density="compact" hide-details label="播多久后停（分钟）"
                      style="width: 160px" />
        <v-btn color="primary" variant="flat" class="text-none" size="small"
               :loading="busy === '保存定时任务'" @click="saveTask">
          {{ editingTask ? '保存修改' : '创建任务' }}
        </v-btn>
        <v-btn size="small" variant="text" @click="newTask">清空</v-btn>
      </div>
      <div class="text-caption text-grey mt-1">
        法定节假日的调休判定没有做（上游把假期表编在插件产物里，插件装在宿主容器里没有那份数据），
        要「只工作日 / 只周末」就分建两条任务勾不同的周几。
      </div>
    </v-card>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </div>
</template>

<style scoped>
/* 仅插件内局部样式；含 .v- 的选择器会被构建的 postcss 门禁整条删掉。配色取宿主变量。 */
.xct {
  display: flex; flex-direction: column;
  --xct-ink: rgba(var(--v-theme-on-surface), .92);
  --xct-line: rgba(var(--v-border-color), var(--v-border-opacity));
}
.xct-title {
  display: flex; align-items: center; gap: 8px; margin-bottom: 6px;
  font-size: 13px; font-weight: 650; letter-spacing: .3px; color: var(--xct-ink);
}
.xct-mark { color: rgb(var(--v-theme-primary)); opacity: .85; }
.xct-sub { font-size: 12px; font-weight: 650; margin-bottom: 8px; color: var(--xct-ink); }
.xct-card { border-radius: 14px; }
.xct-days { display: flex; gap: 6px; flex-wrap: wrap; }
</style>
