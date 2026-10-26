<script setup lang="ts">
import { computed, ref, onMounted, onBeforeUnmount } from 'vue'
import ConfigForm from './ConfigForm.vue'
import ConfigLogin from './ConfigLogin.vue'
import ConfigCommands from './ConfigCommands.vue'
import ConfigTasks from './ConfigTasks.vue'
import { useApi } from './useApi'
import { type Device, type Status, why } from './types'

const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  // 宿主存的配置不作为初值：密码不在里面，而 /config 是「缺省即保留」的语义，
  // 拿一份不含密码的草稿去保存会把已存的账密一起动到。各面板自己从服务端起头。
  initialConfig: { type: Object as any, default: () => ({}) },
  nativeSubscribe: { type: Function as any, default: null },
  // 侧栏全页自己开的对话框要有「关闭」，宿主开的对话框自己有关闭按钮
  showClose: { type: Boolean, default: false },
  // 宿主配置对话框里「切到播放台」才有效；侧栏对话框本来就在播放台上
  showSwitch: { type: Boolean, default: false }
})

const emit = defineEmits(['save', 'close', 'switch', 'layout', 'updated'])

const { snackbar, notify, get } = useApi(props, () => loadStatus())
const status = ref<Status | null>(null)
const loaded = ref(false)

// 六栏：登录与音箱排第一，是因为后面每一栏都要作用在「一台能播的音箱」上；
// 但不再拿它当闸门 —— 没登录也让人先把库路径与偏好填好，切回去登录就生效。
const panes = [
  { key: 'account', title: '账号与音箱', icon: 'mdi-account-sync-outline',
    desc: '登录小米账号（账密 / 扫码 / Cookie）、选默认音箱与设备分组' },
  { key: 'library', title: '曲库与直链', icon: 'mdi-folder-music-outline',
    desc: '哪些目录算音乐库、音箱从哪个地址取流、什么格式能播' },
  { key: 'play', title: '播放行为', icon: 'mdi-sine-wave',
    desc: '走哪条播放接口、推之前设音量、连播与灯效' },
  { key: 'voice', title: '语音与 AI', icon: 'mdi-microphone-message',
    desc: '监听哪几台音箱的对话、命中后怎么打断与播报、AI 兜底' },
  { key: 'commands', title: '语音口令表', icon: 'mdi-key-variant',
    desc: '每一类口令开不开放、内置说法藏不藏，还能加自己的说法' },
  { key: 'tasks', title: '定时任务', icon: 'mdi-clock-outline',
    desc: '到点播什么、哪几天、哪台音箱、什么播放方式' }
] as const
type PaneKey = typeof panes[number]['key']
const pane = ref<PaneKey>('account')
const current = computed(() => panes.find(item => item.key === pane.value) || panes[0])
const formPanes: string[] = ['library', 'play', 'voice']
const isFormPane = computed(() => formPanes.includes(pane.value))

const devices = computed<Device[]>(() => (status.value?.devices as Device[]) || [])
const gateReady = computed(() => !!status.value?.logged_in && !!devices.value.length)

// 表单那一套（草稿、未保存判定、保存）只有一份，放在 ConfigForm 里；
// 顶栏的启用开关与保存按钮通过这个引用使唤它，避免两处各存一份状态互相打架。
const cfgForm = ref<any>(null)
const gate = computed<Record<string, any> | null>(() => cfgForm.value?.form || null)
const dirty = computed<boolean>(() => !!cfgForm.value?.dirty)
const saving = computed<boolean>(() => !!cfgForm.value?.saving)
const formCfg = computed<any>(() => cfgForm.value?.cfg || null)

// 每一栏右上角那几个字：一眼看出这栏现在是什么状况，值不值得点进去
const paneBadge = (key: PaneKey): { text: string; tone: string } | null => {
  if (key === 'account') {
    if (!loaded.value) return null
    return status.value?.logged_in
      ? { text: `${devices.value.length} 台`, tone: 'ok' }
      : { text: '未登录', tone: 'warn' }
  }
  if (key === 'library') {
    const dirs = formCfg.value?.dir_count
    return dirs ? { text: `${dirs} 目录`, tone: 'info' } : { text: '没库', tone: 'warn' }
  }
  if (key === 'play') return { text: (gate.value?.play_mode || 'auto'), tone: 'info' }
  if (key === 'voice') return gate.value?.voice_enabled ? { text: '监听中', tone: 'ok' } : null
  return null
}

function setGate(key: 'enabled' | 'show_sidebar_nav', value: boolean) {
  if (gate.value) gate.value[key] = value
}

async function loadStatus() {
  try {
    status.value = await get('/status') as Status
    emit('updated')
  } catch (e: any) {
    notify(`状态读取失败：${why(e)}`, 'error')
  } finally {
    loaded.value = true
  }
}

async function reloadAll() {
  await Promise.all([loadStatus(), cfgForm.value?.loadConfig()])
}

async function saveForm() {
  await cfgForm.value?.save()
  loadStatus()
}

// 配置页最容易踩的一件事是「改完忘了保存」：给一个和常见编辑器一致的快捷键
function onKeydown(event: KeyboardEvent) {
  if (!(event.metaKey || event.ctrlKey)) return
  if (event.key.toLowerCase() !== 's') return
  event.preventDefault()
  if (dirty.value && !saving.value) saveForm()
}

// 口令表与定时任务改动也会反映到侧栏运行视图，统一往上传一声
function onChanged() {
  loadStatus()
}

onMounted(() => {
  emit('layout', { maxWidth: '72rem' })
  loadStatus()
  window.addEventListener('keydown', onKeydown)
})
onBeforeUnmount(() => window.removeEventListener('keydown', onKeydown))
</script>

<template>
  <div class="xc">
    <!-- ---------- 顶栏：品牌 + 全局状况 + 启用开关 + 保存 ---------- -->
    <div class="xc-head">
      <v-avatar color="primary" variant="tonal" rounded="lg" size="38" class="mr-3">
        <v-icon icon="mdi-speaker" size="22" />
      </v-avatar>
      <div class="xc-head-text">
        <div class="xc-title">小爱音箱 · 配置</div>
        <div class="xc-sub">六栏分开改，保存一次生效；带「未保存」记号的是本页那份表单</div>
      </div>
      <v-spacer />

      <div class="xc-head-side">
        <v-menu v-if="gate" offset="8" location="bottom end" min-width="290" shade-elevation="8">
          <template #activator="{ props: menuProps }">
            <v-chip v-bind="menuProps" size="small" variant="tonal"
                    :color="gate.enabled ? (gate.show_sidebar_nav ? 'success' : 'info') : 'grey'"
                    :prepend-icon="gate.enabled ? 'mdi-toggle-switch-outline' : 'mdi-toggle-switch-off-outline'">
              {{ cfgForm?.navText }}
            </v-chip>
          </template>
          <v-card variant="outlined" class="xc-menu pa-3">
            <label class="xc-menu-row">
              <v-switch :model-value="gate.enabled" color="primary" hide-details inset density="compact"
                       @update:model-value="(v: any) => setGate('enabled', !!v)" />
              <span><b>启用插件</b><em>侧栏入口、播放台巡检、语音轮询都按这一项准入</em></span>
            </label>
            <label class="xc-menu-row">
              <v-switch :model-value="gate.show_sidebar_nav" :disabled="!gate.enabled" color="primary"
                       hide-details inset density="compact"
                       @update:model-value="(v: any) => setGate('show_sidebar_nav', !!v)" />
              <span><b>在「发现」显示侧栏入口</b><em>关掉后仍可用插件管理页打开，只是不收进侧栏</em></span>
            </label>
            <div class="xc-menu-note">
              宿主眼里的「插件已启用」就是这一项 —— <b>插件管理页那个开关改的是宿主自己的实例记录，
              碰不到这里</b>。只装好插件不在这里打开的话，侧栏永远不会出现。保存后立刻生效，不用重启主程序。
            </div>
          </v-card>
        </v-menu>

        <v-btn size="small" variant="text" class="text-none" icon="mdi-refresh"
               :loading="!loaded" @click="reloadAll" title="重新读取" />
        <v-btn size="small" color="primary" variant="flat" class="text-none"
               :loading="saving" :disabled="!dirty" @click="saveForm">
          <template #prepend>
            <v-icon icon="mdi-content-save" size="18" />
          </template>
          保存配置
          <span v-if="dirty" class="xc-dot" title="有未保存的改动" />
        </v-btn>
        <v-btn v-if="props.showSwitch" size="small" variant="text" class="text-none"
               prepend-icon="mdi-speaker" @click="emit('switch')">播放台
        </v-btn>
        <v-btn v-if="props.showClose" size="small" variant="text" class="text-none"
               prepend-icon="mdi-close" @click="emit('close')">关闭
        </v-btn>
      </div>
    </div>
    <v-divider />

    <div class="xc-shell">
      <!-- ---------- 左栏：分栏导航 ---------- -->
      <nav class="xc-rail">
        <button v-for="item in panes" :key="item.key" type="button"
                class="xc-rail-item" :class="{ 'is-active': pane === item.key }"
                @click="pane = item.key">
          <v-icon :icon="item.icon" size="17" class="xc-rail-icon" />
          <span class="xc-rail-title">{{ item.title }}</span>
          <span v-if="paneBadge(item.key)" class="xc-rail-badge"
                :class="`xc-rail-badge-${paneBadge(item.key)!.tone}`">{{ paneBadge(item.key)!.text }}</span>
          <span v-else-if="['library', 'play', 'voice'].includes(item.key) && dirty"
                class="xc-rail-badge xc-rail-badge-dirty" title="本页有未保存的改动">未保存</span>
        </button>
      </nav>

      <section class="xc-main">
        <!-- 表单那三栏没有自己的标题行，由这里给；账号栏与口令表/任务栏自带带状态与
             操作按钮的标题行（登录态、条数、重读、保存），再叠一层栏名就是重复 -->
        <div v-if="isFormPane" class="xc-pane-head">
          <span class="xc-pane-name">{{ current.title }}</span>
          <span class="xc-pane-desc">{{ current.desc }}</span>
        </div>

        <v-alert v-if="!gateReady && pane !== 'account'" class="xc-gate" variant="tonal"
                :type="loaded ? 'warning' : 'info'" density="compact">
          <div class="text-body-2">
            {{ loaded
               ? '还没登录，或登录后没取到音箱：这一栏改完也得等第 1 步做完才有实际效果'
               : '正在读取登录状态…' }}
          </div>
          <v-btn v-if="loaded" size="x-small" variant="text" class="text-none mt-1"
                 prepend-icon="mdi-arrow-left" @click="pane = 'account'">去「账号与音箱」
          </v-btn>
        </v-alert>

        <!-- 这三栏共用 ConfigForm 里那一份草稿，所以它既不能 v-if 也不能进 Transition：
             重建就等于把人刚敲进去、还没保存的值丢掉 -->
        <div v-show="isFormPane" class="xc-pane">
          <ConfigForm ref="cfgForm"
                      :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                      :pane="pane as any" :devices="devices"
                      @toast="notify" @saved="onChanged" @loaded="loaded = true" />
        </div>

        <Transition name="xc-fade" mode="out-in">
          <div v-if="!isFormPane" :key="pane" class="xc-pane">
            <ConfigLogin v-if="pane === 'account'"
                         :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                         :status="status" @changed="onChanged" />

            <ConfigCommands v-else-if="pane === 'commands'"
                            :api="props.api" :plugin-id="props.pluginId"
                            :source-plugin-id="props.sourcePluginId" @changed="onChanged" />

            <ConfigTasks v-else :api="props.api" :plugin-id="props.pluginId"
                         :source-plugin-id="props.sourcePluginId"
                         :devices="devices" @changed="onChanged" />
          </div>
        </Transition>
      </section>
    </div>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </div>
</template>

<style scoped>
/* 仅插件内局部样式；含 .v- 的类名选择器会被构建的 postcss 门禁整条删掉，所以只碰自己的类。
   配色一律取宿主 --v-theme-*，深浅色跟着主程序走。 */
.xc {
  width: 100%; display: flex; flex-direction: column;
  --xc-radius: 14px;
  --xc-ink: rgba(var(--v-theme-on-surface), .92);
  --xc-ink-soft: rgba(var(--v-theme-on-surface), .62);
  --xc-ink-faint: rgba(var(--v-theme-on-surface), .42);
  --xc-line: rgba(var(--v-border-color), var(--v-border-opacity));
  --xc-veil: rgba(var(--v-theme-on-surface), .045);
}
.xc-head {
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  padding: 14px 18px;
  background: linear-gradient(180deg, var(--xc-veil), transparent);
}
.xc-head-text { min-width: 0; }
.xc-title { font-size: 15px; font-weight: 650; letter-spacing: .2px; color: var(--xc-ink); }
.xc-sub { font-size: 11.5px; color: var(--xc-ink-faint); }
.xc-head-side { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.xc-dot {
  width: 7px; height: 7px; border-radius: 999px; margin-left: 7px; flex: 0 0 auto;
  background: rgb(var(--v-theme-warning)); box-shadow: 0 0 0 3px rgba(var(--v-theme-warning), .22);
}
.xc-menu-row {
  display: flex; align-items: flex-start; gap: 4px; padding: 4px 0; cursor: pointer;
}
.xc-menu-row b { display: block; font-size: 13px; font-weight: 600; color: var(--xc-ink); }
.xc-menu-row em {
  display: block; font-style: normal; font-size: 11.5px; line-height: 1.55;
  color: var(--xc-ink-faint);
}
.xc-menu-note {
  margin-top: 8px; padding-top: 8px; border-top: 1px solid var(--xc-line);
  font-size: 11px; line-height: 1.6; color: var(--xc-ink-faint);
}
.xc-menu-note b { color: var(--xc-ink); }

.xc-shell { display: flex; align-items: flex-start; min-height: 460px; }
.xc-rail {
  flex: 0 0 194px; align-self: stretch;
  display: flex; flex-direction: column; gap: 2px;
  padding: 12px 8px;
  border-right: 1px solid var(--xc-line);
  background: var(--xc-veil);
}
.xc-rail-item {
  position: relative; display: flex; align-items: center; gap: 8px;
  width: 100%; padding: 8px 10px; text-align: left; cursor: pointer;
  border: 0; border-radius: 11px; background: transparent;
  font: inherit; font-size: 12.5px; font-weight: 550; color: var(--xc-ink-soft);
  transition: background .16s ease, color .16s ease;
}
.xc-rail-item:hover { background: rgba(var(--v-theme-on-surface), .06); color: var(--xc-ink); }
.xc-rail-item.is-active {
  color: rgb(var(--v-theme-primary));
  background: rgba(var(--v-theme-primary), .12);
}
/* 选中的那一栏靠左留一条主色竖线，扫一眼就知道现在在哪一栏 */
.xc-rail-item.is-active::before {
  content: ''; position: absolute; left: -8px; top: 8px; bottom: 8px; width: 3px;
  border-radius: 0 3px 3px 0; background: rgb(var(--v-theme-primary));
}
.xc-rail-icon { flex: 0 0 auto; opacity: .9; }
.xc-rail-title { flex: 1 1 auto; min-width: 0; white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; }
.xc-rail-badge {
  flex: 0 0 auto; padding: 1px 7px; border-radius: 999px;
  font-size: 10.5px; font-weight: 650; letter-spacing: .2px;
  color: var(--xc-ink-faint); background: rgba(var(--v-theme-on-surface), .08);
}
.xc-rail-badge-ok { color: rgb(var(--v-theme-success)); background: rgba(var(--v-theme-success), .14); }
.xc-rail-badge-warn { color: rgb(var(--v-theme-warning)); background: rgba(var(--v-theme-warning), .16); }
.xc-rail-badge-info { color: rgb(var(--v-theme-info)); background: rgba(var(--v-theme-info), .14); }
.xc-rail-badge-dirty {
  color: rgb(var(--v-theme-warning)); background: rgba(var(--v-theme-warning), .16);
}

.xc-main { flex: 1 1 auto; min-width: 0; }
.xc-pane-head {
  display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap;
  padding: 14px 18px 0;
}
.xc-pane-name { font-size: 13px; font-weight: 650; color: var(--xc-ink); }
.xc-pane-desc { font-size: 11.5px; color: var(--xc-ink-faint); }
.xc-gate { margin: 12px 18px 0; border-radius: 12px; }
.xc-pane { padding: 14px 18px 22px; }

.xc-fade-enter-active, .xc-fade-leave-active { transition: opacity .16s ease, transform .16s ease; }
.xc-fade-enter-from { opacity: 0; transform: translateY(6px); }
.xc-fade-leave-to { opacity: 0; transform: translateY(-4px); }
@media (prefers-reduced-motion: reduce) {
  .xc-fade-enter-active, .xc-fade-leave-active { transition: none; }
}

@media (max-width: 880px) {
  .xc-head, .xc-pane-head, .xc-pane { padding-left: 14px; padding-right: 14px; }
  /* 窄屏（宿主小窗 / 手机）把左栏压成顶部横向滚动的一排 */
  .xc-shell { flex-direction: column; }
  .xc-rail {
    flex: none; width: 100%; flex-direction: row; align-self: auto;
    overflow-x: auto; padding: 8px 12px;
    border-right: 0; border-bottom: 1px solid var(--xc-line);
  }
  .xc-rail-item { width: auto; white-space: nowrap; }
  .xc-rail-item.is-active::before { left: 8px; right: 8px; top: auto; bottom: 0;
    width: auto; height: 2px; border-radius: 2px 2px 0 0; }
}
</style>
