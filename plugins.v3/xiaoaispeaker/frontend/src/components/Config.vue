<script setup lang="ts">
import { computed, ref, onMounted } from 'vue'
import ConfigForm from './ConfigForm.vue'
import ConfigLogin from './ConfigLogin.vue'
import ConfigCommands from './ConfigCommands.vue'
import ConfigTasks from './ConfigTasks.vue'
import { useApi } from './useApi'
import { type Device, type Status } from './types'

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
const forceOpen = ref(false)

const devices = computed<Device[]>(() => (status.value?.devices as Device[]) || [])
const gateReady = computed(() => !!status.value?.logged_in && !!devices.value.length)
const sectionsOpen = computed(() => gateReady.value || forceOpen.value)

async function loadStatus() {
  try {
    status.value = await get('/status') as Status
    emit('updated')
  } catch (e: any) {
    notify(`状态读取失败：${e?.message || e}`, 'error')
  } finally {
    loaded.value = true
  }
}

// 口令表与定时任务改动也会反映到侧栏运行视图，统一往上传一声
function onChanged() {
  loadStatus()
}

onMounted(() => {
  emit('layout', { maxWidth: '64rem' })
  loadStatus()
})
</script>

<template>
  <v-card flat class="xai-config">
    <div class="xai-head">
      <v-avatar color="primary" variant="tonal" rounded="lg" size="36" class="mr-3">
        <v-icon icon="mdi-speaker" size="22" />
      </v-avatar>
      <div class="xai-head-text">
        <div class="text-h6">小爱音箱 · 配置</div>
        <div class="text-caption text-medium-emphasis">
          先登录小米账号并选到音箱，再改音乐库、直链、语音与定时
        </div>
      </div>
      <v-spacer />
      <v-btn v-if="props.showSwitch" size="small" variant="text" class="text-none"
             prepend-icon="mdi-speaker" @click="emit('switch')">查看播放台
      </v-btn>
      <v-btn v-if="props.showClose" size="small" variant="text" class="text-none"
             prepend-icon="mdi-close" @click="emit('close')">关闭
      </v-btn>
    </div>
    <v-divider />

    <div class="xai-cfg-body">
      <ConfigLogin :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                   :status="status" @changed="onChanged" />

      <template v-if="!sectionsOpen">
        <v-alert class="mt-4" :type="loaded ? 'warning' : 'info'" variant="tonal" density="compact">
          <div class="text-body-2">
            {{ loaded ? '还没有登录，或登录后没取到音箱：后面的配置改动了也不会有实际效果' : '正在读取登录状态…' }}
          </div>
          <div class="text-caption">
            音乐库、直链地址、语音与定时这些配置项要作用在「一台能播的音箱」上才有意义，
            所以默认先做完上面第 1 步。
          </div>
          <v-btn v-if="loaded" size="x-small" variant="text" class="text-none mt-1"
                 prepend-icon="mdi-eye-off-outline" @click="forceOpen = true">
            我知道，仍然直接编辑其他配置
          </v-btn>
        </v-alert>
      </template>

      <template v-else>
        <div class="xai-section">
          <div class="xai-section-title">
            <span class="xai-section-mark">2</span>
            插件与播放配置
          </div>
          <ConfigForm :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                      :can-switch="false" :sections-open="true" :devices="devices"
                      @saved="onChanged" />
        </div>

        <v-divider class="my-5" />
        <ConfigCommands :api="props.api" :plugin-id="props.pluginId"
                        :source-plugin-id="props.sourcePluginId" @changed="onChanged" />

        <v-divider class="my-5" />
        <ConfigTasks :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                     :devices="devices" @changed="onChanged" />
      </template>
    </div>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.xai-config { width: 100%; display: flex; flex-direction: column; }
.xai-head { display: flex; align-items: center; gap: 8px; padding: 14px 18px; }
.xai-head-text { min-width: 0; }
.xai-cfg-body {
  padding: 16px 18px 20px; display: flex; flex-direction: column; gap: 4px;
  --xai-ink: rgba(var(--v-theme-on-surface), .92);
}
.xai-section { margin-top: 18px; }
.xai-section-title {
  display: flex; align-items: center; gap: 8px; margin-bottom: 10px;
  font-size: 13px; font-weight: 650; letter-spacing: .3px; color: var(--xai-ink);
}
.xai-section-mark {
  display: inline-flex; align-items: center; justify-content: center;
  width: 20px; height: 20px; border-radius: 7px; flex: 0 0 auto;
  font-size: 11.5px; font-weight: 700;
  color: rgb(var(--v-theme-primary)); background: rgba(var(--v-theme-primary), .14);
}
@media (max-width: 760px) {
  .xai-head, .xai-cfg-body { padding-left: 14px; padding-right: 14px; }
}
</style>
