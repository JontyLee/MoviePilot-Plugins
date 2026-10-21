<script setup lang="ts">
import { computed, onMounted } from 'vue'
import ConfigForm from './ConfigForm.vue'

const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  // 宿主存的配置不作为初值：密码不在里面，而 /config 是「缺省即保留」的语义，
  // 拿一份不含密码的草稿去保存会把已存的账密一起动到。表单自己从 /status 起头。
  initialConfig: { type: Object as any, default: () => ({}) },
  nativeSubscribe: { type: Function as any, default: null }
})

const emit = defineEmits(['save', 'close', 'switch', 'layout'])

const title = computed(() => '小爱音箱 · 配置')

onMounted(() => {
  emit('layout', { maxWidth: '64rem' })
})
</script>

<template>
  <v-card flat class="xai-config">
    <div class="xai-head">
      <v-avatar color="primary" variant="tonal" rounded="lg" size="36" class="mr-3">
        <v-icon icon="mdi-speaker" size="22" />
      </v-avatar>
      <div class="xai-head-text">
        <div class="text-h6">{{ title }}</div>
        <div class="text-caption text-medium-emphasis">
          小米账号、可播放的音乐库与音箱访问得到的直链地址
        </div>
      </div>
    </div>
    <v-divider />
    <ConfigForm :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                can-switch @switch="emit('switch')" />
  </v-card>
</template>

<style scoped>
/* 仅插件内局部样式；不得写全局 v- 或 mdi- 类规则（构建有 postcss 门禁）。 */
.xai-config { width: 100%; display: flex; flex-direction: column; }
.xai-head { display: flex; align-items: center; gap: 8px; padding: 14px 18px; }
.xai-head-text { min-width: 0; }
</style>
