<script setup lang="ts">
import { ref } from 'vue'
import Page from './Page.vue'
import Config from './Config.vue'

// 主界面「整理」分区的全页入口。正文就是数据页那一套（./Page），这里只多管一件事：
// 宿主给 AppPage 的 props 只有 api/pluginId/navKey，没有 initialConfig，
// 所以配置不能像插件管理页那样 emit('save') 交给宿主 —— 改成页内弹一个
// standalone 的 Config，它自己从 /config 起头、自己往 /config 写。
const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  navKey: { type: String, default: 'main' },
  nativeSubscribe: { type: Function as any, default: null }
})

const dataPage = ref<InstanceType<typeof Page> | null>(null)
const configDialog = ref(false)

function onConfigSaved() {
  dataPage.value?.refresh()
}
</script>

<template>
  <div class="mls-app">
    <Page ref="dataPage" :api="props.api" :plugin-id="props.pluginId"
          :source-plugin-id="props.sourcePluginId" :show-switch="true"
          @switch="configDialog = true" />

    <v-dialog v-model="configDialog" max-width="980px" scrollable persistent>
      <v-card variant="flat" class="mls-config-dialog">
        <Config :api="props.api" :plugin-id="props.pluginId" :source-plugin-id="props.sourcePluginId"
                standalone @close="configDialog = false" @updated="onConfigSaved" />
      </v-card>
    </v-dialog>
  </div>
</template>

<style scoped>
/* 仅插件内局部样式；含 .v- 的选择器会被构建的 postcss 门禁整条删掉，所以只碰自己的类。 */
.mls-app { width: 100%; }
.mls-config-dialog {
  border-radius: 14px;
  background: rgb(var(--v-theme-surface));
}
</style>
