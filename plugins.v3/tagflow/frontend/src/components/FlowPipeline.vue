<script setup lang="ts">
import { computed } from 'vue'
import type { FlowStep, StageCounts } from './flow'

// 流水线动画：节点是判据环节，连线上的光点表示"文件正在往下一环走"。
// running=false 时光点停住、节点回到常态 —— 动画表达的是"在跑"，没在跑就不该动。
const props = defineProps({
  steps: { type: Array as () => FlowStep[], default: () => [] },
  // 环节键 → 数字；数据页传本轮汇总折算出来的各环节计数
  counts: { type: Object as () => StageCounts, default: () => ({}) },
  running: { type: Boolean, default: false },
  // config：节点下标出"这一环由哪个配置决定"；live：标出这一环落了多少文件
  variant: { type: String as () => 'config' | 'live', default: 'live' }
})

// 一个光点从第一个节点走到最后一个的时长，除以环节数得到每段的延迟，
// 于是各段光点首尾相接，看着像一条连续的行进而非各自闪一下。
const SEGMENT = 0.9
const cycle = computed(() => Math.max(props.steps.length, 1) * SEGMENT)

function style(index: number) {
  return {
    '--tfp-delay': `${(index * cycle.value) / Math.max(props.steps.length, 1)}s`,
    '--tfp-cycle': `${cycle.value}s`
  } as Record<string, string>
}

function count(key: string): number | null {
  const value = (props.counts as Record<string, number | undefined>)?.[key]
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}
</script>

<template>
  <div class="tfp" :class="[`tfp-${variant}`, running ? 'is-running' : '']">
    <template v-for="(step, i) in steps" :key="step.key">
      <div class="tfp-node" :style="style(i)">
        <span class="tfp-ring" />
        <v-avatar :color="running ? 'primary' : 'grey-lighten-3'" variant="tonal"
                  rounded="circle" size="42">
          <v-icon :icon="step.icon" size="21" />
        </v-avatar>
        <div class="tfp-title">{{ step.title }}</div>
        <div class="tfp-caption">{{ step.caption }}</div>
        <div v-if="variant === 'live' && count(step.key) !== null" class="tfp-count">
          {{ count(step.key) }}
        </div>
        <div v-if="variant === 'config' && step.knobs" class="tfp-knobs">{{ step.knobs }}</div>
      </div>
      <div v-if="i < steps.length - 1" class="tfp-link" :style="style(i)">
        <span class="tfp-pulse" />
      </div>
    </template>
  </div>
</template>

<style scoped>
/* 只写本组件自己的类名：插件页只能给自己的类定样式，全局 v- / mdi- 选择器会被构建门禁删掉。 */
.tfp {
  display: flex;
  align-items: flex-start;
  gap: 4px;
  overflow-x: auto;
  padding: 4px 2px 8px;
}
.tfp-node {
  position: relative;
  flex: 1 1 0;
  min-width: 92px;
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  gap: 2px;
}
.tfp-ring {
  position: absolute;
  top: 3px;
  left: 50%;
  width: 42px;
  height: 42px;
  margin-left: -21px;
  border-radius: 50%;
  border: 2px solid rgba(var(--v-theme-primary), .55);
  opacity: 0;
  pointer-events: none;
}
.is-running .tfp-ring {
  animation: tfp-bloom var(--tfp-cycle) linear var(--tfp-delay) infinite;
}
.tfp-title { font-size: 13px; font-weight: 600; line-height: 1.3; }
.tfp-caption { font-size: 11px; color: rgba(var(--v-theme-on-surface), .62); line-height: 1.35; }
.tfp-knobs { font-size: 11px; color: rgb(var(--v-theme-primary)); opacity: .8; }
.tfp-count {
  font-size: 16px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: rgb(var(--v-theme-primary));
}
.tfp-live .tfp-count { min-height: 20px; }
.tfp-link {
  position: relative;
  flex: 0 0 26px;
  /* 连线是"轨道"，不是色块：只有头像中线那一条 5px，节点才读得出是串起来的 */
  align-self: flex-start;
  height: 5px;
  margin-top: 18px;
  background: linear-gradient(90deg,
    rgba(var(--v-theme-primary), .12), rgba(var(--v-theme-primary), .3));
  border-radius: 3px;
  overflow: hidden;
}
.tfp-pulse {
  position: absolute;
  top: 0;
  left: -40%;
  width: 40%;
  height: 100%;
  background: linear-gradient(90deg, transparent, rgb(var(--v-theme-primary)));
  border-radius: 3px;
  opacity: 0;
}
.is-running .tfp-pulse {
  animation: tfp-travel var(--tfp-cycle) linear var(--tfp-delay) infinite;
}
.tfp-config .tfp-link { opacity: .7; }
@keyframes tfp-travel {
  0% { left: -40%; opacity: 0; }
  6% { opacity: 1; }
  26% { left: 100%; opacity: 0; }
  100% { left: 100%; opacity: 0; }
}
@keyframes tfp-bloom {
  0% { transform: scale(.85); opacity: 0; }
  4% { opacity: .9; }
  22% { transform: scale(1.25); opacity: 0; }
  100% { transform: scale(1.25); opacity: 0; }
}
/* 跟随系统的"减少动态效果"：动画只是"在跑"的辅助表达，关掉后静态样式仍然说得出同一条链。 */
@media (prefers-reduced-motion: reduce) {
  .is-running .tfp-pulse,
  .is-running .tfp-ring { animation: none; }
  .tfp-pulse { opacity: 0; }
  .is-running .tfp-ring { opacity: .55; transform: none; }
}
@media (max-width: 760px) {
  .tfp-link { flex-basis: 12px; }
  .tfp-node { min-width: 74px; }
}
</style>
