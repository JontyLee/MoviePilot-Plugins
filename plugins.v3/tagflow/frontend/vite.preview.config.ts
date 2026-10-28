import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// 仅本地预览用：不走模块联邦，也不套生产配置的 .v-/.mdi-* 门禁（那两条会把 vuetify 自带样式清空）。
// 产物构建仍然只用 vite.config.ts，这个文件不参与发布。
export default defineConfig({
  root: 'preview',
  base: './',
  plugins: [vue()],
  server: { port: 5179, host: '127.0.0.1' }
})
