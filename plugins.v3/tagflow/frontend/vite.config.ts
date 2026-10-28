import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import federation from '@originjs/vite-plugin-federation'

// MoviePilot V3 联邦远程组件：侧栏（nav_key=main）解析顺序是 ./AppPage → ./Page，
// 因此三个都暴露：./AppPage 侧栏全页、./Page 数据页、./Config 配置页。
// vue/vuetify/vuetify~styles 复用主程序实例（generate:false），绝不打包 Vuetify 全局样式。
export default defineConfig({
  plugins: [
    vue(),
    federation({
      name: 'TagFlow',
      filename: 'remoteEntry.js',
      base: 'auto',
      exposes: {
        './Page': './src/components/Page.vue',
        './Config': './src/components/Config.vue',
        './AppPage': './src/components/AppPage.vue'
      },
      shared: {
        vue: { requiredVersion: false, generate: false },
        vuetify: { requiredVersion: false, generate: false, singleton: true },
        'vuetify/styles': { requiredVersion: false, generate: false, singleton: true }
      },
      format: 'esm'
    })
  ],
  build: {
    target: 'esnext',
    minify: false,
    cssCodeSplit: true,
    modulePreload: false
  },
  css: {
    postcss: {
      plugins: [
        {
          postcssPlugin: 'internal:charset-removal',
          AtRule: {
            charset: (atRule) => {
              if (atRule.name === 'charset') atRule.remove()
            }
          }
        },
        {
          // 强制门禁：剥离任何来自 vuetify/mdi 的全局 `.v-*`/`.mdi-*` 规则，避免污染主程序。
          postcssPlugin: 'vuetify-filter',
          Root(root) {
            const file = (root.source?.input?.file || '').replace(/\\/g, '/')
            if (file.includes('/node_modules/vuetify/') || file.includes('/node_modules/@mdi/')) {
              root.nodes = []
              return
            }
            root.walkRules((rule) => {
              if (rule.selector && (rule.selector.includes('.v-') || rule.selector.includes('.mdi-'))) {
                rule.remove()
              }
            })
          }
        }
      ]
    }
  }
})
