/// <reference types="vite/client" />

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<{}, {}, any>
  export default component
}

// 宿主注入的插件 HTTP 客户端（bear 认证、按 pluginId 作用域）。
interface MoviePilotApi {
  get: (url: string, params?: Record<string, any>) => Promise<any>
  post: (url: string, body?: Record<string, any>) => Promise<any>
}
