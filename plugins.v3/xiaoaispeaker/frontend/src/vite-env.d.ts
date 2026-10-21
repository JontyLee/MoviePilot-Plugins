/// <reference types="vite/client" />

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<{}, {}, any>
  export default component
}

// 宿主注入的插件 HTTP 客户端（bear 认证、按 pluginId 作用域）。
// get 只认第一个参数：第二个对象不会拼成查询串，而是被宿主丢掉（豆瓣中心的插件都是把
// 条件直接写进 url 的）。传了它，请求发出的是裸路径，后端只能按默认值回答。
interface MoviePilotApi {
  get: (url: string) => Promise<any>
  post: (url: string, body?: Record<string, any>) => Promise<any>
}
