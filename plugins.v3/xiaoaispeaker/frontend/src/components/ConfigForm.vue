<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { type Device } from './types'

const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  // 配置页把这张长表单拆成了三栏（曲库与直链 / 播放行为 / 语音与 AI），
  // 草稿与保存仍由本组件一处持有：切栏不丢未保存的改动。
  pane: { type: String as () => 'library' | 'play' | 'voice', default: 'library' },
  devices: { type: Array as () => Device[], default: () => [] }
})

const emit = defineEmits(['toast', 'loaded', 'saved'])

const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`)
const saving = ref(false)
const loaded = ref(false)
const cfg = ref<any>(null)

// /config 只改传上来的字段，所以草稿要按「服务端当前值」起头。
// 账号、密码与默认音箱不在这里：它们跟着「账号与音箱」那一栏走（见 ConfigLogin），
// 表单里不带这三个键，保存时也就不会把它们一起写没。
const form = ref<Record<string, any>>({
  enabled: false,
  show_sidebar_nav: true,
  music_dirs: '',
  base_url: '',
  play_mode: 'auto',
  audio_exts: '',
  force_mp3: false,
  audio_id: '',
  stream_ttl: 3600,
  default_volume: 0,
  auto_next: true,
  keep_light: false,
  tts_announce: false,
  voice_enabled: false,
  voice_devices: '',
  voice_poll: 2,
  voice_interrupt: true,
  voice_hint_tts: false,
  voice_hint_text: '正在搜索，请稍候',
  voice_reply: true,
  voice_memory: true,
  ai_enabled: false,
  ai_chat: false,
  ai_timeout: 6,
  library_limit: 10000,
  library_refresh: 60
})

// 与服务端那一份逐键比较得出「有没有未保存的改动」：只看表单里这些键，
// 登录态与设备缓存跟着 /status 变，不该算成用户的改动。
const pristine = ref<Record<string, any>>({ ...form.value })
const dirty = computed(() => Object.keys(form.value).some(
  key => JSON.stringify(form.value[key]) !== JSON.stringify(pristine.value[key])
))

const playModeItems = [
  { title: '自动（按型号白名单选接口）', value: 'auto' },
  { title: 'player_play_url（直链电台式）', value: 'url' },
  { title: 'player_play_music（走小米曲库）', value: 'music' }
]

const devices = computed<Device[]>(() => props.devices)

const navText = computed(() => {
  if (!loaded.value) return '读取中'
  if (!form.value.enabled) return '未启用：侧栏与巡检都不会跑'
  return form.value.show_sidebar_nav ? '侧栏入口已出现' : '已启用，只是不收进侧栏'
})

// 「监听哪些音箱」后端收的是逗号/空格分隔的 device_id，这里用多选组件来回转，
// 免得让人手抄那串又长又难认的 ID。
const voiceDeviceList = computed<string[]>({
  get: () => String(form.value.voice_devices || '')
    .split(/[,;\s]+/)
    .map(item => item.trim())
    .filter(Boolean),
  set: (value) => { form.value.voice_devices = value.join(',') }
})

const voiceDeviceItems = computed(() => devices.value.map(d => ({
  title: `${d.name || d.alias || d.device_id}${d.presence === 'online' ? '' : '（离线）'}`,
  value: d.device_id
})))

function fillFromStatus(data: any) {
  cfg.value = (data?.config || {}) as any
  const c = cfg.value
  form.value = {
    ...form.value,
    enabled: !!c.enabled,
    show_sidebar_nav: c.show_sidebar_nav !== false,
    // 账号、密码、默认音箱由「账号与音箱」那一栏管，这里不占位：/config 是「缺省即保留」，
    // 不带这三个键就等于不动它们
    music_dirs: c.music_dirs || '',
    base_url: c.base_url || '',
    play_mode: c.play_mode || 'auto',
    audio_exts: c.audio_exts || '',
    force_mp3: !!c.force_mp3,
    audio_id: c.audio_id || '',
    stream_ttl: c.stream_ttl ?? 3600,
    default_volume: c.default_volume ?? 0,
    auto_next: c.auto_next !== false,
    keep_light: !!c.keep_light,
    tts_announce: !!c.tts_announce,
    voice_enabled: !!c.voice_enabled,
    voice_devices: c.voice_devices || '',
    voice_poll: c.voice_poll ?? 2,
    voice_interrupt: c.voice_interrupt !== false,
    voice_hint_tts: !!c.voice_hint_tts,
    voice_hint_text: c.voice_hint_text ?? '正在搜索，请稍候',
    voice_reply: c.voice_reply !== false,
    voice_memory: c.voice_memory !== false,
    ai_enabled: !!c.ai_enabled,
    ai_chat: !!c.ai_chat,
    ai_timeout: c.ai_timeout ?? 6,
    library_limit: c.library_limit ?? 10000,
    library_refresh: c.library_refresh ?? 60
  }
  pristine.value = { ...form.value }
  loaded.value = true
}

async function loadConfig() {
  try {
    fillFromStatus(await (props.api as MoviePilotApi).get(`${base.value}/status`))
    emit('loaded')
  } catch (e: any) {
    emit('toast', `配置读取失败：${e?.message || e}`, 'error')
  }
}

async function save() {
  saving.value = true
  try {
    fillFromStatus(await (props.api as MoviePilotApi).post(`${base.value}/config`, { ...form.value }))
    emit('toast', '配置已保存', 'success')
    emit('saved')
  } catch (e: any) {
    emit('toast', `保存失败：${e?.message || e}`, 'error')
  } finally {
    saving.value = false
  }
}

onMounted(loadConfig)

// 配置页顶栏要用这两枚开关、要显示「有未保存的改动」、也要能直接保存：
// 草稿只有一份，所以把出入口交给父组件，而不是让顶栏自己另存一份状态。
defineExpose({ form, dirty, saving, loaded, cfg, navText, loadConfig, save })
</script>

<template>
  <div class="xcf">
    <!-- 「曲库与直链」：库范围、音箱访问得到的地址、能播的格式与索引 -->
    <template v-if="props.pane === 'library'">
      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-folder-music-outline" size="16" />
          音乐库
        </div>
        <v-textarea v-model="form.music_dirs" :rows="3" hide-details variant="outlined"
                    label="库路径（一行一个目录，只有这些目录里的文件可被列出与播放）"
                    placeholder="/media/music&#10;/nas/music" />
        <div class="xcf-hint">
          <span v-if="cfg && cfg.dir_count">
            当前生效 {{ cfg.dir_count }} 个目录
            <template v-if="cfg.missing_dirs.length">｜读不到：{{ cfg.missing_dirs.join('、') }}</template>
          </span>
          <span v-else>留空就用主程序的音乐目录设置</span>
        </div>
      </div>

      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-link-variant" size="16" />
          直链地址
        </div>
        <v-text-field v-model="form.base_url" hide-details variant="outlined"
                      label="外部访问地址（音箱能访问到的 MP 地址）"
                      placeholder="http://192.168.1.10:3000，留空用主程序设置" />
        <div class="xcf-hint">
          实际使用：<b>{{ cfg?.base_url_effective || '拿不到可用地址（音箱必然拉不到流）' }}</b>
          <template v-if="cfg?.base_url_source">｜来自 {{ cfg.base_url_source }}</template>
        </div>
        <div class="xcf-grid mt-3">
          <v-text-field v-model.number="form.stream_ttl" type="number" min="300" max="259200"
                        hide-details variant="outlined" label="直链有效期（秒）" />
          <v-text-field v-model="form.audio_id" hide-details variant="outlined"
                        label="play_music audioID（留空用默认）" />
        </div>
        <div class="xcf-hint">
          地址必须是<b>音箱所在网段连得到</b>的 IP 或域名，不能是 127.0.0.1，也不能是 MP 容器自己看的地址 ——
          这一项填错是「点了没声音」最常见的原因。直链由插件用签名密钥（落在插件数据目录
          <code>state/secret.json</code>）签发并带有效期，换密钥会让已经发出去的链接一起失效，这是有意的。
        </div>
      </div>

      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-file-document-multiple-outline" size="16" />
          格式与解码
        </div>
        <v-text-field v-model="form.audio_exts" hide-details variant="outlined"
                      label="可播放扩展名（留空用默认）"
                      placeholder="mp3 m4a aac flac wav ogg opus wma" />
        <label class="xcf-toggle mt-3">
          <v-switch v-model="form.force_mp3" color="primary" hide-details inset density="compact" />
          <span>
            <b>非 mp3 现场转成 mp3</b>
            <em>需要本机有 ffmpeg。开着时 /stream 边放边转，转出来的长度未知，所以这一条不支持拖动进度。</em>
          </span>
        </label>
        <div class="xcf-hint">
          小爱只稳当解码 mp3；m4a/flac 推过去常是「云端受理、音箱不出声」。
          这一栏不响就先把「播放行为」里的播放接口切成 <b>player_play_music</b>，那条免费的路不通再开转码。
        </div>
      </div>

      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-database-refresh-outline" size="16" />
          曲库索引
        </div>
        <div class="xcf-grid">
          <v-text-field v-model.number="form.library_limit" type="number" min="500" max="200000"
                        hide-details variant="outlined" label="最多入库多少首" />
          <v-text-field v-model.number="form.library_refresh" type="number" min="0" max="1440"
                        hide-details variant="outlined" label="自动保鲜周期（分钟，0=只手动）" />
        </div>
        <div class="xcf-hint">
          索引只为语音点歌服务（读标签与目录名，元数据缓存落在插件数据目录）。
          索引状态与手动重建在侧栏的「音乐库」那一栏。
        </div>
      </div>
    </template>

    <!-- 「播放行为」：推到音箱时怎么播 -->
    <template v-else-if="props.pane === 'play'">
      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-sine-wave" size="16" />
          播放接口
        </div>
        <v-select v-model="form.play_mode" :items="playModeItems" label="走哪条接口"
                  hide-details variant="outlined" />
        <div class="xcf-hint">
          <template v-if="devices.length">
            标记为需要 play_music 的型号：
            {{ devices.filter(d => d.needs_music_api).map(d => d.name || d.device_id).join('、') || '无' }}
          </template>
          <template v-else>型号白名单在登录后取到设备才判得出来；拿不准就用「自动」。</template>
        </div>
        <v-text-field v-model.number="form.default_volume" class="mt-3" type="number" min="0" max="100"
                      hide-details variant="outlined" style="max-width: 220px"
                      label="播放前设置音量（0=不改）" />
      </div>

      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-play-circle-outline" size="16" />
          连播与灯效
        </div>
        <label class="xcf-toggle">
          <v-switch v-model="form.auto_next" color="primary" hide-details inset density="compact" />
          <span>
            <b>队列自动连播</b>
            <em>关掉后侧栏队列播完当前这首不会自己接下一首（手动「下一首」仍然有效）。</em>
          </span>
        </label>
        <label class="xcf-toggle">
          <v-switch v-model="form.keep_light" color="primary" hide-details inset density="compact" />
          <span>
            <b>播放时常亮</b>
            <em>给音频类型带上 MUSIC，音箱播放时不熄灯；关掉更省电，但有些型号会当电台放。</em>
          </span>
        </label>
        <label class="xcf-toggle">
          <v-switch v-model="form.tts_announce" color="primary" hide-details inset density="compact" />
          <span>
            <b>连播时先播报下一曲</b>
            <em>切歌前用 TTS 说一句曲名，音箱多一次请求，家里人多时更清楚在放谁的歌。</em>
          </span>
        </label>
      </div>

      <div class="xcf-note">
        这几项是全局偏好，推给哪台音箱由「账号与音箱」那一栏的默认音箱决定；
        分组推送也在那一栏配。
      </div>
    </template>

    <!-- 「语音与 AI」：听音箱的对话记录，命中口令才动手 -->
    <template v-else>
      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-microphone-message" size="16" />
          语音监听
        </div>
        <div class="xcf-note xcf-note-warn">
          打开后插件周期性读这几台音箱在小米云端的<b>对话记录</b>，命中口令表的话才接手播放。
          每读一次就是一个请求，只对真的要听的那几台开。
        </div>
        <label class="xcf-toggle">
          <v-switch v-model="form.voice_enabled" color="primary" hide-details inset density="compact" />
          <span><b>开启语音监听</b><em>关掉就是纯手动播放台，口令表与 AI 都不再被触发。</em></span>
        </label>
        <div class="xcf-grid mt-3">
          <v-select v-model="voiceDeviceList" :items="voiceDeviceItems" label="监听哪些音箱" multiple
                    chips dense closable hide-details variant="outlined"
                    placeholder="留空 = 只监听默认音箱" />
          <v-text-field v-model.number="form.voice_poll" type="number" min="2" max="30" hide-details
                        variant="outlined" label="轮询间隔（秒，2-30）" />
        </div>
        <v-text-field v-model="form.voice_hint_text" class="mt-3" hide-details variant="outlined"
                      density="compact" label="搜索期间的提示语（留空用默认）"
                      placeholder="正在搜索，请稍候" />
        <div class="xcf-hint">
          {{ devices.length
             ? `已取到 ${devices.length} 台设备，可直接勾选；留空时只跟着默认音箱走`
             : '设备候选要登录后才有；也可以先手填 device_id（逗号分隔）' }}
          ｜逐类口令的开关在本页「语音口令表」那一栏改。
        </div>
      </div>

      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-flash-off-outline" size="16" />
          打断与播报
        </div>
        <label class="xcf-toggle">
          <v-switch v-model="form.voice_interrupt" color="primary" hide-details inset density="compact" />
          <span><b>命中前先打断音箱</b><em>正在放歌时点歌，先停下再推新的。</em></span>
        </label>
        <label class="xcf-toggle">
          <v-switch v-model="form.voice_reply" color="primary" hide-details inset density="compact" />
          <span><b>播报执行结果</b><em>「已播放 XXX」，没播成也说一句为什么。</em></span>
        </label>
        <label class="xcf-toggle">
          <v-switch v-model="form.voice_hint_tts" color="primary" hide-details inset density="compact" />
          <span><b>搜索期间播提示语</b><em>找歌要一两秒，先把上面那句提示播出去。</em></span>
        </label>
        <label class="xcf-toggle">
          <v-switch v-model="form.voice_memory" color="primary" hide-details inset density="compact" />
          <span>
            <b>记住刚才那首</b>
            <em>用于「再播一遍」「换一首」这类指代；只留在本机内存里，不落盘。</em>
          </span>
        </label>
      </div>

      <div class="xcf-group">
        <div class="xcf-group-title">
          <v-icon icon="mdi-robot-outline" size="16" />
          AI 兜底与对话
        </div>
        <label class="xcf-toggle">
          <v-switch v-model="form.ai_enabled" color="primary" hide-details inset density="compact" />
          <span><b>口令表没命中时交给 AI 判定</b><em>借主程序「设定 → AI 大模型」配好的那个助手，插件不存第二份 Key。</em></span>
        </label>
        <label class="xcf-toggle">
          <v-switch v-model="form.ai_chat" color="primary" hide-details inset density="compact" />
          <span><b>没命中的话当成聊天</b><em>说完 2 分钟内可追问；回答只走音箱 TTS，不进曲库播放。</em></span>
        </label>
        <v-alert class="mt-3" variant="tonal" density="compact" :type="cfg?.ai_ready ? 'success' : 'warning'">
          <div class="text-body-2">
            {{ cfg?.ai_ready ? '主程序的智能助手可用，这里不再另配一份' : '主程序的智能助手还没配好' }}
          </div>
          <div class="text-caption">
            <template v-if="cfg?.ai_ready">
              来自「设定 → AI 大模型」：{{ cfg.ai_provider || '未知来源' }}
              <template v-if="cfg.ai_model">｜{{ cfg.ai_model }}</template>
            </template>
            <template v-else>{{ cfg?.ai_message || '拿不到宿主配置' }}</template>
          </div>
        </v-alert>
        <v-text-field v-model.number="form.ai_timeout" class="mt-2" type="number" min="2" max="30"
                      hide-details variant="outlined" style="max-width: 220px"
                      label="超时（秒，2-30）" />
        <div class="xcf-hint">
          AI 只能给出口令表里已有的那几种动作，置信度低或动作未知都当没判出来 ——
          只是多一个入口，权限不比口令表大。
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
/* 仅插件内局部样式；含 .v- 的类名选择器会被构建的 postcss 门禁整条删掉，所以只碰自己的类。
   配色全部取宿主 --v-theme-*，深浅色跟着主程序走。 */
.xcf {
  display: flex; flex-direction: column; gap: 12px;
  --xcf-radius: 14px;
  --xcf-ink: rgba(var(--v-theme-on-surface), .92);
  --xcf-ink-faint: rgba(var(--v-theme-on-surface), .5);
  --xcf-line: rgba(var(--v-border-color), var(--v-border-opacity));
}
.xcf-group {
  padding: 15px 16px 16px;
  border-radius: var(--xcf-radius);
  border: 1px solid var(--xcf-line);
  background: rgba(var(--v-theme-on-surface), .028);
  transition: border-color .18s ease, background .18s ease, transform .18s ease;
}
.xcf-group:hover {
  border-color: rgba(var(--v-theme-primary), .32);
  background: rgba(var(--v-theme-on-surface), .042);
}
.xcf-group-title {
  display: flex; align-items: center; gap: 7px; margin-bottom: 11px;
  font-size: 12px; font-weight: 650; letter-spacing: .3px; color: var(--xcf-ink);
}
.xcf-group-title .v-icon { color: rgb(var(--v-theme-primary)); opacity: .85; }
.xcf-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); }
.xcf-toggle {
  display: flex; align-items: flex-start; gap: 4px; padding: 7px 0;
  border-radius: 10px; cursor: pointer;
}
.xcf-toggle > span { min-width: 0; padding-top: 2px; }
.xcf-toggle b { display: block; font-size: 13px; font-weight: 600; color: var(--xcf-ink); }
.xcf-toggle em {
  display: block; margin-top: 1px; font-style: normal;
  font-size: 11.5px; line-height: 1.6; color: var(--xcf-ink-faint);
}
.xcf-hint { margin-top: 8px; font-size: 11.5px; line-height: 1.65; color: var(--xcf-ink-faint); }
.xcf-hint b { color: var(--xcf-ink); font-weight: 600; }
.xcf-note {
  padding: 10px 13px; border-radius: 12px;
  font-size: 12px; line-height: 1.65; color: var(--xcf-ink);
  background: rgba(var(--v-theme-info), .08);
  border: 1px solid rgba(var(--v-theme-info), .18);
}
.xcf-note-warn {
  background: rgba(var(--v-theme-warning), .08);
  border-color: rgba(var(--v-theme-warning), .2);
}
@media (max-width: 760px) {
  .xcf-group { padding: 13px; }
}
</style>
