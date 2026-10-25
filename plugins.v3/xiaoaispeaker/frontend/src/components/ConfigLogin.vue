<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from 'vue'
import { useApi } from './useApi'
import { type Device, type GroupRow, type LoginResult, type QrResult } from './types'

/**
 * 配置页的第一项：小米账号 → 登录 → 取音箱 → 选默认音箱（顺带设备分组）。
 *
 * 登录只在这一个地方做，三种方式任选其一：账号密码、扫码、Cookie 票据。
 * 账号密码先落进配置再发登录，理由是后端只认配置里那一份：
 * 票据过期要重登、语音轮询要换票，都从配置读，只在请求体里带一次等于没存。
 */
const props = defineProps({
  api: { type: Object as () => MoviePilotApi, default: () => ({}) as any },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  status: { type: Object as any, default: null }
})

const emit = defineEmits(['changed'])

const { base, busy, snackbar, notify, post, raw } = useApi(props, () => emit('changed'))

const cfg = computed(() => props.status?.config)
const devices = computed<Device[]>(() => (props.status?.devices as Device[]) || [])
const onlineCount = computed(() => devices.value.filter(d => d.presence === 'online').length)
const groups = computed<GroupRow[]>(() => (props.status?.extra?.groups as GroupRow[]) || [])
const defaultGroup = computed<string>(() => String(props.status?.extra?.default_group || ''))

// ---------------- 登录方式 ----------------

const LOGIN_MODES = [
  { value: 'password', title: '账号密码', icon: 'mdi-account-key-outline' },
  { value: 'qrcode', title: '扫码', icon: 'mdi-qrcode-scan' },
  { value: 'cookie', title: 'Cookie 票据', icon: 'mdi-cookie' }
]

const loginMode = ref<'password' | 'qrcode' | 'cookie'>('password')

const credential = ref({ username: '', password: '' })
const loginResult = ref<LoginResult | null>(null)
const verifyCode = ref('')
// 手工票据只要两项（与 songloft 的小爱插件一致）：浏览器 cookie 里的 userId 与 passToken
const manual = ref({ user_id: '', pass_token: '' })
// 登录成功后自动把音箱列表端上来，登完了还得自己找地方点「刷新设备」是原来那套的毛病
const picker = ref(false)

const loginStates: Record<string, { color: string; text: string }> = {
  success: { color: 'success', text: '登录成功' },
  need_captcha: { color: 'warning', text: '卡在图形验证码：照上面的图填验证码提交' },
  need_verify: { color: 'warning', text: '卡在二次校验：小米已把验证码发往账号绑定的手机/邮箱，填进来提交' },
  failed: { color: 'error', text: '登录失败' }
}

const loginState = computed(() => loginStates[loginResult.value?.state || ''] || null)
// 扫码登的账号没有密码可填：过期时让它说「重新扫码」而不是「重新登录」
const reloginText = computed(() => (props.status?.login_method === 'qrcode'
  ? '票据已过期，切到「扫码」重新扫一次'
  : '票据已过期，再点一次「登录」即可'))

// 候选按「当前默认 → 在线 → 其余」排：刚登完时最有用的那台要在最上面
const pickerDevices = computed<Device[]>(() => devices.value.slice().sort((a, b) => {
  const rank = (d: Device) => (d.is_default ? 0 : d.presence === 'online' ? 1 : 2)
  return rank(a) - rank(b)
}))

function switchMode(mode: 'password' | 'qrcode' | 'cookie') {
  if (mode !== 'qrcode') stopQr()
  loginMode.value = mode
  // 点「扫码」就该看到码，不该还要再点一次「获取二维码」：
  // 没有正在等的会话（第一次进来、或上一张已过期）就当场取一张
  if (mode === 'qrcode' && qr.value?.state !== 'qr_waiting') startQr()
}

async function startLogin() {
  const body: Record<string, any> = {}
  const username = credential.value.username.trim()
  // 掩码（13****34）回填不算账号，服务端也会当没改过，这里一起挡掉
  if (username && !username.includes('*')) body.username = username
  if (credential.value.password) body.password = credential.value.password
  if (Object.keys(body).length) await post('/config', body, '保存账号')

  const res = await raw('/login', {}, '登录')
  if (res) afterLogin(res as LoginResult)
}

async function submitVerify() {
  const code = verifyCode.value.trim()
  if (!code) {
    notify('填写验证码', 'warning')
    return
  }
  const res = await raw('/login', { code }, '提交验证码')
  if (res) afterLogin(res as LoginResult)
}

async function submitManual() {
  const userId = manual.value.user_id.trim()
  const passToken = manual.value.pass_token.trim()
  if (!userId || !passToken) {
    notify('userId 与 passToken 都要填', 'warning')
    return
  }
  const res = await raw('/login',
    { user_id: userId, pass_token: passToken }, '校验票据')
  if (res) afterLogin(res as LoginResult)
}

async function resendVerify() {
  const res = await raw('/login', { resend: true }, '重发验证码')
  if (res) afterLogin(res as LoginResult)
}

async function afterLogin(res: LoginResult) {
  loginResult.value = res || null
  verifyCode.value = ''
  emit('changed')
  if (res?.state === 'success') {
    notify(res.message || '登录成功', 'success')
    credential.value.password = ''
    manual.value = { user_id: '', pass_token: '' }
    stopQr()
    qr.value = null
    // 后端登录成功时已经强制刷过一遍设备，这里只负责把它直接端到人面前
    if (res.device_count) picker.value = true
    else notify('登录成功，但这个账号下没取到音箱设备', 'warning')
    return
  }
  if (res?.state === 'need_captcha' || res?.state === 'need_verify') {
    notify(res.message || '需要验证码', 'warning')
    return
  }
  notify(res?.message || '登录失败', 'error')
}

// ---------------- 扫码登录 ----------------

const qr = ref<QrResult | null>(null)
const qrBusy = ref(false)
const qrLeft = ref(0)
// 一次 poll 会在后端挂住最长 8 秒（小米的长轮询），所以只能串行问：
// seq 让「重新获取」「切走」「退出登录」之前的那一轮结果作废，不然旧二维码的
// 回调会把新会话的状态盖回去
let qrSeq = 0
let qrTimer: ReturnType<typeof setTimeout> | null = null
let qrTick: ReturnType<typeof setInterval> | null = null

const qrText = computed(() => {
  const state = qr.value?.state || ''
  if (state === 'qr_waiting') return qrLeft.value > 0 ? `等待扫码确认…（${qrLeft.value} 秒内有效）` : '等待扫码确认…'
  if (state === 'qr_expired') return '这张二维码已经作废，重新获取一张'
  if (state === 'failed') return '扫码未成功'
  return ''
})

function stopQr() {
  qrSeq += 1
  if (qrTimer) {
    clearTimeout(qrTimer)
    qrTimer = null
  }
  if (qrTick) {
    clearInterval(qrTick)
    qrTick = null
  }
  qrBusy.value = false
}

async function askQr(action: 'start' | 'poll'): Promise<QrResult | null> {
  const seq = qrSeq
  try {
    const res = await (props.api as MoviePilotApi)
      .post(`${base.value}/login/qrcode`, { action }) as QrResult
    // 这一轮已经被 stopQr 作废（换了二维码、切走或登录成功），结果丢掉
    if (seq !== qrSeq || !res) return null
    return res
  } catch (e: any) {
    if (seq === qrSeq) notify(`扫码请求失败：${e?.message || e}`, 'error')
    return null
  }
}

async function startQr() {
  stopQr()
  const seq = qrSeq
  qrBusy.value = true
  const res = await askQr('start')
  if (seq === qrSeq) qrBusy.value = false
  if (res) applyQr(res)
}

function applyQr(res: QrResult) {
  if (res.state === 'qr_waiting') {
    // 轮询那次的响应不带图片：图属于这张二维码，不属于这一次请求。直接盖上去，
    // 页面就会在第一次轮询回来（约 8 秒）后把二维码换成「还没获取」的空框
    if (!res.qr_image && qr.value?.qr_image) res = { ...res, qr_image: qr.value.qr_image }
    qr.value = res
    // 没给新的剩余时限就沿用正在倒的那个数，别把倒计时打到 0 当场判这张码过期
    qrLeft.value = res.expires_in > 0 ? res.expires_in : Math.max(0, qrLeft.value)
    runCountdown()
    qrTimer = setTimeout(async () => {
      const again = await askQr('poll')
      if (again) applyQr(again)
    }, 400)
    return
  }
  qrLeft.value = 0
  if (res.state === 'success') {
    afterQrSuccess(res)
    return
  }
  notify(res.message || (res.state === 'qr_expired' ? '二维码已过期' : '扫码未成功'), 'warning')
}

function runCountdown() {
  if (qrTick) clearInterval(qrTick)
  qrTick = setInterval(() => {
    qrLeft.value -= 1
    if (qrLeft.value > 0) return
    // 本地倒计时归零就当这张码死了：别再挂着轮询，小米那边也一样过期
    stopQr()
    qr.value = { ...(qr.value as QrResult), state: 'qr_expired', message: '二维码已过期，重新获取一张再扫' }
    notify('二维码已过期，重新获取一张再扫', 'warning')
  }, 1000)
}

function afterQrSuccess(res: QrResult) {
  stopQr()
  qr.value = null
  loginResult.value = {
    state: 'success',
    message: res.message || '扫码登录成功',
    captcha_image: '',
    logged_in: true,
    device_count: res.device_count
  }
  credential.value.password = ''
  manual.value = { user_id: '', pass_token: '' }
  notify(res.message || '扫码登录成功', 'success')
  emit('changed')
  if (res.device_count) picker.value = true
  else notify('登录成功，但这个账号下没取到音箱设备', 'warning')
}

function cancelQr() {
  stopQr()
  qr.value = null
}

onBeforeUnmount(stopQr)

async function doLogout() {
  stopQr()
  qr.value = null
  const res = await post('/logout', {}, '退出登录')
  if (res) {
    loginResult.value = null
    picker.value = false
    credential.value.password = ''
  }
}

async function refreshDevices() {
  busy.value = '刷新设备'
  try {
    const res = await (props.api as MoviePilotApi)
      .get(`plugin/${props.pluginId || props.sourcePluginId}/devices`)
    if (res?.message && !res?.devices?.length) notify(`设备刷新失败：${res.message}`, 'warning')
    else notify(`设备列表已刷新，共 ${(res?.devices || []).length} 台`)
    emit('changed')
  } catch (e: any) {
    notify(`设备刷新失败：${e?.message || e}`, 'error')
  } finally {
    busy.value = ''
  }
}

// ---------------- 音箱与分组 ----------------

function selectDevice(device: Device) {
  return post('/device/select', { device_id: device.device_id }, '设为默认')
}

function clearDefault() {
  return post('/device/select', { device_id: '' }, '取消默认音箱')
}

const groupForm = ref<{ name: string; device_ids: string[] }>({ name: '', device_ids: [] })
const editingGroup = ref('')

const groupDeviceItems = computed(() => devices.value.map(item => ({
  title: item.name || item.alias || item.device_id,
  value: item.device_id
})))

function newGroup() {
  editingGroup.value = ''
  groupForm.value = { name: '', device_ids: [] }
}

function editGroup(row: GroupRow) {
  editingGroup.value = row.name
  groupForm.value = { name: row.name, device_ids: [...row.device_ids] }
}

async function saveGroup() {
  const name = groupForm.value.name.trim()
  if (!name) {
    notify('先给分组起个名字', 'warning')
    return
  }
  if (groupForm.value.device_ids.length < 2) {
    notify('分组至少要有两台音箱才有意义', 'warning')
    return
  }
  const res = await post('/groups', { name, device_ids: groupForm.value.device_ids }, '保存分组')
  if (res && res.ok !== false) {
    editingGroup.value = ''
    groupForm.value = { name: '', device_ids: [] }
  }
}

function removeGroup(name: string) {
  return post('/groups', { name, remove: true }, '删除分组').then(() => {
    if (editingGroup.value === name) newGroup()
  })
}
</script>

<template>
  <div class="xcl">
    <div class="xcl-title">
      <span class="xcl-step">1</span>
      小米账号与音箱
      <v-chip size="x-small" variant="tonal" class="ml-2"
              :color="props.status?.token_expired ? 'error' : props.status?.logged_in ? 'success' : 'grey'">
        {{ props.status?.token_expired ? '票据已过期' : props.status?.logged_in ? '已登录' : '未登录' }}
      </v-chip>
      <span v-if="devices.length" class="text-caption text-grey ml-1">
        {{ devices.length }} 台音箱，{{ onlineCount }} 台在线
      </span>
    </div>

    <div v-if="props.status?.logged_in" class="xcl-now pa-3 mb-3">
      <div class="text-body-2">
        {{ cfg?.username_set ? cfg.username : '（未记录账号名）' }}
        <span v-if="cfg?.username_set" class="text-caption text-grey ml-1">已掩码</span>
        <v-chip v-if="cfg?.default_device_id" size="x-small" variant="flat" color="primary" class="ml-2">
          默认：{{ (devices.find(d => d.device_id === cfg?.default_device_id)?.name
                    || devices.find(d => d.device_id === cfg?.default_device_id)?.alias
                    || '不在当前设备列表里') }}
        </v-chip>
        <v-chip v-else size="x-small" variant="tonal" class="ml-2">默认：第一台在线音箱</v-chip>
      </div>
      <div class="text-caption text-grey">
        票据保存于 {{ props.status?.token_saved_at || '-' }}
        <template v-if="props.status?.manual_token">｜手工填的 Cookie 票据</template>
        <template v-if="props.status?.token_expired">｜{{ reloginText }}</template>
      </div>
    </div>

    <div v-if="props.status?.logged_in" class="d-flex mb-3" style="gap: 8px">
      <v-btn size="small" variant="tonal" :loading="busy === '刷新设备'" prepend-icon="mdi-refresh"
             @click="refreshDevices">刷新设备
      </v-btn>
      <v-btn size="small" variant="text" color="error" :loading="busy === '退出登录'"
             prepend-icon="mdi-logout" @click="doLogout">退出登录
      </v-btn>
    </div>

    <v-btn-toggle :model-value="loginMode" divided mandatory density="compact" color="primary"
                  style="align-self: flex-start" @update:model-value="switchMode($event)">
      <v-btn v-for="mode in LOGIN_MODES" :key="mode.value" :value="mode.value" size="small"
             variant="text" class="text-none" :prepend-icon="mode.icon">{{ mode.title }}
      </v-btn>
    </v-btn-toggle>

    <!-- 一、账号密码 -->
    <template v-if="loginMode === 'password'">
      <div class="d-flex mt-3" style="gap: 8px; flex-wrap: wrap">
        <v-text-field v-model="credential.username" density="compact" hide-details clearable
                      label="小米账号（手机号 / 邮箱 / 账号ID）" :placeholder="cfg?.username || '留空用已保存的'"
                      style="min-width: 220px; flex: 1 1 220px" @keyup.enter="startLogin" />
        <v-text-field v-model="credential.password" density="compact" hide-details
                      :type="credential.password ? 'text' : 'password'"
                      :label="cfg?.password_set ? '密码（留空则用已保存的）' : '密码'"
                      style="min-width: 180px; flex: 1 1 180px" @keyup.enter="startLogin" />
        <v-btn color="primary" variant="flat" class="text-none" :loading="busy === '登录' || busy === '保存账号'"
               prepend-icon="mdi-login" @click="startLogin">登录
        </v-btn>
      </div>
      <div class="text-caption text-grey mt-1">
        账号密码只存进本机配置、接口永不回显明文；点「登录」即先保存再发起，票据存在插件数据里。
        小米判定「新设备」时会要图形验证码或手机/邮箱验证码，下面那步照提示填进来就行。
      </div>
    </template>

    <!-- 二、扫码 -->
    <template v-else-if="loginMode === 'qrcode'">
      <div class="d-flex mt-3" style="gap: 14px; flex-wrap: wrap; align-items: flex-start">
        <v-img v-if="qr?.qr_image" :src="qr.qr_image" width="184" height="184" class="rounded" contain />
        <div v-else class="xcl-qr d-flex flex-column align-center justify-center rounded">
          <v-icon :icon="qrBusy ? 'mdi-loading mdi-refresh' : 'mdi-qrcode-scan'"
                  :color="qrBusy ? 'primary' : 'grey'" size="40" />
          <span class="text-caption text-grey mt-1">{{ qrBusy ? '二维码获取中…' : '没取到二维码' }}</span>
        </div>
        <div style="min-width: 220px; flex: 1 1 220px">
          <div class="text-body-2">{{ qrText || '切到这个标签就已经取好一张，直接用手机 App 扫' }}</div>
          <div v-if="qr?.message && qrText" class="text-caption text-grey mt-1">{{ qr.message }}</div>
          <v-progress-linear v-if="qr?.state === 'qr_waiting' && qr.expires_in" height="4" rounded
                             color="primary" class="mt-2"
                             :model-value="Math.max(0, Math.min(100, qrLeft / (qr.expires_in || 1) * 100))" />
          <div class="d-flex mt-2" style="gap: 8px">
            <v-btn size="small" variant="tonal" color="primary" :loading="qrBusy"
                   :prepend-icon="qr?.qr_image ? 'mdi-refresh' : 'mdi-qrcode-scan'"
                   @click="startQr">{{ qr?.qr_image ? '换一张' : '获取二维码' }}
            </v-btn>
            <v-btn v-if="qr" size="small" variant="text" @click="cancelQr">取消</v-btn>
          </div>
        </div>
      </div>
      <div class="text-caption text-grey mt-2">
        用米家 / 小米音箱 App 扫码后在手机上确认，页面会自动问到结果；中途过期就点「换一张」。
        这条路不走密码，也就绕开了「新设备登录」那套风控，账密反复登不上时通常它能用。
      </div>
    </template>

    <!-- 三、Cookie 票据 -->
    <template v-else>
      <div class="d-flex mt-3" style="gap: 8px; flex-wrap: wrap">
        <v-text-field v-model="manual.user_id" density="compact" hide-details clearable
                      label="User ID" placeholder="浏览器 Cookie 里的 userId"
                      style="min-width: 160px; flex: 1 1 160px" @keyup.enter="submitManual" />
        <v-text-field v-model="manual.pass_token" density="compact" hide-details clearable
                      label="Pass Token" placeholder="浏览器 Cookie 里的 passToken"
                      style="min-width: 220px; flex: 2 1 260px" @keyup.enter="submitManual" />
        <v-btn size="small" variant="tonal" color="primary" :loading="busy === '校验票据'"
               prepend-icon="mdi-key-variant" @click="submitManual">校验并使用
        </v-btn>
        <v-btn size="small" variant="text" @click="manual = { user_id: '', pass_token: '' }">清空</v-btn>
      </div>
      <div class="text-caption text-grey mt-1">
        先在浏览器里登录 <span class="text-no-wrap">account.xiaomi.com</span>（passToken 只在这个域的
        Cookie 里，别处复制的都是死票），再从 DevTools 的 Cookie 里取 userId 和 passToken 两项的
        <b>值</b>填进来（别粘整串 cookie）。凭据只发给本插件用于换票，
        不写日志、接口不回显。后端拿 passToken 现换一张活票，换到了才算这串凭据真的有效。
      </div>
    </template>

    <div v-if="loginResult" class="mt-3">
      <v-alert :type="loginState?.color || 'info'" variant="tonal" density="compact"
               :icon="loginState ? undefined : 'mdi-information-outline'">
        <div class="text-body-2">{{ loginState?.text || loginResult.state }}</div>
        <div class="text-caption">{{ loginResult.message }}</div>
        <div v-if="loginResult.state === 'success'" class="text-caption">
          取到 {{ loginResult.device_count }} 台音箱
          <template v-if="loginResult.device_count">｜下面已列出，点「选它」设为默认</template>
        </div>
      </v-alert>
    </div>

    <div v-if="loginState?.color === 'warning'" class="mt-2">
      <v-img v-if="loginResult?.captcha_image" :src="loginResult.captcha_image" max-height="120"
             class="rounded mb-2" style="max-width: 240px" contain />
      <div class="d-flex" style="gap: 8px; flex-wrap: wrap">
        <v-text-field v-model="verifyCode" density="compact" hide-details
                      :label="loginResult?.state === 'need_verify' ? '邮箱/短信验证码' : '图形验证码'"
                      style="max-width: 220px" @keyup.enter="submitVerify" />
        <v-btn size="small" variant="tonal" color="info" :loading="busy === '提交验证码'"
               @click="submitVerify">提交
        </v-btn>
        <v-btn size="small" variant="tonal" :loading="busy === '重发验证码'"
               prepend-icon="mdi-send" @click="resendVerify">重发验证码
        </v-btn>
        <span v-if="loginResult?.state === 'need_verify'" class="text-caption text-grey align-self-center">
          收不到就点重发：会重新走一遍登录与发码，小米才会再寄一次
        </span>
      </div>
    </div>

    <!-- 音箱 -->
    <template v-if="devices.length">
      <div class="xcl-sub mt-4">
        音箱
        <span class="text-caption text-grey ml-2">{{ picker ? `${devices.length} 台可选` : '点标题展开' }}</span>
        <v-btn size="x-small" variant="text" class="text-none ml-1"
               :prepend-icon="picker ? 'mdi-chevron-up' : 'mdi-chevron-down'"
               @click="picker = !picker">{{ picker ? '收起' : '选择默认音箱' }}
        </v-btn>
      </div>
      <v-expand-transition>
        <div v-if="picker">
          <div v-for="device in pickerDevices" :key="device.device_id" class="xcl-device">
            <v-icon :icon="device.presence === 'online' ? 'mdi-speaker' : 'mdi-speaker-off'"
                    :color="device.is_default ? 'primary' : (device.presence === 'online' ? 'success' : 'grey')" />
            <div style="min-width: 0; flex: 1 1 200px">
              <div class="text-body-2">
                {{ device.name || device.alias || device.device_id }}
                <v-chip v-if="device.is_default" size="x-small" variant="flat" color="primary" class="ml-1">
                  当前默认
                </v-chip>
              </div>
              <div class="text-caption text-grey">
                {{ device.model || device.hardware || '未知型号' }}｜{{ device.presence === 'online' ? '在线' : '离线' }}
                <template v-if="device.needs_music_api">｜该型号需走 play_music，直链可能被当成电台播放</template>
              </div>
            </div>
            <v-btn size="small" variant="text" color="primary" :disabled="device.is_default"
                   :loading="busy === '设为默认'" @click="selectDevice(device)">
              {{ device.is_default ? '已是默认' : '选它' }}
            </v-btn>
            <v-btn size="small" variant="text" :disabled="!cfg?.default_device_id"
                   :loading="busy === '取消默认'" @click="clearDefault">取消默认</v-btn>
          </div>
          <div class="text-caption text-grey mt-1">
            在线的放前面，离线也能选（音箱上线后照样能用）。不选就落在第一台在线设备上；
            侧栏播放台的操作只作用于这台默认音箱。
          </div>
        </div>
      </v-expand-transition>
    </template>
    <div v-else-if="props.status?.logged_in" class="text-caption text-warning mt-3">
      设备列表是空的：可能票据已失效，点上面的「刷新设备」重试。
    </div>
    <div v-else class="text-caption text-grey mt-3">
      登录成功后这里才会列出音箱，也要选了默认音箱，后面的配置与播放才有意义。
    </div>

    <!-- 分组 -->
    <template v-if="devices.length > 1">
      <div class="xcl-sub mt-4">
        设备分组
        <span class="text-caption text-grey ml-2">{{ groups.length }} 组</span>
      </div>
      <v-card v-for="row in groups" :key="row.name" variant="outlined" class="xcl-card pa-3 mb-2">
        <div class="d-flex align-center" style="gap: 10px; flex-wrap: wrap">
          <v-icon icon="mdi-home-group" color="primary" />
          <div style="min-width: 0; flex: 1 1 200px">
            <div class="text-body-2">
              {{ row.name }}
              <v-chip v-if="defaultGroup === row.name" size="x-small" variant="flat" color="primary" class="ml-1">
                默认音箱所在组
              </v-chip>
            </div>
            <div class="text-caption text-grey">{{ row.devices.join('、') || row.device_ids.join('、') }}</div>
          </div>
          <v-btn size="x-small" variant="text" :loading="busy === '保存分组'" @click="editGroup(row)">编辑</v-btn>
          <v-btn size="x-small" variant="text" color="error" :loading="busy === '删除分组'"
                 @click="removeGroup(row.name)">删除
          </v-btn>
        </div>
      </v-card>

      <v-card variant="outlined" class="xcl-card pa-3">
        <div class="d-flex align-center" style="gap: 8px; flex-wrap: wrap">
          <v-text-field v-model="groupForm.name" density="compact" hide-details clearable
                        :label="editingGroup ? `改名或另起一组（原「${editingGroup}」）` : '新建分组'"
                        placeholder="全屋 / 卧室 / 客厅…" style="max-width: 220px" />
          <v-combobox v-model="groupForm.device_ids" :items="groupDeviceItems" item-title="title"
                      item-value="value" density="compact" hide-details multiple chips closable-chips
                      label="组内音箱（≥2 台）" style="min-width: 260px; flex: 1 1 260px" />
          <v-btn size="small" variant="tonal" color="primary" :loading="busy === '保存分组'"
                 @click="saveGroup">保存
          </v-btn>
          <v-btn size="small" variant="text" @click="newGroup">清空</v-btn>
        </div>
        <div class="text-caption text-grey mt-1">
          分组里的音箱共用同一份队列、播放方式与定时，播放/切曲/音量/播报都会同步过去；
          一台设备只属于一个组，选中别组已有的音箱时会自动从原组里挪过来，原组不足两台就随之解散。
          默认音箱所在的那一组即「全屋广播」的落点。
        </div>
      </v-card>
    </template>

    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3500" location="bottom">
      {{ snackbar.text }}
    </v-snackbar>
  </div>
</template>

<style scoped>
/* 仅插件内局部样式；含 .v- 的选择器会被构建的 postcss 门禁整条删掉。配色取宿主变量。 */
.xcl {
  display: flex; flex-direction: column;
  --xcl-radius: 14px;
  --xcl-ink: rgba(var(--v-theme-on-surface), .92);
  --xcl-ink-faint: rgba(var(--v-theme-on-surface), .5);
  --xcl-line: rgba(var(--v-border-color), var(--v-border-opacity));
}
.xcl-title {
  display: flex; align-items: center; gap: 8px; margin-bottom: 12px;
  font-size: 13px; font-weight: 650; letter-spacing: .3px; color: var(--xcl-ink);
}
.xcl-step {
  display: inline-flex; align-items: center; justify-content: center;
  width: 20px; height: 20px; border-radius: 7px; flex: 0 0 auto;
  font-size: 11.5px; font-weight: 700;
  color: rgb(var(--v-theme-primary)); background: rgba(var(--v-theme-primary), .14);
}
.xcl-sub {
  display: flex; align-items: center; gap: 6px; margin-bottom: 8px;
  font-size: 12px; font-weight: 650; color: var(--xcl-ink-faint);
}
.xcl-now {
  border-radius: 12px; border: 1px solid var(--xcl-line);
  background: rgba(var(--v-theme-success), .07);
}
.xcl-device {
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  padding: 7px 0; border-bottom: 1px dashed var(--xcl-line);
}
.xcl-qr {
  width: 184px; height: 184px; flex: 0 0 auto;
  border: 1px dashed var(--xcl-line); background: rgba(var(--v-theme-on-surface), .03);
}
.xcl-device:last-of-type { border-bottom: none; }
.xcl-card { border-radius: var(--xcl-radius); }
</style>
