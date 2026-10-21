import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { C as ConfigForm } from './ConfigForm-CicyDqJK.js';
import { s as statusColor, f as fmtSeconds, a as fmtSize, b as fmtTime, q as query } from './types-BWxQ1VQd.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,unref:_unref,renderList:_renderList,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,normalizeClass:_normalizeClass,withKeys:_withKeys,vShow:_vShow,withDirectives:_withDirectives} = await importShared('vue');

const _hoisted_1 = { class: "xai-head" };
const _hoisted_2 = { class: "xai-head-text" };
const _hoisted_3 = { class: "text-caption text-medium-emphasis" };
const _hoisted_4 = { class: "xai-body" };
const _hoisted_5 = { class: "xai-nav" };
const _hoisted_6 = { class: "xai-content" };
const _hoisted_7 = { class: "xai-pane-title" };
const _hoisted_8 = { class: "xai-pane" };
const _hoisted_9 = {
  class: "d-flex align-center",
  style: { "gap": "12px", "flex-wrap": "wrap" }
};
const _hoisted_10 = { style: { "min-width": "0", "flex": "1 1 240px" } };
const _hoisted_11 = ["title"];
const _hoisted_12 = { class: "text-caption text-grey" };
const _hoisted_13 = {
  key: 0,
  class: "text-caption text-error"
};
const _hoisted_14 = { class: "text-right" };
const _hoisted_15 = { class: "text-caption text-grey" };
const _hoisted_16 = {
  class: "d-flex align-center mt-3",
  style: { "gap": "6px", "flex-wrap": "wrap" }
};
const _hoisted_17 = { class: "xai-block" };
const _hoisted_18 = { class: "xai-block-title" };
const _hoisted_19 = { class: "text-caption text-grey ml-2" };
const _hoisted_20 = ["onClick"];
const _hoisted_21 = { class: "xai-queue-index" };
const _hoisted_22 = ["title"];
const _hoisted_23 = { class: "text-caption text-grey" };
const _hoisted_24 = {
  key: 2,
  class: "text-caption text-grey"
};
const _hoisted_25 = { class: "xai-block" };
const _hoisted_26 = {
  class: "d-flex",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_27 = {
  class: "d-flex mt-2",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_28 = { class: "xai-pane" };
const _hoisted_29 = {
  class: "d-flex align-center mb-2",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_30 = { class: "d-flex align-center flex-wrap gap-1 mb-2" };
const _hoisted_31 = { class: "text-caption text-grey" };
const _hoisted_32 = {
  key: 0,
  class: "text-caption text-grey"
};
const _hoisted_33 = ["onClick"];
const _hoisted_34 = ["title"];
const _hoisted_35 = {
  key: 0,
  class: "text-caption text-grey text-truncate"
};
const _hoisted_36 = { class: "text-caption" };
const _hoisted_37 = { class: "text-caption" };
const _hoisted_38 = {
  key: 2,
  class: "text-caption text-grey"
};
const _hoisted_39 = { class: "xai-pane" };
const _hoisted_40 = {
  key: 0,
  class: "d-flex align-center",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_41 = {
  class: "text-body-2",
  style: { "flex": "1 1 220px" }
};
const _hoisted_42 = { class: "text-caption text-grey" };
const _hoisted_43 = {
  class: "d-flex",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_44 = {
  key: 0,
  class: "mt-3"
};
const _hoisted_45 = {
  class: "d-flex",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_46 = {
  key: 1,
  class: "text-caption text-grey mt-2"
};
const _hoisted_47 = {
  key: 0,
  class: "mt-3"
};
const _hoisted_48 = {
  class: "d-flex mt-1",
  style: { "gap": "8px" }
};
const _hoisted_49 = { class: "text-caption text-grey mt-2" };
const _hoisted_50 = { class: "xai-block-title" };
const _hoisted_51 = { class: "text-caption text-grey ml-2" };
const _hoisted_52 = {
  class: "d-flex align-center",
  style: { "gap": "10px", "flex-wrap": "wrap" }
};
const _hoisted_53 = { style: { "min-width": "0", "flex": "1 1 200px" } };
const _hoisted_54 = { class: "text-body-2" };
const _hoisted_55 = { class: "text-caption text-grey" };
const _hoisted_56 = {
  key: 0,
  class: "xai-pane xai-pane-config"
};
const {ref,computed,inject,onMounted,onUnmounted} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "AppPage",
  props: {
    api: { type: Object, default: () => ({}) },
    nativeSubscribe: { type: Function, default: null },
    navKey: { type: String, default: "main" },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" }
  },
  emits: ["action"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`);
    const toast = inject("moviepilot:toast", null);
    const EMPTY_STATUS = {
      enabled: false,
      version: "",
      logged_in: false,
      token_saved_at: "",
      token_expired: false,
      manual_token: false,
      needs: [],
      notices: [],
      devices: [],
      default_device: null,
      playing: {
        device_id: "",
        device_name: "",
        status: -1,
        status_text: "未知",
        volume: null,
        position: null,
        duration: null,
        title: "",
        path: "",
        url: "",
        source: "",
        updated_at: "",
        error: ""
      },
      queue: { device_id: "", index: -1, total: 0, items: [], auto_next: false, source: "" },
      config: {
        enabled: false,
        show_sidebar_nav: true,
        username: "",
        password_set: false,
        default_device_id: "",
        music_dirs: "",
        dir_count: 0,
        missing_dirs: [],
        base_url: "",
        base_url_effective: "",
        base_url_source: "",
        play_mode: "auto",
        audio_exts: "",
        audio_id: "",
        stream_ttl: 3600,
        default_volume: 0,
        auto_next: true,
        keep_light: false,
        tts_announce: false
      },
      extra: {}
    };
    const status = ref({ ...EMPTY_STATUS });
    const loading = ref(false);
    const busy = ref("");
    const snackbar = ref({ show: false, text: "", color: "info" });
    const pane = ref("play");
    const cfg = computed(() => status.value.config);
    const playing = computed(() => status.value.playing);
    const queue = computed(() => status.value.queue);
    const devices = computed(() => status.value.devices || []);
    const needs = computed(() => status.value.needs || []);
    const notices = computed(() => status.value.notices || []);
    function notify(msg, color = "info") {
      if (toast) {
        const fn = toast[color] || toast.info;
        fn.call(toast, msg);
        return;
      }
      snackbar.value = { show: true, text: msg, color };
    }
    async function loadStatus(showError = true) {
      try {
        status.value = await props.api.get(`${base.value}/status`);
        if (!volumeTouched.value) {
          volume.value = playing.value.volume ?? cfg.value.default_volume ?? 50;
        }
      } catch (e) {
        if (showError) notify(`状态加载失败：${e?.message || e}`, "error");
      }
    }
    let timer = null;
    function startPolling() {
      stopPolling();
      timer = setInterval(() => {
        if (document.hidden) return;
        loadStatus(false);
      }, 5e3);
    }
    function stopPolling() {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
    }
    const volume = ref(50);
    const volumeTouched = ref(false);
    const ttsText = ref("");
    const progress = computed(() => {
      const position = playing.value.position;
      const duration = playing.value.duration;
      if (!position || !duration || duration <= 0) return 0;
      return Math.min(100, position / duration * 100);
    });
    async function post(path, body, label) {
      busy.value = label;
      try {
        const res = await props.api.post(`${base.value}${path}`, body);
        if (res && res.ok === false) {
          notify(res.message || res.error || `${label}失败`, "warning");
        } else if (res && (res.message || res.ok === true)) {
          notify(res.message || `${label}已执行`);
        }
        await loadStatus(false);
        emit("action");
        return res;
      } catch (e) {
        notify(`${label}失败：${e?.message || e}`, "error");
        return null;
      } finally {
        busy.value = "";
      }
    }
    function control(action) {
      return post("/control", { action }, "播放控制");
    }
    function setVolume() {
      volumeTouched.value = true;
      return post("/volume", { volume: Math.round(volume.value) }, "设置音量");
    }
    function speak() {
      const text = ttsText.value.trim();
      if (!text) {
        notify("先写上要播报的内容", "warning");
        return;
      }
      return post("/tts", { text: text.slice(0, 200) }, "播报").then((res) => {
        if (res) ttsText.value = "";
      });
    }
    function playFile(row) {
      return post("/play", { path: row.path }, "播放");
    }
    function playUrl() {
      const url = externalUrl.value.trim();
      if (!url) {
        notify("先粘贴一条音频直链", "warning");
        return;
      }
      return post("/play", { url }, "播放直链").then(() => {
        externalUrl.value = "";
      });
    }
    const externalUrl = ref("");
    async function enqueue(path) {
      const items = (queue.value.items || []).map((i) => i.path);
      if (items.includes(path)) {
        notify("这一首已经在队列里了", "warning");
        return;
      }
      await post("/queue", { paths: [...items, path], index: items.length ? queue.value.index : 0 }, "加入队列");
    }
    function queueDirectory() {
      const paths = browseFiles.value.map((f) => f.path);
      if (!paths.length) {
        notify("这个目录里没有可播放的音频", "warning");
        return;
      }
      return post("/queue", { paths, index: 0 }, "整目录入队");
    }
    function jumpTo(index) {
      return post("/control", { action: String(index) }, "切曲");
    }
    function selectDevice(device) {
      return post("/device/select", { device_id: device.device_id }, "设为默认");
    }
    function clearDefault() {
      return post("/device/select", { device_id: "" }, "取消默认音箱");
    }
    const loginForm = ref({ username: "", password: "" });
    const loginResult = ref(null);
    const verifyCode = ref("");
    const manualToken = ref("");
    const showManual = ref(false);
    const loginPending = computed(() => {
      const state = loginResult.value?.state;
      return state === "need_captcha" || state === "need_verify";
    });
    async function startLogin() {
      busy.value = "登录";
      try {
        const res = await props.api.post(`${base.value}/login`, {
          username: loginForm.value.username.trim(),
          password: loginForm.value.password
        });
        afterLogin(res);
      } catch (e) {
        notify(`登录请求失败：${e?.message || e}`, "error");
      } finally {
        busy.value = "";
      }
    }
    async function submitVerify() {
      const code = verifyCode.value.trim();
      if (!code) {
        notify("填写验证码", "warning");
        return;
      }
      busy.value = "提交验证码";
      try {
        const res = await props.api.post(`${base.value}/login`, { code });
        afterLogin(res);
      } catch (e) {
        notify(`验证码提交失败：${e?.message || e}`, "error");
      } finally {
        busy.value = "";
      }
    }
    async function submitManual() {
      const raw = manualToken.value.trim();
      if (!raw) {
        notify("把整串 cookie 粘进来（要包含 userId 与 serviceToken）", "warning");
        return;
      }
      busy.value = "校验票据";
      try {
        const res = await props.api.post(
          `${base.value}/login`,
          { manual_token: raw }
        );
        afterLogin(res);
      } catch (e) {
        notify(`票据校验失败：${e?.message || e}`, "error");
      } finally {
        busy.value = "";
      }
    }
    function afterLogin(res) {
      loginResult.value = res || null;
      verifyCode.value = "";
      if (res?.state === "success") {
        notify(res.message || "登录成功", "success");
        loginForm.value.password = "";
        showManual.value = false;
        manualToken.value = "";
        loadStatus(false);
        return;
      }
      if (res?.state === "need_captcha" || res?.state === "need_verify") {
        notify(res.message || "需要验证码", "warning");
        return;
      }
      notify(res?.message || "登录失败", "error");
    }
    async function doLogout() {
      const res = await post("/logout", {}, "退出登录");
      if (res) {
        loginResult.value = null;
        loginForm.value = { username: loginForm.value.username, password: "" };
      }
    }
    async function refreshDevices() {
      busy.value = "刷新设备";
      try {
        const res = await props.api.get(`${base.value}/devices`);
        if (res?.message && !res?.devices?.length) notify(`设备刷新失败：${res.message}`, "warning");
        else notify(`设备列表已刷新，共 ${(res?.devices || []).length} 台`);
        await loadStatus(false);
      } catch (e) {
        notify(`设备刷新失败：${e?.message || e}`, "error");
      } finally {
        busy.value = "";
      }
    }
    const browse = ref({ cwd: "", parent: "", roots: [], dirs: [], files: [], reason: "", truncated: false });
    const search = ref({ keyword: "", items: [], reason: "" });
    const mode = ref("browse");
    const keyword = ref("");
    let searchTimer = null;
    const browseFiles = computed(() => mode.value === "search" ? search.value.items : browse.value.files);
    const queueAllLabel = computed(() => mode.value === "search" ? "搜索结果入队并播放" : "整目录入队并播放");
    const BROWSE_REASON = {
      no_dirs: "还没有配置「音乐库路径」，先在配置里填上目录",
      not_dir: "这不是一个目录，或者它已经不存在了",
      unresolvable: "路径解析不了（软链或权限问题）",
      out_of_library: "这个目录不在配置的音乐库范围内"
    };
    const browseReasonText = computed(() => {
      const reason = browse.value.reason || "";
      if (!reason || reason === "truncated") return "";
      return BROWSE_REASON[reason] || reason;
    });
    const SEARCH_REASON = {
      no_dirs: "还没有配置「音乐库路径」",
      no_hits: "库里没有文件名匹配这个关键词的音频"
    };
    async function loadBrowse(path) {
      try {
        browse.value = await props.api.get(`${base.value}/browse${query({ path })}`);
      } catch (e) {
        notify(`目录加载失败：${e?.message || e}`, "error");
      }
    }
    async function runSearch() {
      const text = keyword.value.trim();
      if (!text) {
        search.value = { keyword: "", items: [], reason: "" };
        return;
      }
      try {
        search.value = await props.api.get(`${base.value}/search${query({ keyword: text })}`);
      } catch (e) {
        notify(`搜索失败：${e?.message || e}`, "error");
      }
    }
    function onKeyword() {
      if (searchTimer) clearTimeout(searchTimer);
      searchTimer = setTimeout(runSearch, 350);
    }
    async function switchMode(next) {
      mode.value = next;
      if (next === "search" && keyword.value.trim()) await runSearch();
      if (next === "browse" && !browse.value.cwd) await loadBrowse("");
    }
    onMounted(async () => {
      await loadStatus();
      await loadBrowse("");
      startPolling();
    });
    onUnmounted(stopPolling);
    const paneItems = [
      { key: "play", title: "播放台", icon: "mdi-speaker", desc: "当前播放、队列连播、音量与播报" },
      { key: "library", title: "音乐库", icon: "mdi-folder-music-outline", desc: "浏览库内目录与搜索文件名，点一首直接推到音箱" },
      { key: "account", title: "账号与设备", icon: "mdi-account-circle-outline", desc: "小米账号登录、音箱列表与默认音箱" },
      { key: "config", title: "配置", icon: "mdi-cog-outline", desc: "音乐库范围、外部访问地址与播放接口" }
    ];
    const currentPane = computed(() => paneItems.find((p) => p.key === pane.value) || paneItems[0]);
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_avatar = _resolveComponent("v-avatar");
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_divider = _resolveComponent("v-divider");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_list_item_title = _resolveComponent("v-list-item-title");
      const _component_v_list_item = _resolveComponent("v-list-item");
      const _component_v_list = _resolveComponent("v-list");
      const _component_v_progress_linear = _resolveComponent("v-progress-linear");
      const _component_v_slider = _resolveComponent("v-slider");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_btn_toggle = _resolveComponent("v-btn-toggle");
      const _component_v_table = _resolveComponent("v-table");
      const _component_v_img = _resolveComponent("v-img");
      const _component_v_textarea = _resolveComponent("v-textarea");
      const _component_v_expand_transition = _resolveComponent("v-expand-transition");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock(), _createBlock(_component_v_card, {
        flat: "",
        class: "xai-page"
      }, {
        default: _withCtx(() => [
          _createElementVNode("div", _hoisted_1, [
            _createVNode(_component_v_avatar, {
              color: "primary",
              variant: "tonal",
              rounded: "lg",
              size: "34",
              class: "mr-3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_icon, {
                  icon: "mdi-speaker",
                  size: "22"
                })
              ]),
              _: 1
            }),
            _createElementVNode("div", _hoisted_2, [
              _cache[21] || (_cache[21] = _createElementVNode("div", { class: "text-h6" }, "小爱音箱", -1)),
              _createElementVNode("div", _hoisted_3, " 库内音频直链推送到小爱音箱 · v" + _toDisplayString(status.value.version || "-"), 1)
            ]),
            _createVNode(_component_v_spacer),
            _createVNode(_component_v_chip, {
              size: "small",
              variant: "tonal",
              class: "mr-2",
              color: status.value.token_expired ? "error" : status.value.logged_in ? "success" : "warning",
              "prepend-icon": status.value.logged_in ? "mdi-check-circle-outline" : "mdi-alert-circle-outline"
            }, {
              default: _withCtx(() => [
                _createTextVNode(_toDisplayString(status.value.token_expired ? "票据已过期" : status.value.logged_in ? "已登录" : "未登录"), 1)
              ]),
              _: 1
            }, 8, ["color", "prepend-icon"]),
            _createVNode(_component_v_chip, {
              size: "small",
              variant: "tonal",
              class: "mr-2",
              color: _unref(statusColor)(playing.value.status),
              "prepend-icon": playing.value.status === 1 ? "mdi-play-circle-outline" : "mdi-pause-circle-outline"
            }, {
              default: _withCtx(() => [
                _createTextVNode(_toDisplayString(playing.value.device_name || "无设备") + " · " + _toDisplayString(playing.value.status_text), 1)
              ]),
              _: 1
            }, 8, ["color", "prepend-icon"]),
            _createVNode(_component_v_btn, {
              size: "small",
              variant: "text",
              class: "text-none",
              loading: loading.value || !!busy.value,
              "prepend-icon": "mdi-refresh",
              onClick: _cache[0] || (_cache[0] = ($event) => loadStatus())
            }, {
              default: _withCtx(() => [..._cache[22] || (_cache[22] = [
                _createTextVNode("刷新", -1)
              ])]),
              _: 1
            }, 8, ["loading"])
          ]),
          _createVNode(_component_v_divider),
          needs.value.length || notices.value.length ? (_openBlock(), _createBlock(_component_v_alert, {
            key: 0,
            type: "info",
            variant: "tonal",
            density: "compact",
            class: "ma-3"
          }, {
            default: _withCtx(() => [
              (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(needs.value, (item) => {
                return _openBlock(), _createElementBlock("div", {
                  key: "n" + item,
                  class: "text-body-2"
                }, [
                  _createVNode(_component_v_icon, {
                    icon: "mdi-form-select",
                    size: "14",
                    class: "mr-1"
                  }),
                  _createTextVNode(_toDisplayString(item), 1)
                ]);
              }), 128)),
              (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(notices.value, (item) => {
                return _openBlock(), _createElementBlock("div", {
                  key: "m" + item,
                  class: "text-caption text-grey-darken-1"
                }, _toDisplayString(item), 1);
              }), 128))
            ]),
            _: 1
          })) : _createCommentVNode("", true),
          _createElementVNode("div", _hoisted_4, [
            _createElementVNode("nav", _hoisted_5, [
              _createVNode(_component_v_list, {
                density: "comfortable",
                nav: "",
                class: "py-2"
              }, {
                default: _withCtx(() => [
                  (_openBlock(), _createElementBlock(_Fragment, null, _renderList(paneItems, (item) => {
                    return _createVNode(_component_v_list_item, {
                      key: item.key,
                      active: pane.value === item.key,
                      color: "primary",
                      rounded: "lg",
                      class: "xai-nav-item",
                      onClick: ($event) => pane.value = item.key
                    }, {
                      prepend: _withCtx(() => [
                        _createVNode(_component_v_icon, {
                          icon: item.icon,
                          class: "mr-2"
                        }, null, 8, ["icon"])
                      ]),
                      default: _withCtx(() => [
                        _createVNode(_component_v_list_item_title, null, {
                          default: _withCtx(() => [
                            _createTextVNode(_toDisplayString(item.title), 1)
                          ]),
                          _: 2
                        }, 1024)
                      ]),
                      _: 2
                    }, 1032, ["active", "onClick"]);
                  }), 64))
                ]),
                _: 1
              })
            ]),
            _createElementVNode("section", _hoisted_6, [
              _createElementVNode("div", _hoisted_7, _toDisplayString(currentPane.value.desc), 1),
              _withDirectives(_createElementVNode("div", _hoisted_8, [
                _createVNode(_component_v_card, {
                  variant: "outlined",
                  class: "pa-4"
                }, {
                  default: _withCtx(() => [
                    _createElementVNode("div", _hoisted_9, [
                      _createVNode(_component_v_icon, {
                        icon: playing.value.status === 1 ? "mdi-volume-high" : "mdi-volume-variant-off",
                        size: "34",
                        color: _unref(statusColor)(playing.value.status)
                      }, null, 8, ["icon", "color"]),
                      _createElementVNode("div", _hoisted_10, [
                        _createElementVNode("div", {
                          class: "text-subtitle-1 text-truncate",
                          title: playing.value.title || playing.value.path
                        }, _toDisplayString(playing.value.title || playing.value.path || playing.value.url || "音箱上没有在放的东西"), 9, _hoisted_11),
                        _createElementVNode("div", _hoisted_12, [
                          _createTextVNode(_toDisplayString(playing.value.device_name || "未选择设备") + " ", 1),
                          playing.value.source ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                            _createTextVNode("｜来源 " + _toDisplayString(playing.value.source), 1)
                          ], 64)) : _createCommentVNode("", true),
                          playing.value.updated_at ? (_openBlock(), _createElementBlock(_Fragment, { key: 1 }, [
                            _createTextVNode("｜" + _toDisplayString(playing.value.updated_at), 1)
                          ], 64)) : _createCommentVNode("", true)
                        ]),
                        playing.value.error ? (_openBlock(), _createElementBlock("div", _hoisted_13, _toDisplayString(playing.value.error), 1)) : _createCommentVNode("", true)
                      ]),
                      _createElementVNode("div", _hoisted_14, [
                        _createElementVNode("div", _hoisted_15, _toDisplayString(_unref(fmtSeconds)(playing.value.position)) + " / " + _toDisplayString(_unref(fmtSeconds)(playing.value.duration)), 1),
                        _createVNode(_component_v_chip, {
                          size: "x-small",
                          variant: "tonal",
                          color: _unref(statusColor)(playing.value.status)
                        }, {
                          default: _withCtx(() => [
                            _createTextVNode(_toDisplayString(playing.value.status_text), 1)
                          ]),
                          _: 1
                        }, 8, ["color"])
                      ])
                    ]),
                    _createVNode(_component_v_progress_linear, {
                      "model-value": progress.value,
                      height: "4",
                      color: "primary",
                      class: "mt-3",
                      rounded: ""
                    }, null, 8, ["model-value"]),
                    _createElementVNode("div", _hoisted_16, [
                      _createVNode(_component_v_btn, {
                        size: "small",
                        variant: "tonal",
                        "prepend-icon": "mdi-previous",
                        loading: busy.value === "播放控制",
                        onClick: _cache[1] || (_cache[1] = ($event) => control("prev"))
                      }, {
                        default: _withCtx(() => [..._cache[23] || (_cache[23] = [
                          _createTextVNode("上一首", -1)
                        ])]),
                        _: 1
                      }, 8, ["loading"]),
                      _createVNode(_component_v_btn, {
                        size: "small",
                        color: "primary",
                        variant: "flat",
                        icon: playing.value.status === 1 ? "mdi-pause" : "mdi-play",
                        loading: busy.value === "播放控制",
                        onClick: _cache[2] || (_cache[2] = ($event) => control(playing.value.status === 1 ? "pause" : "play"))
                      }, null, 8, ["icon", "loading"]),
                      _createVNode(_component_v_btn, {
                        size: "small",
                        variant: "tonal",
                        "append-icon": "mdi-skip-next",
                        loading: busy.value === "播放控制",
                        onClick: _cache[3] || (_cache[3] = ($event) => control("next"))
                      }, {
                        default: _withCtx(() => [..._cache[24] || (_cache[24] = [
                          _createTextVNode("下一首", -1)
                        ])]),
                        _: 1
                      }, 8, ["loading"]),
                      _createVNode(_component_v_btn, {
                        size: "small",
                        variant: "tonal",
                        color: "error",
                        "prepend-icon": "mdi-stop",
                        loading: busy.value === "播放控制",
                        onClick: _cache[4] || (_cache[4] = ($event) => control("stop"))
                      }, {
                        default: _withCtx(() => [..._cache[25] || (_cache[25] = [
                          _createTextVNode("停止", -1)
                        ])]),
                        _: 1
                      }, 8, ["loading"]),
                      _createVNode(_component_v_spacer),
                      _cache[26] || (_cache[26] = _createElementVNode("span", { class: "text-caption text-grey" }, "音量", -1)),
                      _createVNode(_component_v_slider, {
                        modelValue: volume.value,
                        "onUpdate:modelValue": [
                          _cache[5] || (_cache[5] = ($event) => volume.value = $event),
                          _cache[6] || (_cache[6] = ($event) => volumeTouched.value = true)
                        ],
                        min: "0",
                        max: "100",
                        step: "1",
                        "hide-details": "",
                        density: "compact",
                        style: { "max-width": "160px" }
                      }, null, 8, ["modelValue"]),
                      _createVNode(_component_v_btn, {
                        size: "small",
                        variant: "tonal",
                        loading: busy.value === "设置音量",
                        onClick: setVolume
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode("设为 " + _toDisplayString(Math.round(volume.value)), 1)
                        ]),
                        _: 1
                      }, 8, ["loading"])
                    ])
                  ]),
                  _: 1
                }),
                _createElementVNode("div", _hoisted_17, [
                  _createElementVNode("div", _hoisted_18, [
                    _cache[27] || (_cache[27] = _createTextVNode(" 播放队列 ", -1)),
                    _createVNode(_component_v_chip, {
                      size: "x-small",
                      variant: "tonal",
                      class: "ml-2",
                      color: queue.value.auto_next ? "success" : "grey"
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode(_toDisplayString(queue.value.auto_next ? "自动连播开" : "自动连播关"), 1)
                      ]),
                      _: 1
                    }, 8, ["color"]),
                    _createElementVNode("span", _hoisted_19, _toDisplayString(queue.value.total) + " 首", 1)
                  ]),
                  !cfg.value.auto_next ? (_openBlock(), _createBlock(_component_v_alert, {
                    key: 0,
                    type: "warning",
                    variant: "tonal",
                    density: "compact",
                    class: "mb-2"
                  }, {
                    default: _withCtx(() => [..._cache[28] || (_cache[28] = [
                      _createTextVNode(" 配置里的「队列自动连播」是关的，音箱播完当前这首不会自己接下一首（这里的手动「下一首」仍然有效）。 ", -1)
                    ])]),
                    _: 1
                  })) : _createCommentVNode("", true),
                  queue.value.total ? (_openBlock(), _createBlock(_component_v_card, {
                    key: 1,
                    variant: "outlined",
                    class: "xai-queue"
                  }, {
                    default: _withCtx(() => [
                      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(queue.value.items, (item, i) => {
                        return _openBlock(), _createElementBlock("div", {
                          key: item.path,
                          class: _normalizeClass(["xai-queue-row", { "xai-queue-current": i === queue.value.index }]),
                          onClick: ($event) => jumpTo(i)
                        }, [
                          _createElementVNode("span", _hoisted_21, _toDisplayString(i + 1), 1),
                          _createElementVNode("span", {
                            class: "xai-queue-name",
                            title: item.path
                          }, _toDisplayString(item.title || item.name), 9, _hoisted_22),
                          _createElementVNode("span", _hoisted_23, _toDisplayString(_unref(fmtSize)(item.size)), 1),
                          i === queue.value.index ? (_openBlock(), _createBlock(_component_v_icon, {
                            key: 0,
                            icon: "mdi-volume-high",
                            size: "16",
                            color: "primary"
                          })) : _createCommentVNode("", true)
                        ], 10, _hoisted_20);
                      }), 128))
                    ]),
                    _: 1
                  })) : (_openBlock(), _createElementBlock("div", _hoisted_24, " 队列为空。到「音乐库」里点一首的「播放」或「加入队列」，也可以整目录入队。 "))
                ]),
                _createElementVNode("div", _hoisted_25, [
                  _cache[31] || (_cache[31] = _createElementVNode("div", { class: "xai-block-title" }, "播报与外部直链", -1)),
                  _createElementVNode("div", _hoisted_26, [
                    _createVNode(_component_v_text_field, {
                      modelValue: ttsText.value,
                      "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => ttsText.value = $event),
                      density: "compact",
                      "hide-details": "",
                      clearable: "",
                      placeholder: "让音箱说一句话（≤200 字）",
                      style: { "min-width": "240px", "flex": "1 1 260px" },
                      onKeyup: _withKeys(speak, ["enter"])
                    }, null, 8, ["modelValue"]),
                    _createVNode(_component_v_btn, {
                      size: "small",
                      variant: "tonal",
                      color: "info",
                      loading: busy.value === "播报",
                      onClick: speak
                    }, {
                      default: _withCtx(() => [..._cache[29] || (_cache[29] = [
                        _createTextVNode("播报", -1)
                      ])]),
                      _: 1
                    }, 8, ["loading"])
                  ]),
                  _createElementVNode("div", _hoisted_27, [
                    _createVNode(_component_v_text_field, {
                      modelValue: externalUrl.value,
                      "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => externalUrl.value = $event),
                      density: "compact",
                      "hide-details": "",
                      clearable: "",
                      placeholder: "http(s)://…/song.mp3（音箱能访问到的直链）",
                      style: { "min-width": "240px", "flex": "1 1 260px" },
                      onKeyup: _withKeys(playUrl, ["enter"])
                    }, null, 8, ["modelValue"]),
                    _createVNode(_component_v_btn, {
                      size: "small",
                      variant: "tonal",
                      loading: busy.value === "播放直链",
                      onClick: playUrl
                    }, {
                      default: _withCtx(() => [..._cache[30] || (_cache[30] = [
                        _createTextVNode("放这条", -1)
                      ])]),
                      _: 1
                    }, 8, ["loading"])
                  ]),
                  _cache[32] || (_cache[32] = _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 库内文件走的是插件签发的限时直链；外部直链由音箱自己访问，插件不转发内容。 播报失败时音箱可能已经播了（小米的 TTS 接口对同一句会返回 3012）。 ", -1))
                ])
              ], 512), [
                [_vShow, pane.value === "play"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_28, [
                _createElementVNode("div", _hoisted_29, [
                  _createVNode(_component_v_btn_toggle, {
                    modelValue: mode.value,
                    "onUpdate:modelValue": [
                      _cache[9] || (_cache[9] = ($event) => mode.value = $event),
                      _cache[10] || (_cache[10] = ($event) => switchMode($event))
                    ],
                    density: "compact",
                    mandatory: ""
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_btn, {
                        value: "browse",
                        size: "small",
                        variant: "text",
                        "prepend-icon": "mdi-folder-outline"
                      }, {
                        default: _withCtx(() => [..._cache[33] || (_cache[33] = [
                          _createTextVNode("浏览", -1)
                        ])]),
                        _: 1
                      }),
                      _createVNode(_component_v_btn, {
                        value: "search",
                        size: "small",
                        variant: "text",
                        "prepend-icon": "mdi-magnify"
                      }, {
                        default: _withCtx(() => [..._cache[34] || (_cache[34] = [
                          _createTextVNode("搜索", -1)
                        ])]),
                        _: 1
                      })
                    ]),
                    _: 1
                  }, 8, ["modelValue"]),
                  mode.value === "search" ? (_openBlock(), _createBlock(_component_v_text_field, {
                    key: 0,
                    modelValue: keyword.value,
                    "onUpdate:modelValue": [
                      _cache[11] || (_cache[11] = ($event) => keyword.value = $event),
                      onKeyword
                    ],
                    density: "compact",
                    "hide-details": "",
                    clearable: "",
                    "prepend-inner-icon": "mdi-magnify",
                    placeholder: "按文件名搜索库内音频",
                    style: { "max-width": "320px" }
                  }, null, 8, ["modelValue"])) : _createCommentVNode("", true),
                  _createVNode(_component_v_spacer),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "primary",
                    disabled: !browseFiles.value.length,
                    loading: busy.value === "整目录入队",
                    onClick: queueDirectory
                  }, {
                    default: _withCtx(() => [
                      _createTextVNode(_toDisplayString(queueAllLabel.value), 1)
                    ]),
                    _: 1
                  }, 8, ["disabled", "loading"])
                ]),
                mode.value === "browse" ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                  _createElementVNode("div", _hoisted_30, [
                    (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(browse.value.roots, (root) => {
                      return _openBlock(), _createBlock(_component_v_chip, {
                        key: root,
                        size: "x-small",
                        variant: "outlined",
                        class: "cursor-pointer",
                        "prepend-icon": "mdi-harddisk",
                        onClick: ($event) => loadBrowse(root)
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode(_toDisplayString(root), 1)
                        ]),
                        _: 2
                      }, 1032, ["onClick"]);
                    }), 128)),
                    browse.value.parent ? (_openBlock(), _createBlock(_component_v_btn, {
                      key: 0,
                      size: "x-small",
                      variant: "text",
                      "prepend-icon": "mdi-arrow-up",
                      onClick: _cache[12] || (_cache[12] = ($event) => loadBrowse(browse.value.parent))
                    }, {
                      default: _withCtx(() => [..._cache[35] || (_cache[35] = [
                        _createTextVNode("上级", -1)
                      ])]),
                      _: 1
                    })) : _createCommentVNode("", true),
                    _createElementVNode("span", _hoisted_31, _toDisplayString(browse.value.cwd), 1)
                  ]),
                  browseReasonText.value ? (_openBlock(), _createElementBlock("div", _hoisted_32, _toDisplayString(browseReasonText.value), 1)) : _createCommentVNode("", true),
                  browse.value.dirs.length ? (_openBlock(), _createBlock(_component_v_card, {
                    key: 1,
                    variant: "outlined",
                    class: "pa-2 mb-2"
                  }, {
                    default: _withCtx(() => [
                      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(browse.value.dirs, (dir) => {
                        return _openBlock(), _createElementBlock("div", {
                          key: dir.path,
                          class: "xai-dir",
                          onClick: ($event) => loadBrowse(dir.path)
                        }, [
                          _createVNode(_component_v_icon, {
                            icon: "mdi-folder-outline",
                            size: "16",
                            class: "mr-2"
                          }),
                          _createTextVNode(_toDisplayString(dir.name), 1)
                        ], 8, _hoisted_33);
                      }), 128))
                    ]),
                    _: 1
                  })) : _createCommentVNode("", true),
                  browse.value.truncated ? (_openBlock(), _createBlock(_component_v_alert, {
                    key: 2,
                    type: "info",
                    variant: "tonal",
                    density: "compact",
                    class: "mb-2"
                  }, {
                    default: _withCtx(() => [..._cache[36] || (_cache[36] = [
                      _createTextVNode(" 这个目录里的条目太多，只列出了前面一部分。 ", -1)
                    ])]),
                    _: 1
                  })) : _createCommentVNode("", true)
                ], 64)) : _createCommentVNode("", true),
                browseFiles.value.length ? (_openBlock(), _createBlock(_component_v_table, {
                  key: 1,
                  density: "compact",
                  class: "xai-table"
                }, {
                  default: _withCtx(() => [
                    _cache[39] || (_cache[39] = _createElementVNode("thead", null, [
                      _createElementVNode("tr", null, [
                        _createElementVNode("th", { style: { "width": "38%" } }, "文件名"),
                        _createElementVNode("th", null, "大小"),
                        _createElementVNode("th", null, "修改时间"),
                        _createElementVNode("th", { style: { "width": "190px" } }, "操作")
                      ])
                    ], -1)),
                    _createElementVNode("tbody", null, [
                      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(browseFiles.value, (row) => {
                        return _openBlock(), _createElementBlock("tr", {
                          key: row.path
                        }, [
                          _createElementVNode("td", null, [
                            _createElementVNode("div", {
                              class: "text-truncate",
                              title: row.path
                            }, _toDisplayString(row.name), 9, _hoisted_34),
                            row.title ? (_openBlock(), _createElementBlock("div", _hoisted_35, _toDisplayString(row.title), 1)) : _createCommentVNode("", true)
                          ]),
                          _createElementVNode("td", _hoisted_36, _toDisplayString(_unref(fmtSize)(row.size)), 1),
                          _createElementVNode("td", _hoisted_37, _toDisplayString(_unref(fmtTime)(row.mtime)), 1),
                          _createElementVNode("td", null, [
                            _createVNode(_component_v_btn, {
                              size: "x-small",
                              variant: "text",
                              color: "primary",
                              loading: busy.value === "播放",
                              onClick: ($event) => playFile(row)
                            }, {
                              default: _withCtx(() => [..._cache[37] || (_cache[37] = [
                                _createTextVNode("播放", -1)
                              ])]),
                              _: 1
                            }, 8, ["loading", "onClick"]),
                            _createVNode(_component_v_btn, {
                              size: "x-small",
                              variant: "text",
                              loading: busy.value === "加入队列",
                              onClick: ($event) => enqueue(row.path)
                            }, {
                              default: _withCtx(() => [..._cache[38] || (_cache[38] = [
                                _createTextVNode("加入队列", -1)
                              ])]),
                              _: 1
                            }, 8, ["loading", "onClick"])
                          ])
                        ]);
                      }), 128))
                    ])
                  ]),
                  _: 1
                })) : (_openBlock(), _createElementBlock("div", _hoisted_38, _toDisplayString(mode.value === "search" ? search.value.reason ? SEARCH_REASON[search.value.reason] || search.value.reason : "输入关键词后，在库内文件名里搜索。" : browseReasonText.value || "这个目录里没有可播放的音频。"), 1)),
                _cache[40] || (_cache[40] = _createElementVNode("div", { class: "text-caption text-grey mt-2" }, " 只有配置在「音乐库路径」里的目录会被列出；播放走插件本机读文件 + 签名限时直链， 不需要音箱登录任何音乐服务。 ", -1))
              ], 512), [
                [_vShow, pane.value === "library"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_39, [
                _createVNode(_component_v_card, {
                  variant: "outlined",
                  class: "pa-4 mb-3"
                }, {
                  default: _withCtx(() => [
                    _cache[50] || (_cache[50] = _createElementVNode("div", { class: "xai-block-title" }, "小米账号", -1)),
                    status.value.logged_in ? (_openBlock(), _createElementBlock("div", _hoisted_40, [
                      _createElementVNode("div", _hoisted_41, [
                        _createTextVNode(_toDisplayString(cfg.value.username || "（未记录账号名）") + " ", 1),
                        _createElementVNode("div", _hoisted_42, [
                          _createTextVNode(" 票据保存于 " + _toDisplayString(status.value.token_saved_at || "-") + " ", 1),
                          status.value.manual_token ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                            _createTextVNode("｜手工粘贴的 cookie")
                          ], 64)) : _createCommentVNode("", true),
                          status.value.token_expired ? (_openBlock(), _createElementBlock(_Fragment, { key: 1 }, [
                            _createTextVNode("｜已过期，需要重新登录")
                          ], 64)) : _createCommentVNode("", true)
                        ])
                      ]),
                      _createVNode(_component_v_btn, {
                        size: "small",
                        variant: "tonal",
                        loading: busy.value === "刷新设备",
                        "prepend-icon": "mdi-refresh",
                        onClick: refreshDevices
                      }, {
                        default: _withCtx(() => [..._cache[41] || (_cache[41] = [
                          _createTextVNode("刷新设备", -1)
                        ])]),
                        _: 1
                      }, 8, ["loading"]),
                      _createVNode(_component_v_btn, {
                        size: "small",
                        variant: "tonal",
                        color: "error",
                        loading: busy.value === "退出登录",
                        "prepend-icon": "mdi-logout",
                        onClick: doLogout
                      }, {
                        default: _withCtx(() => [..._cache[42] || (_cache[42] = [
                          _createTextVNode("退出登录", -1)
                        ])]),
                        _: 1
                      }, 8, ["loading"])
                    ])) : (_openBlock(), _createElementBlock(_Fragment, { key: 1 }, [
                      _createElementVNode("div", _hoisted_43, [
                        _createVNode(_component_v_text_field, {
                          modelValue: loginForm.value.username,
                          "onUpdate:modelValue": _cache[13] || (_cache[13] = ($event) => loginForm.value.username = $event),
                          density: "compact",
                          "hide-details": "",
                          clearable: "",
                          label: "小米账号（手机号 / 邮箱 / 账号ID）",
                          placeholder: cfg.value.username,
                          style: { "min-width": "220px", "flex": "1 1 220px" }
                        }, null, 8, ["modelValue", "placeholder"]),
                        _createVNode(_component_v_text_field, {
                          modelValue: loginForm.value.password,
                          "onUpdate:modelValue": _cache[14] || (_cache[14] = ($event) => loginForm.value.password = $event),
                          density: "compact",
                          "hide-details": "",
                          type: loginForm.value.password ? "text" : "password",
                          label: cfg.value.password_set ? "密码（留空则用已保存的）" : "密码",
                          style: { "min-width": "180px", "flex": "1 1 180px" },
                          onKeyup: _withKeys(startLogin, ["enter"])
                        }, null, 8, ["modelValue", "type", "label"]),
                        _createVNode(_component_v_btn, {
                          color: "primary",
                          variant: "flat",
                          class: "text-none",
                          loading: busy.value === "登录",
                          "prepend-icon": "mdi-login",
                          onClick: startLogin
                        }, {
                          default: _withCtx(() => [..._cache[43] || (_cache[43] = [
                            _createTextVNode("登录", -1)
                          ])]),
                          _: 1
                        }, 8, ["loading"])
                      ]),
                      loginPending.value ? (_openBlock(), _createElementBlock("div", _hoisted_44, [
                        loginResult.value?.captcha_image ? (_openBlock(), _createBlock(_component_v_img, {
                          key: 0,
                          src: loginResult.value.captcha_image,
                          "max-height": "120",
                          class: "rounded mb-2",
                          style: { "max-width": "240px" },
                          contain: ""
                        }, null, 8, ["src"])) : _createCommentVNode("", true),
                        _createElementVNode("div", _hoisted_45, [
                          _createVNode(_component_v_text_field, {
                            modelValue: verifyCode.value,
                            "onUpdate:modelValue": _cache[15] || (_cache[15] = ($event) => verifyCode.value = $event),
                            density: "compact",
                            "hide-details": "",
                            label: loginResult.value?.state === "need_verify" ? "邮箱/短信验证码" : "图形验证码",
                            style: { "max-width": "220px" },
                            onKeyup: _withKeys(submitVerify, ["enter"])
                          }, null, 8, ["modelValue", "label"]),
                          _createVNode(_component_v_btn, {
                            size: "small",
                            variant: "tonal",
                            color: "info",
                            loading: busy.value === "提交验证码",
                            onClick: submitVerify
                          }, {
                            default: _withCtx(() => [..._cache[44] || (_cache[44] = [
                              _createTextVNode("提交", -1)
                            ])]),
                            _: 1
                          }, 8, ["loading"]),
                          _createVNode(_component_v_btn, {
                            size: "small",
                            variant: "text",
                            onClick: startLogin
                          }, {
                            default: _withCtx(() => [..._cache[45] || (_cache[45] = [
                              _createTextVNode("重新发起登录", -1)
                            ])]),
                            _: 1
                          })
                        ])
                      ])) : loginResult.value ? (_openBlock(), _createElementBlock("div", _hoisted_46, _toDisplayString(loginResult.value.message), 1)) : _createCommentVNode("", true),
                      _createVNode(_component_v_expand_transition, null, {
                        default: _withCtx(() => [
                          showManual.value ? (_openBlock(), _createElementBlock("div", _hoisted_47, [
                            _createVNode(_component_v_textarea, {
                              modelValue: manualToken.value,
                              "onUpdate:modelValue": _cache[16] || (_cache[16] = ($event) => manualToken.value = $event),
                              rows: 3,
                              density: "compact",
                              "hide-details": "",
                              placeholder: "userId=…; serviceToken=…; 把浏览器里 i.api.xiaomi.com 登录后的整串 cookie 粘进来"
                            }, null, 8, ["modelValue"]),
                            _createElementVNode("div", _hoisted_48, [
                              _createVNode(_component_v_btn, {
                                size: "small",
                                variant: "tonal",
                                loading: busy.value === "校验票据",
                                onClick: submitManual
                              }, {
                                default: _withCtx(() => [..._cache[46] || (_cache[46] = [
                                  _createTextVNode("校验并使用", -1)
                                ])]),
                                _: 1
                              }, 8, ["loading"]),
                              _createVNode(_component_v_btn, {
                                size: "small",
                                variant: "text",
                                onClick: _cache[17] || (_cache[17] = ($event) => showManual.value = false)
                              }, {
                                default: _withCtx(() => [..._cache[47] || (_cache[47] = [
                                  _createTextVNode("取消", -1)
                                ])]),
                                _: 1
                              })
                            ])
                          ])) : _createCommentVNode("", true)
                        ]),
                        _: 1
                      }),
                      _createElementVNode("div", _hoisted_49, [
                        _cache[49] || (_cache[49] = _createTextVNode(" 账密登不上（小米风控、二次校验过不去）时可以在高级里直接粘贴 cookie 票据； 票据只存在本机插件数据里，配置页不回显明文。 ", -1)),
                        _createVNode(_component_v_btn, {
                          size: "x-small",
                          variant: "text",
                          class: "text-none ml-1",
                          "prepend-icon": showManual.value ? "mdi-chevron-up" : "mdi-chevron-down",
                          onClick: _cache[18] || (_cache[18] = ($event) => showManual.value = !showManual.value)
                        }, {
                          default: _withCtx(() => [..._cache[48] || (_cache[48] = [
                            _createTextVNode("高级", -1)
                          ])]),
                          _: 1
                        }, 8, ["prepend-icon"])
                      ])
                    ], 64))
                  ]),
                  _: 1
                }),
                _createElementVNode("div", _hoisted_50, [
                  _cache[51] || (_cache[51] = _createTextVNode(" 音箱设备 ", -1)),
                  _createElementVNode("span", _hoisted_51, _toDisplayString(devices.value.length) + " 台", 1)
                ]),
                !devices.value.length ? (_openBlock(), _createBlock(_component_v_alert, {
                  key: 0,
                  type: "warning",
                  variant: "tonal",
                  density: "compact"
                }, {
                  default: _withCtx(() => [
                    _createTextVNode(_toDisplayString(status.value.logged_in ? "设备列表是空的：可能票据已失效，点上面的「刷新设备」重试。" : "登录小米账号后才能看到音箱。"), 1)
                  ]),
                  _: 1
                })) : _createCommentVNode("", true),
                (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(devices.value, (device) => {
                  return _openBlock(), _createBlock(_component_v_card, {
                    key: device.device_id,
                    variant: "outlined",
                    class: "pa-3 mb-2"
                  }, {
                    default: _withCtx(() => [
                      _createElementVNode("div", _hoisted_52, [
                        _createVNode(_component_v_icon, {
                          icon: device.presence === "online" ? "mdi-speaker" : "mdi-speaker-off",
                          color: device.presence === "online" ? "success" : "grey"
                        }, null, 8, ["icon", "color"]),
                        _createElementVNode("div", _hoisted_53, [
                          _createElementVNode("div", _hoisted_54, [
                            _createTextVNode(_toDisplayString(device.name || device.alias || device.device_id) + " ", 1),
                            device.is_default ? (_openBlock(), _createBlock(_component_v_chip, {
                              key: 0,
                              size: "x-small",
                              variant: "flat",
                              color: "primary",
                              class: "ml-1"
                            }, {
                              default: _withCtx(() => [..._cache[52] || (_cache[52] = [
                                _createTextVNode("默认", -1)
                              ])]),
                              _: 1
                            })) : _createCommentVNode("", true),
                            device.alias && device.name ? (_openBlock(), _createBlock(_component_v_chip, {
                              key: 1,
                              size: "x-small",
                              variant: "tonal",
                              class: "ml-1"
                            }, {
                              default: _withCtx(() => [
                                _createTextVNode(_toDisplayString(device.alias), 1)
                              ]),
                              _: 2
                            }, 1024)) : _createCommentVNode("", true)
                          ]),
                          _createElementVNode("div", _hoisted_55, [
                            _createTextVNode(_toDisplayString(device.model || device.hardware || "未知型号") + "｜" + _toDisplayString(device.presence === "online" ? "在线" : "离线") + " ", 1),
                            device.needs_music_api ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                              _createTextVNode("｜该型号需走 play_music，直链可能被当成电台播放")
                            ], 64)) : _createCommentVNode("", true)
                          ])
                        ]),
                        _createVNode(_component_v_btn, {
                          size: "x-small",
                          variant: "text",
                          color: "primary",
                          disabled: device.is_default,
                          loading: busy.value === "设为默认",
                          onClick: ($event) => selectDevice(device)
                        }, {
                          default: _withCtx(() => [..._cache[53] || (_cache[53] = [
                            _createTextVNode("设为默认", -1)
                          ])]),
                          _: 1
                        }, 8, ["disabled", "loading", "onClick"]),
                        _createVNode(_component_v_btn, {
                          size: "x-small",
                          variant: "text",
                          disabled: !cfg.value.default_device_id,
                          loading: busy.value === "取消默认",
                          onClick: clearDefault
                        }, {
                          default: _withCtx(() => [..._cache[54] || (_cache[54] = [
                            _createTextVNode("取消默认", -1)
                          ])]),
                          _: 1
                        }, 8, ["disabled", "loading"])
                      ])
                    ]),
                    _: 2
                  }, 1024);
                }), 128)),
                _cache[55] || (_cache[55] = _createElementVNode("div", { class: "text-caption text-grey" }, " 没设默认音箱时，操作落在第一台在线设备上；都不在线就落在列表第一台。 设备列表由插件缓存 5 分钟，改动账号或新增音箱后点「刷新设备」。 ", -1))
              ], 512), [
                [_vShow, pane.value === "account"]
              ]),
              pane.value === "config" ? (_openBlock(), _createElementBlock("div", _hoisted_56, [
                _createVNode(ConfigForm, {
                  api: props.api,
                  "plugin-id": props.pluginId,
                  "source-plugin-id": props.sourcePluginId,
                  onSaved: _cache[19] || (_cache[19] = ($event) => loadStatus())
                }, null, 8, ["api", "plugin-id", "source-plugin-id"])
              ])) : _createCommentVNode("", true)
            ])
          ]),
          _createVNode(_component_v_snackbar, {
            modelValue: snackbar.value.show,
            "onUpdate:modelValue": _cache[20] || (_cache[20] = ($event) => snackbar.value.show = $event),
            color: snackbar.value.color,
            timeout: "3500",
            location: "bottom"
          }, {
            default: _withCtx(() => [
              _createTextVNode(_toDisplayString(snackbar.value.text), 1)
            ]),
            _: 1
          }, 8, ["modelValue", "color"])
        ]),
        _: 1
      });
    };
  }
});

const AppPage = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-ff99681c"]]);

export { AppPage as default };
