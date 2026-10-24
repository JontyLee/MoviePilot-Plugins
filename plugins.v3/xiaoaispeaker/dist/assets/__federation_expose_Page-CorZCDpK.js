import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { f as fmtSeconds, b as fmtSize, q as query } from './types-DlEniWcp.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,createCommentVNode:_createCommentVNode,normalizeClass:_normalizeClass,withCtx:_withCtx,createBlock:_createBlock,renderList:_renderList,unref:_unref,withKeys:_withKeys} = await importShared('vue');

const _hoisted_1 = { class: "xai-head" };
const _hoisted_2 = { class: "xai-mark" };
const _hoisted_3 = { class: "xai-head-text" };
const _hoisted_4 = { class: "xai-head-sub" };
const _hoisted_5 = { class: "xai-body" };
const _hoisted_6 = ["title"];
const _hoisted_7 = { class: "text-caption text-grey" };
const _hoisted_8 = {
  key: 0,
  class: "text-caption text-error"
};
const _hoisted_9 = {
  class: "d-flex align-center mt-3",
  style: { "gap": "10px", "flex-wrap": "wrap" }
};
const _hoisted_10 = { class: "xai-transport" };
const _hoisted_11 = { class: "xai-volume" };
const _hoisted_12 = { class: "xai-row" };
const _hoisted_13 = { class: "xai-title" };
const _hoisted_14 = { class: "text-caption text-grey" };
const _hoisted_15 = ["onClick"];
const _hoisted_16 = { class: "xai-queue-index" };
const _hoisted_17 = ["title"];
const _hoisted_18 = { class: "text-caption text-grey" };
const _hoisted_19 = {
  key: 1,
  class: "text-caption text-grey"
};
const _hoisted_20 = {
  key: 2,
  class: "text-caption text-grey"
};
const _hoisted_21 = {
  class: "text-body-2 text-truncate",
  style: { "flex": "1 1 auto" }
};
const {ref,computed,inject,onMounted,onUnmounted} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Page",
  props: {
    api: { type: Object, default: () => ({}) },
    nativeSubscribe: { type: Function, default: null },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    showSwitch: { type: Boolean, default: true }
  },
  emits: ["action", "switch", "close", "layout"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`);
    const toast = inject("moviepilot:toast", null);
    const status = ref(null);
    const busy = ref("");
    const snackbar = ref({ show: false, text: "", color: "info" });
    const volume = ref(50);
    const playing = computed(() => status.value?.playing);
    const queue = computed(() => status.value?.queue);
    const devices = computed(() => status.value?.devices || []);
    const cfg = computed(() => status.value?.config);
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
        if (playing.value?.volume !== null && playing.value?.volume !== void 0) {
          volume.value = playing.value.volume;
        }
      } catch (e) {
        if (showError) notify(`状态加载失败：${e?.message || e}`, "error");
      }
    }
    async function post(path, body, label) {
      busy.value = label;
      try {
        const res = await props.api.post(`${base.value}${path}`, body);
        if (res && res.ok === false) notify(res.message || res.error || `${label}失败`, "warning");
        else if (res?.message) notify(res.message);
        await loadStatus(false);
        emit("action");
      } catch (e) {
        notify(`${label}失败：${e?.message || e}`, "error");
      } finally {
        busy.value = "";
      }
    }
    function control(action) {
      return post("/control", { action }, "播放控制");
    }
    function setVolume() {
      return post("/volume", { volume: Math.round(volume.value) }, "设置音量");
    }
    function selectDevice(device) {
      return post("/device/select", { device_id: device.device_id }, "设为默认");
    }
    async function playKeyword() {
      const text = keyword.value.trim();
      if (!text) {
        notify("输入曲名关键词（库内文件名搜索，命中第一个直接播）", "warning");
        return;
      }
      try {
        const hits = await props.api.get(`${base.value}/search${query({ keyword: text })}`);
        if (!hits?.items?.length) {
          notify(`库里没找到「${text}」`, "warning");
          return;
        }
      } catch {
      }
      await post("/play", { keyword: text }, "播放");
      keyword.value = "";
    }
    const keyword = ref("");
    let timer = null;
    onMounted(async () => {
      emit("layout", { maxWidth: "46rem" });
      await loadStatus();
      timer = setInterval(() => {
        if (!document.hidden) loadStatus(false);
      }, 6e3);
    });
    onUnmounted(() => {
      if (timer) clearInterval(timer);
    });
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_slider = _resolveComponent("v-slider");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock(), _createBlock(_component_v_card, {
        flat: "",
        class: "xai-detail"
      }, {
        default: _withCtx(() => [
          _createElementVNode("div", _hoisted_1, [
            _createElementVNode("span", _hoisted_2, [
              _createVNode(_component_v_icon, {
                icon: playing.value?.status === 1 ? "mdi-speaker-wireless" : "mdi-speaker",
                size: "18"
              }, null, 8, ["icon"])
            ]),
            _createElementVNode("div", _hoisted_3, [
              _cache[9] || (_cache[9] = _createElementVNode("div", { class: "xai-head-title" }, "小爱音箱", -1)),
              _createElementVNode("div", _hoisted_4, [
                status.value?.logged_in ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                  _createTextVNode(_toDisplayString(cfg.value?.username || "已登录") + "｜" + _toDisplayString(devices.value.length) + " 台设备", 1)
                ], 64)) : (_openBlock(), _createElementBlock(_Fragment, { key: 1 }, [
                  _createTextVNode("未登录：到侧栏「账号与设备」登录小米账号")
                ], 64))
              ])
            ]),
            _createVNode(_component_v_spacer),
            _createElementVNode("span", {
              class: _normalizeClass(["xai-pill", playing.value?.status === 1 ? "xai-pill-ok" : "xai-pill-idle"])
            }, _toDisplayString(playing.value?.status_text || "未知"), 3),
            status.value?.token_expired ? (_openBlock(), _createBlock(_component_v_btn, {
              key: 0,
              size: "x-small",
              variant: "text",
              color: "error",
              class: "ml-1 text-none"
            }, {
              default: _withCtx(() => [..._cache[10] || (_cache[10] = [
                _createTextVNode("票据过期", -1)
              ])]),
              _: 1
            })) : _createCommentVNode("", true),
            props.showSwitch ? (_openBlock(), _createBlock(_component_v_btn, {
              key: 1,
              icon: "mdi-cog-outline",
              size: "x-small",
              variant: "text",
              onClick: _cache[0] || (_cache[0] = ($event) => emit("switch"))
            })) : _createCommentVNode("", true),
            _createVNode(_component_v_btn, {
              icon: "mdi-refresh",
              size: "x-small",
              variant: "text",
              loading: !!busy.value,
              onClick: _cache[1] || (_cache[1] = ($event) => loadStatus())
            }, null, 8, ["loading"])
          ]),
          _createElementVNode("div", _hoisted_5, [
            (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(status.value?.needs || [], (item) => {
              return _openBlock(), _createBlock(_component_v_alert, {
                key: item,
                type: "warning",
                variant: "tonal",
                density: "compact",
                class: "mb-2"
              }, {
                default: _withCtx(() => [
                  _createTextVNode(_toDisplayString(item), 1)
                ]),
                _: 2
              }, 1024);
            }), 128)),
            _createVNode(_component_v_card, {
              variant: "flat",
              class: "xai-hero pa-4"
            }, {
              default: _withCtx(() => [
                _createElementVNode("div", {
                  class: "xai-hero-title text-truncate",
                  title: playing.value?.title || playing.value?.path
                }, _toDisplayString(playing.value?.title || playing.value?.path || playing.value?.url || "当前没有在播放"), 9, _hoisted_6),
                _createElementVNode("div", _hoisted_7, [
                  _createTextVNode(_toDisplayString(playing.value?.device_name || "无设备") + "｜" + _toDisplayString(playing.value?.status_text || "未知") + " ", 1),
                  playing.value?.volume !== null && playing.value?.volume !== void 0 ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                    _createTextVNode(" ｜音量 " + _toDisplayString(playing.value?.volume), 1)
                  ], 64)) : _createCommentVNode("", true),
                  playing.value?.position !== null && playing.value?.position !== void 0 ? (_openBlock(), _createElementBlock(_Fragment, { key: 1 }, [
                    _createTextVNode(" ｜" + _toDisplayString(_unref(fmtSeconds)(playing.value?.position)) + " / " + _toDisplayString(_unref(fmtSeconds)(playing.value?.duration)), 1)
                  ], 64)) : _createCommentVNode("", true)
                ]),
                playing.value?.error ? (_openBlock(), _createElementBlock("div", _hoisted_8, _toDisplayString(playing.value.error), 1)) : _createCommentVNode("", true),
                _createElementVNode("div", _hoisted_9, [
                  _createElementVNode("div", _hoisted_10, [
                    _createVNode(_component_v_btn, {
                      size: "x-small",
                      variant: "text",
                      icon: "mdi-skip-previous-circle-outline",
                      loading: busy.value === "播放控制",
                      onClick: _cache[2] || (_cache[2] = ($event) => control("prev"))
                    }, null, 8, ["loading"]),
                    _createVNode(_component_v_btn, {
                      class: "xai-transport-main",
                      color: "primary",
                      variant: "flat",
                      rounded: "circle",
                      icon: playing.value?.status === 1 ? "mdi-pause" : "mdi-play",
                      loading: busy.value === "播放控制",
                      onClick: _cache[3] || (_cache[3] = ($event) => control(playing.value?.status === 1 ? "pause" : "play"))
                    }, null, 8, ["icon", "loading"]),
                    _createVNode(_component_v_btn, {
                      size: "x-small",
                      variant: "text",
                      icon: "mdi-skip-next-circle-outline",
                      loading: busy.value === "播放控制",
                      onClick: _cache[4] || (_cache[4] = ($event) => control("next"))
                    }, null, 8, ["loading"]),
                    _createVNode(_component_v_btn, {
                      size: "x-small",
                      variant: "text",
                      color: "error",
                      icon: "mdi-stop-circle-outline",
                      loading: busy.value === "播放控制",
                      onClick: _cache[5] || (_cache[5] = ($event) => control("stop"))
                    }, null, 8, ["loading"])
                  ]),
                  _createElementVNode("div", _hoisted_11, [
                    _createVNode(_component_v_icon, {
                      icon: "mdi-volume-low",
                      size: "15",
                      class: "mr-1"
                    }),
                    _createVNode(_component_v_slider, {
                      modelValue: volume.value,
                      "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => volume.value = $event),
                      min: "0",
                      max: "100",
                      step: "1",
                      "hide-details": "",
                      density: "compact"
                    }, null, 8, ["modelValue"]),
                    _createVNode(_component_v_btn, {
                      size: "x-small",
                      variant: "text",
                      loading: busy.value === "设置音量",
                      onClick: setVolume
                    }, {
                      default: _withCtx(() => [..._cache[11] || (_cache[11] = [
                        _createTextVNode("设音量", -1)
                      ])]),
                      _: 1
                    }, 8, ["loading"])
                  ])
                ])
              ]),
              _: 1
            }),
            _createElementVNode("div", _hoisted_12, [
              _createVNode(_component_v_text_field, {
                modelValue: keyword.value,
                "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => keyword.value = $event),
                density: "compact",
                "hide-details": "",
                clearable: "",
                "prepend-inner-icon": "mdi-magnify",
                placeholder: "按曲名播库内第一首",
                style: { "min-width": "200px", "flex": "1 1 200px" },
                onKeyup: _withKeys(playKeyword, ["enter"])
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_btn, {
                size: "small",
                variant: "tonal",
                loading: busy.value === "播放",
                onClick: playKeyword
              }, {
                default: _withCtx(() => [..._cache[12] || (_cache[12] = [
                  _createTextVNode("播放", -1)
                ])]),
                _: 1
              }, 8, ["loading"])
            ]),
            _createElementVNode("div", _hoisted_13, [
              _cache[13] || (_cache[13] = _createTextVNode(" 队列 ", -1)),
              _createElementVNode("span", _hoisted_14, _toDisplayString(queue.value?.total || 0) + " 首", 1),
              _createVNode(_component_v_chip, {
                size: "x-small",
                variant: "tonal",
                color: queue.value?.auto_next ? "success" : "grey"
              }, {
                default: _withCtx(() => [
                  _createTextVNode(_toDisplayString(queue.value?.auto_next ? "连播开" : "连播关"), 1)
                ]),
                _: 1
              }, 8, ["color"])
            ]),
            queue.value?.total ? (_openBlock(), _createBlock(_component_v_card, {
              key: 0,
              variant: "outlined",
              class: "xai-queue"
            }, {
              default: _withCtx(() => [
                (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(queue.value?.items || [], (item, i) => {
                  return _openBlock(), _createElementBlock("div", {
                    key: item.path,
                    class: _normalizeClass(["xai-queue-row", { "xai-queue-current": i === queue.value?.index }]),
                    onClick: ($event) => control(String(i))
                  }, [
                    _createElementVNode("span", _hoisted_16, _toDisplayString(i + 1), 1),
                    _createElementVNode("span", {
                      class: "xai-queue-name",
                      title: item.path
                    }, _toDisplayString(item.title || item.name), 9, _hoisted_17),
                    _createElementVNode("span", _hoisted_18, _toDisplayString(_unref(fmtSize)(item.size)), 1)
                  ], 10, _hoisted_15);
                }), 128))
              ]),
              _: 1
            })) : (_openBlock(), _createElementBlock("div", _hoisted_19, "队列为空（侧栏页可以从音乐库目录或搜索结果建队列）。")),
            _cache[16] || (_cache[16] = _createElementVNode("div", { class: "xai-title" }, "设备", -1)),
            !devices.value.length ? (_openBlock(), _createElementBlock("div", _hoisted_20, _toDisplayString(status.value?.logged_in ? "没有取到设备，可在侧栏页刷新设备列表。" : "登录后才会列出音箱。"), 1)) : _createCommentVNode("", true),
            (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(devices.value, (device) => {
              return _openBlock(), _createElementBlock("div", {
                key: device.device_id,
                class: "xai-row"
              }, [
                _createVNode(_component_v_icon, {
                  icon: device.presence === "online" ? "mdi-speaker" : "mdi-speaker-off",
                  size: "18",
                  color: device.presence === "online" ? "success" : "grey"
                }, null, 8, ["icon", "color"]),
                _createElementVNode("span", _hoisted_21, _toDisplayString(device.name || device.alias || device.device_id), 1),
                device.is_default ? (_openBlock(), _createBlock(_component_v_chip, {
                  key: 0,
                  size: "x-small",
                  variant: "flat",
                  color: "primary"
                }, {
                  default: _withCtx(() => [..._cache[14] || (_cache[14] = [
                    _createTextVNode("默认", -1)
                  ])]),
                  _: 1
                })) : _createCommentVNode("", true),
                _createVNode(_component_v_chip, {
                  size: "x-small",
                  variant: "tonal",
                  color: device.presence === "online" ? "success" : "grey"
                }, {
                  default: _withCtx(() => [
                    _createTextVNode(_toDisplayString(device.presence === "online" ? "在线" : "离线"), 1)
                  ]),
                  _: 2
                }, 1032, ["color"]),
                _createVNode(_component_v_btn, {
                  size: "x-small",
                  variant: "text",
                  disabled: device.is_default,
                  loading: busy.value === "设为默认",
                  onClick: ($event) => selectDevice(device)
                }, {
                  default: _withCtx(() => [..._cache[15] || (_cache[15] = [
                    _createTextVNode("设为默认", -1)
                  ])]),
                  _: 1
                }, 8, ["disabled", "loading", "onClick"])
              ]);
            }), 128)),
            _cache[17] || (_cache[17] = _createElementVNode("div", { class: "text-caption text-grey mt-3" }, " 库内浏览、整目录入队与播报在侧栏「小爱音箱」页面；外部直链推送同样在那里。 ", -1))
          ]),
          _createVNode(_component_v_snackbar, {
            modelValue: snackbar.value.show,
            "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => snackbar.value.show = $event),
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

const Page = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-f18a4813"]]);

export { Page as default };
