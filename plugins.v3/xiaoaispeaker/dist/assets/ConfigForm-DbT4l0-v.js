import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,resolveComponent:_resolveComponent,withCtx:_withCtx,createVNode:_createVNode,createElementVNode:_createElementVNode,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,createCommentVNode:_createCommentVNode,createBlock:_createBlock} = await importShared('vue');

const _hoisted_1 = { class: "xcf" };
const _hoisted_2 = { class: "xcf-top" };
const _hoisted_3 = { class: "xcf-body" };
const _hoisted_4 = { class: "xcf-group" };
const _hoisted_5 = { class: "xcf-group" };
const _hoisted_6 = { class: "text-caption text-grey mt-1" };
const _hoisted_7 = {
  class: "d-flex mt-3",
  style: { "gap": "16px", "flex-wrap": "wrap" }
};
const _hoisted_8 = { class: "xcf-group" };
const _hoisted_9 = {
  key: 0,
  class: "text-caption text-grey mt-1"
};
const _hoisted_10 = { class: "text-caption text-grey mt-1" };
const _hoisted_11 = { class: "xcf-group" };
const _hoisted_12 = {
  class: "d-flex",
  style: { "gap": "16px", "flex-wrap": "wrap" }
};
const _hoisted_13 = { class: "text-caption text-grey mt-1" };
const _hoisted_14 = { class: "xcf-group" };
const _hoisted_15 = {
  class: "d-flex",
  style: { "gap": "16px", "flex-wrap": "wrap" }
};
const _hoisted_16 = { class: "xcf-group" };
const _hoisted_17 = { class: "text-caption text-grey mt-1" };
const _hoisted_18 = { class: "xcf-actions" };
const {ref,computed,inject,onMounted} = await importShared('vue');

const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "ConfigForm",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    // 侧栏全页没有「切换到配置页」这回事（配置就在这一页里），宿主也不接 AppPage 的 switch
    canSwitch: { type: Boolean, default: false }
  },
  emits: ["switch", "saved"],
  setup(__props, { expose: __expose, emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`);
    const toast = inject("moviepilot:toast", null);
    const snackbar = ref({ show: false, text: "", color: "info" });
    const saving = ref(false);
    const status = ref(null);
    const loaded = ref(false);
    const form = ref({
      username: "",
      password: "",
      default_device_id: "",
      music_dirs: "",
      base_url: "",
      play_mode: "auto",
      audio_exts: "",
      audio_id: "",
      stream_ttl: 3600,
      default_volume: 0,
      auto_next: true,
      keep_light: false,
      tts_announce: false,
      voice_enabled: false,
      voice_devices: "",
      voice_poll: 2,
      voice_interrupt: true,
      voice_hint_tts: false,
      voice_hint_text: "正在搜索，请稍候",
      voice_reply: true,
      voice_memory: true,
      ai_enabled: false,
      ai_chat: false,
      ai_api_url: "",
      ai_api_key: "",
      ai_model: "",
      ai_timeout: 6,
      library_limit: 1e4,
      library_refresh: 60
    });
    const playModeItems = [
      { title: "自动（按型号白名单选接口）", value: "auto" },
      { title: "player_play_url（直链电台式）", value: "url" },
      { title: "player_play_music（走小米曲库）", value: "music" }
    ];
    const cfg = computed(() => status.value?.config);
    const devices = computed(() => status.value?.devices || []);
    const deviceItems = computed(() => [
      { title: "留空（第一台在线设备）", value: "" },
      ...devices.value.map((d) => ({
        title: `${d.name || d.alias || d.device_id}${d.presence === "online" ? "" : "（离线）"}`,
        value: d.device_id
      }))
    ]);
    const enabledText = computed(() => {
      if (!loaded.value) return "读取中";
      return cfg.value?.enabled ? "已启用" : "已停用（在插件管理页打开）";
    });
    const voiceDeviceList = computed({
      get: () => String(form.value.voice_devices || "").split(/[,;\s]+/).map((item) => item.trim()).filter(Boolean),
      set: (value) => {
        form.value.voice_devices = value.join(",");
      }
    });
    const voiceDeviceItems = computed(() => devices.value.map((d) => ({
      title: `${d.name || d.alias || d.device_id}${d.presence === "online" ? "" : "（离线）"}`,
      value: d.device_id
    })));
    function notify(msg, color = "info") {
      if (toast) {
        const fn = toast[color] || toast.info;
        fn.call(toast, msg);
        return;
      }
      snackbar.value = { show: true, text: msg, color };
    }
    function fillFromStatus(data) {
      status.value = data;
      const c = data?.config || {};
      form.value = {
        ...form.value,
        username: c.username || "",
        // 密码永不回显：留空即「不改动」，由服务端的 if password 判掉
        password: "",
        default_device_id: c.default_device_id || "",
        music_dirs: c.music_dirs || "",
        base_url: c.base_url || "",
        play_mode: c.play_mode || "auto",
        audio_exts: c.audio_exts || "",
        audio_id: c.audio_id || "",
        stream_ttl: c.stream_ttl ?? 3600,
        default_volume: c.default_volume ?? 0,
        auto_next: c.auto_next !== false,
        keep_light: !!c.keep_light,
        tts_announce: !!c.tts_announce,
        voice_enabled: !!c.voice_enabled,
        voice_devices: c.voice_devices || "",
        voice_poll: c.voice_poll ?? 2,
        voice_interrupt: c.voice_interrupt !== false,
        voice_hint_tts: !!c.voice_hint_tts,
        voice_hint_text: c.voice_hint_text ?? "正在搜索，请稍候",
        voice_reply: c.voice_reply !== false,
        voice_memory: c.voice_memory !== false,
        ai_enabled: !!c.ai_enabled,
        ai_chat: !!c.ai_chat,
        ai_api_url: c.ai_api_url || "",
        ai_api_key: "",
        ai_model: c.ai_model || "",
        ai_timeout: c.ai_timeout ?? 6,
        library_limit: c.library_limit ?? 1e4,
        library_refresh: c.library_refresh ?? 60
      };
    }
    async function loadConfig() {
      try {
        fillFromStatus(await props.api.get(`${base.value}/status`));
        loaded.value = true;
      } catch (e) {
        notify(`配置读取失败：${e?.message || e}`, "error");
      }
    }
    async function save() {
      saving.value = true;
      try {
        const body = { ...form.value };
        if (!body.password) delete body.password;
        if (!body.ai_api_key) delete body.ai_api_key;
        fillFromStatus(await props.api.post(`${base.value}/config`, body));
        notify("配置已保存", "success");
        emit("saved");
      } catch (e) {
        notify(`保存失败：${e?.message || e}`, "error");
      } finally {
        saving.value = false;
      }
    }
    onMounted(loadConfig);
    __expose({ loadConfig, save });
    return (_ctx, _cache) => {
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_select = _resolveComponent("v-select");
      const _component_v_switch = _resolveComponent("v-switch");
      const _component_v_textarea = _resolveComponent("v-textarea");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock(), _createElementBlock("div", _hoisted_1, [
        _createElementVNode("div", _hoisted_2, [
          _createVNode(_component_v_chip, {
            size: "small",
            variant: "tonal",
            color: cfg.value?.enabled ? "success" : "grey"
          }, {
            default: _withCtx(() => [
              _createTextVNode(_toDisplayString(enabledText.value), 1)
            ]),
            _: 1
          }, 8, ["color"]),
          _cache[32] || (_cache[32] = _createElementVNode("span", { class: "text-caption text-grey" }, " 侧栏入口与启用开关在「插件管理 → 小爱音箱」里改，这里改了不会让侧栏立刻出现或消失 ", -1)),
          _createVNode(_component_v_spacer),
          _createVNode(_component_v_btn, {
            size: "small",
            variant: "text",
            class: "text-none",
            "prepend-icon": "mdi-refresh",
            onClick: loadConfig
          }, {
            default: _withCtx(() => [..._cache[31] || (_cache[31] = [
              _createTextVNode("重新读取", -1)
            ])]),
            _: 1
          })
        ]),
        _createElementVNode("div", _hoisted_3, [
          _createElementVNode("div", _hoisted_4, [
            _cache[34] || (_cache[34] = _createElementVNode("div", { class: "xcf-group-title" }, "小米账号", -1)),
            _createVNode(_component_v_alert, {
              type: "info",
              variant: "tonal",
              density: "compact",
              class: "mb-3"
            }, {
              default: _withCtx(() => [..._cache[33] || (_cache[33] = [
                _createTextVNode(" 这里只保存账密与设备偏好；", -1),
                _createElementVNode("b", null, "登录本身（含图形验证码 / 二次校验）在「账号与设备」那一栏做", -1),
                _createTextVNode("， 改完账号密码后重新登录才会生效。 ", -1)
              ])]),
              _: 1
            }),
            _createVNode(_component_v_row, { dense: "" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "6"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.username,
                      "onUpdate:modelValue": _cache[0] || (_cache[0] = ($event) => form.value.username = $event),
                      label: "小米账号（手机号 / 邮箱 / 账号ID）",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "6"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.password,
                      "onUpdate:modelValue": _cache[1] || (_cache[1] = ($event) => form.value.password = $event),
                      type: form.value.password ? "text" : "password",
                      label: cfg.value?.password_set ? "密码（留空保留已存的）" : "密码",
                      "hide-details": ""
                    }, null, 8, ["modelValue", "type", "label"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _cache[35] || (_cache[35] = _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 票据只存在本机插件数据里，任何接口都不回显明文。账密登不上时可以在「账号与设备」里粘贴 cookie 票据。 ", -1))
          ]),
          _createElementVNode("div", _hoisted_5, [
            _cache[36] || (_cache[36] = _createElementVNode("div", { class: "xcf-group-title" }, "音箱", -1)),
            _createVNode(_component_v_select, {
              modelValue: form.value.default_device_id,
              "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => form.value.default_device_id = $event),
              items: deviceItems.value,
              label: "默认音箱",
              "hide-details": "",
              placeholder: "留空则用第一台在线设备"
            }, null, 8, ["modelValue", "items"]),
            _createElementVNode("div", _hoisted_6, [
              _createTextVNode(_toDisplayString(devices.value.length ? `已取到 ${devices.value.length} 台设备` : "还没取到设备列表：登录后这里才会填充候选") + " ", 1),
              cfg.value?.default_device_id && !devices.value.some((d) => d.device_id === cfg.value?.default_device_id) ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                _createTextVNode(" ｜当前配置的默认音箱不在列表里，操作会回落到在线设备 ")
              ], 64)) : _createCommentVNode("", true)
            ]),
            _createElementVNode("div", _hoisted_7, [
              _createVNode(_component_v_switch, {
                modelValue: form.value.auto_next,
                "onUpdate:modelValue": _cache[3] || (_cache[3] = ($event) => form.value.auto_next = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "队列自动连播"
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_switch, {
                modelValue: form.value.keep_light,
                "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => form.value.keep_light = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "播放时常亮"
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_switch, {
                modelValue: form.value.tts_announce,
                "onUpdate:modelValue": _cache[5] || (_cache[5] = ($event) => form.value.tts_announce = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "连播时先播报下一曲"
              }, null, 8, ["modelValue"])
            ])
          ]),
          _createElementVNode("div", _hoisted_8, [
            _cache[37] || (_cache[37] = _createElementVNode("div", { class: "xcf-group-title" }, "音乐库与直链", -1)),
            _createVNode(_component_v_textarea, {
              modelValue: form.value.music_dirs,
              "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => form.value.music_dirs = $event),
              rows: 3,
              "hide-details": "",
              label: "音乐库路径（一行一个目录，只有这些目录里的文件可被列出与播放）",
              placeholder: "/media/music\n/nas/music"
            }, null, 8, ["modelValue"]),
            cfg.value && cfg.value.dir_count ? (_openBlock(), _createElementBlock("div", _hoisted_9, [
              _createTextVNode(" 当前生效 " + _toDisplayString(cfg.value.dir_count) + " 个目录 ", 1),
              cfg.value.missing_dirs.length ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                _createTextVNode("｜读不到：" + _toDisplayString(cfg.value.missing_dirs.join("、")), 1)
              ], 64)) : _createCommentVNode("", true)
            ])) : _createCommentVNode("", true),
            _createVNode(_component_v_row, {
              dense: "",
              class: "mt-2"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "7"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.base_url,
                      "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => form.value.base_url = $event),
                      "hide-details": "",
                      label: "外部访问地址（音箱能访问到的 MP 地址）",
                      placeholder: "http://192.168.1.10:3000，留空用主程序设置"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "5"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_select, {
                      modelValue: form.value.play_mode,
                      "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => form.value.play_mode = $event),
                      items: playModeItems,
                      label: "播放接口",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createElementVNode("div", _hoisted_10, [
              _createTextVNode(" 实际使用：" + _toDisplayString(cfg.value?.base_url_effective || "拿不到可用地址（音箱必然拉不到流）") + " ", 1),
              cfg.value?.base_url_source ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                _createTextVNode("｜来自 " + _toDisplayString(cfg.value.base_url_source), 1)
              ], 64)) : _createCommentVNode("", true)
            ]),
            _createVNode(_component_v_row, {
              dense: "",
              class: "mt-2"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.default_volume,
                      "onUpdate:modelValue": _cache[9] || (_cache[9] = ($event) => form.value.default_volume = $event),
                      modelModifiers: { number: true },
                      type: "number",
                      min: "0",
                      max: "100",
                      "hide-details": "",
                      label: "播放前设置音量（0=不改）"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.stream_ttl,
                      "onUpdate:modelValue": _cache[10] || (_cache[10] = ($event) => form.value.stream_ttl = $event),
                      modelModifiers: { number: true },
                      type: "number",
                      min: "300",
                      max: "259200",
                      "hide-details": "",
                      label: "直链有效期（秒）"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.audio_exts,
                      "onUpdate:modelValue": _cache[11] || (_cache[11] = ($event) => form.value.audio_exts = $event),
                      "hide-details": "",
                      label: "可播放扩展名（留空用默认）",
                      placeholder: "mp3 m4a aac flac wav ogg opus wma"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createVNode(_component_v_row, {
              dense: "",
              class: "mt-2"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "6"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.audio_id,
                      "onUpdate:modelValue": _cache[12] || (_cache[12] = ($event) => form.value.audio_id = $event),
                      "hide-details": "",
                      label: "play_music audioID（留空用默认）"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _createElementVNode("div", _hoisted_11, [
            _cache[39] || (_cache[39] = _createElementVNode("div", { class: "xcf-group-title" }, "语音控制", -1)),
            _createVNode(_component_v_alert, {
              type: "info",
              variant: "tonal",
              density: "compact",
              class: "mb-3"
            }, {
              default: _withCtx(() => [..._cache[38] || (_cache[38] = [
                _createTextVNode(" 打开后插件周期性读这几台音箱在小米云端的", -1),
                _createElementVNode("b", null, "对话记录", -1),
                _createTextVNode("，命中口令表的话才接手播放。 每读一次就是一个请求，只对真的要听的那几台开。 ", -1)
              ])]),
              _: 1
            }),
            _createElementVNode("div", _hoisted_12, [
              _createVNode(_component_v_switch, {
                modelValue: form.value.voice_enabled,
                "onUpdate:modelValue": _cache[13] || (_cache[13] = ($event) => form.value.voice_enabled = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "开启语音监听"
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_switch, {
                modelValue: form.value.voice_interrupt,
                "onUpdate:modelValue": _cache[14] || (_cache[14] = ($event) => form.value.voice_interrupt = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "命中前先打断音箱"
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_switch, {
                modelValue: form.value.voice_reply,
                "onUpdate:modelValue": _cache[15] || (_cache[15] = ($event) => form.value.voice_reply = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "播报执行结果"
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_switch, {
                modelValue: form.value.voice_hint_tts,
                "onUpdate:modelValue": _cache[16] || (_cache[16] = ($event) => form.value.voice_hint_tts = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "搜索期间播提示语"
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_switch, {
                modelValue: form.value.voice_memory,
                "onUpdate:modelValue": _cache[17] || (_cache[17] = ($event) => form.value.voice_memory = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "记住刚才那首（用于「再播一遍」）"
              }, null, 8, ["modelValue"])
            ]),
            _createVNode(_component_v_row, {
              dense: "",
              class: "mt-3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "7"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_select, {
                      modelValue: voiceDeviceList.value,
                      "onUpdate:modelValue": _cache[18] || (_cache[18] = ($event) => voiceDeviceList.value = $event),
                      items: voiceDeviceItems.value,
                      label: "监听哪些音箱",
                      multiple: "",
                      chips: "",
                      dense: "",
                      closable: "",
                      "hide-details": "",
                      placeholder: "留空 = 只监听默认音箱"
                    }, null, 8, ["modelValue", "items"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "5"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.voice_poll,
                      "onUpdate:modelValue": _cache[19] || (_cache[19] = ($event) => form.value.voice_poll = $event),
                      modelModifiers: { number: true },
                      type: "number",
                      min: "2",
                      max: "30",
                      "hide-details": "",
                      label: "轮询间隔（秒，2-30）"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createVNode(_component_v_text_field, {
              modelValue: form.value.voice_hint_text,
              "onUpdate:modelValue": _cache[20] || (_cache[20] = ($event) => form.value.voice_hint_text = $event),
              class: "mt-3",
              "hide-details": "",
              density: "compact",
              label: "搜索期间的提示语（留空用默认）",
              placeholder: "正在搜索，请稍候"
            }, null, 8, ["modelValue"]),
            _createElementVNode("div", _hoisted_13, _toDisplayString(devices.value.length ? `已取到 ${devices.value.length} 台设备，可直接勾选；留空时只跟着默认音箱走` : "设备候选要登录后才有；也可以先手填 device_id（逗号分隔）") + " ｜口令表逐类的开关在「语音控制」那一栏里改，不在这里。 ", 1)
          ]),
          _createElementVNode("div", _hoisted_14, [
            _cache[40] || (_cache[40] = _createElementVNode("div", { class: "xcf-group-title" }, "AI 兜底与对话", -1)),
            _createElementVNode("div", _hoisted_15, [
              _createVNode(_component_v_switch, {
                modelValue: form.value.ai_enabled,
                "onUpdate:modelValue": _cache[21] || (_cache[21] = ($event) => form.value.ai_enabled = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "口令表没命中时交给 AI 判定"
              }, null, 8, ["modelValue"]),
              _createVNode(_component_v_switch, {
                modelValue: form.value.ai_chat,
                "onUpdate:modelValue": _cache[22] || (_cache[22] = ($event) => form.value.ai_chat = $event),
                color: "success",
                "hide-details": "",
                inset: "",
                label: "没命中的话当成聊天（说完 2 分钟内可追问）"
              }, null, 8, ["modelValue"])
            ]),
            _createVNode(_component_v_row, {
              dense: "",
              class: "mt-3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "6"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.ai_api_url,
                      "onUpdate:modelValue": _cache[23] || (_cache[23] = ($event) => form.value.ai_api_url = $event),
                      "hide-details": "",
                      label: "OpenAI 兼容接口地址（不含 /chat/completions）"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "6"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.ai_api_key,
                      "onUpdate:modelValue": _cache[24] || (_cache[24] = ($event) => form.value.ai_api_key = $event),
                      type: form.value.ai_api_key ? "text" : "password",
                      "hide-details": "",
                      label: cfg.value?.ai_key_set ? "API Key（留空保留已存的）" : "API Key"
                    }, null, 8, ["modelValue", "type", "label"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createVNode(_component_v_row, {
              dense: "",
              class: "mt-2"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "7"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.ai_model,
                      "onUpdate:modelValue": _cache[25] || (_cache[25] = ($event) => form.value.ai_model = $event),
                      "hide-details": "",
                      label: "模型名",
                      placeholder: cfg.value?.ai_model || "qwen-flash"
                    }, null, 8, ["modelValue", "placeholder"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "5"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.ai_timeout,
                      "onUpdate:modelValue": _cache[26] || (_cache[26] = ($event) => form.value.ai_timeout = $event),
                      modelModifiers: { number: true },
                      type: "number",
                      min: "2",
                      max: "30",
                      "hide-details": "",
                      label: "超时（秒，2-30）"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _cache[41] || (_cache[41] = _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " AI 只能给出口令表里已有的那几种动作，置信度低或动作未知都当没判出来 —— 它只是多一个入口， 权限不比口令表大。Key 只存本机配置，任何接口都不回显明文。 ", -1))
          ]),
          _createElementVNode("div", _hoisted_16, [
            _cache[42] || (_cache[42] = _createElementVNode("div", { class: "xcf-group-title" }, "曲库索引", -1)),
            _createVNode(_component_v_row, { dense: "" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "5"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.library_limit,
                      "onUpdate:modelValue": _cache[27] || (_cache[27] = ($event) => form.value.library_limit = $event),
                      modelModifiers: { number: true },
                      type: "number",
                      min: "500",
                      max: "200000",
                      "hide-details": "",
                      label: "最多入库多少首"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  md: "5"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_text_field, {
                      modelValue: form.value.library_refresh,
                      "onUpdate:modelValue": _cache[28] || (_cache[28] = ($event) => form.value.library_refresh = $event),
                      modelModifiers: { number: true },
                      type: "number",
                      min: "0",
                      max: "1440",
                      "hide-details": "",
                      label: "自动保鲜周期（分钟，0=只手动）"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createElementVNode("div", _hoisted_17, " 索引只为语音点歌服务（读标签与目录名，元数据缓存落在插件数据目录）。 当前 " + _toDisplayString(cfg.value?.library_limit ?? 1e4) + " 首上限、每 " + _toDisplayString(cfg.value?.library_refresh ?? 60) + " 分钟保鲜一次； 索引状态与手动重建在「音乐库」那一栏。 ", 1)
          ]),
          _cache[43] || (_cache[43] = _createElementVNode("div", { class: "xcf-group" }, [
            _createElementVNode("div", { class: "xcf-group-title" }, "两条最容易踩的坑"),
            _createElementVNode("div", { class: "text-caption text-grey" }, " 「外部访问地址」必须是音箱所在网段连得到的 IP 或域名，不能是 127.0.0.1，也不能是 MP 容器自己看的地址 —— 这一项填错是「点了没声音」最常见的原因。 "),
            _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 直链由插件用本机随机密钥签名并带有效期，只有落在上面这些库目录内的音频能被取到； 换密钥会让已经发出去的链接一起失效，这是有意的，不做兼容层。 ")
          ], -1))
        ]),
        _createElementVNode("div", _hoisted_18, [
          props.canSwitch ? (_openBlock(), _createBlock(_component_v_btn, {
            key: 0,
            variant: "text",
            class: "text-none",
            "prepend-icon": "mdi-speaker",
            onClick: _cache[29] || (_cache[29] = ($event) => emit("switch"))
          }, {
            default: _withCtx(() => [..._cache[44] || (_cache[44] = [
              _createTextVNode("查看播放台 ", -1)
            ])]),
            _: 1
          })) : _createCommentVNode("", true),
          _createVNode(_component_v_spacer),
          _createVNode(_component_v_btn, {
            color: "primary",
            variant: "flat",
            "prepend-icon": "mdi-content-save",
            class: "text-none",
            loading: saving.value,
            onClick: save
          }, {
            default: _withCtx(() => [..._cache[45] || (_cache[45] = [
              _createTextVNode("保存配置 ", -1)
            ])]),
            _: 1
          }, 8, ["loading"])
        ]),
        _createVNode(_component_v_snackbar, {
          modelValue: snackbar.value.show,
          "onUpdate:modelValue": _cache[30] || (_cache[30] = ($event) => snackbar.value.show = $event),
          color: snackbar.value.color,
          timeout: "3500",
          location: "bottom"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(snackbar.value.text), 1)
          ]),
          _: 1
        }, 8, ["modelValue", "color"])
      ]);
    };
  }
});

const ConfigForm = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-85e66795"]]);

export { ConfigForm as C };
