import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc, q as query, t as taskNeedsPlaylist, T as TASK_ACTIONS, W as WEEK_DAYS, M as MODE_ITEMS } from './types-CVxekbje.js';

const {defineComponent:_defineComponent$4} = await importShared('vue');

const {resolveComponent:_resolveComponent$4,createVNode:_createVNode$4,createTextVNode:_createTextVNode$4,createElementVNode:_createElementVNode$4,toDisplayString:_toDisplayString$4,Fragment:_Fragment$4,openBlock:_openBlock$4,createElementBlock:_createElementBlock$4,createCommentVNode:_createCommentVNode$4,withCtx:_withCtx$4} = await importShared('vue');

const _hoisted_1$4 = { class: "xcf" };
const _hoisted_2$4 = { class: "xcf-group" };
const _hoisted_3$4 = { class: "xcf-group-title" };
const _hoisted_4$4 = { class: "xcf-hint" };
const _hoisted_5$4 = { key: 0 };
const _hoisted_6$3 = { key: 1 };
const _hoisted_7$3 = { class: "xcf-group" };
const _hoisted_8$3 = { class: "xcf-group-title" };
const _hoisted_9$3 = { class: "xcf-hint" };
const _hoisted_10$3 = { class: "xcf-grid mt-3" };
const _hoisted_11$3 = { class: "xcf-group" };
const _hoisted_12$3 = { class: "xcf-group-title" };
const _hoisted_13$3 = { class: "xcf-toggle mt-3" };
const _hoisted_14$3 = { class: "xcf-group" };
const _hoisted_15$2 = { class: "xcf-group-title" };
const _hoisted_16$2 = { class: "xcf-grid" };
const _hoisted_17$2 = { class: "xcf-group" };
const _hoisted_18$1 = { class: "xcf-group-title" };
const _hoisted_19$1 = { class: "xcf-hint" };
const _hoisted_20$1 = { class: "xcf-group" };
const _hoisted_21$1 = { class: "xcf-group-title" };
const _hoisted_22$1 = { class: "xcf-toggle" };
const _hoisted_23$1 = { class: "xcf-toggle" };
const _hoisted_24$1 = { class: "xcf-toggle" };
const _hoisted_25$1 = { class: "xcf-group" };
const _hoisted_26$1 = { class: "xcf-group-title" };
const _hoisted_27$1 = { class: "xcf-toggle" };
const _hoisted_28$1 = { class: "xcf-grid mt-3" };
const _hoisted_29$1 = { class: "xcf-hint" };
const _hoisted_30$1 = { class: "xcf-group" };
const _hoisted_31$1 = { class: "xcf-group-title" };
const _hoisted_32$1 = { class: "xcf-toggle" };
const _hoisted_33$1 = { class: "xcf-toggle" };
const _hoisted_34$1 = { class: "xcf-toggle" };
const _hoisted_35$1 = { class: "xcf-toggle" };
const _hoisted_36$1 = { class: "xcf-group" };
const _hoisted_37$1 = { class: "xcf-group-title" };
const _hoisted_38$1 = { class: "xcf-toggle" };
const _hoisted_39$1 = { class: "xcf-toggle" };
const _hoisted_40 = { class: "text-body-2" };
const _hoisted_41 = { class: "text-caption" };
const {ref: ref$5,computed: computed$4,onMounted: onMounted$3} = await importShared('vue');

const _sfc_main$4 = /* @__PURE__ */ _defineComponent$4({
  __name: "ConfigForm",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    // 配置页把这张长表单拆成了三栏（曲库与直链 / 播放行为 / 语音与 AI），
    // 草稿与保存仍由本组件一处持有：切栏不丢未保存的改动。
    pane: { type: String, default: "library" },
    devices: { type: Array, default: () => [] }
  },
  emits: ["toast", "loaded", "saved"],
  setup(__props, { expose: __expose, emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const base = computed$4(() => `plugin/${props.pluginId || props.sourcePluginId}`);
    const saving = ref$5(false);
    const loaded = ref$5(false);
    const cfg = ref$5(null);
    const form = ref$5({
      enabled: false,
      show_sidebar_nav: true,
      music_dirs: "",
      base_url: "",
      play_mode: "auto",
      audio_exts: "",
      force_mp3: false,
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
      ai_timeout: 6,
      library_limit: 1e4,
      library_refresh: 60
    });
    const pristine = ref$5({ ...form.value });
    const dirty = computed$4(() => Object.keys(form.value).some(
      (key) => JSON.stringify(form.value[key]) !== JSON.stringify(pristine.value[key])
    ));
    const playModeItems = [
      { title: "自动（按型号白名单选接口）", value: "auto" },
      { title: "player_play_url（直链电台式）", value: "url" },
      { title: "player_play_music（走小米曲库）", value: "music" }
    ];
    const devices = computed$4(() => props.devices);
    const navText = computed$4(() => {
      if (!loaded.value) return "读取中";
      if (!form.value.enabled) return "未启用：侧栏与巡检都不会跑";
      return form.value.show_sidebar_nav ? "侧栏入口已出现" : "已启用，只是不收进侧栏";
    });
    const voiceDeviceList = computed$4({
      get: () => String(form.value.voice_devices || "").split(/[,;\s]+/).map((item) => item.trim()).filter(Boolean),
      set: (value) => {
        form.value.voice_devices = value.join(",");
      }
    });
    const voiceDeviceItems = computed$4(() => devices.value.map((d) => ({
      title: `${d.name || d.alias || d.device_id}${d.presence === "online" ? "" : "（离线）"}`,
      value: d.device_id
    })));
    function fillFromStatus(data) {
      cfg.value = data?.config || {};
      const c = cfg.value;
      form.value = {
        ...form.value,
        enabled: !!c.enabled,
        show_sidebar_nav: c.show_sidebar_nav !== false,
        // 账号、密码、默认音箱由「账号与音箱」那一栏管，这里不占位：/config 是「缺省即保留」，
        // 不带这三个键就等于不动它们
        music_dirs: c.music_dirs || "",
        base_url: c.base_url || "",
        play_mode: c.play_mode || "auto",
        audio_exts: c.audio_exts || "",
        force_mp3: !!c.force_mp3,
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
        ai_timeout: c.ai_timeout ?? 6,
        library_limit: c.library_limit ?? 1e4,
        library_refresh: c.library_refresh ?? 60
      };
      pristine.value = { ...form.value };
      loaded.value = true;
    }
    async function loadConfig() {
      try {
        fillFromStatus(await props.api.get(`${base.value}/status`));
        emit("loaded");
      } catch (e) {
        emit("toast", `配置读取失败：${e?.message || e}`, "error");
      }
    }
    async function save() {
      saving.value = true;
      try {
        fillFromStatus(await props.api.post(`${base.value}/config`, { ...form.value }));
        emit("toast", "配置已保存", "success");
        emit("saved");
      } catch (e) {
        emit("toast", `保存失败：${e?.message || e}`, "error");
      } finally {
        saving.value = false;
      }
    }
    onMounted$3(loadConfig);
    __expose({ form, dirty, saving, loaded, cfg, navText, loadConfig, save });
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent$4("v-icon");
      const _component_v_textarea = _resolveComponent$4("v-textarea");
      const _component_v_text_field = _resolveComponent$4("v-text-field");
      const _component_v_switch = _resolveComponent$4("v-switch");
      const _component_v_select = _resolveComponent$4("v-select");
      const _component_v_alert = _resolveComponent$4("v-alert");
      return _openBlock$4(), _createElementBlock$4("div", _hoisted_1$4, [
        props.pane === "library" ? (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 0 }, [
          _createElementVNode$4("div", _hoisted_2$4, [
            _createElementVNode$4("div", _hoisted_3$4, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-folder-music-outline",
                size: "16"
              }),
              _cache[24] || (_cache[24] = _createTextVNode$4(" 音乐库 ", -1))
            ]),
            _createVNode$4(_component_v_textarea, {
              modelValue: form.value.music_dirs,
              "onUpdate:modelValue": _cache[0] || (_cache[0] = ($event) => form.value.music_dirs = $event),
              rows: 3,
              "hide-details": "",
              variant: "outlined",
              label: "库路径（一行一个目录，只有这些目录里的文件可被列出与播放）",
              placeholder: "/media/music\n/nas/music"
            }, null, 8, ["modelValue"]),
            _createElementVNode$4("div", _hoisted_4$4, [
              cfg.value && cfg.value.dir_count ? (_openBlock$4(), _createElementBlock$4("span", _hoisted_5$4, [
                _createTextVNode$4(" 当前生效 " + _toDisplayString$4(cfg.value.dir_count) + " 个目录 ", 1),
                cfg.value.missing_dirs.length ? (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 0 }, [
                  _createTextVNode$4("｜读不到：" + _toDisplayString$4(cfg.value.missing_dirs.join("、")), 1)
                ], 64)) : _createCommentVNode$4("", true)
              ])) : (_openBlock$4(), _createElementBlock$4("span", _hoisted_6$3, "留空就用主程序的音乐目录设置"))
            ])
          ]),
          _createElementVNode$4("div", _hoisted_7$3, [
            _createElementVNode$4("div", _hoisted_8$3, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-link-variant",
                size: "16"
              }),
              _cache[25] || (_cache[25] = _createTextVNode$4(" 直链地址 ", -1))
            ]),
            _createVNode$4(_component_v_text_field, {
              modelValue: form.value.base_url,
              "onUpdate:modelValue": _cache[1] || (_cache[1] = ($event) => form.value.base_url = $event),
              "hide-details": "",
              variant: "outlined",
              label: "外部访问地址（音箱能访问到的 MP 地址）",
              placeholder: "http://192.168.1.10:3000，留空用主程序设置"
            }, null, 8, ["modelValue"]),
            _createElementVNode$4("div", _hoisted_9$3, [
              _cache[26] || (_cache[26] = _createTextVNode$4(" 实际使用：", -1)),
              _createElementVNode$4("b", null, _toDisplayString$4(cfg.value?.base_url_effective || "拿不到可用地址（音箱必然拉不到流）"), 1),
              cfg.value?.base_url_source ? (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 0 }, [
                _createTextVNode$4("｜来自 " + _toDisplayString$4(cfg.value.base_url_source), 1)
              ], 64)) : _createCommentVNode$4("", true)
            ]),
            _createElementVNode$4("div", _hoisted_10$3, [
              _createVNode$4(_component_v_text_field, {
                modelValue: form.value.stream_ttl,
                "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => form.value.stream_ttl = $event),
                modelModifiers: { number: true },
                type: "number",
                min: "300",
                max: "259200",
                "hide-details": "",
                variant: "outlined",
                label: "直链有效期（秒）"
              }, null, 8, ["modelValue"]),
              _createVNode$4(_component_v_text_field, {
                modelValue: form.value.audio_id,
                "onUpdate:modelValue": _cache[3] || (_cache[3] = ($event) => form.value.audio_id = $event),
                "hide-details": "",
                variant: "outlined",
                label: "play_music audioID（留空用默认）"
              }, null, 8, ["modelValue"])
            ]),
            _cache[27] || (_cache[27] = _createElementVNode$4("div", { class: "xcf-hint" }, [
              _createTextVNode$4(" 地址必须是"),
              _createElementVNode$4("b", null, "音箱所在网段连得到"),
              _createTextVNode$4("的 IP 或域名，不能是 127.0.0.1，也不能是 MP 容器自己看的地址 —— 这一项填错是「点了没声音」最常见的原因。直链由插件用签名密钥（落在插件数据目录 "),
              _createElementVNode$4("code", null, "state/secret.json"),
              _createTextVNode$4("）签发并带有效期，换密钥会让已经发出去的链接一起失效，这是有意的。 ")
            ], -1))
          ]),
          _createElementVNode$4("div", _hoisted_11$3, [
            _createElementVNode$4("div", _hoisted_12$3, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-file-document-multiple-outline",
                size: "16"
              }),
              _cache[28] || (_cache[28] = _createTextVNode$4(" 格式与解码 ", -1))
            ]),
            _createVNode$4(_component_v_text_field, {
              modelValue: form.value.audio_exts,
              "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => form.value.audio_exts = $event),
              "hide-details": "",
              variant: "outlined",
              label: "可播放扩展名（留空用默认）",
              placeholder: "mp3 m4a aac flac wav ogg opus wma"
            }, null, 8, ["modelValue"]),
            _createElementVNode$4("label", _hoisted_13$3, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.force_mp3,
                "onUpdate:modelValue": _cache[5] || (_cache[5] = ($event) => form.value.force_mp3 = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[29] || (_cache[29] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "非 mp3 现场转成 mp3"),
                _createElementVNode$4("em", null, "需要本机有 ffmpeg。开着时 /stream 边放边转，转出来的长度未知，所以这一条不支持拖动进度。")
              ], -1))
            ]),
            _cache[30] || (_cache[30] = _createElementVNode$4("div", { class: "xcf-hint" }, [
              _createTextVNode$4(" 小爱只稳当解码 mp3；m4a/flac 推过去常是「云端受理、音箱不出声」。 这一栏不响就先把「播放行为」里的播放接口切成 "),
              _createElementVNode$4("b", null, "player_play_music"),
              _createTextVNode$4("，那条免费的路不通再开转码。 ")
            ], -1))
          ]),
          _createElementVNode$4("div", _hoisted_14$3, [
            _createElementVNode$4("div", _hoisted_15$2, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-database-refresh-outline",
                size: "16"
              }),
              _cache[31] || (_cache[31] = _createTextVNode$4(" 曲库索引 ", -1))
            ]),
            _createElementVNode$4("div", _hoisted_16$2, [
              _createVNode$4(_component_v_text_field, {
                modelValue: form.value.library_limit,
                "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => form.value.library_limit = $event),
                modelModifiers: { number: true },
                type: "number",
                min: "500",
                max: "200000",
                "hide-details": "",
                variant: "outlined",
                label: "最多入库多少首"
              }, null, 8, ["modelValue"]),
              _createVNode$4(_component_v_text_field, {
                modelValue: form.value.library_refresh,
                "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => form.value.library_refresh = $event),
                modelModifiers: { number: true },
                type: "number",
                min: "0",
                max: "1440",
                "hide-details": "",
                variant: "outlined",
                label: "自动保鲜周期（分钟，0=只手动）"
              }, null, 8, ["modelValue"])
            ]),
            _cache[32] || (_cache[32] = _createElementVNode$4("div", { class: "xcf-hint" }, " 索引只为语音点歌服务（读标签与目录名，元数据缓存落在插件数据目录）。 索引状态与手动重建在侧栏的「音乐库」那一栏。 ", -1))
          ])
        ], 64)) : props.pane === "play" ? (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 1 }, [
          _createElementVNode$4("div", _hoisted_17$2, [
            _createElementVNode$4("div", _hoisted_18$1, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-sine-wave",
                size: "16"
              }),
              _cache[33] || (_cache[33] = _createTextVNode$4(" 播放接口 ", -1))
            ]),
            _createVNode$4(_component_v_select, {
              modelValue: form.value.play_mode,
              "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => form.value.play_mode = $event),
              items: playModeItems,
              label: "走哪条接口",
              "hide-details": "",
              variant: "outlined"
            }, null, 8, ["modelValue"]),
            _createElementVNode$4("div", _hoisted_19$1, [
              devices.value.length ? (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 0 }, [
                _createTextVNode$4(" 标记为需要 play_music 的型号： " + _toDisplayString$4(devices.value.filter((d) => d.needs_music_api).map((d) => d.name || d.device_id).join("、") || "无"), 1)
              ], 64)) : (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 1 }, [
                _createTextVNode$4("型号白名单在登录后取到设备才判得出来；拿不准就用「自动」。")
              ], 64))
            ]),
            _createVNode$4(_component_v_text_field, {
              modelValue: form.value.default_volume,
              "onUpdate:modelValue": _cache[9] || (_cache[9] = ($event) => form.value.default_volume = $event),
              modelModifiers: { number: true },
              class: "mt-3",
              type: "number",
              min: "0",
              max: "100",
              "hide-details": "",
              variant: "outlined",
              style: { "max-width": "220px" },
              label: "播放前设置音量（0=不改）"
            }, null, 8, ["modelValue"])
          ]),
          _createElementVNode$4("div", _hoisted_20$1, [
            _createElementVNode$4("div", _hoisted_21$1, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-play-circle-outline",
                size: "16"
              }),
              _cache[34] || (_cache[34] = _createTextVNode$4(" 连播与灯效 ", -1))
            ]),
            _createElementVNode$4("label", _hoisted_22$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.auto_next,
                "onUpdate:modelValue": _cache[10] || (_cache[10] = ($event) => form.value.auto_next = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[35] || (_cache[35] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "队列自动连播"),
                _createElementVNode$4("em", null, "关掉后侧栏队列播完当前这首不会自己接下一首（手动「下一首」仍然有效）。")
              ], -1))
            ]),
            _createElementVNode$4("label", _hoisted_23$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.keep_light,
                "onUpdate:modelValue": _cache[11] || (_cache[11] = ($event) => form.value.keep_light = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[36] || (_cache[36] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "播放时常亮"),
                _createElementVNode$4("em", null, "给音频类型带上 MUSIC，音箱播放时不熄灯；关掉更省电，但有些型号会当电台放。")
              ], -1))
            ]),
            _createElementVNode$4("label", _hoisted_24$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.tts_announce,
                "onUpdate:modelValue": _cache[12] || (_cache[12] = ($event) => form.value.tts_announce = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[37] || (_cache[37] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "连播时先播报下一曲"),
                _createElementVNode$4("em", null, "切歌前用 TTS 说一句曲名，音箱多一次请求，家里人多时更清楚在放谁的歌。")
              ], -1))
            ])
          ]),
          _cache[38] || (_cache[38] = _createElementVNode$4("div", { class: "xcf-note" }, " 这几项是全局偏好，推给哪台音箱由「账号与音箱」那一栏的默认音箱决定； 分组推送也在那一栏配。 ", -1))
        ], 64)) : (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 2 }, [
          _createElementVNode$4("div", _hoisted_25$1, [
            _createElementVNode$4("div", _hoisted_26$1, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-microphone-message",
                size: "16"
              }),
              _cache[39] || (_cache[39] = _createTextVNode$4(" 语音监听 ", -1))
            ]),
            _cache[41] || (_cache[41] = _createElementVNode$4("div", { class: "xcf-note xcf-note-warn" }, [
              _createTextVNode$4(" 打开后插件周期性读这几台音箱在小米云端的"),
              _createElementVNode$4("b", null, "对话记录"),
              _createTextVNode$4("，命中口令表的话才接手播放。 每读一次就是一个请求，只对真的要听的那几台开。 ")
            ], -1)),
            _createElementVNode$4("label", _hoisted_27$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.voice_enabled,
                "onUpdate:modelValue": _cache[13] || (_cache[13] = ($event) => form.value.voice_enabled = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[40] || (_cache[40] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "开启语音监听"),
                _createElementVNode$4("em", null, "关掉就是纯手动播放台，口令表与 AI 都不再被触发。")
              ], -1))
            ]),
            _createElementVNode$4("div", _hoisted_28$1, [
              _createVNode$4(_component_v_select, {
                modelValue: voiceDeviceList.value,
                "onUpdate:modelValue": _cache[14] || (_cache[14] = ($event) => voiceDeviceList.value = $event),
                items: voiceDeviceItems.value,
                label: "监听哪些音箱",
                multiple: "",
                chips: "",
                dense: "",
                closable: "",
                "hide-details": "",
                variant: "outlined",
                placeholder: "留空 = 只监听默认音箱"
              }, null, 8, ["modelValue", "items"]),
              _createVNode$4(_component_v_text_field, {
                modelValue: form.value.voice_poll,
                "onUpdate:modelValue": _cache[15] || (_cache[15] = ($event) => form.value.voice_poll = $event),
                modelModifiers: { number: true },
                type: "number",
                min: "2",
                max: "30",
                "hide-details": "",
                variant: "outlined",
                label: "轮询间隔（秒，2-30）"
              }, null, 8, ["modelValue"])
            ]),
            _createVNode$4(_component_v_text_field, {
              modelValue: form.value.voice_hint_text,
              "onUpdate:modelValue": _cache[16] || (_cache[16] = ($event) => form.value.voice_hint_text = $event),
              class: "mt-3",
              "hide-details": "",
              variant: "outlined",
              density: "compact",
              label: "搜索期间的提示语（留空用默认）",
              placeholder: "正在搜索，请稍候"
            }, null, 8, ["modelValue"]),
            _createElementVNode$4("div", _hoisted_29$1, _toDisplayString$4(devices.value.length ? `已取到 ${devices.value.length} 台设备，可直接勾选；留空时只跟着默认音箱走` : "设备候选要登录后才有；也可以先手填 device_id（逗号分隔）") + " ｜逐类口令的开关在本页「语音口令表」那一栏改。 ", 1)
          ]),
          _createElementVNode$4("div", _hoisted_30$1, [
            _createElementVNode$4("div", _hoisted_31$1, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-flash-off-outline",
                size: "16"
              }),
              _cache[42] || (_cache[42] = _createTextVNode$4(" 打断与播报 ", -1))
            ]),
            _createElementVNode$4("label", _hoisted_32$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.voice_interrupt,
                "onUpdate:modelValue": _cache[17] || (_cache[17] = ($event) => form.value.voice_interrupt = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[43] || (_cache[43] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "命中前先打断音箱"),
                _createElementVNode$4("em", null, "正在放歌时点歌，先停下再推新的。")
              ], -1))
            ]),
            _createElementVNode$4("label", _hoisted_33$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.voice_reply,
                "onUpdate:modelValue": _cache[18] || (_cache[18] = ($event) => form.value.voice_reply = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[44] || (_cache[44] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "播报执行结果"),
                _createElementVNode$4("em", null, "「已播放 XXX」，没播成也说一句为什么。")
              ], -1))
            ]),
            _createElementVNode$4("label", _hoisted_34$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.voice_hint_tts,
                "onUpdate:modelValue": _cache[19] || (_cache[19] = ($event) => form.value.voice_hint_tts = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[45] || (_cache[45] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "搜索期间播提示语"),
                _createElementVNode$4("em", null, "找歌要一两秒，先把上面那句提示播出去。")
              ], -1))
            ]),
            _createElementVNode$4("label", _hoisted_35$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.voice_memory,
                "onUpdate:modelValue": _cache[20] || (_cache[20] = ($event) => form.value.voice_memory = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[46] || (_cache[46] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "记住刚才那首"),
                _createElementVNode$4("em", null, "用于「再播一遍」「换一首」这类指代；只留在本机内存里，不落盘。")
              ], -1))
            ])
          ]),
          _createElementVNode$4("div", _hoisted_36$1, [
            _createElementVNode$4("div", _hoisted_37$1, [
              _createVNode$4(_component_v_icon, {
                icon: "mdi-robot-outline",
                size: "16"
              }),
              _cache[47] || (_cache[47] = _createTextVNode$4(" AI 兜底与对话 ", -1))
            ]),
            _createElementVNode$4("label", _hoisted_38$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.ai_enabled,
                "onUpdate:modelValue": _cache[21] || (_cache[21] = ($event) => form.value.ai_enabled = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[48] || (_cache[48] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "口令表没命中时交给 AI 判定"),
                _createElementVNode$4("em", null, "借主程序「设定 → AI 大模型」配好的那个助手，插件不存第二份 Key。")
              ], -1))
            ]),
            _createElementVNode$4("label", _hoisted_39$1, [
              _createVNode$4(_component_v_switch, {
                modelValue: form.value.ai_chat,
                "onUpdate:modelValue": _cache[22] || (_cache[22] = ($event) => form.value.ai_chat = $event),
                color: "primary",
                "hide-details": "",
                inset: "",
                density: "compact"
              }, null, 8, ["modelValue"]),
              _cache[49] || (_cache[49] = _createElementVNode$4("span", null, [
                _createElementVNode$4("b", null, "没命中的话当成聊天"),
                _createElementVNode$4("em", null, "说完 2 分钟内可追问；回答只走音箱 TTS，不进曲库播放。")
              ], -1))
            ]),
            _createVNode$4(_component_v_alert, {
              class: "mt-3",
              variant: "tonal",
              density: "compact",
              type: cfg.value?.ai_ready ? "success" : "warning"
            }, {
              default: _withCtx$4(() => [
                _createElementVNode$4("div", _hoisted_40, _toDisplayString$4(cfg.value?.ai_ready ? "主程序的智能助手可用，这里不再另配一份" : "主程序的智能助手还没配好"), 1),
                _createElementVNode$4("div", _hoisted_41, [
                  cfg.value?.ai_ready ? (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 0 }, [
                    _createTextVNode$4(" 来自「设定 → AI 大模型」：" + _toDisplayString$4(cfg.value.ai_provider || "未知来源") + " ", 1),
                    cfg.value.ai_model ? (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 0 }, [
                      _createTextVNode$4("｜" + _toDisplayString$4(cfg.value.ai_model), 1)
                    ], 64)) : _createCommentVNode$4("", true)
                  ], 64)) : (_openBlock$4(), _createElementBlock$4(_Fragment$4, { key: 1 }, [
                    _createTextVNode$4(_toDisplayString$4(cfg.value?.ai_message || "拿不到宿主配置"), 1)
                  ], 64))
                ])
              ]),
              _: 1
            }, 8, ["type"]),
            _createVNode$4(_component_v_text_field, {
              modelValue: form.value.ai_timeout,
              "onUpdate:modelValue": _cache[23] || (_cache[23] = ($event) => form.value.ai_timeout = $event),
              modelModifiers: { number: true },
              class: "mt-2",
              type: "number",
              min: "2",
              max: "30",
              "hide-details": "",
              variant: "outlined",
              style: { "max-width": "220px" },
              label: "超时（秒，2-30）"
            }, null, 8, ["modelValue"]),
            _cache[50] || (_cache[50] = _createElementVNode$4("div", { class: "xcf-hint" }, " AI 只能给出口令表里已有的那几种动作，置信度低或动作未知都当没判出来 —— 只是多一个入口，权限不比口令表大。 ", -1))
          ])
        ], 64))
      ]);
    };
  }
});

const ConfigForm = /* @__PURE__ */ _export_sfc(_sfc_main$4, [["__scopeId", "data-v-9c4f8d5d"]]);

const {computed: computed$3,inject,ref: ref$4} = await importShared('vue');
function useApi(props, after) {
  const base = computed$3(() => `plugin/${props.pluginId || props.sourcePluginId}`);
  const toast = inject("moviepilot:toast", null);
  const busy = ref$4("");
  const snackbar = ref$4({ show: false, text: "", color: "info" });
  function notify(msg, color = "info") {
    if (toast) {
      const fn = toast[color] || toast.info;
      fn.call(toast, msg);
      return;
    }
    snackbar.value = { show: true, text: msg, color };
  }
  async function get(path, params = {}) {
    return props.api.get(`${base.value}${path}${query(params)}`);
  }
  async function post(path, body, label) {
    busy.value = label;
    try {
      const res = await props.api.post(`${base.value}${path}`, body);
      if (res && res.ok === false) notify(res.message || res.error || `${label}失败`, "warning");
      else if (res && (res.message || res.ok === true)) notify(res.message || `${label}已执行`);
      if (res) after?.();
      return res;
    } catch (e) {
      notify(`${label}失败：${e?.message || e}`, "error");
      return null;
    } finally {
      busy.value = "";
    }
  }
  async function raw(path, body, label) {
    busy.value = label;
    try {
      return await props.api.post(`${base.value}${path}`, body);
    } catch (e) {
      notify(`${label}失败：${e?.message || e}`, "error");
      return null;
    } finally {
      busy.value = "";
    }
  }
  return { base, busy, snackbar, notify, get, post, raw };
}

const {defineComponent:_defineComponent$3} = await importShared('vue');

const {resolveComponent:_resolveComponent$3,createVNode:_createVNode$3,toDisplayString:_toDisplayString$3,createTextVNode:_createTextVNode$3,withCtx:_withCtx$3,openBlock:_openBlock$3,createElementBlock:_createElementBlock$3,createCommentVNode:_createCommentVNode$3,createElementVNode:_createElementVNode$3,createBlock:_createBlock$2,Fragment:_Fragment$3,unref:_unref$3,renderList:_renderList$3,withKeys:_withKeys} = await importShared('vue');

const _hoisted_1$3 = { class: "xcl" };
const _hoisted_2$3 = { class: "xcl-title" };
const _hoisted_3$3 = {
  key: 0,
  class: "text-caption text-grey ml-1"
};
const _hoisted_4$3 = {
  key: 0,
  class: "xcl-now pa-3 mb-3"
};
const _hoisted_5$3 = { class: "text-body-2" };
const _hoisted_6$2 = {
  key: 0,
  class: "text-caption text-grey ml-1"
};
const _hoisted_7$2 = { class: "text-caption text-grey" };
const _hoisted_8$2 = {
  key: 1,
  class: "d-flex mb-3",
  style: { "gap": "8px" }
};
const _hoisted_9$2 = {
  class: "d-flex mt-3",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_10$2 = {
  class: "d-flex mt-3",
  style: { "gap": "14px", "flex-wrap": "wrap", "align-items": "flex-start" }
};
const _hoisted_11$2 = {
  key: 1,
  class: "xcl-qr d-flex flex-column align-center justify-center rounded"
};
const _hoisted_12$2 = { class: "text-caption text-grey mt-1" };
const _hoisted_13$2 = { style: { "min-width": "220px", "flex": "1 1 220px" } };
const _hoisted_14$2 = { class: "text-body-2" };
const _hoisted_15$1 = {
  key: 0,
  class: "text-caption text-grey mt-1"
};
const _hoisted_16$1 = {
  class: "d-flex mt-2",
  style: { "gap": "8px" }
};
const _hoisted_17$1 = {
  class: "d-flex mt-3",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_18 = {
  key: 5,
  class: "mt-3"
};
const _hoisted_19 = { class: "text-body-2" };
const _hoisted_20 = { class: "text-caption" };
const _hoisted_21 = {
  key: 0,
  class: "text-caption"
};
const _hoisted_22 = {
  key: 6,
  class: "mt-2"
};
const _hoisted_23 = {
  class: "d-flex",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_24 = {
  key: 0,
  class: "text-caption text-grey align-self-center"
};
const _hoisted_25 = { class: "xcl-sub mt-4" };
const _hoisted_26 = { class: "text-caption text-grey ml-2" };
const _hoisted_27 = { key: 0 };
const _hoisted_28 = { style: { "min-width": "0", "flex": "1 1 200px" } };
const _hoisted_29 = { class: "text-body-2" };
const _hoisted_30 = { class: "text-caption text-grey" };
const _hoisted_31 = {
  key: 8,
  class: "text-caption text-warning mt-3"
};
const _hoisted_32 = {
  key: 9,
  class: "text-caption text-grey mt-3"
};
const _hoisted_33 = { class: "xcl-sub mt-4" };
const _hoisted_34 = { class: "text-caption text-grey ml-2" };
const _hoisted_35 = {
  class: "d-flex align-center",
  style: { "gap": "10px", "flex-wrap": "wrap" }
};
const _hoisted_36 = { style: { "min-width": "0", "flex": "1 1 200px" } };
const _hoisted_37 = { class: "text-body-2" };
const _hoisted_38 = { class: "text-caption text-grey" };
const _hoisted_39 = {
  class: "d-flex align-center",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const {computed: computed$2,onBeforeUnmount: onBeforeUnmount$1,ref: ref$3} = await importShared('vue');
const _sfc_main$3 = /* @__PURE__ */ _defineComponent$3({
  __name: "ConfigLogin",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    status: { type: Object, default: null }
  },
  emits: ["changed"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const { base, busy, snackbar, notify, post, raw } = useApi(props, () => emit("changed"));
    const cfg = computed$2(() => props.status?.config);
    const devices = computed$2(() => props.status?.devices || []);
    const onlineCount = computed$2(() => devices.value.filter((d) => d.presence === "online").length);
    const groups = computed$2(() => props.status?.extra?.groups || []);
    const defaultGroup = computed$2(() => String(props.status?.extra?.default_group || ""));
    const LOGIN_MODES = [
      { value: "password", title: "账号密码", icon: "mdi-account-key-outline" },
      { value: "qrcode", title: "扫码", icon: "mdi-qrcode-scan" },
      { value: "cookie", title: "Cookie 票据", icon: "mdi-cookie" }
    ];
    const loginMode = ref$3("password");
    const credential = ref$3({ username: "", password: "" });
    const loginResult = ref$3(null);
    const verifyCode = ref$3("");
    const manual = ref$3({ user_id: "", pass_token: "" });
    const picker = ref$3(false);
    const loginStates = {
      success: { color: "success", text: "登录成功" },
      need_captcha: { color: "warning", text: "卡在图形验证码：照上面的图填验证码提交" },
      need_verify: { color: "warning", text: "卡在二次校验：小米已把验证码发往账号绑定的手机/邮箱，填进来提交" },
      failed: { color: "error", text: "登录失败" }
    };
    const loginState = computed$2(() => loginStates[loginResult.value?.state || ""] || null);
    const reloginText = computed$2(() => props.status?.login_method === "qrcode" ? "票据已过期，切到「扫码」重新扫一次" : "票据已过期，再点一次「登录」即可");
    const pickerDevices = computed$2(() => devices.value.slice().sort((a, b) => {
      const rank = (d) => d.is_default ? 0 : d.presence === "online" ? 1 : 2;
      return rank(a) - rank(b);
    }));
    function switchMode(mode) {
      if (mode !== "qrcode") stopQr();
      loginMode.value = mode;
      if (mode === "qrcode" && qr.value?.state !== "qr_waiting") startQr();
    }
    async function startLogin() {
      const body = {};
      const username = credential.value.username.trim();
      if (username && !username.includes("*")) body.username = username;
      if (credential.value.password) body.password = credential.value.password;
      if (Object.keys(body).length) await post("/config", body, "保存账号");
      const res = await raw("/login", {}, "登录");
      if (res) afterLogin(res);
    }
    async function submitVerify() {
      const code = verifyCode.value.trim();
      if (!code) {
        notify("填写验证码", "warning");
        return;
      }
      const res = await raw("/login", { code }, "提交验证码");
      if (res) afterLogin(res);
    }
    async function submitManual() {
      const userId = manual.value.user_id.trim();
      const passToken = manual.value.pass_token.trim();
      if (!userId || !passToken) {
        notify("userId 与 passToken 都要填", "warning");
        return;
      }
      const res = await raw(
        "/login",
        { user_id: userId, pass_token: passToken },
        "校验票据"
      );
      if (res) afterLogin(res);
    }
    async function resendVerify() {
      const res = await raw("/login", { resend: true }, "重发验证码");
      if (res) afterLogin(res);
    }
    async function afterLogin(res) {
      loginResult.value = res || null;
      verifyCode.value = "";
      emit("changed");
      if (res?.state === "success") {
        notify(res.message || "登录成功", "success");
        credential.value.password = "";
        manual.value = { user_id: "", pass_token: "" };
        stopQr();
        qr.value = null;
        if (res.device_count) picker.value = true;
        else notify("登录成功，但这个账号下没取到音箱设备", "warning");
        return;
      }
      if (res?.state === "need_captcha" || res?.state === "need_verify") {
        notify(res.message || "需要验证码", "warning");
        return;
      }
      notify(res?.message || "登录失败", "error");
    }
    const qr = ref$3(null);
    const qrBusy = ref$3(false);
    const qrLeft = ref$3(0);
    let qrSeq = 0;
    let qrTimer = null;
    let qrTick = null;
    const qrText = computed$2(() => {
      const state = qr.value?.state || "";
      if (state === "qr_waiting") return qrLeft.value > 0 ? `等待扫码确认…（${qrLeft.value} 秒内有效）` : "等待扫码确认…";
      if (state === "qr_expired") return "这张二维码已经作废，重新获取一张";
      if (state === "failed") return "扫码未成功";
      return "";
    });
    function stopQr() {
      qrSeq += 1;
      if (qrTimer) {
        clearTimeout(qrTimer);
        qrTimer = null;
      }
      if (qrTick) {
        clearInterval(qrTick);
        qrTick = null;
      }
      qrBusy.value = false;
    }
    async function askQr(action) {
      const seq = qrSeq;
      try {
        const res = await props.api.post(`${base.value}/login/qrcode`, { action });
        if (seq !== qrSeq || !res) return null;
        return res;
      } catch (e) {
        if (seq === qrSeq) notify(`扫码请求失败：${e?.message || e}`, "error");
        return null;
      }
    }
    async function startQr() {
      stopQr();
      const seq = qrSeq;
      qrBusy.value = true;
      const res = await askQr("start");
      if (seq === qrSeq) qrBusy.value = false;
      if (res) applyQr(res);
    }
    function applyQr(res) {
      if (res.state === "qr_waiting") {
        if (!res.qr_image && qr.value?.qr_image) res = { ...res, qr_image: qr.value.qr_image };
        qr.value = res;
        qrLeft.value = res.expires_in > 0 ? res.expires_in : Math.max(0, qrLeft.value);
        runCountdown();
        qrTimer = setTimeout(async () => {
          const again = await askQr("poll");
          if (again) applyQr(again);
        }, 400);
        return;
      }
      qrLeft.value = 0;
      if (res.state === "success") {
        afterQrSuccess(res);
        return;
      }
      notify(res.message || (res.state === "qr_expired" ? "二维码已过期" : "扫码未成功"), "warning");
    }
    function runCountdown() {
      if (qrTick) clearInterval(qrTick);
      qrTick = setInterval(() => {
        qrLeft.value -= 1;
        if (qrLeft.value > 0) return;
        stopQr();
        qr.value = { ...qr.value, state: "qr_expired", message: "二维码已过期，重新获取一张再扫" };
        notify("二维码已过期，重新获取一张再扫", "warning");
      }, 1e3);
    }
    function afterQrSuccess(res) {
      stopQr();
      qr.value = null;
      loginResult.value = {
        state: "success",
        message: res.message || "扫码登录成功",
        captcha_image: "",
        logged_in: true,
        device_count: res.device_count
      };
      credential.value.password = "";
      manual.value = { user_id: "", pass_token: "" };
      notify(res.message || "扫码登录成功", "success");
      emit("changed");
      if (res.device_count) picker.value = true;
      else notify("登录成功，但这个账号下没取到音箱设备", "warning");
    }
    function cancelQr() {
      stopQr();
      qr.value = null;
    }
    onBeforeUnmount$1(stopQr);
    async function doLogout() {
      stopQr();
      qr.value = null;
      const res = await post("/logout", {}, "退出登录");
      if (res) {
        loginResult.value = null;
        picker.value = false;
        credential.value.password = "";
      }
    }
    async function refreshDevices() {
      busy.value = "刷新设备";
      try {
        const res = await props.api.get(`plugin/${props.pluginId || props.sourcePluginId}/devices`);
        if (res?.message && !res?.devices?.length) notify(`设备刷新失败：${res.message}`, "warning");
        else notify(`设备列表已刷新，共 ${(res?.devices || []).length} 台`);
        emit("changed");
      } catch (e) {
        notify(`设备刷新失败：${e?.message || e}`, "error");
      } finally {
        busy.value = "";
      }
    }
    function selectDevice(device) {
      return post("/device/select", { device_id: device.device_id }, "设为默认");
    }
    function clearDefault() {
      return post("/device/select", { device_id: "" }, "取消默认音箱");
    }
    const groupForm = ref$3({ name: "", device_ids: [] });
    const editingGroup = ref$3("");
    const groupDeviceItems = computed$2(() => devices.value.map((item) => ({
      title: item.name || item.alias || item.device_id,
      value: item.device_id
    })));
    function newGroup() {
      editingGroup.value = "";
      groupForm.value = { name: "", device_ids: [] };
    }
    function editGroup(row) {
      editingGroup.value = row.name;
      groupForm.value = { name: row.name, device_ids: [...row.device_ids] };
    }
    async function saveGroup() {
      const name = groupForm.value.name.trim();
      if (!name) {
        notify("先给分组起个名字", "warning");
        return;
      }
      if (groupForm.value.device_ids.length < 2) {
        notify("分组至少要有两台音箱才有意义", "warning");
        return;
      }
      const res = await post("/groups", { name, device_ids: groupForm.value.device_ids }, "保存分组");
      if (res && res.ok !== false) {
        editingGroup.value = "";
        groupForm.value = { name: "", device_ids: [] };
      }
    }
    function removeGroup(name) {
      return post("/groups", { name, remove: true }, "删除分组").then(() => {
        if (editingGroup.value === name) newGroup();
      });
    }
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent$3("v-icon");
      const _component_v_chip = _resolveComponent$3("v-chip");
      const _component_v_btn = _resolveComponent$3("v-btn");
      const _component_v_btn_toggle = _resolveComponent$3("v-btn-toggle");
      const _component_v_text_field = _resolveComponent$3("v-text-field");
      const _component_v_img = _resolveComponent$3("v-img");
      const _component_v_progress_linear = _resolveComponent$3("v-progress-linear");
      const _component_v_alert = _resolveComponent$3("v-alert");
      const _component_v_expand_transition = _resolveComponent$3("v-expand-transition");
      const _component_v_card = _resolveComponent$3("v-card");
      const _component_v_combobox = _resolveComponent$3("v-combobox");
      const _component_v_snackbar = _resolveComponent$3("v-snackbar");
      return _openBlock$3(), _createElementBlock$3("div", _hoisted_1$3, [
        _createElementVNode$3("div", _hoisted_2$3, [
          _createVNode$3(_component_v_icon, {
            icon: "mdi-account-sync-outline",
            size: "18",
            class: "xcl-mark"
          }),
          _cache[11] || (_cache[11] = _createTextVNode$3(" 小米账号与音箱 ", -1)),
          _createVNode$3(_component_v_chip, {
            size: "x-small",
            variant: "tonal",
            class: "ml-2",
            color: props.status?.token_expired ? "error" : props.status?.logged_in ? "success" : "grey"
          }, {
            default: _withCtx$3(() => [
              _createTextVNode$3(_toDisplayString$3(props.status?.token_expired ? "票据已过期" : props.status?.logged_in ? "已登录" : "未登录"), 1)
            ]),
            _: 1
          }, 8, ["color"]),
          devices.value.length ? (_openBlock$3(), _createElementBlock$3("span", _hoisted_3$3, _toDisplayString$3(devices.value.length) + " 台音箱，" + _toDisplayString$3(onlineCount.value) + " 台在线 ", 1)) : _createCommentVNode$3("", true)
        ]),
        props.status?.logged_in ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_4$3, [
          _createElementVNode$3("div", _hoisted_5$3, [
            _createTextVNode$3(_toDisplayString$3(cfg.value?.username_set ? cfg.value.username : "（未记录账号名）") + " ", 1),
            cfg.value?.username_set ? (_openBlock$3(), _createElementBlock$3("span", _hoisted_6$2, "已掩码")) : _createCommentVNode$3("", true),
            cfg.value?.default_device_id ? (_openBlock$3(), _createBlock$2(_component_v_chip, {
              key: 1,
              size: "x-small",
              variant: "flat",
              color: "primary",
              class: "ml-2"
            }, {
              default: _withCtx$3(() => [
                _createTextVNode$3(" 默认：" + _toDisplayString$3(devices.value.find((d) => d.device_id === cfg.value?.default_device_id)?.name || devices.value.find((d) => d.device_id === cfg.value?.default_device_id)?.alias || "不在当前设备列表里"), 1)
              ]),
              _: 1
            })) : (_openBlock$3(), _createBlock$2(_component_v_chip, {
              key: 2,
              size: "x-small",
              variant: "tonal",
              class: "ml-2"
            }, {
              default: _withCtx$3(() => [..._cache[12] || (_cache[12] = [
                _createTextVNode$3("默认：第一台在线音箱", -1)
              ])]),
              _: 1
            }))
          ]),
          _createElementVNode$3("div", _hoisted_7$2, [
            _createTextVNode$3(" 票据保存于 " + _toDisplayString$3(props.status?.token_saved_at || "-") + " ", 1),
            props.status?.manual_token ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 0 }, [
              _createTextVNode$3("｜手工填的 Cookie 票据")
            ], 64)) : _createCommentVNode$3("", true),
            props.status?.token_expired ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 1 }, [
              _createTextVNode$3("｜" + _toDisplayString$3(reloginText.value), 1)
            ], 64)) : _createCommentVNode$3("", true)
          ])
        ])) : _createCommentVNode$3("", true),
        props.status?.logged_in ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_8$2, [
          _createVNode$3(_component_v_btn, {
            size: "small",
            variant: "tonal",
            loading: _unref$3(busy) === "刷新设备",
            "prepend-icon": "mdi-refresh",
            onClick: refreshDevices
          }, {
            default: _withCtx$3(() => [..._cache[13] || (_cache[13] = [
              _createTextVNode$3("刷新设备 ", -1)
            ])]),
            _: 1
          }, 8, ["loading"]),
          _createVNode$3(_component_v_btn, {
            size: "small",
            variant: "text",
            color: "error",
            loading: _unref$3(busy) === "退出登录",
            "prepend-icon": "mdi-logout",
            onClick: doLogout
          }, {
            default: _withCtx$3(() => [..._cache[14] || (_cache[14] = [
              _createTextVNode$3("退出登录 ", -1)
            ])]),
            _: 1
          }, 8, ["loading"])
        ])) : _createCommentVNode$3("", true),
        _createVNode$3(_component_v_btn_toggle, {
          "model-value": loginMode.value,
          divided: "",
          mandatory: "",
          density: "compact",
          color: "primary",
          style: { "align-self": "flex-start" },
          "onUpdate:modelValue": _cache[0] || (_cache[0] = ($event) => switchMode($event))
        }, {
          default: _withCtx$3(() => [
            (_openBlock$3(), _createElementBlock$3(_Fragment$3, null, _renderList$3(LOGIN_MODES, (mode) => {
              return _createVNode$3(_component_v_btn, {
                key: mode.value,
                value: mode.value,
                size: "small",
                variant: "text",
                class: "text-none",
                "prepend-icon": mode.icon
              }, {
                default: _withCtx$3(() => [
                  _createTextVNode$3(_toDisplayString$3(mode.title), 1)
                ]),
                _: 2
              }, 1032, ["value", "prepend-icon"]);
            }), 64))
          ]),
          _: 1
        }, 8, ["model-value"]),
        loginMode.value === "password" ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 2 }, [
          _createElementVNode$3("div", _hoisted_9$2, [
            _createVNode$3(_component_v_text_field, {
              modelValue: credential.value.username,
              "onUpdate:modelValue": _cache[1] || (_cache[1] = ($event) => credential.value.username = $event),
              density: "compact",
              "hide-details": "",
              clearable: "",
              label: "小米账号（手机号 / 邮箱 / 账号ID）",
              placeholder: cfg.value?.username || "留空用已保存的",
              style: { "min-width": "220px", "flex": "1 1 220px" },
              onKeyup: _withKeys(startLogin, ["enter"])
            }, null, 8, ["modelValue", "placeholder"]),
            _createVNode$3(_component_v_text_field, {
              modelValue: credential.value.password,
              "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => credential.value.password = $event),
              density: "compact",
              "hide-details": "",
              type: credential.value.password ? "text" : "password",
              label: cfg.value?.password_set ? "密码（留空则用已保存的）" : "密码",
              style: { "min-width": "180px", "flex": "1 1 180px" },
              onKeyup: _withKeys(startLogin, ["enter"])
            }, null, 8, ["modelValue", "type", "label"]),
            _createVNode$3(_component_v_btn, {
              color: "primary",
              variant: "flat",
              class: "text-none",
              loading: _unref$3(busy) === "登录" || _unref$3(busy) === "保存账号",
              "prepend-icon": "mdi-login",
              onClick: startLogin
            }, {
              default: _withCtx$3(() => [..._cache[15] || (_cache[15] = [
                _createTextVNode$3("登录 ", -1)
              ])]),
              _: 1
            }, 8, ["loading"])
          ]),
          _cache[16] || (_cache[16] = _createElementVNode$3("div", { class: "text-caption text-grey mt-1" }, " 账号密码只存进本机配置、接口永不回显明文；点「登录」即先保存再发起，票据存在插件数据里。 小米判定「新设备」时会要图形验证码或手机/邮箱验证码，下面那步照提示填进来就行。 ", -1))
        ], 64)) : loginMode.value === "qrcode" ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 3 }, [
          _createElementVNode$3("div", _hoisted_10$2, [
            qr.value?.qr_image ? (_openBlock$3(), _createBlock$2(_component_v_img, {
              key: 0,
              src: qr.value.qr_image,
              width: "184",
              height: "184",
              class: "rounded",
              contain: ""
            }, null, 8, ["src"])) : (_openBlock$3(), _createElementBlock$3("div", _hoisted_11$2, [
              _createVNode$3(_component_v_icon, {
                icon: qrBusy.value ? "mdi-loading mdi-refresh" : "mdi-qrcode-scan",
                color: qrBusy.value ? "primary" : "grey",
                size: "40"
              }, null, 8, ["icon", "color"]),
              _createElementVNode$3("span", _hoisted_12$2, _toDisplayString$3(qrBusy.value ? "二维码获取中…" : "没取到二维码"), 1)
            ])),
            _createElementVNode$3("div", _hoisted_13$2, [
              _createElementVNode$3("div", _hoisted_14$2, _toDisplayString$3(qrText.value || "切到这个标签就已经取好一张，直接用手机 App 扫"), 1),
              qr.value?.message && qrText.value ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_15$1, _toDisplayString$3(qr.value.message), 1)) : _createCommentVNode$3("", true),
              qr.value?.state === "qr_waiting" && qr.value.expires_in ? (_openBlock$3(), _createBlock$2(_component_v_progress_linear, {
                key: 1,
                height: "4",
                rounded: "",
                color: "primary",
                class: "mt-2",
                "model-value": Math.max(0, Math.min(100, qrLeft.value / (qr.value.expires_in || 1) * 100))
              }, null, 8, ["model-value"])) : _createCommentVNode$3("", true),
              _createElementVNode$3("div", _hoisted_16$1, [
                _createVNode$3(_component_v_btn, {
                  size: "small",
                  variant: "tonal",
                  color: "primary",
                  loading: qrBusy.value,
                  "prepend-icon": qr.value?.qr_image ? "mdi-refresh" : "mdi-qrcode-scan",
                  onClick: startQr
                }, {
                  default: _withCtx$3(() => [
                    _createTextVNode$3(_toDisplayString$3(qr.value?.qr_image ? "换一张" : "获取二维码"), 1)
                  ]),
                  _: 1
                }, 8, ["loading", "prepend-icon"]),
                qr.value ? (_openBlock$3(), _createBlock$2(_component_v_btn, {
                  key: 0,
                  size: "small",
                  variant: "text",
                  onClick: cancelQr
                }, {
                  default: _withCtx$3(() => [..._cache[17] || (_cache[17] = [
                    _createTextVNode$3("取消", -1)
                  ])]),
                  _: 1
                })) : _createCommentVNode$3("", true)
              ])
            ])
          ]),
          _cache[18] || (_cache[18] = _createElementVNode$3("div", { class: "text-caption text-grey mt-2" }, " 用米家 / 小米音箱 App 扫码后在手机上确认，页面会自动问到结果；中途过期就点「换一张」。 这条路不走密码，也就绕开了「新设备登录」那套风控，账密反复登不上时通常它能用。 ", -1))
        ], 64)) : (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 4 }, [
          _createElementVNode$3("div", _hoisted_17$1, [
            _createVNode$3(_component_v_text_field, {
              modelValue: manual.value.user_id,
              "onUpdate:modelValue": _cache[3] || (_cache[3] = ($event) => manual.value.user_id = $event),
              density: "compact",
              "hide-details": "",
              clearable: "",
              label: "User ID",
              placeholder: "浏览器 Cookie 里的 userId",
              style: { "min-width": "160px", "flex": "1 1 160px" },
              onKeyup: _withKeys(submitManual, ["enter"])
            }, null, 8, ["modelValue"]),
            _createVNode$3(_component_v_text_field, {
              modelValue: manual.value.pass_token,
              "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => manual.value.pass_token = $event),
              density: "compact",
              "hide-details": "",
              clearable: "",
              label: "Pass Token",
              placeholder: "浏览器 Cookie 里的 passToken",
              style: { "min-width": "220px", "flex": "2 1 260px" },
              onKeyup: _withKeys(submitManual, ["enter"])
            }, null, 8, ["modelValue"]),
            _createVNode$3(_component_v_btn, {
              size: "small",
              variant: "tonal",
              color: "primary",
              loading: _unref$3(busy) === "校验票据",
              "prepend-icon": "mdi-key-variant",
              onClick: submitManual
            }, {
              default: _withCtx$3(() => [..._cache[19] || (_cache[19] = [
                _createTextVNode$3("校验并使用 ", -1)
              ])]),
              _: 1
            }, 8, ["loading"]),
            _createVNode$3(_component_v_btn, {
              size: "small",
              variant: "text",
              onClick: _cache[5] || (_cache[5] = ($event) => manual.value = { user_id: "", pass_token: "" })
            }, {
              default: _withCtx$3(() => [..._cache[20] || (_cache[20] = [
                _createTextVNode$3("清空", -1)
              ])]),
              _: 1
            })
          ]),
          _cache[21] || (_cache[21] = _createElementVNode$3("div", { class: "text-caption text-grey mt-1" }, [
            _createTextVNode$3(" 先在浏览器里登录 "),
            _createElementVNode$3("span", { class: "text-no-wrap" }, "account.xiaomi.com"),
            _createTextVNode$3("（passToken 只在这个域的 Cookie 里，别处复制的都是死票），再从 DevTools 的 Cookie 里取 userId 和 passToken 两项的 "),
            _createElementVNode$3("b", null, "值"),
            _createTextVNode$3("填进来（别粘整串 cookie）。凭据只发给本插件用于换票， 不写日志、接口不回显。后端拿 passToken 现换一张活票，换到了才算这串凭据真的有效。 ")
          ], -1))
        ], 64)),
        loginResult.value ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_18, [
          _createVNode$3(_component_v_alert, {
            type: loginState.value?.color || "info",
            variant: "tonal",
            density: "compact",
            icon: loginState.value ? void 0 : "mdi-information-outline"
          }, {
            default: _withCtx$3(() => [
              _createElementVNode$3("div", _hoisted_19, _toDisplayString$3(loginState.value?.text || loginResult.value.state), 1),
              _createElementVNode$3("div", _hoisted_20, _toDisplayString$3(loginResult.value.message), 1),
              loginResult.value.state === "success" ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_21, [
                _createTextVNode$3(" 取到 " + _toDisplayString$3(loginResult.value.device_count) + " 台音箱 ", 1),
                loginResult.value.device_count ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 0 }, [
                  _createTextVNode$3("｜下面已列出，点「选它」设为默认")
                ], 64)) : _createCommentVNode$3("", true)
              ])) : _createCommentVNode$3("", true)
            ]),
            _: 1
          }, 8, ["type", "icon"])
        ])) : _createCommentVNode$3("", true),
        loginState.value?.color === "warning" ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_22, [
          loginResult.value?.captcha_image ? (_openBlock$3(), _createBlock$2(_component_v_img, {
            key: 0,
            src: loginResult.value.captcha_image,
            "max-height": "120",
            class: "rounded mb-2",
            style: { "max-width": "240px" },
            contain: ""
          }, null, 8, ["src"])) : _createCommentVNode$3("", true),
          _createElementVNode$3("div", _hoisted_23, [
            _createVNode$3(_component_v_text_field, {
              modelValue: verifyCode.value,
              "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => verifyCode.value = $event),
              density: "compact",
              "hide-details": "",
              label: loginResult.value?.state === "need_verify" ? "邮箱/短信验证码" : "图形验证码",
              style: { "max-width": "220px" },
              onKeyup: _withKeys(submitVerify, ["enter"])
            }, null, 8, ["modelValue", "label"]),
            _createVNode$3(_component_v_btn, {
              size: "small",
              variant: "tonal",
              color: "info",
              loading: _unref$3(busy) === "提交验证码",
              onClick: submitVerify
            }, {
              default: _withCtx$3(() => [..._cache[22] || (_cache[22] = [
                _createTextVNode$3("提交 ", -1)
              ])]),
              _: 1
            }, 8, ["loading"]),
            _createVNode$3(_component_v_btn, {
              size: "small",
              variant: "tonal",
              loading: _unref$3(busy) === "重发验证码",
              "prepend-icon": "mdi-send",
              onClick: resendVerify
            }, {
              default: _withCtx$3(() => [..._cache[23] || (_cache[23] = [
                _createTextVNode$3("重发验证码 ", -1)
              ])]),
              _: 1
            }, 8, ["loading"]),
            loginResult.value?.state === "need_verify" ? (_openBlock$3(), _createElementBlock$3("span", _hoisted_24, " 收不到就点重发：会重新走一遍登录与发码，小米才会再寄一次 ")) : _createCommentVNode$3("", true)
          ])
        ])) : _createCommentVNode$3("", true),
        devices.value.length ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 7 }, [
          _createElementVNode$3("div", _hoisted_25, [
            _cache[24] || (_cache[24] = _createTextVNode$3(" 音箱 ", -1)),
            _createElementVNode$3("span", _hoisted_26, _toDisplayString$3(picker.value ? `${devices.value.length} 台可选` : "点标题展开"), 1),
            _createVNode$3(_component_v_btn, {
              size: "x-small",
              variant: "text",
              class: "text-none ml-1",
              "prepend-icon": picker.value ? "mdi-chevron-up" : "mdi-chevron-down",
              onClick: _cache[7] || (_cache[7] = ($event) => picker.value = !picker.value)
            }, {
              default: _withCtx$3(() => [
                _createTextVNode$3(_toDisplayString$3(picker.value ? "收起" : "选择默认音箱"), 1)
              ]),
              _: 1
            }, 8, ["prepend-icon"])
          ]),
          _createVNode$3(_component_v_expand_transition, null, {
            default: _withCtx$3(() => [
              picker.value ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_27, [
                (_openBlock$3(true), _createElementBlock$3(_Fragment$3, null, _renderList$3(pickerDevices.value, (device) => {
                  return _openBlock$3(), _createElementBlock$3("div", {
                    key: device.device_id,
                    class: "xcl-device"
                  }, [
                    _createVNode$3(_component_v_icon, {
                      icon: device.presence === "online" ? "mdi-speaker" : "mdi-speaker-off",
                      color: device.is_default ? "primary" : device.presence === "online" ? "success" : "grey"
                    }, null, 8, ["icon", "color"]),
                    _createElementVNode$3("div", _hoisted_28, [
                      _createElementVNode$3("div", _hoisted_29, [
                        _createTextVNode$3(_toDisplayString$3(device.name || device.alias || device.device_id) + " ", 1),
                        device.is_default ? (_openBlock$3(), _createBlock$2(_component_v_chip, {
                          key: 0,
                          size: "x-small",
                          variant: "flat",
                          color: "primary",
                          class: "ml-1"
                        }, {
                          default: _withCtx$3(() => [..._cache[25] || (_cache[25] = [
                            _createTextVNode$3(" 当前默认 ", -1)
                          ])]),
                          _: 1
                        })) : _createCommentVNode$3("", true)
                      ]),
                      _createElementVNode$3("div", _hoisted_30, [
                        _createTextVNode$3(_toDisplayString$3(device.model || device.hardware || "未知型号") + "｜" + _toDisplayString$3(device.presence === "online" ? "在线" : "离线") + " ", 1),
                        device.needs_music_api ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 0 }, [
                          _createTextVNode$3("｜该型号需走 play_music，直链可能被当成电台播放")
                        ], 64)) : _createCommentVNode$3("", true)
                      ])
                    ]),
                    _createVNode$3(_component_v_btn, {
                      size: "small",
                      variant: "text",
                      color: "primary",
                      disabled: device.is_default,
                      loading: _unref$3(busy) === "设为默认",
                      onClick: ($event) => selectDevice(device)
                    }, {
                      default: _withCtx$3(() => [
                        _createTextVNode$3(_toDisplayString$3(device.is_default ? "已是默认" : "选它"), 1)
                      ]),
                      _: 2
                    }, 1032, ["disabled", "loading", "onClick"]),
                    _createVNode$3(_component_v_btn, {
                      size: "small",
                      variant: "text",
                      disabled: !cfg.value?.default_device_id,
                      loading: _unref$3(busy) === "取消默认",
                      onClick: clearDefault
                    }, {
                      default: _withCtx$3(() => [..._cache[26] || (_cache[26] = [
                        _createTextVNode$3("取消默认", -1)
                      ])]),
                      _: 1
                    }, 8, ["disabled", "loading"])
                  ]);
                }), 128)),
                _cache[27] || (_cache[27] = _createElementVNode$3("div", { class: "text-caption text-grey mt-1" }, " 在线的放前面，离线也能选（音箱上线后照样能用）。不选就落在第一台在线设备上； 侧栏播放台的操作只作用于这台默认音箱。 ", -1))
              ])) : _createCommentVNode$3("", true)
            ]),
            _: 1
          })
        ], 64)) : props.status?.logged_in ? (_openBlock$3(), _createElementBlock$3("div", _hoisted_31, " 设备列表是空的：可能票据已失效，点上面的「刷新设备」重试。 ")) : (_openBlock$3(), _createElementBlock$3("div", _hoisted_32, " 登录成功后这里才会列出音箱，也要选了默认音箱，后面的配置与播放才有意义。 ")),
        devices.value.length > 1 ? (_openBlock$3(), _createElementBlock$3(_Fragment$3, { key: 10 }, [
          _createElementVNode$3("div", _hoisted_33, [
            _cache[28] || (_cache[28] = _createTextVNode$3(" 设备分组 ", -1)),
            _createElementVNode$3("span", _hoisted_34, _toDisplayString$3(groups.value.length) + " 组", 1)
          ]),
          (_openBlock$3(true), _createElementBlock$3(_Fragment$3, null, _renderList$3(groups.value, (row) => {
            return _openBlock$3(), _createBlock$2(_component_v_card, {
              key: row.name,
              variant: "outlined",
              class: "xcl-card pa-3 mb-2"
            }, {
              default: _withCtx$3(() => [
                _createElementVNode$3("div", _hoisted_35, [
                  _createVNode$3(_component_v_icon, {
                    icon: "mdi-home-group",
                    color: "primary"
                  }),
                  _createElementVNode$3("div", _hoisted_36, [
                    _createElementVNode$3("div", _hoisted_37, [
                      _createTextVNode$3(_toDisplayString$3(row.name) + " ", 1),
                      defaultGroup.value === row.name ? (_openBlock$3(), _createBlock$2(_component_v_chip, {
                        key: 0,
                        size: "x-small",
                        variant: "flat",
                        color: "primary",
                        class: "ml-1"
                      }, {
                        default: _withCtx$3(() => [..._cache[29] || (_cache[29] = [
                          _createTextVNode$3(" 默认音箱所在组 ", -1)
                        ])]),
                        _: 1
                      })) : _createCommentVNode$3("", true)
                    ]),
                    _createElementVNode$3("div", _hoisted_38, _toDisplayString$3(row.devices.join("、") || row.device_ids.join("、")), 1)
                  ]),
                  _createVNode$3(_component_v_btn, {
                    size: "x-small",
                    variant: "text",
                    loading: _unref$3(busy) === "保存分组",
                    onClick: ($event) => editGroup(row)
                  }, {
                    default: _withCtx$3(() => [..._cache[30] || (_cache[30] = [
                      _createTextVNode$3("编辑", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading", "onClick"]),
                  _createVNode$3(_component_v_btn, {
                    size: "x-small",
                    variant: "text",
                    color: "error",
                    loading: _unref$3(busy) === "删除分组",
                    onClick: ($event) => removeGroup(row.name)
                  }, {
                    default: _withCtx$3(() => [..._cache[31] || (_cache[31] = [
                      _createTextVNode$3("删除 ", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading", "onClick"])
                ])
              ]),
              _: 2
            }, 1024);
          }), 128)),
          _createVNode$3(_component_v_card, {
            variant: "outlined",
            class: "xcl-card pa-3"
          }, {
            default: _withCtx$3(() => [
              _createElementVNode$3("div", _hoisted_39, [
                _createVNode$3(_component_v_text_field, {
                  modelValue: groupForm.value.name,
                  "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => groupForm.value.name = $event),
                  density: "compact",
                  "hide-details": "",
                  clearable: "",
                  label: editingGroup.value ? `改名或另起一组（原「${editingGroup.value}」）` : "新建分组",
                  placeholder: "全屋 / 卧室 / 客厅…",
                  style: { "max-width": "220px" }
                }, null, 8, ["modelValue", "label"]),
                _createVNode$3(_component_v_combobox, {
                  modelValue: groupForm.value.device_ids,
                  "onUpdate:modelValue": _cache[9] || (_cache[9] = ($event) => groupForm.value.device_ids = $event),
                  items: groupDeviceItems.value,
                  "item-title": "title",
                  "item-value": "value",
                  density: "compact",
                  "hide-details": "",
                  multiple: "",
                  chips: "",
                  "closable-chips": "",
                  label: "组内音箱（≥2 台）",
                  style: { "min-width": "260px", "flex": "1 1 260px" }
                }, null, 8, ["modelValue", "items"]),
                _createVNode$3(_component_v_btn, {
                  size: "small",
                  variant: "tonal",
                  color: "primary",
                  loading: _unref$3(busy) === "保存分组",
                  onClick: saveGroup
                }, {
                  default: _withCtx$3(() => [..._cache[32] || (_cache[32] = [
                    _createTextVNode$3("保存 ", -1)
                  ])]),
                  _: 1
                }, 8, ["loading"]),
                _createVNode$3(_component_v_btn, {
                  size: "small",
                  variant: "text",
                  onClick: newGroup
                }, {
                  default: _withCtx$3(() => [..._cache[33] || (_cache[33] = [
                    _createTextVNode$3("清空", -1)
                  ])]),
                  _: 1
                })
              ]),
              _cache[34] || (_cache[34] = _createElementVNode$3("div", { class: "text-caption text-grey mt-1" }, " 分组里的音箱共用同一份队列、播放方式与定时，播放/切曲/音量/播报都会同步过去； 一台设备只属于一个组，选中别组已有的音箱时会自动从原组里挪过来，原组不足两台就随之解散。 默认音箱所在的那一组即「全屋广播」的落点。 ", -1))
            ]),
            _: 1
          })
        ], 64)) : _createCommentVNode$3("", true),
        _createVNode$3(_component_v_snackbar, {
          modelValue: _unref$3(snackbar).show,
          "onUpdate:modelValue": _cache[10] || (_cache[10] = ($event) => _unref$3(snackbar).show = $event),
          color: _unref$3(snackbar).color,
          timeout: "3500",
          location: "bottom"
        }, {
          default: _withCtx$3(() => [
            _createTextVNode$3(_toDisplayString$3(_unref$3(snackbar).text), 1)
          ]),
          _: 1
        }, 8, ["modelValue", "color"])
      ]);
    };
  }
});

const ConfigLogin = /* @__PURE__ */ _export_sfc(_sfc_main$3, [["__scopeId", "data-v-671d4a56"]]);

const {defineComponent:_defineComponent$2} = await importShared('vue');

const {resolveComponent:_resolveComponent$2,createVNode:_createVNode$2,toDisplayString:_toDisplayString$2,openBlock:_openBlock$2,createElementBlock:_createElementBlock$2,createCommentVNode:_createCommentVNode$2,createTextVNode:_createTextVNode$2,withCtx:_withCtx$2,unref:_unref$2,createElementVNode:_createElementVNode$2,renderList:_renderList$2,Fragment:_Fragment$2} = await importShared('vue');

const _hoisted_1$2 = { class: "xcc" };
const _hoisted_2$2 = { class: "xcc-title" };
const _hoisted_3$2 = {
  key: 0,
  class: "text-caption text-grey ml-2"
};
const _hoisted_4$2 = { class: "text-caption text-grey xcc-keywords" };
const _hoisted_5$2 = {
  key: 0,
  class: "text-caption text-grey pa-2"
};
const {ref: ref$2,onMounted: onMounted$2} = await importShared('vue');
const _sfc_main$2 = /* @__PURE__ */ _defineComponent$2({
  __name: "ConfigCommands",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" }
  },
  emits: ["changed"],
  setup(__props, { expose: __expose, emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const { busy, snackbar, notify, post, get } = useApi(props, () => emit("changed"));
    const rows = ref$2([]);
    const loaded = ref$2(false);
    async function load() {
      try {
        const res = await get("/commands");
        rows.value = res?.items || [];
        loaded.value = true;
      } catch (e) {
        notify(`口令表读取失败：${e?.message || e}`, "error");
      }
    }
    async function save() {
      const res = await post("/commands", {
        enabled: rows.value.filter((row) => row.enabled).map((row) => row.kind)
      }, "保存口令表");
      if (res?.items) rows.value = res.items;
    }
    onMounted$2(load);
    __expose({ load });
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent$2("v-icon");
      const _component_v_spacer = _resolveComponent$2("v-spacer");
      const _component_v_btn = _resolveComponent$2("v-btn");
      const _component_v_switch = _resolveComponent$2("v-switch");
      const _component_v_card = _resolveComponent$2("v-card");
      const _component_v_snackbar = _resolveComponent$2("v-snackbar");
      return _openBlock$2(), _createElementBlock$2("div", _hoisted_1$2, [
        _createElementVNode$2("div", _hoisted_2$2, [
          _createVNode$2(_component_v_icon, {
            icon: "mdi-microphone-message",
            size: "20",
            class: "xcc-mark"
          }),
          _cache[3] || (_cache[3] = _createTextVNode$2(" 语音口令表 ", -1)),
          loaded.value ? (_openBlock$2(), _createElementBlock$2("span", _hoisted_3$2, " 启用 " + _toDisplayString$2(rows.value.filter((r) => r.enabled).length) + "/" + _toDisplayString$2(rows.value.length) + " 类 ", 1)) : _createCommentVNode$2("", true),
          _createVNode$2(_component_v_spacer),
          _createVNode$2(_component_v_btn, {
            size: "x-small",
            variant: "text",
            class: "text-none",
            "prepend-icon": "mdi-refresh",
            onClick: load
          }, {
            default: _withCtx$2(() => [..._cache[1] || (_cache[1] = [
              _createTextVNode$2("重新读取 ", -1)
            ])]),
            _: 1
          }),
          _createVNode$2(_component_v_btn, {
            size: "small",
            variant: "tonal",
            color: "primary",
            loading: _unref$2(busy) === "保存口令表",
            "prepend-icon": "mdi-content-save",
            onClick: save
          }, {
            default: _withCtx$2(() => [..._cache[2] || (_cache[2] = [
              _createTextVNode$2("保存开关 ", -1)
            ])]),
            _: 1
          }, 8, ["loading"])
        ]),
        _cache[4] || (_cache[4] = _createElementVNode$2("div", { class: "text-caption text-grey mb-2" }, " 关掉某一类，那句话就不会被接手（会留给小爱自己的回答）。「停止播放」这类保底口令不受这里影响。 监听开关、轮询间隔与 AI 兜底在上面「语音控制」那组里改。 ", -1)),
        _createVNode$2(_component_v_card, {
          variant: "outlined",
          class: "xcc-card pa-2"
        }, {
          default: _withCtx$2(() => [
            (_openBlock$2(true), _createElementBlock$2(_Fragment$2, null, _renderList$2(rows.value, (row) => {
              return _openBlock$2(), _createElementBlock$2("div", {
                key: row.kind,
                class: "xcc-row"
              }, [
                _createVNode$2(_component_v_switch, {
                  modelValue: row.enabled,
                  "onUpdate:modelValue": ($event) => row.enabled = $event,
                  color: "primary",
                  "hide-details": "",
                  inset: "",
                  density: "compact",
                  label: row.label,
                  style: { "flex": "0 0 168px" }
                }, null, 8, ["modelValue", "onUpdate:modelValue", "label"]),
                _createElementVNode$2("span", _hoisted_4$2, _toDisplayString$2(row.keywords.join("、")), 1)
              ]);
            }), 128)),
            !rows.value.length ? (_openBlock$2(), _createElementBlock$2("div", _hoisted_5$2, _toDisplayString$2(loaded.value ? "后端没有给出口令表。" : "正在读取口令表…"), 1)) : _createCommentVNode$2("", true)
          ]),
          _: 1
        }),
        _createVNode$2(_component_v_snackbar, {
          modelValue: _unref$2(snackbar).show,
          "onUpdate:modelValue": _cache[0] || (_cache[0] = ($event) => _unref$2(snackbar).show = $event),
          color: _unref$2(snackbar).color,
          timeout: "3500",
          location: "bottom"
        }, {
          default: _withCtx$2(() => [
            _createTextVNode$2(_toDisplayString$2(_unref$2(snackbar).text), 1)
          ]),
          _: 1
        }, 8, ["modelValue", "color"])
      ]);
    };
  }
});

const ConfigCommands = /* @__PURE__ */ _export_sfc(_sfc_main$2, [["__scopeId", "data-v-c900b981"]]);

const {defineComponent:_defineComponent$1} = await importShared('vue');

const {resolveComponent:_resolveComponent$1,createVNode:_createVNode$1,toDisplayString:_toDisplayString$1,createElementVNode:_createElementVNode$1,createTextVNode:_createTextVNode$1,withCtx:_withCtx$1,renderList:_renderList$1,Fragment:_Fragment$1,openBlock:_openBlock$1,createElementBlock:_createElementBlock$1,unref:_unref$1,createCommentVNode:_createCommentVNode$1,createBlock:_createBlock$1} = await importShared('vue');

const _hoisted_1$1 = { class: "xct" };
const _hoisted_2$1 = { class: "xct-title" };
const _hoisted_3$1 = { class: "text-caption text-grey ml-2" };
const _hoisted_4$1 = {
  class: "d-flex align-center",
  style: { "gap": "10px", "flex-wrap": "wrap" }
};
const _hoisted_5$1 = { style: { "min-width": "0", "flex": "1 1 220px" } };
const _hoisted_6$1 = { class: "text-body-2" };
const _hoisted_7$1 = { class: "text-caption text-grey" };
const _hoisted_8$1 = {
  key: 0,
  class: "text-caption text-grey mb-2"
};
const _hoisted_9$1 = { class: "xct-sub" };
const _hoisted_10$1 = {
  class: "d-flex",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_11$1 = { class: "xct-days mt-2" };
const _hoisted_12$1 = {
  class: "d-flex align-center mt-2",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_13$1 = {
  key: 0,
  class: "d-flex mt-2",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const _hoisted_14$1 = {
  class: "d-flex mt-2",
  style: { "gap": "8px", "flex-wrap": "wrap" }
};
const {ref: ref$1,computed: computed$1,onMounted: onMounted$1} = await importShared('vue');
const _sfc_main$1 = /* @__PURE__ */ _defineComponent$1({
  __name: "ConfigTasks",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    devices: { type: Array, default: () => [] }
  },
  emits: ["changed"],
  setup(__props, { expose: __expose, emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const { busy, snackbar, notify, post, get } = useApi(props, () => emit("changed"));
    const tasks = ref$1([]);
    const playlists = ref$1([]);
    const taskActions = TASK_ACTIONS;
    const weekDays = WEEK_DAYS;
    const monthDays = Array.from({ length: 31 }, (_unused, i) => i + 1);
    const deviceItems = computed$1(() => props.devices.map((item) => ({
      title: item.name || item.alias || item.device_id,
      value: item.device_id
    })));
    function emptyTask() {
      return {
        name: "",
        enabled: true,
        action: "play_playlist",
        schedule: { type: "weekly", time: "07:00", weekdays: [1, 2, 3, 4, 5], monthdays: [1] },
        target: { all: false, device_ids: [] },
        params: {
          playlist_name: "",
          song_name: "",
          start: "first",
          play_mode: "",
          volume: null,
          stop_after_minutes: null
        }
      };
    }
    const form = ref$1(emptyTask());
    const editingTask = ref$1("");
    function newTask() {
      editingTask.value = "";
      form.value = emptyTask();
    }
    function editTask(row) {
      editingTask.value = row.id;
      form.value = {
        name: row.name,
        enabled: row.enabled,
        action: row.action,
        schedule: {
          type: row.schedule?.type || "weekly",
          time: row.schedule?.time || "07:00",
          weekdays: [...row.schedule?.weekdays || []],
          monthdays: [...row.schedule?.monthdays || []]
        },
        target: { all: !!row.target?.all, device_ids: [...row.target?.device_ids || []] },
        params: {
          playlist_name: row.params?.playlist_name || "",
          song_name: row.params?.song_name || "",
          start: row.params?.start || "first",
          play_mode: row.params?.play_mode || "",
          volume: row.params?.volume ?? null,
          stop_after_minutes: row.params?.stop_after_minutes ?? null
        }
      };
    }
    function toggleIn(list, value) {
      const at = list.indexOf(value);
      if (at >= 0) list.splice(at, 1);
      else list.push(value);
    }
    async function load() {
      try {
        const [taskRes, playlistRes] = await Promise.all([get("/tasks"), get("/playlists")]);
        tasks.value = taskRes?.items || [];
        playlists.value = playlistRes?.items || [];
      } catch (e) {
        notify(`定时任务读取失败：${e?.message || e}`, "error");
      }
    }
    async function saveTask() {
      const draft = form.value;
      if (!draft.name.trim()) {
        notify("先给任务起个名字", "warning");
        return;
      }
      if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(draft.schedule.time.trim())) {
        notify("时间要写成 HH:MM，比如 07:00", "warning");
        return;
      }
      const key = draft.schedule.type === "monthly" ? "monthdays" : "weekdays";
      if (!draft.schedule[key].length) {
        notify(draft.schedule.type === "monthly" ? "勾出每月哪几天" : "勾出每周哪几天", "warning");
        return;
      }
      if (taskNeedsPlaylist(draft.action) && !draft.params.playlist_name.trim()) {
        notify("播放歌单的任务要先选中歌单（歌单来自曲库索引）", "warning");
        return;
      }
      const res = await post("/tasks", {
        id: editingTask.value || void 0,
        name: draft.name.trim(),
        enabled: draft.enabled,
        action: draft.action,
        schedule: {
          type: draft.schedule.type,
          time: draft.schedule.time.trim(),
          weekdays: [...draft.schedule.weekdays].sort((a, b) => a - b),
          monthdays: [...draft.schedule.monthdays].sort((a, b) => a - b)
        },
        target: { all: draft.target.all, device_ids: draft.target.all ? [] : [...draft.target.device_ids] },
        params: { ...draft.params }
      }, "保存定时任务");
      if (res && res.ok !== false) {
        editingTask.value = "";
        form.value = emptyTask();
        await load();
      }
    }
    function removeTask(row) {
      return post("/tasks", { id: row.id, remove: true }, "删除定时任务").then(async () => {
        if (editingTask.value === row.id) newTask();
        await load();
      });
    }
    function toggleTask(row) {
      return post("/tasks", { ...row, enabled: !row.enabled }, row.enabled ? "停用任务" : "启用任务");
    }
    onMounted$1(load);
    __expose({ load });
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent$1("v-icon");
      const _component_v_spacer = _resolveComponent$1("v-spacer");
      const _component_v_btn = _resolveComponent$1("v-btn");
      const _component_v_switch = _resolveComponent$1("v-switch");
      const _component_v_chip = _resolveComponent$1("v-chip");
      const _component_v_card = _resolveComponent$1("v-card");
      const _component_v_text_field = _resolveComponent$1("v-text-field");
      const _component_v_select = _resolveComponent$1("v-select");
      const _component_v_btn_toggle = _resolveComponent$1("v-btn-toggle");
      const _component_v_checkbox = _resolveComponent$1("v-checkbox");
      const _component_v_combobox = _resolveComponent$1("v-combobox");
      const _component_v_snackbar = _resolveComponent$1("v-snackbar");
      return _openBlock$1(), _createElementBlock$1("div", _hoisted_1$1, [
        _createElementVNode$1("div", _hoisted_2$1, [
          _createVNode$1(_component_v_icon, {
            icon: "mdi-clock-outline",
            size: "20",
            class: "xct-mark"
          }),
          _cache[15] || (_cache[15] = _createTextVNode$1(" 定时任务 ", -1)),
          _createElementVNode$1("span", _hoisted_3$1, _toDisplayString$1(tasks.value.length) + " 条", 1),
          _createVNode$1(_component_v_spacer),
          _createVNode$1(_component_v_btn, {
            size: "x-small",
            variant: "text",
            class: "text-none",
            "prepend-icon": "mdi-refresh",
            onClick: load
          }, {
            default: _withCtx$1(() => [..._cache[14] || (_cache[14] = [
              _createTextVNode$1("重新读取 ", -1)
            ])]),
            _: 1
          })
        ]),
        _cache[22] || (_cache[22] = _createElementVNode$1("div", { class: "text-caption text-grey mb-2" }, " 到点由插件的周期巡检触发（20 秒一次，同一分钟只跑一次）。执行情况、日志和「立即执行」在侧栏的 「定时任务」栏看；这里只管怎么定义。播放类任务要音箱在线、且配好了「外部访问地址」。 ", -1)),
        (_openBlock$1(true), _createElementBlock$1(_Fragment$1, null, _renderList$1(tasks.value, (row) => {
          return _openBlock$1(), _createBlock$1(_component_v_card, {
            key: row.id,
            variant: "outlined",
            class: "xct-card pa-3 mb-2"
          }, {
            default: _withCtx$1(() => [
              _createElementVNode$1("div", _hoisted_4$1, [
                _createVNode$1(_component_v_switch, {
                  "model-value": row.enabled,
                  "hide-details": "",
                  density: "compact",
                  color: "primary",
                  loading: _unref$1(busy) === "启用任务" || _unref$1(busy) === "停用任务",
                  "onUpdate:modelValue": ($event) => toggleTask(row)
                }, null, 8, ["model-value", "loading", "onUpdate:modelValue"]),
                _createElementVNode$1("div", _hoisted_5$1, [
                  _createElementVNode$1("div", _hoisted_6$1, [
                    _createTextVNode$1(_toDisplayString$1(row.name) + " ", 1),
                    _createVNode$1(_component_v_chip, {
                      size: "x-small",
                      variant: "tonal",
                      class: "ml-1"
                    }, {
                      default: _withCtx$1(() => [
                        _createTextVNode$1(_toDisplayString$1(row.action_text), 1)
                      ]),
                      _: 2
                    }, 1024)
                  ]),
                  _createElementVNode$1("div", _hoisted_7$1, [
                    _createTextVNode$1(_toDisplayString$1(row.rule) + "｜" + _toDisplayString$1(row.target.all ? "全部在线音箱" : row.target.device_ids.length ? `${row.target.device_ids.length} 台指定音箱` : "默认音箱") + " ", 1),
                    _unref$1(taskNeedsPlaylist)(row.action) ? (_openBlock$1(), _createElementBlock$1(_Fragment$1, { key: 0 }, [
                      _createTextVNode$1(" ｜歌单 " + _toDisplayString$1(row.params?.playlist_name || row.params?.playlist_id || "（未选）"), 1)
                    ], 64)) : _createCommentVNode$1("", true)
                  ])
                ]),
                _createVNode$1(_component_v_btn, {
                  size: "x-small",
                  variant: "text",
                  onClick: ($event) => editTask(row)
                }, {
                  default: _withCtx$1(() => [..._cache[16] || (_cache[16] = [
                    _createTextVNode$1("编辑", -1)
                  ])]),
                  _: 1
                }, 8, ["onClick"]),
                _createVNode$1(_component_v_btn, {
                  size: "x-small",
                  variant: "text",
                  color: "error",
                  loading: _unref$1(busy) === "删除定时任务",
                  onClick: ($event) => removeTask(row)
                }, {
                  default: _withCtx$1(() => [..._cache[17] || (_cache[17] = [
                    _createTextVNode$1("删除 ", -1)
                  ])]),
                  _: 1
                }, 8, ["loading", "onClick"])
              ])
            ]),
            _: 2
          }, 1024);
        }), 128)),
        !tasks.value.length ? (_openBlock$1(), _createElementBlock$1("div", _hoisted_8$1, "还没有任务，下面建一条。")) : _createCommentVNode$1("", true),
        _createVNode$1(_component_v_card, {
          variant: "outlined",
          class: "xct-card pa-3"
        }, {
          default: _withCtx$1(() => [
            _createElementVNode$1("div", _hoisted_9$1, _toDisplayString$1(editingTask.value ? `编辑任务「${editingTask.value}」` : "新建任务"), 1),
            _createElementVNode$1("div", _hoisted_10$1, [
              _createVNode$1(_component_v_text_field, {
                modelValue: form.value.name,
                "onUpdate:modelValue": _cache[0] || (_cache[0] = ($event) => form.value.name = $event),
                density: "compact",
                "hide-details": "",
                clearable: "",
                label: "任务名",
                placeholder: "早上起床放歌",
                style: { "min-width": "180px", "flex": "1 1 180px" }
              }, null, 8, ["modelValue"]),
              _createVNode$1(_component_v_select, {
                modelValue: form.value.action,
                "onUpdate:modelValue": _cache[1] || (_cache[1] = ($event) => form.value.action = $event),
                items: _unref$1(taskActions),
                density: "compact",
                "hide-details": "",
                label: "做什么",
                style: { "min-width": "160px" }
              }, null, 8, ["modelValue", "items"]),
              _createVNode$1(_component_v_text_field, {
                modelValue: form.value.schedule.time,
                "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => form.value.schedule.time = $event),
                density: "compact",
                "hide-details": "",
                label: "时间 HH:MM",
                placeholder: "07:00",
                style: { "width": "110px" }
              }, null, 8, ["modelValue"]),
              _createVNode$1(_component_v_btn_toggle, {
                modelValue: form.value.schedule.type,
                "onUpdate:modelValue": _cache[3] || (_cache[3] = ($event) => form.value.schedule.type = $event),
                density: "compact",
                mandatory: ""
              }, {
                default: _withCtx$1(() => [
                  _createVNode$1(_component_v_btn, {
                    value: "weekly",
                    size: "small",
                    variant: "text"
                  }, {
                    default: _withCtx$1(() => [..._cache[18] || (_cache[18] = [
                      _createTextVNode$1("按星期", -1)
                    ])]),
                    _: 1
                  }),
                  _createVNode$1(_component_v_btn, {
                    value: "monthly",
                    size: "small",
                    variant: "text"
                  }, {
                    default: _withCtx$1(() => [..._cache[19] || (_cache[19] = [
                      _createTextVNode$1("按几号", -1)
                    ])]),
                    _: 1
                  })
                ]),
                _: 1
              }, 8, ["modelValue"]),
              _createVNode$1(_component_v_checkbox, {
                modelValue: form.value.enabled,
                "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => form.value.enabled = $event),
                label: "启用",
                density: "compact",
                "hide-details": ""
              }, null, 8, ["modelValue"])
            ]),
            _createElementVNode$1("div", _hoisted_11$1, [
              form.value.schedule.type === "weekly" ? (_openBlock$1(true), _createElementBlock$1(_Fragment$1, { key: 0 }, _renderList$1(_unref$1(weekDays), (label, day) => {
                return _openBlock$1(), _createBlock$1(_component_v_chip, {
                  key: day,
                  size: "small",
                  variant: form.value.schedule.weekdays.includes(day) ? "flat" : "outlined",
                  color: form.value.schedule.weekdays.includes(day) ? "primary" : "default",
                  onClick: ($event) => toggleIn(form.value.schedule.weekdays, day)
                }, {
                  default: _withCtx$1(() => [
                    _createTextVNode$1(_toDisplayString$1(label), 1)
                  ]),
                  _: 2
                }, 1032, ["variant", "color", "onClick"]);
              }), 128)) : (_openBlock$1(true), _createElementBlock$1(_Fragment$1, { key: 1 }, _renderList$1(_unref$1(monthDays), (day) => {
                return _openBlock$1(), _createBlock$1(_component_v_chip, {
                  key: day,
                  size: "small",
                  variant: form.value.schedule.monthdays.includes(day) ? "flat" : "outlined",
                  color: form.value.schedule.monthdays.includes(day) ? "primary" : "default",
                  onClick: ($event) => toggleIn(form.value.schedule.monthdays, day)
                }, {
                  default: _withCtx$1(() => [
                    _createTextVNode$1(_toDisplayString$1(day), 1)
                  ]),
                  _: 2
                }, 1032, ["variant", "color", "onClick"]);
              }), 128))
            ]),
            _createElementVNode$1("div", _hoisted_12$1, [
              _createVNode$1(_component_v_checkbox, {
                modelValue: form.value.target.all,
                "onUpdate:modelValue": _cache[5] || (_cache[5] = ($event) => form.value.target.all = $event),
                label: "全部在线音箱",
                density: "compact",
                "hide-details": ""
              }, null, 8, ["modelValue"]),
              !form.value.target.all ? (_openBlock$1(), _createBlock$1(_component_v_combobox, {
                key: 0,
                modelValue: form.value.target.device_ids,
                "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => form.value.target.device_ids = $event),
                items: deviceItems.value,
                "item-title": "title",
                "item-value": "value",
                density: "compact",
                "hide-details": "",
                multiple: "",
                chips: "",
                "closable-chips": "",
                label: "指定音箱（留空=默认音箱）",
                style: { "min-width": "240px", "flex": "1 1 240px" }
              }, null, 8, ["modelValue", "items"])) : _createCommentVNode$1("", true)
            ]),
            _unref$1(taskNeedsPlaylist)(form.value.action) ? (_openBlock$1(), _createElementBlock$1("div", _hoisted_13$1, [
              _createVNode$1(_component_v_combobox, {
                modelValue: form.value.params.playlist_name,
                "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => form.value.params.playlist_name = $event),
                items: playlists.value.map((p) => p.name),
                density: "compact",
                "hide-details": "",
                clearable: "",
                label: "歌单（来自曲库索引）",
                placeholder: "范特西",
                style: { "min-width": "200px", "flex": "1 1 200px" }
              }, null, 8, ["modelValue", "items"]),
              form.value.action === "play_playlist_from" ? (_openBlock$1(), _createBlock$1(_component_v_text_field, {
                key: 0,
                modelValue: form.value.params.song_name,
                "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => form.value.params.song_name = $event),
                density: "compact",
                "hide-details": "",
                clearable: "",
                label: "从这首歌开始",
                placeholder: "稻香",
                style: { "min-width": "160px", "flex": "1 1 160px" }
              }, null, 8, ["modelValue"])) : _createCommentVNode$1("", true),
              form.value.action === "play_playlist" ? (_openBlock$1(), _createBlock$1(_component_v_select, {
                key: 1,
                modelValue: form.value.params.start,
                "onUpdate:modelValue": _cache[9] || (_cache[9] = ($event) => form.value.params.start = $event),
                items: [{ title: "从第一首", value: "first" }, { title: "随机起点", value: "random" }],
                density: "compact",
                "hide-details": "",
                label: "起点",
                style: { "width": "130px" }
              }, null, 8, ["modelValue"])) : _createCommentVNode$1("", true)
            ])) : _createCommentVNode$1("", true),
            _createElementVNode$1("div", _hoisted_14$1, [
              _createVNode$1(_component_v_select, {
                modelValue: form.value.params.play_mode,
                "onUpdate:modelValue": _cache[10] || (_cache[10] = ($event) => form.value.params.play_mode = $event),
                items: [{ title: "跟随上次", value: "" }, ..._unref$1(MODE_ITEMS)],
                density: "compact",
                "hide-details": "",
                label: "播放方式",
                style: { "min-width": "150px" }
              }, null, 8, ["modelValue", "items"]),
              form.value.action === "set_volume" || _unref$1(taskNeedsPlaylist)(form.value.action) ? (_openBlock$1(), _createBlock$1(_component_v_text_field, {
                key: 0,
                modelValue: form.value.params.volume,
                "onUpdate:modelValue": _cache[11] || (_cache[11] = ($event) => form.value.params.volume = $event),
                modelModifiers: { number: true },
                type: "number",
                min: "0",
                max: "100",
                density: "compact",
                "hide-details": "",
                label: "音量 0-100",
                style: { "width": "120px" }
              }, null, 8, ["modelValue"])) : _createCommentVNode$1("", true),
              _unref$1(taskNeedsPlaylist)(form.value.action) ? (_openBlock$1(), _createBlock$1(_component_v_text_field, {
                key: 1,
                modelValue: form.value.params.stop_after_minutes,
                "onUpdate:modelValue": _cache[12] || (_cache[12] = ($event) => form.value.params.stop_after_minutes = $event),
                modelModifiers: { number: true },
                type: "number",
                min: "0",
                density: "compact",
                "hide-details": "",
                label: "播多久后停（分钟）",
                style: { "width": "160px" }
              }, null, 8, ["modelValue"])) : _createCommentVNode$1("", true),
              _createVNode$1(_component_v_btn, {
                color: "primary",
                variant: "flat",
                class: "text-none",
                size: "small",
                loading: _unref$1(busy) === "保存定时任务",
                onClick: saveTask
              }, {
                default: _withCtx$1(() => [
                  _createTextVNode$1(_toDisplayString$1(editingTask.value ? "保存修改" : "创建任务"), 1)
                ]),
                _: 1
              }, 8, ["loading"]),
              _createVNode$1(_component_v_btn, {
                size: "small",
                variant: "text",
                onClick: newTask
              }, {
                default: _withCtx$1(() => [..._cache[20] || (_cache[20] = [
                  _createTextVNode$1("清空", -1)
                ])]),
                _: 1
              })
            ]),
            _cache[21] || (_cache[21] = _createElementVNode$1("div", { class: "text-caption text-grey mt-1" }, " 法定节假日的调休判定没有做（上游把假期表编在插件产物里，插件装在宿主容器里没有那份数据）， 要「只工作日 / 只周末」就分建两条任务勾不同的周几。 ", -1))
          ]),
          _: 1
        }),
        _createVNode$1(_component_v_snackbar, {
          modelValue: _unref$1(snackbar).show,
          "onUpdate:modelValue": _cache[13] || (_cache[13] = ($event) => _unref$1(snackbar).show = $event),
          color: _unref$1(snackbar).color,
          timeout: "3500",
          location: "bottom"
        }, {
          default: _withCtx$1(() => [
            _createTextVNode$1(_toDisplayString$1(_unref$1(snackbar).text), 1)
          ]),
          _: 1
        }, 8, ["modelValue", "color"])
      ]);
    };
  }
});

const ConfigTasks = /* @__PURE__ */ _export_sfc(_sfc_main$1, [["__scopeId", "data-v-dd8a850a"]]);

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,mergeProps:_mergeProps,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,createElementBlock:_createElementBlock,renderList:_renderList,Fragment:_Fragment,normalizeClass:_normalizeClass,unref:_unref,vShow:_vShow,withDirectives:_withDirectives,Transition:_Transition} = await importShared('vue');

const _hoisted_1 = { class: "xc" };
const _hoisted_2 = { class: "xc-head" };
const _hoisted_3 = { class: "xc-head-side" };
const _hoisted_4 = { class: "xc-menu-row" };
const _hoisted_5 = { class: "xc-menu-row" };
const _hoisted_6 = {
  key: 0,
  class: "xc-dot",
  title: "有未保存的改动"
};
const _hoisted_7 = { class: "xc-shell" };
const _hoisted_8 = { class: "xc-rail" };
const _hoisted_9 = ["onClick"];
const _hoisted_10 = { class: "xc-rail-title" };
const _hoisted_11 = {
  key: 1,
  class: "xc-rail-badge xc-rail-badge-dirty",
  title: "本页有未保存的改动"
};
const _hoisted_12 = { class: "xc-main" };
const _hoisted_13 = {
  key: 0,
  class: "xc-pane-head"
};
const _hoisted_14 = { class: "xc-pane-name" };
const _hoisted_15 = { class: "xc-pane-desc" };
const _hoisted_16 = { class: "text-body-2" };
const _hoisted_17 = { class: "xc-pane" };
const {computed,ref,onMounted,onBeforeUnmount} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Config",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    // 宿主存的配置不作为初值：密码不在里面，而 /config 是「缺省即保留」的语义，
    // 拿一份不含密码的草稿去保存会把已存的账密一起动到。各面板自己从服务端起头。
    initialConfig: { type: Object, default: () => ({}) },
    nativeSubscribe: { type: Function, default: null },
    // 侧栏全页自己开的对话框要有「关闭」，宿主开的对话框自己有关闭按钮
    showClose: { type: Boolean, default: false },
    // 宿主配置对话框里「切到播放台」才有效；侧栏对话框本来就在播放台上
    showSwitch: { type: Boolean, default: false }
  },
  emits: ["save", "close", "switch", "layout", "updated"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const { snackbar, notify, get } = useApi(props, () => loadStatus());
    const status = ref(null);
    const loaded = ref(false);
    const panes = [
      {
        key: "account",
        title: "账号与音箱",
        icon: "mdi-account-sync-outline",
        desc: "登录小米账号（账密 / 扫码 / Cookie）、选默认音箱与设备分组"
      },
      {
        key: "library",
        title: "曲库与直链",
        icon: "mdi-folder-music-outline",
        desc: "哪些目录算音乐库、音箱从哪个地址取流、什么格式能播"
      },
      {
        key: "play",
        title: "播放行为",
        icon: "mdi-sine-wave",
        desc: "走哪条播放接口、推之前设音量、连播与灯效"
      },
      {
        key: "voice",
        title: "语音与 AI",
        icon: "mdi-microphone-message",
        desc: "监听哪几台音箱的对话、命中后怎么打断与播报、AI 兜底"
      },
      {
        key: "commands",
        title: "语音口令表",
        icon: "mdi-key-variant",
        desc: "每一类口令开不开放，以及它认哪些说法"
      },
      {
        key: "tasks",
        title: "定时任务",
        icon: "mdi-clock-outline",
        desc: "到点播什么、哪几天、哪台音箱、什么播放方式"
      }
    ];
    const pane = ref("account");
    const current = computed(() => panes.find((item) => item.key === pane.value) || panes[0]);
    const formPanes = ["library", "play", "voice"];
    const isFormPane = computed(() => formPanes.includes(pane.value));
    const devices = computed(() => status.value?.devices || []);
    const gateReady = computed(() => !!status.value?.logged_in && !!devices.value.length);
    const cfgForm = ref(null);
    const gate = computed(() => cfgForm.value?.form || null);
    const dirty = computed(() => !!cfgForm.value?.dirty);
    const saving = computed(() => !!cfgForm.value?.saving);
    const formCfg = computed(() => cfgForm.value?.cfg || null);
    const paneBadge = (key) => {
      if (key === "account") {
        if (!loaded.value) return null;
        return status.value?.logged_in ? { text: `${devices.value.length} 台`, tone: "ok" } : { text: "未登录", tone: "warn" };
      }
      if (key === "library") {
        const dirs = formCfg.value?.dir_count;
        return dirs ? { text: `${dirs} 目录`, tone: "info" } : { text: "没库", tone: "warn" };
      }
      if (key === "play") return { text: gate.value?.play_mode || "auto", tone: "info" };
      if (key === "voice") return gate.value?.voice_enabled ? { text: "监听中", tone: "ok" } : null;
      return null;
    };
    function setGate(key, value) {
      if (gate.value) gate.value[key] = value;
    }
    async function loadStatus() {
      try {
        status.value = await get("/status");
        emit("updated");
      } catch (e) {
        notify(`状态读取失败：${e?.message || e}`, "error");
      } finally {
        loaded.value = true;
      }
    }
    async function reloadAll() {
      await Promise.all([loadStatus(), cfgForm.value?.loadConfig()]);
    }
    async function saveForm() {
      await cfgForm.value?.save();
      loadStatus();
    }
    function onKeydown(event) {
      if (!(event.metaKey || event.ctrlKey)) return;
      if (event.key.toLowerCase() !== "s") return;
      event.preventDefault();
      if (dirty.value && !saving.value) saveForm();
    }
    function onChanged() {
      loadStatus();
    }
    onMounted(() => {
      emit("layout", { maxWidth: "72rem" });
      loadStatus();
      window.addEventListener("keydown", onKeydown);
    });
    onBeforeUnmount(() => window.removeEventListener("keydown", onKeydown));
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_avatar = _resolveComponent("v-avatar");
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_switch = _resolveComponent("v-switch");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_menu = _resolveComponent("v-menu");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_divider = _resolveComponent("v-divider");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock(), _createElementBlock("div", _hoisted_1, [
        _createElementVNode("div", _hoisted_2, [
          _createVNode(_component_v_avatar, {
            color: "primary",
            variant: "tonal",
            rounded: "lg",
            size: "38",
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
          _cache[13] || (_cache[13] = _createElementVNode("div", { class: "xc-head-text" }, [
            _createElementVNode("div", { class: "xc-title" }, "小爱音箱 · 配置"),
            _createElementVNode("div", { class: "xc-sub" }, "六栏分开改，保存一次生效；带「未保存」记号的是本页那份表单")
          ], -1)),
          _createVNode(_component_v_spacer),
          _createElementVNode("div", _hoisted_3, [
            gate.value ? (_openBlock(), _createBlock(_component_v_menu, {
              key: 0,
              offset: "8",
              location: "bottom end",
              "min-width": "290",
              "shade-elevation": "8"
            }, {
              activator: _withCtx(({ props: menuProps }) => [
                _createVNode(_component_v_chip, _mergeProps(menuProps, {
                  size: "small",
                  variant: "tonal",
                  color: gate.value.enabled ? gate.value.show_sidebar_nav ? "success" : "info" : "grey",
                  "prepend-icon": gate.value.enabled ? "mdi-toggle-switch-outline" : "mdi-toggle-switch-off-outline"
                }), {
                  default: _withCtx(() => [
                    _createTextVNode(_toDisplayString(cfgForm.value?.navText), 1)
                  ]),
                  _: 1
                }, 16, ["color", "prepend-icon"])
              ]),
              default: _withCtx(() => [
                _createVNode(_component_v_card, {
                  variant: "outlined",
                  class: "xc-menu pa-3"
                }, {
                  default: _withCtx(() => [
                    _createElementVNode("label", _hoisted_4, [
                      _createVNode(_component_v_switch, {
                        "model-value": gate.value.enabled,
                        color: "primary",
                        "hide-details": "",
                        inset: "",
                        density: "compact",
                        "onUpdate:modelValue": _cache[0] || (_cache[0] = (v) => setGate("enabled", !!v))
                      }, null, 8, ["model-value"]),
                      _cache[7] || (_cache[7] = _createElementVNode("span", null, [
                        _createElementVNode("b", null, "启用插件"),
                        _createElementVNode("em", null, "侧栏入口、播放台巡检、语音轮询都按这一项准入")
                      ], -1))
                    ]),
                    _createElementVNode("label", _hoisted_5, [
                      _createVNode(_component_v_switch, {
                        "model-value": gate.value.show_sidebar_nav,
                        disabled: !gate.value.enabled,
                        color: "primary",
                        "hide-details": "",
                        inset: "",
                        density: "compact",
                        "onUpdate:modelValue": _cache[1] || (_cache[1] = (v) => setGate("show_sidebar_nav", !!v))
                      }, null, 8, ["model-value", "disabled"]),
                      _cache[8] || (_cache[8] = _createElementVNode("span", null, [
                        _createElementVNode("b", null, "在「发现」显示侧栏入口"),
                        _createElementVNode("em", null, "关掉后仍可用插件管理页打开，只是不收进侧栏")
                      ], -1))
                    ]),
                    _cache[9] || (_cache[9] = _createElementVNode("div", { class: "xc-menu-note" }, [
                      _createTextVNode(" 宿主眼里的「插件已启用」就是这一项 —— "),
                      _createElementVNode("b", null, "插件管理页那个开关改的是宿主自己的实例记录， 碰不到这里"),
                      _createTextVNode("。只装好插件不在这里打开的话，侧栏永远不会出现。保存后立刻生效，不用重启主程序。 ")
                    ], -1))
                  ]),
                  _: 1
                })
              ]),
              _: 1
            })) : _createCommentVNode("", true),
            _createVNode(_component_v_btn, {
              size: "small",
              variant: "text",
              class: "text-none",
              icon: "mdi-refresh",
              loading: !loaded.value,
              onClick: reloadAll,
              title: "重新读取"
            }, null, 8, ["loading"]),
            _createVNode(_component_v_btn, {
              size: "small",
              color: "primary",
              variant: "flat",
              class: "text-none",
              loading: saving.value,
              disabled: !dirty.value,
              onClick: saveForm
            }, {
              prepend: _withCtx(() => [
                _createVNode(_component_v_icon, {
                  icon: "mdi-content-save",
                  size: "18"
                })
              ]),
              default: _withCtx(() => [
                _cache[10] || (_cache[10] = _createTextVNode(" 保存配置 ", -1)),
                dirty.value ? (_openBlock(), _createElementBlock("span", _hoisted_6)) : _createCommentVNode("", true)
              ]),
              _: 1
            }, 8, ["loading", "disabled"]),
            props.showSwitch ? (_openBlock(), _createBlock(_component_v_btn, {
              key: 1,
              size: "small",
              variant: "text",
              class: "text-none",
              "prepend-icon": "mdi-speaker",
              onClick: _cache[2] || (_cache[2] = ($event) => emit("switch"))
            }, {
              default: _withCtx(() => [..._cache[11] || (_cache[11] = [
                _createTextVNode("播放台 ", -1)
              ])]),
              _: 1
            })) : _createCommentVNode("", true),
            props.showClose ? (_openBlock(), _createBlock(_component_v_btn, {
              key: 2,
              size: "small",
              variant: "text",
              class: "text-none",
              "prepend-icon": "mdi-close",
              onClick: _cache[3] || (_cache[3] = ($event) => emit("close"))
            }, {
              default: _withCtx(() => [..._cache[12] || (_cache[12] = [
                _createTextVNode("关闭 ", -1)
              ])]),
              _: 1
            })) : _createCommentVNode("", true)
          ])
        ]),
        _createVNode(_component_v_divider),
        _createElementVNode("div", _hoisted_7, [
          _createElementVNode("nav", _hoisted_8, [
            (_openBlock(), _createElementBlock(_Fragment, null, _renderList(panes, (item) => {
              return _createElementVNode("button", {
                key: item.key,
                type: "button",
                class: _normalizeClass(["xc-rail-item", { "is-active": pane.value === item.key }]),
                onClick: ($event) => pane.value = item.key
              }, [
                _createVNode(_component_v_icon, {
                  icon: item.icon,
                  size: "17",
                  class: "xc-rail-icon"
                }, null, 8, ["icon"]),
                _createElementVNode("span", _hoisted_10, _toDisplayString(item.title), 1),
                paneBadge(item.key) ? (_openBlock(), _createElementBlock("span", {
                  key: 0,
                  class: _normalizeClass(["xc-rail-badge", `xc-rail-badge-${paneBadge(item.key).tone}`])
                }, _toDisplayString(paneBadge(item.key).text), 3)) : ["library", "play", "voice"].includes(item.key) && dirty.value ? (_openBlock(), _createElementBlock("span", _hoisted_11, "未保存")) : _createCommentVNode("", true)
              ], 10, _hoisted_9);
            }), 64))
          ]),
          _createElementVNode("section", _hoisted_12, [
            isFormPane.value ? (_openBlock(), _createElementBlock("div", _hoisted_13, [
              _createElementVNode("span", _hoisted_14, _toDisplayString(current.value.title), 1),
              _createElementVNode("span", _hoisted_15, _toDisplayString(current.value.desc), 1)
            ])) : _createCommentVNode("", true),
            !gateReady.value && pane.value !== "account" ? (_openBlock(), _createBlock(_component_v_alert, {
              key: 1,
              class: "xc-gate",
              variant: "tonal",
              type: loaded.value ? "warning" : "info",
              density: "compact"
            }, {
              default: _withCtx(() => [
                _createElementVNode("div", _hoisted_16, _toDisplayString(loaded.value ? "还没登录，或登录后没取到音箱：这一栏改完也得等第 1 步做完才有实际效果" : "正在读取登录状态…"), 1),
                loaded.value ? (_openBlock(), _createBlock(_component_v_btn, {
                  key: 0,
                  size: "x-small",
                  variant: "text",
                  class: "text-none mt-1",
                  "prepend-icon": "mdi-arrow-left",
                  onClick: _cache[4] || (_cache[4] = ($event) => pane.value = "account")
                }, {
                  default: _withCtx(() => [..._cache[14] || (_cache[14] = [
                    _createTextVNode("去「账号与音箱」 ", -1)
                  ])]),
                  _: 1
                })) : _createCommentVNode("", true)
              ]),
              _: 1
            }, 8, ["type"])) : _createCommentVNode("", true),
            _withDirectives(_createElementVNode("div", _hoisted_17, [
              _createVNode(ConfigForm, {
                ref_key: "cfgForm",
                ref: cfgForm,
                api: props.api,
                "plugin-id": props.pluginId,
                "source-plugin-id": props.sourcePluginId,
                pane: pane.value,
                devices: devices.value,
                onToast: _unref(notify),
                onSaved: onChanged,
                onLoaded: _cache[5] || (_cache[5] = ($event) => loaded.value = true)
              }, null, 8, ["api", "plugin-id", "source-plugin-id", "pane", "devices", "onToast"])
            ], 512), [
              [_vShow, isFormPane.value]
            ]),
            _createVNode(_Transition, {
              name: "xc-fade",
              mode: "out-in"
            }, {
              default: _withCtx(() => [
                !isFormPane.value ? (_openBlock(), _createElementBlock("div", {
                  key: pane.value,
                  class: "xc-pane"
                }, [
                  pane.value === "account" ? (_openBlock(), _createBlock(ConfigLogin, {
                    key: 0,
                    api: props.api,
                    "plugin-id": props.pluginId,
                    "source-plugin-id": props.sourcePluginId,
                    status: status.value,
                    onChanged
                  }, null, 8, ["api", "plugin-id", "source-plugin-id", "status"])) : pane.value === "commands" ? (_openBlock(), _createBlock(ConfigCommands, {
                    key: 1,
                    api: props.api,
                    "plugin-id": props.pluginId,
                    "source-plugin-id": props.sourcePluginId,
                    onChanged
                  }, null, 8, ["api", "plugin-id", "source-plugin-id"])) : (_openBlock(), _createBlock(ConfigTasks, {
                    key: 2,
                    api: props.api,
                    "plugin-id": props.pluginId,
                    "source-plugin-id": props.sourcePluginId,
                    devices: devices.value,
                    onChanged
                  }, null, 8, ["api", "plugin-id", "source-plugin-id", "devices"]))
                ])) : _createCommentVNode("", true)
              ]),
              _: 1
            })
          ])
        ]),
        _createVNode(_component_v_snackbar, {
          modelValue: _unref(snackbar).show,
          "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => _unref(snackbar).show = $event),
          color: _unref(snackbar).color,
          timeout: "3500",
          location: "bottom"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(_unref(snackbar).text), 1)
          ]),
          _: 1
        }, 8, ["modelValue", "color"])
      ]);
    };
  }
});

const Config = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-a5b4913e"]]);

export { Config as default };
