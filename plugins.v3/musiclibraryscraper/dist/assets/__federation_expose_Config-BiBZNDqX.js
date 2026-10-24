import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { F as FlowPipeline, a as FLOW_STEPS, _ as _export_sfc } from './flow-BYkeE1Vm.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,renderList:_renderList,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,createTextVNode:_createTextVNode,unref:_unref,vShow:_vShow,withDirectives:_withDirectives,createBlock:_createBlock,createCommentVNode:_createCommentVNode,normalizeClass:_normalizeClass} = await importShared('vue');

const _hoisted_1 = { class: "mls-head" };
const _hoisted_2 = { class: "mls-head-text" };
const _hoisted_3 = { class: "text-caption text-medium-emphasis" };
const _hoisted_4 = { class: "mls-body" };
const _hoisted_5 = { class: "mls-nav" };
const _hoisted_6 = { class: "mls-content" };
const _hoisted_7 = { class: "mls-pane" };
const _hoisted_8 = { class: "mls-docs" };
const _hoisted_9 = { class: "mls-pane" };
const _hoisted_10 = { class: "mls-block" };
const _hoisted_11 = { class: "text-caption text-grey" };
const _hoisted_12 = { class: "mls-block" };
const _hoisted_13 = { class: "mls-pane" };
const _hoisted_14 = { class: "text-caption text-grey" };
const _hoisted_15 = { class: "mls-pane" };
const _hoisted_16 = { class: "mls-block" };
const _hoisted_17 = {
  key: 0,
  class: "mls-chips mt-2"
};
const _hoisted_18 = { class: "mls-actions" };
const {ref,computed,inject,onMounted} = await importShared('vue');
const DOCS_URL = "https://github.com/JontyLee/MoviePilot-Plugins/blob/main/docs/musiclibraryscraper.md";
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Config",
  props: {
    initialConfig: { type: Object, default: () => ({}) },
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    nativeSubscribe: { type: Function, default: null },
    // 侧栏全页里自己开的这个配置框：宿主不给 AppPage 传 initialConfig，
    // 起头和落盘都得走插件自己的 /config，而不是 emit('save') 交给宿主整体替换。
    standalone: { type: Boolean, default: false }
  },
  emits: ["save", "close", "switch", "layout", "updated"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const DEFAULTS = {
      enabled: false,
      // 缺键按开：升级过来的装机没有这一项，默认关会让入口"升级完就消失"
      show_sidebar_nav: true,
      onlyonce: false,
      mode: "",
      cron: "0 3 * * sun",
      scraper_paths: "",
      exclude_paths: "",
      // 识别源留空 = 全部源：预填五个等于把这条规则从界面上抹掉
      resources: [],
      min_score: 3,
      // 关掉时歌词仍由宿主 lrclib / AMLL 取
      fill_lyrics: false
    };
    function toResourceList(value) {
      const raw = Array.isArray(value) ? value : String(value ?? "").split(/[;,]/);
      const out = [];
      for (const item of raw) {
        const name = String(item ?? "").trim();
        if (name && !out.includes(name)) out.push(name);
      }
      return out;
    }
    const config = ref({
      ...DEFAULTS,
      ...props.initialConfig || {},
      resources: toResourceList((props.initialConfig || {}).resources)
    });
    const saving = ref(false);
    const audioExtensions = ref([]);
    const snackbar = ref({ show: false, text: "", color: "info" });
    const toast = inject("moviepilot:toast", null);
    const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`);
    function notify(msg, color = "info") {
      if (toast) {
        const fn = toast[color] || toast.info;
        fn.call(toast, msg);
        return;
      }
      snackbar.value = { show: true, text: msg, color };
    }
    const pane = ref("flow");
    const panes = [
      { key: "flow", title: "流程", icon: "mdi-source-branch", desc: "一个文件从被扫到写标签，要过的几道判断" },
      { key: "run", title: "运行与写入", icon: "mdi-play-circle-outline", desc: "什么时候跑、跑到已有标签的文件动不动它" },
      { key: "match", title: "识别与匹配", icon: "mdi-magnify", desc: "问哪几个源、几分算命中" },
      { key: "library", title: "音乐库范围", icon: "mdi-folder-multiple-outline", desc: "扫哪些目录、跳过哪些" }
    ];
    const currentPane = computed(() => panes.find((p) => p.key === pane.value) || panes[0]);
    const modeItems = [
      { title: "只补缺（推荐）", value: "" },
      { title: "强制覆盖已有标签", value: "force_all" }
    ];
    const resourceItems = [
      { title: "网易云 netease", value: "netease" },
      { title: "QQ 音乐 qmusic", value: "qmusic" },
      { title: "酷狗 kugou", value: "kugou" },
      { title: "咪咕 migu", value: "migu" },
      { title: "MusicBrainz", value: "musicbrainz" }
    ];
    const picked = computed(() => toResourceList(config.value.resources));
    const sourceNote = computed(
      () => picked.value.length ? `按这个顺序逐源问，某源出到满分就停` : `留空 = 全部源（${resourceItems.map((i) => i.title.split(" ")[0]).join(" / ")}）`
    );
    function saveConfig() {
      if (props.standalone) return saveStandalone();
      emit("save", config.value);
    }
    async function loadStandalone() {
      try {
        const res = await props.api.get(`${base.value}/config`);
        if (!res) return;
        config.value = {
          ...DEFAULTS,
          ...res,
          onlyonce: false,
          resources: toResourceList(res.resources)
        };
        audioExtensions.value = Array.isArray(res.audio_extensions) ? res.audio_extensions : [];
      } catch (e) {
        notify(`配置读取失败：${e?.message || e}`, "error");
      }
    }
    async function saveStandalone() {
      saving.value = true;
      try {
        const body = { ...config.value, onlyonce: !!config.value.onlyonce };
        const res = await props.api.post(`${base.value}/config`, body);
        if (res && res.ok === false) {
          notify(res.message || "保存失败", "warning");
          return;
        }
        await loadStandalone();
        notify("配置已保存", "success");
        emit("updated");
      } catch (e) {
        notify(`保存失败：${e?.message || e}`, "error");
      } finally {
        saving.value = false;
      }
    }
    onMounted(() => {
      emit("layout", { maxWidth: "74rem" });
      if (props.standalone) loadStandalone();
    });
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_avatar = _resolveComponent("v-avatar");
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_switch = _resolveComponent("v-switch");
      const _component_v_divider = _resolveComponent("v-divider");
      const _component_v_list_item_title = _resolveComponent("v-list-item-title");
      const _component_v_list_item = _resolveComponent("v-list-item");
      const _component_v_list = _resolveComponent("v-list");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_select = _resolveComponent("v-select");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_textarea = _resolveComponent("v-textarea");
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      const _component_v_card = _resolveComponent("v-card");
      return _openBlock(), _createBlock(_component_v_card, {
        flat: "",
        class: _normalizeClass(["mls-card", __props.standalone ? "mls-card-fit" : ""])
      }, {
        default: _withCtx(() => [
          _createElementVNode("div", _hoisted_1, [
            _createVNode(_component_v_avatar, {
              color: "primary",
              variant: "tonal",
              rounded: "lg",
              size: "40",
              class: "mr-3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_icon, {
                  icon: "mdi-music-circle-outline",
                  size: "24"
                })
              ]),
              _: 1
            }),
            _createElementVNode("div", _hoisted_2, [
              _cache[13] || (_cache[13] = _createElementVNode("div", { class: "text-h6" }, "TagFlow音乐库刮削 · 配置", -1)),
              _createElementVNode("div", _hoisted_3, _toDisplayString(currentPane.value.desc), 1)
            ]),
            _createVNode(_component_v_spacer),
            _createVNode(_component_v_switch, {
              modelValue: config.value.enabled,
              "onUpdate:modelValue": _cache[0] || (_cache[0] = ($event) => config.value.enabled = $event),
              color: "success",
              "hide-details": "",
              inset: "",
              label: config.value.enabled ? "已启用" : "已停用"
            }, null, 8, ["modelValue", "label"])
          ]),
          _createVNode(_component_v_divider),
          _createElementVNode("div", _hoisted_4, [
            _createElementVNode("nav", _hoisted_5, [
              _createVNode(_component_v_list, {
                density: "comfortable",
                nav: "",
                class: "py-2"
              }, {
                default: _withCtx(() => [
                  (_openBlock(), _createElementBlock(_Fragment, null, _renderList(panes, (item) => {
                    return _createVNode(_component_v_list_item, {
                      key: item.key,
                      active: pane.value === item.key,
                      color: "primary",
                      rounded: "lg",
                      class: "mls-nav-item",
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
              _withDirectives(_createElementVNode("div", _hoisted_7, [
                _createVNode(FlowPipeline, {
                  steps: _unref(FLOW_STEPS),
                  variant: "config",
                  running: true
                }, null, 8, ["steps"]),
                _createVNode(_component_v_alert, {
                  type: "warning",
                  variant: "tonal",
                  density: "compact",
                  class: "mt-3"
                }, {
                  default: _withCtx(() => [..._cache[14] || (_cache[14] = [
                    _createElementVNode("div", { class: "text-body-2 font-weight-medium" }, "写进文件就回不去", -1),
                    _createElementVNode("div", { class: "text-caption mt-1" }, [
                      _createTextVNode(" 「最低匹配分数」调到 1：曲名对上就写，翻唱、钢琴版也会被当成原曲。"),
                      _createElementVNode("br"),
                      _createTextVNode(" 「强制覆盖」：手工改过的标签会被替换，封面重新下载内嵌。"),
                      _createElementVNode("br"),
                      _createTextVNode(" 两者都取这里与宿主刮削策略里较强者；先小范围试一轮再全库跑。 ")
                    ], -1)
                  ])]),
                  _: 1
                }),
                _createElementVNode("div", _hoisted_8, [
                  _cache[16] || (_cache[16] = _createElementVNode("div", { class: "mls-block-title" }, "想看逐项判据与失败归因", -1)),
                  _cache[17] || (_cache[17] = _createElementVNode("div", { class: "text-caption text-grey" }, " 配置项下面的说明只留最短的一句；目录结构兜底、同分时取哪一份、各源现状与 没接进来的源，都写在仓库的使用说明里。 ", -1)),
                  _createVNode(_component_v_btn, {
                    href: DOCS_URL,
                    target: "_blank",
                    rel: "noopener",
                    variant: "text",
                    size: "small",
                    class: "text-none mt-1",
                    "prepend-icon": "mdi-open-in-new"
                  }, {
                    default: _withCtx(() => [..._cache[15] || (_cache[15] = [
                      _createTextVNode(" docs/musiclibraryscraper.md ", -1)
                    ])]),
                    _: 1
                  })
                ])
              ], 512), [
                [_vShow, pane.value === "flow"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_9, [
                _createVNode(_component_v_row, { dense: "" }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_col, {
                      cols: "12",
                      md: "6"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_v_select, {
                          modelValue: config.value.mode,
                          "onUpdate:modelValue": _cache[1] || (_cache[1] = ($event) => config.value.mode = $event),
                          items: modeItems,
                          label: "写入模式",
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
                          modelValue: config.value.cron,
                          "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => config.value.cron = $event),
                          label: "执行周期",
                          placeholder: "5 位 cron；星期 0=周一…6=周日、7=周日，留空默认 0 3 * * sun",
                          "hide-details": ""
                        }, null, 8, ["modelValue"])
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                }),
                _createElementVNode("div", _hoisted_10, [
                  _createElementVNode("div", _hoisted_11, _toDisplayString(config.value.mode === "force_all" ? "强制覆盖：全库重扫重问，手工改过的标签会被替换（候选里没有的字段不会清空）。" : "只补缺：四项齐全的文件直接跳过，缺哪项补哪项，手工改过的标签不动。") + " 最终是否覆盖取这里与宿主「设置 → 媒体刮削」的较强者。 ", 1)
                ]),
                _createVNode(_component_v_switch, {
                  modelValue: config.value.onlyonce,
                  "onUpdate:modelValue": _cache[3] || (_cache[3] = ($event) => config.value.onlyonce = $event),
                  label: "保存后立即运行一次",
                  "hide-details": "",
                  color: "primary",
                  inset: "",
                  class: "mt-1"
                }, null, 8, ["modelValue"]),
                _createElementVNode("div", _hoisted_12, [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.show_sidebar_nav,
                    "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => config.value.show_sidebar_nav = $event),
                    label: "在主界面「整理」显示侧栏入口",
                    "hide-details": "",
                    color: "primary",
                    inset: ""
                  }, null, 8, ["modelValue"]),
                  _cache[18] || (_cache[18] = _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 数据页（逐文件核对与补齐）在这个入口里；关掉不影响周期扫描，也不影响宿主「插件管理 → 配置」。 ", -1))
                ])
              ], 512), [
                [_vShow, pane.value === "run"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_13, [
                _createVNode(_component_v_row, { dense: "" }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_col, {
                      cols: "12",
                      md: "4"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_v_text_field, {
                          modelValue: config.value.min_score,
                          "onUpdate:modelValue": _cache[5] || (_cache[5] = ($event) => config.value.min_score = $event),
                          modelModifiers: { number: true },
                          label: "最低匹配分数（1-3）",
                          type: "number",
                          min: "1",
                          max: "3",
                          "hide-details": ""
                        }, null, 8, ["modelValue"])
                      ]),
                      _: 1
                    }),
                    _createVNode(_component_v_col, {
                      cols: "12",
                      md: "8"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_v_select, {
                          modelValue: config.value.resources,
                          "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => config.value.resources = $event),
                          items: resourceItems,
                          label: "识别源（可多选，留空=全部源）",
                          multiple: "",
                          chips: "",
                          "closable-chips": "",
                          density: "compact"
                        }, null, 8, ["modelValue"])
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                }),
                _createElementVNode("div", _hoisted_14, _toDisplayString(sourceNote.value) + "。", 1),
                _createVNode(_component_v_switch, {
                  modelValue: config.value.fill_lyrics,
                  "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => config.value.fill_lyrics = $event),
                  label: "缺歌词时主动去识别源抓（默认交给宿主）",
                  "hide-details": "",
                  color: "primary",
                  inset: "",
                  class: "mt-1"
                }, null, 8, ["modelValue"]),
                _cache[19] || (_cache[19] = _createElementVNode("div", { class: "mls-block" }, [
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 曲名 / 艺人 / 专辑 各 1 分，默认要 3 分全中才写；分数低于阈值只记失败、不碰文件。 调低到 1-2 就可能把翻唱、纯音乐版当成原曲，标签写进文件无法回滚。 "),
                  _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 五个源都是插件直连的公开检索接口，不必部署任何服务；只发清洗后的「曲名-艺人」， 库内路径与目录结构不出本机。 "),
                  _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 比对用的本地身份优先读文件已有标签，缺哪项就按目录结构兜底；猜不出来宁可判未达阈值， 也不拿目录名凑假身份改写标签。 ")
                ], -1))
              ], 512), [
                [_vShow, pane.value === "match"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_15, [
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.scraper_paths,
                  "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => config.value.scraper_paths = $event),
                  rows: 6,
                  label: "音乐库路径（一行一个目录）",
                  placeholder: "/media/music/周杰伦\n/media/music",
                  "hide-details": ""
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.exclude_paths,
                  "onUpdate:modelValue": _cache[9] || (_cache[9] = ($event) => config.value.exclude_paths = $event),
                  rows: 2,
                  label: "排除路径",
                  "hide-details": "",
                  class: "mt-2"
                }, null, 8, ["modelValue"]),
                _createElementVNode("div", _hoisted_16, [
                  _cache[20] || (_cache[20] = _createElementVNode("div", { class: "text-caption text-grey" }, " 只处理宿主音频白名单与本插件支持列表的交集；.trickplay 缓存目录整棵跳过， 指向库外的软链不碰 —— 写标签是按路径落到真实文件的。 ", -1)),
                  audioExtensions.value.length ? (_openBlock(), _createElementBlock("div", _hoisted_17, [
                    (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(audioExtensions.value, (ext) => {
                      return _openBlock(), _createBlock(_component_v_chip, {
                        key: ext,
                        size: "x-small",
                        variant: "tonal",
                        color: "grey"
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode(_toDisplayString(ext), 1)
                        ]),
                        _: 2
                      }, 1024);
                    }), 128))
                  ])) : _createCommentVNode("", true)
                ])
              ], 512), [
                [_vShow, pane.value === "library"]
              ])
            ])
          ]),
          _createVNode(_component_v_divider),
          _createElementVNode("div", _hoisted_18, [
            !__props.standalone ? (_openBlock(), _createBlock(_component_v_btn, {
              key: 0,
              variant: "text",
              class: "text-none",
              "prepend-icon": "mdi-database-eye-outline",
              onClick: _cache[10] || (_cache[10] = ($event) => emit("switch"))
            }, {
              default: _withCtx(() => [..._cache[21] || (_cache[21] = [
                _createTextVNode(" 查看数据页 ", -1)
              ])]),
              _: 1
            })) : (_openBlock(), _createBlock(_component_v_btn, {
              key: 1,
              variant: "text",
              class: "text-none",
              "prepend-icon": "mdi-close",
              onClick: _cache[11] || (_cache[11] = ($event) => emit("close"))
            }, {
              default: _withCtx(() => [..._cache[22] || (_cache[22] = [
                _createTextVNode(" 关闭 ", -1)
              ])]),
              _: 1
            })),
            _createVNode(_component_v_spacer),
            _createVNode(_component_v_btn, {
              color: "primary",
              variant: "flat",
              "prepend-icon": "mdi-content-save",
              class: "text-none",
              loading: saving.value,
              onClick: saveConfig
            }, {
              default: _withCtx(() => [..._cache[23] || (_cache[23] = [
                _createTextVNode(" 保存配置 ", -1)
              ])]),
              _: 1
            }, 8, ["loading"])
          ]),
          _createVNode(_component_v_snackbar, {
            modelValue: snackbar.value.show,
            "onUpdate:modelValue": _cache[12] || (_cache[12] = ($event) => snackbar.value.show = $event),
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
      }, 8, ["class"]);
    };
  }
});

const Config = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-054efbaa"]]);

export { Config as default };
