import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { F as FLOW_STEPS, _ as _export_sfc } from './_plugin-vue_export-helper-iTF4IZwH.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,renderList:_renderList,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,createTextVNode:_createTextVNode,unref:_unref,createCommentVNode:_createCommentVNode,vShow:_vShow,withDirectives:_withDirectives,createBlock:_createBlock} = await importShared('vue');

const _hoisted_1 = { class: "mls-head" };
const _hoisted_2 = { class: "mls-head-text" };
const _hoisted_3 = { class: "text-caption text-medium-emphasis" };
const _hoisted_4 = { class: "mls-body" };
const _hoisted_5 = { class: "mls-nav" };
const _hoisted_6 = { class: "mls-content" };
const _hoisted_7 = { class: "mls-pane" };
const _hoisted_8 = { class: "mls-flow" };
const _hoisted_9 = { class: "mls-flow-index" };
const _hoisted_10 = { class: "mls-flow-body" };
const _hoisted_11 = { class: "text-body-2 font-weight-medium" };
const _hoisted_12 = {
  key: 0,
  class: "mls-flow-knob"
};
const _hoisted_13 = { class: "text-caption text-grey" };
const _hoisted_14 = { class: "mls-pane" };
const _hoisted_15 = { class: "mls-block" };
const _hoisted_16 = { class: "mls-block-title" };
const _hoisted_17 = { class: "mls-pane" };
const _hoisted_18 = { class: "text-caption text-grey" };
const _hoisted_19 = { class: "mls-pane" };
const _hoisted_20 = { class: "mls-actions" };
const {ref,computed,onMounted} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Config",
  props: {
    initialConfig: { type: Object, default: () => ({}) },
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    nativeSubscribe: { type: Function, default: null }
  },
  emits: ["save", "close", "switch", "layout"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const DEFAULTS = {
      enabled: false,
      onlyonce: false,
      mode: "",
      cron: "0 3 * * sun",
      scraper_paths: "",
      exclude_paths: "",
      mtw_base_url: "",
      mtw_username: "",
      mtw_password: "",
      mtw_resource: [],
      min_score: 3
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
      mtw_resource: toResourceList((props.initialConfig || {}).mtw_resource)
    });
    const pane = ref("flow");
    const panes = [
      { key: "flow", title: "流程总览", icon: "mdi-source-branch", desc: "一个文件从被扫到写到标签，中间经过哪些判断、各由哪个配置决定" },
      { key: "run", title: "运行与写入", icon: "mdi-play-circle-outline", desc: "什么时候跑、跑到已有标签的文件时动不动它" },
      { key: "match", title: "识别与匹配", icon: "mdi-magnify", desc: "问谁、问几个源、几分才算命中" },
      { key: "library", title: "音乐库范围", icon: "mdi-folder-multiple-outline", desc: "扫哪些目录、跳过哪些目录" }
    ];
    const currentPane = computed(() => panes.find((p) => p.key === pane.value) || panes[0]);
    const modeItems = [
      { title: "只补缺（推荐）", value: "" },
      { title: "强制覆盖已有标签", value: "force_all" }
    ];
    const resourceItems = [
      { title: "网易云 netease", value: "netease" },
      { title: "QQ 音乐 qmusic", value: "qmusic" },
      { title: "酷我 kuwo", value: "kuwo" },
      { title: "酷狗 kugou", value: "kugou" },
      { title: "咪咕 migu", value: "migu" },
      { title: "MusicBrainz", value: "musicbrainz" },
      { title: "智能 smart_tag", value: "smart_tag" }
    ];
    const ALL_SOURCES_TEXT = resourceItems.map((i) => i.title.split(" ")[0]).join(" / ");
    const sourceNote = computed(
      () => config.value.mtw_resource?.length ? `已选 ${config.value.mtw_resource.length} 个源，就按这个顺序问` : `留空 = 全部源（${ALL_SOURCES_TEXT}）`
    );
    const modeNote = computed(
      () => config.value.mode === "force_all" ? "强制覆盖（当前）" : "只补缺（当前，推荐）"
    );
    function saveConfig() {
      emit("save", config.value);
    }
    onMounted(() => {
      emit("layout", { maxWidth: "74rem" });
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
      const _component_v_select = _resolveComponent("v-select");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_textarea = _resolveComponent("v-textarea");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_card = _resolveComponent("v-card");
      return _openBlock(), _createBlock(_component_v_card, {
        flat: "",
        class: "mls-card"
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
              _cache[12] || (_cache[12] = _createElementVNode("div", { class: "text-h6" }, "MusicTagWeb音乐库刮削 · 配置", -1)),
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
                _createElementVNode("div", _hoisted_8, [
                  (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(_unref(FLOW_STEPS), (step, i) => {
                    return _openBlock(), _createElementBlock("div", {
                      key: step.key,
                      class: "mls-flow-step"
                    }, [
                      _createElementVNode("div", _hoisted_9, _toDisplayString(i + 1), 1),
                      _createElementVNode("div", _hoisted_10, [
                        _createElementVNode("div", _hoisted_11, [
                          _createVNode(_component_v_icon, {
                            icon: step.icon,
                            size: "16",
                            class: "mr-1"
                          }, null, 8, ["icon"]),
                          _createTextVNode(_toDisplayString(step.title) + " ", 1),
                          step.knobs ? (_openBlock(), _createElementBlock("span", _hoisted_12, "由「" + _toDisplayString(step.knobs) + "」决定", 1)) : _createCommentVNode("", true)
                        ]),
                        _createElementVNode("div", _hoisted_13, _toDisplayString(step.desc), 1)
                      ])
                    ]);
                  }), 128))
                ]),
                _createVNode(_component_v_alert, {
                  type: "warning",
                  variant: "tonal",
                  density: "compact",
                  class: "mt-3"
                }, {
                  default: _withCtx(() => [..._cache[13] || (_cache[13] = [
                    _createElementVNode("div", { class: "text-body-2" }, "写进文件就回不去的三件事", -1),
                    _createElementVNode("ul", { class: "mls-list" }, [
                      _createElementVNode("li", null, "「最低匹配分数」调到 1 表示曲名对上就写入，翻唱、钢琴版、纯音乐同名都会被当成 原曲写进文件，且无法回滚。"),
                      _createElementVNode("li", null, "「强制覆盖」会替换手工改过的曲名/艺人/专辑/年份/轨道号，也会重新下载并内嵌封面。"),
                      _createElementVNode("li", null, "MTW 与 MP 必须挂载同一份音乐库路径，否则查到的元数据会写到另一个文件上。")
                    ], -1)
                  ])]),
                  _: 1
                })
              ], 512), [
                [_vShow, pane.value === "flow"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_14, [
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
                _createElementVNode("div", _hoisted_15, [
                  _createElementVNode("div", _hoisted_16, "写入模式 · " + _toDisplayString(modeNote.value), 1),
                  _cache[14] || (_cache[14] = _createElementVNode("div", { class: "text-caption text-grey" }, " 只补缺：曲名+艺人+专辑+内嵌封面 四项齐全的文件按 mtime+size 记账直接跳过（不再请求 MTW）， 缺哪项补哪项，已有非空标签保持不动，手工修改不会被覆盖。 ", -1)),
                  _cache[15] || (_cache[15] = _createElementVNode("div", { class: "text-caption text-grey" }, " 强制覆盖：每轮忽略增量账、全库重扫重问，候选与现有标签不同就重写，封面重新下载内嵌、歌词强制重写。 候选里没有的字段不会清空，插件不认识的自定义注释字段也不会删。 ", -1)),
                  _cache[16] || (_cache[16] = _createElementVNode("div", { class: "text-caption text-grey" }, " 最终是否覆盖还取宿主「设置 → 媒体刮削」音乐三项策略与这里的较强者：宿主标签策略=覆盖时， 即使这里选只补缺也会重写；=跳过时，两种模式都不写该项。宿主默认 标签仅缺失、封面仅缺失、歌词更优才换。 ", -1))
                ]),
                _cache[17] || (_cache[17] = _createElementVNode("div", { class: "mls-block" }, [
                  _createElementVNode("div", { class: "mls-block-title" }, "执行周期"),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 留空按默认 0 3 * * sun（每周日 03:00）。星期编号按宿主的调度器算：0=周一…6=周日、7 视作周日， 和 crontab 的 0/7=周日 差一位；保存后数据页概览会回显「下次触发在周几」，以它为准。 "),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 非法表达式不会让插件挂掉：会记一条日志并回落默认值，但你自己配的时刻会一起丢掉。 ")
                ], -1)),
                _createVNode(_component_v_switch, {
                  modelValue: config.value.onlyonce,
                  "onUpdate:modelValue": _cache[3] || (_cache[3] = ($event) => config.value.onlyonce = $event),
                  label: "保存后立即运行一次",
                  "hide-details": "",
                  color: "primary",
                  inset: "",
                  class: "mt-1"
                }, null, 8, ["modelValue"])
              ], 512), [
                [_vShow, pane.value === "run"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_17, [
                _createVNode(_component_v_row, { dense: "" }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_col, {
                      cols: "12",
                      md: "8"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_v_text_field, {
                          modelValue: config.value.mtw_base_url,
                          "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => config.value.mtw_base_url = $event),
                          label: "Music Tag Web 地址",
                          placeholder: "http://192.168.x.x:8001",
                          "hide-details": ""
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
                    })
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_row, {
                  dense: "",
                  class: "mt-1"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_col, {
                      cols: "12",
                      md: "6"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_v_text_field, {
                          modelValue: config.value.mtw_username,
                          "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => config.value.mtw_username = $event),
                          label: "MTW 用户名",
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
                          modelValue: config.value.mtw_password,
                          "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => config.value.mtw_password = $event),
                          label: "MTW 密码",
                          type: "password",
                          placeholder: "仅本机保存，不入 git",
                          "hide-details": ""
                        }, null, 8, ["modelValue"])
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_select, {
                  modelValue: config.value.mtw_resource,
                  "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => config.value.mtw_resource = $event),
                  items: resourceItems,
                  label: "MTW 识别源（可多选，留空=全部）",
                  multiple: "",
                  chips: "",
                  "closable-chips": "",
                  density: "compact",
                  class: "mt-1"
                }, null, 8, ["modelValue"]),
                _createElementVNode("div", _hoisted_18, _toDisplayString(sourceNote.value) + "。", 1),
                _cache[18] || (_cache[18] = _createElementVNode("div", { class: "mls-block" }, [
                  _createElementVNode("div", { class: "mls-block-title" }, "元数据来源"),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 全部取自 Music Tag Web（MTW）的识别结果，插件不直连任何音乐平台。 qq / spotify / acoustid / all 实测恒返回空候选，没有列进来。 ")
                ], -1)),
                _cache[19] || (_cache[19] = _createElementVNode("div", { class: "mls-block" }, [
                  _createElementVNode("div", { class: "mls-block-title" }, "查询与选取"),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 每个文件按源顺序逐源查询，某源出现 3/3 满分候选就立刻停止，不再问后面的源；都没满分则合并所有候选取最高分。 分数为曲名 / 艺人 / 专辑 各 1 分（先做繁简与大小写归一，互相包含即算命中）；同分时按源顺序、同源内按 MTW 返回顺序取第一个。最高分仍低于「最低匹配分数」（默认 3，即要求三项全中）就记为失败、不写入。 "),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 这个阈值只在 1-3 之间生效，越界会按最近的一界执行。用于匹配的本地身份优先读文件已有的 tag， 缺哪项就按 艺人/专辑 (年)/NN - 曲名 的目录结构兜底；只有严格位于「音乐库路径」之下的目录才算艺人/专辑， 库根本身的名字不参与猜测 —— 猜不出来就留空，宁可判「未达阈值」也不拿目录名凑一个假身份去改写标签。 ")
                ], -1))
              ], 512), [
                [_vShow, pane.value === "match"]
              ]),
              _withDirectives(_createElementVNode("div", _hoisted_19, [
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.scraper_paths,
                  "onUpdate:modelValue": _cache[9] || (_cache[9] = ($event) => config.value.scraper_paths = $event),
                  rows: 7,
                  label: "音乐库路径（一行一个目录）",
                  placeholder: "/media/music/周杰伦\n/media/music",
                  "hide-details": ""
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.exclude_paths,
                  "onUpdate:modelValue": _cache[10] || (_cache[10] = ($event) => config.value.exclude_paths = $event),
                  rows: 3,
                  label: "排除路径",
                  "hide-details": "",
                  class: "mt-2"
                }, null, 8, ["modelValue"]),
                _cache[20] || (_cache[20] = _createElementVNode("div", { class: "mls-block" }, [
                  _createElementVNode("div", { class: "mls-block-title" }, "会处理的文件"),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 只扫描宿主音频白名单（设置 → 媒体刮削 → 音频扩展名）与本插件支持列表的交集：宿主不收的扩展名写标签时 会被原生刮削链直接拒成「刮削路径中没有支持的音频文件」，先排除掉才不会白打请求、又把文件记成失败并烧满重试次数。 "),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " Jellyfin 的 .trickplay 预览缓存目录整棵跳过；目录软链不跟随，避免回环目录把这一轮卡死。 指向库外的音频软链同样会跳过并在日志里报数量 —— 写标签是按路径落到真实文件的， 让扫描改到音乐库之外的文件不可接受；确有需要请把它指向的目录也加进库路径。 "),
                  _createElementVNode("div", { class: "text-caption text-grey" }, " 路径必须写到实际挂载的目录：只有严格位于「音乐库路径」之下的目录才按 艺人/专辑 解释， 库根本身的名字不算专辑名。同一份库需要 MTW 与 MP 挂载到相同路径。 ")
                ], -1))
              ], 512), [
                [_vShow, pane.value === "library"]
              ])
            ])
          ]),
          _createVNode(_component_v_divider),
          _createElementVNode("div", _hoisted_20, [
            _createVNode(_component_v_btn, {
              variant: "text",
              class: "text-none",
              "prepend-icon": "mdi-database-eye-outline",
              onClick: _cache[11] || (_cache[11] = ($event) => emit("switch"))
            }, {
              default: _withCtx(() => [..._cache[21] || (_cache[21] = [
                _createTextVNode(" 查看数据页 ", -1)
              ])]),
              _: 1
            }),
            _createVNode(_component_v_spacer),
            _createVNode(_component_v_btn, {
              color: "primary",
              variant: "flat",
              "prepend-icon": "mdi-content-save",
              class: "text-none",
              onClick: saveConfig
            }, {
              default: _withCtx(() => [..._cache[22] || (_cache[22] = [
                _createTextVNode(" 保存配置 ", -1)
              ])]),
              _: 1
            })
          ])
        ]),
        _: 1
      });
    };
  }
});

const Config = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-6e57b98b"]]);

export { Config as default };
