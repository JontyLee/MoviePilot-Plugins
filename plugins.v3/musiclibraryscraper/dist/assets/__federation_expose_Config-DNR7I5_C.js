import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {createElementVNode:_createElementVNode,resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createTextVNode:_createTextVNode,openBlock:_openBlock,createElementBlock:_createElementBlock} = await importShared('vue');

const _hoisted_1 = { class: "plugin-config pa-4" };
const _hoisted_2 = { class: "d-flex align-center gap-2 mt-4" };
const {ref,computed,inject} = await importShared('vue');

const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Config",
  props: {
    initialConfig: { type: Object, default: () => ({}) },
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    nativeSubscribe: { type: Function, default: null }
  },
  emits: ["save", "close", "switch"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const toast = inject("moviepilot:toast", null);
    const DEFAULTS = {
      enabled: false,
      onlyonce: false,
      mode: "",
      cron: "0 3 * * 0",
      scraper_paths: "",
      exclude_paths: "",
      mtw_base_url: "",
      mtw_username: "",
      mtw_password: "",
      mtw_resource: [],
      mtw_select_mode: 0,
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
    const resourceHint = computed(
      () => config.value.mtw_resource?.length ? `已选 ${config.value.mtw_resource.length} 个源，按顺序逐源查询，出现满分候选即停` : `留空 = 全部源（${ALL_SOURCES_TEXT}）；按顺序逐源查询，出现满分候选即停`
    );
    const modeHint = computed(
      () => config.value.mode === "force_all" ? "强制覆盖：每轮忽略增量账、全库重扫重问 MTW；凡候选与现有标签不同就重写，封面重新下载并内嵌，歌词强制重写。手工改过的曲名、艺人、专辑、年份、轨道号会被替换（候选里没有的字段不会清空，插件不认识的自定义注释字段也不会删）。" : "只补缺（推荐）：曲名+艺人+专辑+内嵌封面 四项齐全的文件直接跳过（按 mtime+size 记账，之后不再请求）；缺哪项补哪项，已有的非空标签保持不动，手工修改不会被覆盖。"
    );
    const selectModeItems = [
      { title: "0 默认", value: 0 },
      { title: "1 精准匹配（时长）", value: 1 }
    ];
    function saveConfig() {
      emit("save", config.value);
      toast?.success("配置已保存");
    }
    return (_ctx, _cache) => {
      const _component_v_switch = _resolveComponent("v-switch");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_select = _resolveComponent("v-select");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_textarea = _resolveComponent("v-textarea");
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_btn = _resolveComponent("v-btn");
      return _openBlock(), _createElementBlock("div", _hoisted_1, [
        _cache[16] || (_cache[16] = _createElementVNode("div", { class: "text-h6 mb-3" }, "MusicTagWeb音乐库刮削 · 配置", -1)),
        _createVNode(_component_v_row, { dense: "" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_switch, {
                  modelValue: config.value.enabled,
                  "onUpdate:modelValue": _cache[0] || (_cache[0] = ($event) => config.value.enabled = $event),
                  label: "启用插件",
                  "hide-details": "",
                  color: "primary",
                  inset: ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_switch, {
                  modelValue: config.value.onlyonce,
                  "onUpdate:modelValue": _cache[1] || (_cache[1] = ($event) => config.value.onlyonce = $event),
                  label: "立即运行一次",
                  "hide-details": "",
                  color: "primary",
                  inset: ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_select, {
                  modelValue: config.value.mode,
                  "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => config.value.mode = $event),
                  items: modeItems,
                  label: "写入模式",
                  hint: "决定已有标签是保留还是重写，下方有逐项差别说明",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_alert, {
          type: config.value.mode === "force_all" ? "warning" : "success",
          variant: "tonal",
          density: "compact",
          class: "mb-2",
          text: modeHint.value
        }, null, 8, ["type", "text"]),
        _createVNode(_component_v_row, { dense: "" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "12",
              md: "6"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_text_field, {
                  modelValue: config.value.cron,
                  "onUpdate:modelValue": _cache[3] || (_cache[3] = ($event) => config.value.cron = $event),
                  label: "执行周期",
                  placeholder: "5 位 cron，留空默认 0 3 * * 0",
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
                _createVNode(_component_v_select, {
                  modelValue: config.value.mtw_resource,
                  "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => config.value.mtw_resource = $event),
                  items: resourceItems,
                  label: "MTW 识别源（可多选）",
                  multiple: "",
                  chips: "",
                  "closable-chips": "",
                  density: "compact",
                  hint: resourceHint.value,
                  "persistent-hint": ""
                }, null, 8, ["modelValue", "hint"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_row, { dense: "" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "12",
              md: "8"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_text_field, {
                  modelValue: config.value.mtw_base_url,
                  "onUpdate:modelValue": _cache[5] || (_cache[5] = ($event) => config.value.mtw_base_url = $event),
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
                _createVNode(_component_v_select, {
                  modelValue: config.value.mtw_select_mode,
                  "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => config.value.mtw_select_mode = $event),
                  items: selectModeItems,
                  label: "MTW 候选排序",
                  "hide-details": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_row, { dense: "" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_text_field, {
                  modelValue: config.value.mtw_username,
                  "onUpdate:modelValue": _cache[7] || (_cache[7] = ($event) => config.value.mtw_username = $event),
                  label: "MTW 用户名",
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
                  modelValue: config.value.mtw_password,
                  "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => config.value.mtw_password = $event),
                  label: "MTW 密码",
                  type: "password",
                  placeholder: "仅本机保存，不入 git",
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
                  "onUpdate:modelValue": _cache[9] || (_cache[9] = ($event) => config.value.min_score = $event),
                  modelModifiers: { number: true },
                  label: "最低匹配分数",
                  type: "number",
                  min: "0",
                  max: "3",
                  hint: "曲名/艺人/专辑 各 1 分，满分 3。保持 3 即可：单源召不回的歌由多源轮询补上；降到 2 会接受同名 Live 版/翻唱，写错标签的代价比漏刮高。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_row, { dense: "" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, { cols: "12" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.scraper_paths,
                  "onUpdate:modelValue": _cache[10] || (_cache[10] = ($event) => config.value.scraper_paths = $event),
                  rows: 5,
                  label: "音乐库路径（一行一个目录）",
                  placeholder: "/media/music/周杰伦\n/media/music",
                  "hide-details": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_row, { dense: "" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, { cols: "12" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.exclude_paths,
                  "onUpdate:modelValue": _cache[11] || (_cache[11] = ($event) => config.value.exclude_paths = $event),
                  rows: 2,
                  label: "排除路径",
                  "hide-details": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_alert, {
          type: "info",
          variant: "tonal",
          class: "mt-2",
          density: "compact"
        }, {
          default: _withCtx(() => [..._cache[13] || (_cache[13] = [
            _createElementVNode("div", { class: "mb-1" }, " 元数据全部来自 Music Tag Web（MTW），插件不直连音乐平台。单源 top10 常整批是翻唱/钢琴版， 所以默认逐源查询，而不是放宽匹配分数。 ", -1),
            _createElementVNode("div", { class: "mb-1" }, " 最终是否覆盖还叠加宿主「设置 → 媒体刮削」里的音乐三项策略（标签 / 封面 / 歌词），取两者中更强的一个： 宿主设为「覆盖」时，即使这里选只补缺也会重写已有标签；宿主设为「跳过」时，两种模式都不写该项。 默认策略是 标签仅缺失、封面仅缺失、歌词更优才换。 ", -1),
            _createElementVNode("div", null, "MTW 与 MP 需挂载同一份音乐库路径，否则写标签会落到不同文件。", -1)
          ])]),
          _: 1
        }),
        _createElementVNode("div", _hoisted_2, [
          _createVNode(_component_v_spacer),
          _createVNode(_component_v_btn, {
            variant: "text",
            onClick: _cache[12] || (_cache[12] = ($event) => emit("switch"))
          }, {
            default: _withCtx(() => [..._cache[14] || (_cache[14] = [
              _createTextVNode("查看数据页", -1)
            ])]),
            _: 1
          }),
          _createVNode(_component_v_btn, {
            color: "primary",
            "prepend-icon": "mdi-content-save",
            onClick: saveConfig
          }, {
            default: _withCtx(() => [..._cache[15] || (_cache[15] = [
              _createTextVNode("保存配置", -1)
            ])]),
            _: 1
          })
        ])
      ]);
    };
  }
});

const Config = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-923ccd89"]]);

export { Config as default };
