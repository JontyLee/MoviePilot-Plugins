import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {createElementVNode:_createElementVNode,resolveComponent:_resolveComponent,createVNode:_createVNode,createTextVNode:_createTextVNode,withCtx:_withCtx,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,toDisplayString:_toDisplayString,renderList:_renderList,Fragment:_Fragment,createElementBlock:_createElementBlock} = await importShared('vue');

const _hoisted_1 = { class: "plugin-root pa-4" };
const _hoisted_2 = { class: "d-flex align-center mb-3 flex-wrap gap-2" };
const _hoisted_3 = { class: "text-h5" };
const _hoisted_4 = { class: "text-h5" };
const _hoisted_5 = { class: "text-h5" };
const _hoisted_6 = { class: "text-caption text-no-wrap" };
const _hoisted_7 = { class: "text-caption" };
const _hoisted_8 = { class: "d-flex align-center flex-wrap gap-2 mb-2" };
const _hoisted_9 = ["title"];
const _hoisted_10 = {
  class: "text-caption text-grey text-truncate",
  style: { "max-width": "480px" }
};
const _hoisted_11 = {
  key: 0,
  class: "text-caption"
};
const _hoisted_12 = {
  key: 1,
  class: "text-caption text-grey"
};
const _hoisted_13 = {
  key: 2,
  class: "text-caption text-grey"
};
const _hoisted_14 = { key: 3 };
const {ref,onMounted,computed,inject} = await importShared('vue');

const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Page",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    nativeSubscribe: { type: Function, default: null }
  },
  emits: ["action", "switch", "close"],
  setup(__props, { emit: __emit }) {
    const emit = __emit;
    const props = __props;
    const base = computed(() => `plugin/${props.pluginId || props.sourcePluginId}`);
    const toast = inject("moviepilot:toast", null);
    const loading = ref(false);
    const busy = ref(false);
    const snackbar = ref({ show: false, text: "", color: "info" });
    const status = ref({ runs: {}, counts: {}, config: {}, recent_failed: [] });
    const rows = ref([]);
    const total = ref(0);
    const bucket = ref("failed");
    const search = ref("");
    const limit = ref(50);
    const offset = ref(0);
    const tabs = [
      { key: "done", label: "已处理" },
      { key: "failed", label: "失败待重试" },
      { key: "pending", label: "待处理" }
    ];
    function notify(msg, color = "info") {
      if (toast) {
        const fn = toast[color] || toast.info;
        fn.call(toast, msg);
        return;
      }
      snackbar.value = { show: true, text: msg, color };
    }
    async function loadStatus() {
      try {
        status.value = await props.api.get(`${base.value}/status`);
      } catch (e) {
        notify(`状态加载失败：${e?.message || e}`, "error");
      }
    }
    async function loadFiles() {
      loading.value = true;
      try {
        const data = await props.api.get(`${base.value}/files`, {
          bucket: bucket.value,
          limit: limit.value,
          offset: offset.value,
          search: search.value
        });
        rows.value = data?.items || [];
        total.value = data?.total || 0;
      } catch (e) {
        rows.value = [];
        total.value = 0;
        notify(`列表加载失败：${e?.message || e}`, "error");
      } finally {
        loading.value = false;
      }
    }
    async function refresh() {
      await Promise.all([loadStatus(), loadFiles()]);
      emit("action");
    }
    async function retryPaths(paths, label) {
      if (!paths.length) return;
      busy.value = true;
      try {
        const res = await props.api.post(`${base.value}/retry`, { paths });
        if (res?.accepted) notify(`已提交重试 ${res.count} 个${label}`);
        else notify(`已有扫描在运行，稍后再试${label}`, "warning");
        await refresh();
      } catch (e) {
        notify(`重试失败：${e?.message || e}`, "error");
      } finally {
        busy.value = false;
      }
    }
    function retryRow(row) {
      retryPaths([row.path], `（${row.name}）`);
    }
    function retryAllFailed() {
      if (!counts.value.failed) {
        notify("没有失败项可重试", "warning");
        return;
      }
      doRetryAll();
    }
    async function doRetryAll() {
      busy.value = true;
      try {
        const res = await props.api.post(`${base.value}/retry`, {
          bucket: "failed",
          all: true
        });
        if (res?.accepted) notify(`已提交重试 ${res.count} 个失败项`);
        else notify("已有扫描在运行，稍后再试", "warning");
        await refresh();
      } catch (e) {
        notify(`重试失败：${e?.message || e}`, "error");
      } finally {
        busy.value = false;
      }
    }
    async function scanNow(force) {
      busy.value = true;
      try {
        const res = await props.api.post(`${base.value}/scan`, { force });
        notify(res?.accepted ? force ? "已开始全量扫描" : "已开始增量扫描" : "已有扫描在运行", res?.accepted ? "info" : "warning");
        await refresh();
      } catch (e) {
        notify(`触发扫描失败：${e?.message || e}`, "error");
      } finally {
        busy.value = false;
      }
    }
    function switchTab(key) {
      bucket.value = key;
      offset.value = 0;
      loadFiles();
    }
    function onPage(nextOffset) {
      offset.value = Math.max(0, nextOffset);
      loadFiles();
    }
    function fmtTime(unix) {
      if (!unix && unix !== 0) return "-";
      if (typeof unix === "string") return unix;
      try {
        return new Date(unix * 1e3).toLocaleString();
      } catch {
        return String(unix);
      }
    }
    function fmtSize(bytes) {
      if (!bytes) return "-";
      const mb = bytes / 1048576;
      return mb >= 1 ? `${mb.toFixed(1)} MB` : `${(bytes / 1024).toFixed(0)} KB`;
    }
    const counts = computed(() => status.value?.counts || {});
    const runs = computed(() => status.value?.runs || {});
    const running = computed(() => !!status.value?.running);
    onMounted(refresh);
    return (_ctx, _cache) => {
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_card_text = _resolveComponent("v-card-text");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_badge = _resolveComponent("v-badge");
      const _component_v_tab = _resolveComponent("v-tab");
      const _component_v_tabs = _resolveComponent("v-tabs");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_data_table = _resolveComponent("v-data-table");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock(), _createElementBlock("div", _hoisted_1, [
        _createElementVNode("div", _hoisted_2, [
          _cache[10] || (_cache[10] = _createElementVNode("div", { class: "text-h6" }, "音乐库刮削 · 数据页", -1)),
          _createVNode(_component_v_spacer),
          running.value ? (_openBlock(), _createBlock(_component_v_chip, {
            key: 0,
            color: "info",
            size: "small",
            variant: "tonal",
            "prepend-icon": "mdi-progress-clock"
          }, {
            default: _withCtx(() => [..._cache[7] || (_cache[7] = [
              _createTextVNode(" 扫描进行中 ", -1)
            ])]),
            _: 1
          })) : _createCommentVNode("", true),
          _createVNode(_component_v_btn, {
            size: "small",
            variant: "text",
            "prepend-icon": "mdi-cog",
            onClick: _cache[0] || (_cache[0] = ($event) => emit("switch"))
          }, {
            default: _withCtx(() => [..._cache[8] || (_cache[8] = [
              _createTextVNode("配置", -1)
            ])]),
            _: 1
          }),
          _createVNode(_component_v_btn, {
            size: "small",
            color: "primary",
            loading: loading.value || busy.value,
            "prepend-icon": "mdi-refresh",
            onClick: refresh
          }, {
            default: _withCtx(() => [..._cache[9] || (_cache[9] = [
              _createTextVNode(" 刷新 ", -1)
            ])]),
            _: 1
          }, 8, ["loading"])
        ]),
        _createVNode(_component_v_row, {
          dense: "",
          class: "mb-2"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "6",
              md: "3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_card, {
                  variant: "tonal",
                  color: "success"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_card_text, { class: "py-2" }, {
                      default: _withCtx(() => [
                        _cache[11] || (_cache[11] = _createElementVNode("div", { class: "text-caption" }, "已处理", -1)),
                        _createElementVNode("div", _hoisted_3, _toDisplayString(counts.value.done ?? 0), 1)
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "6",
              md: "3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_card, {
                  variant: "tonal",
                  color: "warning"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_card_text, { class: "py-2" }, {
                      default: _withCtx(() => [
                        _cache[12] || (_cache[12] = _createElementVNode("div", { class: "text-caption" }, "待重试", -1)),
                        _createElementVNode("div", _hoisted_4, _toDisplayString(counts.value.retryable ?? 0), 1)
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "6",
              md: "3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_card, {
                  variant: "tonal",
                  color: "error"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_card_text, { class: "py-2" }, {
                      default: _withCtx(() => [
                        _cache[13] || (_cache[13] = _createElementVNode("div", { class: "text-caption" }, "超限放弃", -1)),
                        _createElementVNode("div", _hoisted_5, _toDisplayString(counts.value.exhausted ?? 0), 1)
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "6",
              md: "3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_card, {
                  variant: "tonal",
                  color: "grey"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_card_text, { class: "py-2" }, {
                      default: _withCtx(() => [
                        _cache[14] || (_cache[14] = _createElementVNode("div", { class: "text-caption" }, "上次扫描", -1)),
                        _createElementVNode("div", _hoisted_6, _toDisplayString(fmtTime(runs.value.ts)), 1),
                        _createElementVNode("div", _hoisted_7, " 缓存" + _toDisplayString(runs.value.from_cache ?? 0) + "｜成功" + _toDisplayString(runs.value.ok ?? 0) + "｜失败" + _toDisplayString(runs.value.failed ?? 0), 1)
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createElementVNode("div", _hoisted_8, [
          _createVNode(_component_v_tabs, {
            density: "compact",
            "model-value": "failed",
            "bg-color": "transparent",
            "show-arrows": ""
          }, {
            default: _withCtx(() => [
              (_openBlock(), _createElementBlock(_Fragment, null, _renderList(tabs, (t) => {
                return _createVNode(_component_v_tab, {
                  key: t.key,
                  value: t.key,
                  onClick: ($event) => switchTab(t.key)
                }, {
                  default: _withCtx(() => [
                    _createTextVNode(_toDisplayString(t.label) + " ", 1),
                    t.key === "failed" ? (_openBlock(), _createBlock(_component_v_badge, {
                      key: 0,
                      content: counts.value.failed || 0,
                      inline: "",
                      class: "ml-1",
                      color: "warning"
                    }, null, 8, ["content"])) : _createCommentVNode("", true)
                  ]),
                  _: 2
                }, 1032, ["value", "onClick"]);
              }), 64))
            ]),
            _: 1
          }),
          _createVNode(_component_v_spacer),
          _createVNode(_component_v_text_field, {
            modelValue: search.value,
            "onUpdate:modelValue": [
              _cache[1] || (_cache[1] = ($event) => search.value = $event),
              _cache[2] || (_cache[2] = ($event) => onPage(0))
            ],
            density: "compact",
            "hide-details": "",
            placeholder: "搜索路径/文件名",
            "prepend-inner-icon": "mdi-magnify",
            style: { "max-width": "260px" },
            clearable: ""
          }, null, 8, ["modelValue"]),
          _createVNode(_component_v_btn, {
            size: "small",
            variant: "tonal",
            color: "warning",
            loading: busy.value,
            onClick: retryAllFailed
          }, {
            default: _withCtx(() => [..._cache[15] || (_cache[15] = [
              _createTextVNode("重试全部失败", -1)
            ])]),
            _: 1
          }, 8, ["loading"]),
          _createVNode(_component_v_btn, {
            size: "small",
            variant: "tonal",
            color: "info",
            loading: busy.value,
            onClick: _cache[3] || (_cache[3] = ($event) => scanNow(false))
          }, {
            default: _withCtx(() => [..._cache[16] || (_cache[16] = [
              _createTextVNode("增量扫描", -1)
            ])]),
            _: 1
          }, 8, ["loading"]),
          _createVNode(_component_v_btn, {
            size: "small",
            variant: "tonal",
            color: "error",
            loading: busy.value,
            onClick: _cache[4] || (_cache[4] = ($event) => scanNow(true))
          }, {
            default: _withCtx(() => [..._cache[17] || (_cache[17] = [
              _createTextVNode("全量覆盖", -1)
            ])]),
            _: 1
          }, 8, ["loading"])
        ]),
        _createVNode(_component_v_data_table, {
          headers: [
            { title: "文件名", key: "name", sortable: false },
            { title: "状态", key: "status", width: 130 },
            { title: "原因/分数", key: "reason", sortable: false },
            { title: "重试", key: "retries", width: 70 },
            { title: "大小", key: "size", width: 90 },
            { title: "修改时间", key: "mtime", width: 180 },
            { title: "操作", key: "actions", sortable: false, width: 90 }
          ],
          items: rows.value,
          loading: loading.value,
          "footer-props": { "items-per-page-options": [20, 50, 100] },
          "items-per-page": limit.value,
          density: "compact",
          class: "elevation-1",
          "onUpdate:page": _cache[5] || (_cache[5] = ($event) => onPage(($event - 1) * limit.value))
        }, {
          "item.name": _withCtx(({ item }) => [
            _createElementVNode("div", {
              title: item.path
            }, _toDisplayString(item.name), 9, _hoisted_9),
            _createElementVNode("div", _hoisted_10, _toDisplayString(item.path), 1)
          ]),
          "item.status": _withCtx(({ item }) => [
            _createVNode(_component_v_chip, {
              size: "x-small",
              variant: "tonal",
              color: item.status === "done" ? "success" : item.status === "failed" ? "error" : "grey"
            }, {
              default: _withCtx(() => [
                _createTextVNode(_toDisplayString(item.status), 1)
              ]),
              _: 2
            }, 1032, ["color"])
          ]),
          "item.reason": _withCtx(({ item }) => [
            item.reason ? (_openBlock(), _createElementBlock("span", _hoisted_11, _toDisplayString(item.reason), 1)) : _createCommentVNode("", true),
            item.score != null ? (_openBlock(), _createElementBlock("span", _hoisted_12, " · score " + _toDisplayString(item.score), 1)) : item.source ? (_openBlock(), _createElementBlock("span", _hoisted_13, _toDisplayString(item.source), 1)) : !item.reason ? (_openBlock(), _createElementBlock("span", _hoisted_14, "-")) : _createCommentVNode("", true)
          ]),
          "item.mtime": _withCtx(({ item }) => [
            _createTextVNode(_toDisplayString(fmtTime(item.mtime)), 1)
          ]),
          "item.size": _withCtx(({ item }) => [
            _createTextVNode(_toDisplayString(fmtSize(item.size)), 1)
          ]),
          "item.actions": _withCtx(({ item }) => [
            _createVNode(_component_v_btn, {
              size: "x-small",
              variant: "text",
              color: "primary",
              loading: busy.value,
              onClick: ($event) => retryRow(item)
            }, {
              default: _withCtx(() => [..._cache[18] || (_cache[18] = [
                _createTextVNode("重试", -1)
              ])]),
              _: 1
            }, 8, ["loading", "onClick"])
          ]),
          "no-data": _withCtx(() => [..._cache[19] || (_cache[19] = [
            _createElementVNode("div", { class: "text-center pa-6 text-grey" }, "该桶暂无数据", -1)
          ])]),
          _: 1
        }, 8, ["items", "loading", "items-per-page"]),
        _createVNode(_component_v_snackbar, {
          modelValue: snackbar.value.show,
          "onUpdate:modelValue": _cache[6] || (_cache[6] = ($event) => snackbar.value.show = $event),
          color: snackbar.value.color,
          timeout: "3000",
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

const Page = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-42db8f93"]]);

export { Page as default };
