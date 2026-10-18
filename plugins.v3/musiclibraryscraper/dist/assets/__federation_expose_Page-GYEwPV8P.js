import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { F as FLOW_STEPS, _ as _export_sfc } from './_plugin-vue_export-helper-BDo7-ek5.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,createTextVNode:_createTextVNode,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,renderList:_renderList,Fragment:_Fragment,createElementBlock:_createElementBlock,toDisplayString:_toDisplayString,unref:_unref} = await importShared('vue');

const _hoisted_1 = { class: "mls-head" };
const _hoisted_2 = { class: "mls-body" };
const _hoisted_3 = { class: "mls-nav" };
const _hoisted_4 = { class: "mls-content" };
const _hoisted_5 = {
  key: 0,
  class: "mls-pane"
};
const _hoisted_6 = { class: "text-h5" };
const _hoisted_7 = { class: "text-h5" };
const _hoisted_8 = { class: "text-h5" };
const _hoisted_9 = { class: "text-h5" };
const _hoisted_10 = { class: "mls-section" };
const _hoisted_11 = { class: "text-caption text-medium-emphasis mb-1" };
const _hoisted_12 = { key: 0 };
const _hoisted_13 = { class: "text-body-2" };
const _hoisted_14 = { class: "text-body-2" };
const _hoisted_15 = {
  key: 0,
  class: "text-caption text-warning mt-1"
};
const _hoisted_16 = {
  key: 1,
  class: "text-caption text-grey mt-1"
};
const _hoisted_17 = { class: "text-body-2" };
const _hoisted_18 = { class: "text-caption text-grey" };
const _hoisted_19 = { class: "text-caption text-grey" };
const _hoisted_20 = { class: "text-caption text-grey" };
const _hoisted_21 = {
  key: 0,
  class: "mls-section"
};
const _hoisted_22 = { class: "mls-reason-text" };
const _hoisted_23 = { class: "text-caption text-no-wrap" };
const _hoisted_24 = { class: "mls-section" };
const _hoisted_25 = { class: "mls-flow" };
const _hoisted_26 = { class: "mls-flow-index" };
const _hoisted_27 = { class: "mls-flow-body" };
const _hoisted_28 = { class: "text-body-2 font-weight-medium" };
const _hoisted_29 = { class: "text-caption text-grey" };
const _hoisted_30 = { class: "d-flex align-center gap-2 mt-4" };
const _hoisted_31 = {
  key: 1,
  class: "mls-pane"
};
const _hoisted_32 = { class: "d-flex align-center flex-wrap gap-2 mb-2" };
const _hoisted_33 = { class: "text-subtitle-2" };
const _hoisted_34 = { class: "text-caption text-grey" };
const _hoisted_35 = ["title"];
const _hoisted_36 = {
  class: "text-caption text-grey text-truncate",
  style: { "max-width": "480px" }
};
const _hoisted_37 = {
  key: 0,
  class: "text-caption"
};
const _hoisted_38 = {
  key: 1,
  class: "text-caption text-grey"
};
const _hoisted_39 = {
  key: 2,
  class: "text-caption text-grey"
};
const _hoisted_40 = { key: 3 };
const {ref,onMounted,computed,inject} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Page",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    nativeSubscribe: { type: Function, default: null },
    showSwitch: { type: Boolean, default: true }
  },
  emits: ["action", "switch", "close", "layout"],
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
    const view = ref("overview");
    const bucket = ref("failed");
    const search = ref("");
    const page = ref(1);
    const limit = ref(50);
    const offset = ref(0);
    let searchTimer = null;
    const buckets = [
      { key: "done", label: "已处理", icon: "mdi-check-circle-outline", hint: "已经写好标签的文件（本轮与往轮累计）" },
      { key: "failed", label: "失败待重试", icon: "mdi-alert-circle-outline", hint: "没查到或没达阈值，下一轮还会再试" },
      { key: "pending", label: "待处理", icon: "mdi-progress-clock", hint: "账本里没有、下一轮要问的文件" }
    ];
    const pageText = computed(() => {
      if (!total.value) return "共 0 条";
      const from = offset.value + 1;
      const to = Math.min(offset.value + limit.value, total.value);
      return `第 ${from}–${to} 条 / 共 ${total.value} 条`;
    });
    const STATUS_TEXT = {
      done: "已处理",
      failed: "失败",
      pending: "待处理"
    };
    const SOURCE_TEXT = {
      netease: "网易云",
      qmusic: "QQ 音乐",
      kuwo: "酷我",
      kugou: "酷狗",
      migu: "咪咕",
      musicbrainz: "MusicBrainz",
      smart_tag: "智能"
    };
    function statusText(value) {
      return STATUS_TEXT[value] || value;
    }
    function sourceText(value) {
      return SOURCE_TEXT[value] || value;
    }
    function notify(msg, color = "info") {
      if (toast) {
        const fn = toast[color] || toast.info;
        fn.call(toast, msg);
        return;
      }
      snackbar.value = { show: true, text: msg, color };
    }
    function query(params) {
      const pairs = [];
      for (const [key, value] of Object.entries(params)) {
        if (value === void 0 || value === null || value === "") continue;
        pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
      }
      return pairs.length ? `?${pairs.join("&")}` : "";
    }
    async function loadStatus() {
      try {
        status.value = await props.api.get(`${base.value}/status`);
      } catch (e) {
        notify(`状态加载失败：${e?.message || e}`, "error");
      }
    }
    async function loadFiles(retryOutOfRange = true) {
      loading.value = true;
      try {
        const data = await props.api.get(
          `${base.value}/files${query({
            bucket: bucket.value,
            limit: limit.value,
            offset: offset.value,
            search: search.value
          })}`
        );
        rows.value = data?.items || [];
        total.value = data?.total || 0;
        if (retryOutOfRange && total.value > 0 && !rows.value.length) {
          const lastPage = Math.max(1, Math.ceil(total.value / limit.value));
          if (page.value > lastPage) {
            page.value = lastPage;
            offset.value = (lastPage - 1) * limit.value;
            await loadFiles(false);
            return;
          }
        }
      } catch (e) {
        rows.value = [];
        total.value = 0;
        notify(`列表加载失败：${e?.message || e}`, "error");
      } finally {
        loading.value = false;
      }
    }
    async function refresh() {
      await Promise.all([loadStatus(), view.value === "overview" ? Promise.resolve() : loadFiles()]);
      emit("action");
    }
    function selectView(key) {
      view.value = key;
      if (key === "overview") return;
      if (bucket.value !== key) {
        bucket.value = key;
        search.value = "";
        page.value = 1;
        offset.value = 0;
      }
      loadFiles();
    }
    function retryResult(res, label = "") {
      if (res?.accepted) {
        const rejected = res?.rejected ? `，忽略 ${res.rejected} 个库外路径` : "";
        notify(`已提交重试 ${res.count} 个${label}${rejected}`);
      } else if (res?.reason === "out_of_library") {
        notify("这些路径不在配置的音乐库范围内，已拒绝", "warning");
      } else if (res?.reason === "no_paths") {
        notify("没有可重试的路径", "warning");
      } else {
        notify("已有扫描在运行，请稍后再试", "warning");
      }
    }
    async function retryPaths(paths, label) {
      if (!paths.length) return;
      busy.value = true;
      try {
        const res = await props.api.post(`${base.value}/retry`, { paths });
        retryResult(res, label);
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
        retryResult(res, "失败项");
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
    function onPage(nextPage) {
      page.value = Math.max(1, nextPage);
      offset.value = (page.value - 1) * limit.value;
      loadFiles();
    }
    function onItemsPerPage(next) {
      limit.value = next > 0 ? next : 50;
      page.value = 1;
      offset.value = 0;
      loadFiles();
    }
    function onSearch() {
      if (searchTimer) clearTimeout(searchTimer);
      searchTimer = setTimeout(() => {
        page.value = 1;
        offset.value = 0;
        loadFiles();
      }, 300);
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
    const cfg = computed(() => status.value?.config || {});
    const lastTargeted = computed(() => status.value?.last_targeted || {});
    const running = computed(() => !!status.value?.running);
    const failureReasons = computed(() => status.value?.failure_reasons || []);
    const minScore = computed(() => cfg.value?.min_score ?? 3);
    function reasonLabel(group) {
      const parts = [`${group.count} 个`];
      const best = group.best_score;
      if (typeof best === "number") {
        parts.push(`最高 ${best} 分`);
        const gap = minScore.value - best;
        if (gap > 0) parts.push(`阈值降 ${gap} 分可命中`);
      }
      if (!group.retryable) parts.push("均已超限");
      return parts.join(" · ");
    }
    const navItems = computed(() => [
      { key: "overview", title: "概览", icon: "mdi-view-dashboard-outline", badge: 0 },
      ...buckets.map((b) => ({
        key: b.key,
        title: b.label,
        icon: b.icon,
        badge: b.key === "failed" ? Number(counts.value.failed ?? 0) : b.key === "done" ? Number(counts.value.done ?? 0) : 0
      }))
    ]);
    const sourceNames = computed(
      () => (cfg.value?.mtw_resource || []).map((s) => sourceText(s)).join(" → ") || "全部源"
    );
    const activeBucket = computed(() => buckets.find((b) => b.key === bucket.value));
    onMounted(() => {
      emit("layout", { maxWidth: "86rem" });
      refresh();
    });
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_avatar = _resolveComponent("v-avatar");
      const _component_v_spacer = _resolveComponent("v-spacer");
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_divider = _resolveComponent("v-divider");
      const _component_v_list_item_title = _resolveComponent("v-list-item-title");
      const _component_v_list_item = _resolveComponent("v-list-item");
      const _component_v_list = _resolveComponent("v-list");
      const _component_v_card_text = _resolveComponent("v-card-text");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_data_table_server = _resolveComponent("v-data-table-server");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock(), _createBlock(_component_v_card, {
        flat: "",
        class: "mls-page"
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
                  icon: "mdi-music-circle-outline",
                  size: "22"
                })
              ]),
              _: 1
            }),
            _cache[10] || (_cache[10] = _createElementVNode("div", { class: "mls-head-text" }, [
              _createElementVNode("div", { class: "text-h6" }, "MusicTagWeb音乐库刮削"),
              _createElementVNode("div", { class: "text-caption text-medium-emphasis" }, " 枚举库内音频 → 读本地身份 → 逐源问 MTW → 打分定阈值 → 宿主原生写入 → 记账 ")
            ], -1)),
            _createVNode(_component_v_spacer),
            running.value ? (_openBlock(), _createBlock(_component_v_chip, {
              key: 0,
              color: "info",
              size: "small",
              variant: "tonal",
              "prepend-icon": "mdi-progress-clock",
              class: "mr-2"
            }, {
              default: _withCtx(() => [..._cache[7] || (_cache[7] = [
                _createTextVNode(" 扫描进行中 ", -1)
              ])]),
              _: 1
            })) : _createCommentVNode("", true),
            props.showSwitch ? (_openBlock(), _createBlock(_component_v_btn, {
              key: 1,
              size: "small",
              variant: "text",
              class: "text-none",
              "prepend-icon": "mdi-cog-outline",
              onClick: _cache[0] || (_cache[0] = ($event) => emit("switch"))
            }, {
              default: _withCtx(() => [..._cache[8] || (_cache[8] = [
                _createTextVNode("配置", -1)
              ])]),
              _: 1
            })) : _createCommentVNode("", true),
            _createVNode(_component_v_btn, {
              size: "small",
              variant: "text",
              class: "text-none",
              loading: loading.value || busy.value,
              "prepend-icon": "mdi-refresh",
              onClick: refresh
            }, {
              default: _withCtx(() => [..._cache[9] || (_cache[9] = [
                _createTextVNode("刷新", -1)
              ])]),
              _: 1
            }, 8, ["loading"])
          ]),
          _createVNode(_component_v_divider),
          _createElementVNode("div", _hoisted_2, [
            _createElementVNode("nav", _hoisted_3, [
              _createVNode(_component_v_list, {
                density: "comfortable",
                nav: "",
                class: "py-2"
              }, {
                default: _withCtx(() => [
                  (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(navItems.value, (item) => {
                    return _openBlock(), _createBlock(_component_v_list_item, {
                      key: item.key,
                      active: view.value === item.key,
                      color: "primary",
                      rounded: "lg",
                      class: "mls-nav-item",
                      onClick: ($event) => selectView(item.key)
                    }, {
                      prepend: _withCtx(() => [
                        _createVNode(_component_v_icon, {
                          icon: item.icon,
                          class: "mr-2"
                        }, null, 8, ["icon"])
                      ]),
                      append: _withCtx(() => [
                        item.badge ? (_openBlock(), _createBlock(_component_v_chip, {
                          key: 0,
                          size: "x-small",
                          variant: "tonal",
                          color: item.key === "failed" ? "warning" : "grey"
                        }, {
                          default: _withCtx(() => [
                            _createTextVNode(_toDisplayString(item.badge), 1)
                          ]),
                          _: 2
                        }, 1032, ["color"])) : _createCommentVNode("", true)
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
                  }), 128))
                ]),
                _: 1
              })
            ]),
            _createElementVNode("section", _hoisted_4, [
              view.value === "overview" ? (_openBlock(), _createElementBlock("div", _hoisted_5, [
                _createVNode(_component_v_row, { dense: "" }, {
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
                                _createElementVNode("div", _hoisted_6, _toDisplayString(counts.value.done ?? 0), 1)
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
                                _createElementVNode("div", _hoisted_7, _toDisplayString(counts.value.retryable ?? 0), 1)
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
                                _createElementVNode("div", _hoisted_8, _toDisplayString(counts.value.exhausted ?? 0), 1)
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
                                _cache[14] || (_cache[14] = _createElementVNode("div", { class: "text-caption" }, "待处理", -1)),
                                _createElementVNode("div", _hoisted_9, _toDisplayString(counts.value.pending_unknown ?? 0), 1)
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
                _createElementVNode("div", _hoisted_10, [
                  _cache[16] || (_cache[16] = _createElementVNode("div", { class: "mls-section-title" }, "最近一轮", -1)),
                  _createVNode(_component_v_row, { dense: "" }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_col, {
                        cols: "12",
                        md: "6"
                      }, {
                        default: _withCtx(() => [
                          _createVNode(_component_v_card, {
                            variant: "outlined",
                            class: "pa-3 mls-fill"
                          }, {
                            default: _withCtx(() => [
                              _createElementVNode("div", _hoisted_11, [
                                _createTextVNode(" 扫描时间 " + _toDisplayString(fmtTime(runs.value.ts)) + " ", 1),
                                cfg.value.cron ? (_openBlock(), _createElementBlock("span", _hoisted_12, "｜定时 " + _toDisplayString(cfg.value.cron) + "（下次 " + _toDisplayString(cfg.value.cron_next) + "）", 1)) : _createCommentVNode("", true)
                              ]),
                              _createElementVNode("div", _hoisted_13, " 发现 " + _toDisplayString(runs.value.scanned ?? 0) + "｜缓存跳过 " + _toDisplayString(runs.value.from_cache ?? 0) + "｜已有tag跳过 " + _toDisplayString(runs.value.skipped_tagged ?? 0), 1),
                              _createElementVNode("div", _hoisted_14, " 成功 " + _toDisplayString(runs.value.ok ?? 0) + "｜失败 " + _toDisplayString(runs.value.failed ?? 0) + "｜MTW 故障 " + _toDisplayString(runs.value.transport ?? 0) + "｜处理异常 " + _toDisplayString(runs.value.error ?? 0), 1),
                              runs.value.aborted ? (_openBlock(), _createElementBlock("div", _hoisted_15, " MTW 连续故障，本轮已中止，未处理的留到下一轮 ")) : _createCommentVNode("", true),
                              lastTargeted.value.ts ? (_openBlock(), _createElementBlock("div", _hoisted_16, " 最近定向重试 " + _toDisplayString(fmtTime(lastTargeted.value.ts)) + "：成功 " + _toDisplayString(lastTargeted.value.ok ?? 0) + "｜失败 " + _toDisplayString(lastTargeted.value.failed ?? 0), 1)) : _createCommentVNode("", true)
                            ]),
                            _: 1
                          })
                        ]),
                        _: 1
                      }),
                      _createVNode(_component_v_col, {
                        cols: "12",
                        md: "6"
                      }, {
                        default: _withCtx(() => [
                          _createVNode(_component_v_card, {
                            variant: "outlined",
                            class: "pa-3 mls-fill"
                          }, {
                            default: _withCtx(() => [
                              _cache[15] || (_cache[15] = _createElementVNode("div", { class: "text-caption text-medium-emphasis mb-1" }, "本轮生效的配置", -1)),
                              _createElementVNode("div", _hoisted_17, _toDisplayString(cfg.value.mode === "force_all" ? "强制覆盖" : "只补缺") + "｜最低匹配分数 " + _toDisplayString(cfg.value.min_score) + "｜" + _toDisplayString(sourceNames.value), 1),
                              _createElementVNode("div", _hoisted_18, " MTW " + _toDisplayString(cfg.value.mtw_base_url || "未配置") + "｜账密 " + _toDisplayString(cfg.value.has_username ? "已配" : "未配"), 1),
                              _createElementVNode("div", _hoisted_19, " 库路径 " + _toDisplayString((cfg.value.scraper_paths || []).length) + " 个｜排除 " + _toDisplayString((cfg.value.exclude_paths || []).length) + " 个 ", 1),
                              _createElementVNode("div", _hoisted_20, " 实际处理的扩展名：" + _toDisplayString((cfg.value.audio_extensions || []).join(" / ")), 1)
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
                failureReasons.value.length ? (_openBlock(), _createElementBlock("div", _hoisted_21, [
                  _cache[17] || (_cache[17] = _createElementVNode("div", { class: "mls-section-title" }, "失败原因分布", -1)),
                  _createVNode(_component_v_card, {
                    variant: "outlined",
                    class: "mls-reason"
                  }, {
                    default: _withCtx(() => [
                      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(failureReasons.value, (group) => {
                        return _openBlock(), _createElementBlock("div", {
                          key: group.reason,
                          class: "mls-reason-row"
                        }, [
                          _createElementVNode("span", _hoisted_22, _toDisplayString(group.reason), 1),
                          _createElementVNode("span", _hoisted_23, _toDisplayString(reasonLabel(group)), 1)
                        ]);
                      }), 128))
                    ]),
                    _: 1
                  }),
                  _cache[18] || (_cache[18] = _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 未达阈值的按上面的分数调「最低匹配分数」；超限的可用下方「重试全部失败」再试。 ", -1))
                ])) : _createCommentVNode("", true),
                _createElementVNode("div", _hoisted_24, [
                  _cache[19] || (_cache[19] = _createElementVNode("div", { class: "mls-section-title" }, "流水线", -1)),
                  _createElementVNode("div", _hoisted_25, [
                    (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(_unref(FLOW_STEPS), (step, i) => {
                      return _openBlock(), _createElementBlock("div", {
                        key: step.key,
                        class: "mls-flow-step"
                      }, [
                        _createElementVNode("div", _hoisted_26, _toDisplayString(i + 1), 1),
                        _createElementVNode("div", _hoisted_27, [
                          _createElementVNode("div", _hoisted_28, [
                            _createVNode(_component_v_icon, {
                              icon: step.icon,
                              size: "16",
                              class: "mr-1"
                            }, null, 8, ["icon"]),
                            _createTextVNode(_toDisplayString(step.title), 1)
                          ]),
                          _createElementVNode("div", _hoisted_29, _toDisplayString(step.desc), 1)
                        ])
                      ]);
                    }), 128))
                  ])
                ]),
                _createElementVNode("div", _hoisted_30, [
                  _createVNode(_component_v_spacer),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "warning",
                    loading: busy.value,
                    onClick: retryAllFailed
                  }, {
                    default: _withCtx(() => [..._cache[20] || (_cache[20] = [
                      _createTextVNode("重试全部失败", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"]),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "info",
                    loading: busy.value,
                    onClick: _cache[1] || (_cache[1] = ($event) => scanNow(false))
                  }, {
                    default: _withCtx(() => [..._cache[21] || (_cache[21] = [
                      _createTextVNode("增量扫描", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"]),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "error",
                    loading: busy.value,
                    onClick: _cache[2] || (_cache[2] = ($event) => scanNow(true))
                  }, {
                    default: _withCtx(() => [..._cache[22] || (_cache[22] = [
                      _createTextVNode("全量覆盖", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"])
                ])
              ])) : (_openBlock(), _createElementBlock("div", _hoisted_31, [
                _createElementVNode("div", _hoisted_32, [
                  _createElementVNode("div", null, [
                    _createElementVNode("div", _hoisted_33, _toDisplayString(activeBucket.value?.label), 1),
                    _createElementVNode("div", _hoisted_34, _toDisplayString(activeBucket.value?.hint), 1)
                  ]),
                  _createVNode(_component_v_spacer),
                  _createVNode(_component_v_text_field, {
                    modelValue: search.value,
                    "onUpdate:modelValue": [
                      _cache[3] || (_cache[3] = ($event) => search.value = $event),
                      onSearch
                    ],
                    density: "compact",
                    "hide-details": "",
                    placeholder: "搜索路径/文件名",
                    "prepend-inner-icon": "mdi-magnify",
                    style: { "max-width": "260px" },
                    clearable: ""
                  }, null, 8, ["modelValue"]),
                  bucket.value === "failed" ? (_openBlock(), _createBlock(_component_v_btn, {
                    key: 0,
                    size: "small",
                    variant: "tonal",
                    color: "warning",
                    loading: busy.value,
                    onClick: retryAllFailed
                  }, {
                    default: _withCtx(() => [..._cache[23] || (_cache[23] = [
                      _createTextVNode("重试全部失败", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"])) : _createCommentVNode("", true),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "info",
                    loading: busy.value,
                    onClick: _cache[4] || (_cache[4] = ($event) => scanNow(false))
                  }, {
                    default: _withCtx(() => [..._cache[24] || (_cache[24] = [
                      _createTextVNode("增量扫描", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"]),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "error",
                    loading: busy.value,
                    onClick: _cache[5] || (_cache[5] = ($event) => scanNow(true))
                  }, {
                    default: _withCtx(() => [..._cache[25] || (_cache[25] = [
                      _createTextVNode("全量覆盖", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"])
                ]),
                _createVNode(_component_v_data_table_server, {
                  headers: [
                    { title: "文件名", key: "name", sortable: false },
                    { title: "状态", key: "status", width: 130, sortable: false },
                    { title: "原因/分数", key: "reason", sortable: false },
                    { title: "重试", key: "retries", width: 70, sortable: false },
                    { title: "大小", key: "size", width: 90, sortable: false },
                    { title: "修改时间", key: "mtime", width: 180, sortable: false },
                    { title: "操作", key: "actions", sortable: false, width: 90 }
                  ],
                  items: rows.value,
                  "items-length": total.value,
                  loading: loading.value,
                  "items-per-page-text": "每页条数",
                  "items-per-page-options": [20, 50, 100],
                  "page-text": pageText.value,
                  page: page.value,
                  "items-per-page": limit.value,
                  density: "compact",
                  class: "elevation-1",
                  "onUpdate:page": onPage,
                  "onUpdate:itemsPerPage": onItemsPerPage
                }, {
                  "item.name": _withCtx(({ item }) => [
                    _createElementVNode("div", {
                      title: item.path
                    }, _toDisplayString(item.name), 9, _hoisted_35),
                    _createElementVNode("div", _hoisted_36, _toDisplayString(item.path), 1)
                  ]),
                  "item.status": _withCtx(({ item }) => [
                    _createVNode(_component_v_chip, {
                      size: "x-small",
                      variant: "tonal",
                      color: item.status === "done" ? "success" : item.status === "failed" ? "error" : "grey"
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode(_toDisplayString(statusText(item.status)), 1)
                      ]),
                      _: 2
                    }, 1032, ["color"])
                  ]),
                  "item.reason": _withCtx(({ item }) => [
                    item.reason ? (_openBlock(), _createElementBlock("span", _hoisted_37, _toDisplayString(item.reason), 1)) : _createCommentVNode("", true),
                    item.score != null ? (_openBlock(), _createElementBlock("span", _hoisted_38, " · 分数 " + _toDisplayString(item.score), 1)) : item.source ? (_openBlock(), _createElementBlock("span", _hoisted_39, " · " + _toDisplayString(sourceText(item.source)), 1)) : !item.reason ? (_openBlock(), _createElementBlock("span", _hoisted_40, "-")) : _createCommentVNode("", true)
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
                      default: _withCtx(() => [..._cache[26] || (_cache[26] = [
                        _createTextVNode("重试", -1)
                      ])]),
                      _: 1
                    }, 8, ["loading", "onClick"])
                  ]),
                  "no-data": _withCtx(() => [..._cache[27] || (_cache[27] = [
                    _createElementVNode("div", { class: "text-center pa-6 text-grey" }, "该桶暂无数据", -1)
                  ])]),
                  _: 1
                }, 8, ["items", "items-length", "loading", "page-text", "page", "items-per-page"])
              ]))
            ])
          ]),
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
        ]),
        _: 1
      });
    };
  }
});

const Page = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-029c5910"]]);

export { Page as default };
