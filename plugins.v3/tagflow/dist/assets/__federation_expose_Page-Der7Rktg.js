import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { s as stageCounts, F as FlowPipeline, a as FLOW_STEPS, _ as _export_sfc } from './flow-BVjcb_2i.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,createTextVNode:_createTextVNode,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,renderList:_renderList,Fragment:_Fragment,createElementBlock:_createElementBlock,toDisplayString:_toDisplayString,unref:_unref,withModifiers:_withModifiers} = await importShared('vue');

const _hoisted_1 = { class: "mls-head" };
const _hoisted_2 = { class: "mls-body" };
const _hoisted_3 = { class: "mls-nav" };
const _hoisted_4 = { class: "mls-content" };
const _hoisted_5 = {
  key: 0,
  class: "mls-pane"
};
const _hoisted_6 = { class: "mls-section" };
const _hoisted_7 = { class: "mls-flow-head" };
const _hoisted_8 = { class: "text-caption text-grey" };
const _hoisted_9 = { class: "text-h5" };
const _hoisted_10 = { class: "text-h5" };
const _hoisted_11 = { class: "text-h5" };
const _hoisted_12 = { class: "text-h5" };
const _hoisted_13 = { class: "mls-section" };
const _hoisted_14 = { class: "text-caption text-medium-emphasis mb-1" };
const _hoisted_15 = {
  key: 0,
  class: "text-caption text-grey"
};
const _hoisted_16 = {
  key: 1,
  class: "text-caption text-warning mt-1"
};
const _hoisted_17 = {
  key: 2,
  class: "text-caption text-grey mt-1"
};
const _hoisted_18 = { class: "text-body-2" };
const _hoisted_19 = { class: "text-caption text-grey" };
const _hoisted_20 = { class: "text-caption text-grey" };
const _hoisted_21 = {
  key: 0,
  class: "mls-section"
};
const _hoisted_22 = { class: "mls-reason-text" };
const _hoisted_23 = { class: "text-caption text-no-wrap" };
const _hoisted_24 = { class: "d-flex align-center gap-2 mt-4" };
const _hoisted_25 = {
  key: 1,
  class: "mls-pane"
};
const _hoisted_26 = { class: "d-flex align-center flex-wrap gap-2 mb-2" };
const _hoisted_27 = { class: "text-subtitle-2" };
const _hoisted_28 = { class: "text-caption text-grey" };
const _hoisted_29 = ["title"];
const _hoisted_30 = {
  class: "text-caption text-grey text-truncate",
  style: { "max-width": "480px" }
};
const _hoisted_31 = {
  key: 0,
  class: "mt-1"
};
const _hoisted_32 = { class: "text-caption text-primary ml-1" };
const _hoisted_33 = {
  key: 0,
  class: "text-caption"
};
const _hoisted_34 = {
  key: 1,
  class: "text-caption"
};
const _hoisted_35 = {
  key: 2,
  class: "text-caption text-grey"
};
const _hoisted_36 = {
  key: 3,
  class: "text-caption text-grey"
};
const _hoisted_37 = { key: 4 };
const _hoisted_38 = { class: "mls-dlg-head" };
const _hoisted_39 = { class: "mls-dlg-head-text" };
const _hoisted_40 = { class: "text-subtitle-1" };
const _hoisted_41 = { class: "text-caption text-grey text-truncate" };
const _hoisted_42 = { class: "text-no-wrap" };
const _hoisted_43 = { class: "mls-diff-old" };
const _hoisted_44 = { class: "mls-diff-new" };
const _hoisted_45 = {
  key: 1,
  class: "text-caption text-grey mb-4"
};
const _hoisted_46 = {
  key: 2,
  class: "text-caption text-grey mt-n3 mb-4"
};
const _hoisted_47 = { class: "mls-kv" };
const _hoisted_48 = { class: "text-caption text-grey" };
const _hoisted_49 = { class: "mls-kv-value" };
const _hoisted_50 = { class: "mls-kv" };
const _hoisted_51 = { class: "mls-kv-value" };
const _hoisted_52 = { class: "mls-kv" };
const _hoisted_53 = { class: "mls-kv-value" };
const _hoisted_54 = {
  key: 3,
  class: "text-caption text-grey mt-2"
};
const _hoisted_55 = { class: "mls-dlg-head" };
const _hoisted_56 = { class: "mls-dlg-head-text" };
const _hoisted_57 = { class: "text-caption text-grey text-truncate" };
const _hoisted_58 = {
  key: 1,
  class: "mls-chips mb-3"
};
const _hoisted_59 = {
  key: 2,
  class: "text-caption text-grey mb-3"
};
const _hoisted_60 = { class: "mls-lyrics mt-2" };
const _hoisted_61 = { class: "d-flex align-center" };
const _hoisted_62 = {
  key: 0,
  class: "text-caption text-grey mt-3"
};
const _hoisted_63 = { class: "text-caption text-grey" };
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
  setup(__props, { expose: __expose, emit: __emit }) {
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
    const writtenOnly = ref(false);
    let searchTimer = null;
    const buckets = [
      { key: "done", label: "已处理", icon: "mdi-check-circle-outline", hint: "已经写好标签的文件（本轮与往轮累计）" },
      { key: "failed", label: "失败待重试", icon: "mdi-alert-circle-outline", hint: "没查到或没达阈值，下一轮还会再试" },
      { key: "pending", label: "待处理", icon: "mdi-progress-clock", hint: "账本里没有、下一轮要问的文件" },
      { key: "deficient", label: "有缺项", icon: "mdi-file-document-alert-outline", hint: "已处理但封面/歌词等仍缺失，可单项编辑补齐" }
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
      pending: "待处理",
      deficient: "有缺项"
    };
    const SOURCE_TEXT = {
      netease: "网易云",
      qmusic: "QQ 音乐",
      kugou: "酷狗",
      migu: "咪咕",
      musicbrainz: "MusicBrainz"
    };
    function statusText(value) {
      return STATUS_TEXT[value] || value;
    }
    function sourceText(value) {
      return SOURCE_TEXT[value] || value;
    }
    const FIELD_TEXT = {
      title: "曲名",
      artist: "艺人",
      album: "专辑",
      year: "年份",
      tracknumber: "音轨号",
      cover: "封面",
      lyrics: "歌词"
    };
    function fieldText(value) {
      return FIELD_TEXT[value] || value;
    }
    const EDIT_FIELDS = [
      { key: "title", cols: "12 md-6" },
      { key: "artist", cols: "12 md-6" },
      { key: "album", cols: "12 md-4" },
      { key: "year", cols: "6 md-4" },
      { key: "tracknumber", cols: "6 md-4" }
    ];
    function changedFieldNames(set) {
      return Object.keys(set || {}).map(fieldText).join("·");
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
            search: search.value,
            // 空值由 query() 丢掉，等于不筛；只对 done 桶出现在界面上，后端也只在 done 应用
            written: writtenOnly.value ? "1" : ""
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
    __expose({ refresh });
    function selectView(key) {
      view.value = key;
      if (key === "overview") return;
      if (bucket.value !== key) {
        bucket.value = key;
        search.value = "";
        page.value = 1;
        offset.value = 0;
        writtenOnly.value = false;
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
    async function loadFile(path) {
      try {
        const res = await props.api.get(`${base.value}/file${query({ path })}`);
        if (res?.accepted) return res;
        if (res?.reason === "not_found") notify("这个文件已经不在了，账本里的记录已过期", "warning");
        else if (res?.reason === "unreadable") notify("读不出这个文件的标签，可能不是音频或容器不认识", "warning");
        else if (res?.reason === "out_of_library") notify("该文件不在配置的音乐库范围内", "warning");
        else notify("读取文件详情失败", "error");
      } catch (e) {
        notify(`读取文件详情失败：${e?.message || e}`, "error");
      }
      return null;
    }
    const editDialog = ref(false);
    const editLoading = ref(false);
    const editRow = ref(null);
    const editForm = ref({ title: "", artist: "", album: "", year: "", tracknumber: "", lyrics: "" });
    const editBase = ref({});
    const editMissing = ref([]);
    const editHasCover = ref(true);
    const editLyricsFrom = ref("");
    const editChanged = computed(
      () => Object.entries(editForm.value).filter(([key, value]) => String(value ?? "").trim() && String(value ?? "").trim() !== (editBase.value[key] || "")).map(([key]) => key)
    );
    async function openEdit(row) {
      editRow.value = row;
      editForm.value = { title: "", artist: "", album: "", year: "", tracknumber: "", lyrics: "" };
      editBase.value = {};
      editMissing.value = row.missing || [];
      editHasCover.value = !editMissing.value.includes("cover");
      editLyricsFrom.value = "";
      editDialog.value = true;
      editLoading.value = true;
      try {
        const detail2 = await loadFile(row.path);
        if (!detail2) return;
        const tags = detail2.tags || {};
        editForm.value = {
          title: tags.title || "",
          artist: tags.artist || "",
          album: tags.album || "",
          year: tags.year || "",
          tracknumber: tags.tracknumber || "",
          lyrics: tags.lyrics || ""
        };
        editBase.value = { ...editForm.value };
        editMissing.value = detail2.missing || [];
        editHasCover.value = !!detail2.has_cover;
        editLyricsFrom.value = detail2.lyrics_from || "";
      } finally {
        editLoading.value = false;
      }
    }
    async function submitEdit() {
      if (!editRow.value) return;
      const fields = {};
      for (const key of editChanged.value) {
        fields[key] = String(editForm.value[key] ?? "").trim();
      }
      if (!Object.keys(fields).length) {
        notify("没有需要写入的改动", "warning");
        return;
      }
      busy.value = true;
      try {
        const res = await props.api.post(`${base.value}/edit`, {
          path: editRow.value.path,
          fields
        });
        if (res?.accepted) {
          notify(`已写入 ${Object.keys(fields).length} 项`, "success");
          editDialog.value = false;
          await refresh();
        } else if (res?.reason === "out_of_library") {
          notify("该文件不在媒体库内，无法写入", "warning");
        } else if (res?.reason === "invalid_fields") {
          notify("字段不合法或为空", "warning");
        } else if (res?.reason === "write_failed") {
          notify("写标签失败，检查文件权限", "error");
        } else if (res?.reason === "no_path") {
          notify("缺少文件路径", "warning");
        } else {
          notify("保存失败", "error");
        }
      } catch (e) {
        notify(`保存失败：${e?.message || e}`, "error");
      } finally {
        busy.value = false;
      }
    }
    const detailDialog = ref(false);
    const detailLoading = ref(false);
    const detail = ref(null);
    const detailRow = ref(null);
    async function openDetail(row) {
      detailRow.value = row;
      detail.value = null;
      detailDialog.value = true;
      detailLoading.value = true;
      try {
        detail.value = await loadFile(row.path);
      } finally {
        detailLoading.value = false;
      }
    }
    function onRowClick(event, args) {
      const target = event.target;
      if (target?.closest?.("button")) return;
      const item = args?.item ?? args;
      if (item?.path) openDetail(item);
    }
    function onWrittenFilterChange() {
      page.value = 1;
      offset.value = 0;
      loadFiles();
    }
    function detailSets() {
      return detail.value?.rewrote || detailRow.value?.set || {};
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
        // 有缺项也挂数字：这一桶要人逐个进来补，不标数量就看不出还剩多少没处理。
        badge: b.key === "failed" ? Number(counts.value.failed ?? 0) : b.key === "deficient" ? Number(counts.value.deficient ?? 0) : b.key === "done" ? Number(counts.value.done ?? 0) : 0
      }))
    ]);
    const sourceNames = computed(
      () => (cfg.value?.resources || []).map((s) => sourceText(s)).join(" → ") || "全部源"
    );
    const stages = computed(() => stageCounts(runs.value));
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
      const _component_v_progress_linear = _resolveComponent("v-progress-linear");
      const _component_v_table = _resolveComponent("v-table");
      const _component_v_card_actions = _resolveComponent("v-card-actions");
      const _component_v_dialog = _resolveComponent("v-dialog");
      const _component_v_textarea = _resolveComponent("v-textarea");
      const _component_v_form = _resolveComponent("v-form");
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
            _cache[20] || (_cache[20] = _createElementVNode("div", { class: "mls-head-text" }, [
              _createElementVNode("div", { class: "text-h6" }, "TagFlow音乐库刮削"),
              _createElementVNode("div", { class: "text-caption text-medium-emphasis" }, " 直连平台检索补标签 · 逐文件核对与补齐在下方分桶里 ")
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
              default: _withCtx(() => [..._cache[17] || (_cache[17] = [
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
              default: _withCtx(() => [..._cache[18] || (_cache[18] = [
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
              default: _withCtx(() => [..._cache[19] || (_cache[19] = [
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
                          color: item.key === "failed" ? "warning" : item.key === "deficient" ? "info" : "grey"
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
                _createElementVNode("div", _hoisted_6, [
                  _createElementVNode("div", _hoisted_7, [
                    _cache[23] || (_cache[23] = _createElementVNode("div", { class: "mls-section-title" }, "本轮流转", -1)),
                    _createElementVNode("div", _hoisted_8, [
                      running.value ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                        _createTextVNode("扫描进行中 · 光点走到哪一环就问到了哪一环")
                      ], 64)) : runs.value.ts ? (_openBlock(), _createElementBlock(_Fragment, { key: 1 }, [
                        _createTextVNode(" 静止的是上一轮：" + _toDisplayString(fmtTime(runs.value.ts)) + "，" + _toDisplayString(runs.value.force === void 0 ? "只补缺" : runs.value.force ? "强制覆盖" : "只补缺") + " · 用时 " + _toDisplayString(runs.value.duration_s ?? 0) + "s ", 1)
                      ], 64)) : (_openBlock(), _createElementBlock(_Fragment, { key: 2 }, [
                        _createTextVNode("还没跑过一轮 —— 下面勾一个按钮开跑")
                      ], 64))
                    ]),
                    _createVNode(_component_v_spacer),
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
                      color: "warning",
                      loading: busy.value,
                      onClick: retryAllFailed
                    }, {
                      default: _withCtx(() => [..._cache[22] || (_cache[22] = [
                        _createTextVNode("重试失败", -1)
                      ])]),
                      _: 1
                    }, 8, ["loading"])
                  ]),
                  _createVNode(FlowPipeline, {
                    steps: _unref(FLOW_STEPS),
                    counts: stages.value,
                    running: running.value,
                    variant: "live"
                  }, null, 8, ["steps", "counts", "running"])
                ]),
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
                                _cache[24] || (_cache[24] = _createElementVNode("div", { class: "text-caption" }, "已处理", -1)),
                                _createElementVNode("div", _hoisted_9, _toDisplayString(counts.value.done ?? 0), 1)
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
                                _cache[25] || (_cache[25] = _createElementVNode("div", { class: "text-caption" }, "待重试", -1)),
                                _createElementVNode("div", _hoisted_10, _toDisplayString(counts.value.retryable ?? 0), 1)
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
                                _cache[26] || (_cache[26] = _createElementVNode("div", { class: "text-caption" }, "超限放弃", -1)),
                                _createElementVNode("div", _hoisted_11, _toDisplayString(counts.value.exhausted ?? 0), 1)
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
                                _cache[27] || (_cache[27] = _createElementVNode("div", { class: "text-caption" }, "待处理", -1)),
                                _createElementVNode("div", _hoisted_12, _toDisplayString(counts.value.pending_unknown ?? 0), 1)
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
                _createElementVNode("div", _hoisted_13, [
                  _cache[29] || (_cache[29] = _createElementVNode("div", { class: "mls-section-title" }, "最近一轮", -1)),
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
                              _createElementVNode("div", _hoisted_14, " 扫描时间 " + _toDisplayString(fmtTime(runs.value.ts)) + "｜用时 " + _toDisplayString(runs.value.duration_s ?? 0) + "s ", 1),
                              cfg.value.cron ? (_openBlock(), _createElementBlock("div", _hoisted_15, " 定时 " + _toDisplayString(cfg.value.cron) + "｜下次 " + _toDisplayString(cfg.value.cron_next), 1)) : _createCommentVNode("", true),
                              runs.value.aborted ? (_openBlock(), _createElementBlock("div", _hoisted_16, " 识别源连续故障，本轮已中止，未处理的留到下一轮 ")) : _createCommentVNode("", true),
                              lastTargeted.value.ts ? (_openBlock(), _createElementBlock("div", _hoisted_17, " 最近定向重试 " + _toDisplayString(fmtTime(lastTargeted.value.ts)) + "：成功 " + _toDisplayString(lastTargeted.value.ok ?? 0) + "｜失败 " + _toDisplayString(lastTargeted.value.failed ?? 0), 1)) : _createCommentVNode("", true)
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
                              _cache[28] || (_cache[28] = _createElementVNode("div", { class: "text-caption text-medium-emphasis mb-1" }, "本轮生效的配置", -1)),
                              _createElementVNode("div", _hoisted_18, _toDisplayString(cfg.value.mode === "force_all" ? "强制覆盖" : "只补缺") + "｜最低匹配分数 " + _toDisplayString(cfg.value.min_score) + "｜" + _toDisplayString(sourceNames.value), 1),
                              _createElementVNode("div", _hoisted_19, " 库路径 " + _toDisplayString((cfg.value.scraper_paths || []).length) + " 个｜排除 " + _toDisplayString((cfg.value.exclude_paths || []).length) + " 个｜歌词" + _toDisplayString(cfg.value.fill_lyrics ? "主动抓取" : "交给宿主"), 1),
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
                  _cache[30] || (_cache[30] = _createElementVNode("div", { class: "mls-section-title" }, "失败原因分布", -1)),
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
                  _cache[31] || (_cache[31] = _createElementVNode("div", { class: "text-caption text-grey mt-1" }, " 原因里写了差哪一项：「未对上」可以调分数或改文件名；「本地无某项」是文件本身没给出 这个信息，降分数也救不回来，该把它排出扫描范围。 ", -1))
                ])) : _createCommentVNode("", true),
                _createElementVNode("div", _hoisted_24, [
                  _createVNode(_component_v_spacer),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "warning",
                    loading: busy.value,
                    onClick: retryAllFailed
                  }, {
                    default: _withCtx(() => [..._cache[32] || (_cache[32] = [
                      _createTextVNode("重试全部失败", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"]),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "info",
                    loading: busy.value,
                    onClick: _cache[2] || (_cache[2] = ($event) => scanNow(false))
                  }, {
                    default: _withCtx(() => [..._cache[33] || (_cache[33] = [
                      _createTextVNode("增量扫描", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"]),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "error",
                    loading: busy.value,
                    onClick: _cache[3] || (_cache[3] = ($event) => scanNow(true))
                  }, {
                    default: _withCtx(() => [..._cache[34] || (_cache[34] = [
                      _createTextVNode("全量覆盖", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"])
                ])
              ])) : (_openBlock(), _createElementBlock("div", _hoisted_25, [
                _createElementVNode("div", _hoisted_26, [
                  _createElementVNode("div", null, [
                    _createElementVNode("div", _hoisted_27, _toDisplayString(activeBucket.value?.label), 1),
                    _createElementVNode("div", _hoisted_28, _toDisplayString(activeBucket.value?.hint), 1)
                  ]),
                  _createVNode(_component_v_spacer),
                  _createVNode(_component_v_text_field, {
                    modelValue: search.value,
                    "onUpdate:modelValue": [
                      _cache[4] || (_cache[4] = ($event) => search.value = $event),
                      onSearch
                    ],
                    density: "compact",
                    "hide-details": "",
                    placeholder: "搜索路径/文件名",
                    "prepend-inner-icon": "mdi-magnify",
                    style: { "max-width": "260px" },
                    clearable: ""
                  }, null, 8, ["modelValue"]),
                  bucket.value === "done" ? (_openBlock(), _createBlock(_component_v_btn, {
                    key: 0,
                    size: "small",
                    variant: "tonal",
                    color: writtenOnly.value ? "primary" : "default",
                    class: "text-none",
                    "prepend-icon": writtenOnly.value ? "mdi-filter" : "mdi-filter-outline",
                    onClick: _cache[5] || (_cache[5] = ($event) => {
                      writtenOnly.value = !writtenOnly.value;
                      onWrittenFilterChange();
                    })
                  }, {
                    default: _withCtx(() => [
                      _createTextVNode(" 只看本插件改写" + _toDisplayString(writtenOnly.value ? "" : `（${counts.value.rewritten ?? 0}）`), 1)
                    ]),
                    _: 1
                  }, 8, ["color", "prepend-icon"])) : _createCommentVNode("", true),
                  bucket.value === "failed" ? (_openBlock(), _createBlock(_component_v_btn, {
                    key: 1,
                    size: "small",
                    variant: "tonal",
                    color: "warning",
                    loading: busy.value,
                    onClick: retryAllFailed
                  }, {
                    default: _withCtx(() => [..._cache[35] || (_cache[35] = [
                      _createTextVNode("重试全部失败", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"])) : _createCommentVNode("", true),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "info",
                    loading: busy.value,
                    onClick: _cache[6] || (_cache[6] = ($event) => scanNow(false))
                  }, {
                    default: _withCtx(() => [..._cache[36] || (_cache[36] = [
                      _createTextVNode("增量扫描", -1)
                    ])]),
                    _: 1
                  }, 8, ["loading"]),
                  _createVNode(_component_v_btn, {
                    size: "small",
                    variant: "tonal",
                    color: "error",
                    loading: busy.value,
                    onClick: _cache[7] || (_cache[7] = ($event) => scanNow(true))
                  }, {
                    default: _withCtx(() => [..._cache[37] || (_cache[37] = [
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
                    { title: "操作", key: "actions", sortable: false, width: 170 }
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
                  class: "elevation-1 mls-table",
                  "onClick:row": onRowClick,
                  "onUpdate:page": onPage,
                  "onUpdate:itemsPerPage": onItemsPerPage
                }, {
                  "item.name": _withCtx(({ item }) => [
                    _createElementVNode("div", {
                      title: item.path
                    }, _toDisplayString(item.name), 9, _hoisted_29),
                    _createElementVNode("div", _hoisted_30, _toDisplayString(item.path), 1),
                    item.set ? (_openBlock(), _createElementBlock("div", _hoisted_31, [
                      _createVNode(_component_v_chip, {
                        size: "x-small",
                        variant: "tonal",
                        color: "primary",
                        "prepend-icon": "mdi-pen-plus"
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode(" 改写 " + _toDisplayString(Object.keys(item.set).length) + " 项 ", 1)
                        ]),
                        _: 2
                      }, 1024),
                      _createElementVNode("span", _hoisted_32, _toDisplayString(changedFieldNames(item.set)), 1)
                    ])) : _createCommentVNode("", true)
                  ]),
                  "item.status": _withCtx(({ item }) => [
                    _createVNode(_component_v_chip, {
                      size: "x-small",
                      variant: "tonal",
                      color: item.status === "done" ? "success" : item.status === "failed" ? "error" : item.status === "deficient" ? "info" : "grey"
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode(_toDisplayString(statusText(item.status)), 1)
                      ]),
                      _: 2
                    }, 1032, ["color"])
                  ]),
                  "item.reason": _withCtx(({ item }) => [
                    item.missing?.length ? (_openBlock(), _createElementBlock("span", _hoisted_33, _toDisplayString(item.missing.map(fieldText).join("、")), 1)) : item.reason ? (_openBlock(), _createElementBlock("span", _hoisted_34, _toDisplayString(item.reason), 1)) : _createCommentVNode("", true),
                    item.score != null ? (_openBlock(), _createElementBlock("span", _hoisted_35, " · 分数 " + _toDisplayString(item.score), 1)) : item.source ? (_openBlock(), _createElementBlock("span", _hoisted_36, " · " + _toDisplayString(sourceText(item.source)), 1)) : !item.reason && !item.missing?.length ? (_openBlock(), _createElementBlock("span", _hoisted_37, "-")) : _createCommentVNode("", true)
                  ]),
                  "item.mtime": _withCtx(({ item }) => [
                    _createTextVNode(_toDisplayString(fmtTime(item.mtime)), 1)
                  ]),
                  "item.size": _withCtx(({ item }) => [
                    _createTextVNode(_toDisplayString(fmtSize(item.size)), 1)
                  ]),
                  "item.actions": _withCtx(({ item }) => [
                    item.status === "deficient" ? (_openBlock(), _createBlock(_component_v_btn, {
                      key: 0,
                      size: "x-small",
                      variant: "text",
                      color: "primary",
                      loading: busy.value,
                      onClick: ($event) => openEdit(item)
                    }, {
                      default: _withCtx(() => [..._cache[38] || (_cache[38] = [
                        _createTextVNode("编辑", -1)
                      ])]),
                      _: 1
                    }, 8, ["loading", "onClick"])) : (_openBlock(), _createBlock(_component_v_btn, {
                      key: 1,
                      size: "x-small",
                      variant: "text",
                      color: "primary",
                      loading: busy.value,
                      onClick: ($event) => retryRow(item)
                    }, {
                      default: _withCtx(() => [..._cache[39] || (_cache[39] = [
                        _createTextVNode("重试", -1)
                      ])]),
                      _: 1
                    }, 8, ["loading", "onClick"])),
                    _createVNode(_component_v_btn, {
                      size: "x-small",
                      variant: "text",
                      onClick: ($event) => openDetail(item)
                    }, {
                      default: _withCtx(() => [..._cache[40] || (_cache[40] = [
                        _createTextVNode("详情", -1)
                      ])]),
                      _: 1
                    }, 8, ["onClick"])
                  ]),
                  "no-data": _withCtx(() => [..._cache[41] || (_cache[41] = [
                    _createElementVNode("div", { class: "text-center pa-6 text-grey" }, "该桶暂无数据", -1)
                  ])]),
                  _: 1
                }, 8, ["items", "items-length", "loading", "page-text", "page", "items-per-page"])
              ]))
            ])
          ]),
          _createVNode(_component_v_snackbar, {
            modelValue: snackbar.value.show,
            "onUpdate:modelValue": _cache[8] || (_cache[8] = ($event) => snackbar.value.show = $event),
            color: snackbar.value.color,
            timeout: "3000",
            location: "bottom"
          }, {
            default: _withCtx(() => [
              _createTextVNode(_toDisplayString(snackbar.value.text), 1)
            ]),
            _: 1
          }, 8, ["modelValue", "color"]),
          _createVNode(_component_v_dialog, {
            modelValue: detailDialog.value,
            "onUpdate:modelValue": _cache[12] || (_cache[12] = ($event) => detailDialog.value = $event),
            "max-width": "680px"
          }, {
            default: _withCtx(() => [
              _createVNode(_component_v_card, { variant: "flat" }, {
                default: _withCtx(() => [
                  _createElementVNode("div", _hoisted_38, [
                    _createVNode(_component_v_avatar, {
                      color: "primary",
                      variant: "tonal",
                      rounded: "lg",
                      size: "36",
                      class: "mr-3"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_v_icon, {
                          icon: "mdi-file-music-outline",
                          size: "20"
                        })
                      ]),
                      _: 1
                    }),
                    _createElementVNode("div", _hoisted_39, [
                      _createElementVNode("div", _hoisted_40, _toDisplayString(detailRow.value?.name), 1),
                      _createElementVNode("div", _hoisted_41, _toDisplayString(detailRow.value?.path), 1)
                    ]),
                    _createVNode(_component_v_spacer),
                    _createVNode(_component_v_btn, {
                      icon: "mdi-close",
                      variant: "text",
                      size: "small",
                      onClick: _cache[9] || (_cache[9] = ($event) => detailDialog.value = false)
                    })
                  ]),
                  _createVNode(_component_v_divider),
                  _createVNode(_component_v_card_text, { class: "pt-4" }, {
                    default: _withCtx(() => [
                      detailLoading.value ? (_openBlock(), _createBlock(_component_v_progress_linear, {
                        key: 0,
                        indeterminate: "",
                        color: "primary",
                        height: "2"
                      })) : detail.value ? (_openBlock(), _createElementBlock(_Fragment, { key: 1 }, [
                        _cache[45] || (_cache[45] = _createElementVNode("div", { class: "mls-section-title" }, "本插件的改写", -1)),
                        Object.keys(detailSets()).length ? (_openBlock(), _createBlock(_component_v_table, {
                          key: 0,
                          density: "compact",
                          class: "mls-diff mb-4"
                        }, {
                          default: _withCtx(() => [
                            _cache[42] || (_cache[42] = _createElementVNode("thead", null, [
                              _createElementVNode("tr", null, [
                                _createElementVNode("th", { class: "text-left" }, "字段"),
                                _createElementVNode("th", { class: "text-left" }, "改前"),
                                _createElementVNode("th", { class: "text-left" }, "改后")
                              ])
                            ], -1)),
                            _createElementVNode("tbody", null, [
                              (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(detailSets(), (pair, field) => {
                                return _openBlock(), _createElementBlock("tr", { key: field }, [
                                  _createElementVNode("td", _hoisted_42, _toDisplayString(fieldText(String(field))), 1),
                                  _createElementVNode("td", _hoisted_43, _toDisplayString(pair[0] || "（空）"), 1),
                                  _createElementVNode("td", _hoisted_44, _toDisplayString(pair[1] || "（空）"), 1)
                                ]);
                              }), 128))
                            ])
                          ]),
                          _: 1
                        })) : (_openBlock(), _createElementBlock("div", _hoisted_45, " 这一条没有改写记录：要么本轮判定已齐直接跳过，要么是宿主自己写入的 ")),
                        detail.value.set_ts ? (_openBlock(), _createElementBlock("div", _hoisted_46, " 最近一次改写 " + _toDisplayString(detail.value.set_ts) + "｜来源 " + _toDisplayString(sourceText(detail.value.source || "")), 1)) : _createCommentVNode("", true),
                        _cache[46] || (_cache[46] = _createElementVNode("div", { class: "mls-section-title" }, "文件里现在写着", -1)),
                        _createVNode(_component_v_row, { dense: "" }, {
                          default: _withCtx(() => [
                            (_openBlock(), _createElementBlock(_Fragment, null, _renderList(["title", "artist", "album", "year", "tracknumber"], (field) => {
                              return _createVNode(_component_v_col, {
                                key: field,
                                cols: "6",
                                md: "4"
                              }, {
                                default: _withCtx(() => [
                                  _createElementVNode("div", _hoisted_47, [
                                    _createElementVNode("div", _hoisted_48, _toDisplayString(fieldText(field)), 1),
                                    _createElementVNode("div", _hoisted_49, _toDisplayString((detail.value.tags || {})[field] || "—"), 1)
                                  ])
                                ]),
                                _: 2
                              }, 1024);
                            }), 64)),
                            _createVNode(_component_v_col, {
                              cols: "6",
                              md: "4"
                            }, {
                              default: _withCtx(() => [
                                _createElementVNode("div", _hoisted_50, [
                                  _cache[43] || (_cache[43] = _createElementVNode("div", { class: "text-caption text-grey" }, "封面", -1)),
                                  _createElementVNode("div", _hoisted_51, _toDisplayString(detail.value.has_cover ? "已内嵌" : "无"), 1)
                                ])
                              ]),
                              _: 1
                            }),
                            _createVNode(_component_v_col, {
                              cols: "12",
                              md: "4"
                            }, {
                              default: _withCtx(() => [
                                _createElementVNode("div", _hoisted_52, [
                                  _cache[44] || (_cache[44] = _createElementVNode("div", { class: "text-caption text-grey" }, "歌词", -1)),
                                  _createElementVNode("div", _hoisted_53, _toDisplayString(detail.value.lyrics_from === "sibling" ? "同名 .lrc 里有" : detail.value.lyrics_from === "embedded" ? "已内嵌" : "无"), 1)
                                ])
                              ]),
                              _: 1
                            })
                          ]),
                          _: 1
                        }),
                        (detail.value.missing || []).length ? (_openBlock(), _createElementBlock("div", _hoisted_54, " 仍缺：" + _toDisplayString(detail.value.missing.map(fieldText).join("、")), 1)) : _createCommentVNode("", true)
                      ], 64)) : _createCommentVNode("", true)
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_card_actions, null, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_spacer),
                      _createVNode(_component_v_btn, {
                        variant: "text",
                        class: "text-none",
                        onClick: _cache[10] || (_cache[10] = ($event) => detailDialog.value = false)
                      }, {
                        default: _withCtx(() => [..._cache[47] || (_cache[47] = [
                          _createTextVNode("关闭", -1)
                        ])]),
                        _: 1
                      }),
                      detailRow.value?.status === "deficient" ? (_openBlock(), _createBlock(_component_v_btn, {
                        key: 0,
                        variant: "flat",
                        color: "primary",
                        class: "text-none",
                        onClick: _cache[11] || (_cache[11] = ($event) => {
                          detailDialog.value = false;
                          openEdit(detailRow.value);
                        })
                      }, {
                        default: _withCtx(() => [..._cache[48] || (_cache[48] = [
                          _createTextVNode("补齐缺项", -1)
                        ])]),
                        _: 1
                      })) : _createCommentVNode("", true)
                    ]),
                    _: 1
                  })
                ]),
                _: 1
              })
            ]),
            _: 1
          }, 8, ["modelValue"]),
          _createVNode(_component_v_dialog, {
            modelValue: editDialog.value,
            "onUpdate:modelValue": _cache[16] || (_cache[16] = ($event) => editDialog.value = $event),
            "max-width": "720px"
          }, {
            default: _withCtx(() => [
              _createVNode(_component_v_card, { variant: "flat" }, {
                default: _withCtx(() => [
                  _createElementVNode("div", _hoisted_55, [
                    _createVNode(_component_v_avatar, {
                      color: "primary",
                      variant: "tonal",
                      rounded: "lg",
                      size: "36",
                      class: "mr-3"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_v_icon, {
                          icon: "mdi-music-box-outline",
                          size: "20"
                        })
                      ]),
                      _: 1
                    }),
                    _createElementVNode("div", _hoisted_56, [
                      _cache[49] || (_cache[49] = _createElementVNode("div", { class: "text-subtitle-1" }, "补齐缺项", -1)),
                      _createElementVNode("div", _hoisted_57, _toDisplayString(editRow.value?.path), 1)
                    ]),
                    _createVNode(_component_v_spacer),
                    _createVNode(_component_v_btn, {
                      icon: "mdi-close",
                      variant: "text",
                      size: "small",
                      onClick: _cache[13] || (_cache[13] = ($event) => editDialog.value = false)
                    })
                  ]),
                  _createVNode(_component_v_divider),
                  _createVNode(_component_v_card_text, { class: "pt-4" }, {
                    default: _withCtx(() => [
                      editLoading.value ? (_openBlock(), _createBlock(_component_v_progress_linear, {
                        key: 0,
                        indeterminate: "",
                        color: "primary",
                        height: "2"
                      })) : _createCommentVNode("", true),
                      editMissing.value.length ? (_openBlock(), _createElementBlock("div", _hoisted_58, [
                        (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(editMissing.value, (field) => {
                          return _openBlock(), _createBlock(_component_v_chip, {
                            key: field,
                            size: "x-small",
                            variant: "tonal",
                            color: "warning",
                            "prepend-icon": "mdi-alert-circle-outline"
                          }, {
                            default: _withCtx(() => [
                              _createTextVNode(_toDisplayString(fieldText(field)), 1)
                            ]),
                            _: 2
                          }, 1024);
                        }), 128)),
                        _cache[50] || (_cache[50] = _createElementVNode("span", { class: "text-caption text-grey ml-1" }, "以上几项文件里没有，其余都是已填好的现值", -1))
                      ])) : !editLoading.value ? (_openBlock(), _createElementBlock("div", _hoisted_59, " 按后端判定这一条已经齐了，下面的值就是文件里的现值 ")) : _createCommentVNode("", true),
                      _createVNode(_component_v_form, {
                        onSubmit: _withModifiers(submitEdit, ["prevent"])
                      }, {
                        default: _withCtx(() => [
                          _createVNode(_component_v_row, { dense: "" }, {
                            default: _withCtx(() => [
                              (_openBlock(), _createElementBlock(_Fragment, null, _renderList(EDIT_FIELDS, (field) => {
                                return _createVNode(_component_v_col, {
                                  key: field.key,
                                  cols: field.cols
                                }, {
                                  default: _withCtx(() => [
                                    _createVNode(_component_v_text_field, {
                                      modelValue: editForm.value[field.key],
                                      "onUpdate:modelValue": ($event) => editForm.value[field.key] = $event,
                                      label: fieldText(field.key),
                                      loading: editLoading.value,
                                      density: "comfortable",
                                      "hide-details": "auto",
                                      variant: "outlined"
                                    }, {
                                      "append-inner": _withCtx(() => [
                                        !editBase.value[field.key] ? (_openBlock(), _createBlock(_component_v_chip, {
                                          key: 0,
                                          size: "x-small",
                                          variant: "text",
                                          color: "warning"
                                        }, {
                                          default: _withCtx(() => [..._cache[51] || (_cache[51] = [
                                            _createTextVNode("缺", -1)
                                          ])]),
                                          _: 1
                                        })) : editChanged.value.includes(field.key) ? (_openBlock(), _createBlock(_component_v_icon, {
                                          key: 1,
                                          icon: "mdi-pencil-circle",
                                          size: "x-small",
                                          color: "primary"
                                        })) : (_openBlock(), _createBlock(_component_v_icon, {
                                          key: 2,
                                          icon: "mdi-check-circle-outline",
                                          size: "x-small",
                                          color: "grey"
                                        }))
                                      ]),
                                      _: 2
                                    }, 1032, ["modelValue", "onUpdate:modelValue", "label", "loading"])
                                  ]),
                                  _: 2
                                }, 1032, ["cols"]);
                              }), 64))
                            ]),
                            _: 1
                          }),
                          _createElementVNode("div", _hoisted_60, [
                            _createElementVNode("div", _hoisted_61, [
                              _cache[54] || (_cache[54] = _createElementVNode("div", { class: "mls-section-title mb-0" }, "歌词", -1)),
                              _createVNode(_component_v_spacer),
                              editLyricsFrom.value === "sibling" ? (_openBlock(), _createBlock(_component_v_chip, {
                                key: 0,
                                size: "x-small",
                                variant: "tonal",
                                color: "info"
                              }, {
                                default: _withCtx(() => [..._cache[52] || (_cache[52] = [
                                  _createTextVNode(" 取自同名 .lrc ", -1)
                                ])]),
                                _: 1
                              })) : editLyricsFrom.value === "embedded" ? (_openBlock(), _createBlock(_component_v_chip, {
                                key: 1,
                                size: "x-small",
                                variant: "text",
                                color: "grey"
                              }, {
                                default: _withCtx(() => [..._cache[53] || (_cache[53] = [
                                  _createTextVNode(" 文件里已有 ", -1)
                                ])]),
                                _: 1
                              })) : _createCommentVNode("", true)
                            ]),
                            _createVNode(_component_v_textarea, {
                              modelValue: editForm.value.lyrics,
                              "onUpdate:modelValue": _cache[14] || (_cache[14] = ($event) => editForm.value.lyrics = $event),
                              density: "compact",
                              rows: "5",
                              "auto-grow": "",
                              "hide-details": "",
                              variant: "outlined",
                              loading: editLoading.value,
                              placeholder: "留空则不改动"
                            }, null, 8, ["modelValue", "loading"])
                          ]),
                          !editHasCover.value ? (_openBlock(), _createElementBlock("div", _hoisted_62, " 缺封面这里补不了：封面要下载后内嵌，用行上的「重试」让识别源再刮一次 ")) : _createCommentVNode("", true)
                        ]),
                        _: 1
                      })
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_divider),
                  _createVNode(_component_v_card_actions, null, {
                    default: _withCtx(() => [
                      _createElementVNode("div", _hoisted_63, _toDisplayString(editChanged.value.length ? `本次写入 ${editChanged.value.length} 项：${editChanged.value.map(fieldText).join("、")}` : "没有改动可写入"), 1),
                      _createVNode(_component_v_spacer),
                      _createVNode(_component_v_btn, {
                        variant: "text",
                        class: "text-none",
                        onClick: _cache[15] || (_cache[15] = ($event) => editDialog.value = false)
                      }, {
                        default: _withCtx(() => [..._cache[55] || (_cache[55] = [
                          _createTextVNode("取消", -1)
                        ])]),
                        _: 1
                      }),
                      _createVNode(_component_v_btn, {
                        variant: "flat",
                        color: "primary",
                        class: "text-none",
                        loading: busy.value,
                        disabled: editLoading.value || !editChanged.value.length,
                        onClick: submitEdit
                      }, {
                        default: _withCtx(() => [..._cache[56] || (_cache[56] = [
                          _createTextVNode(" 保存 ", -1)
                        ])]),
                        _: 1
                      }, 8, ["loading", "disabled"])
                    ]),
                    _: 1
                  })
                ]),
                _: 1
              })
            ]),
            _: 1
          }, 8, ["modelValue"])
        ]),
        _: 1
      });
    };
  }
});

const Page = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-b64a4149"]]);

export { Page as default };
