import { importShared } from './__federation_fn_import-JrT3xvdd.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {renderList:_renderList,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,createElementVNode:_createElementVNode,resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,toDisplayString:_toDisplayString,createCommentVNode:_createCommentVNode,normalizeStyle:_normalizeStyle,normalizeClass:_normalizeClass} = await importShared('vue');

const _hoisted_1 = { class: "tfp-title" };
const _hoisted_2 = { class: "tfp-caption" };
const _hoisted_3 = {
  key: 0,
  class: "tfp-count"
};
const _hoisted_4 = {
  key: 1,
  class: "tfp-knobs"
};
const {computed} = await importShared('vue');

const SEGMENT = 0.9;
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "FlowPipeline",
  props: {
    steps: { type: Array, default: () => [] },
    // 环节键 → 数字；数据页传本轮汇总折算出来的各环节计数
    counts: { type: Object, default: () => ({}) },
    running: { type: Boolean, default: false },
    // config：节点下标出"这一环由哪个配置决定"；live：标出这一环落了多少文件
    variant: { type: String, default: "live" }
  },
  setup(__props) {
    const props = __props;
    const cycle = computed(() => Math.max(props.steps.length, 1) * SEGMENT);
    function style(index) {
      return {
        "--tfp-delay": `${index * cycle.value / Math.max(props.steps.length, 1)}s`,
        "--tfp-cycle": `${cycle.value}s`
      };
    }
    function count(key) {
      const value = props.counts?.[key];
      return typeof value === "number" && Number.isFinite(value) ? value : null;
    }
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_avatar = _resolveComponent("v-avatar");
      return _openBlock(), _createElementBlock("div", {
        class: _normalizeClass(["tfp", [`tfp-${__props.variant}`, __props.running ? "is-running" : ""]])
      }, [
        (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(__props.steps, (step, i) => {
          return _openBlock(), _createElementBlock(_Fragment, {
            key: step.key
          }, [
            _createElementVNode("div", {
              class: "tfp-node",
              style: _normalizeStyle(style(i))
            }, [
              _cache[0] || (_cache[0] = _createElementVNode("span", { class: "tfp-ring" }, null, -1)),
              _createVNode(_component_v_avatar, {
                color: __props.running ? "primary" : "grey-lighten-3",
                variant: "tonal",
                rounded: "circle",
                size: "42"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_icon, {
                    icon: step.icon,
                    size: "21"
                  }, null, 8, ["icon"])
                ]),
                _: 2
              }, 1032, ["color"]),
              _createElementVNode("div", _hoisted_1, _toDisplayString(step.title), 1),
              _createElementVNode("div", _hoisted_2, _toDisplayString(step.caption), 1),
              __props.variant === "live" && count(step.key) !== null ? (_openBlock(), _createElementBlock("div", _hoisted_3, _toDisplayString(count(step.key)), 1)) : _createCommentVNode("", true),
              __props.variant === "config" && step.knobs ? (_openBlock(), _createElementBlock("div", _hoisted_4, _toDisplayString(step.knobs), 1)) : _createCommentVNode("", true)
            ], 4),
            i < __props.steps.length - 1 ? (_openBlock(), _createElementBlock("div", {
              key: 0,
              class: "tfp-link",
              style: _normalizeStyle(style(i))
            }, [..._cache[1] || (_cache[1] = [
              _createElementVNode("span", { class: "tfp-pulse" }, null, -1)
            ])], 4)) : _createCommentVNode("", true)
          ], 64);
        }), 128))
      ], 2);
    };
  }
});

const _export_sfc = (sfc, props) => {
  const target = sfc.__vccOpts || sfc;
  for (const [key, val] of props) {
    target[key] = val;
  }
  return target;
};

const FlowPipeline = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-2b8e88d0"]]);

const FLOW_STEPS = [
  {
    key: "enumerate",
    icon: "mdi-folder-search-outline",
    title: "枚举",
    caption: "宿主白名单交集",
    knobs: "音乐库路径 / 排除路径"
  },
  {
    key: "identity",
    icon: "mdi-tag-text-outline",
    title: "身份",
    caption: "标签优先，目录兜底",
    knobs: "音乐库路径"
  },
  {
    key: "incremental",
    icon: "mdi-skip-forward-outline",
    title: "增量",
    caption: "四项齐全就免问",
    knobs: "写入模式"
  },
  {
    key: "query",
    icon: "mdi-magnify",
    title: "检索",
    caption: "逐源问，满分即停",
    knobs: "识别源"
  },
  {
    key: "score",
    icon: "mdi-scale-balance",
    title: "打分",
    caption: "曲名/艺人/专辑 各 1 分",
    knobs: "最低匹配分数"
  },
  {
    key: "write",
    icon: "mdi-content-save-move-outline",
    title: "写入",
    caption: "宿主原生刮削链",
    knobs: "写入模式 + 宿主策略"
  },
  {
    key: "ledger",
    icon: "mdi-book-check-outline",
    title: "记账",
    caption: "故障不算文件失败"
  }
];
function stageCounts(runs) {
  const n = (v) => Number(v ?? 0) || 0;
  const scanned = n(runs?.scanned);
  const free = n(runs?.from_cache) + n(runs?.skipped_tagged) + n(runs?.exhausted);
  const queried = Math.max(scanned - free, 0);
  return {
    enumerate: scanned,
    identity: scanned,
    incremental: free,
    query: queried,
    score: n(runs?.ok) + n(runs?.failed),
    write: n(runs?.ok),
    ledger: n(runs?.failed) + n(runs?.transport) + n(runs?.error)
  };
}

export { FlowPipeline as F, _export_sfc as _, FLOW_STEPS as a, stageCounts as s };
