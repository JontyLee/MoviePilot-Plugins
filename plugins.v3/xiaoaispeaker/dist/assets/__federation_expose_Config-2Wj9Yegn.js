import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { C as ConfigForm } from './ConfigForm-DbT4l0-v.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,toDisplayString:_toDisplayString,createElementVNode:_createElementVNode,openBlock:_openBlock,createBlock:_createBlock} = await importShared('vue');

const _hoisted_1 = { class: "xai-head" };
const _hoisted_2 = { class: "xai-head-text" };
const _hoisted_3 = { class: "text-h6" };
const {computed,onMounted} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Config",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    // 宿主存的配置不作为初值：密码不在里面，而 /config 是「缺省即保留」的语义，
    // 拿一份不含密码的草稿去保存会把已存的账密一起动到。表单自己从 /status 起头。
    initialConfig: { type: Object, default: () => ({}) },
    nativeSubscribe: { type: Function, default: null }
  },
  emits: ["save", "close", "switch", "layout"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const title = computed(() => "小爱音箱 · 配置");
    onMounted(() => {
      emit("layout", { maxWidth: "64rem" });
    });
    return (_ctx, _cache) => {
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_avatar = _resolveComponent("v-avatar");
      const _component_v_divider = _resolveComponent("v-divider");
      const _component_v_card = _resolveComponent("v-card");
      return _openBlock(), _createBlock(_component_v_card, {
        flat: "",
        class: "xai-config"
      }, {
        default: _withCtx(() => [
          _createElementVNode("div", _hoisted_1, [
            _createVNode(_component_v_avatar, {
              color: "primary",
              variant: "tonal",
              rounded: "lg",
              size: "36",
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
            _createElementVNode("div", _hoisted_2, [
              _createElementVNode("div", _hoisted_3, _toDisplayString(title.value), 1),
              _cache[1] || (_cache[1] = _createElementVNode("div", { class: "text-caption text-medium-emphasis" }, " 小米账号、可播放的音乐库与音箱访问得到的直链地址 ", -1))
            ])
          ]),
          _createVNode(_component_v_divider),
          _createVNode(ConfigForm, {
            api: props.api,
            "plugin-id": props.pluginId,
            "source-plugin-id": props.sourcePluginId,
            "can-switch": "",
            onSwitch: _cache[0] || (_cache[0] = ($event) => emit("switch"))
          }, null, 8, ["api", "plugin-id", "source-plugin-id"])
        ]),
        _: 1
      });
    };
  }
});

const Config = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-dcb84d14"]]);

export { Config as default };
