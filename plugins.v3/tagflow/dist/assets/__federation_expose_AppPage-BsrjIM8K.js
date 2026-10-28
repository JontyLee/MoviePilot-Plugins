import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import Page from './__federation_expose_Page-by-tAwkn.js';
import Config from './__federation_expose_Config-BilinnzG.js';
import { _ as _export_sfc } from './flow-BVjcb_2i.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {createVNode:_createVNode,resolveComponent:_resolveComponent,withCtx:_withCtx,openBlock:_openBlock,createElementBlock:_createElementBlock} = await importShared('vue');

const _hoisted_1 = { class: "mls-app" };
const {ref} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "AppPage",
  props: {
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" },
    sourcePluginId: { type: String, default: "" },
    navKey: { type: String, default: "main" },
    nativeSubscribe: { type: Function, default: null }
  },
  setup(__props) {
    const props = __props;
    const dataPage = ref(null);
    const configDialog = ref(false);
    function onConfigSaved() {
      dataPage.value?.refresh();
    }
    return (_ctx, _cache) => {
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_dialog = _resolveComponent("v-dialog");
      return _openBlock(), _createElementBlock("div", _hoisted_1, [
        _createVNode(Page, {
          ref_key: "dataPage",
          ref: dataPage,
          api: props.api,
          "plugin-id": props.pluginId,
          "source-plugin-id": props.sourcePluginId,
          "show-switch": true,
          onSwitch: _cache[0] || (_cache[0] = ($event) => configDialog.value = true)
        }, null, 8, ["api", "plugin-id", "source-plugin-id"]),
        _createVNode(_component_v_dialog, {
          modelValue: configDialog.value,
          "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => configDialog.value = $event),
          "max-width": "980px",
          scrollable: "",
          persistent: ""
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "flat",
              class: "mls-config-dialog"
            }, {
              default: _withCtx(() => [
                _createVNode(Config, {
                  api: props.api,
                  "plugin-id": props.pluginId,
                  "source-plugin-id": props.sourcePluginId,
                  standalone: "",
                  onClose: _cache[1] || (_cache[1] = ($event) => configDialog.value = false),
                  onUpdated: onConfigSaved
                }, null, 8, ["api", "plugin-id", "source-plugin-id"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }, 8, ["modelValue"])
      ]);
    };
  }
});

const AppPage = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-463c152d"]]);

export { AppPage as default };
