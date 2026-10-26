const TASK_ACTIONS = [
  { title: "播放歌单", value: "play_playlist" },
  { title: "歌单从某首播", value: "play_playlist_from" },
  { title: "停止播放", value: "stop" },
  { title: "设置音量", value: "set_volume" },
  { title: "设置播放方式", value: "set_play_mode" },
  { title: "开启语音监听", value: "enable_monitor" },
  { title: "关闭语音监听", value: "disable_monitor" }
];
const WEEK_DAYS = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"];
function taskNeedsPlaylist(action) {
  return action === "play_playlist" || action === "play_playlist_from";
}
function statusColor(status) {
  if (status === 1) return "success";
  if (status === 2) return "warning";
  if (status === 0) return "grey";
  return "grey-darken-1";
}
function fmtSeconds(value) {
  if (value === null || value === void 0) return "-";
  const total = Math.max(0, Math.floor(value));
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}
function fmtSize(bytes) {
  if (!bytes) return "-";
  const mb = bytes / 1048576;
  return mb >= 1 ? `${mb.toFixed(1)} MB` : `${(bytes / 1024).toFixed(0)} KB`;
}
function fmtTime(value) {
  if (!value && value !== 0) return "-";
  if (typeof value === "string") return value;
  try {
    return new Date(value * 1e3).toLocaleString();
  } catch {
    return String(value);
  }
}
const MODE_ITEMS = [
  { title: "顺序播放", value: "order", icon: "mdi-sort" },
  { title: "列表循环", value: "loop", icon: "mdi-repeat" },
  { title: "随机播放", value: "random", icon: "mdi-shuffle-variant" },
  { title: "单曲循环", value: "single", icon: "mdi-repeat-once" },
  { title: "只播这一首", value: "single_play", icon: "mdi-music-note-minus" }
];
function modeText(mode) {
  return MODE_ITEMS.find((m) => m.value === mode)?.title || mode || "顺序播放";
}
function activeLine(lines, position) {
  if (!lines.length || position === null || position === void 0) return -1;
  let hit = -1;
  for (const [i, line] of lines.entries()) {
    if (line.time < 0) continue;
    if (line.time <= position + 0.5) hit = i;
    else break;
  }
  return hit;
}
function query(params) {
  const pairs = [];
  for (const [key, value] of Object.entries(params)) {
    if (value === void 0 || value === null || value === "") continue;
    pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
  }
  return pairs.length ? `?${pairs.join("&")}` : "";
}
function why(e) {
  const text = String(e?.message ?? e?.response?.statusText ?? e ?? "").trim();
  const lower = text.toLowerCase();
  if (!text || /network error|failed to fetch|load failed|err_connection|econnrefused/.test(lower)) {
    return "连不上插件后端（主程序没起来，或者页面和插件不在同一台机器）";
  }
  if (/canceled|abort/.test(lower)) return "请求取消了";
  if (/timeout|exceeded/.test(lower)) return "请求超时，接口没按时回话";
  const code = lower.match(/status code (\d{3})/)?.[1] || String(e?.response?.status || "");
  if (code === "401" || code === "403") return "没有权限（登录状态失效，重新登录主程序）";
  if (code === "404") return "接口找不到（插件可能刚改过，重载后再试）";
  if (code) return `后端返回 ${code}`;
  return text;
}
function sourceText(source) {
  const map = {
    library: "曲库文件",
    url: "外部直链",
    playlist: "歌单",
    sidecar: "同目录歌词文件",
    tag: "音频内嵌标签"
  };
  return map[source || ""] || source || "";
}
function baseSourceText(source) {
  const map = {
    config: "插件配置",
    app_domain: "主程序外部地址"
  };
  return map[source || ""] || source || "";
}

const _export_sfc = (sfc, props) => {
  const target = sfc.__vccOpts || sfc;
  for (const [key, val] of props) {
    target[key] = val;
  }
  return target;
};

export { MODE_ITEMS as M, TASK_ACTIONS as T, WEEK_DAYS as W, _export_sfc as _, activeLine as a, baseSourceText as b, sourceText as c, fmtSize as d, fmtTime as e, fmtSeconds as f, modeText as m, query as q, statusColor as s, taskNeedsPlaylist as t, why as w };
