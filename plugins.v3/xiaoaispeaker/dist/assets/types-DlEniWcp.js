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

export { MODE_ITEMS as M, TASK_ACTIONS as T, WEEK_DAYS as W, activeLine as a, fmtSize as b, fmtTime as c, fmtSeconds as f, modeText as m, query as q, statusColor as s, taskNeedsPlaylist as t };
