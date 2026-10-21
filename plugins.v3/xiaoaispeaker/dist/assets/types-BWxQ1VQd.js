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
function query(params) {
  const pairs = [];
  for (const [key, value] of Object.entries(params)) {
    if (value === void 0 || value === null || value === "") continue;
    pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
  }
  return pairs.length ? `?${pairs.join("&")}` : "";
}

export { fmtSize as a, fmtTime as b, fmtSeconds as f, query as q, statusColor as s };
