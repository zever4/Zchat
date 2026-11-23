// ===== AUTH CHECK =====
const storedToken = localStorage.getItem("token");
if (!storedToken) {
  window.location.href = "/login.html";
}
const token = storedToken || "";
const currentUsername = localStorage.getItem("username") || "User";

// Namespace local caches only; this decoded subject never authorizes a request.
function accountStorageKey(base: string): string {
  try {
    const payload = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
    const subject = String(payload.sub || "");
    if (/^[1-9][0-9]*$/.test(subject)) return base + ":" + subject;
  } catch { /* Invalid sessions will be rejected by the server. */ }
  return base + ":invalid";
}

// Ownership of legacy global caches is unknown; do not assign them to a new login.
for (const key of ["zchat_notifs", "zchat_user_settings", "zchat_pinned_chats", "zchat_muted_chats"]) localStorage.removeItem(key);
