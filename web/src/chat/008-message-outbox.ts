// Plaintext queue for ordinary chats only. Crypto will use a separate outbox.
interface OutboxEntry {
  version: 1;
  client_message_id: string;
  chat_id: number;
  content: string;
  created_at: string;
  attempts: number;
  state: "queued" | "sending" | "failed";
  retryable: boolean;
  error: string;
}
const outboxIDPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
// Desktop origin stays fixed across servers; include both server and account.
// Never store tokens or session IDs here. One key per message avoids lost updates
// when two tabs enqueue simultaneously; duplicate transmissions are idempotent.
const outboxPrefix = accountStorageKey("zchat_outbox_v1:" + encodeURIComponent(
  window.zchatDesktop?.serverUrl || location.origin)) + ":";
const messageOutbox = new Map<string, OutboxEntry>();
const outboxTimers = new Map<string, number>();
const outboxRetryAt = new Map<string, number>();
const outboxMaxAttempts = 3;

function parseOutboxEntry(value: string | null, id: string): OutboxEntry | null {
  try {
    const entry = JSON.parse(value || "null") as OutboxEntry;
    if (entry?.version !== 1 || entry.client_message_id !== id || !outboxIDPattern.test(id) ||
        !Number.isSafeInteger(entry.chat_id) || entry.chat_id <= 0 || typeof entry.content !== "string" ||
        !entry.content.trim() || entry.content.length > 2000 || !Number.isFinite(Date.parse(entry.created_at)) ||
        !Number.isInteger(entry.attempts) || entry.attempts < 0 || entry.attempts > outboxMaxAttempts ||
        !["queued", "sending", "failed"].includes(entry.state) || typeof entry.retryable !== "boolean" ||
        typeof entry.error !== "string" || entry.error.length > 256) return null;
    return entry;
  } catch { return null; }
}

function restoreMessageOutbox(): void {
  try {
    for (let index = 0; index < localStorage.length; index++) {
      const key = localStorage.key(index);
      if (!key?.startsWith(outboxPrefix)) continue;
      const entry = parseOutboxEntry(localStorage.getItem(key), key.slice(outboxPrefix.length));
      if (!entry) continue;
      if (entry.state === "sending") entry.state = "queued";
      messageOutbox.set(entry.client_message_id, entry);
    }
  } catch { /* A new send explains unavailable storage and preserves the draft. */ }
}
restoreMessageOutbox();

function persistOutboxEntry(entry: OutboxEntry): boolean {
  try { localStorage.setItem(outboxPrefix + entry.client_message_id, JSON.stringify(entry)); return true; }
  catch { return false; }
}

function newClientMessageID(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64; bytes[8] = (bytes[8] & 63) | 128;
  const hex = [...bytes].map(value => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function ordinaryMessageFrame(entry: OutboxEntry): string {
  return JSON.stringify({ chat_id: entry.chat_id, content: entry.content, client_message_id: entry.client_message_id });
}

function queueOrdinaryMessage(chatID: number, content: string): boolean {
  if (!canReconnectChat() || !Number.isSafeInteger(chatID) || chatID <= 0 || !content.trim()) return false;
  // Match Go's escaped UTF-8 size limit, including non-ASCII text.
  const escaped = content.trim().replace(/&/g, "&amp;").replace(/'/g, "&#39;")
    .replace(/"/g, "&#34;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  if (new TextEncoder().encode(escaped).length >= 1500) {
    alert("Сообщение слишком длинное. Разделите его на несколько сообщений."); return false;
  }
  if (messageOutbox.size >= 100) { alert("В очереди уже 100 сообщений. Дождитесь отправки или удалите ненужные."); return false; }
  const entry: OutboxEntry = { version: 1, client_message_id: newClientMessageID(), chat_id: chatID,
    content: content.trim(), created_at: new Date().toISOString(), attempts: 0, state: "queued", retryable: true, error: "" };
  if (new TextEncoder().encode(ordinaryMessageFrame(entry)).length > 2048) {
    alert("Сообщение слишком длинное. Разделите его на несколько сообщений."); return false;
  }
  if (!persistOutboxEntry(entry)) {
    alert("Не удалось сохранить очередь отправки. Освободите локальное хранилище и повторите."); return false;
  }
  messageOutbox.set(entry.client_message_id, entry);
  renderPendingMessages(); flushMessageOutbox(); return true;
}

function clearOutboxTimer(id: string): void {
  clearTimeout(outboxTimers.get(id)); outboxTimers.delete(id); outboxRetryAt.delete(id);
}

function failOutboxEntry(entry: OutboxEntry, error: string, retryable: boolean): void {
  clearOutboxTimer(entry.client_message_id);
  entry.state = "failed"; entry.error = error; entry.retryable = retryable;
  persistOutboxEntry(entry); renderPendingMessages();
}

function deferOutboxEntry(entry: OutboxEntry): void {
  clearOutboxTimer(entry.client_message_id);
  if (entry.attempts >= outboxMaxAttempts) {
    failOutboxEntry(entry, "Сохранение не подтверждено. Можно безопасно повторить", true);
  } else {
    entry.state = "queued"; persistOutboxEntry(entry);
    const delay = 1000 * entry.attempts;
    outboxRetryAt.set(entry.client_message_id, Date.now() + delay);
    outboxTimers.set(entry.client_message_id, window.setTimeout(() => {
      clearOutboxTimer(entry.client_message_id); flushMessageOutbox();
    }, delay));
    renderPendingMessages();
  }
  flushMessageOutbox();
}

function flushMessageOutbox(): void {
  if (!canReconnectChat() || !socket || socket.readyState !== WebSocket.OPEN) return;
  if (messageReceiptsSupported !== true) {
    if (messageReceiptsSupported === false) {
      for (const entry of messageOutbox.values()) {
        if (entry.state === "queued") failOutboxEntry(entry, "Обновите сервер: подтверждения отправки не поддерживаются", true);
      }
    }
    return;
  }
  let active = [...messageOutbox.values()].filter(entry => entry.state === "sending").length;
  for (const entry of messageOutbox.values()) {
    if (active >= 4) break;
    if (entry.state !== "queued" || (outboxRetryAt.get(entry.client_message_id) || 0) > Date.now()) continue;
    if (entry.attempts >= outboxMaxAttempts) {
      failOutboxEntry(entry, "Сохранение не подтверждено. Можно безопасно повторить", true); continue;
    }
    const frame = ordinaryMessageFrame(entry);
    if (new TextEncoder().encode(frame).length > 2048) {
      failOutboxEntry(entry, "Сообщение превышает допустимый размер", false); continue;
    }
    entry.state = "sending"; entry.attempts++;
    if (!persistOutboxEntry(entry)) { failOutboxEntry(entry, "Не удалось обновить локальную очередь", true); continue; }
    active++;
    outboxTimers.set(entry.client_message_id, window.setTimeout(() => {
      outboxTimers.delete(entry.client_message_id); deferOutboxEntry(entry);
    }, 8000));
    try { socket.send(frame); }
    catch { deferOutboxEntry(entry); }
  }
  renderPendingMessages();
}

function pauseMessageOutbox(): void {
  for (const entry of messageOutbox.values()) {
    clearOutboxTimer(entry.client_message_id);
    // Persisted "sending" is restored as queued. Do not rewrite keys here:
    // another tab may just have removed them after an ACK or cleared site data.
    if (entry.state === "sending") entry.state = "queued";
  }
}

function forgetOutboxEntry(id: string): boolean {
  try { localStorage.removeItem(outboxPrefix + id); } catch { return false; }
  clearOutboxTimer(id); messageOutbox.delete(id); renderPendingMessages(); return true;
}

function handleMessageReceipt(msg: ChatMessage): boolean {
  if (msg.type !== "message_ack" && !(msg.type === "error" && msg.client_message_id)) return false;
  const entry = messageOutbox.get(msg.client_message_id);
  if (!entry || Number(msg.chat_id) !== entry.chat_id) return true;
  if (msg.type === "message_ack" && /^[1-9][0-9]*$/.test(String(msg.id))) {
    // Saved does not mean delivered/read by peers. History remains authoritative
    // after an edit/deletion or a lost broadcast following a successful commit.
    if (!forgetOutboxEntry(entry.client_message_id)) {
      failOutboxEntry(entry, "Сохранено на сервере; не удалось очистить локальную очередь", true); return true;
    }
    if (!(messageCache[entry.chat_id] || []).some(item => String(item.id) === String(msg.id))) void loadChatHistory(entry.chat_id, 0);
  } else if (msg.type === "error") {
    if (msg.retryable === true) deferOutboxEntry(entry);
    else failOutboxEntry(entry, String(msg.message || "Сообщение отклонено сервером").slice(0, 256), false);
  }
  flushMessageOutbox(); return true;
}

function renderPendingMessages(): void {
  messagesEl.querySelectorAll(".message-pending").forEach(element => element.remove());
  for (const entry of messageOutbox.values()) {
    if (entry.chat_id !== Number(currentChatID)) continue;
    const row = document.createElement("div"); row.className = "message-pending";
    const text = document.createElement("div"); text.className = "message-pending-text";
    text.textContent = /^\[(img|gif|voice|file[:\]])/.test(entry.content) ? "Вложение" : entry.content;
    const status = document.createElement("div"); status.setAttribute("role", "status");
    status.textContent = entry.state === "failed" ? entry.error : entry.state === "sending" ? "Сохраняется…" : "Ожидает соединения или повторной отправки…";
    row.append(text, status);
    if (entry.state === "failed") {
      if (entry.retryable) {
        const retry = document.createElement("button"); retry.type = "button"; retry.textContent = "Повторить";
        retry.addEventListener("click", () => {
          entry.attempts = 0; entry.state = "queued"; entry.error = "";
          persistOutboxEntry(entry); flushMessageOutbox(); renderPendingMessages();
        });
        row.append(retry);
      }
      const remove = document.createElement("button"); remove.type = "button"; remove.textContent = "Убрать из очереди";
      remove.addEventListener("click", () => {
        if (!confirm("Убрать из очереди? Если сервер уже сохранил сообщение, оно останется в истории.")) return;
        if (!forgetOutboxEntry(entry.client_message_id)) alert("Не удалось очистить локальную очередь");
      });
      row.append(remove);
    }
    messagesEl.append(row);
  }
}

window.addEventListener("storage", event => {
  if (!event.key?.startsWith(outboxPrefix)) return;
  const id = event.key.slice(outboxPrefix.length);
  if (event.newValue === null) { clearOutboxTimer(id); messageOutbox.delete(id); }
  else if (!messageOutbox.has(id)) {
    const entry = parseOutboxEntry(event.newValue, id);
    if (entry) { if (entry.state === "sending") entry.state = "queued"; messageOutbox.set(id, entry); }
  }
  renderPendingMessages(); flushMessageOutbox();
});
