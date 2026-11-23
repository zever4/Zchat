// ===== WEBSOCKET =====
const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
let socket: WebSocket;
let reconnectTimer: number | undefined;
let reconnectProbe: AbortController | undefined;
let socketTicketRequest: AbortController | undefined;
let reconnectAttempt = 0;
let chatConnectionStopped = false;
let connectionLostShown = false;
let messageReceiptsSupported: boolean | undefined;

function canReconnectChat(): boolean {
  return !chatConnectionStopped && !authExpiredHandled && localStorage.getItem("token") === token;
}

function stopChatConnection(): void {
  chatConnectionStopped = true;
  pauseMessageOutbox();
  clearTimeout(reconnectTimer);
  reconnectProbe?.abort();
  reconnectProbe = undefined;
  socketTicketRequest?.abort();
  socketTicketRequest = undefined;
  abortSessionRequests();
  cleanupCall();
  if (typeof stopAllAppSounds === "function") stopAllAppSounds();
  if (socket && socket.readyState < WebSocket.CLOSING) socket.close(1000, "Session ended");
}

function scheduleChatReconnect(immediate = false): void {
  clearTimeout(reconnectTimer);
  reconnectProbe?.abort();
  reconnectProbe = undefined;
  socketTicketRequest?.abort();
  socketTicketRequest = undefined;
  if (!canReconnectChat()) return;
  const delay = immediate ? 0 : Math.min(10000, 500 * 2 ** Math.min(reconnectAttempt++, 5));
  reconnectTimer = window.setTimeout(async () => {
    if (!canReconnectChat()) return;
    const probe = new AbortController();
    reconnectProbe = probe;
    try {
      // Expired/revoked sessions stop here instead of retrying an unauthorized WS forever.
      const response = await apiFetch("/api/profile", { signal: probe.signal, timeoutMs: 5000 });
      // Drain/cancel responses that are used only for their HTTP status.
      await response.body?.cancel();
      if (reconnectProbe !== probe || !canReconnectChat()) return;
      reconnectProbe = undefined;
      if (response.ok) void connectChatSocket();
      else scheduleChatReconnect();
    } catch {
      if (reconnectProbe === probe) { reconnectProbe = undefined; scheduleChatReconnect(); }
    }
  }, delay);
}

async function connectChatSocket(): Promise<void> {
  if (!canReconnectChat() || socketTicketRequest || (socket && socket.readyState < WebSocket.CLOSING)) return;
  const request = new AbortController();
  socketTicketRequest = request;
  let connection: WebSocket;
  try {
    const response = await apiFetch("/api/ws-ticket", { method: "POST", signal: request.signal, timeoutMs: 5000 });
    if (!response.ok) {
      await response.body?.cancel();
      if (socketTicketRequest === request && canReconnectChat()) scheduleChatReconnect();
      return;
    }
    const data = await response.json() as { ticket?: unknown };
    if (socketTicketRequest !== request || !canReconnectChat()) return;
    if (typeof data.ticket !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(data.ticket)) throw new Error("Invalid connection ticket");
    // The one-use ticket stays in this request only; never persist or log it.
    connection = new WebSocket(`${wsProtocol}//${window.location.host}/ws?ticket=${encodeURIComponent(data.ticket)}`);
  } catch {
    if (socketTicketRequest === request && canReconnectChat()) scheduleChatReconnect();
    return;
  } finally {
    if (socketTicketRequest === request) socketTicketRequest = undefined;
  }
  socket = connection;
  const handshakeTimer = window.setTimeout(() => {
    if (socket === connection && connection.readyState === WebSocket.CONNECTING) connection.close();
  }, 10000);
  connection.onopen = (): void => {
    clearTimeout(handshakeTimer);
    if (socket !== connection || !canReconnectChat()) { connection.close(); return; }
    clearTimeout(reconnectTimer);
    reconnectAttempt = 0;
    connectionLostShown = false;
    messageReceiptsSupported = undefined;
    console.log("WebSocket подключен");
    // Resync after reconnect, including missed edits/deletes in the active chat.
    loadChats();
    loadFriendRequests();
    if (currentChatID !== null) loadChatHistory(currentChatID, 0);
    flushMessageOutbox();
  };
  connection.onmessage = (event: MessageEvent): void => {
    if (socket !== connection || !canReconnectChat()) return;
    for (const part of String(event.data || "").split("\n")) {
      if (!part.trim()) continue;
      try {
        const msg = JSON.parse(part) as ChatMessage;
        // Correlated transport receipts must bypass legacy UI wrappers, some
        // of which intercept every error with a blocking group-permission alert.
        if (!handleMessageReceipt(msg)) handleIncomingMessage(msg);
      }
      catch (error) { console.error("Ошибка обработки сообщения:", error); }
    }
  };
  connection.onclose = (): void => {
    clearTimeout(handshakeTimer);
    if (socket !== connection || !canReconnectChat()) return;
    pauseMessageOutbox();
    if (typeof callState !== "undefined" && callState === "ringing") cleanupCall();
    messageReceiptsSupported = undefined;
    if (!connectionLostShown) {
      connectionLostShown = true;
      renderSystemMessage("Соединение разорвано. Переподключение…");
    }
    scheduleChatReconnect();
  };
  connection.onerror = (): void => {
    // onclose schedules a single retry; no competing reconnect loop here.
  };
}

window.addEventListener("online", () => {
  if (!socket || socket.readyState === WebSocket.CLOSED) scheduleChatReconnect(true);
});
window.addEventListener("pagehide", stopChatConnection);
window.addEventListener("pageshow", event => { if (event.persisted) window.location.reload(); });
window.addEventListener("storage", event => {
  if ((event.key === "token" || event.key === null) && localStorage.getItem("token") !== token) handleAuthExpired();
});

let handleIncomingMessage: HandleIncomingMessageFn = function(msg: ChatMessage): void {
  // 1. Приводим ID из сообщения к числу сразу
  const msgChatID = Number(msg.chat_id || 0);
  const activeChatID = Number(currentChatID || 0);

  // Handle typing indicator
  if (msg.type === "typing") {
    showTypingIndicator(msg);
    return;
  }

  if (msg.type === "error") {
    if (msgChatID === activeChatID) renderSystemMessage(msg.message || "Сообщение отклонено сервером");
    return;
  }

  // System messages
  if (msg.type === "system") {
    if (msg.sender_name === "Zchat" && msg.content === "Добро пожаловать!") {
      messageReceiptsSupported = msg.message_receipts === true;
      if (messageReceiptsSupported) flushMessageOutbox();
      else {
        for (const entry of messageOutbox.values()) {
          if (entry.state === "queued") failOutboxEntry(entry, "Обновите сервер: подтверждения отправки не поддерживаются", true);
        }
      }
    }
    renderSystemMessage(msg.content);
    messagesEl.scrollTop = messagesEl.scrollHeight;
    return;
  }

  // Clear typing indicator
  if (msg.sender_id && typingUsers[msg.sender_id]) {
    clearTimeout(typingUsers[msg.sender_id].timeout);
    delete typingUsers[msg.sender_id];
    updateTypingDisplay();
  }

  if (!msgChatID || msg.id == null) return;
  if (!messageCache[msgChatID]) messageCache[msgChatID] = [];
  if (messageCache[msgChatID].some(m => String(m.id) === String(msg.id))) return;
  recordHistoryMutation(msg);
  messageCache[msgChatID].push(msg);
  setChatPreviewFromMessage(msgChatID, msg, true);

  // 2. Сравнение через приведенные к числам переменные
  if (msgChatID === activeChatID) {
    renderMessage(msg);
    renderPendingMessages();
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  // 3. Исправленный поиск в списке чатов (через приведение типов)
  const chatExists = chats.find((c) => Number(c.id) === msgChatID);
  if ((msg.type === "private" || msg.type === "group") && !chatExists) {
    loadChats();
  }
};

void connectChatSocket();
