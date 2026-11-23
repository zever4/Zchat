// ===== SWITCH CHAT / HISTORY =====
interface ChatHistoryRequest {
  controller: AbortController;
  viewVersion: number;
  events: ChatMessage[];
}
const chatHistoryRequests = new Map<number, ChatHistoryRequest>();
const exhaustedChatHistory = new Set<number>();
let chatViewVersion = 0;

function updateHistoryLoadingState(): void {
  isChatLoading = chatHistoryRequests.has(Number(currentChatID));
}

function recordHistoryMutation(msg: ChatMessage): void {
  chatHistoryRequests.get(Number(msg.chat_id))?.events.push({ ...msg });
}

function mergeHistoryMessages(messages: ChatMessage[], events: ChatMessage[]): ChatMessage[] {
  const byID = new Map<string, ChatMessage>();
  for (const msg of messages) {
    if (msg.id != null) byID.set(String(msg.id), msg);
  }
  for (const event of events) {
    const key = String(event.id);
    if (event.type === "delete") byID.delete(key);
    else if (event.type === "edit") {
      const existing = byID.get(key);
      if (existing) byID.set(key, { ...existing, content: event.content });
    } else if (event.id != null) byID.set(key, event);
  }
  return [...byID.values()].sort((a, b) => {
    const first = BigInt(String(a.id)), second = BigInt(String(b.id));
    return first < second ? -1 : first > second ? 1 : 0;
  });
}

let switchChat: SwitchChatFn = function(chatID: number | string, chatName?: string | null, chatType?: string | null): void {
  leaveE2eeView();
  const targetID = Number(chatID);
  currentChatID = targetID;
  chatViewVersion++;
  const chatMeta = chats.find((c) => Number(c.id) === targetID);
  const resolvedType = chatType || (chatMeta ? chatMeta.type : "private");
  currentChatType = resolvedType;

  messagesEl.innerHTML = "";
  clearTypingDisplay();

  const resolvedName = chatName || (chatMeta && (chatMeta.type === "group" ? chatMeta.name : chatMeta.other_username)) || "...";
  chatNameEl.textContent = resolvedName;
  if (resolvedType === "group") {
    chatStatusEl.textContent = "Групповой чат";
  } else {
    chatStatusEl.textContent = "Личная переписка";
  }
  if (groupSettingsBtn) {
    groupSettingsBtn.style.display = resolvedType === "group" ? "inline-flex" : "none";
  }

  queryAll<HTMLElement>(".private-chat-item").forEach((el) => {
    el.classList.toggle("active", Number(el.dataset.chatId) === targetID);
  });

  if (messageCache[targetID] && messageCache[targetID].length > 0) {
    messageCache[targetID].forEach((msg) => renderMessage(msg));
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  loadChatHistory(targetID, 0);
  renderPendingMessages();
};

function maybeJoinGroupFromInvite(): void {
  if (inviteJoinHandled) return;

  const url = new URL(window.location.href);
  const code = (url.searchParams.get("invite") || "").trim();
  if (!code) return;
  inviteJoinHandled = true;

  apiFetch("/api/groups/invite/" + encodeURIComponent(code) + "/join", { method: "POST" })
    .then(async (res) => {
      const payload = await res.json().catch(() => ({} as { message?: string; status?: string; chat_id?: number | string }));
      if (!res.ok) {
        alert(payload.message || "Не удалось вступить по ссылке");
        return;
      }

      if (payload.status === "joined") {
        await loadChats();
        const joinedChat = chats.find((c) => Number(c.id) === Number(payload.chat_id));
        if (joinedChat) {
          const joinedName = joinedChat.type === "group" ? (joinedChat.name || "Группа") : (joinedChat.other_username || "...");
          switchChat(joinedChat.id, joinedName, joinedChat.type);
        }
      } else if (payload.status === "pending") {
        alert("Заявка в закрытую группу отправлена владельцу.");
      }
    })
    .catch((e) => {
      console.error("join invite error:", e);
      alert("Не удалось обработать invite-ссылку.");
    })
    .finally(() => {
      url.searchParams.delete("invite");
      window.history.replaceState({}, "", url.pathname + (url.search || "") + url.hash);
    });
}

async function loadChatHistory(chatID: number | string, lastMessageID: number | string = 0): Promise<void> {
  const targetChatID = Number(chatID);
  if (!targetChatID) return;
  const initialPage = String(lastMessageID) === "0";
  const pending = chatHistoryRequests.get(targetChatID);
  if (!initialPage && (pending || exhaustedChatHistory.has(targetChatID))) return;
  pending?.controller.abort();
  const request: ChatHistoryRequest = {
    controller: new AbortController(), viewVersion: chatViewVersion, events: [],
  };
  chatHistoryRequests.set(targetChatID, request);
  updateHistoryLoadingState();

  try {
    const url = `/api/chats/${targetChatID}/messages?limit=50&last_message_id=${lastMessageID}`;
    const res = await apiFetch(url, { signal: request.controller.signal });
    if (!res.ok) return;
    const data = await res.json() as { messages?: ChatMessage[]; has_more?: boolean };
    // An aborted/superseded response must not change cache, DOM or loading state.
    if (chatHistoryRequests.get(targetChatID) !== request) return;
    const newMessages = data.messages || [];
    if (data.has_more === false || newMessages.length < 50) exhaustedChatHistory.add(targetChatID);
    else exhaustedChatHistory.delete(targetChatID);

    const cached = messageCache[targetChatID] || [];
    const previousIDs = new Set(cached.map(msg => String(msg.id)));
    const merged = mergeHistoryMessages(initialPage ? newMessages : [...newMessages, ...cached], request.events);
    messageCache[targetChatID] = merged;
    syncChatPreviewFromCache(targetChatID, true);

    if (Number(currentChatID) !== targetChatID || chatViewVersion !== request.viewVersion) return;
    if (initialPage) {
      messagesEl.innerHTML = "";
      merged.forEach(msg => renderMessage(msg));
      renderPendingMessages();
      messagesEl.scrollTop = messagesEl.scrollHeight;
    } else {
      const oldHeight = messagesEl.scrollHeight, oldTop = messagesEl.scrollTop;
      merged.filter(msg => !previousIDs.has(String(msg.id))).reverse().forEach(msg => renderMessage(msg, true));
      messagesEl.scrollTop = oldTop + messagesEl.scrollHeight - oldHeight;
    }
  } catch (error) {
    if (!request.controller.signal.aborted) console.error("Error loading history:", error);
  } finally {
    if (chatHistoryRequests.get(targetChatID) === request) chatHistoryRequests.delete(targetChatID);
    updateHistoryLoadingState();
  }
}

messagesEl.onscroll = (): void => {
  if (currentChatID !== null && messagesEl.scrollTop === 0 && !exhaustedChatHistory.has(currentChatID)) {
    const cached = messageCache[currentChatID];
    if (cached && cached.length >= 50) loadChatHistory(currentChatID, cached[0].id);
  }
};
