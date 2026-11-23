// ===== RENDER FUNCTIONS =====
let renderMessage: RenderMessageFn = function(msg: ChatMessage, isPrepend = false): void {
  if (msg.type === "system") {
    renderSystemMessage(msg.content, isPrepend);
    return;
  }

  const wrapper = document.createElement("div");
  wrapper.className = "msg-wrapper";

  const isOwn = msg.sender_name === currentUsername;
  wrapper.classList.add(isOwn ? "outgoing" : "incoming");

  // Avatar
  const avatarDiv = document.createElement("div");
  avatarDiv.className = "msg-avatar";
  // У gRPC-Gateway поля могут быть snake_case (sender_avatar)
  const avatarURL = isOwn ? currentAvatarURL : msg.sender_avatar || "";

  if (avatarURL) {
    avatarDiv.style.backgroundImage = "url(" + displayAssetURL(avatarURL) + ")";
    avatarDiv.style.backgroundSize = "cover";
    avatarDiv.textContent = "";
  } else {
    avatarDiv.style.backgroundImage = "";
    avatarDiv.textContent = (msg.sender_name || "?").charAt(0).toUpperCase();
  }

  // Bubble
  const bubble = document.createElement("div");
  bubble.className = "msg";
  const rich = renderMessageContent(msg.content);
  bubble.innerHTML =
    '<div class="msg-name">' +
    escapeHtml(msg.sender_name) +
    "</div>" +
    '<div class="msg-text">' +
    rich.textHTML +
    "</div>" +
    rich.cardHTML +
    '<div class="msg-time">' +
    formatTime(msg.created_at) +
    "</div>";
  attachInviteCardHandler(bubble);

  wrapper.appendChild(avatarDiv);
  wrapper.appendChild(bubble);

  // ЛОГИКА ВСТАВКИ:
  if (isPrepend) {
    messagesEl.prepend(wrapper); // Добавляем в начало (верх) списка
  } else {
    messagesEl.appendChild(wrapper); // Добавляем в конец (низ)
  }
};

function renderSystemMessage(text: string | null | undefined, isPrepend = false): void {
  const div = document.createElement("div");
  div.className = "msg-system";
  div.textContent = text || "";

  if (isPrepend) {
    messagesEl.prepend(div);
  } else {
    messagesEl.appendChild(div);
  }
}

function formatTime(isoString: string | null | undefined): string {
  if (!isoString) return "";
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return "";
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function escapeAttr(text: unknown): string {
  if (!text) return "";
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function trimTrailingPunctuation(url: string): { clean: string; tail: string } {
  let clean = url;
  let tail = "";
  while (/[),.!?;:]$/.test(clean)) {
    tail = clean.slice(-1) + tail;
    clean = clean.slice(0, -1);
  }
  return { clean, tail };
}

const inviteCardMetaCache: Record<string, InviteCardMeta> = {};
const inviteCardMetaPending: Record<string, Promise<InviteCardMeta | null>> = {};

function getInviteCodeFromURL(urlText: string): string {
  try {
    const parsed = new URL(urlText, window.location.href);
    const code = (parsed.searchParams.get("invite") || "").trim();
    return code;
  } catch {
    return "";
  }
}

function isInviteLink(urlText: string): boolean {
  return getInviteCodeFromURL(urlText).length > 0;
}

function buildInviteCardHTML(urlText: string): string {
  const code = getInviteCodeFromURL(urlText);
  return (
    '<div class="invite-card" data-invite-link="' +
    escapeAttr(urlText) +
    '" data-invite-code="' +
    escapeAttr(code) +
    '">' +
    '<div class="invite-card-head">' +
    '<div class="invite-card-top">' +
    '<div class="invite-card-avatar">G</div>' +
    '<div class="invite-card-meta">' +
    '<div class="invite-card-brand">Zchat</div>' +
    '<div class="invite-card-title">Приглашение в группу</div>' +
    '<div class="invite-card-name">Загрузка...</div>' +
    '<div class="invite-card-desc">Получаем данные группы...</div>' +
    "</div>" +
    "</div>" +
    "</div>" +
    '<button type="button" class="invite-card-btn">Открыть группу</button>' +
    "</div>"
  );
}

function inviteButtonLabel(meta: InviteCardMeta | null): string {
  if (!meta) return "Открыть группу";
  if (meta.is_member) return "Перейти в группу";
  if (meta.request_status === "pending") return "Заявка отправлена";
  if (meta.is_public === false) return "Подать заявку";
  return "Вступить в группу";
}

function inviteDescription(meta: InviteCardMeta | null): string {
  if (!meta) return "Перейдите по ссылке, чтобы вступить.";
  if (meta.is_member) return "Вы уже участник этой группы.";
  if (meta.request_status === "pending") return "Заявка уже отправлена владельцу.";
  if (meta.is_public === false) return "Группа закрытая: вход после одобрения заявки.";
  return "Открытая группа: вступление по кнопке.";
}

function applyInviteCardMeta(card: HTMLElement | null, meta: InviteCardMeta | null): void {
  if (!card || !meta) return;

  card.dataset.chatId = String(meta.chat_id || 0);
  card.dataset.isMember = meta.is_member ? "1" : "0";
  card.dataset.isPublic = meta.is_public === false ? "0" : "1";
  card.dataset.requestStatus = meta.request_status || "";

  const nameEl = card.querySelector<HTMLElement>(".invite-card-name");
  const descEl = card.querySelector<HTMLElement>(".invite-card-desc");
  const avatarEl = card.querySelector<HTMLElement>(".invite-card-avatar");
  const btn = card.querySelector<HTMLButtonElement>(".invite-card-btn");

  const name = String(meta.name || "Группа");
  if (nameEl) nameEl.textContent = name;
  if (descEl) descEl.textContent = inviteDescription(meta);

  if (avatarEl) {
    if (meta.avatar_url) {
      avatarEl.style.backgroundImage = "url(" + displayAssetURL(meta.avatar_url) + ")";
      avatarEl.style.backgroundSize = "cover";
      avatarEl.style.backgroundPosition = "center";
      avatarEl.textContent = "";
    } else {
      avatarEl.style.backgroundImage = "";
      avatarEl.textContent = name.charAt(0).toUpperCase();
    }
  }

  if (btn) {
    btn.textContent = inviteButtonLabel(meta);
    btn.disabled = meta.request_status === "pending";
    btn.classList.toggle("is-pending", meta.request_status === "pending");
  }
}

async function fetchInviteCardMeta(code: string): Promise<InviteCardMeta | null> {
  const key = String(code || "");
  if (!key) return null;
  if (inviteCardMetaCache[key]) return inviteCardMetaCache[key];
  if (inviteCardMetaPending[key]) return inviteCardMetaPending[key];

  inviteCardMetaPending[key] = apiFetch("/api/group-invites/" + encodeURIComponent(key))
    .then(async (res) => {
      if (!res.ok) return null;
      const data = await res.json() as InviteCardMeta;
      inviteCardMetaCache[key] = data;
      return data;
    })
    .catch((e) => {
      console.error("invite card meta fetch error:", e);
      return null;
    })
    .finally(() => {
      delete inviteCardMetaPending[key];
    });

  return inviteCardMetaPending[key];
}

async function openGroupFromInviteMeta(meta: InviteCardMeta | null, fallbackLink: string): Promise<void> {
  const chatID = Number(meta && meta.chat_id ? meta.chat_id : 0);
  if (!chatID) {
    if (fallbackLink) window.location.href = fallbackLink;
    return;
  }

  let chat = chats.find((c) => Number(c.id) === chatID);
  if (!chat) {
    await loadChats();
    chat = chats.find((c) => Number(c.id) === chatID);
  }

  if (chat) {
    const chatName = chat.type === "group" ? (chat.name || "Группа") : (chat.other_username || "...");
    switchChat(chat.id, chatName, chat.type || "group");
    return;
  }

  if (fallbackLink) window.location.href = fallbackLink;
}

async function hydrateInviteCard(card: HTMLElement | null): Promise<InviteCardMeta | null> {
  if (!card) return null;
  const code = card.dataset.inviteCode || getInviteCodeFromURL(card.dataset.inviteLink || "");
  if (!code) return null;
  card.dataset.inviteCode = code;

  const meta = await fetchInviteCardMeta(code);
  if (meta) {
    applyInviteCardMeta(card, meta);
  }
  return meta;
}

async function handleInviteCardAction(card: HTMLElement | null): Promise<void> {
  if (!card) return;

  const link = card.dataset.inviteLink || "";
  const code = card.dataset.inviteCode || getInviteCodeFromURL(link);
  if (!code) {
    if (link) window.location.href = link;
    return;
  }

  const btn = card.querySelector<HTMLButtonElement>(".invite-card-btn");
  const cached = inviteCardMetaCache[code] || null;
  const meta = cached || await hydrateInviteCard(card);
  if (!meta) {
    if (link) window.location.href = link;
    return;
  }

  if (meta.is_member) {
    await openGroupFromInviteMeta(meta, link);
    return;
  }
  if (meta.request_status === "pending") {
    return;
  }

  if (btn) btn.disabled = true;
  try {
    const res = await apiFetch("/api/groups/invite/" + encodeURIComponent(code) + "/join", {
      method: "POST",
    });
    const payload = await res.json().catch(() => ({} as InviteCardMeta));
    if (!res.ok) {
      if (btn) btn.disabled = false;
      alert(payload.message || "Не удалось обработать приглашение");
      return;
    }

    if (payload.status === "joined") {
      const nextMeta = {
        ...meta,
        chat_id: Number(payload.chat_id || meta.chat_id || 0),
        is_member: true,
        request_status: "",
      };
      inviteCardMetaCache[code] = nextMeta;
      applyInviteCardMeta(card, nextMeta);
      await loadChats();
      await openGroupFromInviteMeta(nextMeta, link);
      return;
    }

    if (payload.status === "pending") {
      const nextMeta = {
        ...meta,
        chat_id: Number(payload.chat_id || meta.chat_id || 0),
        is_member: false,
        request_status: "pending",
      };
      inviteCardMetaCache[code] = nextMeta;
      applyInviteCardMeta(card, nextMeta);
      return;
    }

    if (btn) btn.disabled = false;
  } catch (e) {
    console.error("invite join action error:", e);
    if (btn) btn.disabled = false;
  }
}

function renderMessageContent(content: unknown): RenderedMessageContent {
  const raw = String(content || "");
  const urlRegex = /https?:\/\/[^\s<>"']+/gi;

  let result = "";
  let lastIndex = 0;
  let inviteLink = "";
  let match;

  while ((match = urlRegex.exec(raw)) !== null) {
    const fullMatch = match[0];
    const start = match.index;
    const end = start + fullMatch.length;
    result += escapeHtml(raw.slice(lastIndex, start));

    const trimmed = trimTrailingPunctuation(fullMatch);
    let safeHref = "";
    try {
      const parsed = new URL(trimmed.clean);
      if (parsed.protocol === "http:" || parsed.protocol === "https:") {
        safeHref = parsed.href;
      }
    } catch {
      safeHref = "";
    }

    if (safeHref) {
      if (!inviteLink && isInviteLink(safeHref)) inviteLink = safeHref;
      result +=
        '<a class="msg-link" href="' +
        escapeAttr(safeHref) +
        '" target="_blank" rel="noopener noreferrer">' +
        escapeHtml(trimmed.clean) +
        "</a>" +
        escapeHtml(trimmed.tail);
    } else {
      result += escapeHtml(fullMatch);
    }

    lastIndex = end;
  }

  result += escapeHtml(raw.slice(lastIndex));

  return {
    textHTML: result,
    cardHTML: inviteLink ? buildInviteCardHTML(inviteLink) : "",
  };
}

function attachInviteCardHandler(rootNode: ParentNode | null): void {
  if (!rootNode) return;
  const cards = rootNode.querySelectorAll<HTMLElement>(".invite-card");
  cards.forEach((card) => {
    if (card.dataset.bound === "1") return;
    card.dataset.bound = "1";
    hydrateInviteCard(card);
    const btn = card.querySelector<HTMLButtonElement>(".invite-card-btn");
    if (!btn) return;
    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      await handleInviteCardAction(card);
    });
  });
}

function escapeHtml(text: unknown): string {
  if (!text) return "";
  const div = document.createElement("div");
  div.textContent = String(text);
  return div.innerHTML;
}
