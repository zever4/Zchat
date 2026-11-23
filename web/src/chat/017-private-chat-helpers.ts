// ===== PRIVATE CHAT AND NOTIFICATION HELPERS =====
function getChatOtherUserID(chat: ChatRecord | null | undefined): number {
  if (!chat) return 0;
  const rawID = chat.other_user_id ?? chat.otherUserId ?? chat.otherUserID ?? 0;
  const id = Number(rawID);
  return Number.isFinite(id) ? id : 0;
}

async function resolvePrivateChatUserID(target: ChatTarget | null | undefined): Promise<number> {
  if (!target) return 0;
  const chat = target.chat || getChatByID(target.id);
  const directID = getChatOtherUserID(chat);
  if (directID > 0) return directID;
  const chatID = Number(target.id || 0);
  if (!chatID) return 0;
  try {
    const res = await apiFetch("/api/chats/" + chatID + "/members");
    if (!res.ok) return 0;
    const members = await res.json() as ChatMemberListItem[];
    const otherUser = members.find((member) => member && member.username !== currentUsername);
    return Number(otherUser && otherUser.id ? otherUser.id : 0);
  } catch (e) {
    console.error("Failed to resolve private chat user id:", e);
    return 0;
  }
}

function buildIncomingChatNotificationText(msg: ChatMessage): string {
  const chatID = Number(msg.chat_id || 0);
  const chat = getChatByID(chatID);
  const preview = escapeHtml(truncateText(msg.content || "Новое сообщение", 80));
  const groupInfo = groupInfoCache[chatID] || null;
  if (msg.type === "group") {
    const groupName = escapeHtml((chat && chat.name) || "Группа");
    if (groupInfo && groupInfo.anonymous_mode) {
      return "<strong>" + groupName + "</strong>: " + preview;
    }
    const sender = escapeHtml(msg.sender_name || "Пользователь");
    return "<strong>" + groupName + "</strong>: " + sender + " — " + preview;
  }
  const authorName = escapeHtml(msg.sender_name || (chat && chat.other_username) || "Пользователь");
  return "<strong>" + authorName + "</strong>: " + preview;
}

