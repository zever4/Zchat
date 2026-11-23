// ===== LOAD DATA =====
async function loadChats(): Promise<void> {
  try {
    const res = await apiFetch("/api/chats");
    if (res.ok) {
      const data = await res.json() as { chats?: ChatRecord[] };
      chats = data.chats || [];
      Object.keys(chatPreviewCache).forEach((key) => {
        const numericID = Number(key);
        if (!chats.find((chat) => Number(chat.id) === numericID)) {
          delete chatPreviewCache[key];
        }
      });

      renderChatList();
      loadChatPreviews();
      maybeJoinGroupFromInvite();
    }
  } catch (e) {
    console.error("Failed to load chats:", e);
  }
}

async function loadFriendRequests(): Promise<void> {
  try {
    const res = await apiFetch("/api/friends/requests");
    if (res.ok) {
      const data = await res.json() as { requests?: FriendRequestRecord[] };
      requests = data.requests || [];
      renderFriendRequests(requests);
    }
  } catch (e) {
    console.error("Failed to load friend requests:", e);
  }
}
