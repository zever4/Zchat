// ===== FRIEND REQUESTS =====
let renderFriendRequests: RenderFriendRequestsFn = function(nextRequests: FriendRequestRecord[]): void {
  if (!nextRequests || nextRequests.length === 0) {
    friendRequestsSection.style.display = "none";
    return;
  }
  friendRequestsSection.style.display = "block";
  requestCountEl.textContent = String(nextRequests.length);
  friendRequestsList.innerHTML = "";

  for (const req of nextRequests) {
    const div = document.createElement("div");
    div.className = "friend-request-sidebar";
    div.innerHTML =
      '<span class="fr-name">' +
      escapeHtml(req.from_username) +
      "</span>" +
      '<div class="fr-actions">' +
      '<button class="action-btn success">&#10003;</button>' +
      '<button class="action-btn danger">&#10005;</button>' +
      "</div>";
    div.querySelector<HTMLButtonElement>("button.success")!.addEventListener("click", () => { void acceptRequest(req.id); });
    div.querySelector<HTMLButtonElement>("button.danger")!.addEventListener("click", () => { void rejectRequest(req.id); });
    friendRequestsList.appendChild(div);
  }
};

async function acceptRequest(requestID: number | string): Promise<void> {
  try {
    const id = Number(requestID);

    const res = await apiFetch(`/api/friends/requests/${id}/accept`, {
      method: "POST",
    });

    if (res.ok) {
      const chat = await res.json() as ChatRecord | null;
      await loadChats();
      await loadFriendRequests();

      if (chat && chat.id) {
        switchChat(chat.id, chat.other_username, "private");
      }
    } else {
      alert("Не удалось принять заявку в друзья");
    }
  } catch (e) {
    console.error("Failed to accept request:", e);
  }
}

async function rejectRequest(requestID: number | string): Promise<void> {
  try {
    const id = Number(requestID);
    const res = await apiFetch(`/api/friends/requests/${id}/reject`, {
      method: "POST",
    });
    if (res.ok) {
      await loadFriendRequests();
    }
  } catch (e) {
    console.error("Failed to reject request:", e);
  }
}
