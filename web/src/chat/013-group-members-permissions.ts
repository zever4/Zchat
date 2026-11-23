// ===== GROUP MEMBERS & PERMISSIONS =====
async function loadGroupMembers(): Promise<void> {
  const list = optionalById<HTMLDivElement>("groupMembersList");
  if (!list) return;
  list.innerHTML = '<div class="group-list-empty">Загрузка...</div>';
  const currentGroupID = Number(currentChatID || 0);

  try {
    const res = await apiFetch("/api/groups/" + currentGroupID + "/members");
    if (!res.ok) {
      list.innerHTML = '<div class="group-list-empty">Не удалось загрузить участников</div>';
      return;
    }
    const data = await res.json() as { members?: GroupMemberRecord[] };
    const members = data.members || [];
    if (activeGroupSettings) {
      activeGroupSettings.member_count = members.length;
    }
    if (groupInfoCache[currentGroupID]) {
      groupInfoCache[currentGroupID].member_count = members.length;
    }
    applyGroupChatStatus(currentGroupID);
    renderGroupMembers(members);
  } catch (e) {
    console.error("load group members error:", e);
    list.innerHTML = '<div class="group-list-empty">Ошибка сети</div>';
  }
}

function renderGroupMembers(members: GroupMemberRecord[]): void {
  const list = optionalById<HTMLDivElement>("groupMembersList");
  if (!list) return;
  list.innerHTML = "";

  if (!members || members.length === 0) {
    list.innerHTML = '<div class="group-list-empty">Нет участников</div>';
    return;
  }

  const myRole = activeGroupSettings ? (activeGroupSettings.role || "member") : "member";

  members.forEach(function(m) {
    const row = document.createElement("div");
    row.className = "gs-member-row";

    const avatarDiv = document.createElement("div");
    avatarDiv.className = "gs-member-avatar";
    if (m.avatar_url) {
      avatarDiv.style.backgroundImage = "url(" + displayAssetURL(m.avatar_url) + ")";
      avatarDiv.style.backgroundSize = "cover";
      avatarDiv.style.backgroundPosition = "center";
      avatarDiv.textContent = "";
    } else {
      avatarDiv.textContent = (m.username || "?").charAt(0).toUpperCase();
    }
    row.appendChild(avatarDiv);

    const info = document.createElement("div");
    info.className = "gs-member-info";

    const nameSpan = document.createElement("span");
    nameSpan.className = "gs-member-name";
    nameSpan.textContent = m.username || "...";
    info.appendChild(nameSpan);

    if (m.role === "owner") {
      const badge = document.createElement("span");
      badge.className = "gs-role-badge gs-role-owner";
      badge.textContent = "Создатель";
      info.appendChild(badge);
    } else if (m.role === "admin") {
      const badge2 = document.createElement("span");
      badge2.className = "gs-role-badge gs-role-admin";
      badge2.textContent = "Админ";
      info.appendChild(badge2);
    }
    row.appendChild(info);

    const actions = document.createElement("div");
    actions.className = "gs-member-actions";

    // Only owner can promote/demote
    if (myRole === "owner" && m.role !== "owner") {
      if (m.role === "admin") {
        const demoteBtn = document.createElement("button");
        demoteBtn.className = "gs-member-btn gs-btn-demote";
        demoteBtn.textContent = "Снять админа";
        demoteBtn.onclick = function() { setMemberRole(m.user_id, "member"); };
        actions.appendChild(demoteBtn);
      } else {
        const promoteBtn = document.createElement("button");
        promoteBtn.className = "gs-member-btn gs-btn-promote";
        promoteBtn.textContent = "Назначить админом";
        promoteBtn.onclick = function() { setMemberRole(m.user_id, "admin"); };
        actions.appendChild(promoteBtn);
      }
    }

    // Owner/admin can kick (owner can kick anyone, admin can kick members)
    if (m.role !== "owner" && (myRole === "owner" || (myRole === "admin" && m.role === "member"))) {
      const kickBtn = document.createElement("button");
      kickBtn.className = "gs-member-btn gs-btn-kick";
      kickBtn.textContent = "Исключить";
      kickBtn.onclick = function() { removeGroupMember(m.user_id, m.username); };
      actions.appendChild(kickBtn);
    }

    row.appendChild(actions);
    list.appendChild(row);
  });
}

async function setMemberRole(targetUserID: number | string, newRole: GroupRole): Promise<void> {
  const currentGroupID = Number(currentChatID || 0);
  try {
    const res = await apiFetch("/api/groups/" + currentGroupID + "/members/" + targetUserID + "/role", {
      method: "PUT",
      body: JSON.stringify({ role: newRole }),
    });
    if (!res.ok) {
      const text = await res.text();
      showGroupSettingsFeedback(text || "Не удалось изменить роль", true);
      return;
    }
    await Promise.all([loadGroupMembers(), loadGroupAuditLogs()]);
    showGroupSettingsFeedback(newRole === "admin" ? "Пользователь назначен администратором" : "Пользователь снят с администратора", false);
  } catch (e) {
    console.error("set member role error:", e);
    showGroupSettingsFeedback("Ошибка сети", true);
  }
}

async function removeGroupMember(targetUserID: number | string, username?: string): Promise<void> {
  if (!confirm('Исключить участника "' + (username || targetUserID) + '" из группы?')) return;
  const currentGroupID = Number(currentChatID || 0);

  try {
    const res = await apiFetch("/api/groups/" + currentGroupID + "/members/" + targetUserID, {
      method: "DELETE",
    });
    const mediaPending = isCallCleanupPending(res);
    if (!res.ok && !mediaPending) {
      const text = await res.text();
      showGroupSettingsFeedback(text || "Не удалось исключить участника", true);
      return;
    }
    await res.body?.cancel();
    await Promise.all([loadGroupMembers(), loadGroupAuditLogs()]);
    await refreshGroupRuntime(currentGroupID);
    showGroupSettingsFeedback("Участник исключён" + (mediaPending ? ". " + pendingCallCleanupNotice() : ""), mediaPending);
  } catch (e) {
    console.error("remove member error:", e);
    showGroupSettingsFeedback("Ошибка сети", true);
  }
}

async function updateGroupPermissions(body: Record<string, unknown>, successText: string, rollback?: () => void): Promise<boolean> {
  const feedback = optionalById<HTMLDivElement>("groupPermsFeedback");
  const currentGroupID = Number(currentChatID || 0);
  try {
    const res = await apiFetch("/api/groups/" + currentGroupID + "/permissions", {
      method: "PUT",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      if (typeof rollback === "function") rollback();
      if (feedback) {
        feedback.textContent = text || "Не удалось обновить права";
        feedback.style.display = "block";
        feedback.style.color = "var(--red)";
      }
      return false;
    }

    const data = await res.json() as Partial<GroupSettingsRecord>;
    if (activeGroupSettings) {
      activeGroupSettings = { ...activeGroupSettings, ...data };
    }
    syncGroupInfoCache(currentGroupID, {
      id: currentGroupID,
      owner_id: activeGroupSettings ? activeGroupSettings.owner_id : 0,
      role: activeGroupSettings ? activeGroupSettings.role : "member",
      is_owner: activeGroupSettings ? activeGroupSettings.is_owner : false,
      name: activeGroupSettings ? activeGroupSettings.name : "",
      member_count: activeGroupSettings ? activeGroupSettings.member_count : 0,
      admins_only_write: data.admins_only_write,
      hide_members_list: data.hide_members_list,
      anonymous_mode: data.anonymous_mode,
    });
    applyGroupChatStatus(currentGroupID);
    updateMessageInputState();
    if (infoPanelOpen && currentChatType === "group") {
      await loadChatMembers(currentGroupID);
    }
    await loadGroupAuditLogs();
    if (feedback) {
      feedback.textContent = successText;
      feedback.style.display = "block";
      feedback.style.color = "var(--green)";
    }
    return true;
  } catch (e) {
    console.error("update group permissions error:", e);
    if (typeof rollback === "function") rollback();
    if (feedback) {
      feedback.textContent = "Ошибка сети";
      feedback.style.display = "block";
      feedback.style.color = "var(--red)";
    }
    return false;
  }
}

async function toggleAdminsOnlyWrite(): Promise<void> {
  const toggle = optionalById<HTMLInputElement>("groupAdminsOnlyWriteToggle");
  if (!toggle) return;
  const value = toggle.checked;
  await updateGroupPermissions(
    { admins_only_write: value },
    value ? "Только администраторы могут писать" : "Все участники могут писать",
    function() { toggle.checked = !value; },
  );
}

async function toggleHideMembersList(): Promise<void> {
  const toggle = optionalById<HTMLInputElement>("groupHideMembersToggle");
  if (!toggle) return;
  const value = toggle.checked;
  await updateGroupPermissions(
    { hide_members_list: value },
    value ? "Список участников скрыт для обычных участников" : "Список участников снова виден всем участникам",
    function() { toggle.checked = !value; },
  );
}

async function toggleAnonymousMode(): Promise<void> {
  const toggle = optionalById<HTMLInputElement>("groupAnonymousModeToggle");
  if (!toggle) return;
  const value = toggle.checked;
  await updateGroupPermissions(
    { anonymous_mode: value },
    value ? "Анонимный режим включён" : "Анонимный режим выключен",
    function() { toggle.checked = !value; },
  );
}

// Track group role and group settings per chat
let groupInfoCache: GroupInfoCacheMap = {};

function formatMemberCountLabel(count: number | string | null | undefined): string {
  const value = Math.max(0, Number(count || 0));
  const mod10 = value % 10;
  const mod100 = value % 100;
  let word = "участников";
  if (mod10 === 1 && mod100 !== 11) {
    word = "участник";
  } else if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) {
    word = "участника";
  }
  return value + " " + word;
}

function applyGroupChatStatus(chatID: number | string | null | undefined): void {
  const key = Number(chatID || 0);
  if (!key || currentChatType !== "group" || Number(currentChatID || 0) !== key) return;
  const info = groupInfoCache[key];
  if (info && typeof info.member_count !== "undefined") {
    chatStatusEl.textContent = formatMemberCountLabel(info.member_count);
    return;
  }
  chatStatusEl.textContent = "Групповой чат";
}

function syncGroupInfoCache(chatID: number | string | null | undefined, group: Partial<GroupSettingsRecord> | null | undefined): GroupRuntimeInfo | null {
  const key = Number(chatID || (group && group.id) || 0);
  if (!key || !group) return null;
  groupOwnerByChat[key] = Number(group.owner_id || group.ownerID || 0);
  const cached: GroupRuntimeInfo = {
    role: group.role || (group.is_owner ? "owner" : "member"),
    admins_only_write: !!group.admins_only_write,
    hide_members_list: !!group.hide_members_list,
    anonymous_mode: !!group.anonymous_mode,
    member_count: Number(group.member_count || 0),
    name: group.name || "Группа",
  };
  groupInfoCache[key] = cached;
  if (activeGroupSettings && Number(activeGroupSettings.id || currentChatID || 0) === key) {
    activeGroupSettings = { ...activeGroupSettings, ...group };
  }
  return cached;
}

async function fetchGroupInfo(chatID: number | string | null | undefined): Promise<GroupSettingsRecord | null> {
  const key = Number(chatID || 0);
  if (!key) return null;
  const res = await apiFetch("/api/groups/" + key);
  if (!res.ok) return null;
  const group = await res.json() as GroupSettingsRecord;
  syncGroupInfoCache(key, group);
  return group;
}

async function refreshGroupRuntime(chatID: number | string | null | undefined): Promise<GroupSettingsRecord | null> {
  const key = Number(chatID || currentChatID || 0);
  if (!key) return null;
  const group = await fetchGroupInfo(key);
  if (group && currentChatType === "group" && Number(currentChatID || 0) === key) {
    applyGroupChatStatus(key);
    updateMessageInputState();
    if (infoPanelOpen) {
      await loadChatMembers(key);
    }
  }
  return group;
}

function updateMessageInputState(): void {
  const input = optionalById<HTMLTextAreaElement>("messageInput");
  const sendBtn = optionalById<HTMLButtonElement>("sendBtn");
  if (!input || !sendBtn) return;

  const currentGroupID = Number(currentChatID || 0);
  const info = groupInfoCache[currentGroupID];
  if (currentChatType === "group" && info && info.admins_only_write && info.role === "member") {
    input.disabled = true;
    input.placeholder = "Только администраторы могут писать";
    sendBtn.disabled = true;
    queryAll<HTMLElement>(".media-btn, #fileBtn").forEach(function(b) { b.style.display = "none"; });
  } else {
    input.disabled = false;
    input.placeholder = "Сообщение...";
    sendBtn.disabled = false;
    queryAll<HTMLElement>(".media-btn, #fileBtn").forEach(function(b) { b.style.display = ""; });
  }
}

// Wrap switchChat to fetch group info and update input state
(function() {
  const _origSwitchChat = typeof switchChat === "function" ? switchChat : null;
  if (!_origSwitchChat) return;

  window.switchChat = function(chatID: number | string, chatName?: string | null, chatType?: string | null) {
    _origSwitchChat.apply(this, arguments as any);

    if ((chatType === "group" || currentChatType === "group") && chatID) {
      applyGroupChatStatus(chatID);
      fetchGroupInfo(chatID).then(function(group) {
        if (!group) return;
        if (Number(currentChatID || 0) === Number(chatID)) {
          applyGroupChatStatus(chatID);
          updateMessageInputState();
          if (infoPanelOpen) loadChatMembers(chatID);
        }
      }).catch(function(e) {
        console.error("fetch group info error:", e);
      });
    } else {
      // Non-group chat — restore input
      const input = optionalById<HTMLTextAreaElement>("messageInput");
      const sendBtn = optionalById<HTMLButtonElement>("sendBtn");
      if (input) { input.disabled = false; input.placeholder = "Сообщение..."; }
      if (sendBtn) sendBtn.disabled = false;
      queryAll<HTMLElement>(".media-btn, #fileBtn").forEach(function(b) { b.style.display = ""; });
    }
  };
})();

// Handle WS error messages (admins_only_write)
(function() {
  setTimeout(() => {
    const _origHandleIncoming = typeof handleIncomingMessage === "function" ? handleIncomingMessage : null;
    if (!_origHandleIncoming) return;

    handleIncomingMessage = function(data: ChatMessage): void {
      if (data && data.type === "error" && data.message) {
        alert(String(data.message));
        return;
      }
      _origHandleIncoming(data);
    };
    window.handleIncomingMessage = handleIncomingMessage;
  }, 0);
})();

async function loadModalPendingRequests(): Promise<void> {
  try {
    const res = await apiFetch("/api/friends/requests");
    if (res.ok) {
      const payload = await res.json() as { requests?: FriendRequestRecord[] } | FriendRequestRecord[];
      const requests = Array.isArray(payload) ? payload : (payload.requests || []);
      const section = optionalById<HTMLDivElement>("modalPendingSection");
      const container = optionalById<HTMLDivElement>("modalPendingRequests");
      if (!section || !container) return;

      if (!requests || requests.length === 0) {
        section.style.display = "none";
        return;
      }

      section.style.display = "block";
      container.innerHTML = "";

      for (const req of requests) {
        const div = document.createElement("div");
        div.className = "friend-request-item";
        div.innerHTML =
          '<span class="friend-request-name">' +
          escapeHtml(req.from_username) +
          "</span>" +
          '<div class="fr-actions">' +
          '<button class="action-btn success">Принять</button>' +
          '<button class="action-btn danger">Отклонить</button>' +
          "</div>";
        div.querySelector<HTMLButtonElement>("button.success")!.addEventListener("click", async () => {
          await acceptRequest(req.id);
          void loadModalPendingRequests();
        });
        div.querySelector<HTMLButtonElement>("button.danger")!.addEventListener("click", async () => {
          await rejectRequest(req.id);
          void loadModalPendingRequests();
        });
        container.appendChild(div);
      }
    }
  } catch (e) {
    console.error("Failed to load modal pending requests:", e);
  }
}

let searchTimeout: number | undefined;
byId<HTMLInputElement>("friendSearchInput").addEventListener("input", (e) => {
  clearTimeout(searchTimeout);
  const query = (e.currentTarget as HTMLInputElement).value.trim();
  if (query.length < 2) {
    byId<HTMLDivElement>("friendSearchResults").innerHTML = "";
    return;
  }
  searchTimeout = setTimeout(() => searchUsers(query), 300);
});

async function searchUsers(query: string): Promise<void> {
  try {
    const res = await apiFetch(
      "/api/users/search?q=" + encodeURIComponent(query),
    );
    if (!res.ok) return;
    const users = await res.json() as SearchUserResult[];
    const resultsEl = byId<HTMLDivElement>("friendSearchResults");
    resultsEl.innerHTML = "";

    if (users.length === 0) {
      resultsEl.innerHTML =
        '<div class="placeholder-text">Пользователи не найдены</div>';
      return;
    }

    for (const user of users) {
      const div = document.createElement("div");
      div.className = "search-result-item";
      div.innerHTML =
        '<button type="button" class="search-result-name" title="Открыть профиль">' +
        escapeHtml(user.username) +
        "</button>" +
        '<button type="button" class="action-btn primary">Добавить</button>';
      div.querySelector<HTMLButtonElement>(".search-result-name")!.addEventListener("click", () => {
        closeFriendModal();
        void openUserProfileModal(user.id);
      });
      const button = div.querySelector<HTMLButtonElement>(".action-btn")!;
      button.addEventListener("click", () => { void sendFriendRequest(user.id, button); });
      resultsEl.appendChild(div);
    }
  } catch (e) {
    console.error("Failed to search users:", e);
  }
}

async function sendFriendRequest(userID: number | string, btnEl: HTMLButtonElement): Promise<void> {
  try {
    const res = await apiFetch("/api/friends/request", {
      method: "POST",
      body: JSON.stringify({ to_user_id: userID }),
    });
    if (res.ok) {
      btnEl.textContent = "Отправлено";
      btnEl.disabled = true;
      btnEl.classList.remove("primary");
      // Check if auto-accepted (mutual request)
      const data = await res.json() as { status?: string };
      if (data.status === "accepted") {
        btnEl.textContent = "Друзья!";
        await loadChats();
        await loadFriendRequests();
      }
    } else {
      const text = await res.text();
      if (text.includes("already")) {
        btnEl.textContent = "Уже друзья";
      } else {
        btnEl.textContent = "Ошибка";
      }
      btnEl.disabled = true;
    }
  } catch (e) {
    console.error("Failed to send friend request:", e);
    btnEl.textContent = "Ошибка";
    btnEl.disabled = true;
  }
}
