// ===== GROUPS =====
let activeGroupSettings: GroupSettingsRecord | null = null;
let activeGroupInvites: GroupInviteRecord[] = [];

function openCreateGroupModal(): void {
  const modal = optionalById<HTMLDivElement>("createGroupModal");
  if (!modal) return;
  byId<HTMLInputElement>("createGroupNameInput").value = "";
  byId<HTMLInputElement>("createGroupPublicToggle").checked = true;
  const feedback = byId<HTMLDivElement>("createGroupFeedback");
  feedback.style.display = "none";
  feedback.textContent = "";
  modal.style.display = "flex";
  byId<HTMLInputElement>("createGroupNameInput").focus();
}

function closeCreateGroupModal(): void {
  const modal = optionalById<HTMLDivElement>("createGroupModal");
  if (modal) modal.style.display = "none";
}

async function submitCreateGroup(): Promise<void> {
  const name = byId<HTMLInputElement>("createGroupNameInput").value.trim();
  const isPublic = !!byId<HTMLInputElement>("createGroupPublicToggle").checked;
  const feedback = byId<HTMLDivElement>("createGroupFeedback");

  feedback.style.display = "none";
  feedback.textContent = "";

  if (name.length < 2) {
    feedback.textContent = "Введите название группы (минимум 2 символа)";
    feedback.style.display = "block";
    return;
  }

  try {
    const res = await apiFetch("/api/groups", {
      method: "POST",
      body: JSON.stringify({ name: name, is_public: isPublic }),
    });
    const data = await res.json().catch(() => ({} as { message?: string; group?: GroupSettingsRecord }));
    if (!res.ok) {
      feedback.textContent = data.message || "Не удалось создать группу";
      feedback.style.display = "block";
      return;
    }

    closeCreateGroupModal();
    await loadChats();

    const created = data.group || {};
    if (created.id) {
      switchChat(created.id, created.name || "Группа", "group");
    }
  } catch (e) {
    console.error("create group error:", e);
    feedback.textContent = "Ошибка сети при создании группы";
    feedback.style.display = "block";
  }
}

function closeGroupSettingsModal(): void {
  const modal = optionalById<HTMLDivElement>("groupSettingsModal");
  if (modal) modal.style.display = "none";
  activeGroupSettings = null;
  activeGroupInvites = [];
}

function switchGroupSettingsTab(tab: string): void {
  queryAll<HTMLElement>(".gs-nav-item[data-group-tab-btn]").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.groupTabBtn === tab);
  });
  queryAll<HTMLElement>(".gs-page[data-group-tab]").forEach((page) => {
    page.classList.toggle("active", page.dataset.groupTab === tab);
  });
}

function showGroupSettingsFeedback(text: string, isError: boolean): void {
  const el = optionalById<HTMLDivElement>("groupSettingsFeedback");
  if (!el) return;
  el.textContent = text || "";
  el.style.display = text ? "block" : "none";
  el.style.color = isError ? "var(--red)" : "var(--green)";
}

function setGroupListEmpty(listID: string, text: string): void {
  const list = optionalById<HTMLElement>(listID);
  if (!list) return;
  list.innerHTML = '<div class="group-list-empty">' + escapeHtml(text) + "</div>";
}

function formatGroupDateTime(isoString: string | null | undefined): string {
  if (!isoString) return "—";
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function parseAuditDetails(raw: unknown): Record<string, any> {
  if (!raw) return {};
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw)) as Record<string, any>;
  } catch {
    return {};
  }
}

function formatGroupAuditAction(action: string, details: Record<string, any>): string {
  switch (action) {
    case "group_created":
      return "создал группу";
    case "settings_updated":
      return "обновил настройки группы";
    case "avatar_updated":
      return "обновил аватар группы";
    case "invite_created":
      return "создал ссылку-приглашение";
    case "invite_activated":
      return "активировал ссылку #" + (details.invite_id || "—");
    case "invite_deactivated":
      return "деактивировал ссылку #" + (details.invite_id || "—");
    case "invite_deleted":
      return "удалил ссылку #" + (details.invite_id || "—");
    case "joined_by_invite":
      return "вступил в группу по ссылке";
    case "join_request_created":
      return "отправил заявку на вступление";
    case "join_request_approved":
      return "одобрил заявку пользователя #" + (details.target_user_id || "—");
    case "join_request_rejected":
      return "отклонил заявку пользователя #" + (details.target_user_id || "—");
    case "promoted_to_admin":
      return "назначил администратором пользователя #" + (details.target_user_id || "—");
    case "demoted_to_member":
      return "снял с администратора пользователя #" + (details.target_user_id || "—");
    case "member_kicked":
      return "исключил пользователя #" + (details.target_user_id || "—");
    case "permissions_updated":
      return "обновил права группы";
    default:
      return "действие: " + action;
  }
}

function setGroupOwnerVisibility(role: string): void {
  const isOwner = role === "owner";
  const isAdmin = role === "admin" || isOwner;
  queryAll<HTMLElement>("[data-group-owner-only='true']").forEach(function(el) {
    el.style.display = isOwner ? "" : "none";
  });
  queryAll<HTMLElement>("[data-group-admin-only='true']").forEach(function(el) {
    el.style.display = isAdmin ? "" : "none";
  });
}

async function copyTextToClipboard(value: unknown): Promise<boolean> {
  const text = String(value || "").trim();
  if (!text) return false;
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    const helper = document.createElement("textarea");
    helper.value = text;
    helper.style.position = "fixed";
    helper.style.opacity = "0";
    document.body.appendChild(helper);
    helper.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(helper);
    return ok;
  }
}

function setGroupAvatarPreview(name: string, avatarURL: string): void {
  const avatarEl = optionalById<HTMLDivElement>("groupSettingsAvatar");
  if (!avatarEl) return;

  if (avatarURL) {
    avatarEl.style.backgroundImage = "url(" + displayAssetURL(avatarURL) + ")";
    avatarEl.style.backgroundSize = "cover";
    avatarEl.style.backgroundPosition = "center";
    avatarEl.textContent = "";
  } else {
    avatarEl.style.backgroundImage = "";
    avatarEl.textContent = (name || "G").charAt(0).toUpperCase();
  }
}

function renderGroupInvites(invites: GroupInviteRecord[]): void {
  const list = optionalById<HTMLDivElement>("groupInvitesList");
  if (!list) return;
  list.innerHTML = "";

  if (!invites || invites.length === 0) {
    setGroupListEmpty("groupInvitesList", "Ссылок пока нет");
    return;
  }

  for (const invite of invites) {
    const row = document.createElement("div");
    row.className = "group-invite-item" + (invite.is_active ? "" : " is-inactive");

    const head = document.createElement("div");
    head.className = "group-invite-head";

    const code = document.createElement("span");
    code.className = "group-invite-code";
    code.textContent = invite.code || "—";

    const chip = document.createElement("span");
    chip.className = "group-chip " + (invite.is_active ? "active" : "inactive");
    chip.textContent = invite.is_active ? "Активна" : "Отключена";

    head.appendChild(code);
    head.appendChild(chip);
    row.appendChild(head);

    const link = document.createElement("div");
    link.className = "group-invite-link";
    link.textContent = invite.invite_link || "";
    row.appendChild(link);

    const meta = document.createElement("div");
    meta.className = "group-invite-meta";
    meta.textContent = "Создана: " + formatGroupDateTime(invite.created_at);
    row.appendChild(meta);

    const actions = document.createElement("div");
    actions.className = "group-invite-actions";

    const copyBtn = document.createElement("button");
    copyBtn.className = "settings-card-btn";
    copyBtn.type = "button";
    copyBtn.textContent = "Копировать";
    copyBtn.onclick = async () => {
      const ok = await copyTextToClipboard(invite.invite_link || "");
      showGroupSettingsFeedback(ok ? "Ссылка скопирована" : "Не удалось скопировать ссылку", !ok);
    };

    const toggleBtn = document.createElement("button");
    toggleBtn.className = "settings-card-btn";
    toggleBtn.type = "button";
    toggleBtn.textContent = invite.is_active ? "Деактивировать" : "Активировать";
    toggleBtn.onclick = () => setGroupInviteActive(invite.id, !invite.is_active);

    const removeBtn = document.createElement("button");
    removeBtn.className = "settings-card-btn danger";
    removeBtn.type = "button";
    removeBtn.textContent = "Удалить";
    removeBtn.onclick = () => deleteGroupInvite(invite.id);

    actions.appendChild(copyBtn);
    actions.appendChild(toggleBtn);
    actions.appendChild(removeBtn);
    row.appendChild(actions);

    list.appendChild(row);
  }
}

async function loadGroupInvites(): Promise<void> {
  if (!canManageGroup()) {
    activeGroupInvites = [];
    setGroupListEmpty("groupInvitesList", "Доступно только администраторам");
    return;
  }

  try {
    const res = await apiFetch("/api/groups/" + currentChatID + "/invites");
    if (!res.ok) {
      setGroupListEmpty("groupInvitesList", "Не удалось загрузить ссылки");
      return;
    }
    const data = await res.json() as { invites?: GroupInviteRecord[] };
    activeGroupInvites = data.invites || [];
    renderGroupInvites(activeGroupInvites);
  } catch (e) {
    console.error("load group invites error:", e);
    setGroupListEmpty("groupInvitesList", "Ошибка сети при загрузке ссылок");
  }
}

async function createGroupInviteLink(): Promise<void> {
  if (!canManageGroup()) return;

  const btn = optionalById<HTMLButtonElement>("groupCreateInviteBtn");
  if (btn) btn.disabled = true;
  try {
    const res = await apiFetch("/api/groups/" + currentChatID + "/invites", {
      method: "POST",
    });
    if (!res.ok) {
      const text = (await res.text()) || "Не удалось создать ссылку";
      showGroupSettingsFeedback(text, true);
      return;
    }

    await Promise.all([loadGroupInvites(), loadGroupAuditLogs()]);
    showGroupSettingsFeedback("Новая ссылка создана", false);
  } catch (e) {
    console.error("create group invite error:", e);
    showGroupSettingsFeedback("Ошибка сети при создании ссылки", true);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function setGroupInviteActive(inviteID: number | string, isActive: boolean): Promise<void> {
  if (!canManageGroup()) return;

  try {
    const res = await apiFetch("/api/groups/" + currentChatID + "/invites/" + inviteID, {
      method: "PUT",
      body: JSON.stringify({ is_active: !!isActive }),
    });
    if (!res.ok) {
      const text = (await res.text()) || "Не удалось обновить состояние ссылки";
      showGroupSettingsFeedback(text, true);
      return;
    }

    await Promise.all([loadGroupInvites(), loadGroupAuditLogs()]);
    showGroupSettingsFeedback(isActive ? "Ссылка активирована" : "Ссылка деактивирована", false);
  } catch (e) {
    console.error("set group invite active error:", e);
    showGroupSettingsFeedback("Ошибка сети при обновлении ссылки", true);
  }
}

async function deleteGroupInvite(inviteID: number | string): Promise<void> {
  if (!canManageGroup()) return;
  if (!confirm("Удалить эту ссылку-приглашение?")) return;

  try {
    const res = await apiFetch("/api/groups/" + currentChatID + "/invites/" + inviteID, {
      method: "DELETE",
    });
    if (!res.ok) {
      const text = (await res.text()) || "Не удалось удалить ссылку";
      showGroupSettingsFeedback(text, true);
      return;
    }

    await Promise.all([loadGroupInvites(), loadGroupAuditLogs()]);
    showGroupSettingsFeedback("Ссылка удалена", false);
  } catch (e) {
    console.error("delete group invite error:", e);
    showGroupSettingsFeedback("Ошибка сети при удалении ссылки", true);
  }
}

function fillGroupRequests(requests: GroupJoinRequestRecord[]): void {
  const list = optionalById<HTMLDivElement>("groupJoinRequestsList");
  if (!list) return;

  list.innerHTML = "";
  if (!requests || requests.length === 0) {
    setGroupListEmpty("groupJoinRequestsList", "Нет заявок на вступление");
    return;
  }

  for (const req of requests) {
    const row = document.createElement("div");
    row.className = "group-request-item";

    const head = document.createElement("div");
    head.className = "group-request-head";

    const name = document.createElement("span");
    name.className = "group-request-user";
    name.textContent = req.username || ("ID " + req.user_id);

    const actions = document.createElement("div");
    actions.className = "group-request-actions";

    const acceptBtn = document.createElement("button");
    acceptBtn.className = "action-btn success";
    acceptBtn.type = "button";
    acceptBtn.textContent = "Принять";
    acceptBtn.onclick = () => approveGroupJoinRequest(req.user_id);

    const rejectBtn = document.createElement("button");
    rejectBtn.className = "action-btn danger";
    rejectBtn.type = "button";
    rejectBtn.textContent = "Отклонить";
    rejectBtn.onclick = () => rejectGroupJoinRequest(req.user_id);

    actions.appendChild(acceptBtn);
    actions.appendChild(rejectBtn);
    head.appendChild(name);
    head.appendChild(actions);
    row.appendChild(head);

    const meta = document.createElement("div");
    meta.className = "group-invite-meta";
    meta.textContent = "Создана: " + formatGroupDateTime(req.created_at);
    row.appendChild(meta);

    list.appendChild(row);
  }
}

async function loadGroupJoinRequests(): Promise<void> {
  if (!canManageGroup()) {
    fillGroupRequests([]);
    return;
  }

  try {
    const res = await apiFetch("/api/groups/" + currentChatID + "/requests");
    if (!res.ok) {
      fillGroupRequests([]);
      return;
    }
    const data = await res.json() as { requests?: GroupJoinRequestRecord[] };
    fillGroupRequests(data.requests || []);
  } catch (e) {
    console.error("load group join requests error:", e);
    fillGroupRequests([]);
  }
}

function renderGroupAuditLogs(logs: GroupAuditLogRecord[]): void {
  const list = optionalById<HTMLDivElement>("groupAuditLogList");
  if (!list) return;
  list.innerHTML = "";

  if (!logs || logs.length === 0) {
    setGroupListEmpty("groupAuditLogList", "В журнале пока нет записей");
    return;
  }

  for (const item of logs) {
    const row = document.createElement("div");
    row.className = "group-audit-item";

    const head = document.createElement("div");
    head.className = "group-audit-head";

    const user = document.createElement("span");
    user.className = "group-audit-user";
    user.textContent = item.actor_username || ("ID " + item.actor_user_id);

    const time = document.createElement("span");
    time.className = "group-audit-time";
    time.textContent = formatGroupDateTime(item.created_at);

    head.appendChild(user);
    head.appendChild(time);
    row.appendChild(head);

    const details = parseAuditDetails(item.details_json);
    const action = document.createElement("div");
    action.className = "group-audit-action";
    action.textContent = formatGroupAuditAction(item.action || "", details);
    row.appendChild(action);

    list.appendChild(row);
  }
}

async function loadGroupAuditLogs(): Promise<void> {
  try {
    const res = await apiFetch("/api/groups/" + currentChatID + "/audit?limit=80");
    if (!res.ok) {
      setGroupListEmpty("groupAuditLogList", "Не удалось загрузить журнал аудита");
      return;
    }
    const data = await res.json() as { logs?: GroupAuditLogRecord[] };
    renderGroupAuditLogs(data.logs || []);
  } catch (e) {
    console.error("load group audit error:", e);
    setGroupListEmpty("groupAuditLogList", "Ошибка сети при загрузке журнала");
  }
}

async function openGroupSettingsModal(): Promise<void> {
  if (!currentChatID || currentChatType !== "group") return;
  const currentGroupID = Number(currentChatID);

  try {
    const res = await apiFetch("/api/groups/" + currentGroupID);
    if (!res.ok) {
      alert("Не удалось открыть настройки группы");
      return;
    }
    const group = await res.json() as GroupSettingsRecord;
    activeGroupSettings = group;
    activeGroupInvites = [];
    groupOwnerByChat[currentGroupID] = Number(group.owner_id || 0);
    syncGroupInfoCache(currentGroupID, group);

    const modal = byId<HTMLDivElement>("groupSettingsModal");
    const nameInput = byId<HTMLInputElement>("groupSettingsNameInput");
    const publicToggle = byId<HTMLInputElement>("groupSettingsPublicToggle");
    const saveBtn = optionalById<HTMLButtonElement>("groupSaveBtn");
    const avatarInput = optionalById<HTMLInputElement>("groupAvatarFileInput");
    const createInviteBtn = optionalById<HTMLButtonElement>("groupCreateInviteBtn");
    const aowToggle = optionalById<HTMLInputElement>("groupAdminsOnlyWriteToggle");
    const hideMembersToggle = optionalById<HTMLInputElement>("groupHideMembersToggle");
    const anonymousModeToggle = optionalById<HTMLInputElement>("groupAnonymousModeToggle");
    const groupRole = group.role || (group.is_owner ? "owner" : "member");
    const canEdit = groupRole === "owner" || groupRole === "admin";

    nameInput.value = group.name || "";
    publicToggle.checked = !!group.is_public;
    setGroupAvatarPreview(group.name || "Группа", group.avatar_url || "");
    showGroupSettingsFeedback("", false);

    nameInput.disabled = !canEdit;
    publicToggle.disabled = !canEdit;
    if (saveBtn) saveBtn.disabled = !canEdit;
    if (avatarInput) avatarInput.disabled = !canEdit;
    if (createInviteBtn) createInviteBtn.disabled = !canEdit;
    if (aowToggle) aowToggle.disabled = !canEdit;
    if (hideMembersToggle) hideMembersToggle.disabled = !canEdit;
    if (anonymousModeToggle) anonymousModeToggle.disabled = !canEdit;
    setGroupOwnerVisibility(groupRole);

    if (aowToggle) aowToggle.checked = !!group.admins_only_write;
    if (hideMembersToggle) hideMembersToggle.checked = !!group.hide_members_list;
    if (anonymousModeToggle) anonymousModeToggle.checked = !!group.anonymous_mode;

    modal.style.display = "flex";
    switchGroupSettingsTab("main");
    const tasks: Promise<unknown>[] = [
      loadGroupJoinRequests(),
      loadGroupInvites(),
      loadGroupAuditLogs(),
    ];
    if (canEdit) tasks.push(loadGroupMembers());
    await Promise.all(tasks);
  } catch (e) {
    console.error("open group settings error:", e);
    alert("Не удалось загрузить настройки группы");
  }
}

function canManageGroup(): boolean {
  if (!activeGroupSettings) return false;
  const role = activeGroupSettings.role || (activeGroupSettings.is_owner ? "owner" : "member");
  return role === "owner" || role === "admin";
}

async function saveGroupSettings(): Promise<void> {
  if (!canManageGroup()) return;

  const currentGroupID = Number(currentChatID || 0);
  const name = byId<HTMLInputElement>("groupSettingsNameInput").value.trim();
  const isPublic = !!byId<HTMLInputElement>("groupSettingsPublicToggle").checked;
  if (name.length < 2) {
    showGroupSettingsFeedback("Название должно быть минимум 2 символа", true);
    return;
  }

  try {
    const res = await apiFetch("/api/groups/" + currentGroupID, {
      method: "PUT",
      body: JSON.stringify({ name: name, is_public: isPublic }),
    });
    if (!res.ok) {
      const text = await res.text();
      showGroupSettingsFeedback(text || "Не удалось сохранить настройки", true);
      return;
    }
    const data = await res.json() as Partial<GroupSettingsRecord>;
    activeGroupSettings = activeGroupSettings
      ? { ...activeGroupSettings, ...data }
      : ({ id: currentGroupID, type: "group", ...data } as GroupSettingsRecord);
    groupOwnerByChat[currentGroupID] = Number(activeGroupSettings.owner_id || 0);
    setGroupAvatarPreview(String(data.name || name), String(activeGroupSettings.avatar_url || ""));
    await loadChats();
    switchChat(currentGroupID, String(data.name || name), "group");
    await refreshGroupRuntime(currentGroupID);
    await loadGroupAuditLogs();
    showGroupSettingsFeedback("Настройки сохранены", false);
  } catch (e) {
    console.error("save group settings error:", e);
    showGroupSettingsFeedback("Ошибка сети при сохранении", true);
  }
}

async function approveGroupJoinRequest(targetUserID: number | string): Promise<void> {
  const currentGroupID = Number(currentChatID || 0);
  try {
    const res = await apiFetch("/api/groups/" + currentGroupID + "/requests/" + targetUserID + "/approve", {
      method: "POST",
    });
    if (!res.ok) {
      showGroupSettingsFeedback("Не удалось принять заявку", true);
      return;
    }
    await loadGroupJoinRequests();
    await refreshGroupRuntime(currentGroupID);
    await loadGroupAuditLogs();
    showGroupSettingsFeedback("Заявка принята", false);
  } catch (e) {
    console.error("approve group request error:", e);
    showGroupSettingsFeedback("Ошибка сети", true);
  }
}

async function rejectGroupJoinRequest(targetUserID: number | string): Promise<void> {
  const currentGroupID = Number(currentChatID || 0);
  try {
    const res = await apiFetch("/api/groups/" + currentGroupID + "/requests/" + targetUserID + "/reject", {
      method: "POST",
    });
    if (!res.ok) {
      showGroupSettingsFeedback("Не удалось отклонить заявку", true);
      return;
    }
    await loadGroupJoinRequests();
    await loadGroupAuditLogs();
    showGroupSettingsFeedback("Заявка отклонена", false);
  } catch (e) {
    console.error("reject group request error:", e);
    showGroupSettingsFeedback("Ошибка сети", true);
  }
}

async function deleteCurrentGroup(): Promise<void> {
  if (!activeGroupSettings || activeGroupSettings.role !== "owner") return;

  const title = (activeGroupSettings.name || "эту группу").trim();
  if (!confirm('Удалить группу "' + title + '"? Это действие необратимо.')) return;

  try {
    const deleteID = Number(currentChatID);
    const res = await apiFetch("/api/groups/" + deleteID, { method: "DELETE" });
    const mediaPending = isCallCleanupPending(res);
    if (!res.ok && !mediaPending) {
      const text = (await res.text()) || "Не удалось удалить группу";
      showGroupSettingsFeedback(text, true);
      return;
    }
    await res.body?.cancel();
    if (Number(callChatID) === deleteID) cleanupCall();

	    delete groupOwnerByChat[deleteID];
	    delete groupInfoCache[deleteID];
	    closeGroupSettingsModal();
    if (infoPanelOpen) closeInfoPanel();

    await loadChats();
    if (chats && chats.length > 0) {
      const fallback = chats[0];
      const fallbackName = fallback.type === "group"
        ? (fallback.name || "Группа")
        : (fallback.other_username || "...");
      switchChat(fallback.id, fallbackName, fallback.type);
    } else {
      currentChatID = null;
      currentChatType = null;
      chatNameEl.textContent = "Выберите чат";
      chatStatusEl.textContent = "";
      messagesEl.innerHTML = "";
    }
    if (mediaPending) renderSystemMessage("Группа удалена. " + pendingCallCleanupNotice());
  } catch (e) {
    console.error("delete group error:", e);
    showGroupSettingsFeedback("Ошибка сети при удалении группы", true);
  }
}

const groupAvatarInput = optionalById<HTMLInputElement>("groupAvatarFileInput");
if (groupAvatarInput) {
  groupAvatarInput.addEventListener("change", async (e) => {
    if (!currentChatID || !canManageGroup()) {
      groupAvatarInput.value = "";
      return;
    }
    const input = e.currentTarget as HTMLInputElement;
    const file = input.files && input.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append("avatar", file);

    try {
      const res = await sessionFetch("/api/groups/" + currentChatID + "/avatar", {
        method: "POST",
      headers: { Authorization: "Bearer " + token },
      body: formData,
    });
      if (!res.ok) {
        showGroupSettingsFeedback("Не удалось загрузить аватар", true);
        return;
      }
      const data = await res.json() as UserProfileResponse;
      activeGroupSettings = activeGroupSettings
        ? { ...activeGroupSettings, avatar_url: data.avatar_url || "" }
        : { id: Number(currentChatID || 0), type: "group", avatar_url: data.avatar_url || "" };
      setGroupAvatarPreview(byId<HTMLInputElement>("groupSettingsNameInput").value, String(data.avatar_url || ""));
      await loadChats();
      await loadGroupAuditLogs();
      showGroupSettingsFeedback("Аватар обновлён", false);
    } catch (err) {
      console.error("upload group avatar error:", err);
      showGroupSettingsFeedback("Ошибка сети при загрузке аватара", true);
    } finally {
      input.value = "";
    }
  });
}
