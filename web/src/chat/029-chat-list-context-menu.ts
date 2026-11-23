// ===== CHAT LIST CONTEXT MENU =====
function hideChatListContextMenu(): void {
  const menu = optionalById<HTMLDivElement>("chatListContextMenu");
  if (menu) menu.style.display = "none";
  activeChatListMenuTarget = null;
}

function showChatListContextMenu(x: number, y: number, target: ChatTarget | null | undefined): void {
  const menu = optionalById<HTMLDivElement>("chatListContextMenu");
  if (!menu || !target) return;
  activeChatListMenuTarget = target;
  byId<HTMLDivElement>("contextMenu").style.display = "none";
  byId<HTMLElement>("chatCtxPin").textContent = isChatPinned(target) ? "Открепить чат" : "Закрепить чат";
  byId<HTMLElement>("chatCtxMute").textContent = isChatMuted(target) ? "Включить уведомления" : "Заглушить чат";
  menu.style.left = x + "px";
  menu.style.top = y + "px";
  menu.style.display = "block";
  requestAnimationFrame(() => {
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth) menu.style.left = Math.max(8, x - rect.width) + "px";
    if (rect.bottom > window.innerHeight) menu.style.top = Math.max(8, y - rect.height) + "px";
  });
}

async function openChatContextProfile(): Promise<void> {
  const target = activeChatListMenuTarget;
  hideChatListContextMenu();
  if (!target) return;
  if (target.kind === "group") {
    switchChat(target.id, target.name, "group");
    await openGroupSettingsModal();
    return;
  }
  const profileUserID = await resolvePrivateChatUserID(target);
  if (profileUserID > 0) {
    openUserProfileModal(profileUserID);
    return;
  }
  if (Number(target.id || 0) > 0) {
    switchChat(target.id, target.name, target.chatType || "private");
  }
}

function toggleChatContextPinned(): void {
  const target = activeChatListMenuTarget;
  hideChatListContextMenu();
  if (!target) return;
  setChatPinned(target, !isChatPinned(target));
  renderChatList();
}

function toggleChatContextMuted(): void {
  const target = activeChatListMenuTarget;
  hideChatListContextMenu();
  if (!target) return;
  setChatMuted(target, !isChatMuted(target));
  renderChatList();
}
