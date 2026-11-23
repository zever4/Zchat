// ===== CHAT LIST HELPERS =====
function chatPinIconSVG() {
  return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<path d="M12 16v5"></path>' +
    '<path d="M8 3v5l-3 4v2h14v-2l-3-4V3z"></path>' +
    "</svg>";
}

function chatMuteIconSVG() {
  return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<path d="M11 5 6 9H3v6h3l5 4z"></path>' +
    '<path d="m16 9 5 5"></path>' +
    '<path d="m21 9-5 5"></path>' +
    "</svg>";
}

function createChatStateFlag(className: string, title: string, iconMarkup: string): HTMLSpanElement {
  const flag = document.createElement("span");
  flag.className = "chat-item-flag " + className;
  flag.title = title;
  flag.innerHTML = iconMarkup;
  return flag;
}

function appendChatStateFlags(container: HTMLElement, target: ChatTarget): void {
  const flags = document.createElement("div");
  flags.className = "chat-item-flags";

  if (isChatPinned(target)) {
    flags.appendChild(createChatStateFlag("pinned", "Закреплён", chatPinIconSVG()));
  }
  if (isChatMuted(target)) {
    flags.appendChild(createChatStateFlag("muted", "Заглушён", chatMuteIconSVG()));
  }

  if (!flags.childElementCount) return;
  container.appendChild(flags);
}

function attachChatListContextMenu(node: HTMLElement | null, target: ChatTarget): void {
  if (!node) return;
  node.addEventListener("contextmenu", (e) => {
    e.preventDefault();
    e.stopPropagation();
    showChatListContextMenu(e.clientX, e.clientY, target);
  });
}

function getSortedSidebarChats(): ChatRecord[] {
  return chats
    .map((chat, index) => ({ chat, index, target: buildChatListTarget(chat) }))
    .sort((a, b) => {
      const pinnedDelta = Number(isChatPinned(b.target)) - Number(isChatPinned(a.target));
      if (pinnedDelta !== 0) return pinnedDelta;
      return a.index - b.index;
    })
    .map((entry) => entry.chat);
}

