// ===== RENDER CHAT LIST =====
function renderChatList(): void {
  privateChatsList.innerHTML = "";

  const otherChats = getSortedSidebarChats();

  for (const chat of otherChats) {
    const isGroup = chat.type === "group";
    const chatName = isGroup ? (chat.name || "Группа") : (chat.other_username || "...");
    const target = buildChatListTarget(chat);
    const previewText = getChatPreviewText(chat.id);
    const div = document.createElement("div");
    div.className =
      "private-chat-item" +
      (Number(chat.id) === Number(currentChatID) ? " active" : "") +
      (isChatMuted(target) ? " is-muted" : "");
    div.dataset.chatId = String(chat.id);
    div.onclick = () => switchChat(chat.id, chatName, chat.type);
    attachChatListContextMenu(div, target);

    const avatar = document.createElement("div");
    avatar.className = "contact-avatar";
    const avatarURL = isGroup ? (chat.other_avatar_url || chat.avatar_url || "") : (chat.other_avatar_url || "");
    if (avatarURL) {
      avatar.style.backgroundImage = "url(" + displayAssetURL(avatarURL) + ")";
      avatar.style.backgroundSize = "cover";
      avatar.style.backgroundPosition = "center";
      avatar.textContent = "";
    } else {
      avatar.textContent = chatName.charAt(0).toUpperCase();
    }

    const contactMain = document.createElement("div");
    contactMain.className = "contact-main";

    const titleRow = document.createElement("div");
    titleRow.className = "contact-title-row";

    const nameWrap = document.createElement("div");
    nameWrap.className = "contact-name-wrap";

    const name = document.createElement("span");
    name.className = "contact-name";
    name.textContent = chatName;
    nameWrap.appendChild(name);

    const preview = document.createElement("div");
    preview.className = "contact-preview";
    preview.textContent = previewText;

    titleRow.appendChild(nameWrap);
    appendChatStateFlags(titleRow, target);
    contactMain.appendChild(titleRow);
    contactMain.appendChild(preview);

    div.appendChild(avatar);
    div.appendChild(contactMain);
    privateChatsList.appendChild(div);
  }

}
