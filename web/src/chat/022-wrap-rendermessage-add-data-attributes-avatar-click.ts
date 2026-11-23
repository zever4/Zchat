// ===== WRAP renderMessage — add data attributes + avatar click =====
const _origRenderMessage = renderMessage;
renderMessage = function(msg: ChatMessage, isPrepend = false): void {
    _origRenderMessage(msg, isPrepend);
    if (msg.type === "system") return;
    const lastEl = isPrepend ? messagesEl.firstElementChild : messagesEl.lastElementChild;
    if (!lastEl || !lastEl.classList.contains("msg-wrapper")) return;
    const wrapper = lastEl as HTMLElement;
    if (msg.id) wrapper.dataset.msgId = String(msg.id);
    const senderID = Number(msg.sender_id || 0);
    if (senderID > 0) wrapper.dataset.senderId = String(senderID);
    // Avatar click → open profile
    const avatar = wrapper.querySelector<HTMLElement>(".msg-avatar");
    if (avatar && senderID > 0) {
        avatar.addEventListener("click", () => openUserProfileModal(senderID));
    }
    // Context menu for own messages
    const isOwn = (msg.sender_name === currentUsername);
    if (isOwn) {
        wrapper.addEventListener("contextmenu", (e: MouseEvent) => {
            e.preventDefault();
            showContextMenu(e.clientX, e.clientY, msg.id, msg.chat_id, wrapper);
        });
    }
};
