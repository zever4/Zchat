// ===== HELPERS for edit/delete =====
function removeMessageFromDOM(msgID: number | string | null | undefined): void {
    const el = document.querySelector<HTMLElement>('[data-msg-id="' + msgID + '"]');
    if (el) el.remove();
}

function removeMessageFromCache(msgID: number | string | null | undefined, chatID: number | string | null | undefined): void {
    const numericChatID = Number(chatID || 0);
    if (messageCache[numericChatID]) {
        messageCache[numericChatID] = messageCache[numericChatID].filter(m => String(m.id) !== String(msgID));
    }
    syncChatPreviewFromCache(numericChatID, true);
}

function updateMessageInDOM(msgID: number | string | null | undefined, newContent: string): void {
    const el = document.querySelector<HTMLElement>('[data-msg-id="' + msgID + '"]');
    if (el) {
        const bubble = el.querySelector<HTMLElement>(".msg");
        const textEl = el.querySelector<HTMLElement>(".msg-text");
        if (textEl) {
            const rich = renderMessageContent(newContent);
            textEl.innerHTML = rich.textHTML;

            if (bubble) {
                const oldCard = bubble.querySelector<HTMLElement>(".invite-card");
                if (rich.cardHTML) {
                    if (oldCard) {
                        oldCard.outerHTML = rich.cardHTML;
                    } else {
                        const timeEl = bubble.querySelector<HTMLElement>(".msg-time");
                        if (timeEl) timeEl.insertAdjacentHTML("beforebegin", rich.cardHTML);
                        else bubble.insertAdjacentHTML("beforeend", rich.cardHTML);
                    }
                } else if (oldCard) {
                    oldCard.remove();
                }
                attachInviteCardHandler(bubble);
            }
        }
    }
}

function updateMessageInCache(msgID: number | string | null | undefined, chatID: number | string | null | undefined, newContent: string): void {
    const numericChatID = Number(chatID || 0);
    if (messageCache[numericChatID]) {
        const msg = messageCache[numericChatID].find(m => String(m.id) === String(msgID));
        if (msg) msg.content = newContent;
    }
    syncChatPreviewFromCache(numericChatID, true);
}
