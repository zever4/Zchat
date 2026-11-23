// ===== CONTEXT MENU (edit/delete) =====
let contextMessageID: number | string | null = null;
let contextChatID: number | string | null = null;
let contextElement: HTMLElement | null = null;

let showContextMenu: ShowContextMenuFn = function(x, y, msgID, chatID, el) {
    contextMessageID = msgID;
    contextChatID = chatID;
    contextElement = el;
    const menu = byId<HTMLDivElement>("contextMenu");
    menu.style.left = x + "px";
    menu.style.top = y + "px";
    menu.style.display = "block";
    // Keep inside viewport
    requestAnimationFrame(() => {
        const rect = menu.getBoundingClientRect();
        if (rect.right > window.innerWidth) menu.style.left = (x - rect.width) + "px";
        if (rect.bottom > window.innerHeight) menu.style.top = (y - rect.height) + "px";
    });
};

// Close context menu on click anywhere
document.addEventListener("click", () => {
    byId<HTMLDivElement>("contextMenu").style.display = "none";
    hideChatListContextMenu();
});

async function deleteMessage(): Promise<void> {
    if (!contextMessageID) return;
    try {
        const res = await apiFetch("/api/messages/" + contextMessageID, { method: "DELETE" });
        if (res.ok) {
            removeMessageFromDOM(contextMessageID);
            removeMessageFromCache(contextMessageID, contextChatID);
        }
    } catch (e) {
        console.error("Failed to delete message:", e);
    }
}

function startEditMessage(): void {
    if (!contextMessageID || !contextElement) return;
    const textEl = contextElement.querySelector<HTMLElement>(".msg-text");
    if (!textEl) return;
    const oldText = textEl.textContent || "";
    const input = document.createElement("input");
    input.type = "text";
    input.value = oldText;
    input.className = "edit-input";
    textEl.replaceWith(input);
    input.focus();
    input.select();

    let saved = false;
    const save = async (): Promise<void> => {
        if (saved) return;
        saved = true;
        const newContent = input.value.trim();
        if (!newContent || newContent === oldText) {
            restoreText(input, oldText);
            return;
        }
        try {
            const res = await apiFetch("/api/messages/" + contextMessageID, {
                method: "PUT",
                body: JSON.stringify({ content: newContent })
            });
            if (res.ok) {
                restoreText(input, newContent);
                updateMessageInCache(contextMessageID, contextChatID, newContent);
            } else {
                restoreText(input, oldText);
            }
        } catch (e) {
            console.error("Failed to edit message:", e);
            restoreText(input, oldText);
        }
    };

    input.addEventListener("keydown", (e: KeyboardEvent) => {
        if (e.key === "Enter") { e.preventDefault(); save(); }
        if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); saved = true; restoreText(input, oldText); }
    });
    input.addEventListener("blur", () => { if (!saved) save(); });
}

function restoreText(input: HTMLInputElement, text: string): void {
    const span = document.createElement("div");
    span.className = "msg-text";
    const rich = renderMessageContent(text);
    span.innerHTML = rich.textHTML;
    const bubble = input.closest<HTMLElement>(".msg");
    if (input.parentNode) input.replaceWith(span);
    if (!bubble) return;

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
