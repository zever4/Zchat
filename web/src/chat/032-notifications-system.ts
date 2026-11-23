// ===== NOTIFICATIONS SYSTEM =====
function loadNotifications(): NotificationRecord[] {
    try {
        const saved: unknown = JSON.parse(localStorage.getItem(NOTIFICATIONS_STORAGE_KEY) || "[]");
        if (Array.isArray(saved)) return saved.filter(n => n && typeof n.id === "number" && typeof n.text === "string" && typeof n.type === "string" && typeof n.time === "string").slice(0, 50);
    } catch { /* Discard corrupt local state instead of breaking the chat. */ }
    return [];
}
let notifications: NotificationRecord[] = loadNotifications();
let notifPanelOpen = false;

function toggleNotifPanel(): void {
    notifPanelOpen = !notifPanelOpen;
    const panel = byId<HTMLDivElement>("notifPanel");
    panel.style.display = notifPanelOpen ? "flex" : "none";
    if (notifPanelOpen) {
        // Mark all as read
        notifications.forEach(n => n.unread = false);
        saveNotifications();
        updateNotifBadge();
    }
}

function closeNotifPanel(): void {
    notifPanelOpen = false;
    byId<HTMLDivElement>("notifPanel").style.display = "none";
}

// Close notif panel on outside click
document.addEventListener("click", (e: MouseEvent) => {
    if (!notifPanelOpen) return;
    const panel = byId<HTMLDivElement>("notifPanel");
    const btn = byId<HTMLButtonElement>("notifBellBtn");
    const target = e.target;
    if (!(target instanceof Node)) return;
    if (!panel.contains(target) && !btn.contains(target)) {
        closeNotifPanel();
    }
});

function addNotification(type: string, text: string, extra?: NotificationExtra | null): void {
    if (localStorage.getItem("token") !== token) return;
    if (!canShowAppNotifications()) return;
    const notif: NotificationRecord = {
        id: Date.now(),
        type: type,
        text: text,
        time: new Date().toISOString(),
        unread: true,
        extra: extra || null
    };
    notifications.unshift(notif);
    if (notifications.length > 50) notifications.pop();
    saveNotifications();
    updateNotifBadge();
    renderNotifications();
    playNotificationSound();
    maybeShowDesktopNotification("Zchat", String(text || "").replace(/<[^>]+>/g, ""));
}

function saveNotifications(): void {
    if (localStorage.getItem("token") === token) localStorage.setItem(NOTIFICATIONS_STORAGE_KEY, JSON.stringify(notifications));
}

function updateNotifBadge(): void {
    const badge = byId<HTMLElement>("notifBadge");
    const count = notifications.filter(n => n.unread).length;
    badge.textContent = String(count);
    badge.style.display = count > 0 ? "flex" : "none";
}

function clearAllNotifications(): void {
    notifications = [];
    saveNotifications();
    updateNotifBadge();
    renderNotifications();
}

function removeNotification(id: number): void {
    notifications = notifications.filter(n => n.id !== id);
    saveNotifications();
    updateNotifBadge();
    renderNotifications();
}

function formatNotifTime(iso: string): string {
    const d = new Date(iso);
    const now = new Date();
    const diff = now.getTime() - d.getTime();
    if (diff < 60000) return "только что";
    if (diff < 3600000) return Math.floor(diff / 60000) + " мин. назад";
    if (diff < 86400000) return Math.floor(diff / 3600000) + " ч. назад";
    return d.toLocaleDateString("ru-RU", { day: "numeric", month: "short" });
}

// Notifications use text and optional bold emphasis, never arbitrary cached HTML.
function appendNotificationText(target: HTMLElement, value: string): void {
    const template = document.createElement("template");
    template.innerHTML = value;
    for (const node of template.content.childNodes) {
        if (node.nodeType === Node.COMMENT_NODE) continue;
        if (node instanceof HTMLElement && node.tagName === "STRONG") {
            const strong = document.createElement("strong");
            strong.textContent = node.textContent || "";
            target.appendChild(strong);
        } else {
            target.appendChild(document.createTextNode(node.textContent || ""));
        }
    }
}

function renderNotifications(): void {
    const list = byId<HTMLDivElement>("notifList");
    if (notifications.length === 0) {
        list.innerHTML = '<div class="notif-empty">Нет уведомлений</div>';
        return;
    }
    list.innerHTML = "";
    for (const n of notifications) {
        const div = document.createElement("div");
        div.className = "notif-item" + (n.unread ? " unread" : "");

        let iconClass = "system";
        let iconChar = "i";
        if (n.type === "friend_request") { iconClass = "friend"; iconChar = "👤"; }
        else if (n.type === "friend_accepted") { iconClass = "friend"; iconChar = "✓"; }
        else if (n.type === "message") { iconClass = "message"; iconChar = "💬"; }
        else if (n.type === "system") { iconClass = "system"; iconChar = "ℹ"; }

        let actionsHTML = "";
        if (n.type === "friend_request" && n.extra && n.extra.requestId) {
            actionsHTML = '<div class="notif-actions">' +
                '<button class="notif-accept-btn">Принять</button>' +
                '<button class="notif-decline-btn">Отклонить</button>' +
                '</div>';
        }

        div.innerHTML =
            '<div class="notif-icon ' + iconClass + '">' + iconChar + '</div>' +
            '<div class="notif-content">' +
                '<div class="notif-text"></div>' +
                actionsHTML +
                '<div class="notif-time">' + formatNotifTime(n.time) + '</div>' +
            '</div>';
        appendNotificationText(div.querySelector<HTMLElement>(".notif-text")!, n.text);
        if (n.type === "friend_request" && n.extra && n.extra.requestId) {
            const requestID = n.extra.requestId;
            div.querySelector<HTMLButtonElement>(".notif-accept-btn")!.addEventListener("click", () => { void notifAcceptFriend(requestID, n.id); });
            div.querySelector<HTMLButtonElement>(".notif-decline-btn")!.addEventListener("click", () => { void notifDeclineFriend(requestID, n.id); });
        }
        list.appendChild(div);
    }
}

async function notifAcceptFriend(requestID: number | string, notifID: number): Promise<void> {
    await acceptRequest(requestID);
    removeNotification(notifID);
}

async function notifDeclineFriend(requestID: number | string, notifID: number): Promise<void> {
    await rejectRequest(requestID);
    removeNotification(notifID);
}

// Hook into friend requests — wrap renderFriendRequests to also create notifications
const _origRenderFriendRequests = renderFriendRequests;
let _prevFriendRequestIDs = new Set<number | string>();
renderFriendRequests = function(requests: FriendRequestRecord[]): void {
    _origRenderFriendRequests(requests);
    if (!requests || requests.length === 0) return;
    for (const req of requests) {
        if (_prevFriendRequestIDs.has(req.id)) continue;
        _prevFriendRequestIDs.add(req.id);
        // Check if we already have a notification for this request
        const exists = notifications.some(n => n.type === "friend_request" && n.extra && n.extra.requestId === req.id);
        if (!exists) {
            addNotification("friend_request",
                "<strong>" + escapeHtml(req.from_username) + "</strong> хочет добавить вас в друзья",
                { requestId: req.id, fromUsername: req.from_username });
        }
    }
};

// Hook into incoming messages — notify about system joins, friend accepted
const _origHandleIncoming2 = handleIncomingMessage;
handleIncomingMessage = function(msg: ChatMessage): void {
    if ((msg.type === "private" || msg.type === "group") && msg.id != null &&
        (Number(msg.chat_id || 0) !== Number(currentChatID || 0) || !document.hasFocus() || document.hidden) &&
        String(msg.sender_id ?? "") !== callUserID &&
        !(messageCache[Number(msg.chat_id)] || []).some(cached => String(cached.id) === String(msg.id)) &&
        !isChatMutedByID(msg.chat_id)) {
        addNotification("message", buildIncomingChatNotificationText(msg), { chatId: Number(msg.chat_id || 0) });
    }
    if (msg.type === "system" && msg.content && msg.content.includes("залетел")) {
        addNotification("system", msg.content);
    }
    if (msg.type === "friend_accepted") {
        addNotification("friend_accepted", "<strong>" + escapeHtml(msg.sender_name || "Пользователь") + "</strong> принял вашу заявку в друзья");
    }
    _origHandleIncoming2(msg);
};

// Init
updateNotifBadge();
renderNotifications();
