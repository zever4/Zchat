// Local read projections only: one chained cycle/page, never one task per chat.
// Server admission/transport/ACKs remain exclusively with the native owner.
const e2eeDialogs = new Map<string, DesktopE2eeConversationReply["conversation"]>();
const e2eePeerNames = new Map<string, string>();
let e2eeDiscoveryReady = false;
let e2eeDiscoveryContext: number | null = null;
let e2eeDiscoveryCursor: string | null = null;
let e2eeSidebarOffset = 0;
let e2eeSidebarSnapshot = "";
let e2eeDiscoveryTimer: ReturnType<typeof setTimeout> | null = null;
let e2eeDiscoveryBusy = false;
let e2eeDiscoveryStopped = true;
let e2eeDiscoveryRevision = 0;
let e2eeDiscoveryFailures = 0;
let e2eeNamesRequest: AbortController | null = null;
function e2eeDiscoveryAvailable(): boolean {
    return !e2eeDiscoveryStopped && !authExpiredHandled
        && localStorage.getItem("token") === token && !!window.zchatDesktop?.e2ee?.compiled;
}
function e2eeDiscoveryVisible(): boolean {
    return document.visibilityState !== "hidden" && e2eeDiscoveryAvailable();
}
function renderE2eeSidebar(): void {
    const list = byId("e2eeDialogsList");
    const entries = [...e2eeDialogs.values()].sort((a, b) => a.conversationId.localeCompare(b.conversationId));
    e2eeSidebarOffset = Math.min(e2eeSidebarOffset, Math.max(0, Math.floor((entries.length - 1) / 8) * 8));
    const visible = entries.slice(e2eeSidebarOffset, e2eeSidebarOffset + 8);
    const snapshot = JSON.stringify([e2eeDiscoveryContext, e2eeDiscoveryReady,
        e2eeHistoryView?.reply.conversation.conversationId || null, e2eeSidebarOffset, entries.length,
        visible.map(dialog => [dialog, e2eePeerNames.get(dialog.peerUserId) || null])]);
    if (snapshot === e2eeSidebarSnapshot) return; // Preserve keyboard focus on unchanged rows.
    e2eeSidebarSnapshot = snapshot;
    list.replaceChildren();
    for (const dialog of visible) {
        const button = document.createElement("button");
        button.type = "button";
        button.disabled = !e2eeDiscoveryReady;
        button.className = "e2ee-dialog";
        button.dataset.conversationId = dialog.conversationId;
        button.setAttribute("aria-pressed", String(e2eeHistoryView?.reply.conversation.conversationId === dialog.conversationId));
        const name = document.createElement("strong");
        name.textContent = e2eePeerNames.get(dialog.peerUserId) || "Защищённый диалог";
        const state = document.createElement("small");
        state.textContent = (dialog.state === "closed" ? "Закрыт · " : "E2EE · ") + dialog.conversationId.slice(0, 8);
        button.append(name, state);
        const context = e2eeDiscoveryContext;
        button.addEventListener("click", () => {
            if (context === null || context !== e2eeDiscoveryContext || !e2eeDiscoveryReady || !e2eeDiscoveryVisible()) return;
            if (!openE2eeConversation({ context, conversation: dialog }, e2eePeerNames.get(dialog.peerUserId) || "Собеседник")) {
                byId("e2eeDialogsNotice").textContent = "Завершите запись голосового сообщения перед открытием диалога.";
            }
        });
        list.append(button);
    }
    byId<HTMLButtonElement>("e2eeDialogsPrevious").disabled = e2eeSidebarOffset === 0;
    byId<HTMLButtonElement>("e2eeDialogsNext").disabled = e2eeSidebarOffset + 8 >= entries.length;
    byId("e2eeDialogsCount").textContent = entries.length ? `${Math.floor(e2eeSidebarOffset / 8) + 1} / ${Math.ceil(entries.length / 8)}` : "";
}
function rememberE2eeDialog(reply: DesktopE2eeConversationReply, render: boolean = true): void {
    if (e2eeDiscoveryContext !== null && reply.context !== e2eeDiscoveryContext) return;
    e2eeDiscoveryContext = reply.context;
    const old = e2eeDialogs.get(reply.conversation.conversationId);
    if (!old && e2eeDialogs.size >= 4096) throw new Error("dialog capacity");
    // A delayed read must not reopen a known closed dialog.
    const dialog = old?.state === "closed" ? old : { ...reply.conversation };
    e2eeDialogs.set(dialog.conversationId, dialog);
    const view = e2eeHistoryView;
    if (view && view.reply.context === reply.context && view.reply.conversation.conversationId === dialog.conversationId) {
        view.reply.conversation = { ...dialog };
        if (dialog.state === "closed") {
            byId("e2eeConversationState").textContent = "Этот диалог закрыт после замены ключей. Старая история доступна на этом устройстве.";
            renderE2eeComposer();
        }
    }
    if (render) renderE2eeSidebar();
}
async function loadE2eePeerNames(): Promise<void> {
    if (e2eeNamesRequest || !e2eeDiscoveryVisible()) return;
    const controller = new AbortController();
    e2eeNamesRequest = controller;
    const timeout = setTimeout(() => controller.abort(), 5000);
    try {
        const response = await apiFetch("/api/friends", { signal: controller.signal });
        if (!response.ok) return;
        const friends: unknown = await response.json();
        if (controller.signal.aborted || e2eeDiscoveryStopped || authExpiredHandled || localStorage.getItem("token") !== token || !Array.isArray(friends)) return;
        e2eePeerNames.clear();
        // Names are display-only authenticated metadata, never identity trust or
        // an access gate for already known conversations. No ordinary ID coercion.
        for (const friend of friends.slice(0, 4096)) {
            if (profilePeerUUID(friend?.user_uuid) && typeof friend.username === "string") {
                e2eePeerNames.set(friend.user_uuid, friend.username.slice(0, 128));
            }
        }
        renderE2eeSidebar();
    } catch { /* Offline/private metadata unavailable: neutral labels still work. */ }
    finally { clearTimeout(timeout); if (e2eeNamesRequest === controller) e2eeNamesRequest = null; }
}
function scheduleE2eeDiscovery(delay: number): void {
    if (!e2eeDiscoveryAvailable() || e2eeDiscoveryBusy || e2eeDiscoveryTimer !== null) return;
    e2eeDiscoveryTimer = setTimeout(() => { e2eeDiscoveryTimer = null; void refreshE2eeDiscovery(); }, delay);
}
async function refreshE2eeDiscovery(): Promise<void> {
    if (!e2eeDiscoveryAvailable() || e2eeDiscoveryBusy) return;
    e2eeDiscoveryBusy = true;
    const revision = e2eeDiscoveryRevision;
    const current = () => e2eeDiscoveryVisible() && revision === e2eeDiscoveryRevision;
    const sessionCurrent = () => e2eeDiscoveryAvailable() && revision === e2eeDiscoveryRevision;
    try {
        const api = window.zchatDesktop!.e2ee!;
        const state = await api.status();
        if (!sessionCurrent()) return;
        if (typeof observeProtectedMessageSound === "function") observeProtectedMessageSound(state);
        if (!current()) return; // Hidden pages need only the local notification counter.
        if (typeof observeE2eeArchiveStatus === "function") observeE2eeArchiveStatus(state);
        if (e2eeDiscoveryContext !== state.context) {
            e2eeDialogs.clear();
            e2eeDiscoveryContext = state.context;
            e2eeDiscoveryCursor = null;
            e2eeSidebarOffset = 0;
            renderE2eeSidebar();
        }
        const ready = state.phase === "active" && state.runtimePhase === "Running";
        e2eeDiscoveryReady = ready;
        renderE2eeSidebar();
        const view = e2eeHistoryView;
        if (view) pauseE2eeView(view.reply.context !== state.context
            ? "Сессия устройства изменилась. Скопируйте нужный текст и откройте диалог заново."
            : ready ? null : "Локальная история временно недоступна. Проверьте состояние E2EE в настройках.");
        if (!ready) {
            byId("e2eeDialogsNotice").textContent = "Для защищённой переписки откройте «Настройки → Безопасность».";
            return;
        }
        const page = await api.listConversations(e2eeDiscoveryCursor);
        if (!current()) return;
        if (page.context !== state.context) throw "stale";
        for (const dialog of page.page.items) rememberE2eeDialog({ context: page.context, conversation: dialog }, false);
        renderE2eeSidebar();
        e2eeDiscoveryCursor = page.page.nextCursor; // Wrap on the next tick, not in a tight loop.
        byId("e2eeDialogsNotice").textContent = e2eeDiscoveryCursor
            ? "Список обновляется из локальной базы…" : e2eeDialogs.size ? "" : "На этом устройстве пока нет защищённых диалогов.";
        if (view && e2eeViewCurrent(view) && !view.suspended) {
            if (typeof refreshE2eeRecovery === "function") await refreshE2eeRecovery(view, current);
            if (!current()) return;
            if (!view.loaded) await loadE2eeHistory(view, current);
            else await refreshE2eeHistory(view, current);
        }
        if (current()) e2eeDiscoveryFailures = 0;
    } catch (error) {
        if (!current()) return;
        e2eeDiscoveryFailures = Math.min(3, e2eeDiscoveryFailures + 1);
        if (error === "stale" || (error as { name?: string })?.name === "AbortError") e2eeDiscoveryCursor = null;
        byId("e2eeDialogsNotice").textContent = "Обновление локальных данных задерживается. Повторим автоматически.";
    } finally {
        e2eeDiscoveryBusy = false;
        scheduleE2eeDiscovery((document.visibilityState === "hidden" ? 5000 : 2000) * 2 ** e2eeDiscoveryFailures);
    }
}
function stopE2eeDiscovery(): void {
    e2eeDiscoveryStopped = true;
    e2eeDiscoveryReady = false;
    e2eeDiscoveryRevision++;
    if (e2eeDiscoveryTimer !== null) clearTimeout(e2eeDiscoveryTimer);
    e2eeDiscoveryTimer = null;
    e2eeNamesRequest?.abort();
    e2eeDialogs.clear();
    e2eePeerNames.clear();
    e2eeDiscoveryContext = null;
    e2eeDiscoveryCursor = null;
    byId("e2eeDialogsSection").hidden = true;
    renderE2eeSidebar();
    byId("e2eeDialogsNotice").textContent = "";
}
function startE2eeDiscovery(): void {
    if (!window.zchatDesktop?.e2ee?.compiled || authExpiredHandled || localStorage.getItem("token") !== token) return;
    e2eeDiscoveryStopped = false;
    byId("e2eeDialogsSection").hidden = false;
    void loadE2eePeerNames();
    scheduleE2eeDiscovery(0);
}
byId("e2eeDialogsPrevious").addEventListener("click", () => { e2eeSidebarOffset = Math.max(0, e2eeSidebarOffset - 8); renderE2eeSidebar(); });
byId("e2eeDialogsNext").addEventListener("click", () => { e2eeSidebarOffset += 8; renderE2eeSidebar(); });
byId("e2eeDialogsRefresh").addEventListener("click", () => {
    if (!e2eeDiscoveryVisible()) return;
    // Don't invalidate an in-flight pass on repeated clicks or create a second lane.
    if (!e2eeDiscoveryBusy) { e2eeDiscoveryCursor = null; if (e2eeDiscoveryTimer !== null) clearTimeout(e2eeDiscoveryTimer); e2eeDiscoveryTimer = null; scheduleE2eeDiscovery(0); }
    void loadE2eePeerNames();
});
document.addEventListener("visibilitychange", () => {
    e2eeDiscoveryRevision++;
    if (e2eeDiscoveryTimer !== null) clearTimeout(e2eeDiscoveryTimer);
    e2eeDiscoveryTimer = null;
    if (document.visibilityState === "hidden") e2eeNamesRequest?.abort();
    scheduleE2eeDiscovery(0);
});
window.addEventListener("pagehide", stopE2eeDiscovery);
window.addEventListener("storage", event => {
    if ((event.key === "token" || event.key === null) && localStorage.getItem("token") !== token) stopE2eeDiscovery();
});

window.addEventListener("pageshow", event => { if (event.persisted) startE2eeDiscovery(); });
