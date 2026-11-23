// Separate local read-only modal. Never binds the active composer, history,
// recovery consent or ordinary chat queues. No own polling or persistence.
type E2eeArchiveStage = "generations" | "conversations" | "segments" | "history";
interface E2eeArchiveView {
    context: number | null;
    stage: E2eeArchiveStage;
    generation: Readonly<DesktopArchiveGeneration> | null;
    conversation: Readonly<DesktopE2eeConversationReply["conversation"]> | null;
    epoch: string | null;
    cursor: string | null;
    seen: Set<string>;
    revision: number;
    retired: boolean;
    cursorBlocked: boolean;
    notice: string;
    count: number;
}
let e2eeArchiveView: E2eeArchiveView | null = null;
let e2eeArchiveBusy = false;
let e2eeArchiveObservedContext: number | null = null;
function e2eeArchiveAuthenticated(): boolean {
    return !!token && !authExpiredHandled && localStorage.getItem("token") === token;
}
function e2eeArchiveAvailable(): boolean {
    const api = window.zchatDesktop?.e2ee;
    return !!api?.compiled && typeof api.listArchives === "function" && typeof api.archiveConversations === "function"
        && typeof api.archiveSegments === "function" && typeof api.archiveHistory === "function";
}
function e2eeArchiveReadable(status: DesktopE2eeStatus): boolean {
    return status.phase === "active" && status.runtimePhase === "Running"
        || ["device_unavailable", "replacement_pending", "replacement_required"].includes(status.phase) && status.runtimePhase === "ArchiveReadOnly";
}
function e2eeArchiveCurrent(view: E2eeArchiveView): boolean {
    return e2eeArchiveView === view && byId<HTMLDialogElement>("e2eeArchiveDialog").open && !view.retired && e2eeArchiveAuthenticated();
}
function syncE2eeArchiveButtons(): void {
    for (const id of ["e2eeArchiveOpen", "desktopE2eeArchiveOpen"]) {
        const button = document.getElementById(id) as HTMLButtonElement | null;
        if (button) { button.hidden = !e2eeArchiveAvailable() || !e2eeArchiveAuthenticated(); button.disabled = e2eeArchiveBusy; }
    }
}
function renderE2eeArchiveControls(): void {
    syncE2eeArchiveButtons();
    const v = e2eeArchiveView;
    if (!v) return;
    const focused = document.activeElement;
    const blocked = e2eeArchiveBusy || v.retired || v.context === null;
    byId<HTMLButtonElement>("e2eeArchiveGenerations").disabled = blocked || v.stage === "generations";
    byId<HTMLButtonElement>("e2eeArchiveConversations").disabled = blocked || !v.generation || v.stage === "conversations";
    byId<HTMLButtonElement>("e2eeArchiveSegments").disabled = blocked || !v.conversation || v.stage === "segments";
    byId<HTMLButtonElement>("e2eeArchiveAll").disabled = blocked || !v.conversation || !v.generation?.historyReady || v.stage === "history" && v.epoch === null;
    byId<HTMLButtonElement>("e2eeArchiveRefresh").disabled = blocked;
    const more = byId<HTMLButtonElement>("e2eeArchiveMore");
    more.hidden = !v.cursor;
    more.disabled = blocked || v.cursorBlocked;
    more.textContent = v.stage === "history" ? "Более ранние сообщения" : "Следующая страница";
    byId("e2eeArchiveContent").querySelectorAll<HTMLButtonElement>("button").forEach(button => { button.disabled = blocked || button.dataset.archiveUnavailable === "true"; });
    byId("e2eeArchiveContent").setAttribute("aria-busy", String(e2eeArchiveBusy));
    byId("e2eeArchiveNotice").textContent = v.notice;
    byId("e2eeArchiveCount").textContent = v.count ? v.stage === "history"
        ? `Сообщений в окне: ${v.count}${v.count >= 128 ? " (предел 128)" : ""}` : `Записей на странице: ${v.count}` : "";
    if (focused?.tagName === "BUTTON" && (focused as HTMLButtonElement).disabled && byId("e2eeArchiveDialog").contains(focused)) {
        byId("e2eeArchiveHeading").focus({ preventScroll: true });
    }
}
function clearE2eeArchiveContent(view: E2eeArchiveView): void {
    if (typeof closeE2eeLocalFile === "function") closeE2eeLocalFile(view);
    const content = byId("e2eeArchiveContent");
    const focused = content.contains(document.activeElement);
    content.replaceChildren();
    content.scrollTop = 0;
    // A selected list button is removed on navigation. Keep keyboard focus in
    // the dialog rather than letting it fall through to the underlying chat.
    if (focused) byId("e2eeArchiveHeading").focus({ preventScroll: true });
    view.seen.clear(); view.count = 0; view.cursor = null; view.cursorBlocked = false;
}
function closeE2eeArchive(): void {
    const view = e2eeArchiveView;
    e2eeArchiveView = null;
    if (view) { view.revision++; clearE2eeArchiveContent(view); view.generation = null; view.conversation = null; }
    byId("e2eeArchiveScope").textContent = byId("e2eeArchiveNotice").textContent = byId("e2eeArchiveCount").textContent = "";
    const dialog = byId<HTMLDialogElement>("e2eeArchiveDialog");
    if (dialog.open) dialog.close();
    syncE2eeArchiveButtons();
}
function retireE2eeArchive(view: E2eeArchiveView, message: string): void {
    if (e2eeArchiveView !== view) return;
    view.revision++; view.retired = true;
    clearE2eeArchiveContent(view); view.generation = null; view.conversation = null;
    byId("e2eeArchiveScope").textContent = "";
    view.notice = message;
    renderE2eeArchiveControls();
}
// Observations reuse existing discovery/settings cycles. They NEVER adopt a
// new context into an open archive or restart its read automatically.
function observeE2eeArchiveStatus(status: DesktopE2eeStatus): void {
    if (!Number.isSafeInteger(status.context) || status.context < 0) return;
    if (e2eeArchiveObservedContext !== null && status.context < e2eeArchiveObservedContext) return;
    e2eeArchiveObservedContext = status.context;
    const view = e2eeArchiveView;
    if (!view) return;
    if (!e2eeArchiveAuthenticated()) { closeE2eeArchive(); return; }
    if (view.context === null || view.retired) return;
    if (view.context !== status.context) retireE2eeArchive(view, "Сессия устройства изменилась. Закройте архив и откройте его заново.");
    else if (!e2eeArchiveReadable(status)) retireE2eeArchive(view, "Локальный архив сейчас недоступен. Проверьте состояние E2EE в настройках и откройте архив заново.");
}
async function openE2eeArchive(): Promise<void> {
    if (!e2eeArchiveAvailable() || !e2eeArchiveAuthenticated() || e2eeArchiveBusy || e2eeArchiveView || document.visibilityState === "hidden") return;
    const dialog = byId<HTMLDialogElement>("e2eeArchiveDialog");
    const view: E2eeArchiveView = { context: null, stage: "generations", generation: null, conversation: null, epoch: null,
        cursor: null, seen: new Set(), revision: 0, retired: false, cursorBlocked: false, notice: "Проверяем доступ к локальному архиву…", count: 0 };
    e2eeArchiveView = view;
    clearE2eeArchiveContent(view);
    byId("e2eeArchiveHeading").textContent = "Выберите установку";
    try { dialog.showModal(); } catch { e2eeArchiveView = null; return; }
    e2eeArchiveBusy = true; renderE2eeArchiveControls();
    let load = false;
    try {
        const status = await window.zchatDesktop!.e2ee!.status();
        if (!e2eeArchiveCurrent(view)) return;
        if (e2eeArchiveObservedContext !== null && status.context < e2eeArchiveObservedContext) throw "stale";
        view.context = status.context;
        observeE2eeArchiveStatus(status);
        if (!view.retired) load = true;
    } catch {
        if (e2eeArchiveCurrent(view)) retireE2eeArchive(view, "Не удалось открыть локальный архив. Проверьте состояние E2EE в настройках и откройте архив заново.");
    } finally {
        e2eeArchiveBusy = false;
        if (!e2eeArchiveAuthenticated()) closeE2eeArchive();
        renderE2eeArchiveControls();
    }
    if (load && e2eeArchiveCurrent(view)) await loadE2eeArchivePage(view, true);
}
function e2eeArchivePeer(view: E2eeArchiveView): string {
    const id = view.conversation?.peerUserId;
    const name = id && typeof e2eePeerNames !== "undefined" && typeof e2eeDiscoveryContext !== "undefined" && e2eeDiscoveryContext === view.context
        ? e2eePeerNames.get(id) : null;
    return name || (id ? `Собеседник · ${id.slice(0, 8)}` : "Собеседник");
}
function e2eeArchiveGenerationLabel(g: Readonly<DesktopArchiveGeneration>): string {
    return `${g.replacedByGenerationId === null ? "Эта установка" : "Сохранённая установка"} · ${g.generationId.slice(0, 8)}`;
}
function renderE2eeArchiveScope(view: E2eeArchiveView): void {
    byId("e2eeArchiveHeading").textContent = { generations: "Выберите установку", conversations: "Выберите диалог", segments: "Выберите сессию шифрования", history: "Сохранённая история" }[view.stage];
    byId("e2eeArchiveScope").textContent = [view.generation && e2eeArchiveGenerationLabel(view.generation), view.conversation && e2eeArchivePeer(view),
        view.conversation && (view.epoch === null ? "Все сессии" : `Сессия №${view.epoch}`)].filter(Boolean).join(" → ");
}
function chooseE2eeArchive(view: E2eeArchiveView, stage: E2eeArchiveStage): void {
    if (!e2eeArchiveCurrent(view) || e2eeArchiveBusy || view.context === null) return;
    if (stage !== "generations" && !view.generation || ["segments", "history"].includes(stage) && !view.conversation) return;
    if (stage === "history" && !view.generation!.historyReady) return;
    if (stage === "generations") { view.generation = null; view.conversation = null; }
    if (stage === "conversations") view.conversation = null;
    view.epoch = null; view.stage = stage;
    void loadE2eeArchivePage(view, true);
}
function e2eeArchiveOption(title: string, note: string, select: () => void): HTMLButtonElement {
    const button = document.createElement("button"); button.type = "button"; button.className = "e2ee-archive-option";
    const label = document.createElement("strong"); label.textContent = title;
    const hint = document.createElement("small"); hint.textContent = note;
    button.append(label, hint); button.addEventListener("click", select);
    return button;
}
function e2eeArchiveDate(value: string): string {
    const ms = BigInt(value);
    return ms <= 8640000000000000n ? new Date(Number(ms)).toLocaleString("ru-RU") : "Время недоступно";
}
function e2eeArchiveMessage(view: E2eeArchiveView, item: Readonly<DesktopHistoryItem>): HTMLElement {
    const row = document.createElement("article"); row.className = "e2ee-message" + (item.outgoing ? " e2ee-message-own" : ""); row.dataset.messageId = item.messageId;
    const author = document.createElement("strong"); author.textContent = item.outgoing ? "Вы" : e2eeArchivePeer(view);
    const text = document.createElement("p"); text.textContent = item.attachment ? e2eeAttachmentLabel(item.attachment) : item.text;
    const detail = document.createElement("small");
    const delivery = item.delivery;
    const states: Record<string, string> = { recipient_stored: "Сохранено получателем", recipient_rejected: "Получатель не принял сообщение", expired: "Срок доставки истёк", recipient_replaced: "Установка получателя заменена" };
    const saved = delivery && (delivery.sealedUnsent ? "Не отправлено" : states[delivery.state || ""] || (delivery.storedOnServer ? "Сохранено на сервере" : "Сервер не подтвердил отправку"));
    const file = item.attachment && (e2eeCacheLabel(item.cacheState) || (!item.outgoing && item.cacheState === "missing" ? "Файл отсутствует на этом устройстве" : ""));
    detail.textContent = `${e2eeArchiveDate(item.createdAtUnixMs)} · Сессия №${item.cryptoEpoch}${saved ? ` · Сохранённый статус: ${saved}` : ""}${file ? ` · ${file}` : ""}`;
    row.append(author, text, detail);
    if (item.attachment && item.cacheState === "saved" && typeof e2eeLocalFileActions === "function") {
        const generationId = view.generation!.generationId;
        const conversationId = item.conversationId;
        const actions = e2eeLocalFileActions(view, item.attachment, { generationId, conversationId, messageId: item.messageId }, view.context!,
            () => e2eeArchiveCurrent(view) && !e2eeArchiveBusy && view.generation?.generationId === generationId && view.conversation?.conversationId === conversationId);
        if (actions) row.append(actions);
    }
    return row;
}
async function loadE2eeArchivePage(view: E2eeArchiveView, reset: boolean): Promise<void> {
    if (!e2eeArchiveCurrent(view) || e2eeArchiveBusy || view.context === null || document.visibilityState === "hidden") return;
    const revision = ++view.revision, context = view.context, stage = view.stage;
    const generation = view.generation, conversation = view.conversation, epoch = view.epoch;
    const cursor = reset ? null : view.cursor;
    if (!reset && (!cursor || view.cursorBlocked)) return;
    if (reset) clearE2eeArchiveContent(view);
    view.notice = "Читаем локальный архив…";
    renderE2eeArchiveScope(view);
    e2eeArchiveBusy = true; renderE2eeArchiveControls();
    const current = () => e2eeArchiveCurrent(view) && view.revision === revision && document.visibilityState !== "hidden";
    try {
        const api = window.zchatDesktop!.e2ee!;
        const reply = await (stage === "generations" ? api.listArchives(context, cursor) : stage === "conversations"
            ? api.archiveConversations({ generationId: generation!.generationId }, context, cursor) : stage === "segments"
                ? api.archiveSegments({ generationId: generation!.generationId, conversationId: conversation!.conversationId }, context, cursor)
                : api.archiveHistory({ generationId: generation!.generationId, conversationId: conversation!.conversationId, cryptoEpoch: epoch }, context, cursor));
        if (!current()) return;
        if (reply.context !== context || reply.page.kind !== { generations: "archives", conversations: "archive_dialogs", segments: "segments", history: "archive_history" }[stage]) throw "stale";
        const content = byId("e2eeArchiveContent"), previousHeight = content.scrollHeight, previousTop = content.scrollTop;
        if (stage !== "history") content.replaceChildren();
        const page = reply.page;
        for (const item of page.items) {
            if (page.kind === "archives") {
                const g = Object.freeze({ ...item as DesktopArchiveGeneration });
                const button = e2eeArchiveOption(e2eeArchiveGenerationLabel(g), g.historyReady ? "История на этом ПК" : "Индекс истории не подготовлен", () => {
                    if (!e2eeArchiveCurrent(view) || e2eeArchiveBusy || view.stage !== stage || view.revision !== revision) return;
                    view.generation = g; chooseE2eeArchive(view, "conversations");
                }); button.title = `Идентификатор установки: ${g.generationId}`; content.append(button);
            } else if (page.kind === "archive_dialogs") {
                const c = Object.freeze({ ...item as DesktopE2eeConversationReply["conversation"] });
                const name = typeof e2eePeerNames !== "undefined" && typeof e2eeDiscoveryContext !== "undefined" && e2eeDiscoveryContext === context ? e2eePeerNames.get(c.peerUserId) : null;
                const button = e2eeArchiveOption(name || `Собеседник · ${c.peerUserId.slice(0, 8)}`, c.state === "closed" ? "Снимок закрытого диалога" : "Сохранённый диалог", () => {
                    if (!e2eeArchiveCurrent(view) || e2eeArchiveBusy || view.stage !== stage || view.revision !== revision || !view.generation?.historyReady) return;
                    view.conversation = c; chooseE2eeArchive(view, "history");
                });
                button.dataset.archiveUnavailable = String(!view.generation?.historyReady);
                content.append(button);
            } else if (page.kind === "segments") {
                const s = item as DesktopArchiveSegment;
                content.append(e2eeArchiveOption(`Сессия №${s.cryptoEpoch}`, `${e2eeArchiveDate(s.createdAtUnixMs)} · ${s.sealed ? "Завершена" : "Последняя сохранённая сессия"}`, () => {
                    if (!e2eeArchiveCurrent(view) || e2eeArchiveBusy || view.stage !== stage || view.revision !== revision || !view.generation?.historyReady) return;
                    view.stage = "history"; view.epoch = s.cryptoEpoch; void loadE2eeArchivePage(view, true);
                }));
            } else {
                const message = item as DesktopHistoryItem;
                if (view.seen.has(message.messageId)) continue;
                view.seen.add(message.messageId); content.prepend(e2eeArchiveMessage(view, message));
            }
        }
        const addedHeight = content.scrollHeight - previousHeight;
        while (stage === "history" && content.children.length > 128) {
            const last = content.lastElementChild as HTMLElement; view.seen.delete(last.dataset.messageId!); content.removeChild(last);
        }
        view.cursor = page.nextCursor;
        view.count = stage === "history" ? view.seen.size : page.items.length;
        if (stage === "history") content.scrollTop = reset ? content.scrollHeight : previousTop + addedHeight;
        else content.scrollTop = 0;
        view.notice = !view.count ? { generations: "Сохранённых E2EE-установок на этом ПК нет.", conversations: "На этой установке нет сохранённых диалогов.", segments: "Сессии шифрования ещё не созданы.", history: "Нет сохранённых текстовых сообщений." }[stage]
            : stage === "segments" ? "После восстановления переписки создаётся новая сессия шифрования. Прежняя история сохраняется." : "";
        if (view.generation && !view.generation.historyReady) view.notice = "Индекс истории этой установки не подготовлен. Читать сообщения сейчас нельзя; архив не будет изменён автоматически.";
    } catch (error) {
        if (!current()) return;
        if (error === "stale" || (error as { name?: string })?.name === "AbortError") {
            try { const state = await window.zchatDesktop!.e2ee!.status(); if (current()) observeE2eeArchiveStatus(state); }
            catch { if (current()) retireE2eeArchive(view, "Сессия недоступна. Закройте архив и откройте его заново."); }
        }
        if (!current()) return;
        const expired = error === "stale" || (error as { name?: string })?.name === "InvalidStateError";
        view.cursorBlocked = !!cursor;
        view.notice = expired ? "Страница архива устарела. Нажмите «Обновить», чтобы начать список заново." : "Не удалось прочитать локальный архив. Повторите чтение кнопкой «Обновить».";
    } finally {
        e2eeArchiveBusy = false;
        if (!e2eeArchiveAuthenticated()) closeE2eeArchive();
        renderE2eeArchiveControls();
    }
}
for (const id of ["e2eeArchiveOpen", "desktopE2eeArchiveOpen"]) document.getElementById(id)?.addEventListener("click", () => { void openE2eeArchive(); });
byId("e2eeArchiveClose").addEventListener("click", closeE2eeArchive);
byId<HTMLDialogElement>("e2eeArchiveDialog").addEventListener("cancel", event => { event.preventDefault(); closeE2eeArchive(); });
byId<HTMLDialogElement>("e2eeArchiveDialog").addEventListener("close", () => { if (!byId<HTMLDialogElement>("e2eeArchiveDialog").open) closeE2eeArchive(); });
for (const [id, stage] of [["e2eeArchiveGenerations", "generations"], ["e2eeArchiveConversations", "conversations"], ["e2eeArchiveSegments", "segments"], ["e2eeArchiveAll", "history"]] as const) {
    byId(id).addEventListener("click", () => { if (e2eeArchiveView) chooseE2eeArchive(e2eeArchiveView, stage); });
}
byId("e2eeArchiveRefresh").addEventListener("click", () => { if (e2eeArchiveView) void loadE2eeArchivePage(e2eeArchiveView, true); });
byId("e2eeArchiveMore").addEventListener("click", () => { if (e2eeArchiveView) void loadE2eeArchivePage(e2eeArchiveView, false); });
document.addEventListener("keydown", (event: KeyboardEvent) => {
    if (event.key === "Escape" && byId<HTMLDialogElement>("e2eeArchiveDialog").open) {
        if (byId<HTMLDialogElement>("e2eeAttachmentPreview").open) return;
        event.preventDefault(); event.stopImmediatePropagation(); closeE2eeArchive();
    }
}, true); // Preserve the underlying settings/inline edit on Escape.
document.addEventListener("visibilitychange", () => {
    const view = e2eeArchiveView;
    if (view && document.visibilityState === "hidden" && e2eeArchiveBusy) {
        view.revision++; view.notice = "Чтение приостановлено. После возвращения нажмите «Обновить»."; renderE2eeArchiveControls();
    }
});
window.addEventListener("pagehide", closeE2eeArchive);
window.addEventListener("storage", event => { if ((event.key === "token" || event.key === null) && !e2eeArchiveAuthenticated()) closeE2eeArchive(); });
syncE2eeArchiveButtons();
