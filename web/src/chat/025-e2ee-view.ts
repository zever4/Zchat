// Separate renderer state/DOM. UUID chats never enter ordinary numeric history,
// composer, attachments, calls, WebSocket or localStorage queues.
interface E2eeHistoryView {
    recovery?: E2eeRecoveryPanel;
    reply: DesktopE2eeConversationReply;
    name: string;
    cursor: string | null;
    seen: Set<string>;
    busy: boolean;
    suspended: string | null;
    items: Map<string, DesktopHistoryItem>;
    browsingOlder: boolean;
    loaded: boolean;
    statusOffset: number;
    auditTicks: number;
    catchup: { cursor: string | null; items: DesktopHistoryItem[]; replacing: boolean } | null;
    pendingAttachments: Map<string, DesktopPendingAttachment>;
    pendingCursor: string | null;
    pendingBusy: boolean;
    pendingReload: boolean;
    download: { messageId: string; controller: AbortController } | null;
    downloadError: { messageId: string; text: string } | null;
}
let e2eeHistoryView: E2eeHistoryView | null = null;
function e2eeVoiceRecording(): boolean {
    return document.getElementById("voiceBtn")?.classList.contains("recording") === true;
}
function e2eeViewCurrent(view: E2eeHistoryView): boolean {
    return e2eeHistoryView === view && !authExpiredHandled && localStorage.getItem("token") === token;
}
function leaveE2eeView(): void {
    if (typeof clearE2eeRecovery === "function") clearE2eeRecovery();
    e2eeHistoryView?.download?.controller.abort();
    if (e2eeHistoryView) closeE2eeLocalFile(e2eeHistoryView);
    e2eeHistoryView = null;
    clearE2eeComposer();
    if (typeof leaveE2eePhoto === "function") leaveE2eePhoto();
    if (typeof leaveE2eeFile === "function") leaveE2eeFile();
    if (typeof leaveE2eeVoice === "function") leaveE2eeVoice();
    if (typeof renderE2eeSidebar === "function") renderE2eeSidebar();
    byId("e2eeConversationView").hidden = true;
    delete byId("chatArea").dataset.chatMode;
    byId("e2eeHistory").replaceChildren();
    byId("e2eeConversationTitle").textContent = "";
    byId("e2eeConversationState").textContent = "";
    byId("e2eeHistoryNotice").textContent = "";
    resetE2eePendingAttachments();
    // Unhiding the ordinary pane during asynchronous auth retirement must not
    // reveal the predecessor account's messages, header or unsubmitted input.
    if (authExpiredHandled || localStorage.getItem("token") !== token) {
        if (typeof stopE2eeDiscovery === "function") stopE2eeDiscovery();
        messagesEl.replaceChildren();
        chatNameEl.textContent = chatStatusEl.textContent = "";
        messageInput.value = "";
        currentChatID = null;
        currentChatType = null;
    }
}
function e2eeDeliveryLabel(value: DesktopDelivery | null): string {
    if (!value) return "";
    if (value.sealedUnsent) return "Не отправлено";
    const states: Record<string, string> = {
        recipient_stored: "Сохранено получателем", recipient_rejected: "Не принято получателем",
        recipient_replaced: "Устройство собеседника заменено", expired: "Срок доставки истёк",
    };
    return states[value.state || ""] || (value.storedOnServer ? "На сервере" : "Ожидает отправки");
}
function openE2eeConversation(reply: DesktopE2eeConversationReply, name: string): boolean {
    if (!window.zchatDesktop?.e2ee?.compiled || authExpiredHandled || localStorage.getItem("token") !== token || e2eeVoiceRecording()) return false;
    reply = { context: reply.context, conversation: { ...reply.conversation } };
    leaveE2eeView();
    // Fence old ordinary responses before exposing a UUID conversation. Already
    // queued ordinary sends remain owned by their original explicit chat action.
    currentChatID = null;
    currentChatType = "e2ee";
    chatViewVersion++;
    for (const request of chatHistoryRequests.values()) request.controller.abort();
    clearTypingDisplay();
    closeInfoPanel();
    byId("chatArea").dataset.chatMode = "e2ee";
    byId("e2eeConversationView").hidden = false;
    byId("e2eeConversationTitle").textContent = name || "Защищённая переписка";
    byId("e2eeConversationState").textContent = reply.conversation.state === "closed"
        ? "Этот диалог закрыт после замены ключей. Старая история доступна на этом устройстве."
        : "Сквозное шифрование · История на этом устройстве";
    const view: E2eeHistoryView = { reply, name, cursor: null, seen: new Set(), busy: false,
        suspended: null, items: new Map(), browsingOlder: false, loaded: false, statusOffset: 0, auditTicks: 0, catchup: null,
        pendingAttachments: new Map(), pendingCursor: null, pendingBusy: false, pendingReload: false,
        download: null, downloadError: null };
    e2eeHistoryView = view;
    if (typeof initE2eeRecovery === "function") initE2eeRecovery(view);
    bindE2eeComposer(view);
    if (typeof rememberE2eeDialog === "function") rememberE2eeDialog(reply);
    document.querySelectorAll?.(".private-chat-item.active").forEach(row => row.classList.remove("active"));
    void loadE2eeHistory(view);
    void loadE2eePendingAttachments(view);
    return true;
}
async function loadE2eeHistory(view: E2eeHistoryView, current: () => boolean = () => document.visibilityState !== "hidden"): Promise<void> {
    if (!current() || !e2eeViewCurrent(view) || view.busy || view.suspended) return;
    view.busy = true;
    const more = byId<HTMLButtonElement>("e2eeHistoryMore");
    const refresh = byId<HTMLButtonElement>("e2eeHistoryRefresh");
    const notice = byId("e2eeHistoryNotice");
    more.disabled = refresh.disabled = true;
    notice.textContent = "Загружаем локальную историю…";
    try {
        const page = await window.zchatDesktop!.e2ee!.history(view.reply.conversation.conversationId, view.cursor);
        if (!current() || !e2eeViewCurrent(view)) return;
        if (page.context !== view.reply.context) throw "stale";
        removePromotedE2eeAttachments(view, page.page.items);
        const fresh = page.page.items.filter(item => !view.seen.has(item.messageId));
        const history = byId("e2eeHistory");
        // Native order is newest first. Prepend older items to display a normal
        // oldest-to-newest transcript, without interpreting any text as markup.
        const previousHeight = history.scrollHeight;
        const previousTop = history.scrollTop;
        const initial = view.cursor === null;
        for (const item of fresh) {
            view.seen.add(item.messageId);
            view.items.set(item.messageId, item);
            const row = e2eeMessageRow(view, item);
            history.prepend(row);
        }
        const addedHeight = history.scrollHeight - previousHeight;
        // Bound visible plaintext/DOM without cutting off access to older pages.
        // Removed rows stay in native history; "refresh" returns to the latest.
        while (history.children.length > 128) {
            const last = history.lastElementChild as HTMLElement;
            view.seen.delete(last.dataset.messageId!);
            view.items.delete(last.dataset.messageId!);
            history.removeChild(last);
        }
        pruneE2eeLocalFiles(view);
        view.loaded = true;
        view.browsingOlder ||= !initial;
        view.catchup = null;
        view.cursor = page.page.nextCursor;
        more.hidden = !view.cursor;
        history.scrollTop = initial ? history.scrollHeight : previousTop + addedHeight;
        notice.textContent = view.seen.size === 0 ? "В локальной истории пока нет сообщений."
            : view.seen.size >= 128 ? "Отображается фрагмент истории до 128 сообщений. Обновите историю, чтобы вернуться к новым." : "";
    } catch (error) {
        if (!current() || !e2eeViewCurrent(view)) return;
        // A stale native/account/cursor answer cannot leave an apparently current
        // transcript. Refresh explicitly starts at the latest page, never recovery.
        if (error === "stale" || (error as { name?: string })?.name === "AbortError") {
            closeE2eeLocalFile(view);
            byId("e2eeHistory").replaceChildren();
            view.seen.clear();
            view.items.clear();
            view.loaded = false;
            notice.textContent = "Состояние истории изменилось. Обновите её или откройте диалог заново.";
        } else notice.textContent = "Не удалось прочитать локальную историю. Повторите обновление.";
        view.cursor = null;
        more.hidden = true;
    } finally {
        view.busy = false;
        if (e2eeViewCurrent(view)) more.disabled = refresh.disabled = !!view.suspended;
    }
}
byId("e2eeHistoryMore").addEventListener("click", () => {
    if (e2eeHistoryView?.cursor) void loadE2eeHistory(e2eeHistoryView);
});
byId("e2eeHistoryRefresh").addEventListener("click", () => {
    if (e2eeHistoryView && !e2eeHistoryView.suspended) openE2eeConversation(e2eeHistoryView.reply, e2eeHistoryView.name);
});
byId("e2eeViewClose").addEventListener("click", () => {
    leaveE2eeView();
    currentChatType = null;
    messagesEl.replaceChildren();
    chatNameEl.textContent = "Выберите чат";
    chatStatusEl.textContent = "";
    byId("audioCallBtn").style.display = "none";
});
window.addEventListener("pagehide", leaveE2eeView);
window.addEventListener("storage", event => {
    if ((event.key === "token" || event.key === null) && localStorage.getItem("token") !== token) leaveE2eeView();
});

function e2eeMessageRow(view: E2eeHistoryView, item: DesktopHistoryItem): HTMLElement {
    const row = document.createElement("article");
    row.dataset.messageId = item.messageId;
    row.className = "e2ee-message" + (item.outgoing ? " e2ee-message-own" : "");
    const author = document.createElement("strong");
    author.textContent = item.outgoing ? "Вы" : view.name || "Собеседник";
    const text = document.createElement("p");
    text.textContent = item.attachment ? e2eeAttachmentLabel(item.attachment) : item.text;
    const status = document.createElement("small");
    renderE2eeDelivery(status, item.delivery);
    row.append(author, text, status);
    if (item.attachment && item.cacheState) {
        const actions = document.createElement("div");
        actions.className = "e2ee-attachment-download";
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = "Скачать";
        button.addEventListener("click", () => { void downloadE2eeAttachment(view, item.messageId); });
        const notice = document.createElement("small");
        notice.setAttribute("role", "status");
        actions.append(button, notice);
        const local = e2eeLocalFileActions(view, item.attachment, {
            generationId: null, conversationId: item.conversationId, messageId: item.messageId,
        }, view.reply.context, () => e2eeViewCurrent(view) && !view.suspended && view.items.get(item.messageId)?.cacheState === "saved");
        if (local) actions.append(local);
        row.append(actions);
        renderE2eeDownload(view, row, item);
    }
    return row;
}
function e2eeCacheLabel(state: DesktopHistoryItem["cacheState"]): string {
    return state === "saved" ? "Сохранено на этом устройстве"
        : state === "removed" ? "Локальная копия удалена" : "";
}
function renderE2eeDownload(view: E2eeHistoryView, row: HTMLElement, item: DesktopHistoryItem): void {
    const actions = row.children[3];
    if (!actions) return;
    const button = actions.children[0] as HTMLButtonElement;
    const notice = actions.children[1] as HTMLElement;
    button.hidden = item.outgoing || item.cacheState !== "missing";
    button.disabled = !!view.download || !!view.suspended;
    const local = actions.children[2] as HTMLElement | undefined;
    if (local) local.hidden = item.cacheState !== "saved" || !!view.suspended;
    const label = e2eeCacheLabel(item.cacheState) || (view.download?.messageId === item.messageId ? "Скачиваем…"
        : view.downloadError?.messageId === item.messageId ? view.downloadError.text : "");
    if (notice.textContent !== label) notice.textContent = label;
}
function renderE2eeDownloads(view: E2eeHistoryView): void {
    for (const element of Array.from(byId("e2eeHistory").children)) {
        const row = element as HTMLElement;
        const item = view.items.get(row.dataset.messageId!);
        if (item) renderE2eeDownload(view, row, item);
    }
}
async function downloadE2eeAttachment(view: E2eeHistoryView, messageId: string): Promise<void> {
    const item = view.items.get(messageId);
    if (!e2eeViewCurrent(view) || view.suspended || view.download || !item?.attachment || item.outgoing || item.cacheState !== "missing") return;
    const context = view.reply.context;
    const conversationId = view.reply.conversation.conversationId;
    const job = { messageId, controller: new AbortController() };
    view.download = job;
    view.downloadError = null;
    renderE2eeDownloads(view);
    const current = () => e2eeViewCurrent(view) && !view.suspended && view.download === job && !job.controller.signal.aborted;
    try {
        const reply = await window.zchatDesktop!.e2ee!.downloadAttachment({ conversationId, messageId }, context, job.controller.signal);
        if (!current()) return;
        if (reply.context !== context || reply.attachment.conversationId !== conversationId || reply.attachment.messageId !== messageId) throw "stale";
        const visible = view.items.get(messageId);
        if (visible?.cacheState === "missing") visible.cacheState = "saved";
    } catch (error) {
        if (!current()) return;
        const visible = view.items.get(messageId);
        if (visible?.cacheState === "missing" && error === "attachment_removed") visible.cacheState = "removed";
        const labels = new Map([
            ["attachment_unavailable", "Файл больше недоступен на сервере. Попросите отправить его ещё раз."],
            ["attachment_integrity", "Не удалось проверить целостность файла. Повторите попытку."],
            ["busy", "Загрузка занята. Повторите попытку."],
            ["native_wait", "Время ожидания истекло. Повторите попытку."],
            ["authentication", "Сессия завершена. Войдите снова."],
        ]);
        view.downloadError = { messageId, text: typeof error === "string" && labels.get(error) || "Не удалось скачать файл. Повторите попытку." };
    } finally {
        if (view.download === job) {
            view.download = null;
            if (e2eeViewCurrent(view)) renderE2eeDownloads(view);
        }
    }
}
function e2eeAttachmentLabel(item: DesktopAttachmentMetadata): string {
    const kind = { photo: "Фото", voice: "Голосовое сообщение", file: "Файл" }[item.kind];
    const format = new Intl.NumberFormat("ru", { maximumFractionDigits: 1 });
    const size = item.size >= 1024 * 1024 ? `${format.format(item.size / (1024 * 1024))} МБ`
        : item.size >= 1024 ? `${format.format(item.size / 1024)} КБ` : `${item.size} Б`;
    return `${kind} · ${item.filename} · ${size}${item.caption ? "\n" + item.caption : ""}`;
}

let e2eeLocalFile: { owner: object; actions: HTMLElement; controller: AbortController; previewUrl: string | null } | null = null;
let e2eeVoicePlayback: { owner: object; actions: HTMLElement; audio: HTMLAudioElement; button: HTMLButtonElement; url: string } | null = null;

function stopE2eeVoicePlayback(): void {
    const playback = e2eeVoicePlayback;
    if (!playback) return;
    e2eeVoicePlayback = null;
    playback.audio.pause();
    playback.audio.removeAttribute("src");
    playback.audio.load();
    playback.audio.hidden = true;
    playback.button.hidden = false;
    URL.revokeObjectURL(playback.url);
}

function finishE2eeLocalFile(): void {
    const job = e2eeLocalFile;
    if (!job) return;
    e2eeLocalFile = null;
    job.controller.abort();
    if (!job.previewUrl) return;
    byId("e2eeAttachmentPreviewImage").removeAttribute("src");
    URL.revokeObjectURL(job.previewUrl);
    const dialog = byId<HTMLDialogElement>("e2eeAttachmentPreview");
    if (dialog.open) dialog.close();
}

function closeE2eeLocalFile(owner: object): void {
    if (e2eeVoicePlayback?.owner === owner) stopE2eeVoicePlayback();
    if (e2eeLocalFile?.owner === owner) finishE2eeLocalFile();
}
function pruneE2eeLocalFiles(owner: object): void {
    if (e2eeVoicePlayback?.owner === owner && !e2eeVoicePlayback.actions.isConnected) stopE2eeVoicePlayback();
    if (e2eeLocalFile?.owner === owner && !e2eeLocalFile.actions.isConnected) finishE2eeLocalFile();
}
function closeE2eePhotoPreview(): void {
    if (e2eeLocalFile?.previewUrl) finishE2eeLocalFile();
}
document.getElementById("e2eeAttachmentPreviewClose")?.addEventListener("click", closeE2eePhotoPreview);
document.getElementById("e2eeAttachmentPreview")?.addEventListener("click", event => {
    if (event.target === event.currentTarget) closeE2eePhotoPreview();
});
document.getElementById("e2eeAttachmentPreview")?.addEventListener("cancel", event => {
    event.preventDefault();
    closeE2eePhotoPreview();
});
document.getElementById("e2eeAttachmentPreview")?.addEventListener("close", () => {
    if (!byId<HTMLDialogElement>("e2eeAttachmentPreview").open) closeE2eePhotoPreview();
});

function e2eeLocalFileActions(owner: object, item: DesktopAttachmentMetadata, target: DesktopLocalAttachmentTarget,
    context: number, current: () => boolean): HTMLElement | null {
    const api = window.zchatDesktop?.e2ee;
    if (!api || typeof api.exportAttachment !== "function" || typeof api.previewAttachment !== "function") return null;
    const actions = document.createElement("div");
    actions.className = "e2ee-attachment-download";
    const notice = document.createElement("small");
    notice.setAttribute("role", "status");
    const audio = item.kind === "voice" && typeof api.listenAttachment === "function" ? document.createElement("audio") : null;
    const run = async (action: "preview" | "export" | "listen"): Promise<void> => {
        if (!current() || !actions.isConnected) return;
        if (e2eeLocalFile) { notice.textContent = "Дождитесь завершения операции с вложением."; return; }
        const job = { owner, actions, controller: new AbortController(), previewUrl: null as string | null };
        e2eeLocalFile = job;
        const valid = () => e2eeLocalFile === job && current() && actions.isConnected;
        notice.textContent = action === "listen" ? "Открываем запись…" : action === "preview" ? "Готовим предпросмотр…" : "Выберите, куда сохранить незашифрованную копию.";
        try {
            const reply = await (action === "preview" ? api.previewAttachment(target, context, job.controller.signal)
                : action === "listen" ? api.listenAttachment(target, context, job.controller.signal)
                : api.exportAttachment(target, context, item.filename, job.controller.signal));
            if (!valid()) return;
            if (reply.context !== context || reply.target.generationId !== target.generationId ||
                reply.target.conversationId !== target.conversationId || reply.target.messageId !== target.messageId) throw "stale";
            notice.textContent = reply.result.kind === "exported" ? "Копия сохранена." : "";
            if (reply.result.kind === "preview") {
                const media = new Blob([Uint8Array.from(atob(reply.result.png), char => char.charCodeAt(0))], { type: "image/png" });
                const image = byId<HTMLImageElement>("e2eeAttachmentPreviewImage");
                job.previewUrl = URL.createObjectURL(media);
                image.src = job.previewUrl;
                await image.decode();
                if (!valid()) return;
                image.style.setProperty("--e2ee-photo-ratio", String(image.naturalWidth / image.naturalHeight));
                byId<HTMLDialogElement>("e2eeAttachmentPreview").showModal();
            } else if (reply.result.kind === "voice" && "audio" in reply && audio && listen) {
                stopE2eeVoicePlayback();
                const url = URL.createObjectURL(new Blob([reply.audio], { type: reply.result.mimeType }));
                const playback = { owner, actions, audio, button: listen, url };
                e2eeVoicePlayback = playback;
                const focused = document.activeElement === listen;
                audio.src = url;
                audio.hidden = false;
                listen.hidden = true;
                if (focused) audio.focus({ preventScroll: true });
                // Some WebViews can block autoplay after an asynchronous read.
                // The native controls remain available for an explicit click.
                void audio.play().catch(() => {
                    if (e2eeVoicePlayback === playback) notice.textContent = "Нажмите ▶ в плеере, чтобы начать воспроизведение.";
                });
            }
        } catch (error) {
            if (valid()) {
                notice.textContent = error === "attachment_exists" ? "Файл с таким именем уже есть. Выберите другое имя."
                    : error === "attachment_preview" ? "Предпросмотр этого фото недоступен. Можно сохранить копию."
                    : error === "not_found" || error === "attachment_removed" ? "Локальная копия недоступна. Обновите историю."
                    : error === "busy" ? "Файл занят. Повторите попытку."
                    : action === "listen" ? "Не удалось открыть запись. Можно сохранить копию."
                    : action === "preview" ? "Не удалось открыть предпросмотр." : "Не удалось сохранить копию.";
            }
            if (e2eeLocalFile === job) finishE2eeLocalFile();
        } finally {
            if (e2eeLocalFile === job && (!job.previewUrl || !valid())) finishE2eeLocalFile();
        }
    };
    const button = (label: string, action: "preview" | "export" | "listen"): HTMLButtonElement => {
        const element = document.createElement("button");
        element.type = "button";
        element.textContent = label;
        element.addEventListener("click", () => { void run(action); });
        return element;
    };
    if (item.kind === "photo") actions.append(button("Предпросмотр", "preview"));
    const listen = audio ? button("Прослушать", "listen") : null;
    if (audio && listen) {
        audio.className = "e2ee-voice-player";
        audio.controls = true;
        audio.hidden = true;
        audio.preload = "metadata";
        audio.setAttribute("aria-label", "Голосовое сообщение");
        audio.addEventListener("playing", () => { notice.textContent = ""; });
        audio.addEventListener("error", () => {
            if (e2eeVoicePlayback?.audio !== audio) return;
            stopE2eeVoicePlayback();
            notice.textContent = "Не удалось воспроизвести запись.";
        });
        actions.append(listen, audio);
    }
    actions.append(button("Сохранить копию…", "export"), notice);
    return actions;
}
function resetE2eePendingAttachments(): void {
    const panel = byId<HTMLDetailsElement>("e2eePendingAttachments");
    panel.hidden = true;
    panel.open = false;
    byId("e2eePendingAttachmentList").replaceChildren();
    byId("e2eePendingAttachmentNotice").textContent = "";
    byId("e2eePendingAttachmentMore").hidden = true;
}
function renderE2eePendingAttachments(view: E2eeHistoryView, notice = ""): void {
    const items = [...view.pendingAttachments.values()];
    byId("e2eePendingAttachmentList").replaceChildren(...items.map(item => {
        const row = document.createElement("li");
        row.textContent = e2eeAttachmentLabel(item.attachment);
        return row;
    }));
    byId("e2eePendingAttachmentNotice").textContent = notice;
    const more = byId<HTMLButtonElement>("e2eePendingAttachmentMore");
    more.hidden = view.pendingCursor === null;
    more.disabled = view.pendingBusy;
    byId("e2eePendingAttachments").hidden = !items.length && !notice && view.pendingCursor === null;
}
function removePromotedE2eeAttachments(view: E2eeHistoryView, items: DesktopHistoryItem[]): void {
    let changed = false;
    for (const item of items) {
        if (item.outgoing && view.pendingAttachments.delete(item.messageId)) changed = true;
    }
    if (changed) renderE2eePendingAttachments(view);
}
async function loadE2eePendingAttachments(view: E2eeHistoryView, reload = false): Promise<void> {
    const api = window.zchatDesktop?.e2ee;
    if (typeof api?.pendingAttachments !== "function" || view.suspended || !e2eeViewCurrent(view)) return;
    view.pendingReload ||= reload;
    if (view.pendingBusy) return;
    if (view.pendingReload) {
        view.pendingCursor = null;
        view.pendingReload = false;
    }
    view.pendingBusy = true;
    byId<HTMLButtonElement>("e2eePendingAttachmentMore").disabled = true;
    let notice = "";
    try {
        const { context, page } = await api.pendingAttachments(view.reply.conversation.conversationId, view.reply.context, view.pendingCursor);
        if (!e2eeViewCurrent(view) || view.suspended) return;
        if (context !== view.reply.context) throw "stale";
        view.pendingAttachments.clear();
        for (const item of page.items) {
            if (!view.seen.has(item.messageId)) view.pendingAttachments.set(item.messageId, item);
        }
        view.pendingCursor = page.nextCursor;
    } catch (error) {
        if (!e2eeViewCurrent(view) || view.suspended) return;
        view.pendingAttachments.clear();
        view.pendingCursor = null;
        if (error !== "stale" && (error as { name?: string })?.name !== "AbortError") {
            notice = "Не удалось прочитать список вложений. Обновите историю.";
        }
    } finally {
        view.pendingBusy = false;
        if (e2eeViewCurrent(view) && !view.suspended) {
            renderE2eePendingAttachments(view, notice);
            if (view.pendingReload) void loadE2eePendingAttachments(view);
        }
    }
}
byId("e2eePendingAttachmentMore").addEventListener("click", () => {
    if (e2eeHistoryView?.pendingCursor) void loadE2eePendingAttachments(e2eeHistoryView);
});
function renderE2eeDelivery(node: HTMLElement, delivery: DesktopDelivery | null): void {
    const label = e2eeDeliveryLabel(delivery);
    if (node.textContent !== label) node.textContent = label;
    node.title = delivery?.removal === "expired" ? "Срок хранения серверной копии истёк"
        : delivery?.removal ? "Серверная копия удалена после замены устройства" : "";
}
function pauseE2eeView(message: string | null): void {
    const view = e2eeHistoryView;
    if (!view || !e2eeViewCurrent(view)) return;
    view.suspended = message;
    if (message && typeof invalidateE2eeRecovery === "function") invalidateE2eeRecovery(view);
    if (message) {
        view.download?.controller.abort();
        if (typeof leaveE2eeFile === "function") leaveE2eeFile();
        if (typeof leaveE2eeVoice === "function") leaveE2eeVoice();
        closeE2eeLocalFile(view);
        view.catchup = null; view.auditTicks = 0;
        view.pendingAttachments.clear(); view.pendingCursor = null; view.pendingReload = false;
        resetE2eePendingAttachments();
    }
    byId<HTMLButtonElement>("e2eeHistoryRefresh").disabled = !!message || view.busy;
    byId<HTMLButtonElement>("e2eeHistoryMore").disabled = !!message || view.busy;
    renderE2eeDownloads(view);
    renderE2eeComposer();
}
// One local history page and one outgoing status batch per call. Catch-up is
// staged until a known row/end is reached; a gap is never silently skipped.
async function refreshE2eeHistory(view: E2eeHistoryView, current: () => boolean = () => document.visibilityState !== "hidden"): Promise<void> {
    if (!current() || !e2eeViewCurrent(view) || view.busy || view.suspended || !view.loaded) return;
    view.busy = true;
    const valid = () => current() && e2eeViewCurrent(view) && !view.suspended;
    try {
        const api = window.zchatDesktop!.e2ee!;
        const history = byId("e2eeHistory");
        const page = await api.history(view.reply.conversation.conversationId, view.catchup?.cursor || null);
        if (!valid()) return;
        if (page.context !== view.reply.context) throw "stale";
        removePromotedE2eeAttachments(view, page.page.items);
        const fresh = page.page.items.filter(item => !view.seen.has(item.messageId));
        const atBottom = history.scrollHeight - history.scrollTop - history.clientHeight < 48;
        if (view.browsingOlder || !atBottom) {
            view.catchup = null;
            view.auditTicks = 0;
            if (fresh.length) byId("e2eeHistoryNotice").textContent = "Есть новые сообщения. Обновите историю, когда закончите чтение старых.";
        } else {
            // Periodically revisit the whole latest window: a delayed delivery
            // can sort below the first page (old timestamp/retained epoch).
            const audit = !view.catchup && ++view.auditTicks >= 15;
            const pending = view.catchup || { cursor: null, items: [], replacing: audit };
            const pendingIDs = new Set(pending.items.map(item => item.messageId));
            pending.items.push(...page.page.items.filter(item => !pendingIDs.has(item.messageId)));
            pending.cursor = page.page.nextCursor;
            const joined = page.page.items.some(item => view.seen.has(item.messageId)) || !pending.cursor;
            const newCount = pending.items.filter(item => !view.seen.has(item.messageId)).length;
            pending.replacing ||= view.seen.size + newCount > 128;
            // Stop at a complete page boundary below 128 (pages contain <=8),
            // so its opaque cursor never skips rows trimmed from that page.
            if ((!pending.replacing && joined) || !pending.cursor || pending.items.length >= 120) {
                const replaced = pending.replacing || !joined; // More than a full window arrived: expose the entire latest window.
                const previousItems = view.items;
                if (replaced) { view.seen.clear(); view.items = new Map(); view.cursor = pending.cursor; }
                for (const item of pending.items) {
                    if (view.seen.has(item.messageId)) continue;
                    // A multi-tick window snapshot can contain an earlier status
                    // than the currently rendered ACK. Preserve known facts;
                    // fresh status batches, not staged history, update them.
                    const known = previousItems.get(item.messageId);
                    const cacheState = item.cacheState === "missing" ? known?.cacheState ?? item.cacheState : item.cacheState;
                    view.items.set(item.messageId, known ? { ...item, delivery: known.delivery, cacheState } : item);
                    view.seen.add(item.messageId);
                }
                if (newCount || replaced) {
                    // Match the native protected index (epoch, timestamp, UUID),
                    // including late insertions between already visible records.
                    const ordered = [...view.items.values()].sort((a, b) => {
                        const epoch = BigInt(a.cryptoEpoch) - BigInt(b.cryptoEpoch);
                        const time = BigInt(a.createdAtUnixMs) - BigInt(b.createdAtUnixMs);
                        return epoch < 0n ? -1 : epoch > 0n ? 1 : time < 0n ? -1 : time > 0n ? 1
                            : a.messageId.localeCompare(b.messageId);
                    });
                    // Keep the visible inline player and any pending media read
                    // attached when other messages arrive or history is audited.
                    const rows = new Map(Array.from(history.children, element => {
                        const row = element as HTMLElement;
                        return [row.dataset.messageId, row] as const;
                    }));
                    history.replaceChildren(...ordered.map(item => {
                        const row = rows.get(item.messageId);
                        if (!row) return e2eeMessageRow(view, item);
                        renderE2eeDelivery(row.children[2] as HTMLElement, item.delivery);
                        if (item.attachment) renderE2eeDownload(view, row, item);
                        return row;
                    }));
                    pruneE2eeLocalFiles(view);
                }
                if (replaced) view.cursor = pending.cursor;
                byId("e2eeHistoryMore").hidden = !view.cursor;
                if (pending.items.length) {
                    if (fresh.length || replaced) history.scrollTop = history.scrollHeight;
                    byId("e2eeHistoryNotice").textContent = replaced
                        ? "Показан последний фрагмент истории. Предыдущие сообщения доступны через «Ранее»." : "";
                }
                view.catchup = null;
                if (replaced) view.auditTicks = 0;
            } else {
                view.catchup = pending;
                byId("e2eeHistoryNotice").textContent = "Загружаем новые сообщения из локальной истории…";
            }
        }
        const ids = [...view.items.values()].filter(item => item.outgoing).map(item => item.messageId);
        if (!ids.length || !valid()) return;
        const offset = view.statusOffset % ids.length;
        const batch = ids.slice(offset, offset + 8);
        view.statusOffset = (offset + batch.length) % ids.length;
        const statuses = await api.messageStatuses(view.reply.conversation.conversationId, batch);
        if (!valid()) return;
        if (statuses.context !== view.reply.context) throw "stale";
        for (const item of statuses.page.items) {
            const original = view.items.get(item.messageId);
            if (!original || original.cryptoEpoch !== item.cryptoEpoch) continue;
            original.delivery = item.delivery;
            const row = Array.from(history.children).find(row => (row as HTMLElement).dataset.messageId === item.messageId);
            if (row) renderE2eeDelivery(row.children[2] as HTMLElement, item.delivery);
        }
    } catch (error) {
        if (!valid()) return;
        view.catchup = null;
        if (error === "stale" || (error as { name?: string })?.name === "AbortError") {
            // Context may have retired; don't unlock or silently rebind this draft.
            pauseE2eeView("Состояние устройства изменилось. Скопируйте нужный текст и откройте диалог заново.");
        } else throw error; // Shared scheduler applies bounded backoff.
    } finally {
        view.busy = false;
    }
}
