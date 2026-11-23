interface E2eeFileDraft {
    context: number;
    chat: string;
    file: File;
    kind: "file" | "voice";
    attempt: DesktopE2eeFile | null;
    controller: AbortController | null;
    notice: string;
}
const E2EE_FILE_BYTES = 50 * 1024 * 1024;
let e2eeFileDraft: E2eeFileDraft | null = null;
let e2eeFilePicker: E2eeHistoryView | null = null;

function e2eeFileMatches(view: E2eeHistoryView, draft: E2eeFileDraft | null): draft is E2eeFileDraft {
    return draft !== null && draft.context === view.reply.context && draft.chat === view.reply.conversation.conversationId;
}
function e2eeFileBusy(view: E2eeHistoryView): boolean {
    return e2eeFileMatches(view, e2eeFileDraft) && e2eeFileDraft.controller !== null;
}
function e2eeFileSendBlock(view: E2eeHistoryView): string | null {
    if (view.reply.conversation.state === "closed") return "Диалог закрыт для отправки.";
    return view.suspended || e2eeRecoverySendBlock(view);
}
function e2eeFileNotice(message: string): void {
    const node = byId("e2eeFileNotice");
    node.textContent = message;
    node.hidden = !message;
}
function closeE2eeFileDialog(): void {
    const dialog = byId<HTMLDialogElement>("e2eeFileDialog");
    if (dialog.open) dialog.close();
    byId("e2eeFileName").textContent = "";
    byId<HTMLTextAreaElement>("e2eeFileCaption").value = "";
    byId("e2eeFileStatus").textContent = "";
}
function leaveE2eeFile(): void {
    e2eeFilePicker = null;
    if (e2eeFileDraft?.controller) {
        e2eeFileDraft.controller.abort();
        e2eeFileDraft.notice = "Проверьте, сохранился ли файл на устройстве.";
    }
    if (!e2eeFileDraft?.attempt || authExpiredHandled || localStorage.getItem("token") !== token) e2eeFileDraft = null;
    byId<HTMLInputElement>("e2eeFileInput").value = "";
    closeE2eeFileDialog();
    e2eeFileNotice("");
}
function renderE2eeFile(view: E2eeHistoryView): void {
    if (e2eeFileDraft && e2eeFileDraft.context !== view.reply.context) {
        e2eeFileDraft.controller?.abort();
        e2eeFileDraft = null;
        closeE2eeFileDialog();
    }
    const pick = byId<HTMLButtonElement>("e2eeFilePick");
    const draft = e2eeFileMatches(view, e2eeFileDraft) ? e2eeFileDraft : null;
    const block = e2eeFileSendBlock(view);
    const api = window.zchatDesktop?.e2ee;
    pick.hidden = !api?.compiled || typeof api.prepareFile !== "function";
    pick.disabled = pick.hidden || !!e2eeFilePicker || !!e2eeFileDraft?.controller || (typeof e2eeVoiceBusy === "function" && e2eeVoiceBusy()) || (!!block && !draft?.attempt);
    pick.textContent = draft?.attempt ? "Проверить вложение" : "Файл";
    if (!draft || !byId<HTMLDialogElement>("e2eeFileDialog").open) return;
    const caption = byId<HTMLTextAreaElement>("e2eeFileCaption");
    caption.readOnly = !!draft.attempt;
    const tooLong = new TextEncoder().encode(caption.value).length > 2000;
    const send = byId<HTMLButtonElement>("e2eeFileSend");
    send.textContent = draft.attempt ? "Проверить и повторить" : draft.kind === "voice" ? "Отправить голосовое" : "Отправить файл";
    send.disabled = !!view.suspended || !!draft.controller || (!draft.attempt && (!!block || tooLong));
    byId<HTMLButtonElement>("e2eeFileCancel").disabled = !!draft.controller;
    byId("e2eeFileStatus").textContent = draft.notice || block || (tooLong ? "Сократите подпись до 2000 байт." : "");
}
function showE2eeFile(view: E2eeHistoryView, draft: E2eeFileDraft): void {
    byId("e2eeFileTitle").textContent = draft.kind === "voice" ? "Отправить голосовое сообщение" : "Отправить защищённый файл";
    byId("e2eeFileMetadata").hidden = draft.kind === "voice";
    byId("e2eeFileName").textContent = `${draft.file.name} · ${imageByteLabel(draft.file.size)}`;
    byId<HTMLTextAreaElement>("e2eeFileCaption").value = draft.attempt?.caption || "";
    byId<HTMLDialogElement>("e2eeFileDialog").showModal();
    renderE2eeFile(view);
}
function discardE2eeFile(): boolean {
    if (e2eeFileDraft?.controller) return false;
    if (e2eeFileDraft?.attempt && !confirm("Файл мог уже сохраниться на устройстве. Закрытие формы не остановит его доставку. Закрыть попытку?")) return false;
    e2eeFileDraft = null;
    closeE2eeFileDialog();
    if (e2eeHistoryView) renderE2eeFile(e2eeHistoryView);
    return true;
}

async function submitE2eeFile(): Promise<void> {
    const view = e2eeHistoryView;
    const draft = e2eeFileDraft;
    if (!view || !e2eeViewCurrent(view) || !e2eeFileMatches(view, draft) || view.suspended || draft.controller
        || !byId<HTMLDialogElement>("e2eeFileDialog").open) return;
    const api = window.zchatDesktop!.e2ee!;
    const retry = !!draft.attempt;
    const epoch = () => view.recovery?.state?.cryptoEpoch ?? "0";
    if (!draft.attempt) {
        const caption = byId<HTMLTextAreaElement>("e2eeFileCaption").value;
        if (e2eeFileSendBlock(view) || new TextEncoder().encode(caption).length > 2000) return;
        draft.attempt = { conversationId: draft.chat, messageId: crypto.randomUUID(), file: draft.file, caption, expectedEpoch: epoch() };
    }
    const attempt = draft.attempt;
    const { conversationId, messageId } = attempt;
    const controller = new AbortController();
    draft.controller = controller;
    draft.notice = retry ? "Проверяем сохранённую попытку…" : "Шифруем и сохраняем файл…";
    renderE2eeComposer();
    const current = () => e2eeFileDraft === draft && e2eeViewCurrent(view) && !controller.signal.aborted;
    const matches = (reply: DesktopAttachmentReply) => reply.context === draft.context
        && reply.attachment.conversationId === conversationId && reply.attachment.messageId === messageId;
    try {
        let result = retry ? await api.attachmentInput({ conversationId, messageId }, draft.context, controller.signal) : null;
        if (!current()) return;
        if (result && !matches(result)) throw "stale";
        if (!result || result.attachment.kind === "vacant") {
            if (retry && (view.pendingAttachments.has(messageId) || view.items.get(messageId)?.outgoing)) {
                draft.notice = "Файл уже был сохранён на устройстве. Обновите историю перед повтором.";
                return;
            }
            const blocked = e2eeFileSendBlock(view);
            if (blocked) { draft.notice = blocked; return; }
            if (epoch() !== attempt.expectedEpoch) {
                draft.notice = "Сессия шифрования изменилась. Закройте попытку и выберите файл заново.";
                return;
            }
            draft.notice = "Шифруем и сохраняем файл…";
            renderE2eeFile(view);
            result = draft.kind === "voice" ? await api.prepareVoice(attempt, draft.context, controller.signal)
                : await api.prepareFile(attempt, draft.context, controller.signal);
        }
        if (!current()) return;
        if (!matches(result) || result.attachment.kind === "vacant" || result.attachment.cryptoEpoch !== attempt.expectedEpoch) throw "stale";
        e2eeFileDraft = null;
        closeE2eeFileDialog();
        const attachment = result.attachment;
        e2eeFileNotice(attachment.kind === "prepared" ? (draft.kind === "voice" ? "Голосовое сохранено на этом устройстве и ожидает отправки." : "Файл сохранён на этом устройстве и ожидает отправки.")
            : attachment.sealedUnsent ? "Файл сохранён. Отправка остановлена."
            : attachment.storedOnServer ? "Файл сохранён на сервере." : "Файл ожидает отправки.");
        if (attachment.kind === "prepared") byId<HTMLDetailsElement>("e2eePendingAttachments").open = true;
        void loadE2eePendingAttachments(view, true);
        void refreshE2eeHistory(view).catch(() => {});
    } catch (error) {
        if (!current()) return;
        if (error === "voice_format" || error === "voice_source_too_large") {
            draft.attempt = null;
            draft.notice = "Не удалось подготовить запись. Запишите её заново в формате Ogg или WebM с Opus до 10 МБ.";
        } else if (error === "invalid" || error === "file_source_too_large") {
            draft.attempt = null;
            draft.notice = "Проверьте имя и подпись файла. Максимальный размер — 50 МБ.";
        } else {
            draft.notice = "Не удалось подтвердить сохранение. Проверьте эту попытку перед новой отправкой.";
        }
    } finally {
        draft.controller = null;
        if (e2eeHistoryView && e2eeViewCurrent(e2eeHistoryView)) renderE2eeComposer();
    }
}

byId("e2eeFilePick").addEventListener("click", () => {
    const view = e2eeHistoryView;
    if (!view || !e2eeViewCurrent(view) || e2eeFilePicker || e2eeFileDraft?.controller) return;
    if (e2eeFileMatches(view, e2eeFileDraft) && e2eeFileDraft.attempt) { showE2eeFile(view, e2eeFileDraft); return; }
    if (e2eeFileSendBlock(view) || typeof window.zchatDesktop?.e2ee?.prepareFile !== "function") return;
    if (e2eeFileDraft && !discardE2eeFile()) return;
    e2eeFileNotice("");
    e2eeFilePicker = view;
    const selector = byId<HTMLInputElement>("e2eeFileInput");
    selector.value = "";
    selector.click();
});
byId<HTMLInputElement>("e2eeFileInput").addEventListener("change", event => {
    const view = e2eeFilePicker;
    e2eeFilePicker = null;
    const input = event.currentTarget as HTMLInputElement;
    const file = input.files?.[0];
    input.value = "";
    if (!view || !e2eeViewCurrent(view)) return;
    renderE2eeFile(view);
    if (!file || e2eeFileSendBlock(view)) return;
    if (!file.size || file.size > E2EE_FILE_BYTES || !file.name || new TextEncoder().encode(file.name).length > 255) {
        e2eeFileNotice("Выберите непустой файл до 50 МБ с именем не длиннее 255 байт.");
        return;
    }
    e2eeFileDraft = { context: view.reply.context, chat: view.reply.conversation.conversationId, file, kind: "file", attempt: null, controller: null, notice: "" };
    showE2eeFile(view, e2eeFileDraft);
});
byId("e2eeFileInput").addEventListener("cancel", () => {
    e2eeFilePicker = null;
    if (e2eeHistoryView) renderE2eeFile(e2eeHistoryView);
});
byId("e2eeFileCaption").addEventListener("input", () => { if (e2eeHistoryView) renderE2eeFile(e2eeHistoryView); });
byId("e2eeFileDialog").addEventListener("cancel", event => { event.preventDefault(); discardE2eeFile(); });
byId("e2eeFileCancel").addEventListener("click", discardE2eeFile);
byId("e2eeFileSend").addEventListener("click", () => { void submitE2eeFile(); });
window.addEventListener("pagehide", () => { leaveE2eeFile(); e2eeFileDraft = null; });
