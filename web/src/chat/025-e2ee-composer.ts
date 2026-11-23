// Bounded, page-local drafts, never a second delivery outbox. Native owns durable
// acceptance and retries after that point. An uncertain attempt keeps its body/UUID.
interface E2eeDraft {
    context: number;
    chat: string;
    text: string;
    messageId: string | null;
    busy: boolean;
    error: string | null;
}
const e2eeDrafts = new Map<string, E2eeDraft>();
function e2eeDraftKey(context: number, chat: string): string { return context + ":" + chat; }
function clearE2eeComposer(): void {
    byId<HTMLTextAreaElement>("e2eeTextInput").value = "";
    byId<HTMLTextAreaElement>("e2eeTextInput").disabled = true;
    byId<HTMLButtonElement>("e2eeTextSend").disabled = true;
    byId("e2eeTextNotice").textContent = "";
    byId("e2eeTextSize").textContent = "";
    if (authExpiredHandled || localStorage.getItem("token") !== token) e2eeDrafts.clear();
}
function bindE2eeComposer(view: E2eeHistoryView): void {
    for (const [key, draft] of e2eeDrafts) if (draft.context !== view.reply.context) e2eeDrafts.delete(key);
    const draft = e2eeDrafts.get(e2eeDraftKey(view.reply.context, view.reply.conversation.conversationId));
    byId<HTMLTextAreaElement>("e2eeTextInput").value = draft?.text || "";
    renderE2eeComposer();
}
function renderE2eeComposer(): void {
    const view = e2eeHistoryView;
    if (!view || !e2eeViewCurrent(view)) { clearE2eeComposer(); return; }
    const draft = e2eeDrafts.get(e2eeDraftKey(view.reply.context, view.reply.conversation.conversationId));
    const input = byId<HTMLTextAreaElement>("e2eeTextInput");
    if (draft && input.value !== draft.text) input.value = draft.text;
    input.disabled = false;
    input.readOnly = !!view.suspended || view.reply.conversation.state === "closed" || !!draft?.messageId;
    const size = new TextEncoder().encode(input.value).length;
    const send = byId<HTMLButtonElement>("e2eeTextSend");
    send.textContent = draft?.messageId ? "Повторить" : "Отправить";
    const recoveryBlock = typeof e2eeRecoverySendBlock === "function" ? e2eeRecoverySendBlock(view) : null;
    send.disabled = !!recoveryBlock || !!view.suspended || view.reply.conversation.state === "closed" || !!draft?.busy || !input.value.trim() || size > 2000;
    const clear = byId<HTMLButtonElement>("e2eeDraftClear");
    clear.disabled = !!draft?.busy;
    clear.hidden = !input.value;
    byId("e2eeTextSize").textContent = size + " / 2000 байт";
    byId("e2eeTextNotice").textContent = draft?.busy ? "Сохраняем сообщение на этом устройстве…"
        : view.suspended || recoveryBlock || draft?.error || (size > 2000 ? "Слишком длинное сообщение. Сократите текст." : "");
    if (typeof renderE2eeRecovery === "function") renderE2eeRecovery();
    if (typeof renderE2eePhoto === "function") renderE2eePhoto(view);
    if (typeof renderE2eeFile === "function") renderE2eeFile(view);
    if (typeof renderE2eeVoice === "function") renderE2eeVoice(view);
}
function updateE2eeDraft(): void {
    const view = e2eeHistoryView;
    if (!view || !e2eeViewCurrent(view) || view.suspended || view.reply.conversation.state === "closed") return;
    const key = e2eeDraftKey(view.reply.context, view.reply.conversation.conversationId);
    const old = e2eeDrafts.get(key);
    if (old?.messageId) { renderE2eeComposer(); return; }
    const text = byId<HTMLTextAreaElement>("e2eeTextInput").value;
    if (!text) e2eeDrafts.delete(key);
    else if (old) old.text = text;
    else if (e2eeDrafts.size < 8) e2eeDrafts.set(key, {
        context: view.reply.context, chat: view.reply.conversation.conversationId,
        text, messageId: null, busy: false, error: null,
    });
    else {
        byId("e2eeTextNotice").textContent = "Открыто слишком много черновиков. Завершите или очистите один из них.";
        byId<HTMLButtonElement>("e2eeTextSend").disabled = true;
        return;
    }
    renderE2eeComposer();
}
function e2eeSendError(error: unknown): string {
    if (error === "stale" || (error as { name?: string })?.name === "AbortError") return "Сессия устройства изменилась. Скопируйте нужный текст и откройте диалог заново.";
    if (error === "conversation_closed") return "Диалог закрыт. Для новой переписки откройте профиль собеседника.";
    if (error === "sending_frozen" || error === "recovery_required") return "Защищённая сессия требует восстановления. Сообщение не будет автоматически перенесено в новую сессию.";
    if (error === "session_pending") return "Ожидаем установления защищённой сессии. Можно повторить эту попытку позже.";
    if (error === "forbidden") return "Нет доступа к отправке в этот диалог. Проверьте состояние защищённой переписки.";
    return "Не удалось подтвердить сохранение. Повторите отправку с тем же текстом: повторная попытка не создаст копию сообщения.";
}
async function sendE2eeText(): Promise<void> {
    const view = e2eeHistoryView;
    if (view && typeof e2eeRecoverySendBlock === "function" && e2eeRecoverySendBlock(view)) { renderE2eeComposer(); return; }
    if (!view || !e2eeViewCurrent(view) || view.suspended || view.reply.conversation.state === "closed") return;
    const key = e2eeDraftKey(view.reply.context, view.reply.conversation.conversationId);
    const input = byId<HTMLTextAreaElement>("e2eeTextInput");
    let draft = e2eeDrafts.get(key);
    if (draft?.busy) return;
    // Retrying always uses immutable model text, never a changed DOM field.
    const text = draft?.messageId ? draft.text : input.value;
    if (!text.trim() || new TextEncoder().encode(text).length > 2000) { renderE2eeComposer(); return; }
    if (!draft) {
        if (e2eeDrafts.size >= 8) {
            byId("e2eeTextNotice").textContent = "Открыто слишком много черновиков. Завершите или очистите один из них.";
            return;
        }
        draft = { context: view.reply.context, chat: view.reply.conversation.conversationId, text, messageId: null, busy: false, error: null };
        e2eeDrafts.set(key, draft);
    }
    if (!draft.messageId) { draft.text = text; draft.messageId = crypto.randomUUID(); }
    draft.busy = true;
    draft.error = null;
    renderE2eeComposer();
    try {
        const result = await window.zchatDesktop!.e2ee!.sendText({
            conversationId: draft.chat, messageId: draft.messageId, text: draft.text,
        }, draft.context);
        if (authExpiredHandled || localStorage.getItem("token") !== token) { leaveE2eeView(); return; }
        if (e2eeDrafts.get(key) !== draft) return;
        if (result.context !== draft.context) throw "stale";
        e2eeDrafts.delete(key); // Native durable local COMMIT, not delivery ACK.
        const selected = e2eeHistoryView;
        if (selected && e2eeViewCurrent(selected) && selected.reply.context === draft.context && selected.reply.conversation.conversationId === draft.chat) {
            input.value = "";
            // Same chat/draft may have been reopened while the command awaited.
            // A different selection/draft must not be cleared or refreshed.
            renderE2eeComposer();
            if (!selected.suspended) void refreshE2eeHistory(selected).catch(() => {
                if (e2eeViewCurrent(selected)) byId("e2eeHistoryNotice").textContent = "Обновление истории задерживается. Сообщение уже сохранено локально.";
            });
            byId("e2eeTextNotice").textContent = result.queued.sealedUnsent
                ? "Сообщение сохранено, но его отправка остановлена. Проверьте статус в истории."
                : "Сохранено на этом устройстве. Доставку показываем отдельно в истории.";
        }
    } catch (error) {
        if (authExpiredHandled || localStorage.getItem("token") !== token) { leaveE2eeView(); return; }
        if (e2eeDrafts.get(key) === draft) draft.error = e2eeSendError(error);
    } finally {
        draft.busy = false;
        const selected = e2eeHistoryView;
        if (selected && e2eeViewCurrent(selected) && selected.reply.context === draft.context && selected.reply.conversation.conversationId === draft.chat && e2eeDrafts.get(key) === draft) renderE2eeComposer();
    }
}
function discardE2eeDraft(): void {
    const view = e2eeHistoryView;
    if (!view || !e2eeViewCurrent(view)) return;
    const key = e2eeDraftKey(view.reply.context, view.reply.conversation.conversationId);
    const draft = e2eeDrafts.get(key);
    if (draft?.busy) return;
    if (draft?.messageId && !confirm("Сообщение может быть уже сохранено и доставлено. Очистка поля не отменяет его отправку. Начать новое сообщение?")) return;
    e2eeDrafts.delete(key);
    byId<HTMLTextAreaElement>("e2eeTextInput").value = "";
    renderE2eeComposer();
}
byId("e2eeTextInput").addEventListener("input", updateE2eeDraft);
byId("e2eeTextInput").addEventListener("keydown", (event: KeyboardEvent) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) { event.preventDefault(); void sendE2eeText(); }
});
byId("e2eeTextSend").addEventListener("click", () => { void sendE2eeText(); });
byId("e2eeDraftClear").addEventListener("click", discardE2eeDraft);
window.addEventListener("pagehide", () => { e2eeDrafts.clear(); clearE2eeComposer(); });
