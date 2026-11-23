type E2eeCleanPhoto = Extract<DesktopPhotoChoice, { mode: "clean" }>;
interface E2eePhotoDraft {
    context: number;
    chat: string;
    file: File;
    attempt: { message: DesktopE2eePhoto; epoch: string | null; remember: string | null } | null;
    busy: boolean;
    notice: string;
}
const E2EE_PHOTO_ASK_ABOVE = 5 * 1024 * 1024;
const E2EE_PHOTO_INPUT_MAX = 20 * 1024 * 1024;
let e2eePhotoDraft: E2eePhotoDraft | null = null;
let e2eePhotoPickerView: E2eeHistoryView | null = null;

function e2eePhotoPreferenceKey(): string {
    return accountStorageKey("zchat_e2ee_photo_quality_v1");
}
function e2eePhotoQualityValid(quality: number): boolean {
    return Number.isInteger(quality) && quality >= 40 && quality <= 95 && quality % 5 === 0;
}
function e2eePhotoSizeValid(size: number): boolean {
    return size === 0 || size === 1600 || size === 2560;
}
function savedE2eePhotoChoice(): E2eeCleanPhoto | null {
    try {
        const value = JSON.parse(localStorage.getItem(e2eePhotoPreferenceKey()) || "null");
        if (value?.mode === "clean" && (value.quality === null || e2eePhotoQualityValid(value.quality))
            && e2eePhotoSizeValid(value.maxDimension)) {
            return { mode: "clean", quality: value.quality, maxDimension: value.maxDimension };
        }
    } catch { /* A malformed or unavailable preference leaves the defaults. */ }
    return null;
}
function e2eePhotoMatches(view: E2eeHistoryView, draft: E2eePhotoDraft | null): draft is E2eePhotoDraft {
    return !!draft && draft.context === view.reply.context && draft.chat === view.reply.conversation.conversationId;
}
function e2eePhotoBusy(view: E2eeHistoryView): boolean {
    return e2eePhotoMatches(view, e2eePhotoDraft) && e2eePhotoDraft.busy;
}
function e2eePhotoSendBlock(view: E2eeHistoryView): string | null {
    if (view.reply.conversation.state === "closed") return "Диалог закрыт для отправки.";
    return view.suspended || e2eeRecoverySendBlock(view);
}
function e2eePhotoNotice(message: string): void {
    const notice = byId("e2eePhotoNotice");
    notice.textContent = message;
    notice.hidden = !message;
}
function closeE2eePhotoDialog(): void {
    const dialog = byId<HTMLDialogElement>("e2eePhotoDialog");
    if (dialog.open) dialog.close();
    byId("e2eePhotoName").textContent = "";
    byId("e2eePhotoStatus").textContent = "";
    byId<HTMLTextAreaElement>("e2eePhotoCaption").value = "";
    byId<HTMLInputElement>("e2eePhotoOriginal").checked = false;
}
function leaveE2eePhoto(): void {
    e2eePhotoPickerView = null;
    byId<HTMLInputElement>("e2eePhotoFile").value = "";
    closeE2eePhotoDialog();
    e2eePhotoNotice("");
    if (authExpiredHandled || localStorage.getItem("token") !== token || !e2eePhotoDraft?.attempt) {
        e2eePhotoDraft = null;
    }
}
function e2eePhotoChoiceFromForm(): DesktopPhotoChoice | null {
    const mode = byId<HTMLSelectElement>("e2eePhotoMode").value;
    if (mode === "original") {
        return byId<HTMLInputElement>("e2eePhotoOriginal").checked
            ? { mode: "original", metadataWarningAcknowledged: true } : null;
    }
    const quality = byId<HTMLInputElement>("e2eePhotoQuality").valueAsNumber;
    const maxDimension = Number(byId<HTMLSelectElement>("e2eePhotoSize").value);
    if ((mode !== "clean" && mode !== "compress") || !e2eePhotoSizeValid(maxDimension)
        || (mode === "compress" && !e2eePhotoQualityValid(quality))) return null;
    return { mode: "clean", quality: mode === "compress" ? quality : null, maxDimension };
}
function renderE2eePhoto(view: E2eeHistoryView): void {
    const api = window.zchatDesktop?.e2ee;
    const button = byId<HTMLButtonElement>("e2eePhotoPick");
    button.hidden = !api?.compiled || typeof api.preparePhoto !== "function";
    if (e2eePhotoDraft && e2eePhotoDraft.context !== view.reply.context) {
        e2eePhotoDraft = null;
        closeE2eePhotoDialog();
        e2eePhotoNotice("");
    }
    const draft = e2eePhotoMatches(view, e2eePhotoDraft) ? e2eePhotoDraft : null;
    const block = e2eePhotoSendBlock(view);
    button.disabled = button.hidden || !!e2eePhotoDraft?.busy || !!e2eePhotoPickerView || (typeof e2eeVoiceBusy === "function" && e2eeVoiceBusy()) || (!!block && !draft?.attempt);
    button.textContent = draft?.attempt ? "Проверить фото" : "Фото";
    if (!draft || !byId<HTMLDialogElement>("e2eePhotoDialog").open) return;
    const mode = byId<HTMLSelectElement>("e2eePhotoMode").value;
    const locked = draft.attempt !== null;
    byId("e2eePhotoQualityField").hidden = mode !== "compress";
    byId("e2eePhotoSizeField").hidden = mode === "original";
    byId("e2eePhotoRememberField").hidden = mode === "original" || locked;
    byId("e2eePhotoOriginalField").hidden = mode !== "original" || locked;
    for (const field of ["Mode", "Quality", "Size", "Remember", "Original", "Caption"]) {
        byId<HTMLInputElement>("e2eePhoto" + field).disabled = locked;
    }
    const quality = byId<HTMLInputElement>("e2eePhotoQuality").value;
    byId("e2eePhotoQualityValue").textContent = quality + " / 100";
    const dimension = Number(byId<HTMLSelectElement>("e2eePhotoSize").value);
    const label = mode === "compress" ? "Метаданные удаляются, качество " + quality
        : mode === "original" ? "Оригинал с метаданными" : "PNG без метаданных";
    byId("e2eePhotoSettingsSummary").textContent = mode !== "original" && dimension ? label + ", до " + dimension + " px" : label;
    const captionBytes = new TextEncoder().encode(byId<HTMLTextAreaElement>("e2eePhotoCaption").value).length;
    const submit = byId<HTMLButtonElement>("e2eePhotoSend");
    submit.disabled = draft.busy || !!view.suspended || (!locked && (!!block || captionBytes > 2000 || !e2eePhotoChoiceFromForm()));
    submit.textContent = locked ? "Проверить и повторить" : "Отправить";
    const cancel = byId<HTMLButtonElement>("e2eePhotoCancel");
    cancel.disabled = draft.busy;
    cancel.textContent = locked ? "Закрыть" : "Отмена";
    byId("e2eePhotoStatus").textContent = draft.notice || block || (captionBytes > 2000 ? "Сократите подпись до 2000 байт." : "");
}
function showE2eePhoto(view: E2eeHistoryView, draft: E2eePhotoDraft): void {
    const attempt = draft.attempt?.message;
    const saved = savedE2eePhotoChoice();
    const choice = attempt?.choice || saved || { mode: "clean", quality: null, maxDimension: 0 } as const;
    byId("e2eePhotoName").textContent = draft.file.name + " · " + imageByteLabel(draft.file.size);
    byId<HTMLTextAreaElement>("e2eePhotoCaption").value = attempt?.caption || "";
    byId<HTMLSelectElement>("e2eePhotoMode").value = choice.mode === "original" ? "original" : choice.quality === null ? "clean" : "compress";
    byId<HTMLInputElement>("e2eePhotoQuality").value = String(choice.mode === "clean" ? choice.quality ?? 80 : 80);
    byId<HTMLSelectElement>("e2eePhotoSize").value = String(choice.mode === "clean" ? choice.maxDimension : 0);
    byId<HTMLInputElement>("e2eePhotoOriginal").checked = false;
    byId<HTMLInputElement>("e2eePhotoRemember").checked = false;
    byId<HTMLDetailsElement>("e2eePhotoSettings").open = !attempt && (!saved || draft.file.size > E2EE_PHOTO_ASK_ABOVE);
    byId<HTMLDialogElement>("e2eePhotoDialog").showModal();
    renderE2eePhoto(view);
}
function discardE2eePhoto(): boolean {
    if (e2eePhotoDraft?.busy) return false;
    if (e2eePhotoDraft?.attempt && !confirm("Фото могло уже сохраниться на устройстве. Закрытие формы не остановит его доставку. Закрыть попытку?")) return false;
    e2eePhotoDraft = null;
    closeE2eePhotoDialog();
    if (e2eeHistoryView) renderE2eePhoto(e2eeHistoryView);
    return true;
}

const e2eePhotoRejected = new Map([
    ["photo_output_too_large", "Подготовленное фото больше 10 МБ. Уменьшите качество или размеры."],
    ["photo_unsupported", "Этот формат фото не поддерживается."],
    ["photo_dimensions", "Изображение слишком большое. Выберите файл с меньшими размерами."],
    ["photo_animation_original", "Анимацию можно отправить только в оригинале."],
    ["photo_multipage_original", "Многостраничное изображение можно отправить только в оригинале."],
    ["photo_original_consent", "Подтвердите отправку оригинала с метаданными."],
    ["photo_source_too_large", "Выберите фото размером до 20 МБ."],
    ["photo_choice_invalid", "Проверьте выбранное качество и размеры."],
    ["photo_decode", "Не удалось прочитать изображение."],
    ["photo_encode", "Не удалось подготовить фото. Попробуйте другой режим очистки или размер."],
]);
async function submitE2eePhoto(): Promise<void> {
    const view = e2eeHistoryView;
    const draft = e2eePhotoDraft;
    if (!view || !e2eeViewCurrent(view) || !e2eePhotoMatches(view, draft) || draft.busy || view.suspended
        || !byId<HTMLDialogElement>("e2eePhotoDialog").open) return;
    const api = window.zchatDesktop!.e2ee!;
    const retry = !!draft.attempt;
    const epoch = () => view.recovery?.state?.cryptoEpoch ?? null;
    if (!draft.attempt) {
        const choice = e2eePhotoChoiceFromForm();
        const caption = byId<HTMLTextAreaElement>("e2eePhotoCaption").value;
        if (!choice || e2eePhotoSendBlock(view) || new TextEncoder().encode(caption).length > 2000) return;
        draft.attempt = {
            message: { conversationId: draft.chat, messageId: crypto.randomUUID(), file: draft.file, caption, choice },
            epoch: epoch(), remember: choice.mode === "clean" && byId<HTMLInputElement>("e2eePhotoRemember").checked ? e2eePhotoPreferenceKey() : null,
        };
    }
    const attempt = draft.attempt;
    const { conversationId, messageId } = attempt.message;
    draft.busy = true;
    draft.notice = retry ? "Проверяем сохранённую попытку…" : "Готовим фото на этом устройстве…";
    renderE2eeComposer();
    const selected = () => e2eePhotoDraft === draft && e2eeViewCurrent(view);
    try {
        let result = retry ? await api.attachmentInput({ conversationId, messageId }, draft.context) : null;
        if (result && (result.context !== draft.context || result.attachment.messageId !== messageId
            || result.attachment.conversationId !== conversationId)) throw "stale";
        if (!result || result.attachment.kind === "vacant") {
            if (!selected()) return;
            if (retry && (view.pendingAttachments.has(messageId) || view.items.get(messageId)?.outgoing)) {
                draft.notice = "Фото уже было в локальной истории или списке вложений. Обновите историю перед повтором.";
                return;
            }
            const block = e2eePhotoSendBlock(view);
            if (block) { draft.notice = block; return; }
            // The first attempt can discover epoch zero before the UI sees it.
            if (retry && epoch() !== attempt.epoch && !(attempt.epoch === null && epoch() === "0")) {
                draft.notice = "Сессия шифрования изменилась. Закройте попытку и выберите фото заново.";
                return;
            }
            draft.notice = "Готовим фото на этом устройстве…";
            renderE2eePhoto(view);
            result = await api.preparePhoto(attempt.message, draft.context);
        }
        if (result.context !== draft.context || result.attachment.conversationId !== conversationId
            || result.attachment.messageId !== messageId || result.attachment.kind === "vacant") throw "stale";
        if (e2eePhotoDraft !== draft || authExpiredHandled || localStorage.getItem("token") !== token) return;
        const active = e2eeHistoryView;
        const show = active && e2eeViewCurrent(active) && e2eePhotoMatches(active, draft);
        if (attempt.remember && show) {
            try { localStorage.setItem(attempt.remember, JSON.stringify(attempt.message.choice)); } catch { /* The photo is saved even if preferences are unavailable. */ }
        }
        e2eePhotoDraft = null;
        if (show) {
            closeE2eePhotoDialog();
            const attachment = result.attachment;
            e2eePhotoNotice(attachment.kind === "prepared" ? "Фото подготовлено на этом устройстве. Статус доставки появится в истории."
                : attachment.sealedUnsent ? "Фото сохранено. Отправка остановлена."
                : attachment.storedOnServer ? "Фото сохранено на сервере." : "Фото ожидает отправки.");
            if (attachment.kind === "prepared") byId<HTMLDetailsElement>("e2eePendingAttachments").open = true;
            void loadE2eePendingAttachments(active, true);
            void refreshE2eeHistory(active).catch(() => {});
        }
    } catch (error) {
        if (e2eePhotoDraft !== draft) return;
        const rejection = typeof error === "string" ? e2eePhotoRejected.get(error) : undefined;
        // Codec errors precede the local commit. Other failures may hide a commit.
        if (rejection && selected()) {
            draft.attempt = null;
            byId<HTMLInputElement>("e2eePhotoOriginal").checked = false;
            byId<HTMLDetailsElement>("e2eePhotoSettings").open = true;
        }
        draft.notice = rejection && selected() ? rejection : "Не удалось подтвердить сохранение. Проверьте эту попытку перед новой отправкой.";
    } finally {
        draft.busy = false;
        if (e2eeHistoryView && e2eeViewCurrent(e2eeHistoryView)) renderE2eeComposer();
    }
}
byId("e2eePhotoPick").addEventListener("click", () => {
    const view = e2eeHistoryView;
    if (!view || !e2eeViewCurrent(view) || e2eePhotoDraft?.busy || e2eePhotoPickerView) return;
    if (e2eePhotoMatches(view, e2eePhotoDraft) && e2eePhotoDraft.attempt) {
        showE2eePhoto(view, e2eePhotoDraft);
        return;
    }
    if (e2eePhotoSendBlock(view) || typeof window.zchatDesktop?.e2ee?.preparePhoto !== "function") return;
    if (e2eePhotoDraft && !discardE2eePhoto()) return;
    e2eePhotoNotice("");
    e2eePhotoPickerView = view;
    const input = byId<HTMLInputElement>("e2eePhotoFile");
    input.value = "";
    input.click();
});
byId<HTMLInputElement>("e2eePhotoFile").addEventListener("change", event => {
    const view = e2eePhotoPickerView;
    e2eePhotoPickerView = null;
    const input = event.currentTarget as HTMLInputElement;
    const file = input.files?.[0];
    input.value = "";
    if (view && e2eeViewCurrent(view)) renderE2eePhoto(view);
    if (!view || !e2eeViewCurrent(view) || !file || e2eePhotoSendBlock(view)) return;
    if (!file.name || !file.size || file.size > E2EE_PHOTO_INPUT_MAX || new TextEncoder().encode(file.name).length > 255) {
        e2eePhotoNotice("Выберите непустое фото до 20 МБ с именем не длиннее 255 байт.");
        return;
    }
    e2eePhotoDraft = { chat: view.reply.conversation.conversationId, context: view.reply.context, file, attempt: null, busy: false, notice: "" };
    showE2eePhoto(view, e2eePhotoDraft);
});
byId("e2eePhotoFile").addEventListener("cancel", () => {
    e2eePhotoPickerView = null;
    if (e2eeHistoryView) renderE2eePhoto(e2eeHistoryView);
});
function updateE2eePhotoControls(event: Event): void {
    if (!e2eePhotoDraft || e2eePhotoDraft.attempt || !e2eeHistoryView) return;
    if ((event.target as HTMLElement)?.id === "e2eePhotoMode") byId<HTMLInputElement>("e2eePhotoOriginal").checked = false;
    e2eePhotoDraft.notice = "";
    renderE2eePhoto(e2eeHistoryView);
}
byId("e2eePhotoDialog").addEventListener("input", updateE2eePhotoControls);
byId("e2eePhotoDialog").addEventListener("change", updateE2eePhotoControls);
byId("e2eePhotoDialog").addEventListener("cancel", event => { event.preventDefault(); discardE2eePhoto(); });
byId("e2eePhotoCancel").addEventListener("click", discardE2eePhoto);
byId("e2eePhotoSend").addEventListener("click", () => { void submitE2eePhoto(); });
window.addEventListener("pagehide", () => { e2eePhotoDraft = null; leaveE2eePhoto(); });
