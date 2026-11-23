interface E2eeVoiceCapture {
    view: E2eeHistoryView;
    recorder: MediaRecorder | null;
    stream: MediaStream | null;
    chunks: Blob[];
    size: number;
    stopping: boolean;
}

const E2EE_VOICE_BYTES = 10 * 1024 * 1024;
let e2eeVoiceCapture: E2eeVoiceCapture | null = null;
let e2eeVoiceOpening = false;

function e2eeVoiceBusy(): boolean { return e2eeVoiceCapture !== null || e2eeVoiceOpening; }

function leaveE2eeVoice(): void {
    const capture = e2eeVoiceCapture;
    if (!capture) return;
    e2eeVoiceCapture = null;
    if (capture.recorder) {
        capture.recorder.ondataavailable = capture.recorder.onstop = capture.recorder.onerror = null;
        if (capture.recorder.state !== "inactive") {
            try { capture.recorder.stop(); } catch { /* Tracks are released below. */ }
        }
    }
    capture.stream?.getTracks().forEach(track => track.stop());
    capture.chunks.length = 0;
}

function renderE2eeVoice(view: E2eeHistoryView): void {
    const button = byId<HTMLButtonElement>("e2eeVoiceRecord");
    const cancel = byId<HTMLButtonElement>("e2eeVoiceCancel");
    const capture = e2eeVoiceCapture;
    const api = window.zchatDesktop?.e2ee;
    button.hidden = !api?.compiled || typeof api.prepareVoice !== "function";
    button.textContent = capture?.recorder ? "Остановить запись" : capture ? "Запрашиваем микрофон…" : "Голосовое";
    button.disabled = button.hidden || (capture ? !capture.recorder || capture.stopping
        : e2eeVoiceOpening || !!e2eeFileSendBlock(view) || !!e2eeFileDraft?.controller || !!e2eeFilePicker);
    cancel.hidden = !capture;
}

function finishE2eeVoice(capture: E2eeVoiceCapture, message: string): void {
    if (e2eeVoiceCapture !== capture) return;
    leaveE2eeVoice();
    if (e2eeViewCurrent(capture.view)) {
        e2eeFileNotice(message);
        renderE2eeComposer();
    }
}

async function toggleE2eeVoice(): Promise<void> {
    const view = e2eeHistoryView;
    if (!view || !e2eeViewCurrent(view)) return;
    const active = e2eeVoiceCapture;
    if (active) {
        if (view !== active.view || !active.recorder || active.stopping) return;
        active.stopping = true;
        try { active.recorder.stop(); } catch { finishE2eeVoice(active, "Не удалось завершить запись."); }
        renderE2eeComposer();
        return;
    }
    if (e2eeVoiceOpening || e2eeVoiceRecording() || e2eeFileSendBlock(view) || typeof window.zchatDesktop?.e2ee?.prepareVoice !== "function") return;
    if (e2eeFileDraft && !discardE2eeFile()) return;
    const mime = preferredVoiceMime(true);
    if (!mime || !navigator.mediaDevices?.getUserMedia) { e2eeFileNotice("Запись Opus в Ogg или WebM недоступна."); return; }
    const capture: E2eeVoiceCapture = { view, recorder: null, stream: null, chunks: [], size: 0, stopping: false };
    e2eeVoiceCapture = capture;
    e2eeVoiceOpening = true;
    e2eeFileNotice("");
    renderE2eeComposer();
    let stream: MediaStream;
    try {
        stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    } catch (error) {
        finishE2eeVoice(capture, voiceMicrophoneError(error));
        return;
    } finally {
        e2eeVoiceOpening = false;
        if (e2eeHistoryView && e2eeViewCurrent(e2eeHistoryView)) renderE2eeComposer();
    }
    if (e2eeVoiceCapture !== capture || !e2eeViewCurrent(view) || e2eeFileSendBlock(view)) {
        stream.getTracks().forEach(track => track.stop());
        finishE2eeVoice(capture, "Запись отменена.");
        return;
    }
    capture.stream = stream;
    try {
        const recorder = new MediaRecorder(stream, { mimeType: mime });
        capture.recorder = recorder;
        recorder.ondataavailable = event => {
            if (e2eeVoiceCapture !== capture || !event.data.size) return;
            capture.size += event.data.size;
            if (capture.size > E2EE_VOICE_BYTES) {
                finishE2eeVoice(capture, "Запись превысила 10 МБ. Запишите более короткое сообщение.");
                return;
            }
            capture.chunks.push(event.data);
        };
        recorder.onerror = () => finishE2eeVoice(capture, "Не удалось записать голосовое сообщение.");
        recorder.onstop = () => {
            if (e2eeVoiceCapture !== capture) return;
            if (!capture.size || !e2eeViewCurrent(view) || e2eeFileSendBlock(view)) { finishE2eeVoice(capture, "Запись отменена."); return; }
            const type = (recorder.mimeType || mime).split(";")[0];
            if (type !== "audio/ogg" && type !== "audio/webm") { finishE2eeVoice(capture, "Формат записи не поддерживается."); return; }
            const file = new File(capture.chunks, type === "audio/ogg" ? "voice.ogg" : "voice.webm", { type });
            finishE2eeVoice(capture, "");
            e2eeFileDraft = { kind: "voice", context: view.reply.context, chat: view.reply.conversation.conversationId,
                file, attempt: null, controller: null, notice: "" };
            showE2eeFile(view, e2eeFileDraft);
        };
        recorder.start(1000);
        e2eeFileNotice("Идёт запись голосового сообщения…");
        renderE2eeComposer();
    } catch {
        finishE2eeVoice(capture, "Не удалось начать запись голосового сообщения.");
    }
}

byId("e2eeVoiceRecord").addEventListener("click", () => { void toggleE2eeVoice(); });
byId("e2eeVoiceCancel").addEventListener("click", () => {
    if (e2eeVoiceCapture) finishE2eeVoice(e2eeVoiceCapture, "Запись отменена.");
});
