// ---- Voice Messages (MediaRecorder) ----
function preferredVoiceMime(opusOnly = false): string | undefined {
  if (typeof MediaRecorder === "undefined") return undefined;
  return ["audio/ogg;codecs=opus", "audio/webm;codecs=opus", ...(opusOnly ? [] : ["audio/ogg", "audio/webm"])]
    .find(type => MediaRecorder.isTypeSupported(type));
}

function voiceMicrophoneError(error: unknown): string {
  const denied = error instanceof DOMException && error.name === "NotAllowedError";
  return denied && window.zchatDesktop
    ? "Доступ к микрофону запрещён. В настройках чата откройте «Микрофон» → «Сбросить и перезагрузить» и разрешите доступ при следующей записи. Также проверьте доступ в настройках конфиденциальности Windows."
    : "Не удалось получить доступ к микрофону";
}

(function (): void {
  const voiceBtn = optionalById<HTMLButtonElement>("voiceBtn");
  if (!voiceBtn) return;

  let mediaRecorder: MediaRecorder | null = null;
  let isRecording = false;

  voiceBtn.addEventListener("click", async () => {
    if (isRecording) {
      stopRecording();
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      startRecording(stream);
    } catch (error) {
      alert(voiceMicrophoneError(error));
    }
  });

  function startRecording(stream: MediaStream): void {
    const recordingChatID = currentChatID;
    if (!recordingChatID) { stream.getTracks().forEach(track => track.stop()); return; }
    // Firefox's MediaRecorder WebM fails to open in some Chromium decoders.
    // Prefer Ogg/Opus when recording is supported; Chromium falls back to WebM.
    const mimeType = preferredVoiceMime();
    if (!mimeType) {
      stream.getTracks().forEach(track => track.stop());
      alert("Этот браузер не поддерживает запись голосовых сообщений");
      return;
    }

    let recorder: MediaRecorder;
    try {
      recorder = new MediaRecorder(stream, { mimeType });
    } catch {
      stream.getTracks().forEach(track => track.stop());
      alert("Не удалось начать запись голосового сообщения");
      return;
    }
    mediaRecorder = recorder;
    const audioChunks: Blob[] = [];
    isRecording = true;
    voiceBtn.classList.add("recording");
    voiceBtn.title = "Остановить запись";
    recorder.ondataavailable = (e: BlobEvent) => {
      if (e.data.size > 0) audioChunks.push(e.data);
    };
    recorder.onstop = async () => {
      stream.getTracks().forEach((track) => track.stop());
      isRecording = false;
      voiceBtn.classList.remove("recording");
      voiceBtn.title = "Голосовое сообщение";

      if (audioChunks.length === 0) return;
      const blob = new Blob(audioChunks, { type: recorder.mimeType || mimeType });
      if (blob.size > 10 * 1024 * 1024) {
        alert("Голосовое сообщение слишком длинное");
        return;
      }
      await uploadVoice(blob, recordingChatID);
    };
    try {
      recorder.start();
    } catch {
      stream.getTracks().forEach(track => track.stop());
      isRecording = false;
      mediaRecorder = null;
      voiceBtn.classList.remove("recording");
      voiceBtn.title = "Голосовое сообщение";
      alert("Не удалось начать запись голосового сообщения");
    }
  }

  function stopRecording(): void {
    if (mediaRecorder && mediaRecorder.state !== "inactive") {
      mediaRecorder.stop();
    }
  }

  async function uploadVoice(blob: Blob, chatID: number): Promise<void> {
    const fd = new FormData();
    fd.append("chat_id", String(chatID));
    fd.append("media", blob, blob.type.startsWith("audio/ogg") ? "voice.ogg" : "voice.webm");

    try {
      const res = await sessionFetch("/api/media/upload", {
        method: "POST",
        headers: { Authorization: "Bearer " + token },
        body: fd,
      });
      if (!res.ok) {
        alert("Ошибка загрузки голосового");
        return;
      }
      const data = await res.json() as { url?: string };
      sendMediaMessage("voice", String(data.url || ""), chatID);
    } catch {
      alert("Ошибка сети при загрузке голосового");
    }
  }
})();
