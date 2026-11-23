// ---- Image upload and optional desktop preparation ----
interface ImageSendChoice { file: File; asFile: boolean; }
interface ImageSendPreference { version: 1; mode: "original" | "compress"; quality: number; maxDimension: number; }
const IMAGE_ASK_ABOVE = 5 * 1024 * 1024;

function savedImagePreference(): ImageSendPreference | null {
  try {
    const p = JSON.parse(localStorage.getItem(accountStorageKey("zchat_image_send_v1")) || "null") as ImageSendPreference;
    return p?.version === 1 && ["original", "compress"].includes(p.mode) && Number.isInteger(p.quality) &&
      p.quality >= 40 && p.quality <= 95 && p.quality % 5 === 0 && [0, 1600, 2560].includes(p.maxDimension) ? p : null;
  } catch { return null; }
}

function syncImagePreferenceSettings(): void {
  const card = document.getElementById("desktopImageSettings");
  if (!card) return;
  card.hidden = !window.zchatDesktop;
  const p = savedImagePreference();
  document.getElementById("imagePreferenceStatus")!.textContent = p
    ? (p.mode === "original" ? "Оригинал" : "Сжатие: качество " + p.quality + "/100, " + (p.maxDimension ? "до " + p.maxDimension + " px" : "исходные размеры")) +
      ". Для фото до 5 МБ выбор применяется автоматически; для более крупных появится вопрос."
    : "Спрашивать перед каждым фото. Выбор можно запомнить при отправке.";
}

function preparedImageChoice(result: DesktopImageResult): ImageSendChoice {
  if (result.changed && result.file.size > 10 * 1024 * 1024) {
    throw new Error("Результат больше 10 МБ. Уменьшите качество или размеры либо выберите оригинал для отправки файлом.");
  }
  return imageOriginalChoice(result.file, result.mime);
}

function imageOriginalChoice(file: File, mime = file.type): ImageSendChoice {
  return { file, asFile: file.size > 10 * 1024 * 1024 || !["image/jpeg", "image/png", "image/webp", "image/gif"].includes(mime) };
}

function imageByteLabel(size: number): string {
  return size >= 1024 * 1024 ? (size / (1024 * 1024)).toFixed(2) + " МБ" : (size / 1024).toFixed(1) + " КБ";
}

async function chooseImageToSend(file: File, isCurrent: () => boolean): Promise<ImageSendChoice | null> {
  const desktop = window.zchatDesktop;
  if (!desktop) return Promise.resolve({ file, asFile: false });
  if (!isCurrent()) return null;
  const preference = savedImagePreference();
  let preparationError = "";
  if (preference && file.size <= IMAGE_ASK_ABOVE) {
    if (preference.mode === "original") return imageOriginalChoice(file);
    try {
      const result = await desktop.prepareImage(file, preference.quality, preference.maxDimension);
      return isCurrent() ? preparedImageChoice(result) : null;
    } catch (error) {
      if (!isCurrent()) return null;
      preparationError = typeof error === "string" ? error : error instanceof Error ? error.message : "Не удалось обработать изображение";
      // A failed remembered compression never silently uploads the original.
    }
  }
  const dialog = document.getElementById("imageSendDialog") as HTMLDialogElement;
  const mode = document.getElementById("imageSendMode") as HTMLSelectElement;
  const quality = document.getElementById("imageSendQuality") as HTMLInputElement;
  const qualityValue = document.getElementById("imageSendQualityValue")!;
  const dimensions = document.getElementById("imageSendDimensions") as HTMLSelectElement;
  const status = document.getElementById("imageSendStatus")!;
  const preview = document.getElementById("imageSendPreview") as HTMLImageElement;
  const prepare = document.getElementById("imageSendPrepare") as HTMLButtonElement;
  const submit = document.getElementById("imageSendSubmit") as HTMLButtonElement;
  const cancel = document.getElementById("imageSendCancel") as HTMLButtonElement;
  const remember = document.getElementById("imageSendRemember") as HTMLInputElement;
  document.getElementById("imageSendName")!.textContent = file.name;
  document.getElementById("imageSendOriginalSize")!.textContent = "Исходный размер: " + imageByteLabel(file.size);
  mode.value = preference?.mode || "original"; quality.value = String(preference?.quality || 80); dimensions.value = String(preference?.maxDimension || 0);
  remember.checked = false;
  let prepared: ImageSendChoice | null = null;
  let selected: ImageSendChoice | null = null;
  let busy = false, closed = false, previewURL = "";
  const releasePreview = (): void => {
    preview.hidden = true; preview.removeAttribute("src");
    if (previewURL) URL.revokeObjectURL(previewURL);
    previewURL = "";
  };
  const controls = (): void => {
    const original = mode.value === "original";
    mode.disabled = busy; quality.disabled = busy || original; dimensions.disabled = busy || original;
    prepare.hidden = original;
    prepare.disabled = busy || file.size > 20 * 1024 * 1024;
    submit.disabled = busy || !prepared;
    submit.textContent = prepared?.asFile ? "Отправить файл" : "Отправить";
    qualityValue.textContent = quality.value + " / 100";
  };
  const invalidate = (): void => {
    releasePreview();
    prepared = mode.value === "original" ? imageOriginalChoice(file) : null;
    status.textContent = prepared
      ? prepared.asFile ? "Оригинал будет отправлен файлом, без изменений." : "Оригинал будет отправлен без изменений."
      : file.size > 20 * 1024 * 1024 ? "Для сжатия выберите изображение до 20 МБ или отправьте оригинал файлом."
        : "Выберите качество и нажмите «Подготовить», чтобы увидеть результат. Меньше качество — обычно меньше размер.";
    controls();
  };
  return new Promise(resolve => {
    const close = (): void => {
      closed = true;
      releasePreview();
      mode.removeEventListener("change", invalidate); quality.removeEventListener("input", invalidate);
      dimensions.removeEventListener("change", invalidate); prepare.removeEventListener("click", prepareImage);
      cancel.removeEventListener("click", cancelImage); submit.removeEventListener("click", send);
      dialog.removeEventListener("close", close);
      resolve(selected);
    };
    const cancelImage = (): void => { dialog.close(); };
    const send = (): void => {
      if (busy || !prepared) return;
      if (isCurrent()) {
        if (remember.checked) {
          try {
            const p: ImageSendPreference = { version: 1, mode: mode.value as ImageSendPreference["mode"], quality: Number(quality.value), maxDimension: Number(dimensions.value) };
            localStorage.setItem(accountStorageKey("zchat_image_send_v1"), JSON.stringify(p));
            syncImagePreferenceSettings();
          } catch {
            status.textContent = "Не удалось запомнить выбор. Снимите галочку, чтобы отправить без сохранения настройки.";
            return;
          }
        }
        selected = prepared;
      }
      dialog.close();
    };
    const prepareImage = async (): Promise<void> => {
      if (busy || !isCurrent()) { if (!isCurrent()) dialog.close(); return; }
      busy = true; prepared = null; releasePreview(); controls(); status.textContent = "Обработка изображения…";
      try {
        const result = await desktop.prepareImage(file, Number(quality.value), Number(dimensions.value));
        if (closed) return;
        if (!isCurrent()) { dialog.close(); return; }
        prepared = preparedImageChoice(result);
        const notes: Record<string, string> = {
          compressed: "Изображение подготовлено.",
          original_smaller: "Оригинал меньше результата сжатия. Будет отправлен оригинал без изменения размеров и качества.",
          animation: "Анимация сохранена: будет отправлен исходный файл без изменения качества и размеров.",
          multipage: "Все страницы TIFF сохранены: оригинал будет отправлен файлом.",
        };
        const saving = Math.round((1 - result.file.size / Math.max(file.size, 1)) * 100);
        status.textContent = notes[result.reason] + " " + imageByteLabel(result.file.size) +
          (saving > 0 ? " (меньше на " + saving + "%)" : "") + ", " + result.width + " × " + result.height +
          (prepared.asFile ? ". Отправка файлом." : ".");
        if (!prepared.asFile && result.reason !== "animation") {
          previewURL = URL.createObjectURL(result.file); preview.src = previewURL; preview.hidden = false;
        }
      } catch (error) {
        if (!closed) status.textContent = typeof error === "string" ? error : error instanceof Error ? error.message : "Не удалось обработать изображение. Можно выбрать оригинал.";
      } finally {
        if (!closed) { busy = false; controls(); }
      }
    };
    mode.addEventListener("change", invalidate); quality.addEventListener("input", invalidate);
    dimensions.addEventListener("change", invalidate); prepare.addEventListener("click", prepareImage);
    cancel.addEventListener("click", cancelImage); submit.addEventListener("click", send);
    dialog.addEventListener("close", close); invalidate();
    if (preparationError) status.textContent = preparationError + ". Повторите подготовку или выберите оригинал.";
    dialog.showModal();
  });
}

(function (): void {
  syncImagePreferenceSettings();
  if (window.zchatDesktop) document.getElementById("resetImagePreference")?.addEventListener("click", () => {
    try {
      localStorage.removeItem(accountStorageKey("zchat_image_send_v1"));
      syncImagePreferenceSettings();
    } catch { document.getElementById("imagePreferenceStatus")!.textContent = "Не удалось сбросить выбор. Повторите попытку."; }
  });
  const attachBtn = optionalById<HTMLButtonElement>("attachBtn");
  if (!attachBtn) return;
  const fileInput = document.createElement("input");
  fileInput.type = "file";
  fileInput.accept = "image/jpeg,image/png,image/gif,image/webp" + (window.zchatDesktop ? ",image/bmp,image/tiff,.bmp,.tif,.tiff" : "");
  fileInput.style.display = "none"; document.body.appendChild(fileInput);
  let busy = false;
  attachBtn.addEventListener("click", () => { if (!busy) fileInput.click(); });
  fileInput.addEventListener("change", async () => {
    const file = fileInput.files?.[0]; fileInput.value = "";
    if (!file || busy) return;
    const max = (window.zchatDesktop ? 50 : 10) * 1024 * 1024;
    if (file.size > max) { alert(window.zchatDesktop ? "Файл слишком большой (макс. 50 МБ)" : "Файл слишком большой (макс. 10 МБ)"); return; }
    const uploadChatID = currentChatID, uploadToken = token;
    if (!uploadChatID) { alert("Сначала выберите чат"); return; }
    const isCurrent = (): boolean => !authExpiredHandled && localStorage.getItem("token") === uploadToken && currentChatID === uploadChatID;
    busy = true; attachBtn.disabled = true;
    try {
      const choice = await chooseImageToSend(file, isCurrent);
      if (!choice || !isCurrent()) return;
      const fd = new FormData(); fd.append("chat_id", String(uploadChatID)); fd.append(choice.asFile ? "file" : "media", choice.file);
      const res = await sessionFetch(choice.asFile ? "/api/files/upload" : "/api/media/upload", {
        method: "POST", headers: { Authorization: "Bearer " + uploadToken }, body: fd,
      });
      if (!isCurrent()) return;
      if (!res.ok) { alert("Ошибка загрузки: " + await res.text()); return; }
      const data = await res.json() as { url?: string; name?: string; size?: number | string };
      if (!isCurrent()) return;
      if (choice.asFile) sendFileMessage(data, uploadChatID);
      else sendMediaMessage("img", String(data.url || ""), uploadChatID);
    } catch (error) { if (isCurrent()) alert(error instanceof Error ? error.message : "Ошибка отправки изображения"); }
    finally { busy = false; attachBtn.disabled = false; }
  });
})();
