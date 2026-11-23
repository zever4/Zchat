// ---- Media content detection & rendering in messages ----
(function (): void {
  const mediaTagRegex = /^\[(img|gif|voice)\](.*)\[\/\1\]$/;
  const fileTagRegex = /^\[file:([^:]*):(\d+)\](.*)\[\/file\]$/;

  function formatFileSize(bytes: number): string {
    if (bytes < 1024) return bytes + " Б";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " КБ";
    if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " МБ";
    return (bytes / (1024 * 1024 * 1024)).toFixed(1) + " ГБ";
  }

  const svgFile =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>';
  const svgImg =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>';
  const svgMusic =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/></svg>';
  const svgVideo =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="20" rx="2.18"/><path d="M10 8l6 4-6 4V8z"/></svg>';
  const svgCode =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>';
  const svgZip =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 8v13H3V3h12l6 5z"/><path d="M14 3v5h6"/><rect x="9" y="12" width="6" height="5" rx="1"/><line x1="12" y1="12" x2="12" y2="10"/></svg>';
  const svgGear =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>';
  const svgClip =
    '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg>';

  function getFileIcon(name: string): string {
    const ext = (name.split(".").pop() || "").toLowerCase();
    const map: Record<string, string> = {
      pdf: svgFile,
      doc: svgFile,
      docx: svgFile,
      txt: svgFile,
      rtf: svgFile,
      xls: svgFile,
      xlsx: svgFile,
      csv: svgFile,
      ppt: svgFile,
      pptx: svgFile,
      zip: svgZip,
      rar: svgZip,
      "7z": svgZip,
      tar: svgZip,
      gz: svgZip,
      mp3: svgMusic,
      wav: svgMusic,
      flac: svgMusic,
      ogg: svgMusic,
      aac: svgMusic,
      mp4: svgVideo,
      avi: svgVideo,
      mkv: svgVideo,
      mov: svgVideo,
      webm: svgVideo,
      js: svgCode,
      ts: svgCode,
      py: svgCode,
      go: svgCode,
      java: svgCode,
      c: svgCode,
      cpp: svgCode,
      rs: svgCode,
      html: svgCode,
      css: svgCode,
      json: svgCode,
      xml: svgCode,
      png: svgImg,
      jpg: svgImg,
      jpeg: svgImg,
      gif: svgImg,
      svg: svgImg,
      webp: svgImg,
      exe: svgGear,
      apk: svgGear,
      dmg: svgGear,
      iso: svgGear,
    };
    return map[ext] || svgClip;
  }

  const originalRenderMedia = renderMessage;
  renderMessage = function(msg: ChatMessage, isPrepend = false): void {
    originalRenderMedia(msg, isPrepend);
    if (msg.type === "system") return;

    const lastEl = isPrepend ? messagesEl.firstElementChild : messagesEl.lastElementChild;
    if (!(lastEl instanceof HTMLElement) || !lastEl.classList.contains("msg-wrapper")) return;

    const textEl = lastEl.querySelector<HTMLElement>(".msg-text");
    if (!textEl) return;

    const raw = String(msg.content || "");
    const plainAttachment = (): void => {
      textEl.textContent = raw;
      lastEl.querySelectorAll(".invite-card").forEach(card => card.remove());
    };

    const fileMatch = raw.match(fileTagRegex);
    if (fileMatch) {
      const [, fileName = "", fileSizeRaw = "0", fileUrl = ""] = fileMatch;
      const safeURL = attachmentURL(fileUrl, "file");
      if (!safeURL) { plainAttachment(); return; }
      const fileSize = Number.parseInt(fileSizeRaw, 10) || 0;
      textEl.innerHTML = "";

      const card = document.createElement("a");
      card.href = safeURL;
      card.download = fileName;
      card.className = "file-card";
      let downloading = false;
      card.addEventListener("click", event => {
        event.preventDefault();
        if (downloading) return;
        downloading = true;
        void downloadAttachment(safeURL, fileName)
          .catch(() => { card.title = "Не удалось скачать файл"; })
          .finally(() => { downloading = false; });
      });
      card.innerHTML =
        '<span class="file-card-icon">' +
        getFileIcon(fileName) +
        "</span>" +
        '<div class="file-card-info">' +
        '<span class="file-card-name">' +
        escapeHtml(fileName) +
        "</span>" +
        '<span class="file-card-size">' +
        formatFileSize(fileSize) +
        "</span>" +
        "</div>" +
        '<svg class="file-card-dl" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>';
      textEl.appendChild(card);
      lastEl.dataset.mediaType = "file";
      if (!isPrepend) messagesEl.scrollTop = messagesEl.scrollHeight;
      return;
    }

    const match = raw.match(mediaTagRegex);
    if (!match) return;

    const tag = match[1] as MediaMessageTag;
    const url = attachmentURL(match[2] || "", tag);
    if (!url) { plainAttachment(); return; }

    if (tag === "img" || tag === "gif") {
      textEl.innerHTML = "";
      const img = document.createElement("img");
      if (url.startsWith("/uploads/")) {
        loadProtectedAttachment(url, lastEl, objectURL => { img.src = objectURL; });
      } else {
        img.src = url;
        img.referrerPolicy = "no-referrer";
      }
      img.className = tag === "gif" ? "media-gif" : "media-image";
      img.loading = "lazy";
      img.addEventListener("click", () => {
        if (img.src) window.openLightbox?.(img.src);
      });
      textEl.appendChild(img);
      lastEl.dataset.mediaType = tag;
    } else if (tag === "voice") {
      textEl.innerHTML = "";
      const player = buildVoicePlayer(url);
      textEl.appendChild(player);
      lastEl.dataset.mediaType = "voice";
    }

    if (!isPrepend) messagesEl.scrollTop = messagesEl.scrollHeight;
  };
})();
