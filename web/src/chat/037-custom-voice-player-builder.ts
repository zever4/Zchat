// ---- Custom voice player builder ----
function buildVoicePlayer(url: string): HTMLDivElement {
  const audio = new Audio();
  audio.preload = "metadata";

  const container = document.createElement("div");
  container.className = "voice-player";
  container.appendChild(audio);
  loadProtectedAttachment(url, container, objectURL => { audio.src = objectURL; }, () => {
    audio.pause();
    audio.removeAttribute("src");
    audio.load();
  });

  const playBtn = document.createElement("button");
  playBtn.className = "vp-play-btn";
  playBtn.innerHTML =
    '<svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>';
  const pauseIcon =
    '<svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>';
  const playIcon = playBtn.innerHTML;

  const progressWrap = document.createElement("div");
  progressWrap.className = "vp-progress-wrap";
  const progressBar = document.createElement("div");
  progressBar.className = "vp-progress-bar";
  progressWrap.appendChild(progressBar);

  const timeEl = document.createElement("span");
  timeEl.className = "vp-time";
  timeEl.textContent = "0:00";

  const volWrap = document.createElement("div");
  volWrap.className = "vp-vol-wrap";

  const volBtn = document.createElement("button");
  volBtn.className = "vp-vol-btn";
  volBtn.innerHTML =
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 9v6h4l5 5V4L7 9H3zm13.5 3c0-1.77-1.02-3.29-2.5-4.03v8.05c1.48-.73 2.5-2.25 2.5-4.02z"/></svg>';
  const volMuteIcon =
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M16.5 12c0-1.77-1.02-3.29-2.5-4.03v2.21l2.45 2.45c.03-.2.05-.41.05-.63zm2.5 0c0 .94-.2 1.82-.54 2.64l1.51 1.51C20.63 14.91 21 13.5 21 12c0-4.28-2.99-7.86-7-8.77v2.06c2.89.86 5 3.54 5 6.71zM4.27 3L3 4.27 7.73 9H3v6h4l5 5v-6.73l4.25 4.25c-.67.52-1.42.93-2.25 1.18v2.06c1.38-.31 2.63-.95 3.69-1.81L19.73 21 21 19.73l-9-9L4.27 3zM12 4L9.91 6.09 12 8.18V4z"/></svg>';
  const volOnIcon = volBtn.innerHTML;

  const volSlider = document.createElement("input");
  volSlider.type = "range";
  volSlider.className = "vp-vol-slider";
  volSlider.min = "0";
  volSlider.max = "1";
  volSlider.step = "0.05";
  volSlider.value = "1";

  const volPopup = document.createElement("div");
  volPopup.className = "vp-vol-popup";
  volPopup.appendChild(volSlider);

  volWrap.appendChild(volBtn);
  volWrap.appendChild(volPopup);

  container.appendChild(playBtn);
  container.appendChild(progressWrap);
  container.appendChild(timeEl);
  container.appendChild(volWrap);

  function fmt(seconds: number): string {
    if (!Number.isFinite(seconds) || seconds < 0) return "0:00";
    const minutes = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return minutes + ":" + (secs < 10 ? "0" : "") + secs;
  }

  let isMuted = false;
  let prevVol = 1;

  playBtn.addEventListener("click", () => {
    if (audio.paused) {
      void audio.play().then(() => { playBtn.innerHTML = pauseIcon; }).catch(() => { container.title = "Не удалось воспроизвести запись"; });
    } else {
      audio.pause();
      playBtn.innerHTML = playIcon;
    }
  });

  function updateTime(): void {
    if (audio.error) {
      timeEl.textContent = "Ошибка записи";
      container.title = "Не удалось воспроизвести запись";
      playBtn.innerHTML = playIcon;
      return;
    }
    const duration = audio.duration;
    const knownDuration = Number.isFinite(duration) && duration > 0;
    const progress = knownDuration ? Math.min(100, Math.max(0, audio.currentTime / duration * 100)) : 0;
    progressBar.style.width = progress + "%";
    timeEl.textContent = fmt(audio.currentTime) + " / " + (knownDuration ? fmt(duration) : "—");
  }

  audio.addEventListener("timeupdate", updateTime);
  audio.addEventListener("loadedmetadata", updateTime);
  audio.addEventListener("durationchange", updateTime);
  audio.addEventListener("error", updateTime);

  audio.addEventListener("ended", () => {
    playBtn.innerHTML = playIcon;
    progressBar.style.width = "0%";
  });

  progressWrap.addEventListener("click", (e: MouseEvent) => {
    if (!Number.isFinite(audio.duration) || audio.duration <= 0) return;
    const rect = progressWrap.getBoundingClientRect();
    if (!rect.width) return;
    const pct = Math.min(Math.max((e.clientX - rect.left) / rect.width, 0), 1);
    audio.currentTime = pct * audio.duration;
  });

  volSlider.addEventListener("input", () => {
    audio.volume = Number.parseFloat(volSlider.value);
    isMuted = audio.volume === 0;
    volBtn.innerHTML = isMuted ? volMuteIcon : volOnIcon;
  });

  volBtn.addEventListener("click", () => {
    if (isMuted) {
      audio.volume = prevVol || 1;
      volSlider.value = String(audio.volume);
      isMuted = false;
      volBtn.innerHTML = volOnIcon;
    } else {
      prevVol = audio.volume;
      audio.volume = 0;
      volSlider.value = "0";
      isMuted = true;
      volBtn.innerHTML = volMuteIcon;
    }
  });

  return container;
}
