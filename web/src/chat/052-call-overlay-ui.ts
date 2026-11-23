// --- Call overlay UI ---
type CallParticipantProfile = {
  identity: string;
  displayName: string;
  avatarURL: string;
};

function isRadioMicrophoneMode(): boolean {
  return String(userSettings.microphone_mode || "toggle") === "radio";
}

function playCallRadioTone(isPressed: boolean): void {
  try {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) return;
    callAudioCtx = callAudioCtx || new AudioCtx();
    if (callAudioCtx.state === "suspended") {
      callAudioCtx.resume().catch(() => {});
    }

    const now = callAudioCtx.currentTime;
    const osc = callAudioCtx.createOscillator();
    const gain = callAudioCtx.createGain();
    osc.type = "triangle";
    osc.frequency.setValueAtTime(isPressed ? 780 : 620, now);
    osc.frequency.exponentialRampToValueAtTime(isPressed ? 680 : 520, now + 0.07);
    gain.gain.setValueAtTime(0.0001, now);
    gain.gain.exponentialRampToValueAtTime(0.011, now + 0.01);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.08);
    osc.connect(gain);
    gain.connect(callAudioCtx.destination);
    osc.start(now);
    osc.stop(now + 0.085);
  } catch (e) {
    console.debug("call radio tone unavailable:", e);
  }
}

async function setCallMicEnabled(
  enabled: boolean,
  options?: { playTone?: boolean; force?: boolean },
): Promise<void> {
  if (!callRoom || !callRoom.localParticipant) return;
  const nextEnabled = !!enabled;
  const opts = options || {};
  const shouldPlayTone = opts.playTone === true;
  const changed = localTracks.mic !== nextEnabled;
  const room = callRoom;

  localTracks.mic = nextEnabled;
  if (changed || opts.force === true) {
    try {
      await room.localParticipant.setMicrophoneEnabled(nextEnabled);
    } catch {
      if (callRoom !== room) return;
      if (!nextEnabled) {
        endCall();
        renderSystemMessage("Не удалось отключить микрофон. Звонок завершён.");
        return;
      }
      callMicHoldActive = false;
      syncCallMicrophoneState();
      updateCallControlButtons();
      updateCallParticipantUI(room.localParticipant, true);
      renderSystemMessage("Не удалось включить микрофон. Проверьте устройство и разрешение браузера, затем повторите попытку.");
      return;
    }
    if (callRoom !== room) {
      await room.disconnect();
      return;
    }
  }
  syncCallMicrophoneState();
  if (shouldPlayTone && changed) {
    playCallRadioTone(nextEnabled);
  }
  updateCallControlButtons();
  updateCallParticipantUI(callRoom.localParticipant, true);
}

function syncCallMicrophoneState(): void {
  if (!callRoom) return;
  const publication = callRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Microphone);
  localTracks.mic = !!publication && !publication.isMuted && publication.track?.mediaStreamTrack.readyState === "live";
}

async function startCallRadioTransmission(): Promise<void> {
  if (!isRadioMicrophoneMode() || !callRoom || callMicHoldActive) return;
  callMicHoldActive = true;
  await setCallMicEnabled(true, { playTone: true });
}

async function stopCallRadioTransmission(): Promise<void> {
  if (!callRoom) return;
  if (callMicPointerActive || callMicKeyboardActive) return;
  const wasHolding = callMicHoldActive;
  callMicHoldActive = false;
  if (!isRadioMicrophoneMode()) return;
  if (!wasHolding && !localTracks.mic) return;
  await setCallMicEnabled(false, { playTone: wasHolding || localTracks.mic });
}

function applyCallMicrophoneModeRuntime(): void {
  updateCallControlButtons();
  if (callState === "in_call" && callRoom && isRadioMicrophoneMode() && !callMicHoldActive && localTracks.mic) {
    void setCallMicEnabled(false);
  }
}

function getCallScreenShareDraft(): { screen_share_resolution: ScreenShareResolution; screen_share_fps: number } {
  return {
    screen_share_resolution: normalizeScreenShareResolution(callScreenShareDraft.resolution),
    screen_share_fps: normalizeScreenShareFPS(callScreenShareDraft.fps),
  };
}

function loadCallScreenShareDraftFromSettings(): void {
  const preset = getScreenSharePreset(userSettings);
  callScreenShareDraft = {
    resolution: preset.resolution,
    fps: preset.fps,
  };
}

function updateCallDockToggleButton(): void {
  const toggleBtn = optionalById<HTMLButtonElement>("callOverlayToggle");
  const toggleLabel = optionalById<HTMLElement>("callOverlayToggleLabel");
  if (toggleBtn) {
    toggleBtn.title = callDockExpanded ? "Свернуть панель звонка" : "Показать участников и предпросмотр";
  }
  if (toggleLabel) {
    toggleLabel.textContent = callDockExpanded ? "Свернуть" : "Панель";
  }
}

function setCallDockExpanded(expanded: boolean): void {
  callDockExpanded = !!expanded;
  const overlay = optionalById<HTMLDivElement>("callOverlay");
  if (overlay) {
    overlay.classList.toggle("expanded", callDockExpanded);
  }
  if (!callDockExpanded) {
    hideCallParticipantMenu();
  }
  updateCallDockToggleButton();
}

function toggleCallDockDetails(): void {
  if (!callRoom || callState !== "in_call") return;
  setCallDockExpanded(!callDockExpanded);
}

function applyCallParticipantSpeakingState(identity: unknown, isLocal: boolean): void {
  const key = String(identity || "");
  const speaking = callSpeakingIdentities.has(key);
  const tile = optionalById<HTMLElement>(getCallParticipantTileID(key, isLocal));
  const listItem = optionalById<HTMLElement>(getCallParticipantListID(key, isLocal));
  if (tile) tile.classList.toggle("is-speaking", speaking);
  if (listItem) listItem.classList.toggle("is-speaking", speaking);
}

function setCallActiveSpeakers(participants: any[]): void {
  const next = new Set<string>();
  if (Array.isArray(participants)) {
    participants.forEach((participant) => {
      if (!participant) return;
      const isLocal = isLocalCallParticipant(participant);
      next.add(getCallParticipantIdentity(participant, isLocal));
    });
  }
  callSpeakingIdentities = next;
  document
    .querySelectorAll(".participant-tile.is-speaking, .call-participant-item.is-speaking")
    .forEach((node) => {
      node.classList.remove("is-speaking");
    });
  if (!callRoom) return;
  if (callRoom.localParticipant) {
    applyCallParticipantSpeakingState(getCallParticipantIdentity(callRoom.localParticipant, true), true);
    updateCallParticipantUI(callRoom.localParticipant, true);
  }
  callRoom.remoteParticipants.forEach((participant: any) => {
    const identity = getCallParticipantIdentity(participant, false);
    applyCallParticipantSpeakingState(identity, false);
    updateCallParticipantUI(participant, false, identity);
  });
}

function isCallParticipantSpeaking(identity: unknown): boolean {
  return callSpeakingIdentities.has(String(identity || ""));
}

function getCallParticipantIdentity(participant: any, isLocal: boolean): string {
  if (isLocal) {
    if (callRoom && callRoom.localParticipant && callRoom.localParticipant.identity) {
      return String(callRoom.localParticipant.identity);
    }
    return "local";
  }
  return String((participant && participant.identity) || "unknown");
}

function isLocalCallParticipant(participant: any): boolean {
  if (!participant || !callRoom || !callRoom.localParticipant) return false;
  return String(participant.identity || "") === String(callRoom.localParticipant.identity || "");
}

function getCallParticipantDomID(prefix: string, identity: unknown): string {
  return prefix + String(identity || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
}

function getCallParticipantTileID(identity: unknown, isLocal: boolean): string {
  return isLocal ? "participant-local" : getCallParticipantDomID("participant-", identity);
}

function getCallParticipantListID(identity: unknown, isLocal: boolean): string {
  return isLocal ? "call-participant-local" : getCallParticipantDomID("call-participant-", identity);
}

function parseCallParticipantMetadata(participant: any): Record<string, any> {
  if (!participant || !participant.metadata) return {};
  try {
    return JSON.parse(participant.metadata) as Record<string, any>;
  } catch {
    return {};
  }
}

function formatFallbackCallParticipantName(identity: unknown): string {
  const raw = String(identity || "").trim().split(":")[0];
  if (!raw) return "Участник";
  if (/^\d+$/.test(raw)) return "Участник " + raw;
  return raw;
}

function getCallParticipantProfile(
  participant: any,
  isLocal: boolean,
  identityOverride?: string,
): CallParticipantProfile {
  const identity = identityOverride || getCallParticipantIdentity(participant, isLocal);
  const metadata = parseCallParticipantMetadata(participant);
  const directoryEntry = callParticipantDirectory[identity.split(":")[0]] || {};
  const displayName = isLocal
    ? currentUsername
    : String(
        (participant && participant.name) ||
          directoryEntry.username ||
          metadata.username ||
          formatFallbackCallParticipantName(identity),
      );
  const avatarURL = isLocal
    ? currentAvatarURL
    : String(metadata.avatar_url || directoryEntry.avatar_url || "");

  return {
    identity: identity,
    displayName: displayName,
    avatarURL: avatarURL,
  };
}

async function loadCallParticipantDirectory(chatID: number | string | null | undefined): Promise<void> {
  const attempt = callAttempt;
  callParticipantDirectory = {};
  if (!chatID) return;
  try {
    const res = await apiFetch("/api/chats/" + chatID + "/members");
    if (!res.ok) return;
    const members = await res.json() as ChatMemberListItem[];
    if (attempt !== callAttempt) return;
    if (!Array.isArray(members)) return;
    members.forEach((member) => {
      if (!member || typeof member.id === "undefined" || member.id === null) return;
      callParticipantDirectory[String(member.id)] = {
        username: member.username || "",
        avatar_url: member.avatar_url || "",
      };
    });
  } catch (e) {
    console.error("Failed to load call participant directory:", e);
  }
}

function setCallAvatarContent(node: HTMLElement | null, name: string, avatarURL: string): void {
  if (!node) return;
  if (avatarURL) {
    node.textContent = "";
    node.style.backgroundImage = "url(" + displayAssetURL(avatarURL) + ")";
  } else {
    node.style.backgroundImage = "";
    node.textContent = String(name || "?").charAt(0).toUpperCase();
  }
}

function createCallParticipantPlaceholder(name: string, avatarURL: string): HTMLDivElement {
  const placeholder = document.createElement("div");
  placeholder.className = "participant-avatar-placeholder";
  setCallAvatarContent(placeholder, name, avatarURL);
  return placeholder;
}

function getCallParticipantVolume(identity: unknown): number {
  const key = String(identity || "");
  if (Object.prototype.hasOwnProperty.call(callParticipantVolumes, key)) {
    return callParticipantVolumes[key];
  }
  const participant = getRemoteCallParticipant(key);
  if (participant && typeof participant.getVolume === "function") {
    try {
      const volume = participant.getVolume();
      if (typeof volume === "number" && !Number.isNaN(volume)) {
        callParticipantVolumes[key] = volume;
        return volume;
      }
    } catch {
      // ignore and use fallback
    }
  }
  return 1;
}

function getRemoteCallParticipant(identity: unknown): any | null {
  if (!callRoom || !callRoom.remoteParticipants) return null;
  return callRoom.remoteParticipants.get(String(identity || "")) || null;
}

function registerCallParticipantAudioElement(identity: unknown, element: HTMLAudioElement): void {
  const key = String(identity || "");
  if (!callParticipantAudioElements[key]) {
    callParticipantAudioElements[key] = [];
  }
  if (!callParticipantAudioElements[key].includes(element)) {
    callParticipantAudioElements[key].push(element);
  }
  element.volume = getCallParticipantVolume(key);
}

function unregisterCallParticipantAudioElement(identity: unknown, element: HTMLAudioElement): void {
  const key = String(identity || "");
  if (!callParticipantAudioElements[key]) return;
  callParticipantAudioElements[key] = callParticipantAudioElements[key].filter((item) => item !== element);
  if (callParticipantAudioElements[key].length === 0) {
    delete callParticipantAudioElements[key];
  }
}

function applyCallParticipantVolume(identity: unknown): void {
  const key = String(identity || "");
  const volume = getCallParticipantVolume(key);
  const audioElements = callParticipantAudioElements[key] || [];
  audioElements.forEach((element) => {
    element.volume = volume;
  });
  const participant = getRemoteCallParticipant(key);
  if (participant && typeof participant.setVolume === "function") {
    try {
      participant.setVolume(volume);
    } catch {
      // ignore and keep DOM volume applied
    }
  }
}

function setCallParticipantVolume(identity: unknown, nextVolume: number): void {
  const key = String(identity || "");
  const volume = Math.max(0, Math.min(1, Number(nextVolume) || 0));
  callParticipantVolumes[key] = volume;
  applyCallParticipantVolume(key);
  const participant = getRemoteCallParticipant(key);
  if (participant) {
    updateCallParticipantUI(participant, false, key);
  } else if (activeCallParticipantMenuIdentity === key) {
    hideCallParticipantMenu();
  }

  const valueEl = optionalById<HTMLElement>("callParticipantVolumeValue");
  const slider = optionalById<HTMLInputElement>("callParticipantVolumeSlider");
  if (valueEl && activeCallParticipantMenuIdentity === key) {
    valueEl.textContent = Math.round(volume * 100) + "%";
  }
  if (slider && activeCallParticipantMenuIdentity === key) {
    slider.value = String(Math.round(volume * 100));
  }
}

function clearCallParticipantContextTarget(): void {
  document
    .querySelectorAll(".participant-tile.is-context-target, .call-participant-item.is-context-target")
    .forEach((node) => {
      node.classList.remove("is-context-target");
    });
}

function highlightCallParticipantTarget(identity: unknown): void {
  clearCallParticipantContextTarget();
  const key = String(identity || "");
  const tile = optionalById<HTMLElement>(getCallParticipantTileID(key, false));
  const listItem = optionalById<HTMLElement>(getCallParticipantListID(key, false));
  if (tile) tile.classList.add("is-context-target");
  if (listItem) listItem.classList.add("is-context-target");
}

function hideCallParticipantMenu(): void {
  const menu = optionalById<HTMLElement>("callParticipantMenu");
  if (menu) menu.style.display = "none";
  activeCallParticipantMenuIdentity = null;
  clearCallParticipantContextTarget();
}

function showCallParticipantMenu(x: number, y: number, identity: unknown): void {
  const key = String(identity || "");
  const participant = getRemoteCallParticipant(key);
  const menu = optionalById<HTMLDivElement>("callParticipantMenu");
  const slider = optionalById<HTMLInputElement>("callParticipantVolumeSlider");
  const valueEl = optionalById<HTMLElement>("callParticipantVolumeValue");
  const titleEl = optionalById<HTMLElement>("callParticipantMenuName");
  if (!participant || !menu || !slider || !valueEl || !titleEl) return;

  const profile = getCallParticipantProfile(participant, false);
  const volumeValue = Math.round(getCallParticipantVolume(key) * 100);

  activeCallParticipantMenuIdentity = key;
  titleEl.textContent = profile.displayName;
  valueEl.textContent = volumeValue + "%";
  slider.value = String(volumeValue);
  menu.style.left = x + "px";
  menu.style.top = y + "px";
  menu.style.display = "block";
  highlightCallParticipantTarget(key);

  requestAnimationFrame(() => {
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth) menu.style.left = Math.max(8, x - rect.width) + "px";
    if (rect.bottom > window.innerHeight) menu.style.top = Math.max(8, y - rect.height) + "px";
  });
}

function attachCallParticipantContextMenu(node: HTMLElement | null, identity: string): void {
  if (!node || node.dataset.callContextBound === "1") return;
  node.dataset.callContextBound = "1";
  node.addEventListener("contextmenu", (e: MouseEvent) => {
    if (!getRemoteCallParticipant(identity)) return;
    e.preventDefault();
    e.stopPropagation();
    showCallParticipantMenu(e.clientX, e.clientY, identity);
  });
}

function getCallParticipantMicEnabled(participant: any, isLocal: boolean): boolean {
  if (isLocal) return !!localTracks.mic;
  if (participant && typeof participant.isMicrophoneEnabled === "boolean") {
    return participant.isMicrophoneEnabled;
  }
  if (participant && participant.getTrackPublication && LivekitClient && LivekitClient.Track && LivekitClient.Track.Source) {
    try {
      const publication = participant.getTrackPublication(LivekitClient.Track.Source.Microphone);
      if (publication) {
        if (typeof publication.isMuted === "boolean") return !publication.isMuted;
        if (typeof publication.muted === "boolean") return !publication.muted;
      }
    } catch {
      // ignore
    }
  }
  return true;
}

function buildCallParticipantStatusText(identity: string, participant: any, isLocal: boolean): string {
  const speaking = isCallParticipantSpeaking(identity);
  if (isLocal) {
    if (isRadioMicrophoneMode()) {
      if (speaking) return "Рация активна · вы говорите";
      return localTracks.mic ? "Рация активна · говорите" : "Рация · удерживайте кнопку";
    }
    if (speaking) {
      return "Вы в звонке · говорите";
    }
    return "Вы в звонке · " + (localTracks.mic ? "микрофон включен" : "микрофон выключен");
  }
  const micText = getCallParticipantMicEnabled(participant, isLocal) ? "Микрофон включен" : "Микрофон выключен";
  if (speaking) {
    return "Говорит · громкость " + Math.round(getCallParticipantVolume(identity) * 100) + "%";
  }
  return micText + " · громкость " + Math.round(getCallParticipantVolume(identity) * 100) + "%";
}

function ensureCallParticipantTile(
  participant: any,
  isLocal: boolean,
  identityOverride?: string,
): HTMLDivElement | null {
  const identity = identityOverride || getCallParticipantIdentity(participant, isLocal);
  const grid = optionalById<HTMLDivElement>("callVideoGrid");
  if (!grid) return null;

  let tile = optionalById<HTMLDivElement>(getCallParticipantTileID(identity, isLocal));
  if (!tile) {
    tile = document.createElement("div");
    tile.className = "participant-tile" + (isLocal ? " is-local" : " is-remote");
    tile.id = getCallParticipantTileID(identity, isLocal);
    tile.dataset.identity = identity;

    const stateBadge = document.createElement("div");
    stateBadge.className = "participant-state-badge";
    tile.appendChild(stateBadge);

    const label = document.createElement("div");
    label.className = "participant-name-label";
    tile.appendChild(label);

    const expand = document.createElement("button");
    expand.type = "button";
    expand.className = "call-video-fullscreen";
    expand.textContent = "На весь экран";
    expand.hidden = true;
    expand.addEventListener("click", (event: MouseEvent) => {
      event.stopPropagation();
      void toggleCallVideoFullscreen(tile!);
    });
    tile.appendChild(expand);
    tile.addEventListener("dblclick", (event: MouseEvent) => {
      if ((event.target as HTMLElement).closest("button") || !tile!.querySelector("video")) return;
      void toggleCallVideoFullscreen(tile!);
    });

    grid.appendChild(tile);
    if (!isLocal) attachCallParticipantContextMenu(tile, identity);
  }

  return tile;
}

async function toggleCallVideoFullscreen(tile: HTMLElement): Promise<void> {
  if (!tile.querySelector("video")) return;
  try {
    if (document.fullscreenElement === tile) await document.exitFullscreen();
    else await tile.requestFullscreen();
  } catch {
    renderSystemMessage("Браузер не разрешил полноэкранный режим. Попробуйте ещё раз кнопкой на видео.");
  }
}

function syncCallVideoControls(tile: HTMLElement): void {
  const hasVideo = !!tile.querySelector("video");
  if (!hasVideo && document.fullscreenElement === tile) {
    void document.exitFullscreen().catch(() => {});
  }
  const button = tile.querySelector<HTMLButtonElement>(".call-video-fullscreen");
  if (!button) return;
  button.hidden = !hasVideo;
  button.textContent = document.fullscreenElement === tile ? "Выйти из полного экрана" : "На весь экран";
  button.setAttribute("aria-label", button.textContent);
}

document.addEventListener("fullscreenchange", () => {
  document.querySelectorAll<HTMLElement>(".participant-tile").forEach(syncCallVideoControls);
});

function detachCallParticipantTracks(participant: any, kind?: "video"): void {
  if (!participant?.trackPublications) return;
  const identity = String(participant.identity || "");
  participant.trackPublications.forEach((publication: any) => {
    const track = publication.track;
    if (!track || (kind && track.kind !== kind)) return;
    (track.detach() as HTMLMediaElement[]).forEach((element) => {
      if (track.kind === "audio") unregisterCallParticipantAudioElement(identity, element as HTMLAudioElement);
      element.pause();
      element.srcObject = null;
      element.remove();
    });
  });
}

function detachCallMedia(room: any): void {
  if (!room) return;
  detachCallParticipantTracks(room.localParticipant);
  room.remoteParticipants.forEach((participant: any) => detachCallParticipantTracks(participant));
  callParticipantAudioElements = {};
}

function ensureCallParticipantListItem(
  participant: any,
  isLocal: boolean,
  identityOverride?: string,
): HTMLDivElement | null {
  const identity = identityOverride || getCallParticipantIdentity(participant, isLocal);
  const list = optionalById<HTMLDivElement>("callParticipantsList");
  if (!list) return null;

  let item = optionalById<HTMLDivElement>(getCallParticipantListID(identity, isLocal));
  if (!item) {
    item = document.createElement("div");
    item.className = "call-participant-item" + (isLocal ? " is-local" : " is-remote");
    item.id = getCallParticipantListID(identity, isLocal);
    item.dataset.identity = identity;

    const avatar = document.createElement("div");
    avatar.className = "call-participant-avatar";
    item.appendChild(avatar);

    const meta = document.createElement("div");
    meta.className = "call-participant-meta";

    const name = document.createElement("div");
    name.className = "call-participant-name";
    meta.appendChild(name);

    const status = document.createElement("div");
    status.className = "call-participant-status";
    meta.appendChild(status);

    item.appendChild(meta);
    list.appendChild(item);
    if (!isLocal) attachCallParticipantContextMenu(item, identity);
  }

  return item;
}

function ensureCallParticipantPlaceholder(tile: HTMLElement | null, name: string, avatarURL: string): void {
  if (!tile) return;
  let placeholder = tile.querySelector<HTMLDivElement>(".participant-avatar-placeholder");
  const hasVideo = !!tile.querySelector("video");

  if (hasVideo) {
    if (placeholder) placeholder.remove();
    return;
  }

  if (!placeholder) {
    placeholder = createCallParticipantPlaceholder(name, avatarURL);
    tile.prepend(placeholder);
  } else {
    setCallAvatarContent(placeholder, name, avatarURL);
  }
}

function updateCallParticipantUI(participant: any, isLocal: boolean, identityOverride?: string): void {
  const profile = getCallParticipantProfile(participant, isLocal, identityOverride);
  const identity = identityOverride || profile.identity;
  const tile = ensureCallParticipantTile(participant, isLocal, identity);
  const listItem = ensureCallParticipantListItem(participant, isLocal, identity);
  const micEnabled = getCallParticipantMicEnabled(participant, isLocal);

  if (tile) {
    const label = tile.querySelector<HTMLElement>(".participant-name-label");
    const badge = tile.querySelector<HTMLElement>(".participant-state-badge");
    const speaking = isCallParticipantSpeaking(identity);
    if (label) {
      label.textContent = isLocal ? profile.displayName + " (Вы)" : profile.displayName;
    }
    if (badge) {
      badge.textContent = speaking ? "Говорит" : micEnabled ? "Мик" : "Выкл";
    }
    tile.classList.toggle("is-muted", !micEnabled);
    ensureCallParticipantPlaceholder(tile, profile.displayName, profile.avatarURL);
    applyCallParticipantSpeakingState(identity, isLocal);
  }

  if (listItem) {
    const avatar = listItem.querySelector<HTMLElement>(".call-participant-avatar");
    const name = listItem.querySelector<HTMLElement>(".call-participant-name");
    const status = listItem.querySelector<HTMLElement>(".call-participant-status");
    if (avatar) setCallAvatarContent(avatar, profile.displayName, profile.avatarURL);
    if (name) name.textContent = isLocal ? profile.displayName + " (Вы)" : profile.displayName;
    if (status) status.textContent = buildCallParticipantStatusText(identity, participant, isLocal);
    listItem.classList.toggle("is-muted", !micEnabled);
    applyCallParticipantSpeakingState(identity, isLocal);
  }

  updateCallParticipantCount();
}

function syncRemoteParticipantTracks(participant: any): void {
  if (!participant || !participant.trackPublications || typeof participant.trackPublications.forEach !== "function") {
    return;
  }
  participant.trackPublications.forEach((publication: any) => {
    if (publication && publication.track) {
      attachCallTrack(publication.track, participant, false);
    }
  });
}

function syncLocalParticipantMedia(): void {
  if (!callRoom || !callRoom.localParticipant || !LivekitClient || !LivekitClient.Track) return;
  const localParticipant = callRoom.localParticipant;
  syncCallMicrophoneState();
  const tile = ensureCallParticipantTile(localParticipant, true);
  if (!tile) return;

  const existingVideos = Array.from(tile.querySelectorAll<HTMLVideoElement>("video"));
  const clearVideos = (): void => {
    detachCallParticipantTracks(localParticipant, "video");
    // Unpublish can remove the SDK publication before this callback. Its former
    // elements still need clearing even when track.detach() cannot find them.
    existingVideos.forEach((videoEl) => {
      videoEl.pause();
      videoEl.srcObject = null;
      videoEl.remove();
    });
  };
  const screenPublication = localParticipant.getTrackPublication(LivekitClient.Track.Source.ScreenShare);
  const cameraPublication = localParticipant.getTrackPublication(LivekitClient.Track.Source.Camera);
  const activePublication =
    screenPublication && screenPublication.track && localTracks.screen
      ? screenPublication
      : cameraPublication && cameraPublication.track && localTracks.camera
        ? cameraPublication
        : null;

  if (activePublication && activePublication.track) {
    const activeTrack = activePublication.track;
    const activeTrackSid = String(activeTrack.sid || "");
    const needsAttach =
      existingVideos.length === 0 ||
      !existingVideos.some((videoEl) => videoEl.dataset.trackSid === activeTrackSid);
    if (needsAttach) {
      clearVideos();
      const videoEl = activeTrack.attach() as HTMLVideoElement;
      videoEl.style.width = "100%";
      videoEl.style.height = "100%";
      videoEl.style.objectFit = activePublication === screenPublication ? "contain" : "cover";
      videoEl.playsInline = true;
      videoEl.dataset.trackSid = activeTrackSid;
      videoEl.dataset.localVideo = "1";
      tile.prepend(videoEl);
    }
    const placeholder = tile.querySelector(".participant-avatar-placeholder");
    if (placeholder) placeholder.remove();
  } else {
    clearVideos();
    const profile = getCallParticipantProfile(localParticipant, true);
    ensureCallParticipantPlaceholder(tile, profile.displayName, profile.avatarURL);
  }

  updateCallParticipantUI(localParticipant, true);
  syncCallVideoControls(tile);
}

function addParticipantToCallUI(participant: any, isLocal: boolean): void {
  if (!isLocal && !participant) return;
  updateCallParticipantUI(participant, isLocal);
  if (isLocal) {
    syncLocalParticipantMedia();
  } else {
    syncRemoteParticipantTracks(participant);
  }
  updateVideoGridLayout();
}

function removeParticipantFromCallUI(participant: any, isLocal: boolean): void {
  detachCallParticipantTracks(participant);
  const identity = getCallParticipantIdentity(participant, isLocal);
  const tile = optionalById<HTMLElement>(getCallParticipantTileID(identity, isLocal));
  const listItem = optionalById<HTMLElement>(getCallParticipantListID(identity, isLocal));
  if (tile) tile.remove();
  if (listItem) listItem.remove();
  callSpeakingIdentities.delete(String(identity));
  delete callParticipantAudioElements[String(identity)];
  if (activeCallParticipantMenuIdentity === String(identity)) {
    hideCallParticipantMenu();
  }
  updateCallParticipantCount();
}

function findAttachedTrackElement(
  tile: HTMLElement,
  tagName: "video" | "audio",
  trackSID: unknown,
): HTMLMediaElement | undefined {
  const elements = Array.from(tile.querySelectorAll<HTMLMediaElement>(tagName));
  return elements.find((element) => element.dataset.trackSid === String(trackSID || ""));
}

function attachCallTrack(track: any, participant: any, isLocal: boolean): void {
  const identity = getCallParticipantIdentity(participant, isLocal);
  const tile = ensureCallParticipantTile(participant, isLocal, identity);
  if (!tile) return;

  if (track.kind === "video") {
    const existingVideo = findAttachedTrackElement(tile, "video", track.sid);
    if (existingVideo) { track.attach(existingVideo); return; }
    const videoEl = track.attach() as HTMLVideoElement;
    videoEl.style.width = "100%";
    videoEl.style.height = "100%";
    videoEl.style.objectFit = track.source === LivekitClient.Track.Source.ScreenShare ? "contain" : "cover";
    videoEl.playsInline = true;
    videoEl.dataset.trackSid = String(track.sid || "");
    tile.prepend(videoEl);
    const placeholder = tile.querySelector(".participant-avatar-placeholder");
    if (placeholder) placeholder.remove();
  } else if (track.kind === "audio") {
    const existingAudio = findAttachedTrackElement(tile, "audio", track.sid);
    if (existingAudio) {
      track.attach(existingAudio);
      if (!isLocal) registerCallParticipantAudioElement(identity, existingAudio as HTMLAudioElement);
      return;
    }
    const audioEl = track.attach() as HTMLAudioElement;
    audioEl.style.display = "none";
    audioEl.autoplay = true;
    audioEl.dataset.trackSid = String(track.sid || "");
    tile.appendChild(audioEl);
    if (!isLocal) registerCallParticipantAudioElement(identity, audioEl);
  }

  updateCallParticipantUI(participant, isLocal, identity);
  syncCallVideoControls(tile);
}

function setCallConnectionStatus(message: string): void {
  const status = optionalById<HTMLElement>("callConnectionStatus");
  if (status) {
    status.textContent = message;
    status.hidden = !message;
  }
}

function showCallOverlay(): void {
  const overlay = optionalById<HTMLDivElement>("callOverlay");
  const nameEl = optionalById<HTMLElement>("callChatName");
  const grid = optionalById<HTMLDivElement>("callVideoGrid");
  const list = optionalById<HTMLDivElement>("callParticipantsList");
  if (!overlay || !nameEl || !grid) return;

  const chatInfo = chats.find((chat) => Number(chat.id) === Number(callChatID));
  nameEl.textContent = chatInfo ? chatInfo.name || chatInfo.other_username || "Звонок" : "Звонок";

  document.body.classList.add("call-dock-active");
  overlay.style.display = "grid";
  hideCallParticipantMenu();
  closeScreenSharePanel();
  loadCallScreenShareDraftFromSettings();
  setCallDockExpanded(false);

  // Removing a DOM node alone leaves it attached in LiveKit, including audio.
  detachCallMedia(callRoom);
  grid.innerHTML = "";
  if (list) list.innerHTML = "";
  addParticipantToCallUI(callRoom ? callRoom.localParticipant : null, true);

  if (callRoom) {
    callRoom.remoteParticipants.forEach((participant: any) => {
      addParticipantToCallUI(participant, false);
    });
  }
  updateVideoGridLayout();
  updateCallControlButtons();
  syncCallScreenSharePanelFromState();
  setCallActiveSpeakers(callRoom ? callRoom.activeSpeakers || [] : []);

  if (callDurationInterval) {
    clearInterval(callDurationInterval);
  }
  callStartTime = Date.now();
  updateCallDuration();
  callDurationInterval = window.setInterval(updateCallDuration, 1000);
}

function handleTrackSubscribed(track: any, participant: any): void {
  attachCallTrack(track, participant, false);
}

function handleTrackUnsubscribed(track: any, participant: any): void {
  const identity = getCallParticipantIdentity(participant, false);
  const tile = optionalById<HTMLElement>(getCallParticipantTileID(identity, false));
  const detached = new Set<HTMLMediaElement>(track.detach() as HTMLMediaElement[]);
  // LiveKit detaches the media before emitting TrackUnsubscribed. Its detach()
  // can therefore return no elements while our empty video/audio is still in DOM.
  tile?.querySelectorAll<HTMLMediaElement>("video, audio").forEach((element) => {
    if (element.dataset.trackSid === String(track.sid || "")) detached.add(element);
  });
  detached.forEach((element) => {
    if (track.kind === "audio") {
      unregisterCallParticipantAudioElement(identity, element as HTMLAudioElement);
    }
    element.pause();
    element.srcObject = null;
    element.remove();
  });
  updateCallParticipantUI(participant, false, identity);
  if (tile) {
    syncCallVideoControls(tile);
  }
}

function updateVideoGridLayout(): void {
  const grid = optionalById<HTMLDivElement>("callVideoGrid");
  if (!grid) return;
  grid.dataset.count = String(grid.children.length);
  updateCallParticipantCount();
}

function updateCallDuration(): void {
  if (!callStartTime) return;
  const elapsed = Math.floor((Date.now() - callStartTime) / 1000);
  const mins = String(Math.floor(elapsed / 60)).padStart(2, "0");
  const secs = String(elapsed % 60).padStart(2, "0");
  const durationEl = optionalById<HTMLElement>("callDuration");
  if (durationEl) durationEl.textContent = mins + ":" + secs;
}

function updateCallParticipantCount(): void {
  const countEl = optionalById<HTMLElement>("callParticipantCount");
  const inlineCountEl = optionalById<HTMLElement>("callParticipantCountInline");
  const list = optionalById<HTMLDivElement>("callParticipantsList");
  if (!list) return;
  const count = String(list.children.length);
  if (countEl) countEl.textContent = count;
  if (inlineCountEl) inlineCountEl.textContent = count;
}

function updateCallControlButtons(): void {
  const micBtn = optionalById<HTMLButtonElement>("callToggleMic");
  const micLabel = optionalById<HTMLElement>("callMicLabel");
  const screenBtn = optionalById<HTMLButtonElement>("callShareScreen");
  const screenLabel = optionalById<HTMLElement>("callScreenShareLabel");
  const radioMode = isRadioMicrophoneMode();
  const radioHotkey = getConfiguredRadioHotkeyCode();
  const radioHotkeyLabel = radioHotkey ? formatKeyboardCodeLabel(radioHotkey) : "";

  if (micBtn) {
    micBtn.disabled = callState !== "in_call";
    micBtn.classList.toggle("radio-mode", radioMode);
    micBtn.classList.toggle("active", radioMode ? !!localTracks.mic : false);
    micBtn.classList.toggle("muted", radioMode ? false : !localTracks.mic);
    micBtn.title = radioMode
      ? localTracks.mic
        ? "Рация активна" + (radioHotkeyLabel ? " · " + radioHotkeyLabel : "")
        : "Удерживайте, чтобы говорить" + (radioHotkeyLabel ? " · " + radioHotkeyLabel : "")
      : localTracks.mic
        ? "Выключить микрофон"
        : "Включить микрофон";
  }
  if (micLabel) {
    if (radioMode) {
      micLabel.textContent = localTracks.mic
        ? "Говорите"
        : radioHotkeyLabel
          ? "Рация · " + radioHotkeyLabel
          : "Рация";
    } else {
      micLabel.textContent = localTracks.mic ? "Микрофон" : "Микрофон выкл";
    }
  }
  if (screenBtn) {
    screenBtn.disabled = callState !== "in_call";
    screenBtn.classList.toggle("active", !!localTracks.screen);
    screenBtn.title = localTracks.screen ? "Параметры демонстрации · показ активен" : "Параметры демонстрации";
  }
  if (screenLabel) {
    screenLabel.textContent = localTracks.screen ? "Демо активна" : "Экран";
  }
}
