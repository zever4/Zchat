// --- Call controls ---
async function toggleCallMic(): Promise<void> {
  if (!callRoom) return;
  if (isRadioMicrophoneMode()) return;
  await setCallMicEnabled(!localTracks.mic);
}

async function toggleCallCamera(): Promise<void> {
  localTracks.camera = false;
  updateCallControlButtons();
}

function readCallScreenSharePanelValues(): {
  screen_share_resolution: ScreenShareResolution;
  screen_share_fps: number;
} {
  const resolutionEl = optionalById<HTMLSelectElement>("callScreenShareResolution");
  const fpsEl = optionalById<HTMLSelectElement>("callScreenShareFPS");
  return {
    screen_share_resolution: normalizeScreenShareResolution(
      (resolutionEl && resolutionEl.value) || callScreenShareDraft.resolution,
    ),
    screen_share_fps: normalizeScreenShareFPS((fpsEl && fpsEl.value) || callScreenShareDraft.fps),
  };
}

function syncCallScreenSharePanelFromState(): void {
  const panel = optionalById<HTMLDivElement>("callScreenSharePanel");
  const resolutionEl = optionalById<HTMLSelectElement>("callScreenShareResolution");
  const fpsEl = optionalById<HTMLSelectElement>("callScreenShareFPS");
  const statusEl = optionalById<HTMLElement>("callScreenSharePanelStatus");
  const applyBtn = optionalById<HTMLButtonElement>("callScreenShareApplyBtn");
  const activePreset = getScreenSharePreset(getCallScreenShareDraft());
  if (resolutionEl) {
    resolutionEl.value = activePreset.resolution;
    resolutionEl.disabled = !!localTracks.screen;
  }
  if (fpsEl) {
    fpsEl.value = String(activePreset.fps);
    fpsEl.disabled = !!localTracks.screen;
  }
  if (statusEl) {
    statusEl.textContent = localTracks.screen
      ? "Сейчас идёт демонстрация " + buildScreenSharePresetLabel(getCallScreenShareDraft())
      : "Будет запущено: " + buildScreenSharePresetLabel(getCallScreenShareDraft());
  }
  if (applyBtn) {
    applyBtn.textContent = localTracks.screen ? "Остановить демонстрацию" : "Начать демонстрацию";
    applyBtn.classList.toggle("is-stop", !!localTracks.screen);
  }
  if (!callRoom && panel) panel.style.display = "none";
}

function handleCallScreenSharePresetInput(): void {
  const next = readCallScreenSharePanelValues();
  callScreenShareDraft = {
    resolution: next.screen_share_resolution,
    fps: next.screen_share_fps,
  };
  syncCallScreenSharePanelFromState();
}

function openScreenSharePanel(): void {
  if (!callRoom) return;
  const panel = optionalById<HTMLDivElement>("callScreenSharePanel");
  if (!panel) return;
  syncCallScreenSharePanelFromState();
  panel.style.display = "block";
}

function closeScreenSharePanel(): void {
  const panel = optionalById<HTMLDivElement>("callScreenSharePanel");
  if (panel) panel.style.display = "none";
}

function toggleScreenSharePanel(): void {
  const panel = optionalById<HTMLDivElement>("callScreenSharePanel");
  if (!panel || !callRoom) return;
  if (panel.style.display === "block") {
    closeScreenSharePanel();
    return;
  }
  openScreenSharePanel();
}

async function setCallScreenShareEnabled(
  enabled: boolean,
  nextSettings?: { screen_share_resolution: ScreenShareResolution; screen_share_fps: number },
): Promise<void> {
  if (!callRoom || !callRoom.localParticipant) return;
  const shouldEnable = !!enabled;

  if (!shouldEnable) {
    await callRoom.localParticipant.setScreenShareEnabled(false);
    localTracks.screen = false;
    syncLocalParticipantMedia();
    updateCallParticipantUI(callRoom.localParticipant, true);
    updateCallControlButtons();
    syncCallScreenSharePanelFromState();
    return;
  }

  const preset = getScreenSharePreset(nextSettings || getCallScreenShareDraft());
  callScreenShareDraft = {
    resolution: preset.resolution,
    fps: preset.fps,
  };

  await callRoom.localParticipant.setScreenShareEnabled(
    true,
    {
      resolution: {
        width: preset.width,
        height: preset.height,
        frameRate: preset.fps,
      },
    },
    {
      screenShareEncoding: {
        maxBitrate: preset.maxBitrate,
        maxFramerate: preset.fps,
      },
    },
  );

  localTracks.screen = true;
  syncLocalParticipantMedia();
  updateCallParticipantUI(callRoom.localParticipant, true);
  updateCallControlButtons();
  syncCallScreenSharePanelFromState();
}

async function submitScreenSharePanel(): Promise<void> {
  if (!callRoom) return;
  try {
    if (localTracks.screen) {
      await setCallScreenShareEnabled(false);
      closeScreenSharePanel();
      return;
    }
    const nextSettings = readCallScreenSharePanelValues();
    callScreenShareDraft = {
      resolution: nextSettings.screen_share_resolution,
      fps: nextSettings.screen_share_fps,
    };
    await setCallScreenShareEnabled(true, nextSettings);
    closeScreenSharePanel();
  } catch (err) {
    console.error("Failed to toggle screen share:", err);
    const message = err instanceof Error ? err.message : String(err || "");
    const userCanceled =
      !!err &&
      typeof err === "object" &&
      "name" in err &&
      ((err as { name?: string }).name === "AbortError" || /cancel|dismissed|denied by user/i.test(message));
    if (!userCanceled) {
      alert("Не удалось запустить демонстрацию: " + (message || "неизвестная ошибка"));
    }
    localTracks.screen = false;
    syncLocalParticipantMedia();
    updateCallControlButtons();
    syncCallScreenSharePanelFromState();
  }
}

async function toggleScreenShare(): Promise<void> {
  toggleScreenSharePanel();
}

function endCall(): void {
  if (socket && socket.readyState === WebSocket.OPEN && callChatID) {
    socket.send(
      JSON.stringify({
        type: "call_end",
        chat_id: Number(callChatID),
        content: "hangup",
      }),
    );
  }
  cleanupCall();
}

function cleanupCall(): void {
  if (callCleanupInProgress) return;
  callCleanupInProgress = true;
  stopAppSound("ring");
  ++callAttempt;
  callJoinRequest?.abort();
  callJoinRequest = null;
  if (callPeerLeftTimer !== null) clearTimeout(callPeerLeftTimer);
  if (callInviteTimer !== null) clearTimeout(callInviteTimer);
  if (callRecoveryTimer !== null) clearTimeout(callRecoveryTimer);
  callPeerLeftTimer = null;
  callInviteTimer = null;
  callRecoveryTimer = null;

  const room = callRoom;
  detachCallMedia(room);
  callRoom = null;
  if (room) {
    try {
      void Promise.resolve(room.disconnect()).catch((e: unknown) => {
        console.error("Failed to disconnect call room:", e);
      });
    } catch (e) {
      console.error("Failed to disconnect call room:", e);
    }
  }
  if (callDurationInterval) {
    clearInterval(callDurationInterval);
    callDurationInterval = null;
  }
  if (incomingCallData && incomingCallData._timeout) {
    clearTimeout(incomingCallData._timeout);
  }

  const overlay = optionalById<HTMLDivElement>("callOverlay");
  const incomingModal = optionalById<HTMLDivElement>("incomingCallModal");
  const grid = optionalById<HTMLDivElement>("callVideoGrid");
  const list = optionalById<HTMLDivElement>("callParticipantsList");
  if (overlay) overlay.style.display = "none";
  if (incomingModal) incomingModal.style.display = "none";
  if (grid) grid.innerHTML = "";
  if (list) list.innerHTML = "";
  document.body.classList.remove("call-dock-active");
  hideCallParticipantMenu();
  closeScreenSharePanel();
  setCallDockExpanded(false);

  callState = "idle";
  setCallConnectionStatus("");
  callType = null;
  callChatID = null;
  callStartTime = null;
  incomingCallData = null;
  localTracks = { mic: true, camera: false, screen: false };
  callParticipantDirectory = {};
  callParticipantVolumes = {};
  callParticipantAudioElements = {};
  callMicHoldActive = false;
  callMicPointerActive = false;
  callMicKeyboardActive = false;
  callSpeakingIdentities = new Set();
  loadCallScreenShareDraftFromSettings();

  updateCallControlButtons();
  syncCallScreenSharePanelFromState();
  updateCallParticipantCount();
  callCleanupInProgress = false;
}

function shouldHandleCallRadioHotkeyEvent(e: KeyboardEvent): boolean {
  if (!callRoom || callState !== "in_call" || !isRadioMicrophoneMode()) return false;
  if (microphonePttKeyCaptureActive) return false;
  const hotkey = getConfiguredRadioHotkeyCode();
  if (!hotkey || e.code !== hotkey) return false;
  if (isEditableEventTarget(e.target)) return false;
  return true;
}

const callMicControlBtn = optionalById<HTMLButtonElement>("callToggleMic");
if (callMicControlBtn) {
  callMicControlBtn.addEventListener("pointerdown", (e: PointerEvent) => {
    if (!isRadioMicrophoneMode() || !callRoom) return;
    e.preventDefault();
    callMicPointerActive = true;
    void startCallRadioTransmission();
  });
  callMicControlBtn.addEventListener("keydown", (e: KeyboardEvent) => {
    if (!isRadioMicrophoneMode() || !callRoom) return;
    if (e.repeat) return;
    if (e.code !== "Space" && e.code !== "Enter") return;
    e.preventDefault();
    callMicPointerActive = true;
    void startCallRadioTransmission();
  });
  callMicControlBtn.addEventListener("keyup", (e: KeyboardEvent) => {
    if (!isRadioMicrophoneMode()) return;
    if (e.code !== "Space" && e.code !== "Enter") return;
    e.preventDefault();
    callMicPointerActive = false;
    void stopCallRadioTransmission();
  });
}

const callScreenShareResolutionInput = optionalById<HTMLSelectElement>("callScreenShareResolution");
if (callScreenShareResolutionInput) {
  callScreenShareResolutionInput.addEventListener("change", handleCallScreenSharePresetInput);
}

const callScreenShareFPSInput = optionalById<HTMLSelectElement>("callScreenShareFPS");
if (callScreenShareFPSInput) {
  callScreenShareFPSInput.addEventListener("change", handleCallScreenSharePresetInput);
}

document.addEventListener(
  "keydown",
  (e: KeyboardEvent) => {
    if (!shouldHandleCallRadioHotkeyEvent(e)) return;
    if (e.repeat || callMicKeyboardActive) return;
    e.preventDefault();
    callMicKeyboardActive = true;
    void startCallRadioTransmission();
  },
  true,
);

document.addEventListener(
  "keyup",
  (e: KeyboardEvent) => {
    if (!callMicKeyboardActive) return;
    const hotkey = getConfiguredRadioHotkeyCode();
    if (!hotkey || e.code !== hotkey) return;
    e.preventDefault();
    callMicKeyboardActive = false;
    void stopCallRadioTransmission();
  },
  true,
);

document.addEventListener("pointerup", () => {
  if (!callMicPointerActive) return;
  callMicPointerActive = false;
  void stopCallRadioTransmission();
});

document.addEventListener("pointercancel", () => {
  if (!callMicPointerActive) return;
  callMicPointerActive = false;
  void stopCallRadioTransmission();
});

window.addEventListener("blur", () => {
  if (!callMicPointerActive && !callMicHoldActive && !callMicKeyboardActive) return;
  callMicPointerActive = false;
  callMicKeyboardActive = false;
  void stopCallRadioTransmission();
});

const callParticipantVolumeSlider = optionalById<HTMLInputElement>("callParticipantVolumeSlider");
if (callParticipantVolumeSlider) {
  callParticipantVolumeSlider.addEventListener("input", (e: Event) => {
    if (!activeCallParticipantMenuIdentity) return;
    const target = e.target as HTMLInputElement | null;
    if (!target) return;
    setCallParticipantVolume(activeCallParticipantMenuIdentity, Number(target.value) / 100);
  });
}

document.addEventListener("click", (e: MouseEvent) => {
  const menu = optionalById<HTMLDivElement>("callParticipantMenu");
  if (!menu || menu.style.display !== "block") return;
  if (e.target instanceof Node && menu.contains(e.target)) return;
  hideCallParticipantMenu();
});

document.addEventListener("click", (e: MouseEvent) => {
  const panel = optionalById<HTMLDivElement>("callScreenSharePanel");
  const trigger = optionalById<HTMLElement>("callShareScreen");
  if (!panel || panel.style.display !== "block") return;
  if (e.target instanceof Node && panel.contains(e.target)) return;
  if (trigger && e.target instanceof Node && trigger.contains(e.target)) return;
  closeScreenSharePanel();
});

document.addEventListener(
  "keydown",
  (e: KeyboardEvent) => {
    if (e.key === "Escape") {
      hideCallParticipantMenu();
      closeScreenSharePanel();
    }
  },
  true,
);
