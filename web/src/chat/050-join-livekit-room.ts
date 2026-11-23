// --- Join LiveKit room ---
let callSDKPromise: Promise<void> | null = null;
let callSDKAttempt = 0;

function loadCallSDK(): Promise<void> {
  if (typeof LivekitClient !== "undefined") return Promise.resolve();
  if (callSDKPromise) return callSDKPromise;
  callSDKPromise = new Promise<void>((resolve, reject) => {
    let settled = false;
    const script = document.createElement("script");
    // A removed script can keep downloading. Do not join that stalled fetch on retry.
    const attempt = callSDKAttempt++;
    script.src = "/vendor/livekit-client.umd.js" + (attempt ? "?retry=" + attempt : "");
    script.async = true;
    const fail = (): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      script.remove();
      callSDKPromise = null;
      reject(new Error("Не удалось загрузить модуль звонков. Попробуйте ещё раз."));
    };
    const timer = window.setTimeout(fail, 10000);
    script.onerror = fail;
    script.onload = (): void => {
      if (typeof LivekitClient === "undefined") { fail(); return; }
      clearTimeout(timer);
      settled = true;
      resolve();
    };
    document.head.appendChild(script);
  });
  return callSDKPromise;
}

async function joinLivekitRoom(type: CallMediaType): Promise<void> {
  void type;
  const attempt = ++callAttempt;
  const chatID = callChatID;
  const request = new AbortController();
  callJoinRequest?.abort();
  callJoinRequest = request;
  let room: any = null;
  let microphone: any = null;
  const releaseMicrophone = (): void => {
    const track = microphone;
    microphone = null;
    track?.stop();
  };
  request.signal.addEventListener("abort", releaseMicrophone, { once: true });
  const isCurrent = (): boolean => callAttempt === attempt && callChatID === chatID &&
    callState !== "idle" && !authExpiredHandled && localStorage.getItem("token") === token;
  const isCurrentRoom = (): boolean => isCurrent() && callRoom === room;
  const isPrivate = chats.some((chat) => Number(chat.id) === Number(chatID) && chat.type === "private");
  let stage: "module" | "connection" | "microphone" = "module";
  showCallOverlay();
  setCallConnectionStatus("Подключение…");

  if (callState === "calling" && isPrivate && incomingCallData === null) {
    // Also release the caller when the peer is offline and cannot reject an invite.
    callInviteTimer = window.setTimeout(() => {
      if (isCurrent() && (!room || room.remoteParticipants.size === 0)) endCall();
    }, 35000);
  }

  try {
    await loadCallSDK();
    if (!isCurrent()) return;
    const res = await apiFetch("/api/calls/join?chat=" + chatID, { signal: request.signal });
    if (!res.ok) throw new Error("Failed to get call token");
    const data = await res.json() as { url?: string; token?: string };
    if (!isCurrent()) return;

    const { Room, RoomEvent } = LivekitClient as any;
    room = new Room({
      adaptiveStream: true,
      dynacast: true,
      // Firefox recovery is unreliable with single-PC mode in the pinned SDK/SFU.
      singlePeerConnection: false,
    });
    callRoom = room;
    const clearPeerLeftTimer = (): void => {
      if (callPeerLeftTimer !== null) clearTimeout(callPeerLeftTimer);
      callPeerLeftTimer = null;
    };
    const waitForPeer = (delay: number): void => {
      clearPeerLeftTimer();
      if (!isPrivate || room.state !== "connected" || room.remoteParticipants.size !== 0) return;
      callPeerLeftTimer = window.setTimeout(() => {
        if (isCurrentRoom() && room.state === "connected" && room.remoteParticipants.size === 0) cleanupCall();
      }, delay);
    };
    const reconnecting = (): void => {
      if (!isCurrentRoom()) return;
      clearPeerLeftTimer();
      setCallConnectionStatus("Восстанавливаем соединение…");
      if (callRecoveryTimer === null) {
        callRecoveryTimer = window.setTimeout(() => {
          callRecoveryTimer = null;
          if (!isCurrentRoom() || room.state === "connected") return;
          endCall();
          renderSystemMessage("Не удалось восстановить звонок. Попробуйте позвонить снова после восстановления сети.");
        }, 60000);
      }
    };
    room.on(RoomEvent.Reconnecting, reconnecting);
    room.on(RoomEvent.SignalReconnecting, reconnecting);
    room.on(RoomEvent.Reconnected, () => {
      if (!isCurrentRoom()) return;
      if (callRecoveryTimer !== null) clearTimeout(callRecoveryTimer);
      callRecoveryTimer = null;
      setCallConnectionStatus("");
      // The SDK installs deferred incoming tracks in later Reconnected listeners.
      // Reconcile after that dispatch/its buffered events, including subscriptions
      // whose TrackSubscribed event was missed or whose elements were detached.
      void Promise.resolve().then(() => {
        if (!isCurrentRoom() || room.state !== "connected") return;
        room.remoteParticipants.forEach((participant: any) => addParticipantToCallUI(participant, false));
        updateVideoGridLayout();
      });
      // The other browser may still be retrying after a full SFU restart.
      waitForPeer(10000);
    });

    room.on(RoomEvent.TrackSubscribed, (track: any, publication: any, participant: any) => {
      if (!isCurrentRoom()) return;
      void publication;
      handleTrackSubscribed(track, participant);
    });
    room.on(RoomEvent.TrackUnsubscribed, (track: any, publication: any, participant: any) => {
      if (!isCurrentRoom()) return;
      void publication;
      handleTrackUnsubscribed(track, participant);
    });
    room.on(RoomEvent.ParticipantConnected, (participant: any) => {
      if (!isCurrentRoom()) return;
      clearPeerLeftTimer();
      if (callInviteTimer !== null) clearTimeout(callInviteTimer);
      callInviteTimer = null;
      addParticipantToCallUI(participant, false);
      updateVideoGridLayout();
    });
    room.on(RoomEvent.ParticipantDisconnected, (participant: any) => {
      if (!isCurrentRoom()) return;
      removeParticipantFromCallUI(participant, false);
      updateVideoGridLayout();
      // Full reconnect also emits ParticipantDisconnected for every old peer.
      // Only a connected room can establish that the peer actually left.
      waitForPeer(2000);
    });
    room.on(RoomEvent.Disconnected, (reason: unknown) => {
      // During join, connect()/publish rejects with the useful failure reason.
      // Cleaning up here first would hide that error as a cancelled old attempt.
      if (isCurrentRoom() && callState === "in_call") {
        cleanupCall();
        if (reason === LivekitClient.DisconnectReason.CONNECTION_FAILED) {
          renderSystemMessage("Связь с сервером звонков потеряна. Попробуйте позвонить снова после восстановления сети.");
        }
      }
    });
    if (RoomEvent.TrackMuted) {
      room.on(RoomEvent.TrackMuted, (publication: any, participant: any) => {
        if (!isCurrentRoom()) return;
        void publication;
        if (isLocalCallParticipant(participant)) {
          syncCallMicrophoneState();
          updateCallControlButtons();
        }
        updateCallParticipantUI(participant, isLocalCallParticipant(participant));
      });
    }
    if (RoomEvent.TrackUnmuted) {
      room.on(RoomEvent.TrackUnmuted, (publication: any, participant: any) => {
        if (!isCurrentRoom()) return;
        void publication;
        if (isLocalCallParticipant(participant)) {
          syncCallMicrophoneState();
          updateCallControlButtons();
        }
        updateCallParticipantUI(participant, isLocalCallParticipant(participant));
      });
    }
    if (RoomEvent.ActiveSpeakersChanged) {
      room.on(RoomEvent.ActiveSpeakersChanged, (participants: any[]) => {
        if (!isCurrentRoom()) return;
        setCallActiveSpeakers(participants);
      });
    }
    if (RoomEvent.LocalTrackPublished) {
      room.on(RoomEvent.LocalTrackPublished, () => {
        if (!isCurrentRoom()) return;
        syncLocalParticipantMedia();
        updateCallParticipantUI(room.localParticipant, true);
        updateCallControlButtons();
      });
    }
    if (RoomEvent.LocalTrackUnpublished) {
      room.on(RoomEvent.LocalTrackUnpublished, () => {
        if (!isCurrentRoom()) return;
        syncLocalParticipantMedia();
        updateCallParticipantUI(room.localParticipant, true);
        updateCallControlButtons();
      });
    }

    // Firefox limits ICE interfaces before media permission is granted. Acquire
    // the microphone first, then publish this same track once connected.
    localTracks.camera = false;
    localTracks.screen = false;
    localTracks.mic = !isRadioMicrophoneMode();
    stage = "microphone";
    setCallConnectionStatus("Ожидаем доступ к микрофону…");
    microphone = await LivekitClient.createLocalAudioTrack();
    if (!isCurrentRoom()) return;
    if (!localTracks.mic) await microphone.mute();
    if (!isCurrentRoom()) return;

    stage = "connection";
    setCallConnectionStatus("Подключение…");
    await room.connect(window.zchatDesktop?.callGatewayURL || String(data.url || ""), String(data.token || ""));
    if (!isCurrentRoom()) { await room.disconnect(); return; }
    await loadCallParticipantDirectory(chatID);
    if (!isCurrentRoom()) { await room.disconnect(); return; }
    loadCallScreenShareDraftFromSettings();

    await room.localParticipant.publishTrack(microphone, { source: LivekitClient.Track.Source.Microphone });
    microphone = null; // The room now owns the published track and stops it on disconnect.
    if (!isCurrentRoom()) { await room.disconnect(); return; }
    if (room.state !== "connected") {
      stage = "connection";
      throw new Error("Media connection closed while joining");
    }

    callState = "in_call";
    setCallConnectionStatus("");
    showCallOverlay();
  } catch (err) {
    if (!isCurrent()) {
      if (room) await room.disconnect().catch(() => {});
      return;
    }
    const name = err && typeof err === "object" && "name" in err ? String(err.name) : "Error";
    console.error("Call connection failed", { stage, name });
    let message = "Не удалось установить медиасоединение. Проверьте сеть или настройки сервера звонков.";
    if (stage === "module") message = "Не удалось загрузить модуль звонков. Попробуйте ещё раз.";
    if (stage === "microphone") {
      let denied = name === "NotAllowedError" || name === "PermissionDeniedError";
      if (!denied && navigator.permissions) {
        try {
          denied = (await navigator.permissions.query({ name: "microphone" as PermissionName })).state === "denied";
        } catch { /* Some browsers do not support querying microphone permission. */ }
      }
      if (!isCurrent()) return;
      message = denied
        ? window.zchatDesktop
          ? "Доступ к микрофону запрещён. В настройках чата откройте «Микрофон» → «Сбросить и перезагрузить», затем повторите звонок и разрешите доступ. Также проверьте доступ в настройках конфиденциальности системы."
          : "Доступ к микрофону запрещён. Разрешите микрофон для этого сайта в настройках браузера и повторите звонок."
        : name === "NotFoundError"
          ? "Микрофон не найден. Подключите его и повторите звонок."
          : "Не удалось включить микрофон. Проверьте выбранное устройство и доступ к нему.";
    }
    endCall();
    renderSystemMessage(message);
    alert(message);
  } finally {
    request.signal.removeEventListener("abort", releaseMicrophone);
    releaseMicrophone();
    if (callJoinRequest === request) callJoinRequest = null;
  }
}
