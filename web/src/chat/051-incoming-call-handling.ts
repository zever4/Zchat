// --- Incoming call handling ---
function handleCallInvite(msg: IncomingCallMessage): void {
  if (callState !== "idle") return;

  callState = "ringing";
  incomingCallData = msg;

  const avatarEl = optionalById<HTMLDivElement>("callModalAvatar");
  const callerEl = optionalById<HTMLElement>("callModalCaller");
  const typeEl = optionalById<HTMLElement>("callModalType");
  const modal = optionalById<HTMLDivElement>("incomingCallModal");

  if (callerEl) callerEl.textContent = msg.sender_name || "Неизвестный";
  if (avatarEl) {
    if (msg.sender_avatar) {
      avatarEl.style.backgroundImage = "url(" + displayAssetURL(msg.sender_avatar) + ")";
      avatarEl.textContent = "";
    } else {
      avatarEl.style.backgroundImage = "";
      avatarEl.textContent = (msg.sender_name || "?").charAt(0).toUpperCase();
    }
  }
  if (typeEl) typeEl.textContent = "Аудиозвонок...";
  if (modal) modal.style.display = "flex";
  syncIncomingCallSound();

  incomingCallData._timeout = window.setTimeout(() => {
    if (callState === "ringing") {
      rejectIncomingCall();
    }
  }, 30000);
}

function acceptIncomingCall(): void {
  if (callState !== "ringing" || !incomingCallData) return;
  if (incomingCallData._timeout) clearTimeout(incomingCallData._timeout);

  const msg = incomingCallData;
  // Leave ringing before sending: the server echoes acceptance to every own tab.
  callState = "calling";
  stopAppSound("ring");
  incomingCallData = null;
  callType = msg.content === "video" ? "video" : "audio";
  callChatID = Number(msg.chat_id || 0) || null;

  const modal = optionalById<HTMLDivElement>("incomingCallModal");
  if (modal) modal.style.display = "none";

  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(
      JSON.stringify({
        type: "call_accept",
        chat_id: Number(callChatID),
        content: callType,
      }),
    );
  }

  void joinLivekitRoom(callType);
}

function rejectIncomingCall(): void {
  if (!incomingCallData) return;
  if (incomingCallData._timeout) clearTimeout(incomingCallData._timeout);
  stopAppSound("ring");

  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(
      JSON.stringify({
        type: "call_reject",
        chat_id: Number(incomingCallData.chat_id),
        content: "",
      }),
    );
  }

  const modal = optionalById<HTMLDivElement>("incomingCallModal");
  if (modal) modal.style.display = "none";
  callState = "idle";
  incomingCallData = null;
}

function handleCallAccept(msg: ChatMessage): void {
  if (String(msg.sender_id) === callUserID && callState === "ringing" &&
      Number(incomingCallData?.chat_id) === Number(msg.chat_id)) {
    cleanupCall();
    return;
  }
  // Keep the caller's deadline until the peer actually joins the media room.
}

function handleCallReject(msg: ChatMessage): void {
  if (String(msg.sender_id) === callUserID && callState === "ringing" &&
      Number(incomingCallData?.chat_id) === Number(msg.chat_id)) {
    cleanupCall();
    return;
  }
  if ((callState === "calling" || callState === "in_call") && Number(callChatID) === Number(msg.chat_id)) {
    const chatMeta = chats.find((chat) => Number(chat.id) === Number(callChatID));
    if (chatMeta && chatMeta.type === "private") {
      cleanupCall();
    }
  }
}

function handleCallEnd(msg: ChatMessage): void {
  if (Number(callChatID) === Number(msg.chat_id) && (callState === "calling" || callState === "in_call")) {
    const chatMeta = chats.find((chat) => Number(chat.id) === Number(callChatID));
    if (chatMeta && chatMeta.type === "private") {
      cleanupCall();
    }
  }

  if (callState === "ringing" && incomingCallData && Number(incomingCallData.chat_id) === Number(msg.chat_id)) {
    cleanupCall();
  }
}
