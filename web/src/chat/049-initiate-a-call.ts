// --- Initiate a call ---
async function initiateCall(type: CallMediaType): Promise<void> {
  if (callState !== "idle") return;
  if (!currentChatID) return;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    renderSystemMessage("Нет соединения с сервером. Дождитесь переподключения перед звонком.");
    return;
  }

  callState = "calling";
  callType = type;
  callChatID = currentChatID;

  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(
      JSON.stringify({
        type: "call_invite",
        chat_id: Number(currentChatID),
        content: type,
      }),
    );
  }

  await joinLivekitRoom(type);
}
