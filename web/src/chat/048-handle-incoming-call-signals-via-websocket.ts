// --- Handle incoming call signals via WebSocket ---
(function (): void {
  const originalHandleForCalls = handleIncomingMessage;
  handleIncomingMessage = function(msg: ChatMessage): void {
    if (msg.type === "call_invite") {
      handleCallInvite(msg as IncomingCallMessage);
      return;
    }
    if (msg.type === "call_accept") {
      handleCallAccept(msg);
      return;
    }
    if (msg.type === "call_reject") {
      handleCallReject(msg);
      return;
    }
    if (msg.type === "call_end") {
      handleCallEnd(msg);
      return;
    }
    originalHandleForCalls(msg);
  };
})();
