// --- Show/hide call buttons when switching chats ---
(function (): void {
  const originalSwitchForCalls = switchChat;
  switchChat = function(
    chatID: number | string,
    chatName?: string | null,
    chatType?: string | null,
  ): void {
    originalSwitchForCalls(chatID, chatName, chatType);
    const audioBtn = optionalById<HTMLElement>("audioCallBtn");
    if (!audioBtn) return;
    audioBtn.style.display = chatID ? "" : "none";
  };
})();
