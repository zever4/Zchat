// ===== TYPING INDICATOR =====
let typingTimeout: number | undefined;
let isTyping = false;
const typingUsers: Record<string, { name: string; timeout: number }> = {};

messageInput.addEventListener("input", () => {
  if (!isTyping && socket?.readyState === WebSocket.OPEN) {
    isTyping = true;
    socket.send(
      JSON.stringify({
        type: "typing",
        chat_id: Number(currentChatID),
      }),
    );
  }
  clearTimeout(typingTimeout);
  typingTimeout = setTimeout(() => {
    isTyping = false;
  }, 2000);
});

function showTypingIndicator(msg: ChatMessage): void {
  const msgChatID = msg.chat_id || 0;
  if (msgChatID !== currentChatID) return;

  if (typingUsers[msg.sender_id]) {
    clearTimeout(typingUsers[msg.sender_id].timeout);
  }

  typingUsers[msg.sender_id] = {
    name: msg.sender_name,
    timeout: setTimeout(() => {
      delete typingUsers[msg.sender_id];
      updateTypingDisplay();
    }, 3000),
  };

  updateTypingDisplay();
}

function updateTypingDisplay(): void {
  const names = Object.values(typingUsers).map((u) => u.name);
  if (names.length === 0) {
    typingIndicatorEl.style.display = "none";
  } else if (names.length === 1) {
    typingIndicatorEl.textContent = names[0] + " печатает...";
    typingIndicatorEl.style.display = "block";
  } else {
    typingIndicatorEl.textContent = names.join(", ") + " печатают...";
    typingIndicatorEl.style.display = "block";
  }
}

function clearTypingDisplay(): void {
  for (const id in typingUsers) {
    clearTimeout(typingUsers[id].timeout);
    delete typingUsers[id];
  }
  typingIndicatorEl.style.display = "none";
}
