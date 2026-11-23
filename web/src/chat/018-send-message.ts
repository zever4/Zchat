// ===== SEND MESSAGE =====
async function sendMessage(): Promise<void> {
  const content = messageInput.value.trim();
  if (content && currentChatID) {
    const chatID = Number(currentChatID);
    if (!queueOrdinaryMessage(chatID, content)) return;
    messageInput.value = "";
    sendBtn.classList.remove("has-text");
    messageInput.focus();
    isTyping = false;
    clearTimeout(typingTimeout);
  }
}

sendBtn.addEventListener("click", sendMessage);
messageInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();
    sendMessage();
  }
});
