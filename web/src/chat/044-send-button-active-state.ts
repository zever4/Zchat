// ---- Send button active state ----
messageInput.addEventListener("input", () => {
    sendBtn.classList.toggle("has-text", messageInput.value.trim().length > 0);
});
