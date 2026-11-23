// ===== WRAP handleIncomingMessage — intercept edit/delete =====
const _origHandleIncoming = handleIncomingMessage;
handleIncomingMessage = function(msg: ChatMessage): void {
    if (msg.type === "delete") {
        recordHistoryMutation(msg);
        removeMessageFromDOM(msg.id);
        removeMessageFromCache(msg.id, msg.chat_id);
        return;
    }
    if (msg.type === "edit") {
        recordHistoryMutation(msg);
        updateMessageInDOM(msg.id, msg.content);
        updateMessageInCache(msg.id, msg.chat_id, msg.content);
        return;
    }
    _origHandleIncoming(msg);
};
