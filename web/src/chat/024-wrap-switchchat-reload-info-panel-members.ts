// ===== WRAP switchChat — reload info panel members =====
const _origSwitchChat = switchChat;
switchChat = function(chatID: number | string, chatName?: string | null, chatType?: string | null): void {
    _origSwitchChat(chatID, chatName, chatType);
    if (infoPanelOpen) loadChatMembers(chatID);
};
