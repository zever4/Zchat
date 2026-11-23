// ===== DOM ELEMENTS =====
const messagesEl = byId<HTMLDivElement>("messages");
const messageInput = byId<HTMLTextAreaElement>("messageInput");
const sendBtn = byId<HTMLButtonElement>("sendBtn");
const myUsernameEl = byId<HTMLElement>("myUsername");
const myAvatarEl = byId<HTMLDivElement>("myAvatar");
const chatNameEl = byId<HTMLElement>("chatName");
const chatStatusEl = byId<HTMLElement>("chatStatus");
const privateChatsList = byId<HTMLDivElement>("privateChatsList");
const friendRequestsSection = byId<HTMLDivElement>("friendRequestsSection");
const friendRequestsList = byId<HTMLDivElement>("friendRequestsList");
const requestCountEl = byId<HTMLElement>("requestCount");
const typingIndicatorEl = byId<HTMLDivElement>("typingIndicator");
const groupSettingsBtn = optionalById<HTMLButtonElement>("groupSettingsBtn");
