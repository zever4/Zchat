// ===== STATE =====
let currentChatID: number | null = null;
let currentChatType: string | null = null;
let chats: ChatRecord[] = [];
let requests: FriendRequestRecord[] = [];
let messageCache: MessageCacheMap = {};
let currentAvatarURL = localStorage.getItem("avatar_url") || "";
let isChatLoading = false;
let inviteJoinHandled = false;
const groupOwnerByChat: GroupOwnerByChatMap = {};
const chatPreviewCache: ChatPreviewCacheMap = {};
const chatPreviewPending = new Set<number>();
let chatListRenderScheduled = false;

const SETTINGS_CACHE_KEY = accountStorageKey("zchat_user_settings");
const NOTIFICATIONS_STORAGE_KEY = accountStorageKey("zchat_notifs");
const UPDATE_SHOWCASE_VERSION = "2026-03-chat-sidebar-v1";
const UPDATE_SHOWCASE_STORAGE_KEY = "zchat_update_showcase_seen_" + UPDATE_SHOWCASE_VERSION;
const DEFAULT_USER_SETTINGS: UserSettings = {
  notifications_enabled: true,
  notification_sound: true,
  desktop_notifications: false,
  dnd_enabled: false,
  dnd_from_hour: 23,
  dnd_to_hour: 8,
  allow_friend_requests: true,
  allow_direct_messages: true,
  allow_direct_messages_from_non_friends: false,
  show_online_status: true,
  show_email: false,
  microphone_mode: "toggle",
  microphone_ptt_key: "",
  screen_share_resolution: "1080p",
  screen_share_fps: 30,
};
const SCREEN_SHARE_RESOLUTION_PRESETS: Record<ScreenShareResolution, ScreenSharePreset> = {
  "720p": { width: 1280, height: 720, label: "1280 × 720", maxBitrate: 1800000 },
  "1080p": { width: 1920, height: 1080, label: "1920 × 1080", maxBitrate: 3200000 },
  "1440p": { width: 2560, height: 1440, label: "2560 × 1440", maxBitrate: 5200000 },
};
const SCREEN_SHARE_FPS_OPTIONS: number[] = [10, 15, 30, 60];
const BLOCKED_PTT_KEY_CODES = new Set<string>([
  "AltLeft", "AltRight",
  "ControlLeft", "ControlRight",
  "ShiftLeft", "ShiftRight",
  "MetaLeft", "MetaRight",
  "Escape",
]);
let userSettings: UserSettings = { ...DEFAULT_USER_SETTINGS };
let settingsProfileEmailRaw = "—";
let authExpiredHandled = false;
const PINNED_CHATS_KEY = accountStorageKey("zchat_pinned_chats");
const MUTED_CHATS_KEY = accountStorageKey("zchat_muted_chats");
let pinnedChatKeys: Set<string> = loadChatKeySet(PINNED_CHATS_KEY);
let mutedChatKeys: Set<string> = loadChatKeySet(MUTED_CHATS_KEY);
let activeChatListMenuTarget: ChatTarget | null = null;
