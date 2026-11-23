type ChatKind = "private" | "group";
type ChatTargetKind = ChatKind;
type MediaMessageTag = "img" | "gif" | "voice";
type CallStateValue = "idle" | "calling" | "ringing" | "in_call";
type CallMediaType = "audio" | "video";
type MicrophoneMode = "toggle" | "radio";
type ScreenShareResolution = "720p" | "1080p" | "1440p";
type GroupRole = "owner" | "admin" | "member";
type SettingsScope = "notifications" | "privacy" | "microphone" | "screen" | "sessions" | "security";
type FeedbackType = "success" | "error";

interface ChatRecord {
  id: number | string;
  type: string;
  name?: string;
  other_username?: string;
  other_user_id?: number | string;
  other_avatar_url?: string;
  avatar_url?: string;
  [key: string]: any;
}

interface ChatMessage {
  id?: number | string;
  chat_id?: number | string;
  sender_id?: number | string;
  sender_name?: string;
  sender_avatar?: string;
  created_at?: string;
  content?: string;
  type?: string;
  [key: string]: any;
}

interface IncomingCallMessage extends ChatMessage {
  _timeout?: number;
}

interface FriendRequestRecord {
  id: number | string;
  from_user_id?: number | string;
  from_username: string;
  status?: string;
  created_at?: string;
  [key: string]: any;
}

interface UserProfileResponse {
  id?: number | string;
  username?: string;
  avatar_url?: string;
  bio?: string;
  email?: string;
  [key: string]: any;
}

interface GroupSettingsRecord extends ChatRecord {
  owner_id?: number | string;
  ownerID?: number | string;
  role?: GroupRole | string;
  is_owner?: boolean;
  is_public?: boolean;
  member_count?: number | string;
  admins_only_write?: boolean;
  hide_members_list?: boolean;
  anonymous_mode?: boolean;
}

interface GroupInviteRecord {
  id: number | string;
  code?: string;
  invite_link?: string;
  is_active?: boolean;
  created_at?: string;
  [key: string]: any;
}

interface GroupJoinRequestRecord {
  user_id: number | string;
  username?: string;
  created_at?: string;
  [key: string]: any;
}

interface GroupAuditLogRecord {
  actor_user_id?: number | string;
  actor_username?: string;
  created_at?: string;
  action?: string;
  details_json?: unknown;
  [key: string]: any;
}

interface GroupMemberRecord {
  user_id: number | string;
  username?: string;
  avatar_url?: string;
  role?: GroupRole | string;
  [key: string]: any;
}

interface ChatMemberListItem {
  id: number | string;
  username?: string;
  avatar_url?: string;
  [key: string]: any;
}

interface GroupRuntimeInfo {
  role: GroupRole | string;
  admins_only_write: boolean;
  hide_members_list: boolean;
  anonymous_mode: boolean;
  member_count: number;
  name: string;
}

interface SearchUserResult {
  id: number | string;
  username: string;
  [key: string]: any;
}

interface JwtClaims {
  iat?: number | string;
  exp?: number | string;
  [key: string]: any;
}

interface InviteCardMeta {
  chat_id?: number | string;
  is_member?: boolean;
  request_status?: string;
  is_public?: boolean;
  avatar_url?: string;
  name?: string;
  message?: string;
  [key: string]: any;
}

interface RenderedMessageContent {
  textHTML: string;
  cardHTML: string;
}

interface NotificationExtra {
  requestId?: number | string;
  fromUsername?: string;
  chatId?: number;
  [key: string]: any;
}

interface NotificationRecord {
  id: number;
  type: string;
  text: string;
  time: string;
  unread: boolean;
  extra: NotificationExtra | null;
}

interface ScreenSharePreset {
  width: number;
  height: number;
  label: string;
  maxBitrate: number;
}

interface UserSettings {
  notifications_enabled: boolean;
  notification_sound: boolean;
  desktop_notifications: boolean;
  dnd_enabled: boolean;
  dnd_from_hour: number;
  dnd_to_hour: number;
  allow_friend_requests: boolean;
  allow_direct_messages: boolean;
  allow_direct_messages_from_non_friends: boolean;
  show_online_status: boolean;
  show_email: boolean;
  microphone_mode: MicrophoneMode;
  microphone_ptt_key: string;
  screen_share_resolution: ScreenShareResolution;
  screen_share_fps: number;
}

interface ChatTarget {
  kind: ChatTargetKind;
  id: number;
  chatType: ChatKind;
  name: string;
  chat: ChatRecord | null;
}

interface ChatPreviewEntry {
  text: string;
  messageID: number;
  createdAt: string;
}

type MessageCacheMap = Record<number, ChatMessage[]>;
type ChatPreviewCacheMap = Record<number, ChatPreviewEntry>;
type GroupOwnerByChatMap = Record<number, number | null>;
type GroupInfoCacheMap = Record<number, GroupRuntimeInfo>;
type CallParticipantAudioElementMap = Record<string, HTMLAudioElement[]>;
type ApiRequestOptions = RequestInit & { headers?: HeadersInit; timeoutMs?: number };
type RenderMessageFn = (msg: ChatMessage, isPrepend?: boolean) => void;
type RenderFriendRequestsFn = (requests: FriendRequestRecord[]) => void;
type HandleIncomingMessageFn = (msg: ChatMessage) => void;
type SwitchChatFn = (chatID: number | string, chatName?: string | null, chatType?: string | null) => void;
type ShowContextMenuFn = (
  x: number,
  y: number,
  msgID: number | string | null | undefined,
  chatID: number | string | null | undefined,
  el: HTMLElement | null,
) => void;

function byId<T extends HTMLElement = HTMLElement>(id: string): T {
  return document.getElementById(id) as T;
}

function optionalById<T extends HTMLElement = HTMLElement>(id: string): T | null {
  return document.getElementById(id) as T | null;
}

function queryOne<T extends Element = Element>(selector: string): T | null {
  return document.querySelector(selector) as T | null;
}

function queryAll<T extends Element = Element>(selector: string): T[] {
  return Array.from(document.querySelectorAll(selector)) as T[];
}

// Public avatars use the selected server through the native transport.
function displayAssetURL(value: string): string {
  return window.zchatDesktop ? window.zchatDesktop.resourceURL(value) : value;
}
