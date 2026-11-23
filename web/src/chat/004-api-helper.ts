// ===== API HELPER =====
function handleAuthExpired(notice?: string): void {
  if (authExpiredHandled) return;
  authExpiredHandled = true;
  if (typeof closeE2eeArchive === "function") closeE2eeArchive();
  leaveE2eeView();
  stopChatConnection();
  const finish = (): void => {
    // A login in another tab may already have replaced the token. Preserve that session.
    if (localStorage.getItem("token") === token) {
      localStorage.setItem("zchat_auth_revision", Date.now() + ":" + Math.random());
      if (notice) sessionStorage.setItem("zchat_login_notice", notice);
      localStorage.removeItem("token");
      localStorage.removeItem("username");
      localStorage.removeItem("avatar_url");
      localStorage.removeItem(SETTINGS_CACHE_KEY);
      localStorage.removeItem(NOTIFICATIONS_STORAGE_KEY);
    }
    window.location.href = "/login.html";
  };
  const complete = (): void => {
    if (navigator.locks) void navigator.locks.request("zchat-auth-state", finish).catch(finish);
    else finish();
  };
  const native = window.zchatDesktop?.e2ee;
  if (native?.compiled) void native.endSession().then(complete, complete);
  else complete();
}

async function apiFetch(url: string, options: ApiRequestOptions = {}): Promise<Response> {
  const headers = new Headers(options.headers);
  if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  return sessionFetch(url, { ...options, headers });
}

function isCallCleanupPending(response: Response): boolean {
  return response.status === 503 && response.headers.get("X-Zchat-Operation-Committed") === "true";
}

function pendingCallCleanupNotice(): string {
  return "Сервер звонков недоступен; отключение от звонка может задержаться.";
}

const activeSessionRequests = new Set<AbortController>();

function abortSessionRequests(): void {
  for (const controller of activeSessionRequests) controller.abort();
}

async function sessionFetch(url: string, options: ApiRequestOptions = {}): Promise<Response> {
  const destination = new URL(url, window.location.href);
  const applicationURL = window.zchatDesktop
    ? window.zchatDesktop.isApplicationURL(destination.href)
    : destination.origin === window.location.origin && ["http:", "https:"].includes(destination.protocol);
  if (!applicationURL || destination.username || destination.password) {
    throw new TypeError("Authenticated requests require the application origin");
  }
  if (localStorage.getItem("token") !== token) {
    handleAuthExpired();
    throw new DOMException("Session changed", "AbortError");
  }
  const headers = new Headers(options.headers);
  headers.set("Authorization", "Bearer " + token);
  const { timeoutMs, signal: callerSignal, ...requestOptions } = options;
  const controller = new AbortController();
  const abort = (): void => controller.abort(callerSignal?.reason);
  callerSignal?.addEventListener("abort", abort, { once: true });
  if (callerSignal?.aborted) abort();
  activeSessionRequests.add(controller);
  const transfer = options.body instanceof FormData || url.startsWith("/uploads/");
  const timer = window.setTimeout(() => controller.abort(new DOMException("Request timed out", "TimeoutError")),
    timeoutMs ?? (transfer ? 60000 : 15000));
  const finish = (): void => {
    clearTimeout(timer);
    callerSignal?.removeEventListener("abort", abort);
    activeSessionRequests.delete(controller);
  };
  controller.signal.addEventListener("abort", finish, { once: true });
  try {
    const response = await fetch(url, { ...requestOptions, redirect: "error", credentials: "omit", headers, signal: controller.signal });
    if (response.status === 401) {
      void response.body?.cancel().catch(() => {});
      finish();
      handleAuthExpired();
      return new Response(null, { status: response.status, statusText: response.statusText, headers: response.headers });
    }
    if (localStorage.getItem("token") !== token) {
      handleAuthExpired();
      throw new DOMException("Session changed", "AbortError");
    }
    if (!response.body) { finish(); return response; }
    // Keep the native response/stream: Chromium converts custom stream errors to
    // TypeError in Response.json(). Body readers preserve the actual abort reason.
    // No second attachment buffer or eagerly drained copy is needed.
    const cancelBody = response.body.cancel.bind(response.body);
    response.body.cancel = (reason?: unknown): Promise<void> => cancelBody(reason).finally(finish);
    for (const method of ["arrayBuffer", "blob", "formData", "json", "text"] as const) {
      const consume = response[method].bind(response);
      Object.defineProperty(response, method, { value: async () => {
        try { return await consume(); }
        catch (error) { throw controller.signal.aborted ? controller.signal.reason : error; }
        finally { finish(); }
      }});
    }
    return response;
  } catch (error) {
    finish();
    throw controller.signal.aborted ? controller.signal.reason : error;
  }
}

function loadChatKeySet(storageKey: string): Set<string> {
  try {
    const raw = JSON.parse(localStorage.getItem(storageKey) || "[]") as unknown;
    if (!Array.isArray(raw)) return new Set();
    return new Set(raw.map((item) => String(item || "")).filter(Boolean));
  } catch {
    return new Set();
  }
}

function persistChatKeySet(storageKey: string, values: Set<string>): void {
  if (localStorage.getItem("token") !== token) return;
  localStorage.setItem(storageKey, JSON.stringify(Array.from(values)));
}

function getChatByID(chatID: number | string | null | undefined): ChatRecord | null {
  const id = Number(chatID || 0);
  if (!id) return null;
  return chats.find((chat) => Number(chat.id) === id) || null;
}

function buildChatListTarget(chat: ChatRecord | null | undefined): ChatTarget {
  const isGroup = !!(chat && chat.type === "group");
  return {
    kind: isGroup ? "group" : "private",
    id: Number((chat && chat.id) || 0),
    chatType: isGroup ? "group" : "private",
    name: isGroup ? ((chat && chat.name) || "Группа") : ((chat && chat.other_username) || "..."),
    chat: chat || null,
  };
}

function getChatPreferenceKeys(target: ChatTarget | null | undefined): string[] {
  const keys: string[] = [];
  const numericID = Number(target && target.id ? target.id : 0);
  if (numericID > 0) keys.push("chat:" + numericID);
  return Array.from(new Set(keys.filter(Boolean)));
}

function hasChatPreference(values: Set<string>, target: ChatTarget | null | undefined): boolean {
  return getChatPreferenceKeys(target).some((key) => values.has(key));
}

function setChatPreference(values: Set<string>, target: ChatTarget | null | undefined, enabled: boolean, storageKey: string): boolean {
  const keys = getChatPreferenceKeys(target);
  if (!keys.length) return enabled;
  keys.forEach((key) => {
    if (enabled) values.add(key);
    else values.delete(key);
  });
  persistChatKeySet(storageKey, values);
  return enabled;
}

function isChatPinned(target: ChatTarget | null | undefined): boolean {
  return hasChatPreference(pinnedChatKeys, target);
}

function isChatMuted(target: ChatTarget | null | undefined): boolean {
  return hasChatPreference(mutedChatKeys, target);
}

function setChatPinned(target: ChatTarget | null | undefined, enabled: boolean): boolean {
  return setChatPreference(pinnedChatKeys, target, enabled, PINNED_CHATS_KEY);
}

function setChatMuted(target: ChatTarget | null | undefined, enabled: boolean): boolean {
  return setChatPreference(mutedChatKeys, target, enabled, MUTED_CHATS_KEY);
}

function isChatMutedByID(chatID: number | string | null | undefined): boolean {
  const numericID = Number(chatID || 0);
  if (!numericID) return false;
  const chat = getChatByID(numericID);
  if (chat) return isChatMuted(buildChatListTarget(chat));
  return mutedChatKeys.has("chat:" + numericID);
}

function truncateText(text: unknown, maxLength: number): string {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) return normalized;
  return normalized.slice(0, maxLength - 1) + "…";
}

function maybeShowUpdateShowcaseModal(): void {
  if (localStorage.getItem(UPDATE_SHOWCASE_STORAGE_KEY) === "1") return;
  const modal = optionalById<HTMLDivElement>("updateShowcaseModal");
  if (!modal) return;
  modal.style.display = "flex";
}

function closeUpdateShowcaseModal(): void {
  localStorage.setItem(UPDATE_SHOWCASE_STORAGE_KEY, "1");
  const modal = optionalById<HTMLDivElement>("updateShowcaseModal");
  if (modal) modal.style.display = "none";
}

function scheduleChatListRender(): void {
  if (chatListRenderScheduled) return;
  chatListRenderScheduled = true;
  requestAnimationFrame(() => {
    chatListRenderScheduled = false;
    renderChatList();
  });
}

function getLatestCachedChatMessage(chatID: number | string | null | undefined): ChatMessage | null {
  const numericID = Number(chatID || 0);
  const list = messageCache[numericID];
  if (!Array.isArray(list) || list.length === 0) return null;
  return list[list.length - 1] || null;
}

function buildChatPreviewText(chat: ChatRecord | null | undefined, msg: ChatMessage | null | undefined): string {
  if (!msg || !msg.content) return "";
  const chatType = (msg.type || (chat && chat.type) || "private");
  const rawText = truncateText(msg.content, 72);
  if (!rawText) return "";
  const isOwn = msg.sender_name === currentUsername;
  const isAnonymousGroup = chatType === "group" && Number(msg.sender_id || 0) === 0;

  if (chatType === "group") {
    if (isAnonymousGroup) return rawText;
    if (isOwn) return "Вы: " + rawText;
    if (msg.sender_name) return msg.sender_name + ": " + rawText;
  }

  if (isOwn) return "Вы: " + rawText;
  return rawText;
}

function setChatPreviewFromMessage(chatID: number | string | null | undefined, msg: ChatMessage | null | undefined, shouldRender = true): void {
  const numericID = Number(chatID || (msg && msg.chat_id) || 0);
  if (!numericID) return;
  const existing = chatPreviewCache[numericID];
  const nextMessageID = Number(msg && msg.id ? msg.id : 0);
  const existingMessageID = Number(existing && existing.messageID ? existing.messageID : 0);
  if (existing && existingMessageID && nextMessageID && nextMessageID < existingMessageID) {
    return;
  }
  const chat = getChatByID(numericID) || null;
  chatPreviewCache[numericID] = {
    text: buildChatPreviewText(chat, msg),
    messageID: nextMessageID,
    createdAt: msg && msg.created_at ? msg.created_at : "",
  };
  if (shouldRender) scheduleChatListRender();
}

function syncChatPreviewFromCache(chatID: number | string | null | undefined, shouldRender = true): void {
  const numericID = Number(chatID || 0);
  if (!numericID) return;
  const lastMsg = getLatestCachedChatMessage(numericID);
  if (!lastMsg) {
    delete chatPreviewCache[numericID];
    if (shouldRender) scheduleChatListRender();
    return;
  }
  delete chatPreviewCache[numericID];
  setChatPreviewFromMessage(numericID, lastMsg, shouldRender);
}

function getChatPreviewText(chatID: number | string | null | undefined): string {
  const preview = chatPreviewCache[Number(chatID || 0)];
  return preview && preview.text ? preview.text : "";
}

async function loadSingleChatPreview(chatID: number | string | null | undefined): Promise<void> {
  const numericID = Number(chatID || 0);
  if (!numericID || chatPreviewPending.has(numericID)) return;

  const cachedMessage = getLatestCachedChatMessage(numericID);
  if (cachedMessage) {
    setChatPreviewFromMessage(numericID, cachedMessage, true);
    return;
  }

  chatPreviewPending.add(numericID);
  try {
    const res = await apiFetch("/api/chats/" + numericID + "/messages?limit=1&last_message_id=0");
    if (!res.ok) return;
    const data = await res.json();
    const list = data.messages || [];
    if (list.length === 0) {
      delete chatPreviewCache[numericID];
      scheduleChatListRender();
      return;
    }
    const lastMsg = list[list.length - 1];
    setChatPreviewFromMessage(numericID, lastMsg, true);
  } catch (e) {
    console.error("Failed to load chat preview:", e);
  } finally {
    chatPreviewPending.delete(numericID);
  }
}

function loadChatPreviews(): void {
  chats.forEach((chat) => {
    loadSingleChatPreview(chat.id);
  });
}

function normalizeUserSettings(raw: Partial<UserSettings> | Record<string, unknown> | null | undefined): UserSettings {
  const src = raw || {};
  return {
    notifications_enabled: src.notifications_enabled !== false,
    notification_sound: src.notification_sound !== false,
    desktop_notifications: src.desktop_notifications === true,
    dnd_enabled: src.dnd_enabled === true,
    dnd_from_hour: Math.max(0, Math.min(23, Number(src.dnd_from_hour ?? 23))),
    dnd_to_hour: Math.max(0, Math.min(23, Number(src.dnd_to_hour ?? 8))),
    allow_friend_requests: src.allow_friend_requests !== false,
    allow_direct_messages: src.allow_direct_messages !== false,
    allow_direct_messages_from_non_friends: src.allow_direct_messages_from_non_friends === true,
    show_online_status: src.show_online_status !== false,
    show_email: src.show_email === true,
    microphone_mode: src.microphone_mode === "radio" ? "radio" : "toggle",
    microphone_ptt_key: normalizeMicrophonePTTKey(src.microphone_ptt_key),
    screen_share_resolution: normalizeScreenShareResolution(src.screen_share_resolution),
    screen_share_fps: normalizeScreenShareFPS(src.screen_share_fps),
  };
}

function normalizeMicrophonePTTKey(raw: unknown): string {
  const code = String(raw || "").trim();
  if (!code || code.length > 32) return "";
  if (!/^[A-Za-z0-9]+$/.test(code)) return "";
  if (BLOCKED_PTT_KEY_CODES.has(code)) return "";
  return code;
}

function normalizeScreenShareResolution(raw: unknown): ScreenShareResolution {
  const key = String(raw || "");
  return Object.prototype.hasOwnProperty.call(SCREEN_SHARE_RESOLUTION_PRESETS, key) ? (key as ScreenShareResolution) : "1080p";
}

function normalizeScreenShareFPS(raw: unknown): number {
  const fps = Number(raw || 0);
  return SCREEN_SHARE_FPS_OPTIONS.includes(fps) ? fps : 30;
}

function formatKeyboardCodeLabel(code: unknown): string {
  const normalized = normalizeMicrophonePTTKey(code);
  if (!normalized) return "Не назначена";
  if (normalized === "Space") return "Пробел";
  if (normalized === "CapsLock") return "Caps Lock";
  if (normalized === "Enter") return "Enter";
  if (normalized === "Tab") return "Tab";
  if (normalized.startsWith("Key") && normalized.length === 4) return normalized.slice(3);
  if (normalized.startsWith("Digit")) return normalized.slice(5);
  if (normalized.startsWith("Numpad")) return "Num " + normalized.slice(6);
  const symbolMap = {
    Backquote: "`",
    Minus: "-",
    Equal: "=",
    BracketLeft: "[",
    BracketRight: "]",
    Backslash: "\\",
    Semicolon: ";",
    Quote: "'",
    Comma: ",",
    Period: ".",
    Slash: "/",
  };
  return symbolMap[normalized] || normalized;
}

function getConfiguredRadioHotkeyCode(): string {
  return normalizeMicrophonePTTKey(userSettings.microphone_ptt_key);
}

function getScreenSharePreset(settings?: Partial<UserSettings> | null): ScreenSharePreset & { resolution: ScreenShareResolution; fps: number } {
  const source = settings || userSettings;
  const resolution = normalizeScreenShareResolution(source.screen_share_resolution);
  const fps = normalizeScreenShareFPS(source.screen_share_fps);
  return {
    resolution: resolution,
    fps: fps,
    ...SCREEN_SHARE_RESOLUTION_PRESETS[resolution],
  };
}

function buildScreenSharePresetLabel(settings?: Partial<UserSettings> | null): string {
  const preset = getScreenSharePreset(settings);
  return preset.label + " · " + preset.fps + " FPS";
}

function isEditableEventTarget(target: EventTarget | null): boolean {
  if (!target || !(target instanceof Element)) return false;
  if (target.closest("[contenteditable='true']")) return true;
  const field = target.closest("input, textarea, select");
  return !!field;
}

function persistUserSettings(): void {
  if (localStorage.getItem("token") === token) localStorage.setItem(SETTINGS_CACHE_KEY, JSON.stringify(userSettings));
}

function loadCachedUserSettings(): void {
  try {
    const raw = JSON.parse(localStorage.getItem(SETTINGS_CACHE_KEY) || "{}") as Partial<UserSettings>;
    userSettings = normalizeUserSettings(raw);
  } catch {
    userSettings = { ...DEFAULT_USER_SETTINGS };
  }
}

function isDNDActive(): boolean {
  if (!userSettings.dnd_enabled) return false;
  const from = Number(userSettings.dnd_from_hour);
  const to = Number(userSettings.dnd_to_hour);
  const hour = new Date().getHours();
  if (from === to) return true;
  if (from < to) return hour >= from && hour < to;
  return hour >= from || hour < to;
}

function canShowAppNotifications(): boolean {
  if (!userSettings.notifications_enabled) return false;
  if (isDNDActive()) return false;
  return true;
}

function maybeShowDesktopNotification(title: string, body?: string | null): void {
  if (!userSettings.desktop_notifications) return;
  if (!("Notification" in window)) return;
  if (Notification.permission !== "granted") return;
  if (document.hasFocus()) return;
  try {
    const n = new Notification(title, { body: body || "" });
    setTimeout(() => n.close(), 4500);
  } catch (e) {
    console.debug("desktop notification disabled:", e);
  }
}

function applyUserSettingsRuntime(): void {
  syncNotificationSoundPolicy();
  const statusText = queryOne<HTMLElement>(".status-text");
  if (statusText) {
    statusText.textContent = userSettings.show_online_status ? "online" : "скрыт";
    statusText.style.color = userSettings.show_online_status ? "var(--green)" : "var(--text-tertiary)";
  }
  renderFriendRequests(requests);
  if (!localTracks.screen) {
    loadCallScreenShareDraftFromSettings();
  }
  applyCallMicrophoneModeRuntime();
  syncCallScreenSharePanelFromState();
}

async function loadUserSettings(): Promise<void> {
  loadCachedUserSettings();
  applyUserSettingsRuntime();
  try {
    const res = await apiFetch("/api/settings");
    if (!res.ok) return;
    const data = await res.json() as Partial<UserSettings>;
    userSettings = normalizeUserSettings(data);
    persistUserSettings();
    applyUserSettingsRuntime();
    syncSettingsFormFromState();
  } catch (e) {
    console.error("Failed to load user settings:", e);
  }
}
