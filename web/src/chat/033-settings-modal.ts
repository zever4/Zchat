// ===== SETTINGS MODAL =====
const SETTINGS_TABS = ["account", "notifications", "microphone", "screen", "privacy", "security", "sessions"] as const;
let microphonePttKeyCaptureActive = false;

async function resetDesktopMicrophonePermission(): Promise<void> {
    const desktop = window.zchatDesktop;
    const button = optionalById<HTMLButtonElement>("resetMicrophonePermission");
    if (!desktop || !button || button.disabled) return;
    hideSettingsFeedback("microphone");
    if (callState !== "idle" || document.getElementById("voiceBtn")?.classList.contains("recording") || (typeof e2eeVoiceBusy === "function" && e2eeVoiceBusy())) {
        showSettingsFeedback("microphone", "error", "Завершите звонок или запись голосового сообщения перед сбросом доступа.");
        return;
    }
    button.disabled = true;
    try {
        await desktop.resetMicrophonePermission();
        // Apply the profile change to a fresh document; account storage stays intact.
        location.reload();
    } catch (error) {
        button.disabled = false;
        showSettingsFeedback("microphone", "error", typeof error === "string" ? error : error instanceof Error ? error.message : "Не удалось сбросить разрешение микрофона.");
    }
}

for (const id of ["desktopMicrophonePermission", "desktopMicrophoneDivider"]) {
    const element = document.getElementById(id);
    if (element) element.hidden = !window.zchatDesktop || !navigator.userAgent.includes("Windows");
}

function getSettingsFeedbackElements(scope: SettingsScope) {
    if (scope === "notifications") {
        return {
            error: optionalById<HTMLDivElement>("settingsNotifError"),
            success: optionalById<HTMLDivElement>("settingsNotifSuccess"),
        };
    }
    if (scope === "privacy") {
        return {
            error: optionalById<HTMLDivElement>("settingsPrivacyError"),
            success: optionalById<HTMLDivElement>("settingsPrivacySuccess"),
        };
    }
    if (scope === "microphone") {
        return {
            error: optionalById<HTMLDivElement>("settingsMicrophoneError"),
            success: optionalById<HTMLDivElement>("settingsMicrophoneSuccess"),
        };
    }
    if (scope === "screen") {
        return {
            error: optionalById<HTMLDivElement>("settingsScreenError"),
            success: optionalById<HTMLDivElement>("settingsScreenSuccess"),
        };
    }
    if (scope === "sessions") {
        return {
            error: optionalById<HTMLDivElement>("settingsSessionsError"),
            success: optionalById<HTMLDivElement>("settingsSessionsSuccess"),
        };
    }
    return {
        error: optionalById<HTMLDivElement>("settingsPasswordError"),
        success: optionalById<HTMLDivElement>("settingsPasswordSuccess"),
    };
}

function hideSettingsFeedback(scope: SettingsScope): void {
    const box = getSettingsFeedbackElements(scope);
    if (box.error) box.error.style.display = "none";
    if (box.success) box.success.style.display = "none";
}

function showSettingsFeedback(scope: SettingsScope, type: FeedbackType, message: string): void {
    const box = getSettingsFeedbackElements(scope);
    const el = type === "success" ? box.success : box.error;
    const opposite = type === "success" ? box.error : box.success;
    if (opposite) opposite.style.display = "none";
    if (!el) return;
    el.textContent = message;
    el.style.display = "block";
}

function formatSettingsHour(hour: number | string | null | undefined): string {
    const h = String(Math.max(0, Math.min(23, Number(hour || 0)))).padStart(2, "0");
    return h + ":00";
}

function ensureDNDSelectsFilled(): void {
    const fromSel = optionalById<HTMLSelectElement>("settingsDndFrom");
    const toSel = optionalById<HTMLSelectElement>("settingsDndTo");
    if (!fromSel || !toSel || fromSel.options.length > 0) return;
    for (let h = 0; h <= 23; h += 1) {
        const opt1 = document.createElement("option");
        opt1.value = String(h);
        opt1.textContent = formatSettingsHour(h);
        fromSel.appendChild(opt1);

        const opt2 = document.createElement("option");
        opt2.value = String(h);
        opt2.textContent = formatSettingsHour(h);
        toSel.appendChild(opt2);
    }
}

function syncMicrophonePttKeyUI(): void {
    const hidden = optionalById<HTMLInputElement>("settingsMicrophonePttKey");
    const btn = optionalById<HTMLButtonElement>("settingsMicrophonePttKeyBtn");
    const hint = optionalById<HTMLElement>("settingsMicrophonePttKeyHint");
    const keyCode = normalizeMicrophonePTTKey(hidden ? hidden.value : userSettings.microphone_ptt_key);
    const keyLabel = formatKeyboardCodeLabel(keyCode);

    if (hidden) hidden.value = keyCode;
    if (btn) {
        btn.classList.toggle("is-listening", microphonePttKeyCaptureActive);
        btn.textContent = microphonePttKeyCaptureActive
            ? "Нажмите клавишу..."
            : (keyCode ? keyLabel : "Назначить");
    }
    if (hint) {
        hint.textContent = microphonePttKeyCaptureActive
            ? "Нажмите удобную клавишу. Esc отменит ввод, модификаторы отдельно не поддерживаются."
            : (keyCode
                ? "Сейчас используется: " + keyLabel + ". Работает только в звонке и не мешает набору текста."
                : "Клавиша не назначена. Можно продолжать удерживать кнопку микрофона мышью.");
    }
}

function startMicrophonePttKeyCapture(): void {
    microphonePttKeyCaptureActive = true;
    hideSettingsFeedback("microphone");
    syncMicrophonePttKeyUI();
}

function stopMicrophonePttKeyCapture(): void {
    microphonePttKeyCaptureActive = false;
    syncMicrophonePttKeyUI();
}

function toggleMicrophonePttKeyCapture(): void {
    if (microphonePttKeyCaptureActive) {
        stopMicrophonePttKeyCapture();
        return;
    }
    startMicrophonePttKeyCapture();
}

function clearMicrophonePttKey(): void {
    const hidden = optionalById<HTMLInputElement>("settingsMicrophonePttKey");
    if (hidden) hidden.value = "";
    stopMicrophonePttKeyCapture();
    hideSettingsFeedback("microphone");
}

function syncSettingsFormFromState(): void {
    ensureDNDSelectsFilled();
    const map: Array<[string, keyof UserSettings]> = [
        ["settingsNotifEnabled", "notifications_enabled"],
        ["settingsNotifSound", "notification_sound"],
        ["settingsNotifDesktop", "desktop_notifications"],
        ["settingsDndEnabled", "dnd_enabled"],
        ["settingsPrivacyOnline", "show_online_status"],
        ["settingsPrivacyFriends", "allow_friend_requests"],
        ["settingsPrivacyEmail", "show_email"],
    ];
    map.forEach(([id, key]) => {
        const el = optionalById<HTMLInputElement>(id);
        if (el) el.checked = !!userSettings[key];
    });
    const audience = optionalById<HTMLSelectElement>("settingsPrivacyDM");
    if (audience) audience.value = !userSettings.allow_direct_messages ? "nobody"
        : userSettings.allow_direct_messages_from_non_friends ? "everyone" : "friends";
    const fromSel = optionalById<HTMLSelectElement>("settingsDndFrom");
    const toSel = optionalById<HTMLSelectElement>("settingsDndTo");
    if (fromSel) fromSel.value = String(userSettings.dnd_from_hour);
    if (toSel) toSel.value = String(userSettings.dnd_to_hour);
    const micMode = optionalById<HTMLSelectElement>("settingsMicrophoneMode");
    if (micMode) micMode.value = String(userSettings.microphone_mode || "toggle");
    const micPtt = optionalById<HTMLInputElement>("settingsMicrophonePttKey");
    if (micPtt) micPtt.value = String(userSettings.microphone_ptt_key || "");
    syncMicrophonePttKeyUI();
    const screenResolution = optionalById<HTMLSelectElement>("settingsScreenShareResolution");
    const screenFPS = optionalById<HTMLSelectElement>("settingsScreenShareFPS");
    if (screenResolution) screenResolution.value = String(userSettings.screen_share_resolution || "1080p");
    if (screenFPS) screenFPS.value = String(userSettings.screen_share_fps || 30);
    const emailValue = optionalById<HTMLElement>("settingsEmail");
    if (emailValue) {
        emailValue.textContent = userSettings.show_email ? settingsProfileEmailRaw : "Скрыт настройкой конфиденциальности";
    }
}

function collectSettingsFromForm(): UserSettings {
    const audience = optionalById<HTMLSelectElement>("settingsPrivacyDM")?.value;
    return normalizeUserSettings({
        notifications_enabled: !!optionalById<HTMLInputElement>("settingsNotifEnabled")?.checked,
        notification_sound: !!optionalById<HTMLInputElement>("settingsNotifSound")?.checked,
        desktop_notifications: !!optionalById<HTMLInputElement>("settingsNotifDesktop")?.checked,
        dnd_enabled: !!optionalById<HTMLInputElement>("settingsDndEnabled")?.checked,
        dnd_from_hour: Number(optionalById<HTMLSelectElement>("settingsDndFrom")?.value || 23),
        dnd_to_hour: Number(optionalById<HTMLSelectElement>("settingsDndTo")?.value || 8),
        allow_friend_requests: !!optionalById<HTMLInputElement>("settingsPrivacyFriends")?.checked,
        allow_direct_messages: audience === "everyone" || audience === "friends",
        allow_direct_messages_from_non_friends: audience === "everyone",
        show_online_status: !!optionalById<HTMLInputElement>("settingsPrivacyOnline")?.checked,
        show_email: !!optionalById<HTMLInputElement>("settingsPrivacyEmail")?.checked,
        microphone_mode: String(optionalById<HTMLSelectElement>("settingsMicrophoneMode")?.value || "toggle"),
        microphone_ptt_key: String(optionalById<HTMLInputElement>("settingsMicrophonePttKey")?.value || ""),
        screen_share_resolution: String(optionalById<HTMLSelectElement>("settingsScreenShareResolution")?.value || "1080p"),
        screen_share_fps: Number(optionalById<HTMLSelectElement>("settingsScreenShareFPS")?.value || 30),
    });
}

async function ensureDesktopPermissionIfNeeded(nextSettings: UserSettings): Promise<boolean> {
    if (!nextSettings.desktop_notifications) return true;
    if (!("Notification" in window)) return false;
    if (Notification.permission === "granted") return true;
    if (Notification.permission === "denied") return false;
    const permission = await Notification.requestPermission();
    return permission === "granted";
}

function decodeTokenClaims(): JwtClaims | null {
    try {
        const payload = String(token || "").split(".")[1];
        if (!payload) return null;
        const b64 = payload.replace(/-/g, "+").replace(/_/g, "/");
        const json = decodeURIComponent(
            atob(b64).split("").map((c) => "%" + c.charCodeAt(0).toString(16).padStart(2, "0")).join("")
        );
        return JSON.parse(json);
    } catch {
        return null;
    }
}

function getSessionDeviceLabel(): string {
    const ua = navigator.userAgent || "";
    if (ua.includes("Windows")) return "Windows • Web";
    if (ua.includes("Mac OS")) return "macOS • Web";
    if (ua.includes("Linux")) return "Linux • Web";
    if (ua.includes("Android")) return "Android • Web";
    if (ua.includes("iPhone") || ua.includes("iPad")) return "iOS • Web";
    return "Web браузер";
}

function formatSessionDate(unix: number | string | null | undefined): string {
    if (!unix) return "—";
    const dt = new Date(Number(unix) * 1000);
    if (isNaN(dt.getTime())) return "—";
    return dt.toLocaleString("ru-RU", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
    });
}

function fillSessionInfo(): void {
    const claims = decodeTokenClaims() || {};
    const deviceEl = optionalById<HTMLElement>("settingsSessionDevice");
    const issuedEl = optionalById<HTMLElement>("settingsSessionIssued");
    const expireEl = optionalById<HTMLElement>("settingsSessionExpire");
    if (deviceEl) deviceEl.textContent = getSessionDeviceLabel();
    if (issuedEl) issuedEl.textContent = formatSessionDate(claims.iat);
    if (expireEl) expireEl.textContent = formatSessionDate(claims.exp);
}

async function openSettingsModal(): Promise<void> {
    syncImagePreferenceSettings();
    const modal = byId<HTMLDivElement>("settingsModal");
    modal.style.display = "flex";
    switchSettingsTab("account");

    byId<HTMLInputElement>("settingsOldPassword").value = "";
    byId<HTMLInputElement>("settingsNewPassword").value = "";
    byId<HTMLInputElement>("settingsConfirmPassword").value = "";
    hideSettingsFeedback("security");
    hideSettingsFeedback("notifications");
    hideSettingsFeedback("microphone");
    hideSettingsFeedback("screen");
    hideSettingsFeedback("privacy");
    hideSettingsFeedback("sessions");
    ensureDNDSelectsFilled();
    fillSessionInfo();

    loadCachedUserSettings();
    syncSettingsFormFromState();
    applyUserSettingsRuntime();

    try {
        const [profileRes, settingsRes] = await Promise.all([
            apiFetch("/api/profile"),
            apiFetch("/api/settings"),
        ]);

        if (profileRes.ok) {
            const profile = await profileRes.json() as UserProfileResponse;
            settingsProfileEmailRaw = profile.email || "—";
            byId<HTMLElement>("settingsUsername").textContent = profile.username || "—";
        }
        if (settingsRes.ok) {
            userSettings = normalizeUserSettings(await settingsRes.json() as Partial<UserSettings>);
            persistUserSettings();
        }
        syncSettingsFormFromState();
        applyUserSettingsRuntime();
    } catch (e) {
        console.error("Failed to load settings:", e);
    }
}

function closeSettingsModal(): void {
    stopAppSound("preview");
    stopE2eeSettings();
    stopMicrophonePttKeyCapture();
    byId<HTMLDivElement>("settingsModal").style.display = "none";
}

document.addEventListener("keydown", (e) => {
    if (!microphonePttKeyCaptureActive) return;
    e.preventDefault();
    e.stopPropagation();

    if (e.code === "Escape") {
        stopMicrophonePttKeyCapture();
        return;
    }
    if (e.repeat) return;

    const nextCode = normalizeMicrophonePTTKey(e.code);
    if (!nextCode) {
        showSettingsFeedback("microphone", "error", "Эту клавишу нельзя использовать для рации.");
        return;
    }

    const hidden = optionalById<HTMLInputElement>("settingsMicrophonePttKey");
    if (hidden) hidden.value = nextCode;
    stopMicrophonePttKeyCapture();
    showSettingsFeedback("microphone", "success", "Клавиша рации выбрана: " + formatKeyboardCodeLabel(nextCode) + ".");
}, true);

function switchSettingsTab(tab: string): void {
    stopAppSound("preview");
    stopE2eeSettings();
    queryAll<HTMLElement>(".settings-nav-item").forEach((el) => {
        el.classList.toggle("active", el.dataset.tab === tab);
    });
    SETTINGS_TABS.forEach((name) => {
        const node = optionalById<HTMLElement>("settingsTab" + name.charAt(0).toUpperCase() + name.slice(1));
        if (node) node.classList.toggle("active", name === tab);
    });
    if (tab === "security") void refreshE2eeSettings();
    if (tab === "notifications") void refreshNotificationSoundSettings();
}

async function saveSettingsPreferences(scope: SettingsScope): Promise<void> {
    hideSettingsFeedback(scope);
    const nextSettings = collectSettingsFromForm();
    const hasDesktopPermission = await ensureDesktopPermissionIfNeeded(nextSettings);
    if (!hasDesktopPermission && nextSettings.desktop_notifications) {
        showSettingsFeedback(scope, "error", "Браузер не дал разрешение на desktop-уведомления.");
        return;
    }

    try {
        const res = await apiFetch("/api/settings", {
            method: "PUT",
            body: JSON.stringify(nextSettings),
        });
        if (!res.ok) {
            const text = await res.text();
            showSettingsFeedback(scope, "error", text || "Не удалось сохранить настройки.");
            return;
        }
        userSettings = normalizeUserSettings(nextSettings);
        persistUserSettings();
        syncSettingsFormFromState();
        applyUserSettingsRuntime();
        showSettingsFeedback(scope, "success", "Настройки сохранены.");
    } catch (e) {
        showSettingsFeedback(scope, "error", "Ошибка сети при сохранении.");
    }
}

async function logoutCurrentSession(): Promise<void> {
    try {
        const response=await apiFetch("/api/profile/logout",{method:"POST"});
        if (isCallCleanupPending(response)) {
            await response.body?.cancel();
            handleAuthExpired("Сессия завершена. " + pendingCallCleanupNotice());
            return;
        }
        if (response.ok || response.status===401) { handleAuthExpired(); return; }
        alert("Не удалось завершить сессию. Попробуйте ещё раз.");
    } catch { if (!authExpiredHandled) alert("Нет связи с сервером. Сессия ещё не завершена."); }
}

async function logoutAllSessions(): Promise<void> {
    hideSettingsFeedback("sessions");
    if (!confirm("Выйти со всех устройств? Текущая сессия тоже завершится.")) return;
    try {
        const res = await apiFetch("/api/profile/logout-all", { method: "POST" });
        if (isCallCleanupPending(res)) {
            await res.body?.cancel();
            handleAuthExpired("Все сессии завершены. " + pendingCallCleanupNotice());
            return;
        }
        if (!res.ok) {
            const text = await res.text();
            showSettingsFeedback("sessions", "error", text || "Не удалось завершить все сессии.");
            return;
        }
        showSettingsFeedback("sessions", "success", "Все сессии завершены. Выполняю выход...");
        handleAuthExpired();
    } catch (e) {
        showSettingsFeedback("sessions", "error", "Ошибка сети при завершении сессий.");
    }
}

async function changePassword(): Promise<void> {
    const oldPass = byId<HTMLInputElement>("settingsOldPassword").value;
    const newPass = byId<HTMLInputElement>("settingsNewPassword").value;
    const confirmPass = byId<HTMLInputElement>("settingsConfirmPassword").value;
    const errorEl = byId<HTMLDivElement>("settingsPasswordError");
    const successEl = byId<HTMLDivElement>("settingsPasswordSuccess");

    errorEl.style.display = "none";
    successEl.style.display = "none";

    if (!oldPass) {
        errorEl.textContent = "Введите текущий пароль";
        errorEl.style.display = "block";
        return;
    }
    if (newPass.length < 6) {
        errorEl.textContent = "Новый пароль слишком короткий (минимум 6 символов)";
        errorEl.style.display = "block";
        return;
    }
    if (newPass !== confirmPass) {
        errorEl.textContent = "Пароли не совпадают";
        errorEl.style.display = "block";
        return;
    }

    try {
        const res = await apiFetch("/api/profile/password", {
            method: "PUT",
            body: JSON.stringify({ old_password: oldPass, new_password: newPass })
        });
        if (res.ok || isCallCleanupPending(res)) {
            const mediaPending = isCallCleanupPending(res);
            await res.body?.cancel();
            successEl.textContent = "Пароль изменён. Войдите заново на всех устройствах.";
            successEl.style.display = "block";
            byId<HTMLInputElement>("settingsOldPassword").value = "";
            byId<HTMLInputElement>("settingsNewPassword").value = "";
            byId<HTMLInputElement>("settingsConfirmPassword").value = "";
            handleAuthExpired("Пароль изменён. Все сессии завершены — войдите с новым паролем." +
                (mediaPending ? " " + pendingCallCleanupNotice() : ""));
        } else {
            const text = await res.text();
            errorEl.textContent = text || "Ошибка при смене пароля";
            errorEl.style.display = "block";
        }
    } catch (e) {
        errorEl.textContent = "Ошибка сети";
        errorEl.style.display = "block";
    }
}
