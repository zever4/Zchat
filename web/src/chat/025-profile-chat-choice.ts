// Creation choices only. Native owns every creation request/key/session; no
// ordinary outbox or chat ID is reused for a protected conversation.
function profilePeerId(value: unknown): string | null {
    if (typeof value === "number" && !Number.isSafeInteger(value)) return null;
    if (typeof value !== "number" && typeof value !== "string") return null;
    const id = String(value);
    return /^[1-9][0-9]{0,18}$/.test(id) && BigInt(id) <= 9223372036854775807n ? id : null;
}
function profilePeerUUID(value: unknown): value is string {
    return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);
}
function profileE2eeError(error: unknown): string {
    if (error === "forbidden") return "Создать диалог не удалось. Проверьте, что вы друзья и оба разрешаете новые личные сообщения.";
    if (error === "conflict") return "Состояние диалога изменилось. Откройте профиль заново и проверьте его ещё раз.";
    if (error === "not_ready") return "Завершите настройку ключей в разделе «Настройки → Безопасность» и повторите попытку.";
    if (error === "busy") return "Другая операция ещё выполняется. Попробуйте немного позже.";
    return "Не удалось получить результат. Повторите попытку: сохранённая операция продолжится. Обычный чат автоматически не создаётся.";
}
async function mountProfileChatChoices(
    root: HTMLElement,
    peer: { id: string; username: string },
    current: () => boolean,
    signal: AbortSignal,
): Promise<void> {
    const section = document.createElement("section");
    section.className = "profile-chat-choices";
    const heading = document.createElement("h4");
    heading.textContent = "Переписка";
    const ordinary = document.createElement("button");
    ordinary.type = "button";
    ordinary.dataset.chatChoice = "ordinary";
    const ordinaryStatus = document.createElement("p");
    ordinaryStatus.className = "profile-chat-status";
    ordinaryStatus.dataset.chatChoice = "ordinary-status";
    ordinaryStatus.setAttribute("role", "status");
    ordinaryStatus.hidden = true;
    const encrypted = document.createElement("button");
    encrypted.type = "button";
    encrypted.dataset.chatChoice = "encrypted";
    encrypted.textContent = "Создать E2EE-диалог";
    const status = document.createElement("p");
    status.className = "profile-chat-status";
    status.setAttribute("role", "status");
    const consent = document.createElement("label");
    consent.className = "profile-chat-consent";
    consent.hidden = true;
    const confirm = document.createElement("input");
    confirm.type = "checkbox";
    confirm.dataset.chatChoice = "confirm";
    const description = document.createElement("span");
    description.textContent = "Создать отдельный защищённый диалог. Старая история останется в прежнем диалоге.";
    consent.append(confirm, description);
    const successor = document.createElement("button");
    successor.type = "button";
    successor.dataset.chatChoice = "successor";
    successor.textContent = "Создать новый E2EE-диалог";
    successor.hidden = true;
    successor.disabled = true;
    const history = document.createElement("button");
    history.type = "button";
    history.dataset.chatChoice = "history";
    history.textContent = "Открыть прежний E2EE-диалог";
    history.hidden = true;
    section.append(heading, ordinary, ordinaryStatus, encrypted, status, consent, successor, history);
    root.appendChild(section);

    const findPlain = () => chats.find(c => c.type === "private" && profilePeerId(c.other_user_id) === peer.id);
    ordinary.textContent = findPlain() ? "Открыть обычный чат" : "Начать обычный чат";
    const openPlain = async (): Promise<void> => {
        if (!current() || ordinary.disabled) return;
        ordinary.disabled = true;
        ordinaryStatus.hidden = true;
        try {
            let plain = findPlain();
            if (!plain) {
                const response = await apiFetch("/api/users/" + peer.id + "/private-chat", { method: "POST", signal });
                if (!current()) return;
                if (response.status === 403) {
                    ordinaryStatus.textContent = "Настройки собеседника запрещают начинать эту переписку.";
                    ordinaryStatus.hidden = false;
                    return;
                }
                if (!response.ok) throw new Error("private chat request failed");
                const created = await response.json() as ChatRecord;
                if (!current()) return;
                if (created.type !== "private" || profilePeerId(created.other_user_id) !== peer.id ||
                    !Number.isSafeInteger(Number(created.id)) || Number(created.id) <= 0) throw new Error("invalid private chat");
                await loadChats();
                if (!current()) return;
                plain = created;
            }
            closeUserProfileModal();
            switchChat(plain.id, plain.other_username || peer.username, "private");
        } catch {
            if (!current()) return;
            ordinaryStatus.textContent = "Не удалось открыть обычный чат. Попробуйте ещё раз.";
            ordinaryStatus.hidden = false;
        } finally {
            if (current()) ordinary.disabled = false;
        }
    };
    ordinary.addEventListener("click", () => {
        void openPlain();
    });
    const api = window.zchatDesktop?.e2ee;
    if (!api?.compiled) {
        status.textContent = window.zchatDesktop
            ? "Сквозное шифрование пока недоступно в этой версии приложения."
            : "Сквозное шифрование доступно только в приложении для Windows. Здесь можно открыть обычный чат.";
        encrypted.disabled = true;
        return;
    }
    encrypted.disabled = true;
    status.textContent = "Проверяем возможность защищённой переписки…";
    let identity: string;
    try {
        const response = await apiFetch("/api/friends", { signal });
        if (!current()) return;
        if (!response.ok) throw new Error("friendship lookup failed");
        const friends: unknown = await response.json();
        if (!current()) return;
        if (!Array.isArray(friends)) throw new Error("invalid friends response");
        const found = friends.filter(f => f && profilePeerId(f.id) === peer.id);
        if (found.length === 0) {
            status.textContent = "Защищённая переписка доступна только друзьям.";
            return;
        }
        if (found.length !== 1 || !profilePeerUUID(found[0].user_uuid)) throw new Error("invalid peer identity");
        identity = found[0].user_uuid;
    } catch {
        if (current()) status.textContent = "Не удалось проверить пользователя. Откройте профиль ещё раз.";
        return;
    }
    if (!current()) return;
    status.textContent = "E2EE-диалог будет отдельным от обычной переписки. Доступен только на вашем E2EE-устройстве.";
    encrypted.disabled = false;
    let busy = false;
    let closed: DesktopE2eeConversationReply["conversation"] | null = null;
    let closedReply: DesktopE2eeConversationReply | null = null;
    const run = async (replace: boolean): Promise<void> => {
        if (!current() || busy || (replace && (!closed || !confirm.checked))) return;
        if (e2eeVoiceRecording()) {
            status.textContent = "Завершите запись голосового сообщения перед открытием E2EE-диалога.";
            return;
        }
        const expected = replace ? closed!.conversationId : null;
        busy = true;
        encrypted.disabled = successor.disabled = confirm.disabled = history.disabled = true;
        confirm.checked = false; // Every new attempt needs explicit consent again.
        try {
            const state = await api.status();
            if (!current()) return;
            if (state.phase !== "active" || state.runtimePhase !== "Running") {
                status.textContent = state.phase === "active"
                    ? "Открываем локальные ключи и историю. Подождите немного и повторите попытку."
                    : "Завершите настройку ключей в разделе «Настройки → Безопасность» и повторите попытку.";
                return;
            }
            const result = expected
                ? await api.createConversationAfterClosed(peer.id, expected, true)
                : await api.createConversation(peer.id);
            if (!current()) return;
            if (result.conversation.peerUserId !== identity) {
                status.textContent = "Не удалось подтвердить собеседника. Диалог не будет показан под этим профилем.";
                closed = null;
                closedReply = null;
                consent.hidden = successor.hidden = history.hidden = true;
                return;
            }
            closed = result.conversation.state === "closed" ? result.conversation : null;
            closedReply = closed ? result : null;
            consent.hidden = successor.hidden = history.hidden = !closed;
            status.textContent = closed
                ? "Прежний E2EE-диалог закрыт после замены ключей. Старая история сохранена; для продолжения нужен новый отдельный диалог."
                : "Защищённый диалог готов. Обычная переписка остаётся отдельно.";
            if (!closed) {
                if (openE2eeConversation(result, peer.username)) closeUserProfileModal();
                else status.textContent = "Диалог создан. Завершите запись голосового сообщения и повторите открытие.";
            }
        } catch (error) {
            if (current()) status.textContent = profileE2eeError(error);
        } finally {
            busy = false;
            if (current()) {
                encrypted.disabled = false;
                confirm.disabled = false;
                history.disabled = false;
                successor.disabled = true;
            }
        }
    };
    confirm.addEventListener("change", () => { successor.disabled = busy || !confirm.checked || !closed; });
    encrypted.addEventListener("click", () => { void run(false); });
    successor.addEventListener("click", () => { void run(true); });
    history.addEventListener("click", () => {
        if (!current() || busy || !closedReply) return;
        if (openE2eeConversation(closedReply, peer.username)) closeUserProfileModal();
        else status.textContent = "Завершите запись голосового сообщения перед открытием E2EE-диалога.";
    });
}
