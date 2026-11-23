// ===== INFO PANEL (chat members) =====
let infoPanelOpen = false;

function toggleInfoPanel(): void {
    infoPanelOpen = !infoPanelOpen;
    const panel = byId<HTMLDivElement>("infoPanel");
    const btn = byId<HTMLButtonElement>("infoToggleBtn");
    panel.classList.toggle("open", infoPanelOpen);
    btn.style.display = infoPanelOpen ? "none" : "";
    if (infoPanelOpen) loadChatMembers(currentChatID);
}

function closeInfoPanel(): void {
    infoPanelOpen = false;
    byId<HTMLDivElement>("infoPanel").classList.remove("open");
    byId<HTMLButtonElement>("infoToggleBtn").style.display = "";
}

function groupOwnerCrownSVG(): string {
    return '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
        '<path fill="currentColor" d="M5 18h14l1-9-5 3-3-5-3 5-5-3 1 9zm1.5 2a1 1 0 0 1 0-2h11a1 1 0 1 1 0 2h-11z"/>' +
        "</svg>";
}

async function resolveGroupOwnerID(chatID: number | string | null | undefined): Promise<number> {
    const key = Number(chatID);
    if (!Number.isFinite(key) || key <= 0) return 0;
    if (Object.prototype.hasOwnProperty.call(groupOwnerByChat, key)) {
        return Number(groupOwnerByChat[key] || 0);
    }

    try {
        const res = await apiFetch("/api/groups/" + key);
        if (!res.ok) return 0;
        const group = await res.json();
        const ownerID = Number(group.owner_id || 0);
        groupOwnerByChat[key] = ownerID;
        return ownerID;
    } catch (e) {
        console.error("resolve group owner error:", e);
        return 0;
    }
}

async function loadChatMembers(chatID: number | string | null | undefined): Promise<void> {
    try {
        const numericChatID = Number(chatID || 0);
        const cachedGroupInfo = groupInfoCache[numericChatID];
        const memberCountEl = optionalById<HTMLElement>("memberCount");
        const list = optionalById<HTMLDivElement>("membersList");
        if (memberCountEl && cachedGroupInfo && typeof cachedGroupInfo.member_count !== "undefined") {
            memberCountEl.textContent = String(cachedGroupInfo.member_count);
        }

        if (currentChatType === "group" && cachedGroupInfo && cachedGroupInfo.hide_members_list && cachedGroupInfo.role === "member") {
            if (list) {
                list.innerHTML = '<div class="placeholder-text">Список участников скрыт настройками группы</div>';
            }
            return;
        }

        let groupOwnerID = 0;
        if (currentChatType === "group") {
            groupOwnerID = await resolveGroupOwnerID(chatID);
        }

        const res = await apiFetch("/api/chats/" + chatID + "/members");
        if (!res.ok) {
            if (list && res.status === 403) {
                list.innerHTML = '<div class="placeholder-text">Список участников скрыт настройками группы</div>';
            }
            return;
        }
        const members = await res.json() as ChatMemberListItem[];
        if (memberCountEl) {
            memberCountEl.textContent = String(members ? members.length : 0);
        }
        if (!list) return;
        list.innerHTML = "";
        if (!members || members.length === 0) {
            list.innerHTML = '<div class="placeholder-text">Нет участников</div>';
            return;
        }
        if (currentChatType === "group" && groupInfoCache[numericChatID]) {
            groupInfoCache[numericChatID].member_count = members.length;
            applyGroupChatStatus(numericChatID);
        }
        for (const m of members) {
            const isGroupOwner = groupOwnerID > 0 && Number(m.id) === Number(groupOwnerID);
            const div = document.createElement("div");
            div.className = "member-item";
            div.onclick = () => openUserProfileModal(m.id);
            const avatar = document.createElement("div");
            avatar.className = "contact-avatar";
            if (m.avatar_url) {
                avatar.style.backgroundImage = "url(" + displayAssetURL(m.avatar_url) + ")";
                avatar.style.backgroundSize = "cover";
                avatar.style.backgroundPosition = "center";
            } else {
                avatar.textContent = (m.username || "?").charAt(0).toUpperCase();
            }

            const nameWrap = document.createElement("div");
            nameWrap.className = "contact-name-wrap";
            const name = document.createElement("span");
            name.className = "contact-name";
            name.textContent = m.username;
            nameWrap.appendChild(name);

            if (isGroupOwner) {
                const crown = document.createElement("span");
                crown.className = "group-owner-crown";
                crown.title = "Создатель группы";
                crown.innerHTML = groupOwnerCrownSVG();
                nameWrap.appendChild(crown);
            }

            div.appendChild(avatar);
            div.appendChild(nameWrap);
            list.appendChild(div);
        }
    } catch (e) {
        console.error("Failed to load members:", e);
    }
}
