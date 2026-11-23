// Profile requests and their creation choices share a modal/account revision.
let userProfileRevision = 0;
let userProfileRequest: AbortController | null = null;
async function openUserProfileModal(userID: number | string): Promise<void> {
    const id = profilePeerId(userID);
    if (!id) return;
    const revision = ++userProfileRevision;
    userProfileRequest?.abort();
    const request = userProfileRequest = new AbortController();
    const modal = byId<HTMLDivElement>("userProfileModal");
    modal.style.display = "none";
    const current = (): boolean => revision === userProfileRevision && !request.signal.aborted &&
        !authExpiredHandled && localStorage.getItem("token") === token;
    try {
        const res = await apiFetch("/api/users/" + id + "/profile", { signal: request.signal });
        if (!current() || !res.ok) return;
        const profile = await res.json() as UserProfileResponse;
        if (!current() || profilePeerId(profile.id) !== id) return;
        const modalContent = modal.querySelector<HTMLDivElement>(".modal");
        if (!modalContent) return;
        // Normal user profile — restore default layout
        modalContent.innerHTML = `
            <div class="modal-header">
                <h3>Профиль пользователя</h3>
                <button class="modal-close">&times;</button>
            </div>
            <div class="profile-avatar-section">
                <div class="profile-avatar-large" id="userProfileAvatar"></div>
            </div>
            <div class="profile-field">
                <label class="profile-label">Имя пользователя</label>
                <div class="profile-username" id="userProfileUsername"></div>
            </div>
            <div class="profile-field">
                <label class="profile-label">О себе</label>
                <div id="userProfileBio" style="font-size:13.5px;color:var(--text-secondary);min-height:20px"></div>
            </div>
        `;
        modalContent.querySelector<HTMLButtonElement>(".modal-close")!.addEventListener("click", closeUserProfileModal);

        const avatarEl = byId<HTMLDivElement>("userProfileAvatar");
        if (profile.avatar_url) {
            avatarEl.style.backgroundImage = "url(" + displayAssetURL(profile.avatar_url) + ")";
            avatarEl.textContent = "";
        } else {
            avatarEl.style.backgroundImage = "";
            avatarEl.textContent = (profile.username || "?").charAt(0).toUpperCase();
        }

        byId<HTMLElement>("userProfileUsername").textContent = profile.username || "";
        byId<HTMLElement>("userProfileBio").textContent = profile.bio || "Пользователь ничего не рассказал о себе";
        modal.style.display = "flex";
        await mountProfileChatChoices(modalContent, { id, username: profile.username || "" },
            () => current() && modal.style.display !== "none", request.signal);
    } catch (e) {
        if (current()) console.error("Failed to load user profile");
    }
}

function closeUserProfileModal(): void {
    userProfileRevision++;
    userProfileRequest?.abort();
    userProfileRequest = null;
    const modal = byId<HTMLDivElement>("userProfileModal");
    modal.style.display = "none";
}
window.addEventListener("pagehide", () => { closeUserProfileModal(); });
window.addEventListener("storage", event => {
    if ((event.key === "token" || event.key === null) && localStorage.getItem("token") !== token) closeUserProfileModal();
});
