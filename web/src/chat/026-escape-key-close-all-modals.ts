// ===== ESCAPE KEY — close all modals =====
document.addEventListener("keydown", (e: KeyboardEvent) => {
    if (e.key !== "Escape") return;
    if (optionalById<HTMLDialogElement>("e2eeAttachmentPreview")?.open) return;
    if (optionalById<HTMLDialogElement>("e2eePhotoDialog")?.open) return;
    if (optionalById<HTMLDialogElement>("e2eeFileDialog")?.open) return;
    // An in-flight profile load is also cancellable before its overlay appears.
    if (userProfileRequest && optionalById("userProfileModal")?.style.display === "none") closeUserProfileModal();
    // If editing a message inline, cancel edit
    const activeEdit = document.querySelector<HTMLInputElement>(".edit-input");
    if (activeEdit) {
        activeEdit.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
        return;
    }
    const modals = ["updateShowcaseModal", "settingsModal", "friendModal", "profileModal", "userProfileModal", "createGroupModal", "groupSettingsModal"] as const;
    for (const id of modals) {
        const el = optionalById<HTMLDivElement>(id);
        if (el && el.style.display !== "none") {
            if (id === "updateShowcaseModal") {
                closeUpdateShowcaseModal();
            } else if (id === "userProfileModal") {
                closeUserProfileModal();
            } else {
                el.style.display = "none";
            }
            return;
        }
    }
    if (notifPanelOpen) { closeNotifPanel(); return; }
    if (infoPanelOpen) closeInfoPanel();
});
