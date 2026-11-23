// ===== PROFILE MODAL =====
function openProfileModal(): void {
  byId<HTMLDivElement>("profileModal").style.display = "flex";
  byId<HTMLElement>("profileUsername").textContent = currentUsername;

  apiFetch("/api/profile")
    .then((r) => r.json())
    .then((profile: UserProfileResponse) => {
      byId<HTMLTextAreaElement>("profileBioInput").value = profile.bio || "";
      const avatarEl = byId<HTMLDivElement>("profileAvatarLarge");
      if (profile.avatar_url) {
        avatarEl.style.backgroundImage = "url(" + displayAssetURL(profile.avatar_url) + ")";
        avatarEl.textContent = "";
      } else {
        avatarEl.style.backgroundImage = "";
        avatarEl.textContent = currentUsername.charAt(0).toUpperCase();
      }
    });
}

function closeProfileModal(): void {
  byId<HTMLDivElement>("profileModal").style.display = "none";
}

async function saveProfile(): Promise<void> {
  const bio = byId<HTMLTextAreaElement>("profileBioInput").value.trim();
  const res = await apiFetch("/api/profile", {
    method: "PUT",
    body: JSON.stringify({ bio }),
  });
  if (res.ok) {
    closeProfileModal();
  }
}

document
  .getElementById("avatarFileInput")
  .addEventListener("change", async (e) => {
    const input = e.currentTarget as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    const formData = new FormData();
    formData.append("avatar", file);
    const res = await sessionFetch("/api/profile/avatar", {
      method: "POST",
      headers: { Authorization: "Bearer " + token },
      body: formData,
    });
    if (res.ok) {
      const data = await res.json() as UserProfileResponse;
      if (localStorage.getItem("token") !== token) return;
      currentAvatarURL = data.avatar_url || "";
      localStorage.setItem("avatar_url", currentAvatarURL);
      updateMyAvatar();
      // Update modal preview
      const avatarEl = byId<HTMLDivElement>("profileAvatarLarge");
      avatarEl.style.backgroundImage = "url(" + displayAssetURL(currentAvatarURL) + ")";
      avatarEl.textContent = "";
    }
    // Reset input so same file can be re-selected
    input.value = "";
  });
