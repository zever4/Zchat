// ===== LOAD PROFILE =====
async function loadMyProfile(): Promise<void> {
  try {
    const res = await apiFetch("/api/profile");
    if (res.ok) {
      const profile = await res.json() as UserProfileResponse;
      if (localStorage.getItem("token") !== token) return;
      currentAvatarURL = profile.avatar_url || "";
      localStorage.setItem("avatar_url", currentAvatarURL);
      updateMyAvatar();
    }
  } catch (e) {
    console.error("Failed to load profile:", e);
  }
}
