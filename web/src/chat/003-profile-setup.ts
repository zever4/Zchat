// ===== PROFILE SETUP =====
myUsernameEl.textContent = currentUsername;
updateMyAvatar();

function updateMyAvatar(): void {
  if (currentAvatarURL) {
    myAvatarEl.textContent = "";
    myAvatarEl.style.backgroundImage = "url(" + displayAssetURL(currentAvatarURL) + ")";
    myAvatarEl.style.backgroundSize = "cover";
    myAvatarEl.style.backgroundPosition = "center";
  } else {
    myAvatarEl.style.backgroundImage = "";
    myAvatarEl.textContent = currentUsername.charAt(0).toUpperCase();
  }
}
