// ===== INIT =====
loadMyProfile();
loadUserSettings();
loadChats();
loadFriendRequests();
startE2eeDiscovery();

window.addEventListener("load", () => {
  document.body.classList.add("ready");
  // Fonts are optional decoration; basic chat starts before loading them.
  const fonts = document.createElement("link");
  fonts.rel = "stylesheet";
  fonts.href = "/vendor/fonts/plus-jakarta-sans.css";
  document.head.appendChild(fonts);
  setTimeout(maybeShowUpdateShowcaseModal, 650);
});

setInterval(loadFriendRequests, 30000);
