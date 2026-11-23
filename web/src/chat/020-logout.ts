// ===== LOGOUT =====
byId<HTMLButtonElement>("logoutBtn").addEventListener("click", () => {
  if (confirm("Вы точно хотите выйти?")) void logoutCurrentSession();
});

// =============================================================================
