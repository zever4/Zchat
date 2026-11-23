// ===== FRIEND SEARCH MODAL =====
function openFriendModal(): void {
  byId<HTMLDivElement>("friendModal").style.display = "flex";
  const searchInput = byId<HTMLInputElement>("friendSearchInput");
  searchInput.value = "";
  byId<HTMLDivElement>("friendSearchResults").innerHTML = "";
  searchInput.focus();
  loadModalPendingRequests();
}

function closeFriendModal(): void {
  byId<HTMLDivElement>("friendModal").style.display = "none";
}
