package handlers

import (
	"github.com/zever4/Zchat/internal/repository"
	"net/http"
)

// canViewMembers applies the same visibility policy to both member-list routes.
// Callers must check membership before calling this helper.
func canViewMembers(w http.ResponseWriter, r *http.Request, repo repository.ChatRepository, chatID, userID int64) bool {
	chat, err := repo.GetChatByID(r.Context(), chatID)
	if err != nil || chat == nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return false
	}
	if chat.Type == "group" && chat.HideMembersList {
		manager, err := repo.IsGroupAdminOrOwner(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return false
		}
		if !manager {
			http.Error(w, "Members list hidden", http.StatusForbidden)
			return false
		}
	}
	return true
}
