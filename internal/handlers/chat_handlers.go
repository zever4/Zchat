package handlers

import (
	"encoding/json"
	"errors"
	"html"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/zever4/Zchat/internal/chat"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func HandleOpenPrivateChat(authService *service.AuthService, friendService *service.FriendService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		peerID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil || peerID <= 0 || peerID == userID {
			http.Error(w, "Invalid peer ID", http.StatusBadRequest)
			return
		}
		chat, err := friendService.OpenPrivateChat(r.Context(), userID, peerID)
		if err != nil {
			switch {
			case errors.Is(err, domain.ErrDirectMessagesDisabled):
				http.Error(w, "Recipient does not allow a new private chat", http.StatusForbidden)
			case errors.Is(err, domain.ErrUserNotFound):
				http.Error(w, "User not found", http.StatusNotFound)
			default:
				log.Printf("Open private chat error: %v", err)
				http.Error(w, "Failed to open private chat", http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(chat)
	}
}

func HandleGetChatMembers(authService *service.AuthService, chatRepo repository.ChatRepository, userRepo repository.UserRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		idStr := r.PathValue("id")
		chatID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid chat ID", http.StatusBadRequest)
			return
		}

		var members []*domain.User

		isMember, checkErr := chatRepo.IsMember(r.Context(), chatID, userID)
		if checkErr != nil || !isMember {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		if !canViewMembers(w, r, chatRepo, chatID, userID) {
			return
		}
		members, err = chatRepo.GetMembersWithInfo(r.Context(), chatID)

		if err != nil {
			log.Printf("Get members error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		if members == nil {
			members = []*domain.User{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(members)
	}
}

func HandleDeleteMessage(authService *service.AuthService, messageRepo repository.MessageRepository, hub *chat.Hub, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		idStr := r.PathValue("id")
		msgID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid message ID", http.StatusBadRequest)
			return
		}

		msg, err := messageRepo.GetByID(r.Context(), msgID)
		if err != nil {
			http.Error(w, "Message not found", http.StatusNotFound)
			return
		}

		if msg.SenderID != userID {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		if err := messageRepo.SoftDelete(r.Context(), msgID, userID); err != nil {
			writeMessageMutationError(w, err)
			return
		}

		// Broadcast delete event via WebSocket
		payload, _ := json.Marshal(map[string]any{
			"type":    "delete",
			"id":      msgID,
			"chat_id": msg.ChatID,
		})
		memberIDs, err := chatRepo.GetMemberIDs(r.Context(), msg.ChatID)
		if err == nil {
			hub.Enqueue(r.Context(), &chat.DirectMessage{RecipientIDs: memberIDs, Payload: payload})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func HandleEditMessage(authService *service.AuthService, messageRepo repository.MessageRepository, hub *chat.Hub, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		idStr := r.PathValue("id")
		msgID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid message ID", http.StatusBadRequest)
			return
		}

		var body struct {
			Content string `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}

		content := html.EscapeString(strings.TrimSpace(body.Content))
		if len(content) == 0 || len(content) > 999 {
			http.Error(w, "Invalid content", http.StatusBadRequest)
			return
		}

		msg, err := messageRepo.GetByID(r.Context(), msgID)
		if err != nil {
			http.Error(w, "Message not found", http.StatusNotFound)
			return
		}

		if msg.SenderID != userID {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		if err := messageRepo.UpdateContent(r.Context(), msgID, userID, content); err != nil {
			writeMessageMutationError(w, err)
			return
		}

		// Broadcast edit event via WebSocket
		payload, _ := json.Marshal(map[string]any{
			"type":    "edit",
			"id":      msgID,
			"chat_id": msg.ChatID,
			"content": content,
		})
		memberIDs, err := chatRepo.GetMemberIDs(r.Context(), msg.ChatID)
		if err == nil {
			hub.Enqueue(r.Context(), &chat.DirectMessage{RecipientIDs: memberIDs, Payload: payload})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func writeMessageMutationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotChatMember), errors.Is(err, domain.ErrChatWriteForbidden):
		http.Error(w, "Forbidden", http.StatusForbidden)
	case errors.Is(err, domain.ErrMessageNotFound), errors.Is(err, domain.ErrChatNotFound):
		http.Error(w, "Message not found", http.StatusNotFound)
	default:
		log.Printf("Update message: %v", err)
		http.Error(w, "Failed to update message", http.StatusInternalServerError)
	}
}
