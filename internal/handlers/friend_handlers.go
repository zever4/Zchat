package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func authenticateRequest(r *http.Request, authService *service.AuthService) (int64, error) {
	if session, ok := r.Context().Value(authenticatedSessionKey{}).(*service.SessionClaims); ok {
		return session.UserID, nil
	}
	token, err := requestToken(r)
	if err != nil {
		return 0, err
	}
	return authService.ValidateTokenContext(r.Context(), token)
}

func HandleSearchUsers(authService *service.AuthService, userRepo repository.UserRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		query := r.URL.Query().Get("q")
		if len(query) < 2 {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]any{})
			return
		}

		users, err := userRepo.SearchByUsername(r.Context(), query, userID, 20)
		if err != nil {
			log.Printf("Search users error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		if users == nil {
			users = []*domain.User{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(users)
	}
}

func HandleSendFriendRequest(authService *service.AuthService, friendService *service.FriendService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var req struct {
			ToUserID int64 `json:"to_user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		friendReq, err := friendService.SendRequest(r.Context(), userID, req.ToUserID)
		if err != nil {
			log.Printf("Send friend request error: %v", err)
			status := http.StatusInternalServerError
			switch err {
			case domain.ErrUserNotFound:
				status = http.StatusNotFound
			case domain.ErrFriendRequestsDisabled:
				status = http.StatusForbidden
			case domain.ErrSelfFriendRequest, domain.ErrFriendRequestExists, domain.ErrAlreadyFriends, domain.ErrFriendRequestNotPending:
				status = http.StatusConflict
			}
			http.Error(w, err.Error(), status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(friendReq)
	}
}

func HandleAcceptRequest(authService *service.AuthService, friendService *service.FriendService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		idStr := r.PathValue("id")
		requestID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid request ID", http.StatusBadRequest)
			return
		}

		chat, err := friendService.AcceptRequest(r.Context(), requestID, userID)
		if err != nil {
			log.Printf("Accept request error: %v", err)
			status := http.StatusInternalServerError
			switch err {
			case domain.ErrFriendRequestNotFound, domain.ErrUserNotFound:
				status = http.StatusNotFound
			case domain.ErrFriendRequestNotPending:
				status = http.StatusConflict
			default:
				status = http.StatusInternalServerError
			}
			http.Error(w, err.Error(), status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(chat)
	}
}

func HandleRejectRequest(authService *service.AuthService, friendService *service.FriendService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		idStr := r.PathValue("id")
		requestID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "Invalid request ID", http.StatusBadRequest)
			return
		}

		err = friendService.RejectRequest(r.Context(), requestID, userID)
		if err != nil {
			log.Printf("Reject request error: %v", err)
			status := http.StatusInternalServerError
			switch err {
			case domain.ErrFriendRequestNotFound:
				status = http.StatusNotFound
			case domain.ErrFriendRequestNotPending:
				status = http.StatusConflict
			}
			http.Error(w, err.Error(), status)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func HandleGetFriends(authService *service.AuthService, friendService *service.FriendService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		friends, err := friendService.GetFriends(r.Context(), userID)
		if err != nil {
			log.Printf("Get friends error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		if friends == nil {
			friends = []*domain.User{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(friends)
	}
}
