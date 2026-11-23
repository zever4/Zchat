package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/zever4/Zchat/internal/calls"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func parseGroupIDFromPath(r *http.Request) (int64, error) {
	idStr := r.PathValue("id")
	return strconv.ParseInt(idStr, 10, 64)
}

func buildInviteLink(r *http.Request, code string) string {
	if code == "" {
		return ""
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/chat.html?invite=%s", scheme, r.Host, code)
}

func marshalDetailsJSON(data map[string]any) string {
	if data == nil {
		return "{}"
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func writeGroupAudit(ctx context.Context, chatRepo repository.ChatRepository, chatID, actorUserID int64, action string, details map[string]any) {
	if err := chatRepo.AddGroupAuditLog(ctx, chatID, actorUserID, action, marshalDetailsJSON(details)); err != nil {
		log.Printf("group audit write failed: %v", err)
	}
}

func HandleCreateGroup(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var req struct {
			Name     string `json:"name"`
			IsPublic *bool  `json:"is_public"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		name := strings.TrimSpace(req.Name)
		if len([]rune(name)) < 2 || len([]rune(name)) > 100 {
			http.Error(w, "Название группы должно быть от 2 до 100 символов", http.StatusBadRequest)
			return
		}
		isPublic := true
		if req.IsPublic != nil {
			isPublic = *req.IsPublic
		}

		group, err := chatRepo.CreateGroup(r.Context(), userID, name, isPublic)
		if err != nil {
			log.Printf("create group error: %v", err)
			http.Error(w, "Не удалось создать группу", http.StatusInternalServerError)
			return
		}
		writeGroupAudit(r.Context(), chatRepo, group.ID, userID, "group_created", map[string]any{
			"name":      group.Name,
			"is_public": group.IsPublic,
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"group": map[string]any{
				"id":                group.ID,
				"type":              group.Type,
				"name":              group.Name,
				"owner_id":          group.OwnerID,
				"is_public":         group.IsPublic,
				"invite_code":       group.InviteCode,
				"invite_link":       buildInviteLink(r, group.InviteCode),
				"avatar_url":        group.AvatarURL,
				"created_at":        group.CreatedAt,
				"admins_only_write": group.AdminsOnlyWrite,
				"hide_members_list": group.HideMembersList,
				"anonymous_mode":    group.AnonymousMode,
			},
		})
	}
}

func HandleGetGroup(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		group, err := chatRepo.GetChatByID(r.Context(), chatID)
		if err != nil {
			if err == domain.ErrChatNotFound {
				http.Error(w, "Group not found", http.StatusNotFound)
				return
			}
			log.Printf("get group error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if group.Type != "group" {
			http.Error(w, "Not a group chat", http.StatusBadRequest)
			return
		}

		isMember, err := chatRepo.IsMember(r.Context(), group.ID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !isMember {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		memberIDs, err := chatRepo.GetMemberIDs(r.Context(), group.ID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		isOwner := group.OwnerID == userID

		role, _ := chatRepo.GetMemberRole(r.Context(), group.ID, userID)
		if role == "" {
			role = "member"
		}

		canManage := role == "owner" || role == "admin"
		inviteCode := ""
		inviteLink := ""
		if canManage {
			invites, invErr := chatRepo.ListGroupInviteLinks(r.Context(), group.ID)
			if invErr == nil {
				for _, inv := range invites {
					if inv.IsActive {
						inviteCode = inv.Code
						inviteLink = buildInviteLink(r, inv.Code)
						break
					}
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":                group.ID,
			"type":              group.Type,
			"name":              group.Name,
			"owner_id":          group.OwnerID,
			"is_owner":          isOwner,
			"role":              role,
			"is_public":         group.IsPublic,
			"invite_code":       inviteCode,
			"invite_link":       inviteLink,
			"avatar_url":        group.AvatarURL,
			"created_at":        group.CreatedAt,
			"member_count":      len(memberIDs),
			"admins_only_write": group.AdminsOnlyWrite,
			"hide_members_list": group.HideMembersList,
			"anonymous_mode":    group.AnonymousMode,
		})
	}
}

func HandleUpdateGroup(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		var req struct {
			Name     string `json:"name"`
			IsPublic *bool  `json:"is_public"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		name := strings.TrimSpace(req.Name)
		if len([]rune(name)) < 2 || len([]rune(name)) > 100 {
			http.Error(w, "Название группы должно быть от 2 до 100 символов", http.StatusBadRequest)
			return
		}
		if req.IsPublic == nil {
			http.Error(w, "is_public is required", http.StatusBadRequest)
			return
		}

		if err := chatRepo.UpdateGroupSettings(r.Context(), chatID, userID, name, *req.IsPublic); err != nil {
			if err == domain.ErrNotGroupOwner {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			log.Printf("update group settings error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		updated, err := chatRepo.GetChatByID(r.Context(), chatID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		writeGroupAudit(r.Context(), chatRepo, chatID, userID, "settings_updated", map[string]any{
			"name":      updated.Name,
			"is_public": updated.IsPublic,
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":                updated.ID,
			"name":              updated.Name,
			"is_public":         updated.IsPublic,
			"avatar_url":        updated.AvatarURL,
			"invite_code":       updated.InviteCode,
			"invite_link":       buildInviteLink(r, updated.InviteCode),
			"owner_id":          updated.OwnerID,
			"created_at":        updated.CreatedAt,
			"admins_only_write": updated.AdminsOnlyWrite,
			"hide_members_list": updated.HideMembersList,
			"anonymous_mode":    updated.AnonymousMode,
		})
	}
}

func HandleUploadGroupAvatar(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		group, err := chatRepo.GetChatByID(r.Context(), chatID)
		if err != nil {
			if err == domain.ErrChatNotFound {
				http.Error(w, "Group not found", http.StatusNotFound)
				return
			}
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if group.Type != "group" {
			http.Error(w, "Not a group chat", http.StatusBadRequest)
			return
		}
		canManage, _ := chatRepo.IsGroupAdminOrOwner(r.Context(), chatID, userID)
		if !canManage {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		defer cleanupMultipart(r)
		avatarURL, release, err := avatarUpload(w, r)
		if err != nil {
			uploadError(w, err)
			return
		}
		defer release()
		old, err := chatRepo.ReplaceGroupAvatar(r.Context(), chatID, userID, avatarURL)
		if err != nil {
			release()
			if errors.Is(err, domain.ErrNotGroupOwner) || errors.Is(err, domain.ErrChatNotFound) {
				_ = discardUpload(r, avatarURL)
			}
			writeGroupTransitionError(w, err)
			return
		}
		writeGroupAudit(r.Context(), chatRepo, chatID, userID, "avatar_updated", map[string]any{
			"avatar_url": avatarURL,
		})
		release()
		removeAvatar(r, old)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"avatar_url": avatarURL})
	}
}

func HandleRegenerateGroupInvite(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		code, err := chatRepo.RegenerateGroupInviteCode(r.Context(), chatID, userID)
		if err != nil {
			if err == domain.ErrNotGroupOwner {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			log.Printf("regenerate invite code error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		writeGroupAudit(r.Context(), chatRepo, chatID, userID, "invite_created", map[string]any{
			"code": code,
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"invite_code": code,
			"invite_link": buildInviteLink(r, code),
		})
	}
}

func HandleListGroupInvites(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		canManage, err := chatRepo.IsGroupAdminOrOwner(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !canManage {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		invites, err := chatRepo.ListGroupInviteLinks(r.Context(), chatID)
		if err != nil {
			log.Printf("list group invites error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if invites == nil {
			invites = []*domain.GroupInviteLink{}
		}

		resp := make([]map[string]any, 0, len(invites))
		for _, inv := range invites {
			resp = append(resp, map[string]any{
				"id":             inv.ID,
				"code":           inv.Code,
				"is_active":      inv.IsActive,
				"created_at":     inv.CreatedAt,
				"deactivated_at": inv.DeactivatedAt,
				"invite_link":    buildInviteLink(r, inv.Code),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"invites": resp})
	}
}

func HandleCreateGroupInvite(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		invite, err := chatRepo.CreateGroupInviteLink(r.Context(), chatID, userID)
		if err != nil {
			if err == domain.ErrNotGroupOwner {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			log.Printf("create group invite error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		writeGroupAudit(r.Context(), chatRepo, chatID, userID, "invite_created", map[string]any{
			"code": invite.Code,
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":             invite.ID,
			"code":           invite.Code,
			"is_active":      invite.IsActive,
			"created_at":     invite.CreatedAt,
			"deactivated_at": invite.DeactivatedAt,
			"invite_link":    buildInviteLink(r, invite.Code),
		})
	}
}

func HandleSetGroupInviteActive(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}
		inviteID, err := strconv.ParseInt(r.PathValue("invite_id"), 10, 64)
		if err != nil {
			http.Error(w, "Invalid invite ID", http.StatusBadRequest)
			return
		}

		var req struct {
			IsActive *bool `json:"is_active"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if req.IsActive == nil {
			http.Error(w, "is_active is required", http.StatusBadRequest)
			return
		}

		if err := chatRepo.SetGroupInviteActive(r.Context(), chatID, inviteID, userID, *req.IsActive); err != nil {
			switch err {
			case domain.ErrInvalidInput:
				http.Error(w, "Invalid input", http.StatusBadRequest)
			case domain.ErrNotGroupOwner:
				http.Error(w, "Forbidden", http.StatusForbidden)
			case domain.ErrGroupInviteNotFound:
				http.Error(w, "Invite not found", http.StatusNotFound)
			default:
				http.Error(w, "Internal error", http.StatusInternalServerError)
			}
			return
		}
		if *req.IsActive {
			writeGroupAudit(r.Context(), chatRepo, chatID, userID, "invite_activated", map[string]any{"invite_id": inviteID})
		} else {
			writeGroupAudit(r.Context(), chatRepo, chatID, userID, "invite_deactivated", map[string]any{"invite_id": inviteID})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func HandleDeleteGroupInvite(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}
		inviteID, err := strconv.ParseInt(r.PathValue("invite_id"), 10, 64)
		if err != nil {
			http.Error(w, "Invalid invite ID", http.StatusBadRequest)
			return
		}

		if err := chatRepo.DeleteGroupInviteLink(r.Context(), chatID, inviteID, userID); err != nil {
			switch err {
			case domain.ErrNotGroupOwner:
				http.Error(w, "Forbidden", http.StatusForbidden)
			case domain.ErrGroupInviteNotFound:
				http.Error(w, "Invite not found", http.StatusNotFound)
			default:
				http.Error(w, "Internal error", http.StatusInternalServerError)
			}
			return
		}
		writeGroupAudit(r.Context(), chatRepo, chatID, userID, "invite_deleted", map[string]any{"invite_id": inviteID})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func HandleListGroupAuditLogs(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		isMember, err := chatRepo.IsMember(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !isMember {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		limit := 50
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			if parsed, perr := strconv.Atoi(raw); perr == nil {
				limit = parsed
			}
		}
		if limit <= 0 {
			limit = 50
		}
		if limit > 200 {
			limit = 200
		}

		logs, err := chatRepo.ListGroupAuditLogs(r.Context(), chatID, limit)
		if err != nil {
			log.Printf("list group audit logs error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if logs == nil {
			logs = []*domain.GroupAuditLog{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"logs": logs})
	}
}

func HandleDeleteGroup(authService *service.AuthService, chatRepo repository.ChatRepository, gateways ...*calls.Gateway) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		mutate := func() error { return chatRepo.DeleteGroup(r.Context(), chatID, userID) }
		if err := revokeGroupMedia(r.Context(), gateways, chatID, 0, mutate); err != nil {
			if err == calls.ErrMediaUnavailable {
				writePendingCallCleanup(w, "Chat access changed; call server unavailable, media disconnection will be retried")
				return
			}
			if err == domain.ErrNotGroupOwner {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			log.Printf("delete group error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func HandleGetGroupInviteInfo(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		code := strings.TrimSpace(r.PathValue("code"))
		if code == "" {
			http.Error(w, "Invite code is required", http.StatusBadRequest)
			return
		}

		group, err := chatRepo.GetChatByInviteCode(r.Context(), code)
		if err != nil {
			if err == domain.ErrChatNotFound {
				http.Error(w, "Invite link is invalid", http.StatusNotFound)
				return
			}
			log.Printf("get invite group info error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		isMember, err := chatRepo.IsMember(r.Context(), group.ID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		requestStatus := ""
		if !isMember && !group.IsPublic {
			req, reqErr := chatRepo.GetGroupJoinRequest(r.Context(), group.ID, userID)
			if reqErr != nil {
				http.Error(w, "Internal error", http.StatusInternalServerError)
				return
			}
			if req != nil {
				requestStatus = req.Status
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"chat_id":        group.ID,
			"name":           group.Name,
			"avatar_url":     group.AvatarURL,
			"is_public":      group.IsPublic,
			"is_member":      isMember,
			"request_status": requestStatus,
		})
	}
}

func HandleJoinGroupByInvite(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		code := strings.TrimSpace(r.PathValue("code"))
		if code == "" {
			http.Error(w, "Invite code is required", http.StatusBadRequest)
			return
		}
		group, status, err := chatRepo.JoinGroupByInvite(r.Context(), code, userID)
		if err != nil {
			writeGroupTransitionError(w, err)
			return
		}
		message := "Вы вступили в группу"
		if status == "pending" {
			message = "Заявка отправлена владельцу группы"
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": status, "chat_id": group.ID, "message": message, "is_public": group.IsPublic})
	}
}

func HandleListGroupJoinRequests(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		canManage, err := chatRepo.IsGroupAdminOrOwner(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !canManage {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		requests, err := chatRepo.ListGroupJoinRequests(r.Context(), chatID)
		if err != nil {
			log.Printf("list join requests error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if requests == nil {
			requests = []*domain.GroupJoinRequest{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"requests": requests,
		})
	}
}

func HandleApproveGroupJoinRequest(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return handleGroupJoinDecision(authService, chatRepo, true)
}

func handleGroupJoinDecision(authService *service.AuthService, chatRepo repository.ChatRepository, approve bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		chatID, err := parseGroupIDFromPath(r)
		if err != nil || chatID <= 0 {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}
		target, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
		if err != nil || target <= 0 {
			http.Error(w, "Invalid user ID", http.StatusBadRequest)
			return
		}
		if err = chatRepo.DecideGroupJoinRequest(r.Context(), chatID, userID, target, approve); err != nil {
			writeGroupTransitionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func writeGroupTransitionError(w http.ResponseWriter, err error) {
	switch err {
	case domain.ErrChatNotFound, domain.ErrJoinRequestNotFound, domain.ErrNotChatMember:
		http.Error(w, "Not found", http.StatusNotFound)
	case domain.ErrNotGroupOwner:
		http.Error(w, "Forbidden", http.StatusForbidden)
	case domain.ErrJoinRequestNotPending:
		http.Error(w, "Join request is not pending", http.StatusConflict)
	default:
		log.Printf("group transition failed: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
	}
}

func HandleListGroupMembers(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		isMember, err := chatRepo.IsMember(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !isMember {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		if !canViewMembers(w, r, chatRepo, chatID, userID) {
			return
		}

		members, err := chatRepo.GetMembersWithRoles(r.Context(), chatID)
		if err != nil {
			log.Printf("list group members error: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if members == nil {
			members = []*domain.ChatMemberWithRole{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"members": members})
	}
}

func HandleSetMemberRole(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}
		targetUserID, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
		if err != nil {
			http.Error(w, "Invalid user ID", http.StatusBadRequest)
			return
		}

		isOwner, err := chatRepo.IsGroupOwner(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !isOwner {
			http.Error(w, "Only the group owner can change roles", http.StatusForbidden)
			return
		}

		if targetUserID == userID {
			http.Error(w, "Cannot change your own role", http.StatusBadRequest)
			return
		}

		var req struct {
			Role string `json:"role"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if req.Role != "admin" && req.Role != "member" {
			http.Error(w, "Role must be 'admin' or 'member'", http.StatusBadRequest)
			return
		}

		isMember, err := chatRepo.IsMember(r.Context(), chatID, targetUserID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !isMember {
			http.Error(w, "User is not a member of this group", http.StatusBadRequest)
			return
		}

		if err := chatRepo.ChangeGroupMemberRole(r.Context(), chatID, userID, targetUserID, req.Role); err != nil {
			writeGroupTransitionError(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func HandleRemoveGroupMember(authService *service.AuthService, chatRepo repository.ChatRepository, gateways ...*calls.Gateway) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}
		targetUserID, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
		if err != nil {
			http.Error(w, "Invalid user ID", http.StatusBadRequest)
			return
		}

		if targetUserID == userID {
			http.Error(w, "Cannot kick yourself", http.StatusBadRequest)
			return
		}

		actorRole, err := chatRepo.GetMemberRole(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if actorRole != "owner" && actorRole != "admin" {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		targetRole, err := chatRepo.GetMemberRole(r.Context(), chatID, targetUserID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if targetRole == "owner" {
			http.Error(w, "Cannot kick the group owner", http.StatusForbidden)
			return
		}
		if targetRole == "admin" && actorRole != "owner" {
			http.Error(w, "Only the owner can kick admins", http.StatusForbidden)
			return
		}

		mutate := func() error { return chatRepo.KickGroupMember(r.Context(), chatID, userID, targetUserID) }
		if err := revokeGroupMedia(r.Context(), gateways, chatID, targetUserID, mutate); err != nil {
			if err == calls.ErrMediaUnavailable {
				writePendingCallCleanup(w, "Chat access changed; call server unavailable, media disconnection will be retried")
				return
			}
			writeGroupTransitionError(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func revokeGroupMedia(ctx context.Context, gateways []*calls.Gateway, chatID, targetUserID int64, mutate func() error) error {
	if len(gateways) == 0 {
		return mutate()
	}
	return gateways[0].RevokeChat(ctx, chatID, mutate, targetUserID)
}

func HandleGetGroupPermissions(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		isMember, err := chatRepo.IsMember(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !isMember {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		group, err := chatRepo.GetChatByID(r.Context(), chatID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"admins_only_write": group.AdminsOnlyWrite,
			"hide_members_list": group.HideMembersList,
			"anonymous_mode":    group.AnonymousMode,
		})
	}
}

func HandleUpdateGroupPermissions(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		chatID, err := parseGroupIDFromPath(r)
		if err != nil {
			http.Error(w, "Invalid group ID", http.StatusBadRequest)
			return
		}

		canManage, err := chatRepo.IsGroupAdminOrOwner(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !canManage {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		var req struct {
			AdminsOnlyWrite *bool `json:"admins_only_write"`
			HideMembersList *bool `json:"hide_members_list"`
			AnonymousMode   *bool `json:"anonymous_mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if req.AdminsOnlyWrite == nil && req.HideMembersList == nil && req.AnonymousMode == nil {
			http.Error(w, "At least one permission field is required", http.StatusBadRequest)
			return
		}

		if err := chatRepo.ChangeGroupPermissions(r.Context(), chatID, userID, req.AdminsOnlyWrite, req.HideMembersList, req.AnonymousMode); err != nil {
			writeGroupTransitionError(w, err)
			return
		}

		group, err := chatRepo.GetChatByID(r.Context(), chatID)
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok":                true,
			"admins_only_write": group.AdminsOnlyWrite,
			"hide_members_list": group.HideMembersList,
			"anonymous_mode":    group.AnonymousMode,
		})
	}
}

func HandleRejectGroupJoinRequest(authService *service.AuthService, chatRepo repository.ChatRepository) http.HandlerFunc {
	return handleGroupJoinDecision(authService, chatRepo, false)
}
