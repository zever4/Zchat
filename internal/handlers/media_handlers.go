package handlers

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func HandleUploadMedia(auth *service.AuthService, attachments repository.AttachmentRepository) http.HandlerFunc {
	return handleAttachmentUpload(auth, attachments, "media", maxMediaSize)
}
func HandleUploadFile(auth *service.AuthService, attachments repository.AttachmentRepository) http.HandlerFunc {
	return handleAttachmentUpload(auth, attachments, "file", maxFileSize)
}
func handleAttachmentUpload(auth *service.AuthService, attachments repository.AttachmentRepository, kind string, limit int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, auth)
		if err != nil {
			http.Error(w, "Unauthorized", 401)
			return
		}
		defer cleanupMultipart(r)
		file, header, err := parseUpload(w, r, kind, limit)
		if err != nil {
			uploadError(w, err)
			return
		}
		defer file.Close()
		chatID, err := strconv.ParseInt(r.FormValue("chat_id"), 10, 64)
		if err != nil || chatID <= 0 {
			http.Error(w, "chat_id is required", 400)
			return
		}
		mime, err := detectUpload(file)
		if err != nil {
			uploadError(w, err)
			return
		}
		ext := ".bin"
		if kind == "media" {
			ext = mediaExtension(mime)
			if ext == "" {
				http.Error(w, "Invalid media type", 400)
				return
			}
		}
		stored, release, err := storeUpload(r, file, kind, ext)
		if err != nil {
			uploadError(w, err)
			return
		}
		defer release()
		original := strings.Map(func(r rune) rune {
			if r < 32 || r == 127 {
				return -1
			}
			return r
		}, header.Filename)
		if len(original) > 255 {
			original = "attachment"
		}
		a := &domain.Attachment{Path: "/uploads/" + stored.Name, Name: original, MIME: mime, Kind: kind, Size: stored.Size, ChatID: chatID, OwnerID: userID}
		err = attachments.CreateAuthorized(r.Context(), a)
		release()
		if err != nil {
			if errors.Is(err, domain.ErrNotChatMember) || errors.Is(err, domain.ErrChatWriteForbidden) {
				_ = discardUpload(r, a.Path)
				http.Error(w, "Forbidden", 403)
			} else if errors.Is(err, domain.ErrChatNotFound) {
				_ = discardUpload(r, a.Path)
				http.Error(w, "Chat not found", 404)
			} else {
				log.Printf("publish attachment: %v", err)
				http.Error(w, "Failed to save attachment", 500)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": a.Path, "name": original, "size": stored.Size})
	}
}
