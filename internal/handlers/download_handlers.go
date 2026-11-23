package handlers

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
)

// Must also guard the static fallback: encoded separators can bypass a mux prefix.
func IsUploadPath(urlPath string) bool {
	p := path.Clean("/" + urlPath)
	return p == "/uploads" || strings.HasPrefix(p, "/uploads/")
}

func HandleDownload(auth *service.AuthService, attachments repository.AttachmentRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
		w.Header().Set("Cache-Control", "private, no-store")
		name := strings.TrimPrefix(r.URL.Path, "/uploads/")
		if r.URL.Path != "/uploads/"+name || name == "" || strings.Contains(name, "\\") || strings.Contains(name, "..") {
			http.NotFound(w, r)
			return
		}
		publicAvatar := avatarNamePattern.MatchString(name)
		var a *domain.Attachment
		var userID int64
		if !publicAvatar {
			var err error
			userID, err = authenticateRequest(r, auth)
			if err != nil {
				http.Error(w, "Unauthorized", 401)
				return
			}
			a, err = attachments.GetForUser(r.Context(), r.URL.Path, userID)
			if errors.Is(err, domain.ErrMessageNotFound) {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				http.Error(w, "Internal error", 500)
				return
			}
		}
		storage := requestStorage(r)
		if storage == nil {
			http.Error(w, "Upload storage unavailable", http.StatusServiceUnavailable)
			return
		}
		file, err := storage.OpenFileFor(r.Context(), userID, name)
		if err != nil {
			if errors.Is(err, storagebudget.ErrBusy) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Too many file transfers; retry later", http.StatusTooManyRequests)
				return
			}
			http.NotFound(w, r)
			return
		}
		defer file.Close()
		stat, err := file.Stat()
		if err != nil || !stat.Mode().IsRegular() {
			http.NotFound(w, r)
			return
		}
		if publicAvatar {
			buf := make([]byte, 512)
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				http.NotFound(w, r)
				return
			}
			contentType := http.DetectContentType(buf[:n])
			if imageExtension(contentType) == "" {
				http.NotFound(w, r)
				return
			}
			if _, err = file.Seek(0, io.SeekStart); err != nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", contentType)
		} else if a.Kind == "file" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": a.Name}))
		} else {
			if mediaExtension(a.MIME) == "" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", a.MIME)
		}
		// Membership, path, file and MIME are checked while the session is
		// protected. This response is now authorized; its network transfer must
		// not delay logout. A subsequent GET/HEAD/range request authorizes anew.
		releaseDownloadSession(r)
		http.ServeContent(w, r, path.Base(name), stat.ModTime(), file)
	}
}
