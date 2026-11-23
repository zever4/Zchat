package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const maxJSONBody = 32 << 10

type trackedBody struct {
	io.ReadCloser
	complete bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.complete = true
	}
	return n, err
}

func uploadLimit(pattern string) (int64, bool) {
	switch pattern {
	case "POST /api/profile/avatar", "POST /api/groups/{id}/avatar":
		return maxAvatarSize + multipartOverhead, true
	case "POST /api/media/upload":
		return maxMediaSize + multipartOverhead, true
	case "POST /api/files/upload":
		return maxFileSize + multipartOverhead, true
	default:
		return 0, false
	}
}

// Bound ordinary bodies before authentication can acquire a session read barrier.
// Uploads authenticate first, then parse outside that barrier in ProtectSession.
func LimitRequests(next http.Handler, mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		_, pattern := mux.Handler(r)
		if pattern == boundary.BlobDownloadPattern {
			// This route strictly rejects bodies/transfer encoding before auth.
			// Do not read/normalize a forbidden body through the JSON parser.
			next.ServeHTTP(w, r)
			return
		}
		limit, upload := uploadLimit(pattern)
		rawBlob := pattern == boundary.BlobUploadPattern
		readTimeout := 5 * time.Second
		if rawBlob {
			limit = boundary.MaxAttachmentBlobBytes
			readTimeout = 30 * time.Second
		} else if upload {
			readTimeout = 30 * time.Second
		} else {
			limit = maxJSONBody
			if r.URL.Path == "/api/login" || r.URL.Path == "/api/register" || r.URL.Path == "/api/profile/password" {
				limit = 4096
			}
		}
		if r.ContentLength > limit {
			if rawBlob {
				w.Header().Set("Cache-Control", "private, no-store")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				rejectBlobUpload(w, http.StatusRequestEntityTooLarge, "body_too_large")
				return
			}
			_ = http.NewResponseController(w).SetReadDeadline(time.Now())
			w.Header().Set("Connection", "close")
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		bodyState := &trackedBody{ReadCloser: r.Body}
		r.Body = http.MaxBytesReader(w, bodyState, limit)
		rc := http.NewResponseController(w)
		_ = rc.SetReadDeadline(time.Now().Add(readTimeout))
		cancelFinished := make(chan struct{})
		stop := context.AfterFunc(r.Context(), func() { defer close(cancelFinished); _ = rc.SetReadDeadline(time.Now()) })
		var once sync.Once
		finishRead := func() {
			once.Do(func() {
				if !stop() {
					<-cancelFinished
				}
				if bodyState.complete {
					_ = rc.SetReadDeadline(time.Time{})
				} else {
					// net/http also closes/drains its original body after ServeHTTP.
					// Keep unfinished reads expired until that cleanup closes the socket.
					_ = rc.SetReadDeadline(time.Now())
				}
			})
		}
		defer finishRead()
		if !upload && !rawBlob {
			body, err := io.ReadAll(r.Body)
			finishRead()
			if err != nil {
				requestBodyError(w, err)
				return
			}
			if len(body) > 0 && !json.Valid(body) {
				http.Error(w, "Expected a single JSON value", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		}
		next.ServeHTTP(w, r)
	})
}

func requestBodyError(w http.ResponseWriter, err error) {
	// An unread body must not trigger net/http's keep-alive drain before the error.
	w.Header().Set("Connection", "close")
	var maxErr *http.MaxBytesError
	var netErr net.Error
	switch {
	case errors.As(err, &maxErr):
		http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
	case errors.As(err, &netErr) && netErr.Timeout():
		http.Error(w, "Request body timed out", http.StatusRequestTimeout)
	default:
		http.Error(w, "Invalid request body", http.StatusBadRequest)
	}
}
