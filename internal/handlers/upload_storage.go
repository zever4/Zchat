package handlers

import (
	"errors"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/uploadstorage"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
	"sync"
)

const (
	maxAvatarSize     int64 = 5 << 20
	maxMediaSize      int64 = 10 << 20
	maxFileSize       int64 = 50 << 20
	multipartOverhead int64 = 64 << 10
)

var errUploadStorage = errors.New("upload storage failure")
var errUploadTooLarge = errors.New("upload exceeds limit")
var avatarNamePattern = regexp.MustCompile(`^(avatar|group_avatar)_[a-zA-Z0-9_]+\.(jpg|png|gif|webp)$`)

// Body parsing/reservation happens first in ProtectSession; never fall back to
// unbudgeted multipart.ParseMultipartForm here.
func parseUpload(w http.ResponseWriter, r *http.Request, field string, limit int64) (multipart.File, *multipart.FileHeader, error) {
	p := requestUpload(r)
	if p == nil {
		return nil, nil, errStorageUnavailable
	}
	if p.field != field || p.header == nil || p.header.Size <= 0 || p.header.Size > limit {
		return nil, nil, errUploadTooLarge
	}
	return p.file, p.header, nil
}

func cleanupMultipart(r *http.Request) {
	if r.MultipartForm != nil {
		_ = r.MultipartForm.RemoveAll()
	}
}

func uploadError(w http.ResponseWriter, err error) {
	w.Header().Set("Connection", "close")
	var maxErr *http.MaxBytesError
	if errors.Is(err, errUploadReauth) {
		sessionError(w, err)
	} else if errors.Is(err, storagebudget.ErrQuota) || errors.Is(err, storagebudget.ErrHeadroom) {
		http.Error(w, "Недостаточно места для загрузки. Обратитесь к администратору.", http.StatusInsufficientStorage)
	} else if errors.Is(err, storagebudget.ErrBusy) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Слишком много одновременных загрузок. Повторите позже.", http.StatusTooManyRequests)
	} else if errors.Is(err, errStorageUnavailable) {
		http.Error(w, "Upload storage unavailable", http.StatusServiceUnavailable)
	} else if errors.Is(err, errUploadTooLarge) || errors.Is(err, uploadstorage.ErrTooLarge) || errors.Is(err, multipart.ErrMessageTooLarge) || errors.As(err, &maxErr) {
		http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
	} else if errors.Is(err, errUploadStorage) {
		log.Printf("upload storage: %v", err)
		http.Error(w, "Failed to save upload", http.StatusInternalServerError)
	} else {
		requestBodyError(w, err)
	}
}

func detectUpload(file multipart.File) (string, error) {
	buf := make([]byte, 512)
	n, err := io.ReadFull(file, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return http.DetectContentType(buf[:n]), nil
}

func imageExtension(mime string) string {
	return map[string]string{"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp"}[mime]
}

func mediaExtension(mime string) string {
	if ext := imageExtension(mime); ext != "" {
		return ext
	}
	return map[string]string{"audio/webm": ".webm", "video/webm": ".webm", "audio/ogg": ".ogg", "application/ogg": ".ogg", "audio/mpeg": ".mp3", "audio/wave": ".wav"}[mime]
}

func storeUpload(r *http.Request, src io.Reader, kind, ext string) (uploadstorage.Stored, func(), error) {
	p := requestUpload(r)
	if p == nil || p.reauthorize == nil {
		return uploadstorage.Stored{}, nil, errStorageUnavailable
	}
	// File copy/Sync must not hold the login revocation barrier. No metadata
	// publication may follow until the SAME session has been authorized again.
	if release, ok := r.Context().Value(sessionReleaseKey{}).(func()); ok {
		release()
	}
	stored, err := p.ticket.Store(src, kind, ext)
	if err != nil {
		return uploadstorage.Stored{}, nil, errors.Join(errUploadStorage, err)
	}
	release, err := p.reauthorize(r.Context())
	if err != nil {
		cleanupErr := p.ticket.Discard(stored.Name)
		if cleanupErr != nil {
			log.Printf("rejected upload cleanup (quota retained): %v", cleanupErr)
		}
		return uploadstorage.Stored{}, nil, errors.Join(errUploadReauth, err)
	}
	return stored, sync.OnceFunc(release), nil
}

// Only local avatar names can be removed; no client-supplied path traversal.
func removeAvatar(r *http.Request, url string) {
	name := strings.TrimPrefix(url, "/uploads/")
	if url != "/uploads/"+name || !avatarNamePattern.MatchString(name) {
		return
	}
	if m := requestStorage(r); m != nil {
		if err := m.Remove(name); err != nil {
			log.Printf("avatar cleanup (quota retained): %v", err)
		}
	}
}

func avatarUpload(w http.ResponseWriter, r *http.Request) (string, func(), error) {
	file, _, err := parseUpload(w, r, "avatar", maxAvatarSize)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()
	mime, err := detectUpload(file)
	if err != nil {
		return "", nil, err
	}
	ext := imageExtension(mime)
	if ext == "" {
		return "", nil, errors.New("invalid image type")
	}
	stored, release, err := storeUpload(r, file, "avatar", ext)
	if err != nil {
		return "", nil, errors.Join(errUploadStorage, err)
	}
	return "/uploads/" + stored.Name, release, nil
}
