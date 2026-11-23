package handlers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/uploadstorage"
)

func storageAt(t *testing.T, path string) *uploadstorage.Manager {
	t.Helper()
	c, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	c.FreeBytes = 1
	c.FreeEntries = 1 // Host capacity is tested separately with deterministic probes.
	m, err := uploadstorage.Open(t.Context(), path, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}
func withUploadStorage(auth *service.AuthService, h http.HandlerFunc, field string, m *uploadstorage.Manager) http.HandlerFunc {
	pattern := "POST /api/profile/avatar"
	if field == "media" {
		pattern = "POST /api/media/upload"
	} else if field == "file" {
		pattern = "POST /api/files/upload"
	}
	guard := ProtectSession(auth, h, false, m)
	return func(w http.ResponseWriter, r *http.Request) { r.Pattern = pattern; guard(w, r) }
}
func publicRootEntries(t *testing.T) []os.DirEntry {
	t.Helper()
	all, err := os.ReadDir("web/uploads")
	if err != nil {
		t.Fatal(err)
	}
	var result []os.DirEntry
	for _, e := range all {
		if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			result = append(result, e)
		}
	}
	return result
}

type uploadUsers struct {
	repository.UserRepository
	old  string
	fail bool
}

func (u *uploadUsers) GetSessionVersion(context.Context, int64, string) (int, error) { return 1, nil }
func (u *uploadUsers) ReplaceAvatar(_ context.Context, _ int64, next string) (string, error) {
	if u.fail {
		return "", errors.New("injected DB error")
	}
	old := u.old
	u.old = next
	return old, nil
}

type uploadChats struct {
	repository.ChatRepository
	old     string
	revoked bool
}

func (c *uploadChats) GetChatByID(context.Context, int64) (*domain.Chat, error) {
	return &domain.Chat{Type: "group", AvatarURL: c.old}, nil
}
func (c *uploadChats) IsGroupAdminOrOwner(context.Context, int64, int64) (bool, error) {
	return true, nil
}
func (c *uploadChats) ReplaceGroupAvatar(_ context.Context, _, _ int64, next string) (string, error) {
	if c.revoked {
		return "", domain.ErrNotGroupOwner
	}
	old := c.old
	c.old = next
	return old, nil
}

type uploadAttachments struct {
	created bool
	fail    error
	item    *domain.Attachment
}

func (a *uploadAttachments) CreateAuthorized(_ context.Context, item *domain.Attachment) error {
	a.created = true
	a.item = item
	return a.fail
}
func (a *uploadAttachments) GetForUser(context.Context, string, int64) (*domain.Attachment, error) {
	return a.item, a.fail
}

func uploadAuth(t *testing.T) (*service.AuthService, string) {
	t.Helper()
	auth := service.NewAuthService(&uploadUsers{}, "upload-test-secret")
	token, err := auth.GenerateToken(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	return auth, token
}
func uploadRequest(t *testing.T, token, field string, data []byte, chat string, extra int) *http.Request {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	f, err := w.CreateFormFile(field, "test.png")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write(data)
	if chat != "" {
		_ = w.WriteField("chat_id", chat)
	}
	if extra > 0 {
		_ = w.WriteField("padding", strings.Repeat("x", extra))
	}
	_ = w.Close()
	r := httptest.NewRequest("POST", "/upload", &body)
	r.Header.Set("Content-Type", w.FormDataContentType())
	r.Header.Set("Authorization", "Bearer "+token)
	r.ContentLength = -1 // Verify the reader limit, not just a trusted Content-Length.
	r.SetPathValue("id", "7")
	return r
}
func pngBytes(n int) []byte {
	b := make([]byte, n)
	copy(b, []byte{137, 80, 78, 71, 13, 10, 26, 10})
	return b
}

func TestUploadFileAndBodyLimits(t *testing.T) {
	t.Chdir(t.TempDir())
	auth, token := uploadAuth(t)
	m := storageAt(t, "web/uploads")
	for _, tc := range []struct {
		name, field string
		limit       int64
		handler     http.HandlerFunc
	}{
		{"media", "media", maxMediaSize, HandleUploadMedia(auth, &uploadAttachments{})},
		{"file", "file", maxFileSize, HandleUploadFile(auth, &uploadAttachments{})},
		{"avatar", "avatar", maxAvatarSize, HandleUploadAvatar(auth, &uploadUsers{})},
		{"group avatar", "avatar", maxAvatarSize, HandleUploadGroupAvatar(auth, &uploadChats{})},
	} {
		tc.handler = withUploadStorage(auth, tc.handler, tc.field, m)
		t.Run(tc.name+" file limit", func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.handler(w, uploadRequest(t, token, tc.field, pngBytes(int(tc.limit)+1), "7", 0))
			if w.Code != 413 {
				t.Fatalf("status=%d %s", w.Code, w.Body.String())
			}
		})
		t.Run(tc.name+" body limit", func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.handler(w, uploadRequest(t, token, tc.field, pngBytes(8), "7", int(tc.limit+multipartOverhead)))
			if w.Code != 413 {
				t.Fatalf("status=%d %s", w.Code, w.Body.String())
			}
		})
	}
	if len(publicRootEntries(t)) != 0 {
		t.Fatal("rejected uploads published a file")
	}
}

func TestAvatarReplacementPreservesOldOnFailure(t *testing.T) {
	t.Chdir(t.TempDir())
	_ = os.MkdirAll("web/uploads", 0755)
	auth, token := uploadAuth(t)
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "DB failure", true: "admin revoked after early check"}[group], func(t *testing.T) {
			old := "/uploads/avatar_old.png"
			_ = os.WriteFile("web"+old, []byte("old bytes"), 0600)
			m := storageAt(t, "web/uploads")
			h := HandleUploadAvatar(auth, &uploadUsers{old: old, fail: true})
			want := 500
			if group {
				h = HandleUploadGroupAvatar(auth, &uploadChats{old: old, revoked: true})
				want = 403
			}
			w := httptest.NewRecorder()
			h = withUploadStorage(auth, h, "avatar", m)
			h(w, uploadRequest(t, token, "avatar", pngBytes(32), "", 0))
			if w.Code != want {
				t.Fatalf("status=%d", w.Code)
			}
			data, err := os.ReadFile("web" + old)
			if err != nil || string(data) != "old bytes" {
				t.Fatal("old avatar lost", err)
			}
			entries := publicRootEntries(t)
			wantFiles := 1
			if !group {
				wantFiles = 2
			} // Unknown COMMIT retains the new file conservatively.
			if len(entries) != wantFiles {
				t.Fatal("unexpected avatar retention", entries)
			}
			for _, e := range entries {
				if err := m.Remove(e.Name()); err != nil {
					t.Fatal(err)
				}
			} // Explicit synthetic fixture cleanup.
		})
	}
	user := &uploadUsers{old: "/uploads/avatar_initial.png"}
	_ = os.WriteFile("web"+user.old, []byte("old"), 0600)
	m := storageAt(t, "web/uploads")
	h := withUploadStorage(auth, HandleUploadAvatar(auth, user), "avatar", m)
	previous := user.old
	for range 2 {
		w := httptest.NewRecorder()
		h(w, uploadRequest(t, token, "avatar", pngBytes(32), "", 0))
		if w.Code != 200 || user.old == previous {
			t.Fatalf("replacement status=%d url=%s", w.Code, user.old)
		}
		if _, err := os.Stat("web" + previous); !os.IsNotExist(err) {
			t.Fatal("old avatar not removed")
		}
		previous = user.old
	}
	entries := publicRootEntries(t)
	if len(entries) != 1 {
		t.Fatal("replacement leaked file")
	}
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("injected read failure") }
func TestPartialUploadCleanup(t *testing.T) {
	dir := t.TempDir()
	m := storageAt(t, dir)
	for _, src := range []io.Reader{io.MultiReader(strings.NewReader("partial"), failedReader{}), strings.NewReader("oversize")} {
		u, err := m.Begin(t.Context(), 1, 7)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := u.Store(src, "file", ".bin"); err == nil {
			t.Fatal("expected write rejection")
		}
		if err = u.Finish(); err != nil {
			t.Fatal(err)
		}
		files, _ := os.ReadDir(filepath.Join(dir, "files"))
		if len(files) != 0 {
			t.Fatal("partial file leaked", files)
		}
	}
}
func TestRejectedAttachmentPublicationCleansFile(t *testing.T) {
	t.Chdir(t.TempDir())
	auth, token := uploadAuth(t)
	m := storageAt(t, "web/uploads")
	for _, fail := range []error{domain.ErrNotChatMember, domain.ErrChatWriteForbidden, errors.New("DB error")} {
		a := &uploadAttachments{fail: fail}
		w := httptest.NewRecorder()
		withUploadStorage(auth, HandleUploadMedia(auth, a), "media", m)(w, uploadRequest(t, token, "media", pngBytes(32), "7", 0))
		if w.Code != 403 && w.Code != 500 {
			t.Fatalf("status=%d", w.Code)
		}
		files, _ := os.ReadDir("web/uploads/media")
		knownRollback := errors.Is(fail, domain.ErrNotChatMember) || errors.Is(fail, domain.ErrChatWriteForbidden)
		if knownRollback && len(files) != 0 || !knownRollback && len(files) != 1 {
			t.Fatal("incorrect known/unknown publication retention", files)
		}
		for _, f := range files {
			if err := m.Remove("media/" + f.Name()); err != nil {
				t.Fatal(err)
			}
		}
	}
	a := &uploadAttachments{}
	w := httptest.NewRecorder()
	withUploadStorage(auth, HandleUploadFile(auth, a), "file", m)(w, uploadRequest(t, token, "file", []byte("file"), "", 0))
	if w.Code != 400 || a.created {
		t.Fatal("chat binding not required")
	}
}
func TestDownloadBoundaryAndSafeContent(t *testing.T) {
	t.Chdir(t.TempDir())
	auth, token := uploadAuth(t)
	_ = os.MkdirAll("web/uploads/files", 0755)
	file := "/uploads/files/file_test.bin"
	_ = os.WriteFile("web"+file, []byte("<script>unsafe</script>"), 0600)
	a := &uploadAttachments{item: &domain.Attachment{Path: file, Name: "audit.html", MIME: "text/html", Kind: "file"}}
	h := ProtectSession(auth, HandleDownload(auth, a), true, storageAt(t, "web/uploads"))
	for _, tc := range []struct {
		name, path, token string
		fail              error
		status            int
	}{
		{"anonymous", file, "", nil, 401}, {"outsider", file, token, domain.ErrMessageNotFound, 404},
		{"allowed", file, token, nil, 200}, {"traversal", "/uploads/../secret", token, nil, 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a.fail = tc.fail
			r := httptest.NewRequest("GET", tc.path, nil)
			r.Header.Set("Authorization", "Bearer "+tc.token)
			w := httptest.NewRecorder()
			h(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d", w.Code)
			}
			if tc.status == 200 && (w.Header().Get("Content-Type") != "application/octet-stream" || !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment;") || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Cache-Control") != "private, no-store") {
				t.Fatal(w.Header())
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := auth.RevokeSessionsContext(ctx, 1, func() error { return nil }, func() {}); err != nil {
				t.Fatalf("download leaked session barrier: %v", err)
			}
		})
	}
	_ = os.MkdirAll("outside", 0755)
	_ = os.WriteFile("outside/secret", []byte("secret"), 0600)
	abs, _ := filepath.Abs("outside/secret")
	_ = os.Symlink(abs, "web/uploads/files/link")
	r := httptest.NewRequest("GET", "/uploads/files/link", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	a.fail = nil
	h(w, r)
	if w.Code != 404 {
		t.Fatal("symlink escaped upload root")
	}
	for _, p := range []string{"/uploads/files/x", "/uploads", "/x/../uploads/files/x", "//uploads/files/x"} {
		if !IsUploadPath(p) {
			t.Fatal("static guard bypass", p)
		}
	}
}
