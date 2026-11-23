package handlers

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/uploadstorage"
)

type pauseUploadCopy struct {
	multipart.File
	afterSeek        bool
	entered, release chan struct{}
	once             sync.Once
}

func (f *pauseUploadCopy) Seek(off int64, whence int) (int64, error) {
	n, err := f.File.Seek(off, whence)
	f.afterSeek = true
	return n, err
}
func (f *pauseUploadCopy) Read(p []byte) (int, error) {
	if f.afterSeek {
		f.once.Do(func() { close(f.entered); <-f.release })
	}
	return f.File.Read(p)
}
func TestRevocationDoesNotWaitForFinalFileCopyAndPreventsPublish(t *testing.T) {
	m := storageAt(t, t.TempDir())
	base, _ := m.Usage()
	users := &handlerSessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "managed-upload-copy-session-secret-at-least32")
	token, err := auth.GenerateToken(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(resume) })
	repo := &uploadAttachments{}
	h := withUploadStorage(auth, func(w http.ResponseWriter, r *http.Request) {
		p := requestUpload(r)
		p.file = &pauseUploadCopy{File: p.file, entered: entered, release: resume}
		HandleUploadFile(auth, repo)(w, r)
	}, "file", m)
	done := make(chan struct{})
	w := httptest.NewRecorder()
	r := uploadRequest(t, token, "file", bytes.Repeat([]byte("x"), 1024), "7", 0)
	go func() { defer close(done); h(w, r) }()
	t.Cleanup(func() {
		once.Do(func() { close(resume) })
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("copy did not join")
		}
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("final copy not reached")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if err = auth.RevokeSessionsContext(ctx, 1, func() error { users.version.Store(2); return nil }, func() {}); err != nil {
		t.Fatal("revocation held by filesystem copy", err)
	}
	once.Do(func() { close(resume) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not finish")
	}
	if w.Code != 401 || repo.created {
		t.Fatal("revoked session published copied bytes", w.Code, repo.created)
	}
	if got, n := m.Usage(); got != base || n != 0 {
		t.Fatal("rejected copy retained bytes", got, base, n)
	}
}

func TestStorageQuotaRefusesBeforeBodyOrSpool(t *testing.T) {
	root := t.TempDir()
	c, _ := storagebudget.ConfigFromEnv(func(string) string { return "" })
	c.TotalBytes = 10_000_000
	c.AccountBytes = 1_000_000
	c.FreeBytes = 1
	c.FreeEntries = 1
	m, err := uploadstorage.Open(t.Context(), root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	base, _ := m.Usage()
	auth, token := uploadAuth(t)
	reads := 0
	r := httptest.NewRequest("POST", "/api/files/upload", nil)
	r.Pattern = "POST /api/files/upload"
	r.ContentLength = -1
	r.Header.Set("Content-Type", "multipart/form-data; boundary=x")
	r.Header.Set("Authorization", "Bearer "+token)
	r.Body = io.NopCloser(admissionBody{Reader: strings.NewReader("must not read"), read: func() { reads++ }})
	w := httptest.NewRecorder()
	ProtectSession(auth, func(http.ResponseWriter, *http.Request) { t.Error("quota rejection reached handler") }, false, m)(w, r)
	if w.Code != 507 || reads != 0 || w.Header().Get("Connection") != "close" {
		t.Fatal(w.Code, reads, w.Header())
	}
	if got, n := m.Usage(); got != base || n != 0 {
		t.Fatal("rejected reservation leaked", got, n)
	}
	files, err := os.ReadDir(filepath.Join(root, ".spool"))
	if err != nil || len(files) != 0 {
		t.Fatal(files, err)
	}
	// A real client sends only headers and waits: rejection must not drain a body.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/files/upload", ProtectSession(auth, func(http.ResponseWriter, *http.Request) { t.Error("unexpected handler") }, false, m))
	server := httptest.NewServer(LimitRequests(mux, mux))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = fmt.Fprintf(conn, "POST /api/files/upload HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer %s\r\nContent-Type: multipart/form-data; boundary=x\r\nContent-Length: 2000000\r\n\r\n", token)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("response waited for body", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 507 {
		t.Fatal(response.StatusCode)
	}
}
func TestMultipartUsesOnlyControlledSpoolAndPreservesFields(t *testing.T) {
	t.Chdir(t.TempDir())
	m := storageAt(t, "web/uploads")
	outside := t.TempDir()
	t.Setenv("TMPDIR", outside)
	auth, token := uploadAuth(t)
	repo := &uploadAttachments{}
	h := withUploadStorage(auth, func(w http.ResponseWriter, r *http.Request) {
		files, err := os.ReadDir("web/uploads/.spool")
		if err != nil || len(files) != 1 {
			t.Fatal("missing controlled spool", files, err)
		}
		if r.FormValue("chat_id") != "7" {
			t.Fatal("form values lost")
		}
		HandleUploadMedia(auth, repo)(w, r)
	}, "media", m)
	w := httptest.NewRecorder()
	h(w, uploadRequest(t, token, "media", pngBytes(2<<20), "7", 0))
	if w.Code != 200 || !repo.created {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, dir := range []string{outside, "web/uploads/.spool"} {
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 0 {
			t.Fatal("uncontrolled or leaked temp", dir, files, err)
		}
	}
	if !strings.Contains(repo.item.Path, "_u1_") {
		t.Fatal("missing durable account attribution", repo.item.Path)
	}
}
func TestMultipartHugeHeaderStopsBeforeAllocationOrSpool(t *testing.T) {
	m := storageAt(t, t.TempDir())
	auth, token := uploadAuth(t)
	reads := 0
	body := []byte("--x\r\nX-Huge: " + strings.Repeat("a", 2<<20) + "\r\n\r\n")
	r := httptest.NewRequest("POST", "/api/files/upload", nil)
	r.Pattern = "POST /api/files/upload"
	r.ContentLength = -1
	r.Header.Set("Content-Type", "multipart/form-data; boundary=x")
	r.Header.Set("Authorization", "Bearer "+token)
	r.Body = io.NopCloser(admissionBody{Reader: bytes.NewReader(body), read: func() { reads++ }})
	w := httptest.NewRecorder()
	ProtectSession(auth, func(http.ResponseWriter, *http.Request) { t.Error("header overflow reached handler") }, false, m)(w, r)
	if w.Code != 413 || reads > 20 {
		t.Fatal("header read was not bounded", w.Code, reads, w.Body.String())
	}
	if _, n := m.Usage(); n != 0 {
		t.Fatal(n)
	}
}
func TestKnownSmallBodyDoesNotReserveMaximumFileRoute(t *testing.T) {
	root := t.TempDir()
	c, _ := storagebudget.ConfigFromEnv(func(string) string { return "" })
	c.AccountBytes = 1_000_000
	c.FreeBytes = 1
	c.FreeEntries = 1
	m, err := uploadstorage.Open(t.Context(), root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	auth, token := uploadAuth(t)
	repo := &uploadAttachments{}
	r := uploadRequest(t, token, "file", []byte("small"), "7", 0)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	w := httptest.NewRecorder()
	withUploadStorage(auth, HandleUploadFile(auth, repo), "file", m)(w, r)
	if w.Code != 200 || repo.item == nil || repo.item.Size != 5 {
		t.Fatal("small known body charged as50MB", w.Code, w.Body.String())
	}
}
func TestUnknownAttachmentCommitKeepsDownloadableFileAndQuota(t *testing.T) {
	t.Chdir(t.TempDir())
	m := storageAt(t, "web/uploads")
	base, _ := m.Usage()
	auth, token := uploadAuth(t)
	repo := &uploadAttachments{fail: errors.New("reply lost after commit")}
	w := httptest.NewRecorder()
	withUploadStorage(auth, HandleUploadFile(auth, repo), "file", m)(w, uploadRequest(t, token, "file", []byte("persisted"), "7", 0))
	if w.Code != 500 || repo.item == nil {
		t.Fatal(w.Code)
	}
	if got, _ := m.Usage(); got.Bytes <= base.Bytes {
		t.Fatal("unknown outcome lost physical charge")
	}
	repo.fail = nil
	response := httptest.NewRecorder()
	ProtectSession(auth, HandleDownload(auth, repo), false, m)(response, downloadRequest(token, repo.item.Path))
	if response.Code != 200 || response.Body.String() != "persisted" {
		t.Fatal("unknown SQL commit lost file", response.Code, response.Body.String())
	}
}
