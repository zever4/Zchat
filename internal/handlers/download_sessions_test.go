package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
)

// Block the actual download handler at its first response write, independently
// of OS socket buffers. Cover headers too: writes to the connection can block.
type blockedDownloadWriter struct {
	*httptest.ResponseRecorder
	phase   string
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (w *blockedDownloadWriter) wait(phase string) {
	if w.phase == phase {
		w.once.Do(func() { close(w.entered); <-w.resume })
	}
}
func (w *blockedDownloadWriter) WriteHeader(status int) {
	w.wait("headers")
	w.ResponseRecorder.WriteHeader(status)
}
func (w *blockedDownloadWriter) Write(p []byte) (int, error) {
	w.wait("body")
	return w.ResponseRecorder.Write(p)
}

func downloadFixture(t *testing.T) (*service.AuthService, *handlerSessionUsers, string, *uploadAttachments) {
	t.Helper()
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("web/uploads/files", 0700); err != nil {
		t.Fatal(err)
	}
	const path = "/uploads/files/session-test.bin"
	if err := os.WriteFile("web"+path, []byte("already authorized bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	users := &handlerSessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "download-session-test-secret-at-least-32")
	token, err := auth.GenerateToken(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	return auth, users, token, &uploadAttachments{item: &domain.Attachment{Path: path, Name: "file.bin", Kind: "file"}}
}

func downloadRequest(token, path string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}

func TestDownloadResponseDoesNotDelayRevocation(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		// Users 1 and 65 share an auth stripe. A download must block neither
		// its own logout nor another account's revocation on that stripe.
		for _, revokeUser := range []int64{1, 65} {
			t.Run(fmt.Sprintf("%s/user%d", phase, revokeUser), func(t *testing.T) {
				auth, users, token, attachments := downloadFixture(t)
				h := ProtectSession(auth, HandleDownload(auth, attachments), true, storageAt(t, "web/uploads"))
				w := &blockedDownloadWriter{ResponseRecorder: httptest.NewRecorder(), phase: phase, entered: make(chan struct{}), resume: make(chan struct{})}
				done := make(chan struct{})
				go func() { defer close(done); h(w, downloadRequest(token, attachments.item.Path)) }()
				t.Cleanup(func() {
					close(w.resume)
					<-done
					if w.Code != http.StatusOK || w.Body.String() != "already authorized bytes" {
						t.Errorf("authorized transfer changed after revocation: status=%d body=%q", w.Code, w.Body.String())
					}
				})
				select {
				case <-w.entered:
				case <-time.After(time.Second):
					t.Fatal("download did not reach response write")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				defer cancel()
				disconnected := false
				err := auth.RevokeSessionsContext(ctx, revokeUser, func() error {
					if revokeUser == 1 {
						users.version.Add(1)
					}
					return nil
				}, func() { disconnected = true })
				if err != nil || !disconnected {
					t.Fatalf("blocked download delayed revocation: err=%v disconnected=%v", err, disconnected)
				}
				if revokeUser == 1 {
					rr := httptest.NewRecorder()
					h(rr, downloadRequest(token, attachments.item.Path))
					if rr.Code != http.StatusUnauthorized {
						t.Fatalf("new download after revoke: status=%d", rr.Code)
					}
				}
			})
		}
	}
}

type gatedDownloadMetadata struct {
	*uploadAttachments
	entered, resume chan struct{}
}

func (a *gatedDownloadMetadata) GetForUser(ctx context.Context, path string, userID int64) (*domain.Attachment, error) {
	close(a.entered)
	select {
	case <-a.resume:
		return a.uploadAttachments.GetForUser(ctx, path, userID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestDownloadAuthorizationStillHoldsBarrier(t *testing.T) {
	auth, _, token, attachments := downloadFixture(t)
	repo := &gatedDownloadMetadata{uploadAttachments: attachments, entered: make(chan struct{}), resume: make(chan struct{})}
	h := ProtectSession(auth, HandleDownload(auth, repo), true, storageAt(t, "web/uploads"))
	done := make(chan struct{})
	w := httptest.NewRecorder()
	go func() { defer close(done); h(w, downloadRequest(token, attachments.item.Path)) }()
	t.Cleanup(func() { close(repo.resume); <-done })
	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		t.Fatal("download did not reach membership lookup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := auth.RevokeSessionsContext(ctx, 1, func() error {
		t.Error("revocation passed an unfinished authorization lookup")
		return nil
	}, func() {}); err != context.DeadlineExceeded {
		t.Fatalf("barrier during authorization: %v", err)
	}
}
