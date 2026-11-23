package handlers

import (
	"bytes"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/service"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPBusyDoesNotBecomeUnauthorizedOrEnterHandler(t *testing.T) {
	u := &handlerSessionUsers{}
	u.version.Store(1)
	a := service.NewAuthService(u, "admission-http-test-secret-atleast32")
	g := admission.New(admission.DefaultConfig(), nil)
	a.SetRequestAdmission(g)
	raw, _ := a.GenerateToken(1, 1)
	var releases []func()
	for i := int64(10); i < 26; i++ {
		r, e := g.Try(t.Context(), i, admission.Data)
		if e != nil {
			t.Fatal(e)
		}
		releases = append(releases, r)
	}
	defer func() {
		for _, r := range releases {
			r()
		}
	}()
	h := ProtectSession(a, func(http.ResponseWriter, *http.Request) { t.Error("busy request entered handler") }, false)
	for _, value := range []string{raw, "invalid"} {
		r := httptest.NewRequest("GET", "http://fixture/api/settings", nil)
		r.Header.Set("Authorization", "Bearer "+value)
		w := httptest.NewRecorder()
		h(w, r)
		want := http.StatusTooManyRequests
		if value == "invalid" {
			want = http.StatusUnauthorized
		}
		if w.Code != want {
			t.Fatal(w.Code, want)
		}
		if value == raw && w.Header().Get("Retry-After") != "1" {
			t.Fatal("missing retry hint")
		}
	}
}

type admissionBody struct {
	io.Reader
	read func()
}

func (b admissionBody) Read(p []byte) (int, error) { b.read(); return b.Reader.Read(p) }
func TestUploadReleasesAdmissionWhileReadingAndDownloadBeforeStreaming(t *testing.T) {
	u := &handlerSessionUsers{}
	u.version.Store(1)
	a := service.NewAuthService(u, "admission-http-test-secret-atleast32")
	g := admission.New(admission.DefaultConfig(), nil)
	a.SetRequestAdmission(g)
	raw, _ := a.GenerateToken(1, 1)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if e := writer.WriteField("fixture", "value"); e != nil {
		t.Fatal(e)
	}
	part, err := writer.CreateFormFile("file", "test.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = part.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	r := httptest.NewRequest("POST", "http://fixture/api/files/upload", nil)
	r.Pattern = "POST /api/files/upload"
	r.Header.Set("Content-Type", writer.FormDataContentType())
	r.Header.Set("Authorization", "Bearer "+raw)
	reads := 0
	r.Body = io.NopCloser(admissionBody{Reader: bytes.NewReader(body.Bytes()), read: func() {
		reads++
		if g.Snapshot().Active != 0 {
			t.Error("body read holds budget")
		}
	}})
	w := httptest.NewRecorder()
	ProtectSession(a, func(w http.ResponseWriter, r *http.Request) {
		if g.Snapshot().Active != 1 {
			t.Error("publish not admitted")
		}
		releaseDownloadSession(r)
		releaseDownloadSession(r)
		if g.Snapshot().Active != 0 {
			t.Error("stream holds budget")
		}
		w.WriteHeader(http.StatusNoContent)
	}, false, storageAt(t, t.TempDir()))(w, r)
	if w.Code != 204 || reads == 0 || g.Snapshot().Active != 0 {
		t.Fatal(w.Code, reads, g.Snapshot())
	}
}
