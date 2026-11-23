package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type blobUploaderFunc func(context.Context, string, []byte, *domain.E2EEBlobAttemptRequest, io.ReadCloser) (*domain.E2EEBlobWriteLease, error)

func (f blobUploaderFunc) Upload(c context.Context, t string, k []byte, a *domain.E2EEBlobAttemptRequest, b io.ReadCloser) (*domain.E2EEBlobWriteLease, error) {
	return f(c, t, k, a, b)
}

func blobHTTPIntent(data []byte) *domain.E2EEBlobAttemptRequest {
	epoch := uint64(0)
	gen := uuid.NewString()
	hash := sha256.Sum256(data)
	return &domain.E2EEBlobAttemptRequest{AttemptID: uuid.NewString(), Revision: 1, Reservation: &domain.E2EEBlobReservationRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: gen}, MessageID: uuid.NewString(), ConversationID: uuid.NewString(), CryptoEpoch: &epoch,
		Sender: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: gen, DeviceId: 1}, Recipient: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: uuid.NewString(), DeviceId: 1}, Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: uint64(len(data)), Sha256: hash[:]}, OperationID: uuid.NewString()}}
}
func blobHTTPRequest(t *testing.T, a *domain.E2EEBlobAttemptRequest, data []byte) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/api/e2ee/blobs/"+a.AttemptID, bytes.NewReader(data))
	setBlobHTTPHeaders(t, r, a, "synthetic-token", bytes.Repeat([]byte{37}, 32))
	return r
}
func setBlobHTTPHeaders(t *testing.T, r *http.Request, a *domain.E2EEBlobAttemptRequest, token string, credential []byte) {
	t.Helper()
	meta, err := boundary.EncodeBlobUploadMetadata(a)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/octet-stream")
	r.Header.Set(boundary.BlobUploadMetadataHeader, meta)
	r.Header.Set(boundary.BlobDeviceCredentialHeader, base64.RawURLEncoding.EncodeToString(credential))
}
func blobHTTPMux(u E2EEBlobUploader) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc(boundary.BlobUploadPattern, HandleE2EEBlobUpload(u))
	return metrics.MetricsMiddleware(LimitRequests(m, m), m)
}

type blobSpyBody struct {
	io.Reader
	reads int
}

func (b *blobSpyBody) Read(p []byte) (int, error) { b.reads++; return b.Reader.Read(p) }
func (b *blobSpyBody) Close() error               { return nil }

type blobDeadlineRecorder struct{ *httptest.ResponseRecorder }

func (*blobDeadlineRecorder) SetReadDeadline(time.Time) error { return nil }

func TestBlobHTTPRejectsHeadersWithoutReadingBody(t *testing.T) {
	data := bytes.Repeat([]byte{0xff}, 64)
	for _, tc := range []struct {
		name   string
		status int
		change func(*http.Request)
	}{
		{"missing auth", 401, func(r *http.Request) { r.Header.Del("Authorization") }},
		{"duplicate auth", 401, func(r *http.Request) { r.Header.Add("Authorization", "Bearer second") }},
		{"auth alias", 401, func(r *http.Request) { r.Header.Set("Grpc-Metadata-Authorization", "Bearer second") }},
		{"query token", 400, func(r *http.Request) { r.URL.RawQuery = "access_token=secret" }},
		{"empty query", 400, func(r *http.Request) { r.URL.ForceQuery = true }},
		{"json", 415, func(r *http.Request) { r.Header.Set("Content-Type", "application/json") }},
		{"encoding", 415, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
		{"chunked", 411, func(r *http.Request) { r.ContentLength = -1; r.TransferEncoding = []string{"chunked"} }},
		{"length mismatch", 400, func(r *http.Request) { r.ContentLength-- }},
		{"too large", 413, func(r *http.Request) { r.ContentLength = boundary.MaxAttachmentBlobBytes + 1 }},
		{"missing metadata", 400, func(r *http.Request) { r.Header.Del(boundary.BlobUploadMetadataHeader) }},
		{"oversized metadata", 400, func(r *http.Request) { r.Header.Set(boundary.BlobUploadMetadataHeader, strings.Repeat("x", 4097)) }},
		{"duplicate metadata", 400, func(r *http.Request) {
			r.Header.Add(boundary.BlobUploadMetadataHeader, r.Header.Get(boundary.BlobUploadMetadataHeader))
		}},
		{"path mismatch", 400, func(r *http.Request) { r.URL.Path = "/api/e2ee/blobs/" + uuid.NewString() }},
		{"missing device", 401, func(r *http.Request) { r.Header.Del(boundary.BlobDeviceCredentialHeader) }},
		{"duplicate device", 401, func(r *http.Request) {
			r.Header.Add(boundary.BlobDeviceCredentialHeader, r.Header.Get(boundary.BlobDeviceCredentialHeader))
		}},
		{"bad device", 401, func(r *http.Request) { r.Header.Set(boundary.BlobDeviceCredentialHeader, strings.Repeat("!", 43)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := blobHTTPIntent(data)
			r := blobHTTPRequest(t, a, data)
			spy := &blobSpyBody{Reader: bytes.NewReader(data)}
			r.Body = spy
			tc.change(r)
			u := blobUploaderFunc(func(context.Context, string, []byte, *domain.E2EEBlobAttemptRequest, io.ReadCloser) (*domain.E2EEBlobWriteLease, error) {
				t.Error("invalid request reached coordinator")
				return nil, nil
			})
			w := httptest.NewRecorder()
			blobHTTPMux(u).ServeHTTP(w, r)
			if w.Code != tc.status || spy.reads != 0 || w.Header().Get("Connection") != "close" {
				t.Fatal(w.Code, spy.reads, w.Body.String())
			}
		})
	}
}

func TestBlobHTTPRejectsNestedSessionGuardBeforeBody(t *testing.T) {
	data := bytes.Repeat([]byte{42}, 64)
	r := blobHTTPRequest(t, blobHTTPIntent(data), data)
	r = r.WithContext(context.WithValue(r.Context(), sessionReleaseKey{}, func() {}))
	spy := &blobSpyBody{Reader: bytes.NewReader(data)}
	r.Body = spy
	u := blobUploaderFunc(func(context.Context, string, []byte, *domain.E2EEBlobAttemptRequest, io.ReadCloser) (*domain.E2EEBlobWriteLease, error) {
		t.Error("nested session barrier reached coordinator")
		return nil, nil
	})
	w := httptest.NewRecorder()
	blobHTTPMux(u).ServeHTTP(w, r)
	if w.Code != 503 || spy.reads != 0 {
		t.Fatal(w.Code, spy.reads, w.Body.String())
	}
}
func TestBlobHTTPStreamingBypassesJSONAndKeepsConnectionReusable(t *testing.T) {
	data := bytes.Repeat([]byte{0xff}, 65536)
	a := blobHTTPIntent(data)
	u := blobUploaderFunc(func(_ context.Context, token string, cred []byte, input *domain.E2EEBlobAttemptRequest, src io.ReadCloser) (*domain.E2EEBlobWriteLease, error) {
		defer src.Close()
		got, err := io.ReadAll(src)
		if err != nil || !bytes.Equal(got, data) || token != "synthetic-token" || !bytes.Equal(cred, bytes.Repeat([]byte{37}, 32)) {
			t.Error("binary body/auth changed", err)
		}
		return &domain.E2EEBlobWriteLease{Grant: domain.E2EEBlobReservation{MessageID: input.Reservation.MessageID, AttemptID: input.AttemptID, Revision: input.Revision, OperationID: input.Reservation.OperationID, State: "ready", ExpiresAt: time.Now().Add(time.Hour)}}, err
	})
	s := httptest.NewServer(blobHTTPMux(u))
	defer s.Close()
	for i := range 2 {
		r, err := http.NewRequest(http.MethodPut, s.URL+"/api/e2ee/blobs/"+a.AttemptID, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		setBlobHTTPHeaders(t, r, a, "synthetic-token", bytes.Repeat([]byte{37}, 32))
		reused := make(chan bool, 1)
		r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused <- info.Reused }}))
		resp, err := s.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 201 || resp.Close || resp.Header.Get("Cache-Control") != "private, no-store" || !bytes.Contains(body, []byte(`"state":"ready"`)) {
			t.Fatal(resp.StatusCode, string(body), err)
		}
		if wasReused := <-reused; i == 1 && !wasReused {
			t.Fatal("successful EOF left connection unusable")
		}
	}
}
func TestBlobHTTPGenericErrorsAreBoundedAndDoNotLeakDetails(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{domain.ErrInvalidSession, 401, "session_invalid"}, {domain.ErrE2EEDeviceCredential, 401, "device_credential_invalid"},
		{storagebudget.ErrQuota, 507, "storage_capacity"}, {blobstore.ErrBusy, 429, "server_busy"}, {domain.ErrE2EEBlobCAS, 409, "attempt_conflict"},
		{domain.ErrE2EEBlobLeaseExpired, 410, "attempt_expired"}, {blobstore.ErrIntegrity, 400, "invalid_ciphertext"}, {context.DeadlineExceeded, 408, "request_timeout"},
		{errors.New("synthetic DB error contains private-token-or-path"), 503, "storage_unavailable"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			data := bytes.Repeat([]byte{42}, 64)
			r := blobHTTPRequest(t, blobHTTPIntent(data), data)
			u := blobUploaderFunc(func(_ context.Context, _ string, _ []byte, _ *domain.E2EEBlobAttemptRequest, src io.ReadCloser) (*domain.E2EEBlobWriteLease, error) {
				defer src.Close()
				return nil, tc.err
			})
			w := &blobDeadlineRecorder{httptest.NewRecorder()}
			blobHTTPMux(u).ServeHTTP(w, r)
			if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) || strings.Contains(w.Body.String(), "private-token-or-path") || w.Body.Len() > 128 {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}
