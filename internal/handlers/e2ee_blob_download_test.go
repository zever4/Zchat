package handlers

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type blobDownloaderModel struct {
	download func(context.Context, string, []byte, *domain.E2EEBlobReadRequest, io.WriteCloser) error
	head     func(context.Context, string, []byte, *domain.E2EEBlobReadRequest) error
}

func (m blobDownloaderModel) Download(c context.Context, t string, k []byte, r *domain.E2EEBlobReadRequest, w io.WriteCloser) error {
	defer w.Close()
	return m.download(c, t, k, r, w)
}
func (m blobDownloaderModel) Head(c context.Context, t string, k []byte, r *domain.E2EEBlobReadRequest, w io.WriteCloser) error {
	defer w.Close()
	if err := m.head(c, t, k, r); err != nil {
		return err
	}
	return w.(service.E2EEBlobDownloadFinalizer).FinishDownload()
}
func downloadHTTPIntent(data []byte) *domain.E2EEBlobReadRequest {
	r := blobHTTPIntent(data).Reservation
	return &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Recipient.GenerationId}, Binding: &pb.EpochSendBinding{ProtocolVersion: 3, MessageId: r.MessageID, ConversationId: r.ConversationID, Epoch: *r.CryptoEpoch, Sender: r.Sender, Recipient: r.Recipient, EnvelopeDigest: bytes.Repeat([]byte{43}, 32)}, Blob: r.Blob}
}
func downloadHTTPRequest(t *testing.T, method, url string, req *domain.E2EEBlobReadRequest) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, url+"/api/e2ee/attachments/"+req.Binding.MessageId, nil)
	if err != nil {
		t.Fatal(err)
	}
	setDownloadHeaders(t, r, req, "synthetic-token", bytes.Repeat([]byte{37}, 32))
	return r
}
func setDownloadHeaders(t *testing.T, r *http.Request, req *domain.E2EEBlobReadRequest, token string, credential []byte) {
	t.Helper()
	header, err := boundary.EncodeBlobDownloadMetadata(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(boundary.BlobDownloadMetadataHeader, header)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(boundary.BlobDeviceCredentialHeader, base64.RawURLEncoding.EncodeToString(credential))
}
func downloadHTTPMux(d E2EEBlobDownloader) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc(boundary.BlobDownloadPattern, HandleE2EEBlobDownload(d))
	return metrics.MetricsMiddleware(LimitRequests(m, m), m)
}

type downloadDeadlineRecorder struct{ *httptest.ResponseRecorder }

func (*downloadDeadlineRecorder) SetWriteDeadline(time.Time) error { return nil }
func (*downloadDeadlineRecorder) SetReadDeadline(time.Time) error  { return nil }

func TestBlobDownloadHTTPRejectsInputBeforeAuthOrBody(t *testing.T) {
	data := bytes.Repeat([]byte{42}, 64)
	req := downloadHTTPIntent(data)
	for _, tc := range []struct {
		name   string
		status int
		change func(*http.Request)
	}{
		{"query", 400, func(r *http.Request) { r.URL.RawQuery = "access_token=forbidden" }},
		{"empty query", 400, func(r *http.Request) { r.URL.ForceQuery = true }},
		{"body", 400, func(r *http.Request) { r.ContentLength = 1 }},
		{"unknown length", 400, func(r *http.Request) { r.ContentLength = -1 }},
		{"chunked", 400, func(r *http.Request) { r.TransferEncoding = []string{"chunked"} }},
		{"range", 400, func(r *http.Request) { r.Header.Set("Range", "bytes=0-1") }},
		{"conditional", 400, func(r *http.Request) { r.Header.Set("If-None-Match", "x") }},
		{"content type", 400, func(r *http.Request) { r.Header.Set("Content-Type", "application/json") }},
		{"content encoding", 400, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
		{"missing metadata", 400, func(r *http.Request) { r.Header.Del(boundary.BlobDownloadMetadataHeader) }},
		{"duplicate metadata", 400, func(r *http.Request) {
			r.Header.Add(boundary.BlobDownloadMetadataHeader, r.Header.Get(boundary.BlobDownloadMetadataHeader))
		}},
		{"oversize metadata", 400, func(r *http.Request) { r.Header.Set(boundary.BlobDownloadMetadataHeader, strings.Repeat("x", 4097)) }},
		{"no auth", 401, func(r *http.Request) { r.Header.Del("Authorization") }},
		{"duplicate auth", 401, func(r *http.Request) { r.Header.Add("Authorization", "Bearer another") }},
		{"auth alias", 401, func(r *http.Request) { r.Header.Set("Grpc-Metadata-Authorization", "Bearer another") }},
		{"missing credential", 401, func(r *http.Request) { r.Header.Del(boundary.BlobDeviceCredentialHeader) }},
		{"bad credential", 401, func(r *http.Request) { r.Header.Set(boundary.BlobDeviceCredentialHeader, strings.Repeat("!", 43)) }},
		{"duplicate credential", 401, func(r *http.Request) {
			r.Header.Add(boundary.BlobDeviceCredentialHeader, r.Header.Get(boundary.BlobDeviceCredentialHeader))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := downloadHTTPRequest(t, http.MethodGet, "http://example.invalid", req)
			spy := &blobSpyBody{Reader: bytes.NewReader([]byte("not JSON"))}
			r.Body = spy
			tc.change(r)
			d := blobDownloaderModel{download: func(context.Context, string, []byte, *domain.E2EEBlobReadRequest, io.WriteCloser) error {
				t.Error("invalid request reached coordinator")
				return nil
			}}
			w := httptest.NewRecorder()
			downloadHTTPMux(d).ServeHTTP(w, r)
			if w.Code != tc.status || spy.reads != 0 || w.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatal(w.Code, spy.reads, w.Body.String())
			}
		})
	}
}
func TestBlobDownloadHTTPRejectsNestedGuardsAndUnsupportedTransport(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "no deadline support", true: "nested session"}[nested], func(t *testing.T) {
			r := downloadHTTPRequest(t, http.MethodGet, "http://example.invalid", downloadHTTPIntent(bytes.Repeat([]byte{42}, 64)))
			if nested {
				r = r.WithContext(context.WithValue(r.Context(), sessionReleaseKey{}, func() {}))
			}
			w := httptest.NewRecorder()
			downloadHTTPMux(blobDownloaderModel{download: func(context.Context, string, []byte, *domain.E2EEBlobReadRequest, io.WriteCloser) error {
				t.Error("invalid composition reached coordinator")
				return nil
			}}).ServeHTTP(w, r)
			if w.Code != 503 {
				t.Fatal(w.Code)
			}
		})
	}
}
func TestBlobDownloadHTTPErrorMappingAndHeadHasNoBody(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{domain.ErrInvalidSession, 401, "session_invalid"}, {domain.ErrE2EEDeviceCredential, 401, "device_credential_invalid"}, {domain.ErrE2EEGenerationChanged, 401, "device_credential_invalid"},
		{domain.ErrE2EEBlobReadUnavailable, 404, "file_unavailable"}, {storagebudget.ErrBusy, 429, "server_busy"}, {blobstore.ErrIntegrity, 503, "storage_unavailable"},
		{context.DeadlineExceeded, 408, "request_timeout"}, {errors.New("private-token-path-db-detail"), 503, "storage_unavailable"},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			t.Run(method+tc.code, func(t *testing.T) {
				d := blobDownloaderModel{download: func(context.Context, string, []byte, *domain.E2EEBlobReadRequest, io.WriteCloser) error {
					return tc.err
				}, head: func(context.Context, string, []byte, *domain.E2EEBlobReadRequest) error { return tc.err }}
				w := &downloadDeadlineRecorder{httptest.NewRecorder()}
				downloadHTTPMux(d).ServeHTTP(w, downloadHTTPRequest(t, method, "http://example.invalid", downloadHTTPIntent(bytes.Repeat([]byte{42}, 64))))
				if w.Code != tc.status || strings.Contains(w.Body.String(), "private-token-path-db-detail") || w.Body.Len() > 128 {
					t.Fatal(w.Code, w.Body.String())
				}
				if method == http.MethodHead {
					if w.Body.Len() != 0 {
						t.Fatal("HEAD error sent body")
					}
				} else if !strings.Contains(w.Body.String(), tc.code) {
					t.Fatal(w.Body.String())
				}
			})
		}
	}
}
func TestBlobDownloadHTTPSuccessAndKeepAlive(t *testing.T) {
	data := bytes.Repeat([]byte{0xff}, 65536)
	req := downloadHTTPIntent(data)
	heads := 0
	d := blobDownloaderModel{download: func(_ context.Context, token string, k []byte, input *domain.E2EEBlobReadRequest, w io.WriteCloser) error {
		if token != "synthetic-token" || !bytes.Equal(k, bytes.Repeat([]byte{37}, 32)) || input.Binding.MessageId != req.Binding.MessageId {
			t.Error("auth/binding changed")
		}
		if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
			return err
		}
		return w.(service.E2EEBlobDownloadFinalizer).FinishDownload()
	}, head: func(context.Context, string, []byte, *domain.E2EEBlobReadRequest) error { heads++; return nil }}
	s := httptest.NewServer(downloadHTTPMux(d))
	defer s.Close()
	for i, method := range []string{http.MethodGet, http.MethodHead, http.MethodGet} {
		r := downloadHTTPRequest(t, method, s.URL, req)
		reused := make(chan bool, 1)
		r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { reused <- i.Reused }}))
		resp, err := s.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || resp.ContentLength != int64(len(data)) || resp.Close || resp.Header.Get("Content-Type") != "application/octet-stream" || resp.Header.Get("Cache-Control") != "private, no-store" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Fatal(resp.StatusCode, err)
		}
		if method == http.MethodHead {
			if len(body) != 0 {
				t.Fatal("HEAD sent ciphertext")
			}
		} else if !bytes.Equal(body, data) {
			t.Fatal("ciphertext changed")
		}
		if wasReused := <-reused; i > 0 && !wasReused {
			t.Fatal("successful transfer poisoned keep-alive")
		}
	}
	if heads != 1 {
		t.Fatal("HEAD read body")
	}
}
func TestBlobDownloadHTTPFinalFailureLeavesIncompleteBinaryResponse(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing finalizer", true: "final auth error"}[fail], func(t *testing.T) {
			data := bytes.Repeat([]byte{0xff}, 65536)
			d := blobDownloaderModel{download: func(_ context.Context, _ string, _ []byte, _ *domain.E2EEBlobReadRequest, w io.WriteCloser) error {
				if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
					return err
				}
				if fail {
					return domain.ErrInvalidSession
				}
				return nil
			}}
			s := httptest.NewServer(downloadHTTPMux(d))
			defer s.Close()
			resp, err := s.Client().Do(downloadHTTPRequest(t, http.MethodGet, s.URL, downloadHTTPIntent(data)))
			if err != nil {
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err == nil || len(body) >= len(data) || !bytes.Equal(body, bytes.Repeat([]byte{0xff}, len(body))) {
				t.Fatal("failed transfer looked complete or appended JSON", len(body), err)
			}
		})
	}
}
