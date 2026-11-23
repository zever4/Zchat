package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type E2EEBlobDownloader interface {
	Download(context.Context, string, []byte, *domain.E2EEBlobReadRequest, io.WriteCloser) error
	Head(context.Context, string, []byte, *domain.E2EEBlobReadRequest, io.WriteCloser) error
}

// Never mount through ProtectSession/authenticatedHandle: the coordinator owns
// short login/device guards, and streams without holding them. GET implies HEAD.
func HandleE2EEBlobDownload(downloader E2EEBlobDownloader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		outcome := "rejected"
		defer func() { metrics.ObserveBlobDownload(r.Method, outcome) }()
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		fail := func(status int, code string) { rejectBlobDownload(w, r, status, code) }
		if r.Context().Value(sessionReleaseKey{}) != nil {
			fail(503, "transport_unavailable")
			return
		}
		if (r.Method != http.MethodGet && r.Method != http.MethodHead) || r.Pattern != boundary.BlobDownloadPattern || r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			fail(400, "invalid_request")
			return
		}
		for _, name := range []string{"Content-Encoding", "Content-Type", "Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
			if len(r.Header.Values(name)) != 0 {
				fail(400, "invalid_request")
				return
			}
		}
		if len(r.Header.Values(boundary.BlobDownloadMetadataHeader)) != 1 {
			fail(400, "invalid_metadata")
			return
		}
		req, err := boundary.DecodeBlobDownloadMetadata(r.Header.Get(boundary.BlobDownloadMetadataHeader), r.PathValue("message_id"))
		if err != nil {
			fail(400, "invalid_metadata")
			return
		}
		if len(r.Header.Get("Authorization")) > 4096 || len(r.Header.Values("Grpc-Metadata-Authorization")) != 0 {
			fail(401, "session_invalid")
			return
		}
		token, err := requestToken(r)
		if err != nil {
			fail(401, "session_invalid")
			return
		}
		header := r.Header.Get(boundary.BlobDeviceCredentialHeader)
		if len(r.Header.Values(boundary.BlobDeviceCredentialHeader)) != 1 || len(header) != 43 {
			fail(401, "device_credential_invalid")
			return
		}
		credential, err := base64.RawURLEncoding.Strict().DecodeString(header)
		if err != nil || len(credential) != 32 || base64.RawURLEncoding.EncodeToString(credential) != header {
			fail(401, "device_credential_invalid")
			return
		}
		if downloader == nil {
			fail(503, "storage_unavailable")
			return
		}
		rc := http.NewResponseController(w)
		if err = rc.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
			fail(503, "transport_unavailable")
			return
		}
		sink := newBlobHTTPSink(w, rc, int64(req.Blob.CiphertextBytes), r)
		defer sink.Close()
		if r.Method == http.MethodHead {
			err = downloader.Head(r.Context(), token, credential, req, sink)
		} else {
			err = downloader.Download(r.Context(), token, credential, req, sink)
		}
		complete := sink.complete.Load()
		// HTTP/2 HEAD ends with the flushed headers. The request context may
		// be canceled while the coordinator joins cleanup for that response.
		if r.Method == http.MethodHead && complete &&
			errors.Is(r.Context().Err(), context.Canceled) && errors.Is(err, context.Canceled) {
			err = nil
		}
		if err != nil || !complete {
			if sink.started.Load() {
				outcome = "interrupted"
				// Never append an error JSON/change status after binary bytes.
				// Expire the socket/HTTP2 stream. A pre-completion failure keeps
				// the held byte back; bytes already flushed cannot be recalled.
				_ = rc.SetWriteDeadline(time.Now())
				return
			}
			if err == nil {
				err = domain.ErrE2EEBlobNotReady
			}
			blobDownloadHTTPError(w, r, err)
			return
		}
		_ = rc.SetWriteDeadline(time.Time{}) // EOF/flush joined; allow keep-alive reuse.
		outcome = "complete"
	}
}

func blobDownloadHeaders(w http.ResponseWriter, size int64) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("Content-Disposition", `attachment; filename="encrypted.bin"`)
	w.Header().Set("Accept-Ranges", "none")
}

type blobHTTPSink struct {
	head, headHTTP2           bool
	w                         http.ResponseWriter
	rc                        *http.ResponseController
	expected, accepted        int64 // Write/FinishDownload belong to one coordinator goroutine.
	last                      byte
	pending                   bool
	started, closed, complete atomic.Bool
	close                     func() error
}

func newBlobHTTPSink(w http.ResponseWriter, rc *http.ResponseController, size int64, r *http.Request) *blobHTTPSink {
	head := r.Method == http.MethodHead
	s := &blobHTTPSink{w: w, rc: rc, expected: size, head: head, headHTTP2: head && r.ProtoMajor == 2}
	s.close = sync.OnceValue(func() error {
		s.closed.Store(true)
		if s.started.Load() && !s.complete.Load() {
			return rc.SetWriteDeadline(time.Now())
		}
		return nil
	})
	return s
}
func (s *blobHTTPSink) Write(p []byte) (int, error) {
	if s.closed.Load() || s.head {
		return 0, io.ErrClosedPipe
	}
	if int64(len(p)) > s.expected-s.accepted {
		return 0, io.ErrShortWrite
	}
	if len(p) == 0 {
		return 0, nil
	}
	final := int64(len(p)) == s.expected-s.accepted
	body := p
	if final {
		body = p[:len(p)-1]
	}
	if !s.started.Swap(true) {
		blobDownloadHeaders(s.w, s.expected)
	}
	if s.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	n, err := s.w.Write(body)
	if err != nil {
		return n, err
	}
	if n != len(body) {
		return n, io.ErrShortWrite
	}
	s.accepted += int64(n)
	if final {
		s.last = p[len(p)-1]
		s.pending = true
		s.accepted++
		n++
	}
	return n, nil
}

// Hold only ONE final byte, so even final auth/COMMIT failures give an incomplete
// HTTP body. Finish/flush still runs inside the coordinator's ownership/watchers.
func (s *blobHTTPSink) FinishDownload() error {
	if s.closed.Load() {
		return io.ErrClosedPipe
	}
	if s.head {
		s.started.Store(true)
		if s.closed.Load() {
			return io.ErrClosedPipe
		}
		blobDownloadHeaders(s.w, s.expected)
		s.w.WriteHeader(http.StatusOK)
	} else {
		if !s.pending || s.accepted != s.expected {
			return io.ErrClosedPipe
		}
		n, err := s.w.Write([]byte{s.last})
		if err != nil {
			return err
		}
		if n != 1 {
			return io.ErrShortWrite
		}
	}
	// Go's HTTP/2 writer may report its normal END_STREAM closure from
	// FlushError after sending the HEAD headers. net/http keeps the marker
	// private, so match it exactly and only for HEAD on HTTP/2.
	if err := s.rc.Flush(); err != nil && !(s.headHTTP2 && err.Error() == "http2: request body closed due to handler exiting") {
		return err
	}
	// A successful HEAD flush already delivered the entire response.
	if !s.head && s.closed.Load() {
		return io.ErrClosedPipe
	}
	s.complete.Store(true)
	return nil
}

func (s *blobHTTPSink) Close() error { return s.close() }

func rejectBlobDownload(w http.ResponseWriter, r *http.Request, status int, code string) {
	w.Header().Set("Connection", "close")
	_ = http.NewResponseController(w).SetReadDeadline(time.Now())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
	}
}
func blobDownloadHTTPError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := http.StatusServiceUnavailable, "storage_unavailable"
	var network net.Error
	switch {
	case errors.Is(err, domain.ErrInvalidSession):
		status, code = 401, "session_invalid"
	case errors.Is(err, domain.ErrE2EEDeviceCredential), errors.Is(err, domain.ErrE2EEGenerationChanged):
		status, code = 401, "device_credential_invalid"
	case errors.Is(err, domain.ErrE2EEBlobReadUnavailable):
		status, code = 404, "file_unavailable"
	case errors.Is(err, admission.ErrBusy), errors.Is(err, service.ErrAuthBusy), errors.Is(err, blobstore.ErrBusy), errors.Is(err, storagebudget.ErrBusy):
		status, code = 429, "server_busy"
		w.Header().Set("Retry-After", "1")
	case errors.Is(err, boundary.ErrInvalid), errors.Is(err, domain.ErrInvalidInput):
		status, code = 400, "invalid_request"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.As(err, &network) && network.Timeout():
		status, code = 408, "request_timeout"
	}
	rejectBlobDownload(w, r, status, code)
}
