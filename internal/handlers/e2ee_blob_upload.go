package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type E2EEBlobUploader interface {
	Upload(context.Context, string, []byte, *domain.E2EEBlobAttemptRequest, io.ReadCloser) (*domain.E2EEBlobWriteLease, error)
}

// Mount only on BlobUploadPattern, through LimitRequests. The authenticated
// coordinator owns login/device checks and physical admission; ProtectSession's
// multipart parsing and login guard MUST NOT wrap this streaming handler.
// Main mounts it only with opt-in storage maintenance and experimental API flags.
func HandleE2EEBlobUpload(uploader E2EEBlobUploader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		fail := func(status int, code string) { rejectBlobUpload(w, status, code) }
		if r.Context().Value(sessionReleaseKey{}) != nil {
			// Refuse accidental composition through authenticatedHandle: nesting
			// login barriers behind a queued revoke can block the whole stream.
			fail(503, "transport_unavailable")
			return
		}
		if r.Method != http.MethodPut || r.Pattern != boundary.BlobUploadPattern || r.URL.RawQuery != "" || r.URL.ForceQuery {
			fail(400, "invalid_request")
			return
		}
		if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/octet-stream" || len(r.Header.Values("Content-Encoding")) != 0 {
			fail(415, "unsupported_content_type")
			return
		}
		if r.ContentLength < 0 || len(r.TransferEncoding) != 0 {
			fail(411, "content_length_required")
			return
		}
		if len(r.Header.Values(boundary.BlobUploadMetadataHeader)) != 1 {
			fail(400, "invalid_metadata")
			return
		}
		attempt, err := boundary.DecodeBlobUploadMetadata(r.Header.Get(boundary.BlobUploadMetadataHeader), r.PathValue("attempt_id"))
		if err != nil {
			fail(400, "invalid_metadata")
			return
		}
		if r.ContentLength != int64(attempt.Reservation.Blob.CiphertextBytes) {
			fail(400, "content_length_mismatch")
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
		if uploader == nil {
			fail(503, "storage_unavailable")
			return
		}
		rc := http.NewResponseController(w)
		if err = rc.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			fail(503, "transport_unavailable")
			return
		}
		src := &blobHTTPBody{body: http.MaxBytesReader(w, r.Body, r.ContentLength), rc: rc}
		defer src.Close()
		result, err := uploader.Upload(r.Context(), token, credential, attempt, src)
		if err != nil {
			blobHTTPError(w, err)
			return
		}
		if result == nil || result.Grant.State != "ready" || result.Grant.Expired || result.Grant.AttemptID != attempt.AttemptID || result.Grant.Revision != attempt.Revision || result.Grant.MessageID != attempt.Reservation.MessageID || result.Grant.OperationID != attempt.Reservation.OperationID || !src.complete.Load() {
			fail(503, "storage_unavailable")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(struct {
			State       string    `json:"state"`
			MessageID   string    `json:"message_id"`
			AttemptID   string    `json:"attempt_id"`
			Revision    uint64    `json:"revision,string"`
			OperationID string    `json:"operation_id"`
			ExpiresAt   time.Time `json:"expires_at"`
		}{"ready", result.Grant.MessageID, result.Grant.AttemptID, result.Grant.Revision, result.Grant.OperationID, result.Grant.ExpiresAt})
	}
}

// net/http Body.Close may wait for a blocked Read or drain unread bytes. Expire
// the actual socket/stream FIRST; closing the wrapper alone can deadlock revoke.
// EOF and Close are observed concurrently by the store cancellation callback.
type blobHTTPBody struct {
	body     io.ReadCloser
	rc       *http.ResponseController
	complete atomic.Bool
	once     sync.Once
	err      error
}

func (b *blobHTTPBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if err == io.EOF {
		b.complete.Store(true)
	}
	return n, err
}
func (b *blobHTTPBody) Close() error {
	b.once.Do(func() {
		incomplete := !b.complete.Load()
		if incomplete {
			b.err = b.rc.SetReadDeadline(time.Now())
		}
		closeErr := b.body.Close()
		var timeout net.Error
		// The expired deadline deliberately stops net/http's drain. That expected
		// cleanup timeout must not turn an unknown DB COMMIT into a body timeout.
		if incomplete && b.err == nil && errors.As(closeErr, &timeout) && timeout.Timeout() {
			closeErr = nil
		}
		b.err = errors.Join(b.err, closeErr)
	})
	return b.err
}

func rejectBlobUpload(w http.ResponseWriter, status int, code string) {
	// Headers only: do not drain an attacker-controlled incomplete body to reply.
	w.Header().Set("Connection", "close")
	_ = http.NewResponseController(w).SetReadDeadline(time.Now())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}
func blobHTTPError(w http.ResponseWriter, err error) {
	status, code := http.StatusServiceUnavailable, "storage_unavailable"
	var limit *http.MaxBytesError
	var network net.Error
	switch {
	case errors.Is(err, domain.ErrInvalidSession):
		status, code = 401, "session_invalid"
	case errors.Is(err, domain.ErrE2EEDeviceCredential):
		status, code = 401, "device_credential_invalid"
	case errors.Is(err, storagebudget.ErrQuota), errors.Is(err, storagebudget.ErrHeadroom), errors.Is(err, blobstore.ErrHeadroom), errors.Is(err, domain.ErrE2EEBlobCapacity):
		status, code = 507, "storage_capacity"
	case errors.Is(err, admission.ErrBusy), errors.Is(err, service.ErrAuthBusy), errors.Is(err, blobstore.ErrBusy), errors.Is(err, storagebudget.ErrBusy):
		status, code = 429, "server_busy"
		w.Header().Set("Retry-After", "1")
	case errors.Is(err, domain.ErrE2EEBlobLeaseExpired):
		status, code = 410, "attempt_expired"
	case errors.Is(err, domain.ErrE2EEEpochInactive), errors.Is(err, domain.ErrE2EEEpochSealed):
		status, code = 409, "epoch_changed"
	case errors.Is(err, domain.ErrE2EEBlobCAS), errors.Is(err, domain.ErrE2EEBlobWriteState), errors.Is(err, domain.ErrE2EEMessageConflict), errors.Is(err, blobstore.ErrConflict), errors.Is(err, domain.ErrE2EEConversationClosed), errors.Is(err, domain.ErrE2EEGenerationChanged):
		status, code = 409, "attempt_conflict"
	case errors.Is(err, domain.ErrE2EEConversationNotFound), errors.Is(err, domain.ErrE2EEPeerUnavailable):
		status, code = 404, "conversation_unavailable"
	case errors.As(err, &limit):
		status, code = 413, "body_too_large"
	case errors.Is(err, blobstore.ErrIntegrity), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, boundary.ErrInvalid), errors.Is(err, domain.ErrInvalidInput):
		status, code = 400, "invalid_ciphertext"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), errors.As(err, &network) && network.Timeout():
		status, code = 408, "request_timeout"
	}
	rejectBlobUpload(w, status, code)
}
