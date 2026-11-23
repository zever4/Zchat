//go:build linux

package handlers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	dto "github.com/prometheus/client_model/go"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"
)

// Actual auth/PG/private FS/HTTP socket, opaque synthetic descriptor/file bytes.
// No native Signal/decryption/GUI or durable file ACK is implied by these tests.
func acceptedHTTPDownload(t *testing.T, large bool) (*blobHTTPFixture, string, *domain.E2EEBlobReadRequest) {
	t.Helper()
	f := blobHTTPPostgres(t)
	if large {
		// New immutable reservation in the same real chat; never mutate an old claim.
		r := f.intent.Reservation
		f.data = bytes.Repeat([]byte{0xff}, 6<<20)
		next := blobHTTPIntent(f.data)
		next.Reservation.Context = proto.Clone(r.Context).(*pb.ClientContext)
		next.Reservation.ConversationID = r.ConversationID
		next.Reservation.Sender = proto.Clone(r.Sender).(*pb.DeviceRef)
		next.Reservation.Recipient = proto.Clone(r.Recipient).(*pb.DeviceRef)
		g, err := f.repo.Reserve(t.Context(), f.uid, next.Reservation)
		if err != nil {
			t.Fatal(err)
		}
		next.AttemptID, next.Revision = g.AttemptID, g.Revision
		f.intent = next
	}
	if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.intent, io.NopCloser(bytes.NewReader(f.data))); err != nil {
		t.Fatal(err)
	}
	r := f.intent.Reservation
	epoch := *r.CryptoEpoch
	send := &v2.SendMessageRequest{Context: proto.Clone(r.Context).(*pb.ClientContext), Envelope: &v2.Envelope{ProtocolVersion: 3, CryptoSuite: 1, MessageId: r.MessageID, ConversationId: r.ConversationID, Sender: r.Sender, Recipient: r.Recipient, CryptoEpoch: &epoch, MessageType: 1, Ciphertext: []byte("opaque synthetic HTTP attachment descriptor"), Blob: r.Blob}}
	if _, err := repository.NewPgE2EEDeliveryRepository(f.db).SendV2(t.Context(), f.uid, send); err != nil {
		t.Fatal(err)
	}
	binding, err := boundary.EpochSendBinding(send.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	token, _, _, err := f.auth.Login(t.Context(), "bob@http-blob.invalid", "Synthetic-HTTP-blob-password")
	if err != nil {
		t.Fatal(err)
	}
	return f, token, &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Recipient.GenerationId}, Binding: binding, Blob: proto.Clone(r.Blob).(*v2.BlobRef)}
}

type cancelAfterDownloadFlush struct {
	http.ResponseWriter
	cancel  context.CancelFunc
	flushed chan<- error
}

func (w cancelAfterDownloadFlush) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w cancelAfterDownloadFlush) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.flushed <- err
	if err == nil {
		w.cancel()
	}
	return err
}

func TestBlobDownloadHTTP2HeadCountsFlushedHeadersAsComplete(t *testing.T) {
	f, token, req := acceptedHTTPDownload(t, false)
	count := func(outcome string) float64 {
		t.Helper()
		var value dto.Metric
		if err := metrics.BlobDownloads.WithLabelValues(http.MethodHead, outcome).Write(&value); err != nil {
			t.Fatal(err)
		}
		return value.GetCounter().GetValue()
	}
	beforeComplete, beforeInterrupted := count("complete"), count("interrupted")
	done := make(chan struct{})
	flushed := make(chan error, 1)
	serviceDone := make(chan error, 1)
	h := downloadHTTPMux(&trackedHTTPDownloader{f.upload, serviceDone})
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		// Model the request cancellation that may follow END_STREAM.
		h.ServeHTTP(cancelAfterDownloadFlush{w, cancel, flushed}, r.WithContext(ctx))
	}))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.Client().Do(actualDownloadRequest(t, http.MethodHead, s.URL, req, token, f.credential).WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(f.data)) || len(body) != 0 {
		t.Fatalf("HEAD response: HTTP/%d %d length=%d bytes=%d error=%v", resp.ProtoMajor, resp.StatusCode, resp.ContentLength, len(body), err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if complete, interrupted := count("complete")-beforeComplete, count("interrupted")-beforeInterrupted; complete != 1 || interrupted != 0 {
		select {
		case flushErr := <-flushed:
			t.Logf("flush outcome: %v", flushErr)
		default:
			t.Log("flush was not reached")
		}
		t.Fatalf("HEAD outcomes after successful flush: complete=%v interrupted=%v; service outcome=%v", complete, interrupted, <-serviceDone)
	}
	if f.budget.Active() || f.state(t) != "linked" {
		t.Fatal("HEAD leaked read ownership or altered attachment state")
	}
}
func actualDownloadRequest(t *testing.T, method, url string, req *domain.E2EEBlobReadRequest, token string, credential []byte) *http.Request {
	r := downloadHTTPRequest(t, method, url, req)
	setDownloadHeaders(t, r, req, token, credential)
	return r
}
func TestBlobDownloadHTTPActualPostgresGetHeadAndDescriptorAck(t *testing.T) {
	f, token, req := acceptedHTTPDownload(t, false)
	if err := repository.NewPgE2EEDeliveryRepository(f.db).AcknowledgeV2(t.Context(), 2, &pb.AcknowledgeMessagesRequest{Context: req.Context, Messages: []*pb.Acknowledgement{{MessageId: req.Binding.MessageId, EnvelopeDigest: req.Binding.EnvelopeDigest, Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	flushed := make(chan chan struct{}, 1)
	stop := make(chan struct{})
	h := downloadHTTPMux(&trackedHTTPDownloader{f.upload, done})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(&downloadFlushGate{w, r.Context(), flushed, stop}, r)
	}))
	t.Cleanup(s.Close)
	t.Cleanup(func() { close(stop) }) // Release any gate before joining the server on failure.
	before, _ := f.budget.Usage()
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		resp, err := s.Client().Do(actualDownloadRequest(t, method, s.URL, req, token, f.credential))
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || resp.ContentLength != int64(len(f.data)) {
			t.Fatal(resp.StatusCode, err)
		}
		if method == http.MethodHead {
			if len(body) != 0 {
				t.Fatal("HEAD emitted body")
			}
		} else if sha256.Sum256(body) != sha256.Sum256(f.data) {
			t.Fatal("ciphertext changed")
		}
		// Client EOF/HEAD headers precede server-side finalizer return. Hold a
		// REAL flushed response here to exercise that ordering deterministically.
		var release chan struct{}
		select {
		case release = <-flushed:
		case <-time.After(2 * time.Second):
			t.Fatal("HTTP finalizer did not reach the post-flush gate")
		}
		if !f.budget.Active() {
			t.Fatal("read ownership released before the finalizer returned")
		}
		close(release)
		if err = downloadOutcome(t, done); err != nil {
			t.Fatalf("%s server-side completion: %v", method, err)
		}
	}
	after, writers := f.budget.Usage()
	active, state := f.budget.Active(), f.state(t)
	if after != before || active || state != "linked" {
		t.Fatalf("read changed ownership/charge/state: before=%+v after=%+v writers=%d active=%t state=%s", before, after, writers, active, state)
	}
}

// Test-only scheduling barrier AFTER a real network flush, while the service
// still owns the read lease. No sleeps, fake socket writes or runtime hooks.
type downloadFlushGate struct {
	http.ResponseWriter
	ctx     context.Context
	flushed chan<- chan struct{}
	stop    <-chan struct{}
}

func (w *downloadFlushGate) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *downloadFlushGate) FlushError() error {
	if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
		return err
	}
	release := make(chan struct{})
	select {
	case w.flushed <- release:
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.stop:
		return context.Canceled
	}
	select {
	case <-release:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.stop:
		return context.Canceled
	}
}
func TestBlobDownloadHTTPActualPostgresRejectsUnacceptedAndChangedBinding(t *testing.T) {
	f, token, req := acceptedHTTPDownload(t, false)
	s := httptest.NewServer(downloadHTTPMux(f.upload))
	defer s.Close()
	for _, kind := range []string{"unknown", "digest", "blob", "unaccepted"} {
		t.Run(kind, func(t *testing.T) {
			next := &domain.E2EEBlobReadRequest{Context: proto.Clone(req.Context).(*pb.ClientContext), Binding: proto.Clone(req.Binding).(*pb.EpochSendBinding), Blob: proto.Clone(req.Blob).(*v2.BlobRef)}
			switch kind {
			case "unknown":
				next.Binding.MessageId = uuid.NewString()
			case "digest":
				next.Binding.EnvelopeDigest[0] ^= 1
			case "blob":
				next.Blob.BlobId = uuid.NewString()
			case "unaccepted":
				r := blobHTTPIntent(f.data).Reservation
				r.Context = f.intent.Reservation.Context
				r.Sender = f.intent.Reservation.Sender
				r.Recipient = f.intent.Reservation.Recipient
				r.ConversationID = f.intent.Reservation.ConversationID
				if _, err := f.repo.Reserve(t.Context(), f.uid, r); err != nil {
					t.Fatal(err)
				}
				next.Binding.MessageId = r.MessageID
				next.Blob = r.Blob
			}
			resp, err := s.Client().Do(actualDownloadRequest(t, http.MethodGet, s.URL, next, token, f.credential))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 404 || !bytes.Contains(body, []byte("file_unavailable")) || len(body) > 128 || f.budget.Active() {
				t.Fatal(resp.StatusCode, string(body))
			}
		})
	}
}

type downloadWriteEvent struct{ done chan struct{} }
type downloadObservedWriter struct {
	http.ResponseWriter
	events chan *downloadWriteEvent
}

func (w *downloadObservedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *downloadObservedWriter) Write(p []byte) (int, error) {
	e := &downloadWriteEvent{done: make(chan struct{})}
	select {
	case w.events <- e:
	default:
	}
	defer close(e.done)
	return w.ResponseWriter.Write(p) // observe a REAL network write, never simulate a stall
}

type trackedHTTPDownloader struct {
	E2EEBlobDownloader
	done chan error
}

func (d *trackedHTTPDownloader) Download(c context.Context, t string, k []byte, r *domain.E2EEBlobReadRequest, w io.WriteCloser) error {
	err := d.E2EEBlobDownloader.Download(c, t, k, r, w)
	d.done <- err
	return err
}
func (d *trackedHTTPDownloader) Head(c context.Context, t string, k []byte, r *domain.E2EEBlobReadRequest, w io.WriteCloser) error {
	err := d.E2EEBlobDownloader.Head(c, t, k, r, w)
	d.done <- err
	return err
}
func observedDownloadServer(t *testing.T, f *blobHTTPFixture) (*httptest.Server, <-chan *downloadWriteEvent, <-chan error, <-chan struct{}) {
	t.Helper()
	events := make(chan *downloadWriteEvent, 256)
	done := make(chan error, 1)
	h := downloadHTTPMux(&trackedHTTPDownloader{f.upload, done})
	closed := make(chan struct{})
	var once sync.Once
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(&downloadObservedWriter{w, events}, r) }))
	s.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew {
			if tcp, ok := conn.(*net.TCPConn); ok {
				if err := tcp.SetWriteBuffer(4096); err != nil {
					t.Error(err)
				}
			}
		}
		if state == http.StateClosed {
			once.Do(func() { close(closed) })
		}
	}
	s.Start()
	t.Cleanup(s.Close)
	return s, events, done, closed
}
func blockedHTTPDownload(t *testing.T, s *httptest.Server, events <-chan *downloadWriteEvent, req *domain.E2EEBlobReadRequest, token string, credential []byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", s.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err = tcp.SetReadBuffer(1024); err != nil {
			t.Fatal(err)
		}
	}
	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	r := actualDownloadRequest(t, http.MethodGet, s.URL, req, token, credential)
	if _, err = fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: test\r\n", r.URL.Path); err != nil {
		t.Fatal(err)
	}
	if err = r.Header.Write(conn); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(conn, "\r\n"); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case e := <-events:
			timer := time.NewTimer(20 * time.Millisecond)
			select {
			case <-e.done:
				timer.Stop()
			case <-timer.C:
				return conn
			case <-deadline.C:
				timer.Stop()
				t.Fatal("socket write did not block")
			}
		case <-deadline.C:
			t.Fatal("no blocked socket write observed")
		}
	}
}
func downloadOutcome(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("socket download failed to join")
		return nil
	}
}
func TestBlobDownloadHTTPActualPostgresSlowReaderCancellation(t *testing.T) {
	for _, kind := range []string{"logout", "disconnect", "replacement", "shutdown"} {
		t.Run(kind, func(t *testing.T) {
			f, token, req := acceptedHTTPDownload(t, true)
			if kind == "replacement" {
				// This test bounds locks/cancellation during a blocked socket write,
				// not bcrypt throughput. Keep real password verification and its SQL
				// snapshot CAS, using a cheap hash only in this disposable fixture.
				// Production-cost bcrypt under -race can consume the entire1s bound.
				hash, err := bcrypt.GenerateFromPassword([]byte("Synthetic-HTTP-blob-password"), bcrypt.MinCost)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.db.ExecContext(t.Context(), `UPDATE users SET password_hash=$1 WHERE id=2`, string(hash)); err != nil {
					t.Fatal(err)
				}
			}
			s, events, done, closed := observedDownloadServer(t, f)
			conn := blockedHTTPDownload(t, s, events, req, token, f.credential)
			if !f.budget.Active() {
				t.Fatal("no permit while actual socket write is blocked")
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			switch kind {
			case "logout":
				tx, err := f.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				for _, q := range []string{`SELECT id FROM users ORDER BY id FOR UPDATE`, `SELECT user_id FROM e2ee_devices ORDER BY user_id FOR UPDATE`, `SELECT message_id FROM e2ee_message_claims FOR UPDATE`, `SELECT message_id FROM e2ee_message_blobs FOR UPDATE`, `SELECT attempt_id FROM e2ee_blob_attempts FOR UPDATE`} {
					if _, err = tx.ExecContext(ctx, q); err != nil {
						t.Fatal("SQL lock spans blocked network write", err)
					}
				}
				if err = tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err = f.auth.Logout(ctx, token, false, func(int64, string) {}); err != nil {
					t.Fatal("logout blocked by response", err)
				}
			case "disconnect":
				conn.Close()
			case "shutdown":
				if err := f.upload.Close(ctx); err != nil {
					t.Fatal("shutdown failed to join socket", err)
				}
			case "replacement":
				started := time.Now()
				identity := bytes.Repeat([]byte{53}, 33)
				identity[0] = 5
				raw := bytes.Repeat([]byte{54}, 32)
				hash := service.E2EEDeviceCredentialHash(raw)
				if _, err := f.devices.Replace(ctx, 2, domain.E2EEDeviceRegistration{RequestID: uuid.NewString(), ExpectedGenerationID: req.Context.DeviceGenerationId, RegistrationID: 123, IdentityKey: identity, CredentialHash: hash[:]}, "Synthetic-HTTP-blob-password", raw); err != nil {
					t.Fatalf("replacement while socket is blocked: elapsed=%v context=%v error=%v", time.Since(started), ctx.Err(), err)
				}
			}
			err := downloadOutcome(t, done)
			if err == nil {
				t.Fatal("cancelled download returned success")
			}
			if kind == "logout" && !errors.Is(err, domain.ErrInvalidSession) {
				t.Fatal(err)
			}
			if kind == "replacement" && !errors.Is(err, domain.ErrE2EEDeviceCredential) {
				t.Fatal(err)
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP server did not close the interrupted connection")
			}
			if kind != "disconnect" {
				if tcp, ok := conn.(*net.TCPConn); ok {
					if err = tcp.SetReadBuffer(1 << 20); err != nil {
						t.Fatal(err)
					}
				}
				if err = conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				resp, readErr := http.ReadResponse(bufio.NewReader(conn), nil)
				if readErr == nil {
					body, bodyErr := io.ReadAll(resp.Body)
					resp.Body.Close()
					if bodyErr == nil || len(body) >= len(f.data) {
						t.Fatal("revoked transfer looked complete", len(body), bodyErr)
					}
					var timeout net.Error
					if errors.As(bodyErr, &timeout) && timeout.Timeout() {
						t.Fatal("server did not terminate the interrupted response", bodyErr)
					}
				}
			}
			if f.budget.Active() || f.state(t) != "linked" {
				t.Fatal("cancel leaked permit or mutated accepted state")
			}
			actual, err := os.ReadFile(filepath.Join(f.path, f.intent.AttemptID+".blob"))
			if err != nil || !bytes.Equal(actual, f.data) {
				t.Fatal("cancel deleted accepted file", err)
			}
		})
	}
}

func TestBlobDownloadHTTPActualPostgresHTTP2RevocationKeepsOtherSession(t *testing.T) {
	f, token, req := acceptedHTTPDownload(t, true)
	other, _, _, err := f.auth.Login(t.Context(), "bob@http-blob.invalid", "Synthetic-HTTP-blob-password")
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan *downloadWriteEvent, 256)
	done := make(chan error, 1)
	h := downloadHTTPMux(&trackedHTTPDownloader{f.upload, done})
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(&downloadObservedWriter{w, events}, r) }))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	r := actualDownloadRequest(t, http.MethodGet, s.URL, req, token, f.credential).WithContext(ctx)
	resp, err := s.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatal("HTTP2 not negotiated")
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	blocked := false
	for !blocked {
		select {
		case e := <-events:
			stable := time.NewTimer(20 * time.Millisecond)
			select {
			case <-e.done:
				stable.Stop()
			case <-stable.C:
				blocked = true
			case <-timer.C:
				stable.Stop()
				t.Fatal("HTTP2 flow control did not block")
			}
		case <-timer.C:
			t.Fatal("no blocked HTTP2 Write")
		}
	}
	logout, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err = f.auth.Logout(logout, token, false, func(int64, string) {}); err != nil {
		t.Fatal(err)
	}
	if err = downloadOutcome(t, done); !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err == nil || len(body) >= len(f.data) {
		t.Fatal("revoked HTTP2 body completed", len(body), err)
	}
	resp.Body.Close()
	next, err := s.Client().Do(actualDownloadRequest(t, http.MethodHead, s.URL, req, other, f.credential))
	if err != nil {
		t.Fatal(err)
	}
	defer next.Body.Close()
	if next.ProtoMajor != 2 || next.StatusCode != 200 || next.ContentLength != int64(len(f.data)) {
		t.Fatal("one revoked stream broke another session", next.StatusCode)
	}
	if body, err = io.ReadAll(next.Body); err != nil || len(body) != 0 {
		t.Fatal("HEAD response was not complete and bodyless", len(body), err)
	}
	// Go sends END_STREAM with HTTP/2 HEAD headers and may cancel the request
	// context before the coordinator unwinds. The response above is complete;
	// join ownership cleanup without requiring that context to remain live.
	if err = downloadOutcome(t, done); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal("HEAD server-side completion", err)
	}
	if f.budget.Active() || f.state(t) != "linked" {
		t.Fatal("HTTP2 leaked read ownership")
	}
}
