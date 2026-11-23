//go:build linux

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

func startupTestConfig() E2EEBlobStartupConfig {
	return E2EEBlobStartupConfig{MaxAttempts: 64, MaxArtifacts: 128, MaxVerifyBytes: 1 << 20, Timeout: 3 * time.Second}
}
func restartBlobOwner(t *testing.T, f *postgresBlobUpload) *E2EEBlobUploadService {
	t.Helper()
	if err := f.upload.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	files, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewE2EEBlobUploadService(f.repo, files, f.auth, NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(f.db), f.auth), E2EEBlobUploadConfig{MaxUploads: 1, Timeout: 2 * time.Second, RecheckInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if s.Ready() {
		t.Fatal("constructor bypassed startup")
	}
	if got, err := s.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); got != nil || !errors.Is(err, domain.ErrE2EEBlobNotReady) {
		t.Fatal("upload before startup", got, err)
	}
	return s
}
func TestBlobStartupReclaimsIncompleteWritersPostgres(t *testing.T) {
	for _, kind := range []string{"missing", "partial", "published", "publication links"} {
		t.Run(kind, func(t *testing.T) {
			f := blobUploadPostgres(t)
			r := &domain.E2EEBlobWriteRequest{Attempt: f.requests, WriterID: uuid.NewString(), StorageID: f.upload.files.StorageID()}
			if _, err := f.repo.BeginWrite(t.Context(), f.uid, r); err != nil {
				t.Fatal(err)
			}
			if kind == "partial" {
				if err := os.WriteFile(filepath.Join(f.path, f.requests.AttemptID+".part"), f.data[:7], 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "published" {
				if _, err := f.upload.files.Write(t.Context(), blobstore.Attempt{ID: f.requests.AttemptID, Bytes: 64, SHA256: [32]byte(f.requests.Reservation.Blob.Sha256)}, io.NopCloser(bytes.NewReader(f.data))); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "publication links" {
				part := filepath.Join(f.path, f.requests.AttemptID+".part")
				if err := os.WriteFile(part, f.data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(part, filepath.Join(f.path, f.requests.AttemptID+".blob")); err != nil {
					t.Fatal(err)
				}
			}
			s := restartBlobOwner(t, f)
			result, err := s.Reconcile(t.Context(), startupTestConfig())
			if err != nil || !s.Ready() || result.Reclaimed != 1 {
				t.Fatal(result, err, s.Ready())
			}
			assertReclaimed(t, f)
		})
	}
}

func TestBlobStartupContinuesDeletingAndReplaysReclaimedPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); err != nil {
		t.Fatal(err)
	}
	expireServiceAttempt(t, f)
	r := serviceCleanupRequest(f, domain.E2EEBlobCleanupExpired)
	r.StorageID = f.upload.files.StorageID()
	if _, err := f.repo.ClaimCleanup(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	s := restartBlobOwner(t, f)
	got, err := s.Reconcile(t.Context(), startupTestConfig())
	if err != nil || got.Reclaimed != 1 || !s.Ready() {
		t.Fatal(got, err)
	}
	assertReclaimed(t, f)
	if err = s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s = restartBlobOwner(t, f)
	got, err = s.Reconcile(t.Context(), startupTestConfig())
	if err != nil || got.Reclaimed != 0 || !s.Ready() {
		t.Fatal(got, err)
	}
	assertReclaimed(t, f)
}

func TestBlobStartupExpiresUnwrittenReservationPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	expireServiceAttempt(t, f)
	s := restartBlobOwner(t, f)
	got, err := s.Reconcile(t.Context(), startupTestConfig())
	if err != nil || got.Reclaimed != 1 || got.Artifacts != 0 || !s.Ready() {
		t.Fatal(got, err)
	}
	assertReclaimed(t, f)
}
func TestBlobStartupPreservesReadyLinkedAndExpiresStagingPostgres(t *testing.T) {
	for _, kind := range []string{"ready", "linked", "expired ready"} {
		t.Run(kind, func(t *testing.T) {
			f := blobUploadPostgres(t)
			if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); err != nil {
				t.Fatal(err)
			}
			if kind == "linked" {
				// A genuine repository acceptance with synthetic Signal bytes;
				// the production service's Send3 guard remains enabled.
				b := f.requests.Reservation
				_, err := repository.NewPgE2EEDeliveryRepository(f.db).SendV2(t.Context(), f.uid, &v2.SendMessageRequest{
					Context: b.Context, Envelope: &v2.Envelope{ProtocolVersion: 3, CryptoSuite: 1,
						MessageId: b.MessageID, ConversationId: b.ConversationID, Sender: b.Sender, Recipient: b.Recipient,
						CryptoEpoch: b.CryptoEpoch, MessageType: 1, Ciphertext: []byte("synthetic Signal attachment descriptor"), Blob: b.Blob}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == "expired ready" {
				expireServiceAttempt(t, f)
			}
			s := restartBlobOwner(t, f)
			r, err := s.Reconcile(t.Context(), startupTestConfig())
			if err != nil || !s.Ready() || r.Verified != 1 || r.VerifiedBytes != 64 {
				t.Fatal(r, err)
			}
			if kind == "expired ready" {
				if r.Reclaimed != 1 {
					t.Fatal(r)
				}
				assertReclaimed(t, f)
			} else {
				state := "ready"
				if kind == "linked" {
					state = "linked"
				}
				f.assertState(t, state)
				got, err := os.ReadFile(filepath.Join(f.path, f.requests.AttemptID+".blob"))
				if err != nil || !bytes.Equal(got, f.data) {
					t.Fatal("changed persisted ciphertext", err)
				}
			}
		})
	}
}
func TestBlobStartupMismatchNeverDeletesOrPromotesPostgres(t *testing.T) {
	for _, kind := range []string{"orphan", "missing ready", "corrupt ready", "verify budget"} {
		t.Run(kind, func(t *testing.T) {
			f := blobUploadPostgres(t)
			if kind != "orphan" {
				if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); err != nil {
					t.Fatal(err)
				}
			}
			name := filepath.Join(f.path, f.requests.AttemptID+".blob")
			switch kind {
			case "orphan":
				if _, err := f.repo.BeginWrite(t.Context(), f.uid, &domain.E2EEBlobWriteRequest{Attempt: f.requests, WriterID: uuid.NewString(), StorageID: f.upload.files.StorageID()}); err != nil {
					t.Fatal(err)
				}
				name = filepath.Join(f.path, uuid.NewString()+".blob")
				if err := os.WriteFile(name, f.data, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing ready":
				if err := os.Remove(name); err != nil {
					t.Fatal(err)
				}
			case "corrupt ready":
				if err := os.WriteFile(name, bytes.Repeat([]byte{99}, 64), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s := restartBlobOwner(t, f)
			cfg := startupTestConfig()
			if kind == "verify budget" {
				cfg.MaxVerifyBytes = 63
			}
			if got, err := s.Reconcile(t.Context(), cfg); got != nil || err == nil || s.Ready() {
				t.Fatal("startup accepted mismatch", got, err, s.Ready())
			}
			state := "ready"
			if kind == "orphan" {
				state = "writing"
			}
			f.assertState(t, state)
			if kind != "missing ready" {
				if _, err := os.Stat(name); err != nil {
					t.Fatal("deleted unrecognized/mismatched data", err)
				}
			}
			if kind == "corrupt ready" {
				// Explicit fixture repair by an operator; production never does this.
				if err := os.WriteFile(name, f.data, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Reconcile(t.Context(), cfg); err != nil || !s.Ready() {
					t.Fatal("retry after explicit repair failed", err)
				}
			}
		})
	}
}
