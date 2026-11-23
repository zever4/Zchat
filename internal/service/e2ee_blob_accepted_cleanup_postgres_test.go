//go:build linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"google.golang.org/protobuf/proto"
)

func acceptedServiceRequest(f *postgresBlobUpload) *domain.E2EEAcceptedBlobCleanupRequest {
	return &domain.E2EEAcceptedBlobCleanupRequest{StorageID: f.upload.files.StorageID(), MessageID: f.requests.Reservation.MessageID, AttemptID: f.requests.AttemptID, Revision: f.requests.Revision}
}
func acknowledgeAcceptedFile(t *testing.T, f *postgresBlobUpload, read *domain.E2EEBlobReadRequest) *v2.FileAcknowledgements {
	t.Helper()
	ack, err := f.repo.AcknowledgeFiles(t.Context(), 2, &v2.AcknowledgeFilesRequest{Context: read.Context, Files: []*v2.FileAcknowledgement{{Binding: read.Binding, Blob: read.Blob}}})
	if err != nil {
		t.Fatal(err)
	}
	return ack
}

func TestBlobAcceptedCleanupReceiptAndPhysicalReplayPostgres(t *testing.T) {
	f, _, read := acceptedDownloadPostgres(t)
	req := acceptedServiceRequest(f)
	before, _ := f.budget.Usage()
	delivery := repository.NewPgE2EEDeliveryRepository(f.db)
	var wire []byte
	if err := f.db.QueryRowContext(t.Context(), `SELECT envelope FROM e2ee_message_inbox WHERE message_id=$1`, req.MessageID).Scan(&wire); err != nil {
		t.Fatal(err)
	}
	envelope := &v2.Envelope{}
	if err := proto.Unmarshal(wire, envelope); err != nil {
		t.Fatal(err)
	}
	send := &v2.SendMessageRequest{Context: f.requests.Reservation.Context, Envelope: envelope}
	receipt, err := delivery.SendV2(t.Context(), f.uid, send)
	if err != nil {
		t.Fatal(err)
	}
	denied := func() {
		t.Helper()
		if got, err := f.upload.CleanupAccepted(t.Context(), req); got != nil || !errors.Is(err, domain.ErrE2EEBlobCleanup) {
			t.Fatal("live accepted file admitted", got, err)
		}
		f.assertState(t, "linked")
		if u, _ := f.budget.Usage(); u != before {
			t.Fatal("denied deletion freed physical bytes", u, before)
		}
	}
	denied()
	if err = delivery.AcknowledgeV2(t.Context(), 2, &pb.AcknowledgeMessagesRequest{Context: read.Context, Messages: []*pb.Acknowledgement{{MessageId: req.MessageID, EnvelopeDigest: read.Binding.EnvelopeDigest, Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}); err != nil {
		t.Fatal(err)
	}
	denied() // Saving the descriptor is not saving the actual file.
	ack := acknowledgeAcceptedFile(t, f, read)
	got, err := f.upload.CleanupAccepted(t.Context(), req)
	if err != nil || got == nil || got.Reason != domain.E2EEBlobReclaimFileAcknowledged || got.ReclaimedAt == nil {
		t.Fatal(got, err)
	}
	assertReclaimed(t, f)
	after, _ := f.budget.Usage()
	if after.Bytes >= before.Bytes || after.Entries >= before.Entries || f.budget.Active() {
		t.Fatal("physical charge not released", before, after)
	}
	for range 3 {
		again, err := f.upload.CleanupAccepted(t.Context(), req)
		if err != nil || again.ReclaimedAt == nil || !again.ReclaimedAt.Equal(*got.ReclaimedAt) || again.Reason != got.Reason {
			t.Fatal("replay changed immutable reclaim", again, err)
		}
		if u, _ := f.budget.Usage(); u != after {
			t.Fatal("replay double released physical charge", u, after)
		}
	}
	saved, err := delivery.SendV2(t.Context(), f.uid, send)
	if err != nil || !proto.Equal(saved, receipt) {
		t.Fatal("exact Send replay changed receipt", saved, err)
	}
	status, err := f.repo.FileStatuses(t.Context(), f.uid, &v2.GetFileStatusesRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: send.Context.DeviceGenerationId}, Files: []*v2.FileStatusReference{{Binding: read.Binding, Blob: read.Blob}}})
	if err != nil || len(status.Files) != 1 || !proto.Equal(status.Files[0].Receipt, receipt) || status.Files[0].GetAcknowledgedAtUnixMs() != ack.Files[0].AcknowledgedAtUnixMs {
		t.Fatal("GC changed file status", status, err)
	}
	for table, want := range map[string]int{"e2ee_send_receipts": 1, "e2ee_message_blobs": 1, "e2ee_file_acknowledgements": 1, "e2ee_message_inbox": 0} {
		var count int
		if err := f.db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != want {
			t.Fatal("GC removed fact/resurrected inbox", table, count, err)
		}
	}
}

func TestBlobAcceptedCleanupReaderJoinWithoutSQLBarrierPostgres(t *testing.T) {
	f, token, read := acceptedDownloadPostgres(t)
	f.upload.config.RecheckInterval = f.upload.config.Timeout
	sink := heldSink(false)
	defer sink.unblock()
	download := make(chan error, 1)
	go func() { download <- f.upload.Download(t.Context(), token, f.credential, read, sink) }()
	awaitDownload(t, sink.entered)
	before, _ := f.budget.Usage()
	acknowledgeAcceptedFile(t, f, read)
	req := acceptedServiceRequest(f)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cleaned := make(chan error, 1)
	go func() { _, err := f.upload.CleanupAccepted(ctx, req); cleaned <- err }()
	awaitDownload(t, sink.closed)
	f.assertState(t, "deleting")
	if u, _ := f.budget.Usage(); u != before || !f.budget.Active() {
		t.Fatal("freed physical charge before join", u, before)
	}
	if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); err != nil {
		t.Fatal("removed blocked reader's file", err)
	}
	lockCtx, unlock := context.WithTimeout(t.Context(), time.Second)
	defer unlock()
	tx, err := f.db.BeginTx(lockCtx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, query := range []string{`SELECT id FROM users ORDER BY id FOR UPDATE`, `SELECT user_id FROM e2ee_devices ORDER BY user_id FOR UPDATE`, `SELECT conversation_id FROM e2ee_conversations FOR UPDATE`, `SELECT message_id FROM e2ee_message_claims FOR UPDATE`, `SELECT message_id FROM e2ee_send_receipts FOR UPDATE`, `SELECT message_id FROM e2ee_message_blobs FOR UPDATE`, `SELECT attempt_id FROM e2ee_blob_attempts FOR UPDATE`} {
		if _, err = tx.ExecContext(lockCtx, query); err != nil {
			t.Fatal("cleanup holds SQL locks across reader join", err)
		}
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = f.upload.Head(t.Context(), token, f.credential, read, &downloadSink{}); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal("new HEAD escaped committed Deleting", err)
	}
	cancel()
	if err = awaitDownloadResult(t, cleaned); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f.assertState(t, "deleting")
	sink.unblock()
	if err = awaitDownloadResult(t, download); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal(err)
	}
	if _, err = f.upload.CleanupAccepted(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	assertReclaimed(t, f)
}

type acceptedCleanupOutcomeModel struct {
	E2EEBlobWriterRepository
	E2EEAcceptedBlobReclaimer
	claimLost, completeLost bool
}

func (m *acceptedCleanupOutcomeModel) ClaimAcceptedCleanup(ctx context.Context, r *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	x, err := m.E2EEAcceptedBlobReclaimer.ClaimAcceptedCleanup(ctx, r)
	if err == nil && m.claimLost {
		return nil, errBlobSynthetic
	}
	return x, err
}
func (m *acceptedCleanupOutcomeModel) CompleteAcceptedReclaim(ctx context.Context, r *domain.E2EEAcceptedBlobCleanupRequest, p *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	x, err := m.E2EEAcceptedBlobReclaimer.CompleteAcceptedReclaim(ctx, r, p)
	if err == nil && m.completeLost {
		return nil, errBlobSynthetic
	}
	return x, err
}

func TestBlobAcceptedCleanupUnknownCommitsAndFailedUnlinkPostgres(t *testing.T) {
	f, _, read := acceptedDownloadPostgres(t)
	before, _ := f.budget.Usage()
	acknowledgeAcceptedFile(t, f, read)
	req := acceptedServiceRequest(f)
	m := &acceptedCleanupOutcomeModel{E2EEBlobWriterRepository: f.repo, E2EEAcceptedBlobReclaimer: f.repo, claimLost: true}
	f.upload.repo = m
	if got, err := f.upload.CleanupAccepted(t.Context(), req); got != nil || !errors.Is(err, errBlobSynthetic) {
		t.Fatal("unknown claim granted unlink", got, err)
	}
	f.assertState(t, "deleting")
	if u, _ := f.budget.Usage(); u != before {
		t.Fatal("unknown claim released physical bytes", u)
	}
	if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); err != nil {
		t.Fatal("unknown claim unlinked file", err)
	}
	m.claimLost = false
	// A non-regular leaf deterministically fails unlink, including under root.
	// RemoveVerified may remove the other component, but grants NO proof until
	// both components are absent and directory Sync succeeds.
	part := filepath.Join(f.path, req.AttemptID+".part")
	if err := os.Mkdir(part, 0700); err != nil {
		t.Fatal(err)
	}
	if got, err := f.upload.CleanupAccepted(t.Context(), req); got != nil || !errors.Is(err, blobstore.ErrRoot) {
		t.Fatal("failed unlink finalized", got, err)
	}
	f.assertState(t, "deleting")
	if u, _ := f.budget.Usage(); u != before {
		t.Fatal("partial unlink released bytes without proof", u)
	}
	if err := os.Remove(part); err != nil {
		t.Fatal(err)
	}
	m.completeLost = true
	if got, err := f.upload.CleanupAccepted(t.Context(), req); got != nil || !errors.Is(err, errBlobSynthetic) {
		t.Fatal("unknown final COMMIT returned a receipt", got, err)
	}
	assertReclaimed(t, f)
	after, _ := f.budget.Usage()
	if after.Bytes >= before.Bytes || f.budget.Active() {
		t.Fatal("known unlink retained physical charge", before, after)
	}
	m.completeLost = false
	if _, err := f.upload.CleanupAccepted(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if u, _ := f.budget.Usage(); u != after {
		t.Fatal("retry released physical bytes twice", u, after)
	}
}

func TestBlobAcceptedCleanupStartupResumesOnlyAdmittedPostgres(t *testing.T) {
	for _, phase := range []string{"admitted", "unlinked", "reclaimed", "linked acknowledged", "linked missing", "reclaimed artifact"} {
		t.Run(phase, func(t *testing.T) {
			f, _, read := acceptedDownloadPostgres(t)
			req := acceptedServiceRequest(f)
			acknowledgeAcceptedFile(t, f, read)
			if phase == "admitted" || phase == "unlinked" {
				if _, err := f.repo.ClaimAcceptedCleanup(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "unlinked" || phase == "linked missing" {
				proof, err := f.upload.files.RemoveVerified(t.Context(), req.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				proof.Close() // Simulate stopping before logical finalization.
			}
			if phase == "reclaimed" || phase == "reclaimed artifact" {
				if _, err := f.upload.CleanupAccepted(t.Context(), req); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "reclaimed artifact" {
				if err := os.WriteFile(filepath.Join(f.path, req.AttemptID+".blob"), f.data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			s := restartBlobOwner(t, f)
			got, err := s.Reconcile(t.Context(), startupTestConfig())
			if phase == "linked missing" || phase == "reclaimed artifact" {
				if !errors.Is(err, domain.ErrE2EEBlobMismatch) || s.Ready() {
					t.Fatal("corrupt inventory silently repaired", phase, got, err)
				}
				if phase == "reclaimed artifact" {
					if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); err != nil {
						t.Fatal("unexpected artifact erased", err)
					}
				}
				return
			}
			if err != nil || !s.Ready() {
				t.Fatal(got, err)
			}
			wantReclaimed := 0
			if phase == "admitted" || phase == "unlinked" {
				wantReclaimed = 1
			}
			if got.Reclaimed != wantReclaimed {
				t.Fatal("wrong resumed count", phase, got)
			}
			budget := testPhysicalBudget(t)
			if err = s.AttachBudget(t.Context(), budget, startupTestConfig()); err != nil {
				t.Fatal("post-recovery accounting failed", err)
			}
			u, _ := budget.Usage()
			if phase == "linked acknowledged" {
				if got.Verified != 1 || u.Bytes == 0 {
					t.Fatal("unadmitted accepted file was reclaimed", got, u)
				}
				f.assertState(t, "linked")
			} else {
				assertReclaimed(t, f)
				census, err := s.files.PhysicalCensus(t.Context(), 128)
				if err != nil || len(census.Artifacts) != 0 || u != census.Metadata {
					t.Fatal("restart retained deleted file charge", u, census, err)
				}
			}
			for _, table := range []string{"e2ee_send_receipts", "e2ee_message_blobs", "e2ee_file_acknowledgements"} {
				var count int
				if err := f.db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
					t.Fatal("startup discarded accepted history", table, count, err)
				}
			}
		})
	}
}
