package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/domain"
)

func TestE2EEBlobObserveMissingCreatesNoMetadataPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	for range 2 {
		if g, err := f.blobs.Observe(t.Context(), 1, r); g != nil || !errors.Is(err, domain.ErrE2EEBlobOperationNotFound) {
			t.Fatal(g, err)
		}
	}
	for _, table := range []string{"e2ee_message_claims", "e2ee_blob_attempts", "e2ee_blob_heads", "e2ee_blob_account_quota", "e2ee_send_receipts"} {
		registryCount(t, f.f.db, table, 0)
	}
	f.quota(t, 0, 0, 0)
	g := f.reserve(t, 1, r)
	missing := cloneBlobReservation(r)
	missing.OperationID = uuid.NewString()
	if got, err := f.blobs.Observe(t.Context(), 1, missing); got != nil || !errors.Is(err, domain.ErrE2EEBlobOperationNotFound) {
		t.Fatal(got, err)
	}
	observed, err := f.blobs.Observe(t.Context(), 1, r)
	if err != nil || observed.AttemptID != g.AttemptID || !observed.ExpiresAt.Equal(g.ExpiresAt) || observed.Expired {
		t.Fatal(observed, err)
	}
	f.quota(t, 64, 1, 1)
	registryCount(t, f.f.db, "e2ee_blob_attempts", 1)
	// Absence for another message does not prove a globally reusable operation.
	other := f.request(1)
	other.OperationID = r.OperationID
	if got, err := f.blobs.Observe(t.Context(), 1, other); got != nil || !errors.Is(err, domain.ErrE2EEBlobOperationNotFound) {
		t.Fatal(got, err)
	}
	if _, err := f.blobs.Reserve(t.Context(), 1, other); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("observation authorized operation reuse", err)
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 1)
	f.quota(t, 64, 1, 1)
}

func TestE2EEBlobObserveRetainsSupersededOperationAndDeadlinePostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	first := f.reserve(t, 1, r)
	f.age(t, first)
	expired, err := f.blobs.Observe(t.Context(), 1, r)
	if err != nil || !expired.Expired || !expired.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatal(expired, err)
	}
	renew := cloneBlobReservation(r)
	renew.OperationID = uuid.NewString()
	renew.ExpectedRevision = first.Revision
	renew.ExpectedAttemptID = first.AttemptID
	next := f.reserve(t, 1, renew)
	old, err := f.blobs.Observe(t.Context(), 1, r)
	if err != nil || old.AttemptID != first.AttemptID || old.Revision != 1 || old.State != "deleting" || !old.Expired || !old.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatal(old, err)
	}
	current, err := f.blobs.Observe(t.Context(), 1, renew)
	if err != nil || current.AttemptID != next.AttemptID || current.Revision != 2 || current.Expired || !current.ExpiresAt.Equal(next.ExpiresAt) {
		t.Fatal(current, err)
	}
	wrong := cloneBlobReservation(renew)
	wrong.ExpectedAttemptID = uuid.NewString()
	if _, err = f.blobs.Observe(t.Context(), 1, wrong); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("CAS replay mismatch", err)
	}
	if _, err = f.blobs.Reserve(t.Context(), 1, r); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("observation reactivated old operation", err)
	}
	f.quota(t, 128, 1, 2)
	registryCount(t, f.f.db, "e2ee_blob_attempts", 2)
	registryCount(t, f.f.db, "e2ee_send_receipts", 0)
}

func TestE2EEBlobObserveAuthorizationAndBindingPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	f.reserve(t, 1, r)
	for _, uid := range []int64{0, 2, 99} {
		if g, err := f.blobs.Observe(t.Context(), uid, r); g != nil || err == nil || errors.Is(err, domain.ErrE2EEBlobOperationNotFound) {
			t.Fatal("foreign observation", uid, g, err)
		}
	}
	for _, change := range []func(*domain.E2EEBlobReservationRequest){
		func(r *domain.E2EEBlobReservationRequest) { r.Blob.BlobId = uuid.NewString() },
		func(r *domain.E2EEBlobReservationRequest) { r.Blob.Sha256[0]++ },
		func(r *domain.E2EEBlobReservationRequest) { r.Blob.CiphertextBytes++ },
	} {
		bad := cloneBlobReservation(r)
		change(bad)
		if _, err := f.blobs.Observe(t.Context(), 1, bad); !errors.Is(err, domain.ErrE2EEMessageConflict) {
			t.Fatal("changed binding", err)
		}
	}
	f.transition(t)
	if _, err := f.blobs.Observe(t.Context(), 1, r); !errors.Is(err, domain.ErrE2EEEpochInactive) {
		t.Fatal("sealed epoch observation", err)
	}
	newReq := f.request(1)
	*newReq.CryptoEpoch = 1
	f.f.replace(t, 2, f.gen(2))
	if _, err := f.blobs.Observe(t.Context(), 1, newReq); !errors.Is(err, domain.ErrE2EEConversationClosed) {
		t.Fatal("closed missing proof", err)
	}
	f.quota(t, 64, 1, 1)
}

func TestE2EEBlobObserveLockCancellationIsNotMissingPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	f.reserve(t, 1, r)
	tx, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(t.Context(), `SELECT message_id FROM e2ee_message_claims WHERE message_id=$1 FOR UPDATE`, r.MessageID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if g, err := f.blobs.Observe(ctx, 1, r); g != nil || err == nil || errors.Is(err, domain.ErrE2EEBlobOperationNotFound) {
		t.Fatal("canceled lock became observation", g, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.Observe(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	f.quota(t, 64, 1, 1)
}
