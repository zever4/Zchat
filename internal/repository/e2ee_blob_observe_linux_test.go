//go:build linux

package repository

import (
	"github.com/zever4/Zchat/internal/domain"
	"testing"
)

func TestE2EEBlobObserveWriterReadyAndReclaimedPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	check := func(state string) {
		t.Helper()
		g, err := f.blobs.Observe(t.Context(), 1, r.Attempt.Reservation)
		if err != nil || g.State != state || g.AttemptID != r.Attempt.AttemptID || g.Revision != r.Attempt.Revision {
			t.Fatal(state, g, err)
		}
	}
	check("reserved")
	if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	check("writing")
	w := writeBlobProof(t, files, r, data)
	ready, err := f.blobs.CompleteWrite(t.Context(), 1, r, w)
	if err != nil {
		t.Fatal(err)
	}
	check("ready")
	f.age(t, &ready.Grant)
	req := cleanupRequest(r, domain.E2EEBlobCleanupExpired)
	if _, err = f.blobs.ClaimCleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	check("deleting")
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); err != nil {
		t.Fatal(err)
	}
	check("reclaimed")
	f.quota(t, 0, 1, 1)
}
