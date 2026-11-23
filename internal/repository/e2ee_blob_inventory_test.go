//go:build linux

package repository

import (
	"errors"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/domain"
)

func TestBlobInventoryBoundCursorAndNamespacePostgres(t *testing.T) {
	f, r, files, _ := blobWriterFixture(t)
	second := cloneBlobReservation(r.Attempt.Reservation)
	second.MessageID = uuid.NewString()
	second.OperationID = uuid.NewString()
	second.Blob.BlobId = uuid.NewString()
	g := f.reserve(t, 1, second)
	ids := []string{r.Attempt.AttemptID, g.AttemptID}
	sort.Strings(ids)
	var after string
	for i := range 2 {
		page, err := f.blobs.Inventory(t.Context(), files.StorageID(), after, 1)
		if err != nil || len(page) != 1 || page[0].Cleanup.AttemptID != ids[i] || page[0].Cleanup.StorageID != files.StorageID() || page[0].State != "reserved" || page[0].Bytes != 64 || page[0].SHA256 != [32]byte(second.Blob.Sha256) || page[0].Accepted || page[0].Expired {
			t.Fatal(page, err)
		}
		after = page[0].Cleanup.AttemptID
		if page[0].SenderUserID != 1 {
			t.Fatal("inventory lost immutable sender", page[0].SenderUserID)
		}
	}
	if page, err := f.blobs.Inventory(t.Context(), files.StorageID(), after, 1); err != nil || len(page) != 0 {
		t.Fatal(page, err)
	}
	if _, err := f.blobs.Inventory(t.Context(), uuid.NewString(), "", 1); !errors.Is(err, domain.ErrE2EEBlobStorage) {
		t.Fatal("foreign namespace accepted", err)
	}
	for _, bad := range []struct {
		after string
		limit int
	}{{"../invalid", 1}, {"", 0}, {"", 129}} {
		if _, err := f.blobs.Inventory(t.Context(), files.StorageID(), bad.after, bad.limit); err == nil {
			t.Fatal("invalid cursor/limit accepted", bad)
		}
	}
}
