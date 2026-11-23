package repository

import (
	"context"
	"errors"
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
)

func TestE2EEInventoryScopeCountsPostgres(t *testing.T) {
	db, devices, r, req := publicationDB(t)
	get := &pb.GetPreKeyInventoryRequest{Context: req.Context}
	v, err := r.Inventory(t.Context(), 1, get)
	if err != nil || v.PublicationRevision != 0 || v.PublicMaterialReady || v.ActiveSignedId != nil || v.ActiveLastResortId != nil || v.ActiveSignedCreatedAtUnixMs != nil {
		t.Fatal("registry-only inventory", v, err)
	}
	if _, err := r.Publish(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	v, err = r.Inventory(t.Context(), 1, get)
	if err != nil || v.OwnDevice.UserId != "11111111-1111-4111-8111-111111111111" || v.OwnDevice.GenerationId != req.Context.DeviceGenerationId ||
		v.PublicationRevision != 1 || !v.PublicMaterialReady || v.AvailableEc != 1 || v.AvailablePq != 1 || v.RetainedEc != 1 || v.RetainedPq != 2 || v.RetainedSigned != 1 || v.PublicationBatches != 1 ||
		v.GetActiveSignedId() != req.SignedPreKey.KeyId || v.GetActiveSignedCreatedAtUnixMs() != req.SignedPreKey.CreatedAtUnixMs || v.GetActiveLastResortId() != req.KyberPreKeys[1].KeyId ||
		v.MaxRetainedEc != 4096 || v.MaxRetainedPq != 4096 || v.MaxRetainedSigned != 128 || v.MaxPublicationBatches != 8192 {
		t.Fatal("published inventory", v, err)
	}
	if _, err := r.Inventory(t.Context(), 2, get); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("peer inventory leaked", err)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE e2ee_ec_prekeys SET used=TRUE; UPDATE e2ee_kyber_prekeys SET used=TRUE WHERE NOT is_last_resort`); err != nil {
		t.Fatal(err)
	}
	v, err = r.Inventory(t.Context(), 1, get)
	if err != nil || v.AvailableEc != 0 || v.AvailablePq != 0 || v.RetainedEc != 1 || v.RetainedPq != 2 || v.PublicationRevision != 1 {
		t.Fatal("used keys lost quota or LR counted available", v, err)
	}
	// ID zero is present, rather than confused with absent metadata.
	rotated := nextPublication(req, 1)
	rotated.OneTimePreKeys = nil
	rotated.SignedPreKey.KeyId = 0
	rotated.KyberPreKeys = rotated.KyberPreKeys[1:]
	rotated.KyberPreKeys[0].KeyId = 0
	if _, err := r.Publish(t.Context(), 1, rotated); err != nil {
		t.Fatal(err)
	}
	v, err = r.Inventory(t.Context(), 1, get)
	if err != nil || v.ActiveSignedId == nil || *v.ActiveSignedId != 0 || v.ActiveLastResortId == nil || *v.ActiveLastResortId != 0 || v.RetainedPq != 3 || v.AvailablePq != 0 || v.RetainedSigned != 2 || v.PublicationRevision != 2 {
		t.Fatal("rotation/retained LR inventory", v, err)
	}
	replacement := registrationMaterial()
	replacement.ExpectedGenerationID = req.Context.DeviceGenerationId
	if _, err := devices.Replace(t.Context(), 1, replacement, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Inventory(t.Context(), 1, get); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired generation inventory", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Inventory(canceled, 1, get); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled inventory", err)
	}
}

func TestE2EEInventoryConsistentSnapshotPostgres(t *testing.T) {
	_, _, r, initial := publicationDB(t)
	if _, err := r.Publish(t.Context(), 1, initial); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := uint32(1); i <= 12; i++ {
			req := nextPublication(initial, uint64(i))
			req.SignedPreKey = nil
			req.OneTimePreKeys[0].KeyId = 100 + i
			req.KyberPreKeys = req.KyberPreKeys[:1]
			req.KyberPreKeys[0].KeyId = 100 + i
			if _, err := r.Publish(t.Context(), 1, req); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	// Bounded concurrent reads, not a load test. Every publication adds exactly
	// one EC/PQ/batch; a multi-statement READ COMMITTED implementation can tear.
	for range 32 {
		v, err := r.Inventory(t.Context(), 1, &pb.GetPreKeyInventoryRequest{Context: initial.Context})
		if err != nil || uint64(v.RetainedEc) != v.PublicationRevision || v.RetainedPq != v.RetainedEc+1 || v.AvailableEc != v.RetainedEc || v.AvailablePq != v.RetainedEc || v.PublicationBatches != v.RetainedEc {
			t.Error("torn inventory", v, err)
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
