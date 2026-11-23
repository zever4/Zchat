package domain

import "errors"

var (
	ErrE2EEKeyConflict        = errors.New("E2EE key id reused or retired key reactivated")
	ErrE2EEPublicationChanged = errors.New("E2EE publication revision changed")
	ErrE2EEKeyCapacity        = errors.New("E2EE retained public-key capacity exhausted")
)

// Conservative retained-state bounds, not a GC policy. Used/retired keys and
// receipts stay intact until acquisition/private-key retention is designed.
const (
	MaxE2EERetainedPreKeys    = 4096 // Each of EC and Kyber, per generation.
	MaxE2EERetainedSignedKeys = 128
	MaxE2EEPublicationBatches = 8192
)

type E2EEPublication struct {
	BatchID  string
	Digest   [32]byte
	Revision uint64
	Replayed bool
}
