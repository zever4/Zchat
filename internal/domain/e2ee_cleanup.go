package domain

import "time"

type E2EECleanupLane uint8

const (
	E2EECleanupExpired E2EECleanupLane = iota
	E2EECleanupReplaced
	MaxE2EECleanupBatch = 32
)

// Scheduling only, not a delivery cursor. Wrap/restart rechecks skipped rows.
// At is expiry in the first lane and retirement time in the second lane.
type E2EECleanupPosition struct {
	At         time.Time
	Generation string
	Sequence   int64
}

type E2EECleanupResult struct {
	After                  *E2EECleanupPosition
	Visited, Removed, Busy int
}
