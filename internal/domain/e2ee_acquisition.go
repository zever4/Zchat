package domain

import (
	"errors"
	"time"
)

var (
	ErrE2EEConversationClosed  = errors.New("E2EE conversation is closed")
	ErrE2EEAcquisitionExpired  = errors.New("E2EE bundle acquisition expired")
	ErrE2EEAcquisitionCapacity = errors.New("E2EE acquisition capacity exhausted")
	ErrE2EEAcquisitionState    = errors.New("invalid stored E2EE acquisition")
)

// A lease bounds new bootstrap use/retrieval, not message delivery or private-
// key lifetime. Private retention must also cover pending delivery/grace in D4/5.
const (
	E2EEAcquisitionLease                = 24 * time.Hour
	MaxE2EEAcquisitionsPerGeneration    = 4096
	MaxE2EELiveAcquisitionsPerDirection = 32
)
