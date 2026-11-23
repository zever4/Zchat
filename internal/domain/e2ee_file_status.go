package domain

import "errors"

// Unknown, unaccepted, foreign owner and mismatched binding/ref are opaque.
// No error constitutes a receipt, non-Send/absence or upload renewal proof.
var ErrE2EEFileStatusUnavailable = errors.New("E2EE file status unavailable")
