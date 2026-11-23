package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Fixed labels only: lane=expired/replaced, result=ok/error. Never message IDs,
// generation/account IDs, ciphertext, credentials or raw SQL diagnostics.
var (
	E2EECleanupPasses  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_cleanup_passes_total", Help: "Bounded E2EE cleanup passes"}, []string{"lane", "result"})
	E2EECleanupRemoved = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_cleanup_removed_total", Help: "Server copies removed by background cleanup"}, []string{"lane"})
	E2EECleanupBusy    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_cleanup_busy_total", Help: "Busy copies deferred to a later cleanup sweep"}, []string{"lane"})
)
