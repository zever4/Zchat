package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
	"net/http"
)

// Finite labels only; never blob/message/account identifiers or raw errors.
var (
	BlobCleanupPasses    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_blob_cleanup_passes_total", Help: "Private blob cleanup sweeps"}, []string{"lane", "result"})
	BlobCleanupReclaimed = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_blob_cleanup_reclaimed_total", Help: "Attempts observed reclaimed by cleanup, including idempotent replay; not unique removals"}, []string{"lane"})
	BlobCleanupFailures  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_blob_cleanup_failures_total", Help: "Private blob cleanup item failures"}, []string{"lane"})
	BlobCleanupScanned   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_blob_cleanup_scanned_total", Help: "Bounded cleanup candidates considered"}, []string{"lane"})
	BlobCleanupDeferred  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_blob_cleanup_deferred_total", Help: "Cleanup deferrals by bounded scheduling cause"}, []string{"lane", "kind"})
	BlobCleanupEvictions = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_blob_cleanup_retry_evictions_total", Help: "Bounded in-memory retry hints evicted; durable work remains"}, []string{"lane"})
	BlobDownloads        = promauto.NewCounterVec(prometheus.CounterOpts{Name: "e2ee_blob_downloads_total", Help: "Encrypted attachment HTTP outcomes; interrupted bodies may already have HTTP200 headers"}, []string{"method", "result"})
)

func ObserveBlobDownload(method, result string) {
	if method != http.MethodGet && method != http.MethodHead {
		method = "OTHER"
	}
	if result != "complete" && result != "interrupted" {
		result = "rejected"
	}
	BlobDownloads.WithLabelValues(method, result).Inc()
}

func ObserveBlobCleanup(s service.E2EEBlobCleanupSweep, err error) {
	lane := "unknown"
	switch s.Lane {
	case domain.E2EEBlobCleanupUnfinished:
		lane = "unfinished"
	case domain.E2EEBlobCleanupExpiredStaging:
		lane = "expired"
	case domain.E2EEBlobCleanupAccepted:
		lane = "accepted"
	}
	result := "ok"
	if err != nil {
		result = "error"
	} else if s.QueryDeferred {
		result = "deferred"
	}
	BlobCleanupPasses.WithLabelValues(lane, result).Inc()
	BlobCleanupReclaimed.WithLabelValues(lane).Add(float64(s.Reclaimed))
	BlobCleanupFailures.WithLabelValues(lane).Add(float64(s.Failed))
	BlobCleanupScanned.WithLabelValues(lane).Add(float64(s.Scanned))
	BlobCleanupDeferred.WithLabelValues(lane, "item").Add(float64(s.Deferred))
	if s.QueryDeferred {
		BlobCleanupDeferred.WithLabelValues(lane, "query").Inc()
	}
	BlobCleanupEvictions.WithLabelValues(lane).Add(float64(s.RetryEvictions))
}
