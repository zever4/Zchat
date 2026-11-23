package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/zever4/Zchat/internal/admission"
)

// Fixed server-owned categories only; no account/session/token labels.
var AdmissionRequests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "zchat_admission_requests_total", Help: "Request admission decisions, including bounded overload rejection"}, []string{"class", "result"})

func ObserveAdmission(class, result string) { AdmissionRequests.WithLabelValues(class, result).Inc() }
func AdmissionActive(g *admission.Gate) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "zchat_admission_operations_active", Help: "Admitted DB-bound work across transports; excludes idle sockets"}, func() float64 { return float64(g.Snapshot().Active) })
}
