package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Labels contain only server-owned categories: never users, rooms, SQL or tokens.
var (
	WebsocketHandshakes      = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "websocket_handshake_seconds", Help: "Upgrade and hub registration duration; excludes socket lifetime", Buckets: prometheus.DefBuckets}, []string{"result"})
	WebsocketFrames          = promauto.NewCounterVec(prometheus.CounterOpts{Name: "websocket_frames_received_total", Help: "Received application frames, including malformed frames"}, []string{"kind"})
	WebsocketMessageResults  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "websocket_message_results_total", Help: "Persistence outcomes for chat messages"}, []string{"result"})
	WebsocketMessageDuration = promauto.NewHistogram(prometheus.HistogramOpts{Name: "websocket_message_processing_seconds", Help: "Message validation, persistence and enqueue duration", Buckets: prometheus.DefBuckets})
	WebsocketWriteErrors     = promauto.NewCounter(prometheus.CounterOpts{Name: "websocket_write_errors_total", Help: "Failed application frame writes"})
	WebsocketSlowClients     = promauto.NewCounter(prometheus.CounterOpts{Name: "websocket_slow_client_disconnects_total", Help: "Connections closed because their send queue was full"})
	WebsocketQueueFill       = promauto.NewHistogram(prometheus.HistogramOpts{Name: "websocket_send_queue_utilization", Help: "Per-connection send queue fraction observed before enqueue", Buckets: []float64{0, .1, .25, .5, .75, .9, 1}})
	WebsocketEnqueueFailures = promauto.NewCounter(prometheus.CounterOpts{Name: "websocket_broadcast_enqueue_failures_total", Help: "Broadcasts not queued before cancellation or shutdown"})
	CallsEnabled             = promauto.NewGauge(prometheus.GaugeOpts{Name: "zchat_calls_enabled", Help: "Whether calls are configured in this Go process"})
	CallSignalingConnections = promauto.NewGauge(prometheus.GaugeOpts{Name: "zchat_call_signaling_connections_active", Help: "Active upgraded LiveKit signaling connections through Go, not RTP streams"})
	CallProxyErrors          = promauto.NewCounter(prometheus.CounterOpts{Name: "zchat_call_proxy_errors_total", Help: "LiveKit signaling upstream connection failures"})
	CallOperationDuration    = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "zchat_call_operation_duration_seconds", Help: "LiveKit management and access reconciliation duration", Buckets: prometheus.DefBuckets}, []string{"operation", "result"})
	CallReconcileLastSuccess = promauto.NewGauge(prometheus.GaugeOpts{Name: "zchat_call_reconcile_last_success_timestamp_seconds", Help: "Unix timestamp of last successful access reconciliation; zero until first success"})
)

func ObserveCallOperation(operation string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	CallOperationDuration.WithLabelValues(operation, result).Observe(time.Since(start).Seconds())
}
