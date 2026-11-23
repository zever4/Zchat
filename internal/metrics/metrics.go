package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	WebsocketConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "websocket_connections_active",
		Help: "Number of active WebSocket connections",
	})

	WebsocketMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "websocket_messages_total",
		Help: "Total number of WebSocket messages",
	}, []string{"type"})
)
