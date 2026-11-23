package metrics

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "endpoint", "status_code"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)

	httpRequestsInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Number of HTTP requests currently being processed",
		},
		[]string{"method", "endpoint"},
	)

	httpRequestSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_size_bytes",
			Help:    "Size of HTTP requests",
			Buckets: prometheus.ExponentialBuckets(100, 10, 5),
		},
		[]string{"method", "endpoint"},
	)

	httpResponseSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_response_size_bytes",
			Help:    "Size of HTTP responses",
			Buckets: prometheus.ExponentialBuckets(100, 10, 5),
		},
		[]string{"method", "endpoint"},
	)
)

func MetricsMiddleware(next http.Handler, router *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" || r.URL.Path == "/metrics" || strings.HasPrefix(r.URL.Path, "/rtc/") {
			next.ServeHTTP(w, r)
			return
		}
		// Registered patterns form a finite label set. Gateway routes share /api/.
		_, endpoint := router.Handler(r)
		if endpoint == "" {
			endpoint = "unmatched"
		}
		method := metricMethod(r.Method)

		httpRequestsInFlight.WithLabelValues(method, endpoint).Inc()
		defer httpRequestsInFlight.WithLabelValues(method, endpoint).Dec()

		body := &countingBody{ReadCloser: r.Body}
		if r.Body != nil {
			r.Body = body
		}

		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		start := time.Now()
		next.ServeHTTP(rw, r)

		duration := time.Since(start).Seconds()
		statusCode := strconv.Itoa(rw.statusCode)

		httpRequestsTotal.WithLabelValues(method, endpoint, statusCode).Inc()
		httpRequestDuration.WithLabelValues(method, endpoint).Observe(duration)
		httpRequestSize.WithLabelValues(method, endpoint).Observe(float64(body.size))
		httpResponseSize.WithLabelValues(method, endpoint).Observe(float64(rw.size))
	})
}

// responseWriter перехватывает статус код и размер ответа
type responseWriter struct {
	http.ResponseWriter
	statusCode  int
	size        int64
	wroteHeader bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		rw.ResponseWriter.WriteHeader(code)
		return
	}
	if rw.wroteHeader {
		return
	}
	rw.wroteHeader = true
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.size += int64(n)
	return n, err
}

type countingBody struct {
	io.ReadCloser
	size int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.size += int64(n)
	return n, err
}

// Unwrap keeps ResponseController deadlines available through instrumentation.
func (rw *responseWriter) Unwrap() http.ResponseWriter { return rw.ResponseWriter }

func metricMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}
