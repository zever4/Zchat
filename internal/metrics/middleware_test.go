package metrics

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsRoutingNeverPanics(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	h := MetricsMiddleware(mux, mux)
	for _, path := range []string{"/api/groups/1/x/requests/x", "/api/groups/1/x/members/x", "/api/groups/1/invite", "/api/groups/1/members/2/role"} {
		t.Run(path, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					t.Errorf("metric routing panicked: %v", err)
				}
			}()
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
		})
	}
}

func TestMetricsLabelsAreBoundedByRegisteredRoutes(t *testing.T) {
	httpRequestsTotal.Reset()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/groups/{id}/members/{user_id}/role", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	h := MetricsMiddleware(mux, mux)
	for i := range 100 {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", fmt.Sprintf("/api/groups/%d/members/%d/role", i, i), nil))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(fmt.Sprintf("CUSTOM%d", i), fmt.Sprintf("/missing-%d", i), nil))
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "http_requests_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "endpoint" && label.GetValue() != "/" && label.GetValue() != "GET /api/groups/{id}/members/{user_id}/role" {
					t.Fatalf("unregistered endpoint became a metric label: %q", label.GetValue())
				}
				if label.GetName() == "method" && label.GetValue() != "GET" && label.GetValue() != "OTHER" {
					t.Fatalf("arbitrary method became a metric label: %q", label.GetValue())
				}
			}
		}
	}
}

// Match net/http: the first final status wins, including an implicit Write(200).
func TestResponseWriterRecordsTheStatusActuallySent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		status  int
	}{
		{"first final header", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(201); w.WriteHeader(500) }, 201},
		{"implicit write", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")); w.WriteHeader(500) }, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := httptest.NewRecorder()
			wrapped := &responseWriter{ResponseWriter: raw, statusCode: http.StatusOK}
			tc.handler(wrapped, httptest.NewRequest("GET", "/", nil))
			if raw.Code != tc.status || wrapped.statusCode != tc.status {
				t.Fatalf("wire=%d recorded=%d expected=%d", raw.Code, wrapped.statusCode, tc.status)
			}
		})
	}
}

func TestMiddlewareMeasuresBytesReadRatherThanClaimedContentLength(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /body-count", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.CopyN(io.Discard, r.Body, 3)
		w.WriteHeader(204)
	})
	request := httptest.NewRequest("POST", "/body-count", strings.NewReader("abcdefghij"))
	request.ContentLength = 1000000
	before := histogram(httpRequestSize.WithLabelValues("POST", "POST /body-count"))
	beforeResponse := histogram(httpResponseSize.WithLabelValues("POST", "POST /body-count"))
	MetricsMiddleware(mux, mux).ServeHTTP(httptest.NewRecorder(), request)
	after := histogram(httpRequestSize.WithLabelValues("POST", "POST /body-count"))
	afterResponse := histogram(httpResponseSize.WithLabelValues("POST", "POST /body-count"))
	if after.GetSampleSum()-before.GetSampleSum() != 3 || after.GetSampleCount()-before.GetSampleCount() != 1 {
		t.Fatal("request metric must measure the three consumed bytes")
	}
	if afterResponse.GetSampleCount()-beforeResponse.GetSampleCount() != 1 || afterResponse.GetSampleSum() != beforeResponse.GetSampleSum() {
		t.Fatal("empty HTTP204 must be observed as zero bytes")
	}
}

func TestLongLivedSocketsAndMetricsKeepTheirOriginalWriter(t *testing.T) {
	for _, path := range []string{"/ws", "/rtc/rtc", "/metrics"} {
		raw := httptest.NewRecorder()
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if w != raw {
				t.Fatal("socket or scrape writer was wrapped")
			}
		})
		MetricsMiddleware(mux, mux).ServeHTTP(raw, httptest.NewRequest("GET", path, nil))
	}
}

func histogram(observer prometheus.Observer) *dto.Histogram {
	m := &dto.Metric{}
	_ = observer.(prometheus.Metric).Write(m)
	return m.GetHistogram()
}
