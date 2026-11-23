package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
)

func TestBlobCleanupMetricsCountObservationsWithoutErrorDetails(t *testing.T) {
	metric := BlobCleanupPasses.WithLabelValues("expired", "error")
	var before dto.Metric
	if err := metric.Write(&before); err != nil {
		t.Fatal(err)
	}
	ObserveBlobCleanup(service.E2EEBlobCleanupSweep{Lane: domain.E2EEBlobCleanupExpiredStaging, Reclaimed: 2, Failed: 1}, errors.New("private-token-or-blob-id"))
	var after dto.Metric
	if err := metric.Write(&after); err != nil {
		t.Fatal(err)
	}
	if after.GetCounter().GetValue() != before.GetCounter().GetValue()+1 {
		t.Fatal("missing cleanup observation")
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "e2ee_blob_cleanup_passes_total" {
			continue
		}
		for _, m := range family.Metric {
			for _, label := range m.Label {
				switch label.GetName() {
				case "lane":
					if label.GetValue() != "expired" && label.GetValue() != "unfinished" && label.GetValue() != "accepted" && label.GetValue() != "unknown" {
						t.Fatal(label)
					}
				case "result":
					if label.GetValue() != "error" && label.GetValue() != "ok" && label.GetValue() != "deferred" {
						t.Fatal(label)
					}
				default:
					t.Fatal("unbounded label", label)
				}
			}
		}
	}
}

func TestBlobCleanupMetricsAcceptedAndDeferredRemainFinite(t *testing.T) {
	for _, lane := range []domain.E2EEBlobCleanupLane{domain.E2EEBlobCleanupAccepted, domain.E2EEBlobCleanupLaneCount + 10} {
		name := "accepted"
		if lane != domain.E2EEBlobCleanupAccepted {
			name = "unknown"
		}
		counter := BlobCleanupDeferred.WithLabelValues(name, "query")
		var before, after dto.Metric
		if err := counter.Write(&before); err != nil {
			t.Fatal(err)
		}
		ObserveBlobCleanup(service.E2EEBlobCleanupSweep{Lane: lane, Scanned: 3, Deferred: 2, RetryEvictions: 1, QueryDeferred: true}, nil)
		if err := counter.Write(&after); err != nil || after.GetCounter().GetValue() != before.GetCounter().GetValue()+1 {
			t.Fatal("query backoff observation missing", err)
		}
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if len(f.GetName()) < len("e2ee_blob_cleanup_") || f.GetName()[:len("e2ee_blob_cleanup_")] != "e2ee_blob_cleanup_" {
			continue
		}
		for _, metric := range f.Metric {
			for _, label := range metric.Label {
				allowed := map[string]map[string]bool{
					"lane":   {"unfinished": true, "expired": true, "accepted": true, "unknown": true},
					"result": {"ok": true, "error": true, "deferred": true},
					"kind":   {"item": true, "query": true},
				}
				if !allowed[label.GetName()][label.GetValue()] {
					t.Fatal("unbounded/private cleanup label", label)
				}
			}
		}
	}
}

func TestBlobDownloadMetricsBoundResultsIncludingInterruptedHTTP200(t *testing.T) {
	for _, tc := range []struct{ method, result, mappedMethod, mappedResult string }{
		{"GET", "complete", "GET", "complete"}, {"HEAD", "complete", "HEAD", "complete"}, {"GET", "interrupted", "GET", "interrupted"}, {"private-method", "private-id-error", "OTHER", "rejected"},
	} {
		metric := BlobDownloads.WithLabelValues(tc.mappedMethod, tc.mappedResult)
		var before, after dto.Metric
		if err := metric.Write(&before); err != nil {
			t.Fatal(err)
		}
		ObserveBlobDownload(tc.method, tc.result)
		if err := metric.Write(&after); err != nil {
			t.Fatal(err)
		}
		if after.GetCounter().GetValue() != before.GetCounter().GetValue()+1 {
			t.Fatal("missing download outcome")
		}
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "e2ee_blob_downloads_total" {
			continue
		}
		for _, m := range family.Metric {
			for _, l := range m.Label {
				switch l.GetName() {
				case "method":
					if l.GetValue() != "GET" && l.GetValue() != "HEAD" && l.GetValue() != "OTHER" {
						t.Fatal("unbounded method")
					}
				case "result":
					if l.GetValue() != "complete" && l.GetValue() != "rejected" && l.GetValue() != "interrupted" {
						t.Fatal("unbounded result")
					}
				default:
					t.Fatal("unbounded label")
				}
			}
		}
	}
}
