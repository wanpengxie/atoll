package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryRendersPrometheusCountersAndHistograms(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.IncCounter("adapter.dispatch", "adapter", "xhs", "result", "ok")
	r.IncCounter("adapter.dispatch", "result", "ok", "adapter", "xhs")
	r.ObserveHistogram("adapter.dispatch.latency_ms", 12.5, "adapter", "xhs")
	r.ObserveHistogram("adapter.dispatch.latency_ms", 7.5, "adapter", "xhs")

	out := r.RenderPrometheus()
	for _, want := range []string{
		"# TYPE adapter_dispatch counter",
		`adapter_dispatch{adapter="xhs",result="ok"} 2`,
		`adapter_dispatch_latency_ms_count{adapter="xhs"} 2`,
		`adapter_dispatch_latency_ms_sum{adapter="xhs"} 20`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, out)
		}
	}
}

func TestHandlerForServesPrometheusText(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.IncCounter("write_message.reject", "reason", "auth_failed")

	rec := httptest.NewRecorder()
	HandlerFor(r).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("Content-Type=%q want text/plain", ct)
	}
	if body := rec.Body.String(); !strings.Contains(body, `write_message_reject{reason="auth_failed"} 1`) {
		t.Fatalf("body missing counter:\n%s", body)
	}
}
