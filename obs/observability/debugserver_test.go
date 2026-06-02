package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/obs/metrics"
)

func TestNewMuxMountsMetricsAndPprof(t *testing.T) {
	t.Parallel()

	reg := metrics.NewRegistry()
	reg.IncCounter("behavior.dispatch", "adapter", "xhs")
	mux := NewMux(reg)

	metricsRec := httptest.NewRecorder()
	mux.ServeHTTP(metricsRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := metricsRec.Body.String(); !strings.Contains(body, "adapter_dispatch") {
		t.Fatalf("/metrics body missing adapter_dispatch:\n%s", body)
	}

	pprofRec := httptest.NewRecorder()
	mux.ServeHTTP(pprofRec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if pprofRec.Code != http.StatusOK {
		t.Fatalf("/debug/pprof status=%d want 200", pprofRec.Code)
	}
}
