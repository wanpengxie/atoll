package observability

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/wanpengxie/ActOS/pkg/metrics"
)

// NewMux exposes non-contract operator endpoints: /metrics plus stdlib pprof.
func NewMux(reg *metrics.Registry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.HandlerFor(reg))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// NewServer returns a debug HTTP server for the supplied address.
func NewServer(addr string, reg *metrics.Registry) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           NewMux(reg),
		ReadHeaderTimeout: 10 * time.Second,
	}
}
