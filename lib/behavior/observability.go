package behavior

// Logging is *log/slog* — the Go-std structured-logging facade (the role K8s
// gives logr / Erlang gives the kernel logger): one facade the whole project
// funnels through, backend chosen via slog.Handler, configured once at the edge
// and injected down. behavior does NOT define its own Logger interface — that
// was a reinvention of slog. Seams take *slog.Logger; nil → the caller defaults
// to slog.New(slog.DiscardHandler).

// Metrics is the F7 metrics seam. Adapters and the framework increment
// Counter values and record Histogram observations through this single
// interface; concrete backends (Prometheus / OTel / no-op) live outside
// the framework so kernel callers stay backend-agnostic.
//
// Tags are key-value pairs flattened into a string slice ("k1", "v1",
// "k2", "v2", ...). Implementations MUST tolerate a missing trailing
// value by treating it as empty string.
type Metrics interface {
	// IncCounter increments a named counter by 1. delta > 1 callers
	// invoke it that many times to keep the interface minimal.
	IncCounter(name string, tags ...string)

	// ObserveHistogram records one observation in the named histogram
	// (e.g. latency in ms). Implementations may bucket internally.
	ObserveHistogram(name string, value float64, tags ...string)
}

// NoopMetrics drops every call.
type NoopMetrics struct{}

// IncCounter satisfies Metrics.
func (NoopMetrics) IncCounter(string, ...string) {}

// ObserveHistogram satisfies Metrics.
func (NoopMetrics) ObserveHistogram(string, float64, ...string) {}
