package behavior

import "sync"

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

// MemoryMetrics is a deterministic in-memory Metrics implementation for
// tests. Counters and histogram observations are recorded keyed by the
// concatenation of name + sorted tag pairs. It is safe for concurrent
// use.
type MemoryMetrics struct {
	mu         sync.Mutex
	counters   map[string]int64
	histograms map[string][]float64
}

// NewMemoryMetrics returns a fresh MemoryMetrics.
func NewMemoryMetrics() *MemoryMetrics {
	return &MemoryMetrics{
		counters:   map[string]int64{},
		histograms: map[string][]float64{},
	}
}

// IncCounter satisfies Metrics.
func (m *MemoryMetrics) IncCounter(name string, tags ...string) {
	key := metricKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[key]++
}

// ObserveHistogram satisfies Metrics.
func (m *MemoryMetrics) ObserveHistogram(name string, value float64, tags ...string) {
	key := metricKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histograms[key] = append(m.histograms[key], value)
}

// Counter returns the current value of a named counter (0 when absent).
func (m *MemoryMetrics) Counter(name string, tags ...string) int64 {
	key := metricKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[key]
}

// Histogram returns a copy of the recorded observations for a named
// histogram (nil when absent).
func (m *MemoryMetrics) Histogram(name string, tags ...string) []float64 {
	key := metricKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.histograms[key]
	if src == nil {
		return nil
	}
	out := make([]float64, len(src))
	copy(out, src)
	return out
}

func metricKey(name string, tags []string) string {
	if len(tags) == 0 {
		return name
	}
	// Pair sort: tags come as k,v,k,v... so we sort the (k,v) pairs by key.
	// Use a simple stable sort by iterating; tag count is small in practice.
	pairs := make([][2]string, 0, len(tags)/2+1)
	for i := 0; i < len(tags); i += 2 {
		k := tags[i]
		v := ""
		if i+1 < len(tags) {
			v = tags[i+1]
		}
		pairs = append(pairs, [2]string{k, v})
	}
	// insertion sort for small slices
	for i := 1; i < len(pairs); i++ {
		for j := i; j > 0 && pairs[j-1][0] > pairs[j][0]; j-- {
			pairs[j-1], pairs[j] = pairs[j], pairs[j-1]
		}
	}
	out := name
	for _, p := range pairs {
		out += "|" + p[0] + "=" + p[1]
	}
	return out
}

// Tracer is the F7 tracing seam. The framework opens a span around each
// Handle invocation; production wires real OTel.
type Tracer interface {
	// StartSpan opens a span named `name` and returns a Span value the
	// framework will Close when the operation finishes. Tags follow the
	// Metrics tag convention (flattened k/v list).
	StartSpan(name string, tags ...string) Span
}

// Span is the per-operation tracing handle.
type Span interface {
	// SetAttr adds an attribute to the active span.
	SetAttr(key, value string)

	// End marks the span complete. Idempotent.
	End()
}

// NoopTracer returns a NoopSpan for every call.
type NoopTracer struct{}

// StartSpan satisfies Tracer.
func (NoopTracer) StartSpan(string, ...string) Span { return NoopSpan{} }

// NoopSpan is the zero-value Span used by NoopTracer.
type NoopSpan struct{}

// SetAttr satisfies Span.
func (NoopSpan) SetAttr(string, string) {}

// End satisfies Span.
func (NoopSpan) End() {}
