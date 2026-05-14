package framework

import "testing"

func TestMemoryMetricsCounter(t *testing.T) {
	m := NewMemoryMetrics()
	m.IncCounter("ad.hit", "adapter", "feishu", "type", "chat.send")
	m.IncCounter("ad.hit", "adapter", "feishu", "type", "chat.send")
	m.IncCounter("ad.hit", "adapter", "feishu", "type", "chat.create")

	if got := m.Counter("ad.hit", "adapter", "feishu", "type", "chat.send"); got != 2 {
		t.Fatalf("counter chat.send got %d want 2", got)
	}
	if got := m.Counter("ad.hit", "adapter", "feishu", "type", "chat.create"); got != 1 {
		t.Fatalf("counter chat.create got %d want 1", got)
	}
	if got := m.Counter("ad.hit"); got != 0 {
		t.Fatalf("counter no-tags got %d want 0", got)
	}
}

func TestMemoryMetricsTagOrderInvariant(t *testing.T) {
	m := NewMemoryMetrics()
	// Same logical labels in different order MUST collapse to one counter.
	m.IncCounter("x", "a", "1", "b", "2")
	m.IncCounter("x", "b", "2", "a", "1")
	if got := m.Counter("x", "a", "1", "b", "2"); got != 2 {
		t.Fatalf("expected tag-order invariance, got %d", got)
	}
}

func TestMemoryMetricsHistogram(t *testing.T) {
	m := NewMemoryMetrics()
	m.ObserveHistogram("lat", 10.5, "type", "x")
	m.ObserveHistogram("lat", 22.0, "type", "x")
	vals := m.Histogram("lat", "type", "x")
	if len(vals) != 2 || vals[0] != 10.5 || vals[1] != 22.0 {
		t.Fatalf("histogram observations mismatch: %v", vals)
	}
}

func TestNoopMetricsImplementsInterface(t *testing.T) {
	var _ Metrics = NoopMetrics{}
}

func TestNoopLoggerImplementsInterface(t *testing.T) {
	var _ Logger = NoopLogger{}
}

func TestNoopTracerSpan(t *testing.T) {
	var tr Tracer = NoopTracer{}
	sp := tr.StartSpan("x", "a", "b")
	sp.SetAttr("k", "v")
	sp.End()
}
