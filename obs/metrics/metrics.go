package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"unicode"
)

var defaultRegistry = NewRegistry()

// Default returns the process-wide registry used by composition roots.
func Default() *Registry { return defaultRegistry }

// Registry is a small Prometheus-text metrics sink. It intentionally matches
// the adapter/framework and runtime/harness metrics seams without importing
// those packages.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*counterSample
	histograms map[string]*histogramSample
}

type counterSample struct {
	name   string
	labels []label
	value  float64
}

type histogramSample struct {
	name   string
	labels []label
	count  int64
	sum    float64
}

type label struct {
	key   string
	value string
}

// NewRegistry returns an empty metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   map[string]*counterSample{},
		histograms: map[string]*histogramSample{},
	}
}

// IncCounter increments a named counter by one.
func (r *Registry) IncCounter(name string, tags ...string) {
	if r == nil {
		return
	}
	name = sanitizeMetricName(name)
	labels := normalizeLabels(tags)
	key := sampleKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.counters[key]
	if s == nil {
		s = &counterSample{name: name, labels: labels}
		r.counters[key] = s
	}
	s.value++
}

// ObserveHistogram records one observation. The text exporter exposes count
// and sum, which is enough for rate/average alerts without locking in bucket
// policy at the call sites.
func (r *Registry) ObserveHistogram(name string, value float64, tags ...string) {
	if r == nil {
		return
	}
	name = sanitizeMetricName(name)
	labels := normalizeLabels(tags)
	key := sampleKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.histograms[key]
	if s == nil {
		s = &histogramSample{name: name, labels: labels}
		r.histograms[key] = s
	}
	s.count++
	s.sum += value
}

// Handler returns an HTTP handler that renders Prometheus text exposition.
func Handler() http.Handler { return HandlerFor(defaultRegistry) }

// HandlerFor returns an HTTP handler backed by r.
func HandlerFor(r *Registry) http.Handler {
	if r == nil {
		r = NewRegistry()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(r.RenderPrometheus()))
	})
}

// RenderPrometheus returns the registry in Prometheus text format.
func (r *Registry) RenderPrometheus() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	counters := make([]counterSample, 0, len(r.counters))
	for _, s := range r.counters {
		counters = append(counters, *s)
	}
	histograms := make([]histogramSample, 0, len(r.histograms))
	for _, s := range r.histograms {
		histograms = append(histograms, *s)
	}
	r.mu.Unlock()

	sort.Slice(counters, func(i, j int) bool {
		return sampleKey(counters[i].name, counters[i].labels) < sampleKey(counters[j].name, counters[j].labels)
	})
	sort.Slice(histograms, func(i, j int) bool {
		return sampleKey(histograms[i].name, histograms[i].labels) < sampleKey(histograms[j].name, histograms[j].labels)
	})

	var b strings.Builder
	typed := map[string]struct{}{}
	for _, s := range counters {
		if _, ok := typed[s.name]; !ok {
			fmt.Fprintf(&b, "# TYPE %s counter\n", s.name)
			typed[s.name] = struct{}{}
		}
		fmt.Fprintf(&b, "%s%s %s\n", s.name, renderLabels(s.labels), formatFloat(s.value))
	}
	for _, s := range histograms {
		countName := s.name + "_count"
		sumName := s.name + "_sum"
		fmt.Fprintf(&b, "%s%s %d\n", countName, renderLabels(s.labels), s.count)
		fmt.Fprintf(&b, "%s%s %s\n", sumName, renderLabels(s.labels), formatFloat(s.sum))
	}
	return b.String()
}

func normalizeLabels(tags []string) []label {
	if len(tags) == 0 {
		return nil
	}
	labels := make([]label, 0, (len(tags)+1)/2)
	for i := 0; i < len(tags); i += 2 {
		key := sanitizeLabelName(tags[i])
		value := ""
		if i+1 < len(tags) {
			value = tags[i+1]
		}
		labels = append(labels, label{key: key, value: value})
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].key == labels[j].key {
			return labels[i].value < labels[j].value
		}
		return labels[i].key < labels[j].key
	})
	return labels
}

func sampleKey(name string, labels []label) string {
	var b strings.Builder
	b.WriteString(name)
	for _, l := range labels {
		b.WriteByte('|')
		b.WriteString(l.key)
		b.WriteByte('=')
		b.WriteString(l.value)
	}
	return b.String()
}

func renderLabels(labels []label) string {
	if len(labels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.key)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(l.value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func sanitizeMetricName(name string) string {
	return sanitizeName(name, true)
}

func sanitizeLabelName(name string) string {
	return sanitizeName(name, false)
}

func sanitizeName(name string, allowColon bool) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "unnamed"
	}
	var b strings.Builder
	for i, r := range name {
		ok := r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r)) || (allowColon && r == ':')
		if ok {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" || (out[0] != '_' && out[0] != ':' && (out[0] < 'A' || out[0] > 'Z') && (out[0] < 'a' || out[0] > 'z')) {
		out = "_" + out
	}
	return out
}

func escapeLabelValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return strings.ReplaceAll(v, `"`, `\"`)
}

func formatFloat(v float64) string {
	return fmt.Sprintf("%g", v)
}
