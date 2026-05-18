package logger_test

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/pkg/logger"
)

// TestLogger_JSONShape asserts the M1.6-T7 acceptance B contract:
// every line emitted by the logger is a single JSON object with the
// stamped `component` + a top-level `msg` field.
func TestLogger_JSONShape(t *testing.T) {
	var buf bytes.Buffer
	lg := logger.New(logger.Config{
		Component: "server",
		Version:   "v0.1.2",
		Writer:    &buf,
		Level:     "info",
	})
	lg.Info("hello world")

	line := strings.TrimSpace(buf.String())
	if !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
		t.Fatalf("output not a JSON object: %q", line)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, line)
	}
	if got["component"] != "server" {
		t.Errorf("component=%v want server", got["component"])
	}
	if got["message"] != "hello world" {
		t.Errorf("msg=%v want hello world", got["message"])
	}
	if got["version"] != "v0.1.2" {
		t.Errorf("version=%v want v0.1.2", got["version"])
	}
	if _, ok := got["time"]; !ok {
		t.Error("missing time field")
	}
}

// TestLogger_DefaultComponent verifies the fallback label when caller
// forgets to set Component (production callers shouldn't, but the test
// pins the default in case someone wires it via init()).
func TestLogger_DefaultComponent(t *testing.T) {
	var buf bytes.Buffer
	lg := logger.New(logger.Config{Writer: &buf})
	lg.Info("x")
	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if got["component"] != "coagent" {
		t.Errorf("default component=%v want coagent", got["component"])
	}
}

// TestLogger_RedirectStdlib asserts that log.Printf calls flow through
// zerolog after the redirect is installed (we ship cmd/* with a few
// legacy log.Printf survivors and they must not bypass the JSON sink).
func TestLogger_RedirectStdlib(t *testing.T) {
	var buf bytes.Buffer
	lg := logger.New(logger.Config{Component: "test", Writer: &buf})
	restore := lg.RedirectStdlib()
	t.Cleanup(restore)

	log.Printf("legacy line %d", 42)

	line := strings.TrimSpace(buf.String())
	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, line)
	}
	if got["message"] != "legacy line 42" {
		t.Errorf("msg=%v want legacy line 42", got["message"])
	}
	if got["source"] != "stdlib_log" {
		t.Errorf("missing/wrong source tag: %v", got["source"])
	}
}

// TestLogger_LevelFilter — caller passing level=warn drops info lines.
// Useful for prod operators tuning verbosity via env without code change.
func TestLogger_LevelFilter(t *testing.T) {
	var buf bytes.Buffer
	lg := logger.New(logger.Config{Writer: &buf, Level: "warn"})
	lg.Info("should be filtered")
	lg.Warn("should pass")
	out := buf.String()
	if strings.Contains(out, "should be filtered") {
		t.Errorf("info line leaked under level=warn: %s", out)
	}
	if !strings.Contains(out, "should pass") {
		t.Errorf("warn line dropped under level=warn: %s", out)
	}
}
