package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/registry"
)

func TestValidateConfigRejectsRequiredNegativeGroups(t *testing.T) {
	tests := []struct {
		name string
		raws []string
		want string
	}{
		{"transport closed set", []string{`{"name":"test","transport":"tcp","command":"x"}`}, "transport must be"},
		{"stdio command required", []string{`{"name":"test","transport":"stdio"}`}, "requires command"},
		{"http url required", []string{`{"name":"test","transport":"http"}`}, "requires url"},
		{"transport fields exclusive", []string{`{"name":"test","transport":"stdio","command":"x","url":"http://127.0.0.1"}`}, "forbids url"},
		{"url scheme", []string{`{"name":"test","transport":"http","url":"file:///tmp/mcp"}`}, "absolute http or https"},
		{"name required", []string{`{"transport":"http","url":"http://example.test/mcp"}`}, "name required"},
		{"name shape", []string{
			`{"name":"bad.name","transport":"http","url":"http://example.test/mcp"}`,
			`{"name":"Bad","transport":"http","url":"http://example.test/mcp"}`,
			`{"name":"-bad","transport":"http","url":"http://example.test/mcp"}`,
		}, "name must match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, raw := range test.raws {
				err := registry.ValidateConfig("mcp", json.RawMessage(raw))
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("parseConfig(%s) error=%v, want containing %q", raw, err, test.want)
				}
			}
		})
	}
}

func TestValidateConfigDoesNotProbeNetwork(t *testing.T) {
	raw := json.RawMessage(`{"name":"offline","transport":"http","url":"http://127.0.0.1:1/mcp"}`)
	if err := registry.ValidateConfig("mcp", raw); err != nil {
		t.Fatalf("ValidateConfig probed the network or rejected a valid shape: %v", err)
	}
	cfg, err := parseConfig(raw)
	if err != nil {
		t.Fatalf("shape-valid unreachable URL was rejected: %v", err)
	}
	if cfg.URL != "http://127.0.0.1:1/mcp" {
		t.Fatalf("url=%q", cfg.URL)
	}
}

func TestHTTPTransportFieldsAreMutuallyExclusiveEvenWhenEmpty(t *testing.T) {
	for _, raw := range []string{
		`{"name":"test","transport":"http","url":"http://example.test/mcp","command":""}`,
		`{"name":"test","transport":"http","url":"http://example.test/mcp","args":[]}`,
		`{"name":"test","transport":"http","url":"http://example.test/mcp","cwd":""}`,
		`{"name":"test","transport":"http","url":"http://example.test/mcp","env":{}}`,
	} {
		if _, err := parseConfig(json.RawMessage(raw)); err == nil {
			t.Fatalf("mutually exclusive field accepted: %s", raw)
		}
	}
}
