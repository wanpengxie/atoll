package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestCobraParseError_WritesEnvelope（fix-spec.md §Fix-T1.4）：
// cobra 解析错误（unknown command/flag）必须以 stdout JSON envelope 输出，
// 不再写 stderr；exit code 用 ExitUsageError；envelope code="usage_error"。
func TestCobraParseError_WritesEnvelope(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"unknown_command", []string{"publsih", "--title", "T"}}, // typo
		{"unknown_flag", []string{"publish", "--no-such-flag"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			code := RunCLI(tc.args, &buf)
			if code != ExitUsageError {
				t.Fatalf("expected exit code %d, got %d", ExitUsageError, code)
			}
			// stdout should be a single JSON envelope and nothing else.
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(lines) == 0 {
				t.Fatalf("empty stdout; want envelope")
			}
			var env Envelope
			if err := json.Unmarshal([]byte(lines[len(lines)-1]), &env); err != nil {
				t.Fatalf("stdout not JSON envelope: %v\nbody=%s", err, buf.String())
			}
			if env.OK {
				t.Fatalf("expected ok=false, got envelope=%+v", env)
			}
			if env.Error == nil || env.Error.Code != "usage_error" {
				t.Fatalf("expected error.code=usage_error, got %+v (body=%s)", env.Error, buf.String())
			}
			if env.Error.Message == "" {
				t.Fatalf("expected non-empty error.message, got %+v", env.Error)
			}
		})
	}
}

// RunE 返回的 CLIError（如 publish 缺 --title）也应被 RunCLI 转成 envelope，
// 但保留 CLIError 的具体 Code，而不是统一覆盖为 usage_error。
func TestRunCLI_CLIErrorPassThrough(t *testing.T) {
	var buf bytes.Buffer
	code := RunCLI([]string{"publish"}, &buf)
	if code != ExitUsageError {
		t.Fatalf("expected exit code %d, got %d", ExitUsageError, code)
	}
	var env Envelope
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &env); err != nil {
		t.Fatalf("stdout not JSON envelope: %v\nbody=%s", err, buf.String())
	}
	if env.OK {
		t.Fatal("expected ok=false")
	}
	if env.Error == nil || env.Error.Code != "invalid_argument" {
		t.Fatalf("expected error.code=invalid_argument from RunE, got %+v", env.Error)
	}
}
