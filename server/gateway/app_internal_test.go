// FIX-T8 phase-2 + phase-4 unit tests against the gateway-internal
// withDefaults secret-gate and newNonce entropy-failure path. These
// stay in the gateway package so the test can read/write package-private
// state (withDefaults / nonceReader) without exporting them.

package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func withProductionOrigins(cfg Config) Config {
	cfg.DeviceAllowedOrigins = []string{"https://device.example"}
	cfg.PushhubAllowedOrigins = []string{"https://ui.example"}
	cfg.DaemonbusAllowedOrigins = []string{"https://ops.example"}
	return cfg
}

func TestWithDefaults_RejectsEmptySecret(t *testing.T) {
	t.Parallel()
	// SessionSecret empty, everything else populated → fail-fast on
	// SessionSecret.
	cfg := Config{
		DaemonSharedSecret: "abcd",
		DeviceTokenSecret:  "abcd",
		HumanCallerSecret:  "abcd",
	}
	_, err := withDefaults(cfg)
	if err == nil {
		t.Fatal("expected error on empty SessionSecret")
	}
	var insec *ErrInsecureSecret
	if !errors.As(err, &insec) {
		t.Fatalf("err type=%T want *ErrInsecureSecret", err)
	}
	if insec.Field != "SessionSecret" {
		t.Errorf("Field=%q want SessionSecret", insec.Field)
	}
}

func TestWithDefaults_RejectsDevSentinel(t *testing.T) {
	t.Parallel()
	cfg := Config{
		SessionSecret:      "real",
		DaemonSharedSecret: devDaemonSecret, // <- dev sentinel
		DeviceTokenSecret:  "real",
		HumanCallerSecret:  "real",
	}
	_, err := withDefaults(cfg)
	if err == nil {
		t.Fatal("expected error on dev DaemonSharedSecret sentinel")
	}
	var insec *ErrInsecureSecret
	if !errors.As(err, &insec) {
		t.Fatalf("err type=%T want *ErrInsecureSecret", err)
	}
	if insec.Field != "DaemonSharedSecret" || insec.Value != devDaemonSecret {
		t.Errorf("Field=%q Value=%q", insec.Field, insec.Value)
	}
}

func TestWithDefaults_AllowDevSecrets(t *testing.T) {
	t.Parallel()
	// AllowDevSecrets=true → empty values silently filled with dev
	// sentinels; no error.
	cfg := Config{AllowDevSecrets: true}
	out, err := withDefaults(cfg)
	if err != nil {
		t.Fatalf("AllowDevSecrets=true should not error: %v", err)
	}
	if out.SessionSecret != devSessionSecret {
		t.Errorf("SessionSecret=%q", out.SessionSecret)
	}
	if out.DaemonSharedSecret != devDaemonSecret {
		t.Errorf("DaemonSharedSecret=%q", out.DaemonSharedSecret)
	}
	if out.DeviceTokenSecret != devDeviceSecret {
		t.Errorf("DeviceTokenSecret=%q", out.DeviceTokenSecret)
	}
	if out.HumanCallerSecret != devHumanCallerSecret {
		t.Errorf("HumanCallerSecret=%q", out.HumanCallerSecret)
	}
	if len(out.PushhubAllowedOrigins) == 0 || len(out.DaemonbusAllowedOrigins) == 0 {
		t.Fatal("AllowDevSecrets should install dev WS origins")
	}
}

// TestWithDefaults_AllowDevSecrets_EmitsJSONWarning verifies the
// M1.6-T7 phase-2 contract: dev-sentinel substitution emits a
// structured JSON warn line carrying both the offending field name
// and the dev sentinel value, so operators grepping for
// `dev_sentinel_used` can spot misconfiguration immediately.
func TestWithDefaults_AllowDevSecrets_EmitsJSONWarning(t *testing.T) {
	// Not t.Parallel — pkgLogger is package-level.
	orig := pkgLogger
	t.Cleanup(func() { pkgLogger = orig })

	var buf bytes.Buffer
	pkgLogger = zerolog.New(&buf).With().Timestamp().Logger()

	cfg := Config{AllowDevSecrets: true} // all 4 secrets empty → 4 warnings
	if _, err := withDefaults(cfg); err != nil {
		t.Fatalf("AllowDevSecrets=true should not error: %v", err)
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 warn lines, got %d:\n%s", len(lines), buf.String())
	}
	fields := map[string]bool{}
	for _, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Errorf("non-JSON warn line: %v\n%s", err, line)
			continue
		}
		if rec["level"] != "warn" {
			t.Errorf("level=%v want warn: %s", rec["level"], line)
		}
		if rec["event"] != "gateway.dev_sentinel_used" {
			t.Errorf("event=%v want gateway.dev_sentinel_used: %s", rec["event"], line)
		}
		if f, ok := rec["field"].(string); ok {
			fields[f] = true
		}
	}
	for _, want := range []string{"SessionSecret", "DaemonSharedSecret", "DeviceTokenSecret", "HumanCallerSecret"} {
		if !fields[want] {
			t.Errorf("missing dev_sentinel_used warning for field %q\nlines=%s", want, buf.String())
		}
	}
}

func TestWithDefaults_PopulatedSecrets_NoError(t *testing.T) {
	t.Parallel()
	cfg := withProductionOrigins(Config{
		SessionSecret:      "real-1",
		DaemonSharedSecret: "real-2",
		DeviceTokenSecret:  "real-3",
		HumanCallerSecret:  "real-4",
	})
	out, err := withDefaults(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.SessionSecret != "real-1" {
		t.Errorf("SessionSecret rewritten: %q", out.SessionSecret)
	}
}

func TestWithDefaults_RejectsMissingOriginAllowlist(t *testing.T) {
	t.Parallel()
	cfg := Config{
		SessionSecret:      "real-1",
		DaemonSharedSecret: "real-2",
		DeviceTokenSecret:  "real-3",
		HumanCallerSecret:  "real-4",
	}
	_, err := withDefaults(cfg)
	if err == nil {
		t.Fatal("expected error on missing origin allowlist")
	}
	var insec *ErrInsecureOrigin
	if !errors.As(err, &insec) {
		t.Fatalf("err type=%T want *ErrInsecureOrigin", err)
	}
	if insec.Field != "DeviceAllowedOrigins" {
		t.Errorf("Field=%q want DeviceAllowedOrigins", insec.Field)
	}
}

func TestWithDefaults_RejectsWildcardOrigin(t *testing.T) {
	t.Parallel()
	cfg := withProductionOrigins(Config{
		SessionSecret:      "real-1",
		DaemonSharedSecret: "real-2",
		DeviceTokenSecret:  "real-3",
		HumanCallerSecret:  "real-4",
	})
	cfg.PushhubAllowedOrigins = []string{"*"}
	_, err := withDefaults(cfg)
	if err == nil {
		t.Fatal("expected error on wildcard origin")
	}
	var insec *ErrInsecureOrigin
	if !errors.As(err, &insec) {
		t.Fatalf("err type=%T want *ErrInsecureOrigin", err)
	}
	if insec.Field != "PushhubAllowedOrigins" || insec.Value != "*" {
		t.Errorf("Field=%q Value=%q", insec.Field, insec.Value)
	}
}

// --- newNonce entropy failure -----------------------------------------------

type failReader struct {
	err error
}

func (f *failReader) Read(p []byte) (int, error) { return 0, f.err }

func TestNewNonce_PropagatesEntropyError(t *testing.T) {
	// Not t.Parallel — nonceReader is package-level state.
	orig := nonceReader
	t.Cleanup(func() { nonceReader = orig })
	nonceReader = &failReader{err: errors.New("entropy down")}

	if _, err := newNonce(); err == nil {
		t.Fatal("newNonce should propagate the reader error")
	} else if !strings.Contains(err.Error(), "entropy down") {
		t.Errorf("err=%v should wrap reader error", err)
	}
}

func TestNewNonce_ReturnsHex(t *testing.T) {
	// Not t.Parallel — reader swap.
	orig := nonceReader
	t.Cleanup(func() { nonceReader = orig })
	// Inject a deterministic reader so the test asserts on bytes.
	nonceReader = bytes.NewReader([]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	})
	got, err := newNonce()
	if err != nil {
		t.Fatalf("newNonce: %v", err)
	}
	if got != "000102030405060708090a0b0c0d0e0f" {
		t.Errorf("nonce=%q", got)
	}
}

// io.EOF compile reference.
var _ = io.EOF
