// FIX-T8 phase-2 + phase-4 unit tests against the gateway-internal
// withDefaults secret-gate and newNonce entropy-failure path. These
// stay in the gateway package so the test can read/write package-private
// state (withDefaults / nonceReader) without exporting them.

package gateway

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

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
}

func TestWithDefaults_PopulatedSecrets_NoError(t *testing.T) {
	t.Parallel()
	cfg := Config{
		SessionSecret:      "real-1",
		DaemonSharedSecret: "real-2",
		DeviceTokenSecret:  "real-3",
		HumanCallerSecret:  "real-4",
	}
	out, err := withDefaults(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.SessionSecret != "real-1" {
		t.Errorf("SessionSecret rewritten: %q", out.SessionSecret)
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
