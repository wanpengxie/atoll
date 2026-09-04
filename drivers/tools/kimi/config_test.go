package kimi

import (
	"encoding/json"
	"testing"
)

// A declaration that names an address gets that address; one that says nothing
// gets the extension's own default. This is the knob that used to not exist:
// the class hard-coded the default, so a second seat could only ever be born on
// top of the first.
func TestListenAddrComesFromTheDeclaration(t *testing.T) {
	cfg, err := parseConfig(json.RawMessage(`{"listen_addr":"127.0.0.1:11086"}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != "127.0.0.1:11086" {
		t.Fatalf("declared address ignored: got %q", cfg.ListenAddr)
	}

	blank, err := parseConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if blank.ListenAddr != DefaultListenAddr {
		t.Fatalf("empty config should fall back to %q, got %q", DefaultListenAddr, blank.ListenAddr)
	}
}

// The admit-time gate and the build share one parser, so a config that admits
// can always build — and a config that can never work is refused at admit.
func TestABadAddressIsRefusedAtAdmitTime(t *testing.T) {
	for _, raw := range []string{
		`{"listen_addr":"0.0.0.0:10086"}`, // wildcard: keyless endpoint, refused
		`{"listen_addr":"nonsense"}`,
		`{"listen_port":10086}`, // unknown field: strict decode
	} {
		if _, err := parseConfig(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted a config that cannot work: %s", raw)
		}
	}
}
