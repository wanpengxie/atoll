package archtest

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorcaps"
)

// TestConfigNotInCaps is the S8 red-line tripwire (S-P16): per-instance CONFIG
// (registry.InstanceSpec.Config, the app-rewire spec's "ctx.Config") must NEVER
// be welded into the actorcaps.Caps bundle. Config is an independent PARAMETER
// the constructor closure captures — not a substrate-minted capability; the two
// axes stay separate. A config-bearing field appearing on Caps (a json.RawMessage
// member, or one whose name reads as config) means config leaked into the
// capability bundle. The bundle is exactly the five welded capabilities and
// nothing else.
func TestConfigNotInCaps(t *testing.T) {
	rawMessage := reflect.TypeOf(json.RawMessage(nil))
	capsT := reflect.TypeOf(actorcaps.Caps{})

	want := map[string]bool{"Pen": true, "Access": true, "State": true, "Schedule": true, "Lifecycle": true}

	got := map[string]bool{}
	for i := 0; i < capsT.NumField(); i++ {
		f := capsT.Field(i)
		got[f.Name] = true
		// A json.RawMessage field on the caps bundle = config riding a capability.
		if f.Type == rawMessage {
			t.Errorf("actorcaps.Caps.%s is a json.RawMessage — config must not be welded into the caps bundle (it rides registry.InstanceSpec.Config into the constructor closure)", f.Name)
		}
		// A field whose name reads as config = the same leak by another shape.
		if lower := strings.ToLower(f.Name); strings.Contains(lower, "config") || lower == "cfg" {
			t.Errorf("actorcaps.Caps.%s names a config field — config is a constructor parameter, never a capability", f.Name)
		}
	}

	// Pin the exact capability set: an added field is a review checkpoint (is it a
	// capability, or config/data smuggled in?). Update `want` deliberately when a
	// genuine sixth capability is welded.
	for name := range got {
		if !want[name] {
			t.Errorf("actorcaps.Caps grew field %q — the bundle is capabilities only; confirm it is a welded capability, not config/data", name)
		}
	}
	for name := range want {
		if !got[name] {
			t.Errorf("actorcaps.Caps lost expected capability field %q", name)
		}
	}
}
