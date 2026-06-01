package behavior_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wanpengxie/ActOS/lib/behavior"
)

func TestModuleContextDoesNotExposeRawCapabilities(t *testing.T) {
	typ := reflect.TypeOf(adapter.ModuleContext{})
	for _, name := range []string{
		"HarnessChain",
		"Correlation",
		"ErrorPolicy",
		"RelayTransit",
		"ActorReadiness",
	} {
		if _, ok := typ.FieldByName(name); ok {
			t.Fatalf("ModuleContext must not expose raw capability field %s", name)
		}
	}
}

func TestExternalRequestPayloadMarshalsAsRawJSON(t *testing.T) {
	raw, err := json.Marshal(struct {
		Payload adapter.ExternalRequestPayload `json:"payload"`
	}{Payload: adapter.ExternalRequestPayload(`{"request_id":"domain"}`)})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(raw) != `{"payload":{"request_id":"domain"}}` {
		t.Fatalf("payload marshaled as %s", raw)
	}
}
