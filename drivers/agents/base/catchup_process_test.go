package base

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestCatchupFiltersEveryProvisionalResponse(t *testing.T) {
	for _, payload := range []string{
		`{"status":"received"}`,
		`{"status":"queued"}`,
		`{"status":"processing","process":{"kind":"tool","phase":"started"}}`,
		`{"status":"provider.waiting"}`,
	} {
		if !isProvisionalContext(string(message.KindResponse), json.RawMessage(payload)) {
			t.Fatalf("provisional not filtered: %s", payload)
		}
	}
}

func TestCatchupKeepsRequestsEventsAndTerminalResponses(t *testing.T) {
	cases := []struct {
		kind    string
		payload string
	}{
		{string(message.KindRequest), `{"body":{"text":"hi"}}`},
		{string(message.KindEvent), `{"name":"joined"}`},
		{string(message.KindResponse), `{"status":"completed","text":"done"}`},
		{string(message.KindResponse), `{"status":"failed","error_code":"boom"}`},
	}
	for _, tc := range cases {
		if isProvisionalContext(tc.kind, json.RawMessage(tc.payload)) {
			t.Fatalf("durable context filtered: %s %s", tc.kind, tc.payload)
		}
	}
}
