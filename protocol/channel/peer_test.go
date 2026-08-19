package channel

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestPeerFramesHaveNoTargetAndRoundTripDescribeCard(t *testing.T) {
	request := Request{From: From{Channel: "a", Actor: "human:a:1", RequestID: "r1"}, Type: "work.run", Payload: json.RawMessage(`null`)}
	raw, err := json.Marshal(request)
	if err != nil || strings.Contains(string(raw), `"to"`) {
		t.Fatalf("request wire=%s err=%v", raw, err)
	}
	var decoded Request
	if err := json.Unmarshal(raw, &decoded); err != nil || string(decoded.Payload) != "null" {
		t.Fatalf("request=%+v err=%v", decoded, err)
	}
	cardRaw := []byte(`{"words":{"work.run":{"description":"run"}}}`)
	var card Card
	if err := json.Unmarshal(cardRaw, &card); err != nil || string(card.Words["work.run"]) != `{"description":"run"}` {
		t.Fatalf("card=%+v err=%v", card, err)
	}
	var describe Describe
	if err := json.Unmarshal([]byte(`{"from":{"channel":"a"}}`), &describe); err != nil || describe.From.Channel != "a" {
		t.Fatalf("describe=%+v err=%v", describe, err)
	}
}

func TestPeerFrameDecodeClassifiesMalformedUnknownAndSemanticFailures(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		out  any
		want error
	}{
		{name: "malformed", raw: `{"from":`, out: &Request{}, want: ErrMalformedPeerFrame},
		{name: "unknown top level", raw: `{"from":{"channel":"a","actor":"x","request_id":"r"},"type":"x","payload":{},"to":{"channel":"b"}}`, out: &Request{}, want: ErrUnknownPeerField},
		{name: "unknown nested", raw: `{"from":{"channel":"a","actor":"x","request_id":"r","extra":1},"type":"x","payload":{}}`, out: &Request{}, want: ErrUnknownPeerField},
		{name: "missing request type", raw: `{"from":{"channel":"a","actor":"x","request_id":"r"},"payload":{}}`, out: &Request{}, want: ErrInvalidPeerFrame},
		{name: "nonpositive progress sequence", raw: `{"request_id":"r","seq":0,"status":"processing"}`, out: &Progress{}, want: ErrInvalidPeerFrame},
		{name: "result has both arms", raw: `{"body":null,"fail":{"stage":"gate","code":"forbidden"}}`, out: &Result{}, want: ErrInvalidPeerFrame},
		{name: "result has neither arm", raw: `{}`, out: &Result{}, want: ErrInvalidPeerFrame},
		{name: "unknown stage", raw: `{"stage":"network","code":"down"}`, out: &Failure{}, want: ErrInvalidPeerFrame},
		{name: "unknown gate code", raw: `{"stage":"gate","code":"tool_failed"}`, out: &Failure{}, want: ErrInvalidPeerFrame},
		{name: "missing cancel from", raw: `{"request_id":"r"}`, out: &Cancel{}, want: ErrInvalidPeerFrame},
		{name: "missing describe from", raw: `{}`, out: &Describe{}, want: ErrInvalidPeerFrame},
		{name: "missing card words", raw: `{}`, out: &Card{}, want: ErrInvalidPeerFrame},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			if test.name == "malformed" {
				err = decodeClosed([]byte(test.raw), test.out)
			} else {
				err = json.Unmarshal([]byte(test.raw), test.out)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want category %v", err, test.want)
			}
		})
	}
}

func TestPeerResultAcceptsExactlyOneArmIncludingNullBody(t *testing.T) {
	for _, raw := range []string{
		`{"body":null}`,
		`{"body":{"ok":true}}`,
		`{"fail":{"stage":"receiver","code":"tool_failed","detail":"bad"}}`,
	} {
		var result Result
		if err := json.Unmarshal([]byte(raw), &result); err != nil {
			t.Fatalf("valid result %s: %v", raw, err)
		}
	}
}
