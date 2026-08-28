package humancell

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/message"
)

// ui.* is answered by the person's CLIENT, not the person. These pin the two
// things that separates it from a human word: the answer is a free-form result
// the client owns, and — the useful half — the client can say it did not work.

// msgOf turns an envelope into the Msg the serve loop would receive. Log origin
// because that is what a request recovered for the cell carries; the payload is
// the minimal legal request envelope.
func msgOf(env *message.Envelope) actorbase.Msg {
	e := *env
	e.Kind = message.KindRequest
	e.Payload = json.RawMessage(`{"body":{}}`)
	return actorbase.NewMsg(actorbase.OriginLog, context.Background(), e)
}

func uiRequest(word string) *message.Envelope {
	return &message.Envelope{
		ID: "r1", Type: word,
		Sender:   message.Sender{ID: "agent:claude:1"},
		Audience: message.Audience{"human:alice:1"},
	}
}

// The result is the client's own contract; the substrate carries it and does
// not define it. Marshalled exactly once, for the same reason human.approve is:
// handing Reply the raw bytes would encode them as a base64 string and the whole
// answer would silently vanish from truth.
func TestUIResolveCarriesTheClientsOwnResult(t *testing.T) {
	req := uiRequest(subjectgate.WordUIState)
	fs := &fakeSys{self: "human:alice:1", terminalID: "resp1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameResolve, "r", subjectgate.ResolvePayload{
		ChannelID: "c1", ReqID: "r1",
		Result: json.RawMessage(`{"route":{"channel_id":"c0.dev","view":"dynamic"}}`),
	})
	if got := interpretFrame(fs, newDeps("human:alice:1", req, true), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("resolve should receipt: %s", decodeErr(t, got).Code)
	}
	if _, isBytes := fs.replyVal.([]byte); isBytes {
		t.Fatal("Reply must never be handed a []byte — it would be re-marshalled to base64")
	}
	raw, err := json.Marshal(fs.replyVal)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Route struct {
			ChannelID string `json:"channel_id"`
		} `json:"route"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("result is not a JSON object (base64 corruption?): %s", raw)
	}
	if out.Route.ChannelID != "c0.dev" {
		t.Fatalf("result lost its shape: %s", raw)
	}
}

// The half that matters: a client asked to do something impossible must be able
// to answer no. This is the first time the front end can report that an action
// did not work, instead of leaving somebody to notice by eye.
func TestUIResolveCanFailInTheClientsOwnWords(t *testing.T) {
	req := uiRequest(subjectgate.WordUINavigate)
	fs := &fakeSys{self: "human:alice:1", terminalID: "resp1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameResolve, "r", subjectgate.ResolvePayload{
		ChannelID: "c1", ReqID: "r1",
		Error: &subjectgate.ResolveError{Code: "unknown_channel", Message: "no channel c0.nope here"},
	})
	if got := interpretFrame(fs, newDeps("human:alice:1", req, true), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("resolve should receipt: %s", decodeErr(t, got).Code)
	}
	if !fs.failed || fs.replied {
		t.Fatal("a ui error must close through Fail, not Reply")
	}
	if fs.failCode != "unknown_channel" || fs.failDetail != "no channel c0.nope here" {
		t.Fatalf("the client's own words were not passed through: %q / %q", fs.failCode, fs.failDetail)
	}
}

// The two families must not blur. A ui word answered as though a person answered
// it, or a human word answered with a client result, is a caller confusing "no
// screen" with "nobody looked" — and that is exactly the distinction the split
// exists to keep.
func TestTheTwoFamiliesDoNotAcceptEachOthersAnswers(t *testing.T) {
	text := "hello"
	cases := []struct {
		name string
		req  *message.Envelope
		load subjectgate.ResolvePayload
	}{
		{"a ui word answered like a person", uiRequest(subjectgate.WordUIOpen),
			subjectgate.ResolvePayload{ChannelID: "c1", ReqID: "r1", Text: &text}},
		{"a ui word with neither result nor error", uiRequest(subjectgate.WordUIState),
			subjectgate.ResolvePayload{ChannelID: "c1", ReqID: "r1"}},
		{"a ui error with no code", uiRequest(subjectgate.WordUIState),
			subjectgate.ResolvePayload{ChannelID: "c1", ReqID: "r1", Error: &subjectgate.ResolveError{Message: "?"}}},
		{"a human word answered like a client", uiRequest(subjectgate.WordHumanAsk),
			subjectgate.ResolvePayload{ChannelID: "c1", ReqID: "r1", Result: json.RawMessage(`{"x":1}`)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeSys{self: "human:alice:1", terminalID: "resp1"}
			f, _ := subjectgate.NewFrame(subjectgate.FrameResolve, "r", tc.load)
			got := interpretFrame(fs, newDeps("human:alice:1", tc.req, true), f)
			if got.Type == subjectgate.FrameReceipt {
				t.Fatal("accepted; the two word families must not take each other's answers")
			}
			if e := decodeErr(t, got); e.Code != subjectgate.CodeBadPayload && e.Code != subjectgate.CodeInvalidDecision {
				t.Fatalf("code=%q, want a payload refusal", e.Code)
			}
			if fs.replied || fs.failed {
				t.Fatal("a refused resolve must not have closed the request")
			}
		})
	}
}

// A ui word arriving at the cell is left OPEN for the client to answer — it is
// not refused as unsupported the way an unknown word is, and it is not answered
// by the cell on the person's behalf.
func TestAUIWordIsLeftForTheClient(t *testing.T) {
	for _, word := range []string{subjectgate.WordUIState, subjectgate.WordUINavigate, subjectgate.WordUIOpen} {
		fs := &fakeSys{self: "human:alice:1", terminalID: "resp1"}
		humanServeRequest(fs, msgOf(uiRequest(word)))
		if fs.replied || fs.failed {
			t.Fatalf("%s was closed by the cell; only the client can answer it", word)
		}
	}
	// And a word that is neither is still refused, with the client's words named.
	fs := &fakeSys{self: "human:alice:1", terminalID: "resp1"}
	humanServeRequest(fs, msgOf(uiRequest("ui.nonsense")))
	if !fs.failed || fs.failCode != "type_unsupported" {
		t.Fatalf("an unknown word should still be refused: failed=%v code=%q", fs.failed, fs.failCode)
	}
}
