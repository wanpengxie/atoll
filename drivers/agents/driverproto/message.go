package driverproto

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"

	"github.com/wanpengxie/atoll/runtime/harness"
)

type Attachment struct {
	Address string
}

type DriverMessage struct {
	SourceID    string
	Type        string
	Sender      string
	Caller      harness.Caller
	Payload     json.RawMessage
	Text        string
	Attachments []Attachment
}

type ContextMessage struct {
	Seq     int64
	Sender  string
	Kind    string
	Type    string
	Payload json.RawMessage
	Text    string
}

type AttemptToken uint64
type ActionToken uint64
type WorkerTurnRef string

type TurnKind uint8

const (
	TurnChat TurnKind = iota
	TurnCompact
	TurnSelect
)

type TurnOptions struct {
	Model  string `json:"model"`
	Effort string `json:"effort"`
}

type WorkerTurnTarget struct {
	Attempt AttemptToken
	Native  WorkerTurnRef
}

func (t WorkerTurnTarget) Valid() bool { return t.Attempt != 0 && t.Native != "" }

type StartRequest struct {
	Attempt    AttemptToken
	Life       context.Context
	Messages   []DriverMessage
	Background []ContextMessage
	Kind       TurnKind
	Options    TurnOptions
}

// ActorSegments reads the three segments of an inserted member id. The
// substrate treats an ActorID as one opaque identifier and never takes it
// apart; reading the segments is an agent-layer need — a model has to be told
// who is speaking in words it can reason about — so the reading lives here,
// with the agents that need it, and no id-shaped helper is pushed upstream.
//
// The fixed system actor is not an inserted member and has no segments.
func ActorSegments(id actor.ActorID) (kind actor.Kind, seed string, ok bool) {
	parts := strings.Split(string(id), ":")
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	parsed, valid := actor.ParseKind(parts[0])
	if !valid {
		return "", "", false
	}
	return parsed, parts[1], true
}

// SeedLabel names what an id's middle segment means for that kind. It is a
// presentation choice, not a protocol fact: the registry stores one column,
// and it is a login account for a human and a declaration for anything a
// declaration mints. Calling either by the other's name would have the model
// reasoning about the sender from a false premise.
func SeedLabel(kind actor.Kind) string {
	if kind == actor.KindHuman {
		return "principal"
	}
	return "declaration"
}

// CallerLine renders the header prepended to every request text handed to a
// model. It names the sender in the vocabulary the system prompt defines,
// because the id alone reads as an opaque string: an agent shown only
// "human:root:1787128257816" has to infer that the first segment is a kind and
// the second a principal, and the observed failure was an agent that did not —
// it took the sender for itself and answered accordingly.
func CallerLine(caller harness.Caller) string {
	fields := make([]string, 0, 4)
	if caller.Channel != "" {
		fields = append(fields, "channel="+string(caller.Channel))
	}
	if kind, seed, ok := ActorSegments(caller.Actor); ok {
		fields = append(fields, "kind="+string(kind), SeedLabel(kind)+"="+seed)
	}
	fields = append(fields, "actor="+string(caller.Actor))
	return "[from " + strings.Join(fields, " ") + "]"
}
