package base

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// resumeSeedKey is the actor-scoped state locus the会话记忆 seed lives under
// (sys.State — actor-scoped, per-incarnation persistence). checkpoint挂架 (spec
// §1 10.0改性质半的兑现位): the provider gives bytes, the base owns where/when.
const resumeSeedKey resource.ResourceID = "agent.resume-seed"

// eventType is the one envelope type an agent emits for turn output. Agents
// serve no closed request-type set — every trigger becomes a turn — and reply
// with agent.text EVENTS (progress + terminal), never a kind=response (the
// trigger's own account closes by its author#2 deadline; an agent's product is
// its emitted narration, not a request reply).
const eventType = "agent.text"

// NewEngine builds this incarnation's Engine, seeded with the durable resume
// value the base read from sys.State (nil = cold start). It runs INSIDE the
// Proc (after Sys is welded), so the provider's engine construction may depend
// on the resume seed. The provider builds + installs its own tool面 here (via
// BuildMCPCatalog) — how the catalog reaches the engine is适配件内政 (§1 三件套②).
type NewEngine func(resumeSeed []byte) (Engine, error)

// Config assembles the base Proc. The provider supplies NewEngine (its only
// obligation beside the Engine impl); the rest is skeleton.
type Config struct {
	// NewEngine mints the per-incarnation Engine. Required.
	NewEngine NewEngine
	// NowFn returns unix-ms; defaults to time.Now.UnixMilli. Only used for the
	// emitted envelopes' clock via behavior (the engine owns its own clock).
	NowFn func() int64
}

// Def wraps the base Proc into the actorbase.Def a provider registers under.
// doc is the actor's registration doc (introspection白拿). The provider's
// registry Constructor closes over its Config and returns this Def.
func Def(doc string, cfg Config) (actorbase.Def, error) {
	if cfg.NewEngine == nil {
		return actorbase.Def{}, errors.New("agent/base: Config.NewEngine required")
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	proc := newProc(cfg)
	return actorbase.Def{Doc: doc, New: func() (actorbase.Proc, error) { return proc, nil }}, nil
}

// newProc is the base skeleton Proc: read the resume seed, mint the engine,
// then loop Recv — describe机械答 / else a turn — checkpoint挂账 per turn. The
// queue + worker + response分拣 are the actorbase engine's (§1: 零自建).
func newProc(cfg Config) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		seed := readSeed(sys)
		eng, err := cfg.NewEngine(seed)
		if err != nil {
			return fmt.Errorf("agent/base: engine boot: %w", err) // loud死: no half-alive agent
		}
		defer eng.Close()

		self := sys.Self()
		turns := 0
		for {
			msg, err := sys.Recv()
			if err != nil {
				return err // ErrRecvDone / teardown — the loop termination contract
			}

			// Mechanical self-answer (actor citizenship) — never fed to the engine.
			if msg.Kind == message.KindRequest && msg.Type == introspect.QueryDescribe {
				answerDescribe(sys, eng, self, msg)
				continue
			}
			// An actor never reacts to its OWN emissions (agent.text echoes).
			if msg.Sender.ID == self {
				continue
			}

			turns++
			trigger := Trigger{
				Envelope:      envelopeFromMsg(msg),
				CorrelationID: behavior.CorrelationID(msg.CorrelationID, msg.ID),
				Index:         turns,
			}
			sink := &procSink{sys: sys, trigger: trigger}
			// 命长的活传 sys.Life(): a turn is long-lived reasoning, not bounded by
			// the trigger request's own account deadline (agents emit, don't reply).
			if err := eng.Turn(sys.Life(), trigger, sink); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil // teardown-quiet寿终, not横死
				}
				return err // plumbing failure → loud死 (matches old fatal-flag path)
			}

			if cp := eng.Checkpoint(); cp != nil {
				// per-turn挂账 (双线审 F8: resume正确性优先; a value-unchanged rewrite
				// is idempotent). Put是 upsert (first write to a fresh key ≠ error).
				_, _ = sys.State().Put(resumeSeedKey, cp)
			}
		}
	}
}

// readSeed reads the durable resume seed at boot. A missing/empty locus (cold
// start) yields nil — every field is an accepted-but-empty read, never an error
// path the boot should die on.
func readSeed(sys actorbase.Sys) []byte {
	out, err := sys.State().Get(resumeSeedKey)
	if err != nil || !out.Found || len(out.Value) == 0 {
		return nil
	}
	return out.Value
}

// answerDescribe serves the actor.describe self-answer through the standard
// introspect dispatch. The base stamps ActorID from sys.Self() (dynamic — never
// hardcoded, the A2 fix); the provider filled Description/SkillDoc/Types.
func answerDescribe(sys actorbase.Sys, eng Engine, self actor.ActorID, msg actorbase.Msg) {
	req, err := introspect.ParseDescribeRequest(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
		return
	}
	d := eng.Describe()
	d.ActorID = string(self)
	answer, ok := introspect.AnswerDescribe(d, req)
	if !ok {
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("agent has no type %s", req.Type))
		return
	}
	_, _ = sys.Reply(msg, answer)
}

// procSink maps each turn Output onto sys.Emit — the base's output形 (§1 输出映射).
//
// VISIBILITY申报: sys.Emit commits kind=event with visibility=public only (the
// verb table exposes no system-visibility event to a Proc). go-kimi's current
// per-tool-step progress rides visibility=system; when the go-kimi provider
// migrates onto this skeleton (S3), that visibility nuance is the provider
// migration's concern — the base faithfully provides the intermediate output
// PORT (Final=false) but not per-output visibility control, which the substrate
// verb table does not surface. Not changed here (红线7: 改动以切片账为限).
type procSink struct {
	sys     actorbase.Sys
	trigger Trigger
}

var _ Sink = (*procSink)(nil)

func (s *procSink) Emit(o Output) error {
	payload := map[string]any{"turn_index": s.trigger.Index}
	if o.Text != "" {
		payload["text"] = o.Text
	}
	if o.NextAction != "" {
		payload["next_action"] = o.NextAction
	}
	if o.Reason != "" {
		payload["reason"] = o.Reason
	}
	for k, v := range o.Extra {
		payload[k] = v
	}
	_, err := s.sys.Emit(eventType, payload, s.audience())
	return err
}

// audience routes the emitted event to whoever triggered the turn (Erlang From
// routing). A boot-path trigger with an empty sender falls back to the system
// actor (matches the two bridges' replyAudience).
func (s *procSink) audience() actor.ActorID {
	if id := s.trigger.Envelope.Sender.ID; id != "" {
		return id
	}
	return actor.SystemActorID
}

// envelopeFromMsg projects a delivered Msg back into the message.Envelope a
// provider's tool RuntimeContext wants (parent id + correlation). Field-by-
// field assignment onto a zero value — a projection back, NOT a second envelope
// construction primitive (mirrors the engine's own envelopeFromMsg; envelope
// construction proper stays in lib/behavior).
func envelopeFromMsg(m actorbase.Msg) message.Envelope {
	var env message.Envelope
	env.ID = m.ID
	env.TS = m.TS
	env.ChannelID = m.ChannelID
	env.Sender = m.Sender
	env.Kind = m.Kind
	env.Type = m.Type
	env.Payload = m.Payload
	env.ParentID = m.ParentID
	env.CorrelationID = m.CorrelationID
	env.Visibility = m.Visibility
	env.Audience = m.Audience
	env.ExpiresAt = m.ExpiresAt
	return env
}
