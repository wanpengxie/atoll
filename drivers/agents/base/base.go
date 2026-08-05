package base

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// resumeSeedKey is the actor-scoped state locus the会话记忆 seed lives under
// (sys.State — actor-scoped, per-incarnation persistence). checkpoint挂架 (spec
// §1 10.0改性质半的兑现位): the provider gives bytes, the base owns where/when.
const resumeSeedKey resource.ResourceID = "agent.resume-seed"

// NewEngine builds this incarnation's Engine, given the welded Sys and the
// durable resume value the base read from sys.State (nil = cold start). It runs
// INSIDE the Proc (after Sys is welded), so the provider's engine construction
// may depend on both. Sys is passed so the provider can build its meta-tool
// execution face — ExecFace(sys, …) — and thread it into the engine's tool
// surface (§1 三件套②: how the catalog reaches the engine is适配件内政; the
// base only supplies the substrate JobTable + sys.Call face via ExecFace).
type NewEngine func(sys actorbase.Sys, resumeSeed []byte) (Engine, error)

// Config assembles the base Proc. The provider supplies NewEngine (its only
// obligation beside the Engine impl); the rest is skeleton.
type Config struct {
	// NewEngine mints the per-incarnation Engine. Required.
	NewEngine NewEngine
}

// Def wraps the base Proc into the actorbase.Def a provider registers under.
// doc is the actor's registration doc (introspection白拿). The provider's
// registry Constructor closes over its Config and returns this Def.
func Def(doc string, cfg Config) (actorbase.Def, error) {
	if cfg.NewEngine == nil {
		return actorbase.Def{}, errors.New("agent/base: Config.NewEngine required")
	}
	proc := newProc(cfg)
	return actorbase.Def{Doc: doc, New: func() (actorbase.Proc, error) { return proc, nil }}, nil
}

// newProc is the base skeleton Proc: read the resume seed, mint the engine,
// then loop Recv — describe机械答 / request turn — checkpoint挂账 per turn. The
// queue + worker + response分拣 are the actorbase engine's (§1: 零自建).
func newProc(cfg Config) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		seed := readSeed(sys)
		eng, err := cfg.NewEngine(sys, seed)
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
			// Events and responses are context only. A turn is request-backed so its
			// terminal can always close the exact request through Reply/Fail. This
			// also enforces the activity reading law: another agent's activity never
			// wakes this agent (nor does any event).
			if msg.Kind != message.KindRequest || msg.Sender.ID == self {
				continue
			}

			turns++
			trigger := Trigger{
				Envelope:      envelopeFromMsg(msg),
				CorrelationID: behavior.CorrelationID(msg.CorrelationID, msg.ID),
				Index:         turns,
			}
			sink := &procSink{sys: sys, request: msg, trigger: trigger}
			if err := sink.emitTurnStarted(); err != nil {
				return err
			}
			// 命长的活传 sys.Life(): reasoning is incarnation-lived; its terminal
			// nevertheless closes this request through the typed sink.
			if err := eng.Turn(sys.Life(), trigger, sink); err != nil {
				if errors.Is(err, context.Canceled) && sys.Life().Err() != nil {
					// Teardown-quiet寿终 ONLY when the incarnation itself is dying
					// (writes below would fail against a closing cell anyway). A
					// provider surfacing a wrapped Canceled while the life ctx is
					// alive is a plumbing failure and falls through to the loud
					// path — otherwise it would silently end the whole actor with
					// an orphaned request and an unpaired turn.started.
					return nil
				}
				// Best-effort terminal + pairing even when the recovery writes
				// themselves fail: turn.started was already durably emitted, so
				// emitTurnEnded is ALWAYS attempted (join, never early-return).
				if !sink.terminal {
					if failErr := sink.Fail(Failure{ErrorCode: "provider_internal_error", Detail: err.Error()}); failErr != nil {
						err = errors.Join(err, failErr)
					}
				}
				if endErr := sink.emitTurnEnded(); endErr != nil {
					err = errors.Join(err, endErr)
				}
				return err // unrecoverable provider plumbing failure remains loud
			}
			var terminalErr error
			if !sink.terminal {
				if failErr := sink.Fail(Failure{ErrorCode: "provider_no_terminal", Detail: "provider completed without a terminal value"}); failErr != nil {
					terminalErr = failErr
				}
			}
			if endErr := sink.emitTurnEnded(); endErr != nil {
				terminalErr = errors.Join(terminalErr, endErr)
			}
			if terminalErr != nil {
				return terminalErr
			}

			if cp := eng.Checkpoint(); cp != nil {
				// per-turn挂账 (双线审 F8: resume正确性优先; a value-unchanged rewrite
				// is idempotent). Put是 upsert (first write to a fresh key ≠ error).
				// A failed/rejected persist is NOT swallowed: it is surfaced on the
				// actor-obs push, and — because Checkpoint returns the seed every turn
				// the session is non-empty (no dirty micro-opt) — the NEXT turn re-writes
				// the same value, self-healing without killing the actor (P1-2).
				out, err := sys.State().Put(resumeSeedKey, cp)
				if err != nil || !out.Accepted() {
					publishCheckpointDrop(sys, out, err)
				}
			}
		}
	}
}

// ObsCheckpointDrop is the diagnostic kind this base PUSHes when a resume-seed
// persist fails/rejects. agentbase owns this word because the producer owns its
// diagnostic vocabulary.
const ObsCheckpointDrop actorrt.ObsKind = "agentbase.checkpoint_drop"

// publishCheckpointDrop surfaces a failed/rejected resume-seed persist on the
// actor-source obs push (kind agentbase.checkpoint_drop) — the same honest-degrade
// face the engine's recordDrop uses. The turn is NOT failed and the actor does NOT
// die: the next turn re-writes the seed (Checkpoint is stateless-idempotent), so
// the drop self-heals. No watcher → PublishObs is a no-op (its own contract).
func publishCheckpointDrop(sys actorbase.Sys, out accessdoor.Outcome, err error) {
	detail := map[string]any{"reject_reason": string(out.RejectReason)}
	if err != nil {
		detail["error"] = err.Error()
	}
	val, _ := json.Marshal(detail)
	_ = sys.PublishObs(ObsCheckpointDrop, val)
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

// procSink is the only provider-output mapper. Phase changes use Emit; the full
// value and business failure close the triggering request through Reply/Fail.
type procSink struct {
	sys      actorbase.Sys
	request  actorbase.Msg
	trigger  Trigger
	terminal bool
	status   string
}

var _ Sink = (*procSink)(nil)

func (s *procSink) ToolStarted(a ToolActivity) error {
	if a.CallID == "" || a.Tool == "" {
		return errors.New("agent/base: tool activity requires call id and tool")
	}
	return s.emitActivity(registry.ActivityToolStarted, registry.ActivityToolStartedPayload{
		TurnIndex: s.trigger.Index, ToolCallID: a.CallID, Tool: a.Tool,
		Status: registry.ActivityStatusStarted,
	})
}

func (s *procSink) ToolEnded(a ToolActivity) error {
	if a.CallID == "" || a.Tool == "" {
		return errors.New("agent/base: tool activity requires call id and tool")
	}
	status := a.Status
	if status == "" {
		status = registry.ActivityStatusCompleted
	}
	if status != registry.ActivityStatusCompleted && status != registry.ActivityStatusFailed {
		return fmt.Errorf("agent/base: invalid tool activity status %q", status)
	}
	return s.emitActivity(registry.ActivityToolEnded, registry.ActivityToolEndedPayload{
		TurnIndex: s.trigger.Index, ToolCallID: a.CallID, Tool: a.Tool,
		Status: status, Detail: a.Detail,
	})
}

func (s *procSink) Complete(value FinalValue) error {
	if s.terminal {
		return errors.New("agent/base: provider wrote more than one terminal")
	}
	payload := map[string]any{"turn_index": s.trigger.Index}
	if value.Text != "" {
		payload["text"] = value.Text
	}
	if value.NextAction != "" {
		payload["next_action"] = value.NextAction
	}
	for k, v := range value.Extra {
		payload[k] = v
	}
	if _, err := s.sys.Reply(s.request, payload); err != nil {
		return err
	}
	s.terminal = true
	s.status = registry.ActivityStatusCompleted
	return nil
}

func (s *procSink) Fail(f Failure) error {
	if s.terminal {
		return errors.New("agent/base: provider wrote more than one terminal")
	}
	if f.ErrorCode == "" {
		f.ErrorCode = "provider_failed"
	}
	if _, err := s.sys.Fail(s.request, f.ErrorCode, f.Detail); err != nil {
		return err
	}
	s.terminal = true
	s.status = registry.ActivityStatusFailed
	return nil
}

func (s *procSink) emitTurnStarted() error {
	return s.emitActivity(registry.ActivityTurnStarted, registry.ActivityTurnStartedPayload{
		TurnIndex: s.trigger.Index, Status: registry.ActivityStatusStarted,
	})
}

func (s *procSink) emitTurnEnded() error {
	return s.emitActivity(registry.ActivityTurnEnded, registry.ActivityTurnEndedPayload{
		TurnIndex: s.trigger.Index, Status: s.status,
	})
}

func (s *procSink) emitActivity(activityType registry.ActivityType, payload any) error {
	spec, err := behavior.EventSpecJSON(string(activityType), payload, s.audience())
	if err != nil {
		return err
	}
	spec.ParentID = s.request.ID
	spec.CorrelationID = s.trigger.CorrelationID
	spec.Visibility = s.trigger.Envelope.Visibility
	_, err = s.sys.Emit(spec)
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
