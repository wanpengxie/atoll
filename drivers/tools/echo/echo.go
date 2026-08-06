// Package echo provides a minimal actor for dev/test — the actorbase-spec-v1
// §4 S5a "concept budget" consumer: a bare Proc loop over sys.Recv() proving
// the verb table alone (Recv/Reply/Fail) suffices for the simplest possible
// actor, with zero extra concepts riding along.
//
// It ALSO carries the capability-face tour (2026-08-06, owner: "actor 的
// hello world 要展示完整内脏"): the countdown.* types below exercise every
// arm of Sys exactly once — Progress (open account across Recv iterations),
// Emit, Call/Pending (Wait + Cancel + ErrSelfCall), State (Get at boot / Put
// per request), After/CancelTimer (durable timer whose fire re-enters the
// SAME mailbox as an event), PublishObs, and both cancellation planes
// (msg.Ctx() per-request vs sys.Life() per-incarnation). echo.say stays
// byte-identical to the S5a minimal form — read it first, then the tour.
//
// It ALSO demonstrates the config path end to end (the smallest possible
// instance config: one knob). One parser (parseConfig) serves both the
// acceptance gate (ClassDecl.ValidateConfig) and the build (construct), the
// parsed Config is captured into the Proc closure by Def(cfg) — born with the
// incarnation, never hot-read — and enforced at runtime (countdown.start's
// seconds are capped by max_seconds).
package echo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

const DefaultActorID actor.ActorID = "echo"

// TypeSay is the minimal request type: echo the request payload back
// unchanged in a completed response.
const TypeSay = "echo.say"

// Capability-face tour types. A countdown is one request whose账 (serve-ledger
// account) stays OPEN across multiple Recv iterations: admitted at
// countdown.start, held open by Progress, and settled seconds later when the
// timer fire re-enters the mailbox — the minimal prototype of an agent turn
// settled by an asynchronous notification.
const (
	// TypeCountdownStart arms a countdown. Payload:
	//   {seconds:int, note:string, ask?:actorID}
	// seconds is capped by the instance's Config.MaxSeconds (limit_exceeded
	// beyond it). ask (optional) demonstrates the Call arm against another
	// actor; ask == self demonstrates the ErrSelfCall fail-fast instead.
	TypeCountdownStart = "countdown.start"
	// TypeCountdownFire is the timer's msgType: the fire re-enters THIS
	// actor's own mailbox as a kind=event carrying the payload given to
	// After — it has no closure obligation of its own; the obligation it
	// settles is the countdown.start account held open since arming.
	TypeCountdownFire = "countdown.fire"
	// TypeCountdownAbort cancels every armed countdown: timer dismantled,
	// each held account settled with a cancelled failure terminal.
	TypeCountdownAbort = "countdown.abort"
)

// stateKeyLast is the actor-scoped durable state locus (server-backed,
// survives incarnations) the tour writes each armed payload under — the same
// Get-at-boot / Put-per-round shape agent base uses for its resume seed.
const stateKeyLast resource.ResourceID = "echo.countdown-last"

const actorDoc = "Dev/test actor. echo.say replies with its request payload " +
	"unchanged. countdown.start/abort exercise the full Sys capability face " +
	"(Progress/Emit/Call/State/After/PublishObs, deferred terminals, both " +
	"cancellation planes); any other request type fails type_unsupported. " +
	"Config: {max_seconds:int} caps countdown duration (default 300)."

// Config is echo's per-instance deployment config — the smallest possible
// demonstration of the config path: one knob, parsed once at build time,
// captured into the Proc closure, never hot-read.
type Config struct {
	// MaxSeconds caps countdown.start's seconds. Absent or 0 → DefaultMaxSeconds.
	MaxSeconds int `json:"max_seconds"`
}

// DefaultMaxSeconds is the countdown cap a config-less instance gets.
const DefaultMaxSeconds = 300

// parseConfig is the ONE parser both the acceptance gate (ValidateConfig,
// register.go) and the build (construct) go through — accept-time and
// build-time can never disagree about what a valid config is. Empty raw is the
// zero-config case and yields pure defaults; unknown fields are rejected so a
// typo'd knob fails loud at acceptance instead of silently meaning "default".
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{MaxSeconds: DefaultMaxSeconds}
	if len(raw) == 0 {
		return cfg, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("echo config: %w", err)
	}
	if cfg.MaxSeconds < 0 {
		return Config{}, fmt.Errorf("echo config: max_seconds must be >= 0, got %d", cfg.MaxSeconds)
	}
	if cfg.MaxSeconds == 0 {
		cfg.MaxSeconds = DefaultMaxSeconds
	}
	return cfg, nil
}

// Def builds this actor's actorbase.Def over one parsed Config. The config
// rides the closure (registry.Constructor → Def → New() per incarnation, spec
// §1.6): every incarnation is born with the same immutable config; a config
// change replaces desired and rebuilds the body — there is no hot read.
func Def(cfg Config) actorbase.Def {
	return actorbase.Def{Doc: actorDoc, New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error { return run(sys, cfg) }, nil
	}}
}

// startPayload is countdown.start's payload shape.
type startPayload struct {
	Seconds int           `json:"seconds"`
	Note    string        `json:"note"`
	Ask     actor.ActorID `json:"ask,omitempty"`
}

// firePayload rides inside the timer: the fire event's payload carries the
// ORIGIN request id, which is how the fire finds the account it settles —
// no timer-id bookkeeping needed, the correlation travels in the payload.
type firePayload struct {
	Origin message.ID `json:"origin"`
}

// run is the Proc body: entry = birth, return = death (spec §1.6). It is a
// bare loop, not Serve's routes-table sugar — S5a's point is that the raw
// contract alone (no framework layer) is already sufficient here. cfg arrives
// pre-parsed via Def's closure: by the time run breathes, config is a plain
// immutable value, not something to fetch.
func run(sys actorbase.Sys, cfg Config) error {
	// ── State arm, read side: incarnation boot. A cold start yields an
	// accepted-but-empty read (Found=false), never an error to die on.
	if out, err := sys.State().Get(stateKeyLast); err == nil && out.Found {
		_ = sys.PublishObs("echo.countdown-recovered", out.Value)
	}

	// held is the in-memory index of OPEN accounts: origin request id → the
	// delivery Msg whose terminal is still owed. The authoritative account
	// lives in the serve ledger (and in truth); this map is just the worker's
	// working set — lost on crash, and that is fine: the ledger/reaper still
	// closes what an incarnation dropped (spec §1.5 backstop geometry).
	held := map[message.ID]actorbase.Msg{}
	timers := map[message.ID]schedule.TimerID{}

	for {
		msg, err := sys.Recv()
		if err != nil {
			return err // ErrRecvDone: teardown drained us — die cleanly.
		}

		// Events first: they carry no closure obligation, so they must never
		// fall through into the request switch's Fail default. The only event
		// this actor expects is its own timer fire coming home.
		if msg.Kind == message.KindEvent {
			if msg.Type == TypeCountdownFire {
				settleFire(sys, msg, held, timers)
			}
			continue
		}

		switch msg.Type {
		case TypeSay:
			_, _ = sys.Reply(msg, msg.Payload)

		case TypeCountdownStart:
			handleStart(sys, cfg, msg, held, timers)

		case TypeCountdownAbort:
			// Dismantle every armed countdown: CancelTimer + a cancelled
			// failure terminal on each held account. The abort request itself
			// is a second, separate account — it gets its own Reply below.
			for origin, tid := range timers {
				_ = sys.CancelTimer(tid)
				if h, ok := held[origin]; ok {
					_, _ = sys.Fail(h, "cancelled", "aborted by "+string(msg.Sender.ID))
				}
				delete(timers, origin)
				delete(held, origin)
			}
			_, _ = sys.Reply(msg, "ok")

		default:
			_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("echo actor does not handle %s", msg.Type))
		}
	}
}

// handleStart walks the capability face in one pass. Its account does NOT
// close here: Progress keeps it admitted, and the terminal lands seconds
// later in settleFire — one piece of work spanning many Recv iterations.
func handleStart(sys actorbase.Sys, cfg Config, msg actorbase.Msg, held map[message.ID]actorbase.Msg, timers map[message.ID]schedule.TimerID) {
	var p startPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil || p.Seconds <= 0 {
		_, _ = sys.Fail(msg, "payload_invalid", "want {seconds>0, note, ask?}")
		return
	}

	// ── Config enforcement: the knob parsed at build time governs behavior at
	// run time. Note the split from payload_invalid above — that payload is
	// malformed for EVERY echo; this one is fine in shape but over THIS
	// instance's configured limit.
	if p.Seconds > cfg.MaxSeconds {
		_, _ = sys.Fail(msg, "limit_exceeded",
			fmt.Sprintf("seconds %d exceeds this instance's max_seconds %d", p.Seconds, cfg.MaxSeconds))
		return
	}

	// ── Call arm: ask another actor, wait bounded by THIS request's ctx.
	// ctx provenance rule (sys.go header): 这单的活传 msg.Ctx()，命长的活传
	// sys.Life()——if the caller cancels or the deadline passes, this Wait
	// dies with it instead of squatting the worker. ask == self trips
	// ErrSelfCall fail-fast (single-worker deadlock guard), demonstrated
	// rather than hidden.
	askAnswer := json.RawMessage(nil)
	if p.Ask != "" {
		pd, err := sys.Call(p.Ask, TypeSay, p.Note)
		switch {
		case errors.Is(err, actorbase.ErrSelfCall):
			askAnswer = json.RawMessage(`"self-call refused (single-worker deadlock guard)"`)
		case err != nil:
			askAnswer = json.RawMessage(fmt.Sprintf("%q", "call failed: "+err.Error()))
		default:
			if ans, werr := pd.Wait(msg.Ctx(), 5*time.Second); werr == nil {
				askAnswer = ans.Payload
			} else {
				_ = pd.Cancel() // close our out-station account early; stamps cancelled:true
				askAnswer = json.RawMessage(fmt.Sprintf("%q", "no answer: "+werr.Error()))
			}
		}
	}

	// ── Schedule arm: a DURABLE timer — it survives kill -9 of this whole
	// process; the Scheduler re-fires it into whatever incarnation is then
	// alive. The fire's payload carries the origin id (correlation rides the
	// payload, not timer bookkeeping).
	fp, _ := json.Marshal(firePayload{Origin: msg.ID})
	tid, err := sys.After(time.Duration(p.Seconds)*time.Second, TypeCountdownFire, json.RawMessage(fp), schedule.TimerHomeDurable)
	if err != nil {
		_, _ = sys.Fail(msg, "schedule_failed", err.Error())
		return
	}
	held[msg.ID] = msg
	timers[msg.ID] = tid

	// ── State arm, write side: Put is an upsert; a rejected persist is
	// surfaced, not fatal (agent base's checkpoint discipline, in miniature).
	if out, perr := sys.State().Put(stateKeyLast, msg.Payload); perr != nil || !out.Accepted() {
		_ = sys.PublishObs("echo.state-drop", []byte("countdown state put refused"))
	}

	// ── Progress: a NON-terminal provisional response. The account stays
	// open — this is the "I heard you, working on it" tier, the same shape an
	// agent's activity stream refines.
	_, _ = sys.Progress(msg, map[string]any{"armed": true, "seconds": p.Seconds, "ask": askAnswer})

	// ── Emit arm: a kind=event bystander note. No one owes anything on an
	// event; parent/correlation tie it into this request's causal group.
	evp, _ := json.Marshal(map[string]any{"note": p.Note, "seconds": p.Seconds})
	_, _ = sys.Emit(behavior.EventSpec{
		Type:          "echo.countdown-armed",
		Payload:       evp,
		ParentID:      msg.ID,
		CorrelationID: msg.ID,
	})

	// ── Obs arm: push one opaque operational snapshot (kind/val are opaque
	// to the substrate by design).
	_ = sys.PublishObs("echo.countdown-armed", []byte(fmt.Sprintf(`{"held":%d}`, len(held))))
}

// settleFire closes the account a fire came home for. The fire itself is a
// kind=event delivery (no obligation); the obligation is the held request.
// A late Reply — the caller cancelled or timed out while we were counting —
// comes back as ErrRequestClosed and is absorbed: the account is already
// closed in truth, nothing to do, nothing to crash (spec §1.5 "late Reply").
func settleFire(sys actorbase.Sys, msg actorbase.Msg, held map[message.ID]actorbase.Msg, timers map[message.ID]schedule.TimerID) {
	var fp firePayload
	if err := json.Unmarshal(msg.Payload, &fp); err != nil {
		return
	}
	origin, ok := held[fp.Origin]
	if !ok {
		return // fired for an account a previous incarnation held — reaper's business, not ours
	}
	if _, err := sys.Reply(origin, "时间到！"); err != nil && !errors.Is(err, actorbase.ErrRequestClosed) {
		_ = sys.PublishObs("echo.settle-error", []byte(err.Error()))
	}
	delete(held, fp.Origin)
	delete(timers, fp.Origin)
}
