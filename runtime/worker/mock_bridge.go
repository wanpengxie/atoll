package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// Env keys M1.6-T5 phase-3 plumbs from daemon → worker spawn so the
// bridge can hash / grep / forward the L4 §2.4 domain prompt without
// any new IPC frame. Kept exported so cmd/cli + tests can reuse the
// constants (one source of truth for the spawn-time contract).
const (
	// EnvKeyChannelType holds the L4 channel-template key (catalog
	// Channel.Type — e.g. "xhs-creator" / "group" / "").
	EnvKeyChannelType = "COAGENT_CHANNEL_TYPE"

	// EnvKeyDomainPrompt holds the L4 §2.4 prompt segment associated
	// with the channel type. Empty / unset for legacy / group channels.
	EnvKeyDomainPrompt = "COAGENT_DOMAIN_PROMPT"

	// EnvKeyMockSingleShot — when set to "1" the MockBridge switches
	// to single-shot mode: every trigger emits exactly ONE agent.text
	// envelope whose payload carries `next_action=done` directly (no
	// separate emitTerminal frame). Used by tests/e2e/ to assert the
	// "single terminal envelope per turn" contract without having the
	// bridge spam a second envelope.
	EnvKeyMockSingleShot = "COAGENT_MOCK_SINGLE_SHOT"

	// EnvKeyMockReplyText — overrides the default reply text in
	// single-shot mode. Defaults to "pong". Lets e2e harness inject a
	// known sentinel so test assertions don't depend on the default
	// string.
	EnvKeyMockReplyText = "COAGENT_MOCK_REPLY_TEXT"

	// EnvKeyMockScript — when set to a known script name, the mock
	// bridge switches its react path to that script's emission
	// sequence. Today the only known value is "xhs-publish": when the
	// trigger payload.text contains the substring "publish", the bridge
	// emits a `tool:xhs-adapter`-addressed kind=request envelope of
	// type=xhs.publish, lets the adapter framework respond, then emits
	// the agent.text summary terminal. Any other trigger falls through
	// to the default single-shot reply.
	//
	// Used by tests/e2e/ to exercise the full agent → adapter request /
	// response chain without standing up a real LLM.
	EnvKeyMockScript = "COAGENT_MOCK_SCRIPT"

	// ScriptXHSPublish is the recognised value for EnvKeyMockScript that
	// triggers the xhs.publish emission path.
	ScriptXHSPublish = "xhs-publish"

	// ScriptProgressMultiTurn drives the mock bridge through a
	// deterministic "progress + final text" sequence for one trigger:
	//   - emit N progress envelopes (agent.text + visibility=system,
	//     each carrying a turn_index + tool_calls preview) so the UI /
	//     e2e harness can observe intermediate per-turn updates.
	//   - emit one terminal agent.text (visibility=public) envelope
	//     with next_action=done.
	//
	// N defaults to 2 (env EnvKeyMockProgressCount overrides). Used by
	// tests/e2e/ to assert the progress contract end-to-end without
	// standing up a real LLM. (Pre-m1.3 this was carried on a separate
	// agent.progress type; impl-vocabulary §2.3 collapsed that into
	// agent.text + visibility=system.)
	ScriptProgressMultiTurn = "multi-turn-with-progress"

	// EnvKeyMockProgressCount overrides the number of progress envelopes
	// (agent.text + visibility=system) the multi-turn-with-progress
	// script emits before the terminal agent.text. Defaults to 2.
	EnvKeyMockProgressCount = "COAGENT_MOCK_PROGRESS_COUNT"
)

// MockBridge is the deterministic Bridge implementation cmd/worker
// uses in M1.6 before a real LLM-backed bridge lands. For every
// incoming KindTrigger envelope it:
//
//  1. Builds an `agent.text` reply envelope echoing the trigger id +
//     turn counter ("mock-reply to <env.ID> (turn N)").
//  2. Calls IPCClient.WriteMessage to push the reply through the
//     daemon harness chain.
//  3. Increments the per-bridge turn counter. When the counter reaches
//     MaxTurns the bridge emits one final reply tagged with
//     next_action=done in the payload and returns nil; Runtime.Run
//     then calls IPCClient.Shutdown and the worker exits.
//
// Errors from WriteMessage abort the bridge. *FenceInvalidError
// propagates up unchanged so Runtime.Run treats it as a fatal exit.
type MockBridge struct {
	// MaxTurns caps the number of triggers the bridge will react to
	// before signalling next_action=done and exiting. Defaults to 8
	// (small enough that integration tests don't run forever; big
	// enough that the M1.6 e2e acceptance #3 reuse-test fits).
	MaxTurns int

	// NowFn returns unix-ms. Defaults to time.Now.
	NowFn func() int64

	// EnvelopeIDFn produces unique envelope ids. Defaults to a
	// counter-based generator ("mock-<workerID>-<turn>").
	EnvelopeIDFn func(workerID string, turn int) string

	// ReplyFn lets tests override the response envelope payload. The
	// default emits `{"text": "mock reply to <env.id> (turn N)"}`.
	ReplyFn func(in ipc.TriggerPayload, turn int) (envelopeType string, payload json.RawMessage)

	// PromptLogWriter receives the one-line domain_prompt_loaded summary
	// the bridge emits when Run starts and finds a non-empty
	// COAGENT_DOMAIN_PROMPT in the environment (M1.6-T5 phase-3
	// acceptance B3). Defaults to os.Stderr so cmd/worker spawned by
	// the daemon writes the line through to the daemon's stderr pipe.
	// Tests set this to a bytes.Buffer to assert the format.
	PromptLogWriter io.Writer

	// EnvLookup returns the value associated with key in the spawn env.
	// Defaults to os.Getenv. Tests override it to drive the bridge with
	// a synthetic env without actually mutating the test process state.
	EnvLookup func(key string) string

	// SingleShot — when true, every trigger emits ONE agent.text
	// envelope whose payload already carries next_action=done. No
	// separate terminal envelope is written. Backed by env var
	// COAGENT_MOCK_SINGLE_SHOT=1 (resolved in applyDefaults).
	SingleShot bool

	// SingleShotReplyText — payload.text value used in single-shot
	// mode. Defaults to "pong" (env COAGENT_MOCK_REPLY_TEXT overrides).
	SingleShotReplyText string

	// Script names the active emission script. Empty = no script (use
	// the default ReplyFn). Backed by env var COAGENT_MOCK_SCRIPT.
	Script string

	turns int
}

// NewMockBridge returns a MockBridge with defaults applied.
func NewMockBridge() *MockBridge {
	return &MockBridge{}
}

// applyDefaults backfills nil fields. Called once per Run.
//
// MaxTurns <= 0 means UNLIMITED — the bridge does not enforce a
// reaction cap and relies on IPC EOF (`triggers` channel close) or
// ctx cancellation for shutdown. cmd/worker passes 0 by default so
// daemon-spawned workers do not get truncated mid-conversation; unit
// tests set MaxTurns explicitly to bound the run deterministically.
func (m *MockBridge) applyDefaults() {
	if m.NowFn == nil {
		m.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if m.EnvelopeIDFn == nil {
		m.EnvelopeIDFn = func(workerID string, turn int) string {
			if workerID == "" {
				return fmt.Sprintf("mock-%d", turn)
			}
			return fmt.Sprintf("mock-%s-%d", workerID, turn)
		}
	}
	if m.ReplyFn == nil {
		m.ReplyFn = func(in ipc.TriggerPayload, turn int) (string, json.RawMessage) {
			body := fmt.Sprintf("mock reply to %s (turn %d)", in.Envelope.ID, turn)
			payload, _ := json.Marshal(map[string]string{"text": body})
			return "agent.text", payload
		}
	}
	if m.PromptLogWriter == nil {
		m.PromptLogWriter = os.Stderr
	}
	if m.EnvLookup == nil {
		m.EnvLookup = os.Getenv
	}
	// Resolve single-shot mode from env once per Run. Field already
	// set by caller (tests) wins; env only flips a default-false field.
	if !m.SingleShot && m.EnvLookup(EnvKeyMockSingleShot) == "1" {
		m.SingleShot = true
	}
	if m.SingleShotReplyText == "" {
		if v := m.EnvLookup(EnvKeyMockReplyText); v != "" {
			m.SingleShotReplyText = v
		} else {
			m.SingleShotReplyText = "pong"
		}
	}
	if m.SingleShot {
		// Override ReplyFn so the very first per-trigger react frame
		// already carries next_action=done. The Run loop's MaxTurns
		// guard then skips emitTerminal (we set MaxTurns high so the
		// guard never fires; loop exits on IPC EOF / ctx cancel).
		text := m.SingleShotReplyText
		m.ReplyFn = func(in ipc.TriggerPayload, turn int) (string, json.RawMessage) {
			payload, _ := json.Marshal(map[string]any{
				"text":        text,
				"next_action": "done",
			})
			return "agent.text", payload
		}
	}
	if m.Script == "" {
		if v := m.EnvLookup(EnvKeyMockScript); v != "" {
			m.Script = v
		}
	}
}

// logDomainPromptOnce writes the M1.6-T5 acceptance-B3 grep target
// (`mock_bridge: domain_prompt_loaded ...`) to PromptLogWriter when the
// spawn env supplies a non-empty COAGENT_DOMAIN_PROMPT. The line is
// stable + idempotent: caller is Run, which fires exactly once per
// worker.Runtime invocation. Empty prompts emit a "no_domain_prompt"
// line keyed by channel_type so log scans can still distinguish
// "phase-3 not wired" from "phase-3 wired but template is generic".
func (m *MockBridge) logDomainPromptOnce(workerID string) {
	channelType := m.EnvLookup(EnvKeyChannelType)
	prompt := m.EnvLookup(EnvKeyDomainPrompt)
	if prompt == "" {
		// Surface the absence so an operator grepping for
		// "domain_prompt_loaded" sees a deterministic counterpart line
		// for legacy / group channels. Keeps `acceptance B3` honest:
		// the grep target is positive only on type=xhs-creator boots.
		_, _ = fmt.Fprintf(m.PromptLogWriter,
			"mock_bridge: no_domain_prompt worker=%s channel_type=%q\n",
			workerID, channelType)
		return
	}
	sum := sha256.Sum256([]byte(prompt))
	_, _ = fmt.Fprintf(m.PromptLogWriter,
		"mock_bridge: domain_prompt_loaded worker=%s channel_type=%q len=%d sha256=%s\n",
		workerID, channelType, len(prompt), hex.EncodeToString(sum[:8]))
}

// Run implements Bridge. Blocks until ctx is cancelled, the trigger
// channel closes (IPC EOF), MaxTurns is hit, or WriteMessage fails.
func (m *MockBridge) Run(ctx context.Context, client *IPCClient) error {
	m.applyDefaults()
	if client == nil {
		return errors.New("worker: MockBridge nil client")
	}
	if client.WorkerActorID() == "" {
		return errors.New("worker: MockBridge handshake did not populate actor id")
	}
	// M1.6-T5 phase-3 — emit the domain_prompt_loaded breadcrumb once
	// per Run. Stable target for acceptance B3 (`grep prompt log`).
	m.logDomainPromptOnce(client.WorkerID())
	triggers := client.Triggers()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload, ok := <-triggers:
			if !ok {
				return nil
			}
			if err := m.react(ctx, client, payload); err != nil {
				return err
			}
			m.turns++
			// MaxTurns > 0 enforces a cap (used by unit tests). 0 / negative
			// = unlimited; the loop exits on IPC EOF or ctx.Done.
			if m.MaxTurns > 0 && m.turns >= m.MaxTurns {
				// Single-shot mode: the per-trigger react frame already
				// carried next_action=done — emitting a second envelope
				// would violate the "exactly 1 agent.text per trigger"
				// contract that tests/e2e/ asserts.
				if !m.SingleShot {
					// Emit a terminal envelope marking next_action=done so
					// the channel log carries an observable exit point.
					if err := m.emitTerminal(ctx, client); err != nil {
						return err
					}
				}
				return nil
			}
		}
	}
}

// react builds and writes the per-trigger reply envelope. When a
// script is active and the trigger matches the script's emission
// trigger, the scripted emission path runs instead of the default
// ReplyFn.
func (m *MockBridge) react(ctx context.Context, client *IPCClient, in ipc.TriggerPayload) error {
	if m.Script == ScriptXHSPublish && triggerMentionsPublish(in) {
		return m.reactXHSPublish(ctx, client, in)
	}
	if m.Script == ScriptProgressMultiTurn {
		return m.reactProgressMultiTurn(ctx, client, in)
	}
	envType, payload := m.ReplyFn(in, m.turns+1)
	env := message.Envelope{
		ID:            message.ID(m.EnvelopeIDFn(client.WorkerID(), m.turns+1)),
		ChannelID:     client.ChannelID(),
		Type:          envType,
		Kind:          message.KindEvent,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: client.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      mockReplyAudience(in.Envelope.Sender.ID),
		Payload:       payload,
		CorrelationID: in.CorrelationID, // L1 propagation
		ParentID:      in.Envelope.ID,
		TS:            m.NowFn(),
		TSReceived:    m.NowFn(),
	}
	if _, err := client.WriteMessage(ctx, env); err != nil {
		return err
	}
	return nil
}

// triggerMentionsPublish returns true when the inbound envelope's
// payload.text contains the substring "publish" (case-insensitive). The
// xhs-publish script keys off this so a human POST like "请发一条小红书
// publish foo bar" reproduces the production flow without needing the
// LLM to do real NLP.
func triggerMentionsPublish(in ipc.TriggerPayload) bool {
	var body struct {
		Text string `json:"text"`
	}
	_ = json.Unmarshal(in.Envelope.Payload, &body)
	return containsFold(body.Text, "publish")
}

// containsFold is a tiny ASCII-case-insensitive substring check — kept
// inline so the bridge doesn't pull in strings.ToLower allocations on
// the hot trigger path.
func containsFold(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			c1 := haystack[i+j]
			c2 := needle[j]
			if c1 >= 'A' && c1 <= 'Z' {
				c1 += 'a' - 'A'
			}
			if c2 >= 'A' && c2 <= 'Z' {
				c2 += 'a' - 'A'
			}
			if c1 != c2 {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// reactXHSPublish emits a kind=request envelope addressed to
// tool:xhs-adapter (type=xhs.publish). The framework F1-F8 chain on
// the daemon side picks it up, dispatches via device transit, and the
// adapter response surfaces as a separate envelope on the channel log.
// Tests assert the request envelope alone — the response is
// observable as a side-effect via the channel.sqlite messages table.
//
// No terminal agent.text is emitted from here: a separate post-
// response summary would require the bridge to observe the response,
// which the mock cannot today (no IPC for received envelopes). Tests
// asserting on the agent.text terminal must POST a second human.text
// trigger so the single-shot fallback runs.
func (m *MockBridge) reactXHSPublish(ctx context.Context, client *IPCClient, in ipc.TriggerPayload) error {
	body := map[string]any{
		"title":   "hello",
		"content": "world",
	}
	payload, _ := json.Marshal(body)
	env := message.Envelope{
		ID:            message.ID(m.EnvelopeIDFn(client.WorkerID(), m.turns+1) + "-xhsreq"),
		ChannelID:     client.ChannelID(),
		Type:          "xhs.publish",
		Kind:          message.KindRequest,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: client.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{"tool:xhs-adapter"},
		Payload:       payload,
		CorrelationID: in.CorrelationID,
		ParentID:      in.Envelope.ID,
		TS:            m.NowFn(),
		TSReceived:    m.NowFn(),
	}
	if _, err := client.WriteMessage(ctx, env); err != nil {
		return err
	}
	return nil
}

// reactProgressMultiTurn emits N progress envelopes (agent.text +
// visibility=system) followed by one terminal agent.text
// (visibility=public) envelope for a single trigger. N defaults to 2
// (env COAGENT_MOCK_PROGRESS_COUNT overrides, max 10 to avoid log
// floods). Progress payloads mirror the kimi bridge contract:
//
//	{
//	  "turn_index": <1-based>,
//	  "stop_reason": "tool_use",
//	  "tool_calls":  [{"name": "...", "preview": "..."}],
//	  "reasoning":   "<short summary>"
//	}
//
// Terminal payload:
//
//	{
//	  "text":        "<final reply>",
//	  "next_action": "done",
//	  "stop_reason": "end_turn",
//	  "turn_index":  <N+1>
//	}
//
// All envelopes carry the same correlation_id + parent_id (the trigger
// envelope id) so the v4 harness routes them together.
func (m *MockBridge) reactProgressMultiTurn(ctx context.Context, client *IPCClient, in ipc.TriggerPayload) error {
	count := 2
	if v := strings.TrimSpace(m.EnvLookup(EnvKeyMockProgressCount)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 10 {
				n = 10
			}
			count = n
		}
	}

	// Reply text — reuse the single-shot override if set, else default.
	replyText := m.SingleShotReplyText
	if replyText == "" {
		replyText = "pong"
	}

	for i := 1; i <= count; i++ {
		progressPayload := map[string]any{
			"turn_index":  i,
			"stop_reason": "tool_use",
			"tool_calls": []map[string]string{
				{
					"name":    fmt.Sprintf("mock_tool_%d", i),
					"preview": fmt.Sprintf("scripted progress step %d/%d", i, count),
				},
			},
			"reasoning": fmt.Sprintf("step %d: probing channel state", i),
		}
		body, _ := json.Marshal(progressPayload)
		env := message.Envelope{
			ID:            message.ID(fmt.Sprintf("%s-progress-%d", m.EnvelopeIDFn(client.WorkerID(), m.turns+1), i)),
			ChannelID:     client.ChannelID(),
			Type:          "agent.text",
			Kind:          message.KindEvent,
			Sender:        message.Sender{Kind: actor.KindAgent, ID: client.WorkerActorID()},
			Visibility:    message.VisibilitySystem,
			Audience:      mockReplyAudience(in.Envelope.Sender.ID),
			Payload:       body,
			CorrelationID: in.CorrelationID,
			ParentID:      in.Envelope.ID,
			TS:            m.NowFn(),
			TSReceived:    m.NowFn(),
		}
		if _, err := client.WriteMessage(ctx, env); err != nil {
			return err
		}
	}

	// Terminal agent.text — payload carries next_action=done so the
	// trigger turn is considered complete.
	finalPayload := map[string]any{
		"text":        replyText,
		"next_action": "done",
		"stop_reason": "end_turn",
		"turn_index":  count + 1,
	}
	body, _ := json.Marshal(finalPayload)
	env := message.Envelope{
		ID:            message.ID(fmt.Sprintf("%s-final", m.EnvelopeIDFn(client.WorkerID(), m.turns+1))),
		ChannelID:     client.ChannelID(),
		Type:          "agent.text",
		Kind:          message.KindEvent,
		Sender:        message.Sender{Kind: actor.KindAgent, ID: client.WorkerActorID()},
		Visibility:    message.VisibilityPublic,
		Audience:      mockReplyAudience(in.Envelope.Sender.ID),
		Payload:       body,
		CorrelationID: in.CorrelationID,
		ParentID:      in.Envelope.ID,
		TS:            m.NowFn(),
		TSReceived:    m.NowFn(),
	}
	if _, err := client.WriteMessage(ctx, env); err != nil {
		return err
	}
	return nil
}

// emitTerminal pushes the bridge's exit-of-turn-budget signal. The
// payload literally contains `{"text": "...", "next_action": "done"}`
// so test harnesses can pattern-match on the final envelope without
// adding a new envelope.type.
func (m *MockBridge) emitTerminal(ctx context.Context, client *IPCClient) error {
	body := map[string]any{
		"text":        "max_turns reached",
		"next_action": "done",
	}
	payload, _ := json.Marshal(body)
	env := message.Envelope{
		ID:         message.ID(m.EnvelopeIDFn(client.WorkerID(), m.turns+1) + "-done"),
		ChannelID:  client.ChannelID(),
		Type:       "agent.text",
		Kind:       message.KindEvent,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: client.WorkerActorID()},
		Visibility: message.VisibilityPublic,
		// max-turns terminal has no trigger context (caller emits as a
		// budget watchdog); addressed to system as observation-only.
		Audience:   message.Audience{actor.SystemActorID},
		Payload:    payload,
		TS:         m.NowFn(),
		TSReceived: m.NowFn(),
	}
	if _, err := client.WriteMessage(ctx, env); err != nil {
		return err
	}
	return nil
}

// mockReplyAudience returns the reply audience for a worker reaction —
// Erlang-style "reply to From". Falls back to system when the trigger
// sender id is unset (boot trigger / synthetic test fixtures).
func mockReplyAudience(triggerSender actor.ActorID) message.Audience {
	if triggerSender == "" {
		return message.Audience{actor.SystemActorID}
	}
	return message.Audience{triggerSender}
}
