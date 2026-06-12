package kimi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// DefaultActorID is the registry id this adapter owns.
const DefaultActorID actor.ActorID = "tool:kimi"

// Config drives an Actor. ListenAddr is the LOOPBACK address the private device
// WS endpoint binds (e.g. "127.0.0.1:8091", or "127.0.0.1:0" to let the OS pick
// — tests read the resolved addr back via ListenAddr()). The loopback bind is
// the trust boundary (default-trust-local): there is no device key.
type Config struct {
	ListenAddr string
	// NowFn returns the current time; defaults to time.Now. Injectable so
	// tests can shorten deadlines deterministically (the reaper reads it).
	NowFn func() time.Time
	// ReaperInterval is how often the in-flight table is swept for timeouts;
	// defaults to defaultReaperInterval. Injectable so tests catch shortened
	// deadlines promptly without a production-tuned constant.
	ReaperInterval time.Duration
	// Logger surfaces device-face edges (accept/drop/reaper). Defaults to a
	// discard logger.
	Logger *slog.Logger
}

// defaultReaperInterval is the production in-flight sweep cadence. The table is
// tiny, so a 1s scan is cheap; tests inject a shorter one.
const defaultReaperInterval = time.Second

// Actor is the kimi (Kimi WebBridge) adapter cell. The inward (channel) face is
// this struct's Receive/describe; the outward (device) face is the embedded
// *device, which owns the WS endpoint, the connection, the in-flight table, and
// the reaper.
//
// The substrate runs Start/Stop/Receive serially on the cell goroutine, so the
// inward face needs no locks. The device read loop + reaper run on their own
// goroutines and emit channel responses directly through the writer; their
// shared state is guarded inside *device (the one cross-goroutine boundary,
// adapter-actor-spec §4).
//
// Domain face (vs xhs): kimi serves a SINGLE request type, kimi.command, whose
// device verb is the payload's `action` (one of 13 browser primitives) and whose
// device params are the payload's `args`. The closed set is the action allowlist
// (types.go), not a multi-type table.
type Actor struct {
	writer  harness.Writer
	actorID actor.ActorID
	clock   func() time.Time
	dev     *device
}

// NewActor builds a kimi adapter bound to its writer + identity + config. The
// device endpoint is constructed here but only LISTENS at Start (cell
// lifecycle): a half-built actor must never bind a port.
func NewActor(w harness.Writer, cfg Config) *Actor {
	clock := cfg.NowFn
	if clock == nil {
		clock = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	reaperInterval := cfg.ReaperInterval
	if reaperInterval <= 0 {
		reaperInterval = defaultReaperInterval
	}
	a := &Actor{
		writer:  w,
		actorID: DefaultActorID,
		clock:   clock,
	}
	a.dev = newDevice(a, cfg.ListenAddr, clock, reaperInterval, logger)
	return a
}

var _ actorrt.Actor = (*Actor)(nil)
var _ actorrt.Starter = (*Actor)(nil)
var _ actorrt.Stopper = (*Actor)(nil)

// Start binds the device WS endpoint and boots the accept + reaper goroutines.
// A bind failure returns the error so the cell dies fast (positive death) — no
// half-listening adapter ever registers as serviceable.
func (a *Actor) Start(ctx context.Context, _ actorrt.ActorContext) error {
	// The trust model assumes a loopback bind (only same-machine processes can
	// reach the keyless endpoint). A non-loopback addr is a CONFIG ERROR (spec
	// §6.1): it would expose the keyless device port to the network. Fail fast
	// (positive death) rather than start a serviceable-but-exposed endpoint.
	if !isLoopbackAddr(a.dev.addrCfg) {
		return fmt.Errorf("kimi: device endpoint is keyless and trusts localhost; refusing non-loopback bind %q (use 127.0.0.1)", a.dev.addrCfg)
	}
	return a.dev.start(ctx)
}

// isLoopbackAddr reports whether host:port binds the loopback interface (the
// trust boundary for the keyless device endpoint). An unparseable or hostname
// host is treated as non-loopback (warn, don't guess).
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Stop tears the device endpoint down: stop accepting, close the conn, join the
// read loop + reaper. The runtime guarantees no Receive is in flight.
func (a *Actor) Stop(ctx context.Context) error {
	return a.dev.stop(ctx)
}

// ListenAddr returns the resolved device-endpoint address (useful when the
// config asked for port 0). Empty until Start has bound.
func (a *Actor) ListenAddr() string { return a.dev.addr() }

// Receive is the inward mailbox entry. It NEVER blocks on the device: a
// supported request is encoded, registered in the in-flight table, and pushed
// down the conn — the reply comes back asynchronously through the read loop. An
// offline device fails the request immediately (digested, not crashed).
func (a *Actor) Receive(ctx context.Context, env *message.Envelope) error {
	if env == nil {
		return nil
	}
	// The adapter only ACTS on requests — it serves request/response types and
	// answers describe. A non-request (event/response addressed here) has no
	// terminal to author, so it is dropped rather than mis-handled.
	if env.Kind != message.KindRequest {
		return nil
	}
	if env.Type == introspect.QueryDescribe {
		return a.handleDescribe(ctx, env)
	}
	if env.Type == introspect.QueryStatus {
		return a.handleStatus(ctx, env)
	}

	if env.Type != TypeCommand {
		return a.fail(ctx, env, "type_unsupported", fmt.Sprintf("kimi adapter does not handle %s", env.Type))
	}

	// kimi.command carries the device verb in its payload: {action, args}. The
	// verb is resolved here (not a static type→cmd table) and validated against
	// the closed action set before anything reaches the device.
	var cmd commandPayload
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &cmd); err != nil {
			return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode kimi.command payload: %v", err))
		}
	}
	if !isAction(cmd.Action) {
		return a.fail(ctx, env, "invalid_action", fmt.Sprintf("unknown action %q", cmd.Action))
	}

	// args passes through verbatim as the device frame params (the adapter does
	// not interpret it). An absent args becomes an empty object.
	params := cmd.Args
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}

	if err := a.dev.dispatch(ctx, env, cmd.Action, commandDeadline, params); err != nil {
		// dispatch only errors for the digestible offline case; the device
		// being absent is a business failure, not a crash.
		return a.fail(ctx, env, "device_offline", err.Error())
	}
	return nil
}

func (a *Actor) sender() message.Sender {
	return message.Sender{Kind: actor.KindTool, ID: a.actorID}
}

func (a *Actor) fail(ctx context.Context, env *message.Envelope, errorCode, detail string) error {
	_, err := behavior.Fail(ctx, a.writer, a.clock, env, a.sender(), errorCode, detail)
	return err
}

func (a *Actor) handleDescribe(ctx context.Context, env *message.Envelope) error {
	req, err := introspect.ParseDescribeRequest(env.Payload)
	if err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
	}
	answer, ok := introspect.AnswerDescribe(describeCatalog(string(a.actorID)), req)
	if !ok {
		return a.fail(ctx, env, "type_unsupported", fmt.Sprintf("kimi adapter does not handle %s", req.Type))
	}
	_, rerr := behavior.RespondJSON(ctx, a.writer, a.clock, env, a.sender(), answer)
	return rerr
}

// handleStatus answers actor.status with this adapter's live snapshot: whether a
// device is currently attached. This is the adapter's non-trivial live state
// (knowable independent of any in-flight request), surfaced over the ordinary
// request/response path so the app can probe it without a substrate obs frame.
func (a *Actor) handleStatus(ctx context.Context, env *message.Envelope) error {
	if _, err := introspect.ParseStatusRequest(env.Payload); err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode status payload: %v", err))
	}
	answer := introspect.AnswerStatus(string(a.actorID), map[string]any{
		"device_online": a.dev.online(),
	})
	_, rerr := behavior.RespondJSON(ctx, a.writer, a.clock, env, a.sender(), answer)
	return rerr
}
