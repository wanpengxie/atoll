package xhs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// DefaultActorID is the registry id this adapter owns.
const DefaultActorID actor.ActorID = "tool:xhs"

// DefaultListenAddr is the loopback addr this adapter's private device WS
// endpoint binds by default. Owned here, not by the composition root — the
// adapter knows its own port (8090, distinct from kimi's 8091).
const DefaultListenAddr = "127.0.0.1:8090"

// Config drives an Actor. ListenAddr is the LOOPBACK address the private device
// WS endpoint binds (e.g. "127.0.0.1:8090", or "127.0.0.1:0" to let the OS pick
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
	// BindRetryInterval is how long the retry loop waits between listen
	// attempts when the loopback port is still held (Q8=B: exclusive-resource
	// contention is domain policy — the actor re-tries until it can bind or is
	// killed). Defaults to defaultBindRetryInterval; tests inject a shorter one.
	BindRetryInterval time.Duration
	// Logger surfaces device-face edges (accept/drop/reaper/bind). Defaults to a
	// discard logger.
	Logger *slog.Logger
}

// defaultReaperInterval is the production in-flight sweep cadence. The table is
// tiny, so a 1s scan is cheap; tests inject a shorter one.
const defaultReaperInterval = time.Second

// defaultBindRetryInterval is the production listen-retry cadence for the
// exclusive loopback port (Q8=B). A predecessor incarnation releases the port
// within its death grace, so a sub-second retry lands the successor promptly.
const defaultBindRetryInterval = 500 * time.Millisecond

// Internal self-authored wake types (spec §3: reaper migrated from a private
// ticker goroutine to sys.After self-wake, swept on the worker goroutine). They
// are KindEvent messages the incarnation sends to itself; run()'s loop
// intercepts them before dispatch, so they never reach the business handler.
const (
	reaperTickType = "xhs.internal.reaper_tick"
	bindRetryType  = "xhs.internal.bind_retry"
)

// Actor is the xhs adapter as an actorbase Proc (spec §1.6). The inward
// (channel) face is run()'s dispatch off sys.Recv(); the outward (device) face
// is the embedded *device, which owns the WS endpoint, the connection, and the
// in-flight table.
//
// sys is bound once, at run()'s first line (birth). The worker goroutine calls
// Recv/dispatch and runs the reaper sweep serially; the device's read-loop
// goroutines close requests by calling sys.Reply/sys.Fail directly (legal
// fan-out — spec §1.2: Sys is concurrency-safe, Msg is immutable), guarded by
// *device's own mutex for the in-flight table they share.
type Actor struct {
	sys               actorbase.Sys
	clock             func() time.Time
	dev               *device
	reaperInterval    time.Duration
	bindRetryInterval time.Duration
	logger            *slog.Logger
}

// NewActor builds an xhs adapter bound to its config. The device endpoint is
// constructed here but only LISTENS at run() (Proc entry = birth): a
// half-built actor must never bind a port.
func NewActor(cfg Config) *Actor {
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
	bindRetryInterval := cfg.BindRetryInterval
	if bindRetryInterval <= 0 {
		bindRetryInterval = defaultBindRetryInterval
	}
	a := &Actor{
		clock:             clock,
		reaperInterval:    reaperInterval,
		bindRetryInterval: bindRetryInterval,
		logger:            logger,
	}
	a.dev = newDevice(a, cfg.ListenAddr, clock, logger)
	return a
}

// Def is this actor's actorbase registration entry (spec §1.6): New mints a
// fresh Actor + Proc per incarnation, closing over cfg.
func Def(cfg Config) actorbase.Def {
	return actorbase.Def{
		Doc: actorDescription,
		New: func() (actorbase.Proc, error) {
			return NewActor(cfg).run, nil
		},
	}
}

// run is the Proc body (spec §1.6): entry = birth, return = death. It attempts
// to bind the device endpoint, then loops sys.Recv() until the cell dies or
// Stop is requested — the loop's exit IS this incarnation's death, and the
// deferred device teardown is its resource release.
func (a *Actor) run(sys actorbase.Sys) error {
	a.sys = sys
	// The trust model assumes a loopback bind (only same-machine processes can
	// reach the keyless endpoint). A non-loopback addr is a CONFIG ERROR (not a
	// resource-contention error): fail fast (positive death) rather than retry
	// forever or start a serviceable-but-exposed endpoint.
	if !isLoopbackAddr(a.dev.addrCfg) {
		return fmt.Errorf("xhs: device endpoint is keyless and trusts localhost; refusing non-loopback bind %q (use 127.0.0.1)", a.dev.addrCfg)
	}
	defer func() { _ = a.dev.stop(context.Background()) }()
	// Initial L3 edge: a connection-bearing adapter KNOWS it starts disconnected —
	// publish offline so the home shows a definite state, not unknown.
	a.publishDevicePresence(false)
	a.tryBind()

	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		switch msg.Type {
		case bindRetryType:
			a.tryBind()
		case reaperTickType:
			a.dev.sweep()
			_, _ = a.sys.After(a.reaperInterval, reaperTickType, nil)
		default:
			a.handle(msg)
		}
	}
}

// tryBind attempts to listen on the exclusive loopback port. On success it arms
// the reaper self-wake; on failure (the port is still held by a predecessor
// incarnation — Q8=B) it schedules a retry and returns without dying. The
// listener is a bound-once resource, so an already-bound device short-circuits.
func (a *Actor) tryBind() {
	if a.dev.bound() {
		return
	}
	if err := a.dev.start(); err != nil {
		a.logger.Warn("xhs.device.bind_retry", "addr", a.dev.addrCfg, "err", err.Error())
		_, _ = a.sys.After(a.bindRetryInterval, bindRetryType, nil)
		return
	}
	// Bound: arm the first reaper tick; each tick re-arms the next in run().
	_, _ = a.sys.After(a.reaperInterval, reaperTickType, nil)
}

// publishDevicePresence pushes a device-presence edge (L3) on the actor-source
// obs axis. Best-effort, advisory (never authoritative — that is send→terminal).
func (a *Actor) publishDevicePresence(online bool) {
	_ = a.sys.PublishObs(introspect.ObsDevicePresence, introspect.MarshalDevicePresence(online))
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

// ListenAddr returns the resolved device-endpoint address (useful when the
// config asked for port 0). Empty until run() has bound.
func (a *Actor) ListenAddr() string { return a.dev.addr() }

// handle dispatches one delivered request Msg. It NEVER blocks on the device: a
// supported request is encoded, registered in the in-flight table, and pushed
// down the conn — the reply comes back asynchronously through the read loop. An
// offline device fails the request immediately (digested, not crashed).
func (a *Actor) handle(msg actorbase.Msg) {
	// The adapter only ACTS on requests — it serves request/response types and
	// answers describe. A non-request (event/response addressed here) has no
	// terminal to author, so it is dropped rather than mis-handled.
	if msg.Kind != message.KindRequest {
		return
	}
	if msg.Type == introspect.QueryDescribe {
		a.handleDescribe(msg)
		return
	}

	spec, ok := lookupType(msg.Type)
	if !ok {
		_, _ = a.sys.Fail(msg, "type_unsupported", fmt.Sprintf("xhs adapter does not handle %s", msg.Type))
		return
	}

	// Translate the channel request into the device command frame. The inward
	// payload IS the device params (both are business-language JSON for these
	// types), so the params pass through verbatim — the type→cmd mapping is the
	// translation that matters.
	params := msg.Payload
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}

	if err := a.dev.dispatch(msg, spec, params); err != nil {
		// dispatch only errors for the digestible offline case; the device
		// being absent is a business failure, not a crash.
		_, _ = a.sys.Fail(msg, "device_offline", err.Error())
	}
}

func (a *Actor) handleDescribe(msg actorbase.Msg) {
	req, err := introspect.ParseDescribeRequest(msg.Payload)
	if err != nil {
		_, _ = a.sys.Fail(msg, "payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
		return
	}
	answer, ok := introspect.AnswerDescribe(describeCatalog(string(a.sys.Self())), req)
	if !ok {
		_, _ = a.sys.Fail(msg, "type_unsupported", fmt.Sprintf("xhs adapter does not handle %s", req.Type))
		return
	}
	_, _ = a.sys.Reply(msg, answer)
}
