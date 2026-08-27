package kimi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/drivers/tools/plugindevice"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// DefaultActorID is the registry id this adapter owns.
const DefaultActorID actor.ActorID = "tool:kimi"

// DefaultListenAddr is the addr this adapter's private device WS endpoint binds
// when nothing has been set. Owned here, not by the composition root — the
// adapter knows its own port (8091, distinct from xhs's 8090). It is loopback
// because the endpoint is keyless: the default must be the safe one, and moving
// it off loopback is a deliberate act through kimi.listen.set.
const DefaultListenAddr = "127.0.0.1:8091"

// Config drives an Actor. ListenAddr is the STARTING address of the private
// device WS endpoint (e.g. "127.0.0.1:8091", or "127.0.0.1:0" to let the OS
// pick — tests read the resolved addr back via ListenAddr()). A stored address
// from a previous kimi.listen.set wins over it at birth.
type Config struct {
	ListenAddr string
	// NowFn returns the current time; defaults to time.Now. Injectable so
	// tests can shorten deadlines deterministically (the reaper reads it).
	NowFn func() time.Time
	// ReaperInterval is how often the in-flight table is swept for timeouts;
	// defaults to defaultReaperInterval. Injectable so tests catch shortened
	// deadlines promptly without a production-tuned constant.
	ReaperInterval time.Duration
	// BindRetryInterval is how long the retry loop waits between listen attempts
	// when the port is still held (Q8=B: exclusive-resource contention
	// is domain policy — the actor re-tries until it can bind or is killed).
	// Defaults to defaultBindRetryInterval; tests inject a shorter one.
	BindRetryInterval time.Duration
	// Logger surfaces device-face edges (accept/drop/reaper/bind). Defaults to a
	// discard logger.
	Logger *slog.Logger
}

// defaultReaperInterval is the production in-flight sweep cadence. The table is
// tiny, so a 1s scan is cheap; tests inject a shorter one.
const defaultReaperInterval = time.Second

// defaultBindRetryInterval is the production listen-retry cadence for the
// exclusive port (Q8=B). A predecessor incarnation releases the port
// within its death grace, so a sub-second retry lands the successor promptly.
const defaultBindRetryInterval = 500 * time.Millisecond

// Actor is the kimi (Kimi WebBridge) adapter's process state. The inward
// (channel) face is run()'s dispatch off sys.Recv(); the outward (device) face
// is the shared plugindevice.Device, which owns the WS endpoint, the
// connection, the in-flight table, and the reaper.
//
// sys is bound once, at run()'s first line (birth) — before that this Actor is
// a half-built value NewActor produced, never handed a Proc's identity. The
// worker goroutine calls Recv/dispatch serially; the device read-loop and local
// maintenance goroutine close or reap requests through the concurrency-safe
// Sys and device table.
//
// Domain face (vs xhs): kimi serves a SINGLE request type, kimi.command, whose
// device verb is the payload's `action` (one of 13 browser primitives) and whose
// device params are the payload's `args`. The closed set is the action allowlist
// (types.go), not a multi-type table.
type Actor struct {
	sys               actorbase.Sys
	clock             func() time.Time
	dev               *plugindevice.Device
	startAddr         string
	reaperInterval    time.Duration
	bindRetryInterval time.Duration
	logger            *slog.Logger
}

// NewActor builds a kimi adapter bound to its config. The device endpoint is
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
	addr := cfg.ListenAddr
	if addr == "" {
		addr = DefaultListenAddr
	}
	a := &Actor{
		clock:             clock,
		startAddr:         addr,
		reaperInterval:    reaperInterval,
		bindRetryInterval: bindRetryInterval,
		logger:            logger,
	}
	a.dev = plugindevice.New(plugindevice.Deps{
		Tool:       "kimi",
		Sys:        func() actorbase.Sys { return a.sys },
		Clock:      clock,
		Logger:     logger,
		OnPresence: a.publishDevicePresence,
	})
	return a
}

// Def is this actor's actorbase registration entry (spec §1.6): New mints a
// fresh Actor + Proc per incarnation, closing over cfg (New itself takes zero
// parameters — cfg is captured by this closure, not carried by Def).
func Def(cfg Config) actorbase.Def {
	return actorbase.Def{
		Manifest: manifest(),
		New: func() (actorbase.Proc, error) {
			return NewActor(cfg).run, nil
		},
	}
}

// run is the Proc body (spec §1.6): entry = birth, return = death. It binds
// the device endpoint, then loops sys.Recv() until the cell dies or Stop is
// requested — the loop's exit IS this incarnation's death, and the deferred
// device teardown is its resource release.
func (a *Actor) run(sys actorbase.Sys) error {
	a.sys = sys
	// Where a previous kimi.listen.set put the endpoint wins over the config
	// default, because that setting was an operator decision and a restart must
	// not quietly undo it.
	addr := plugindevice.StartAddr(sys, a.startAddr, a.logger)
	// A malformed or wildcard addr is a CONFIG ERROR (not resource contention):
	// fail fast (positive death) rather than retry forever or start a
	// serviceable-but-wrongly-exposed endpoint.
	if err := plugindevice.ValidateAddr(addr); err != nil {
		return fmt.Errorf("kimi: %w", err)
	}
	// Initial L3 edge: a connection-bearing adapter KNOWS it starts disconnected —
	// publish offline so the home shows a definite state, not unknown.
	a.publishDevicePresence(false)
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		a.maintainDevice(sys.Life(), addr)
	}()
	defer func() {
		<-maintenanceDone
		_ = a.dev.Stop(context.Background())
	}()

	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		a.handle(msg)
	}
}

// maintainDevice owns only this adapter's local physical resources. It does
// not use Schedule: a daemon↔Server disconnect may make channel capabilities
// fail-closed, but it must not kill or rebuild an otherwise-live incarnation.
func (a *Actor) maintainDevice(ctx context.Context, addr string) {
	for {
		if err := a.dev.Bind(addr); err == nil {
			break
		} else {
			a.logger.Warn("kimi.device.bind_retry", "addr", addr, "err", err.Error())
		}
		timer := time.NewTimer(a.bindRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	ticker := time.NewTicker(a.reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.dev.Sweep()
		}
	}
}

// publishDevicePresence pushes a device-presence edge (L3) on the actor-source obs axis.
// Best-effort, advisory (never authoritative — that is send→terminal).
func (a *Actor) publishDevicePresence(online bool) {
	if a.sys == nil {
		return
	}
	_ = a.sys.PublishObs(introspect.ObsDevicePresence, introspect.MarshalDevicePresence(online))
}

// ListenAddr returns the resolved device-endpoint address (useful when the
// config asked for port 0). Empty until run() has bound.
func (a *Actor) ListenAddr() string { return a.dev.Addr() }

// Online reports whether a browser device is currently attached.
func (a *Actor) Online() bool { return a.dev.Online() }

// handle dispatches one delivered Msg. It NEVER blocks on the device: a
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

	// The endpoint words are answered by the adapter ITSELF — they are about
	// where the plugin plugs in, so they must work precisely when no plugin is
	// attached and cannot be forwarded down the very connection they configure.
	switch msg.Type {
	case TypeListenSet:
		a.dev.HandleSet(context.Background(), a.sys, msg)
		return
	case TypeListenGet:
		a.dev.HandleGet(a.sys, msg)
		return
	}

	if msg.Type != TypeCommand {
		_, _ = a.sys.Fail(msg, "type_unsupported", fmt.Sprintf("the kimi adapter does not answer %q; it accepts %s", msg.Type, strings.Join([]string{TypeCommand, TypeListenSet, TypeListenGet}, ", ")))
		return
	}

	// kimi.command carries the device verb in its payload: {action, args}. The
	// verb is resolved here (not a static type→cmd table) and validated against
	// the closed action set before anything reaches the device.
	var cmd commandPayload
	if len(msg.Payload) > 0 {
		if err := json.Unmarshal(msg.Payload, &cmd); err != nil {
			_, _ = a.sys.Fail(msg, "payload_invalid", fmt.Sprintf("decode kimi.command payload: %v", err))
			return
		}
	}
	if !isAction(cmd.Action) {
		_, _ = a.sys.Fail(msg, "invalid_action", fmt.Sprintf("unknown action %q; this adapter accepts only: %s", cmd.Action, strings.Join(actionNames(), ", ")))
		return
	}

	// args passes through verbatim as the device frame params (the adapter does
	// not interpret it). An absent args becomes an empty object.
	params := cmd.Args
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}

	spec := plugindevice.Spec{Cmd: cmd.Action, Deadline: commandDeadline}
	if err := a.dev.Dispatch(msg, spec, params); err != nil {
		// Dispatch only errors for the digestible offline case; the device
		// being absent is a business failure, not a crash.
		_, _ = a.sys.Fail(msg, "device_offline", err.Error()+"; the browser device backing this adapter is not connected — check it with list_actors and retry once it is present")
	}
}
