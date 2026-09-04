package xhs

import (
	"sync/atomic"
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
const DefaultActorID actor.ActorID = "tool:xhs"

// DefaultListenAddr is the addr this adapter's private device WS endpoint binds
// when nothing has been set. Owned here, not by the composition root — the
// adapter knows its own port (8090, distinct from kimi's 8091). It is loopback
// because the endpoint is keyless: the default must be the safe one, and moving
// it off loopback is a deliberate act through xhs.listen.set.
const DefaultListenAddr = "127.0.0.1:8090"

// Config drives an Actor. ListenAddr is the STARTING address of the private
// device WS endpoint (e.g. "127.0.0.1:8090", or "127.0.0.1:0" to let the OS
// pick — tests read the resolved addr back via ListenAddr()). A stored address
// from a previous xhs.listen.set wins over it at birth.
type Config struct {
	ListenAddr string
	// Session is this seat's opaque grouping key on the shared device. See
	// plugindevice/shared.go: it lives on the seat because the device is shared
	// and the key is the one thing that must differ between seats.
	Session string
	// NowFn returns the current time; defaults to time.Now. Injectable so
	// tests can shorten deadlines deterministically (the reaper reads it).
	NowFn func() time.Time
	// ReaperInterval is how often the in-flight table is swept for timeouts;
	// defaults to defaultReaperInterval. Injectable so tests catch shortened
	// deadlines promptly without a production-tuned constant.
	ReaperInterval time.Duration
	// BindRetryInterval is how long the retry loop waits between listen
	// attempts when the port is still held (Q8=B: exclusive-resource
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
// exclusive port (Q8=B). A predecessor incarnation releases the port within its
// death grace, so a sub-second retry lands the successor promptly.
const defaultBindRetryInterval = 500 * time.Millisecond

// Actor is the xhs adapter as an actorbase Proc (spec §1.6). The inward
// (channel) face is run()'s dispatch off sys.Recv(); the outward (device) face
// is the shared plugindevice.Device, which owns the WS endpoint, the
// connection, and the in-flight table.
//
// sys is bound once, at run()'s first line (birth). The worker goroutine calls
// Recv/dispatch; the device read-loop and local maintenance goroutine close or
// reap requests through the concurrency-safe Sys and device table.
type Actor struct {
	sys               actorbase.Sys
	clock             func() time.Time
	dev               atomic.Pointer[plugindevice.Handle]
	session           string
	online            bool
	startAddr         string
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
	a.session = cfg.Session
	return a
}

// Def is this actor's actorbase registration entry (spec §1.6): New mints a
// fresh Actor + Proc per incarnation, closing over cfg.
func Def(cfg Config) actorbase.Def {
	return actorbase.Def{
		Manifest: manifest(),
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
	// Where a previous xhs.listen.set put the endpoint wins over the config
	// default, because that setting was an operator decision and a restart must
	// not quietly undo it.
	addr := plugindevice.StartAddr(sys.Life(), sys, a.startAddr, a.logger)
	// A malformed or wildcard addr is a CONFIG ERROR (not resource contention):
	// fail fast (positive death) rather than retry forever or start a
	// serviceable-but-wrongly-exposed endpoint.
	if err := plugindevice.ValidateAddr(addr); err != nil {
		return fmt.Errorf("xhs: %w", err)
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
		if h := a.dev.Load(); h != nil {
			_ = h.Release(context.Background())
		}
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
		// Attach, not Bind — see plugindevice/shared.go: the first seat on this
		// address binds, later seats share the live connection.
		h, err := plugindevice.Attach(addr, a.session, plugindevice.Deps{
			Tool:   "xhs",
			Clock:  a.clock,
			Logger: a.logger,
		})
		if err == nil {
			a.dev.Store(h)
			break
		} else {
			a.logger.Warn("xhs.device.attach_retry", "addr", addr, "err", err.Error())
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
			h := a.dev.Load()
			if h == nil {
				continue
			}
			h.Sweep()
			// Read presence rather than being pushed it: the device is shared
			// and cannot speak in any one actor's name.
			if now := h.Online(); now != a.online {
				a.online = now
				a.publishDevicePresence(now)
			}
		}
	}
}

// publishDevicePresence pushes a device-presence edge (L3) on the actor-source
// obs axis. Best-effort, advisory (never authoritative — that is send→terminal).
func (a *Actor) publishDevicePresence(online bool) {
	if a.sys == nil {
		return
	}
	_ = a.sys.PublishObs(introspect.ObsDevicePresence, introspect.MarshalDevicePresence(online))
}

// ListenAddr returns the resolved device-endpoint address (useful when the
// config asked for port 0). Empty until run() has bound.
// The handle appears only once maintainDevice attaches, so every reader guards
// for nil. Before that this seat has no device — which is a real, reportable
// state ("not attached yet"), not a reason to panic.
func (a *Actor) ListenAddr() string {
	if h := a.dev.Load(); h != nil {
		return h.Addr()
	}
	return ""
}

// Online reports whether a browser device is currently attached.
func (a *Actor) Online() bool {
	if h := a.dev.Load(); h != nil {
		return h.Online()
	}
	return false
}

// Desired is the address this adapter was last asked to listen on.
func (a *Actor) Desired() string {
	if h := a.dev.Load(); h != nil {
		return h.Desired()
	}
	return ""
}

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

	// The endpoint words are answered by the adapter ITSELF — they are about
	// where the plugin plugs in, so they must work precisely when no plugin is
	// attached and cannot be forwarded down the very connection they configure.
	switch msg.Type {
	case TypeListenSet:
		if h := a.dev.Load(); h != nil {
			h.HandleSet(context.Background(), a.sys, msg)
		} else {
			_, _ = a.sys.Fail(msg, "device_offline", "the endpoint is not bound yet")
		}
		return
	case TypeListenGet:
		if h := a.dev.Load(); h != nil {
			h.HandleGet(a.sys, msg)
		} else {
			_, _ = a.sys.Fail(msg, "device_offline", "the endpoint is not bound yet")
		}
		return
	}

	spec, ok := lookupType(msg.Type)
	if !ok {
		_, _ = a.sys.Fail(msg, "type_unsupported", fmt.Sprintf("the xhs adapter does not answer %q; it accepts %s", msg.Type, strings.Join(supportedTypes(), ", ")))
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

	h := a.dev.Load()
	if h == nil {
		_, _ = a.sys.Fail(msg, "device_offline", "this seat has not bound its device endpoint yet")
		return
	}
	if err := h.Call(string(msg.ID), spec, params, func(in plugindevice.Inbound) {
		a.settle(msg, in)
	}); err != nil {
		// Call only errors for the digestible offline case; the device being
		// absent is a business failure, not a crash.
		_, _ = a.sys.Fail(msg, "device_offline", err.Error()+"; the browser device backing this adapter is not connected — check it with list_actors and retry once it is present")
	}
}

// settle turns one device answer into this actor's terminal. It lives here
// rather than in the transport because the error taxonomy is this adapter's,
// and because only this actor holds the Sys entitled to close the request.
func (a *Actor) settle(msg actorbase.Msg, in plugindevice.Inbound) {
	if a.sys == nil {
		return
	}
	if in.OK {
		if _, err := a.sys.Reply(msg, in.Result); err != nil {
			a.logger.Warn("xhs.device.reply_failed", "correlation_id", string(msg.ID), "err", err.Error())
		}
		return
	}
	code, detail := "device_error", "device reported a failure"
	if in.ErrCode != "" {
		code = in.ErrCode
	}
	if in.ErrMsg != "" {
		detail = in.ErrMsg
	}
	if _, err := a.sys.Fail(msg, code, detail); err != nil {
		a.logger.Warn("xhs.device.fail_failed", "correlation_id", string(msg.ID), "err", err.Error())
	}
}
