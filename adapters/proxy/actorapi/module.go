package actorapi

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ModuleConfig is the per-module configuration blob passed to Init.
//
// Empty Raw means the module should use its defaults. Modules own decoding and
// validation of Raw; invalid configuration must be reported by returning an
// error from Init, which prevents that module from being advertised as ready.
type ModuleConfig struct {
	Raw json.RawMessage
}

// ActorModule is the local proxy daemon module contract.
//
// A proxy daemon hosts one or more modules, announces their ActorID values in
// its ready frame, and routes inbound request envelopes to Handle. Each method
// must be safe to call concurrently with Readiness; Handle may also overlap
// with other Handle calls for the same module.
type ActorModule interface {
	// ActorID returns the channel-local actor id this module hosts, for example
	// "tool:kimi". The value must be stable across Init, Readiness, and Handle.
	ActorID() actor.ActorID

	// Declaration returns static actor capability metadata. The current
	// repository represents this metadata with adapter.Declaration; proxy daemon
	// implementations must keep the returned value deterministic and
	// side-effect-free.
	Declaration() adapter.Declaration

	// Init prepares external clients, local resources, and per-module config.
	// A returned error means the daemon must not route requests to this module
	// until a later Init succeeds. Init should return promptly; long background
	// probes belong in Readiness.
	Init(ctx context.Context, cfg ModuleConfig) error

	// Shutdown releases module resources and cancels in-flight background work.
	// It is best-effort and should be idempotent; callers may close the daemon
	// transport even if Shutdown returns an error.
	Shutdown(ctx context.Context) error

	// Readiness reports live module availability independently of the daemon's
	// WebSocket connection. A nil error with ready=false is an expected
	// not-ready state and reason should explain it. A non-nil error means the
	// probe itself failed; callers should treat the module as not ready and keep
	// the error for diagnostics.
	Readiness(ctx context.Context) (ready bool, reason string, err error)

	// Handle processes one request envelope and returns the terminal response
	// envelope to send back through the proxy daemon transport. Returning an
	// error means no response envelope was produced; the daemon must translate
	// that failure into an envelope-observable terminal response or close the
	// request path according to the dispatch policy.
	Handle(ctx context.Context, env message.Envelope) (message.Envelope, error)
}
