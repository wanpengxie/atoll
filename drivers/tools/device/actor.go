package device

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Actor is the generic device tool as an actorbase Proc (spec §1.6): entry =
// birth, return = death. All file and exec operations are confined to
// <root> is already the compartment's one qualified channel directory. It
// holds no connection and arms no timer, so run() is a bare loop over
// sys.Recv() that hands each delivery to its own goroutine (coderunner's
// shape), answering through sys.Reply/sys.Fail from there.
//
// Concurrency is a requirement of this actor, not an optimisation. A device is
// a whole machine and every word on it is potentially slow — device.exec runs
// to a 10-minute cap — so answering deliveries one at a time means one long
// command owns the machine and every other caller waits behind it. Reading a
// file while a build runs is the ordinary case, not an exotic one.
//
// It needs no lock to do that because it shares no mutable state: sys, root,
// clock and logger are written once at birth and only read afterwards, and
// every word builds its own os.Root handle and its own buffers per call. The
// isolation is in the design, so keep it there — a field mutated after birth
// would silently make this unsafe.
type Actor struct {
	sys    actorbase.Sys
	root   string
	clock  func() time.Time
	logger *slog.Logger

	// inflight counts the goroutines still answering, so death waits for the
	// answers it already accepted instead of stranding their callers.
	inflight sync.WaitGroup
}

// NewActor constructs a device Actor bound to its workspace root. The identity
// is welded into sys at birth (read via sys.Self() where needed), not carried
// as a field — a half-built value has no identity.
func NewActor(root string, logger *slog.Logger) *Actor {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Actor{
		root:   root,
		clock:  time.Now,
		logger: logger,
	}
}

// Def is this actor's actorbase registration entry (spec §1.6): New mints a
// fresh Actor + Proc per incarnation, closing over root/logger (New itself
// takes zero parameters — the config is captured by this closure, not carried
// by Def).
func Def(root string, logger *slog.Logger) actorbase.Def {
	return actorbase.Def{
		Manifest: manifest(),
		New: func() (actorbase.Proc, error) {
			return NewActor(root, logger).run, nil
		},
	}
}

// run is the Proc body (spec §1.6): loop sys.Recv() until the cell dies or Stop
// is requested — the loop's exit IS this incarnation's death.
//
// The loop itself only routes: it resolves a handler and hands the delivery to
// a goroutine, so it is never the thing that blocks. An unrecognised type is
// refused right here instead of on a goroutine, because a definite refusal
// costs nothing to produce and spawning to produce it would be theatre.
//
// Death waits for the answers already accepted. A word cannot outlive that wait
// by long: device.exec composes its own bound with the request context and caps
// at MaxExecTimeoutMs, and the file words are single syscalls.
func (a *Actor) run(sys actorbase.Sys) error {
	a.sys = sys
	defer a.inflight.Wait()
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		// A non-request has no terminal to author, so it is dropped rather than
		// mis-handled.
		if msg.Kind != message.KindRequest {
			continue
		}
		handler := a.handlerFor(msg.Type)
		if handler == nil {
			a.fail(msg, "type_unsupported", fmt.Sprintf("device actor does not handle %s", msg.Type))
			continue
		}
		a.inflight.Add(1)
		go func(msg actorbase.Msg) {
			defer a.inflight.Done()
			handler(msg)
		}(msg)
	}
}

// handlerFor resolves one request type to the func that answers it, or nil when
// this actor does not serve that word. Returning the handler rather than
// dispatching inline keeps the type list in exactly one place: the loop needs
// to know whether a word is served BEFORE it spawns, and a second switch would
// be a second place to forget a word.
func (a *Actor) handlerFor(msgType string) func(actorbase.Msg) {
	switch msgType {
	case TypeExec:
		return a.handleExec
	case TypeFileRead:
		return a.handleFileRead
	case TypeFileWrite:
		return a.handleFileWrite
	case TypeFileEdit:
		return a.handleFileEdit
	default:
		return nil
	}
}

// channelWorkspace opens the compartment root. Every subsequent file-system
// operation is relative to this handle, so symlink resolution cannot escape.
func (a *Actor) channelWorkspace(chID channel.ID) (*os.Root, error) {
	if chID == "" {
		return nil, errors.New("envelope has no channel id")
	}
	if err := os.MkdirAll(a.root, 0o755); err != nil {
		return nil, fmt.Errorf("create workspace: %w", err)
	}
	root, err := os.OpenRoot(a.root)
	if err != nil {
		return nil, fmt.Errorf("open workspace: %w", err)
	}
	return root, nil
}

func resolvePath(p string) (string, error) {
	if p == "" {
		return "", errors.New("path required")
	}
	if filepath.IsAbs(p) {
		return "", errors.New("path must be relative to the workspace")
	}
	clean := filepath.Clean(p)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", errors.New("path escapes the workspace")
	}
	return clean, nil
}

func pathEscaped(err error) bool {
	return err != nil && strings.Contains(err.Error(), "path escapes from parent")
}

// respond commits a status=completed final with the given result payload.
func (a *Actor) respond(msg actorbase.Msg, result any) {
	if _, err := a.sys.Reply(msg, result); err != nil {
		a.logger.Warn("device.respond.error",
			"request_id", string(msg.ID), "type", msg.Type, "err", err.Error())
	}
}

// fail closes the request with the conventional {error_code, detail} failure.
func (a *Actor) fail(msg actorbase.Msg, errorCode, detail string) {
	if _, err := a.sys.Fail(msg, errorCode, detail); err != nil {
		a.logger.Error("device.fail.respond.error",
			"request_id", string(msg.ID), "error_code", errorCode, "err", err.Error())
	}
}
