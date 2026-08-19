package device

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Actor is the generic device tool as an actorbase Proc (spec §1.6): entry =
// birth, return = death. All file and exec operations are confined to
// <root> is already the compartment's one qualified channel directory. It
// holds no connection and arms no timer, so run() is a bare loop
// over sys.Recv() (echo's shape), with each delivery answered synchronously on
// the worker goroutine through sys.Reply/sys.Fail.
type Actor struct {
	sys    actorbase.Sys
	root   string
	clock  func() time.Time
	logger *slog.Logger
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
// is requested — the loop's exit IS this incarnation's death. There is no
// resource to release (no listener, no timer), so no defer teardown.
func (a *Actor) run(sys actorbase.Sys) error {
	a.sys = sys
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		a.handle(msg)
	}
}

// handle dispatches one delivered Msg by type. A non-request has no terminal to
// author, so it is dropped rather than mis-handled. Unknown types are rejected
// with a failed terminal so the caller observes a definite outcome instead of
// waiting for the framework timer.
func (a *Actor) handle(msg actorbase.Msg) {
	if msg.Kind != message.KindRequest {
		return
	}
	switch msg.Type {
	case TypeExec:
		a.handleExec(msg)
	case TypeFileRead:
		a.handleFileRead(msg)
	case TypeFileWrite:
		a.handleFileWrite(msg)
	case TypeFileEdit:
		a.handleFileEdit(msg)
	default:
		a.fail(msg, "type_unsupported", fmt.Sprintf("device actor does not handle %s", msg.Type))
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
