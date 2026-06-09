package device

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// Actor implements actorrt.Actor for the generic device tool. All file and
// exec operations are confined to <root>/<channel-id>/ — one workspace
// subdirectory per channel, created on first use.
type Actor struct {
	writer  harness.Writer
	actorID actor.ActorID
	root    string
	clock   func() time.Time
	logger  *slog.Logger
}

// NewActor constructs a device Actor. id is the full actor id
// (device:<name>); root is the workspace root directory.
func NewActor(writer harness.Writer, id actor.ActorID, root string, logger *slog.Logger) *Actor {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Actor{
		writer:  writer,
		actorID: id,
		root:    root,
		clock:   time.Now,
		logger:  logger,
	}
}

// Receive dispatches by env.Type. Unknown types are rejected with a failed
// terminal so the caller observes a definite outcome instead of waiting for
// the framework timer.
func (a *Actor) Receive(ctx context.Context, env *message.Envelope) error {
	switch env.Type {
	case TypeExec:
		return a.handleExec(ctx, env)
	case TypeFileRead:
		return a.handleFileRead(ctx, env)
	case TypeFileWrite:
		return a.handleFileWrite(ctx, env)
	case TypeFileEdit:
		return a.handleFileEdit(ctx, env)
	case introspect.QueryDescribe:
		return a.handleDescribe(ctx, env)
	}
	return a.fail(ctx, env, "type_unsupported", fmt.Sprintf("device actor does not handle %s", env.Type))
}

var _ actorrt.Actor = (*Actor)(nil)

// channelWorkspace resolves (and lazily creates) the per-channel workspace
// directory for the envelope's channel.
func (a *Actor) channelWorkspace(chID channel.ID) (string, error) {
	if chID == "" {
		return "", errors.New("envelope has no channel id")
	}
	ws := filepath.Join(a.root, string(chID))
	if err := os.MkdirAll(ws, 0o755); err != nil {
		return "", fmt.Errorf("create workspace: %w", err)
	}
	return ws, nil
}

// resolvePath confines a caller-supplied relative path to the workspace.
// Absolute paths and any path escaping the workspace are rejected.
func resolvePath(workspace, p string) (string, error) {
	if p == "" {
		return "", errors.New("path required")
	}
	if filepath.IsAbs(p) {
		return "", errors.New("path must be relative to the workspace")
	}
	full := filepath.Clean(filepath.Join(workspace, p))
	if full != workspace && !strings.HasPrefix(full, workspace+string(os.PathSeparator)) {
		return "", errors.New("path escapes the workspace")
	}
	return full, nil
}

func (a *Actor) sender() message.Sender {
	return message.Sender{Kind: actor.KindTool, ID: a.actorID}
}

// respond commits a status=completed final with the given result payload.
func (a *Actor) respond(ctx context.Context, env *message.Envelope, result any) error {
	_, err := behavior.RespondJSON(ctx, a.writer, a.clock, env, a.sender(), result)
	if err != nil {
		a.logger.Warn("device.respond.error",
			"request_id", string(env.ID), "type", env.Type, "err", err.Error())
	}
	return err
}

// fail closes the request with the conventional {error_code, detail} failure.
func (a *Actor) fail(ctx context.Context, env *message.Envelope, errorCode, detail string) error {
	_, err := behavior.Fail(ctx, a.writer, a.clock, env, a.sender(), errorCode, detail)
	if err != nil {
		a.logger.Error("device.fail.respond.error",
			"request_id", string(env.ID), "error_code", errorCode, "err", err)
	}
	return err
}
