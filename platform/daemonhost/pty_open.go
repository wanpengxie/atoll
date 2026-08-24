package daemonhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// ErrNoTerminalHost is returned when a channel has no device that could run a
// shell. It is deliberately distinct from "offline": a channel with no device
// at all is a different fact from a device that is not answering, and the door
// must say which (data-plane axiom 5: 门恒诚实回答"在哪台、为何不可达").
var ErrNoTerminalHost = errors.New("daemonhost: channel has no attached device")

// OpenPTY opens the device leg of one terminal session.
//
// It is the exact sibling of OpenHost: same carrier, same lane, same exact-
// lane tracking — only the stream kind and the header differ. Everything that
// retires a lane therefore kills terminals too, which is what makes
// "daemon 重启 / carrier 换代 → 恒即死" structural rather than a policy
// (terminal-line-design.md §4.4).
//
// daemonID may be empty: with one attached device the choice carries no
// intent, so the door picks it. With more than one it must be named, for the
// same reason system.member.create requires desired_host.
func (h *Host) OpenPTY(ctx context.Context, daemonID string, chID channel.ID, cols, rows uint16, integration bool) (io.ReadWriteCloser, error) {
	if daemonID == "" {
		attached := h.AttachedDaemons(string(chID))
		switch len(attached) {
		case 0:
			return nil, ErrNoTerminalHost
		case 1:
			daemonID = attached[0]
		default:
			// Honest physics (data-plane axiom 5): say which machines exist
			// so the caller can name one, rather than just refusing.
			return nil, fmt.Errorf("daemonhost: channel has %d devices (%s) — name one with ?device=",
				len(attached), strings.Join(attached, ", "))
		}
	}
	lane := h.currentLane(daemonID, chID)
	if lane == nil {
		return nil, ErrLaneUnavailable
	}
	conn, err := lane.carrier.wire.OpenPTY(ctx, chID, lane.stream.Gen)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrLaneUnavailable
	}
	cleanup, ok := lane.trackExchange(conn)
	if !ok {
		_ = conn.Close()
		return nil, ErrLaneUnavailable
	}
	tracked := &trackedExchange{ReadWriteCloser: conn, cleanup: cleanup}
	// The open header is generated here, never taken from the caller: the
	// browser says how big its window is and nothing else. Shell and cwd are
	// the device's own (empty = its login shell, its channel workspace),
	// mirroring the exchange leg's "header 恒由 server 在验票后自己生成".
	if err := link.WritePTYControl(tracked, link.PTYOpen{
		Cols: cols, Rows: rows, Integration: integration,
	}); err != nil {
		_ = tracked.Close()
		return nil, ErrLaneUnavailable
	}
	return tracked, nil
}
