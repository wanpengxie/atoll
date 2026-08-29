package daemonhost

import (
	"context"
	"errors"
	"io"

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
// daemonID is a registry identity. Device selection is policy, so it is
// resolved by the portal from the channel profile before reaching this live
// carrier layer; the host never guesses from whichever lanes happen to exist.
func (h *Host) OpenPTY(ctx context.Context, daemonID string, chID channel.ID, cols, rows uint16, integration bool) (io.ReadWriteCloser, error) {
	if daemonID == "" {
		return nil, ErrNoTerminalHost
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
