package compute

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"

	"github.com/creack/pty"

	"github.com/wanpengxie/atoll/platform/internal/link"
)

// servePTY runs one terminal session on THIS machine.
//
// It lives on the device side, beside serveHostExchange, because that is where
// the shell actually is: platform/daemonhost is the SERVER's view of a device
// and is not even linked into the daemon binary. (Recorded because the first
// implementation put it there and the symptom was a terminal that connected,
// said "ready", then died with no bytes — the server had opened a stream kind
// the device did not know.)
//
// Like an exchange it is an exact-lane child: lane retirement, channel
// teardown or a carrier generation change closes it and joins it, which is
// what makes "daemon 重启 / carrier 换代 → 恒即死" structural rather than a
// policy someone has to remember. See terminal-line-design.md §4.4/§6-4.
func (m *compartmentManager) acceptPTY(carrier *link.ClientCarrier, conn net.Conn, header link.DeviceStreamHeader) {
	if err := header.Validate(); err != nil {
		_ = conn.Close()
		return
	}
	m.mu.Lock()
	cell := m.cells[string(header.Channel)]
	currentCarrier := m.carrier == carrier
	m.mu.Unlock()
	if !currentCarrier || cell == nil {
		_ = conn.Close()
		return
	}
	cell.mu.Lock()
	lane := cell.lane
	current := lane != nil && lane.stream.Gen == header.LaneGen
	cell.mu.Unlock()
	if !current {
		_ = conn.Close()
		return
	}
	cleanup, ok := lane.trackExchange(conn)
	if !ok {
		_ = conn.Close()
		return
	}
	defer cleanup()
	defer func() { _ = conn.Close() }()

	var open link.PTYOpen
	if err := link.ReadPTYControl(conn, &open); err != nil {
		return
	}
	root := lane.boundWorkspace()
	if root == "" {
		_ = link.WritePTYControl(conn, link.PTYReady{OK: false, Code: "unavailable", Detail: "workspace unavailable"})
		return
	}
	runPTY(m.ctx, conn, open, root)
}

// shellOf resolves the program to run. The door may name one; otherwise the
// device's own login shell wins, because the whole point of this line is "the
// zsh I already use" — see terminal-line-design.md §0.
func shellOf(requested string) string {
	if requested != "" {
		return requested
	}
	if sh := os.Getenv("SHELL"); sh != "" {
		return sh
	}
	for _, candidate := range []string{"/bin/zsh", "/bin/bash", "/bin/sh"} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "/bin/sh"
}

func runPTY(ctx context.Context, conn net.Conn, open link.PTYOpen, root string) {
	cwd := root
	if open.Cwd != "" {
		cwd = open.Cwd
	}
	if _, err := os.Stat(cwd); err != nil {
		cwd = root
	}
	cols, rows := open.Cols, open.Rows
	if cols == 0 {
		cols = 80
	}
	if rows == 0 {
		rows = 24
	}
	term := open.Term
	if term == "" {
		term = "xterm-256color"
	}

	shell := shellOf(open.Shell)
	// -l so the user's own rc runs: this line's premise is that it IS the
	// terminal they already use, not a sanitized one.
	cmd := exec.Command(shell, "-l")
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(),
		"TERM="+term,
		"ATOLL_TERMINAL=1",
	)
	if open.Integration {
		// The device advertises that the door wants OSC 133 marks. The rc
		// fragment is the user's to install (design §4.1) — this only tells
		// it to switch itself on, so a machine without the fragment still
		// gets a working terminal, just one whose commands do not land.
		cmd.Env = append(cmd.Env, "ATOLL_SHELL_INTEGRATION=1")
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true}

	f, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: cols, Rows: rows})
	if err != nil {
		_ = link.WritePTYControl(conn, link.PTYReady{OK: false, Code: "unavailable", Detail: err.Error()})
		return
	}
	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	if err := link.WritePTYControl(conn, link.PTYReady{OK: true, Pid: pid}); err != nil {
		_ = f.Close()
		_ = killShell(cmd)
		return
	}

	var writeMu sync.Mutex
	done := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(done) }) }

	// pty → door
	go func() {
		defer finish()
		buf := make([]byte, 32*1024)
		for {
			n, readErr := f.Read(buf)
			if n > 0 {
				writeMu.Lock()
				writeErr := link.WritePTYFrame(conn, link.PTYFrameData, buf[:n])
				writeMu.Unlock()
				if writeErr != nil {
					return
				}
			}
			if readErr != nil {
				return
			}
		}
	}()

	// door → pty
	go func() {
		defer finish()
		for {
			kind, payload, readErr := link.ReadPTYFrame(conn)
			if readErr != nil {
				return
			}
			switch kind {
			case link.PTYFrameData:
				if _, err := f.Write(payload); err != nil {
					return
				}
			case link.PTYFrameResize:
				size, err := link.DecodeResize(payload)
				if err != nil {
					continue
				}
				_ = pty.Setsize(f, &pty.Winsize{Cols: size.Cols, Rows: size.Rows})
			default:
				// Unknown frame kinds are ignored, never fatal: a door newer
				// than this device must be able to add one without killing
				// every session on an older machine.
			}
		}
	}()

	// The context is the lane's: retirement kills the shell. This is the
	// enforcement half of "carrier 换代 → 恒即死".
	select {
	case <-done:
	case <-ctx.Done():
	}

	exitCode := waitShell(cmd, f)
	writeMu.Lock()
	_ = link.WritePTYFrame(conn, link.PTYFrameExit, []byte(strconv.Itoa(exitCode)))
	writeMu.Unlock()
	_ = f.Close()
}

// killShell ends the whole foreground process group, not just the shell: a
// terminal's children (a running build) are the reason the session existed,
// and leaving them orphaned would contradict "恒不留孤儿" (§4.4).
func killShell(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	if pgid, err := syscall.Getpgid(cmd.Process.Pid); err == nil {
		_ = syscall.Kill(-pgid, syscall.SIGHUP)
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		return nil
	}
	return cmd.Process.Kill()
}

func waitShell(cmd *exec.Cmd, f *os.File) int {
	_ = f.Close()
	_ = killShell(cmd)
	err := cmd.Wait()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

var _ io.Reader = (*os.File)(nil)
