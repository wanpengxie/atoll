package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

type streamKind string

const (
	streamControl streamKind = "control"
	streamActor   streamKind = "actor"
	streamLane    streamKind = "lane"
)

const (
	streamWriteBudget       = 10 * time.Second
	controlTaskCapacity     = 16
	controlTaskAbandonAfter = 45 * time.Second
	openAttemptCapacity     = 32
)

var (
	errLinkClosed = errors.New("link: closed")
	ErrOpenBusy   = errors.New("link: open capacity busy")
)

type streamHeader struct {
	Kind streamKind `json:"kind"`
}

func writeStreamHeader(w io.Writer, kind streamKind) error {
	return writeLaneJSON(w, streamHeader{Kind: kind})
}

type boundedConn struct {
	net.Conn
	logger      *slog.Logger
	onWriteFail func(error)
}

func (c *boundedConn) Write(payload []byte) (int, error) {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(streamWriteBudget))
	n, err := c.Conn.Write(payload)
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("link.stream_write_failed", "error", err)
		}
		if c.onWriteFail != nil {
			c.onWriteFail(err)
		}
		_ = c.Conn.Close()
	}
	return n, err
}

type yamuxLogWriter struct{ logger *slog.Logger }

func (w yamuxLogWriter) Write(payload []byte) (int, error) {
	if w.logger != nil {
		w.logger.Warn("link.carrier_library", "message", strings.TrimSpace(string(payload)))
	}
	return len(payload), nil
}

func linkYamuxConfig(loggers ...*slog.Logger) *yamux.Config {
	cfg := yamux.DefaultConfig()
	var logger *slog.Logger
	if len(loggers) != 0 {
		logger = loggers[0]
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	cfg.LogOutput = yamuxLogWriter{logger: logger}
	return cfg
}

type controlTaskPool struct {
	mu       sync.Mutex
	draining bool
	seats    chan struct{}
	wg       sync.WaitGroup
	logger   *slog.Logger
	evidence func(SessionEndReason, string, error)

	active    atomic.Int64
	abandoned atomic.Int64
	zombies   atomic.Int64
	busy      atomic.Int64
}

func newControlTaskPool(logger *slog.Logger, evidence func(SessionEndReason, string, error)) *controlTaskPool {
	return &controlTaskPool{
		seats: make(chan struct{}, controlTaskCapacity), logger: logger, evidence: evidence,
	}
}

// submit admits without waiting. A seat is held until the real goroutine
// returns, even after the caller has accounted it as abandoned.
func (p *controlTaskPool) submit(task func(), busy func()) bool {
	if p == nil || task == nil {
		return false
	}
	p.mu.Lock()
	if p.draining {
		p.mu.Unlock()
		if busy != nil {
			busy()
		}
		return false
	}
	select {
	case p.seats <- struct{}{}:
		p.wg.Add(1)
		p.active.Add(1)
		p.mu.Unlock()
	default:
		p.busy.Add(1)
		active := p.active.Load()
		zombies := p.zombies.Load()
		p.mu.Unlock()
		if p.logger != nil {
			p.logger.Warn("link.control_task_busy",
				"active", active, "zombie_seats", zombies, "busy", p.busy.Load())
		}
		if busy != nil {
			busy()
		}
		return false
	}
	go func() {
		done := make(chan struct{})
		timer := time.AfterFunc(controlTaskAbandonAfter, func() {
			p.abandoned.Add(1)
			p.zombies.Add(1)
			if p.logger != nil {
				p.logger.Warn("link.control_task_abandoned",
					"active", p.active.Load(), "zombie_seats", p.zombies.Load())
			}
			if busy != nil {
				busy()
			}
			close(done)
		})
		defer func() {
			wasZombie := !timer.Stop()
			if wasZombie {
				<-done
				p.zombies.Add(-1)
			}
			if recovered := recover(); recovered != nil && p.evidence != nil {
				p.evidence(SessionLocalFault, "control_task_panic", fmt.Errorf("panic: %v", recovered))
			}
			p.active.Add(-1)
			<-p.seats
			p.wg.Done()
		}()
		task()
	}()
	return true
}

func (p *controlTaskPool) drain(timeout time.Duration) (joined bool, abandoned int64) {
	if p == nil {
		return true, 0
	}
	p.mu.Lock()
	p.draining = true
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true, p.abandoned.Load()
	case <-time.After(timeout):
		return false, p.abandoned.Load() + p.active.Load()
	}
}

type openResult struct {
	conn net.Conn
	err  error
}

// linkSession is only the yamux mechanism. It reports evidence and closes only
// when its owner performs the ledger decision and asks it to collect the
// carrier.
type linkSession struct {
	ys *yamux.Session

	ctrlMu sync.Mutex
	ctrl   net.Conn

	onControl func([]byte)
	onActor   func(net.Conn)
	onLane    func(net.Conn)
	onProbe   func()
	evidence  func(SessionEndReason, string, error)
	logger    *slog.Logger

	controlTasks *controlTaskPool
	openSeats    chan struct{}
	openInFlight atomic.Int64
	lateClosed   atomic.Int64

	startOnce sync.Once
	closeOnce sync.Once
	closing   atomic.Bool
	workerMu  sync.Mutex
	workerWG  sync.WaitGroup
	probeMu   sync.Mutex
	probe     string
	probeAt   time.Time
}

func newLinkSession(
	ys *yamux.Session,
	onControl func([]byte),
	onActor func(net.Conn),
	onLane func(net.Conn),
	onProbe func(),
	evidence func(SessionEndReason, string, error),
	logger *slog.Logger,
) *linkSession {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ls := &linkSession{
		ys: ys, onControl: onControl, onActor: onActor, onLane: onLane,
		onProbe: onProbe, evidence: evidence, logger: logger,
		openSeats: make(chan struct{}, openAttemptCapacity),
	}
	ls.controlTasks = newControlTaskPool(logger, ls.reportEvidence)
	return ls
}

func dialLinkSession(
	ctx context.Context,
	ws *websocket.Conn,
	onControl func([]byte),
	onLane func(net.Conn),
	onProbe func(),
	evidence func(SessionEndReason, string, error),
	logger *slog.Logger,
) (*linkSession, error) {
	ys, err := yamux.Client(newWSByteStream(ws), linkYamuxConfig(logger))
	if err != nil {
		return nil, err
	}
	ls := newLinkSession(ys, onControl, nil, onLane, onProbe, evidence, logger)
	ctrl, err := ls.openTagged(ctx, streamControl)
	if err != nil {
		_ = ys.Close()
		return nil, err
	}
	ls.ctrlMu.Lock()
	ls.ctrl = ctrl
	ls.ctrlMu.Unlock()
	return ls, nil
}

func acceptLinkSession(
	ws *websocket.Conn,
	onControl func([]byte),
	onActor func(net.Conn),
	onLane func(net.Conn),
	onProbe func(),
	evidence func(SessionEndReason, string, error),
	logger *slog.Logger,
) (*linkSession, error) {
	ys, err := yamux.Server(newWSByteStream(ws), linkYamuxConfig(logger))
	if err != nil {
		return nil, err
	}
	return newLinkSession(ys, onControl, onActor, onLane, onProbe, evidence, logger), nil
}

func (ls *linkSession) reportEvidence(reason SessionEndReason, detail string, err error) {
	if ls == nil || ls.closing.Load() || ls.evidence == nil {
		return
	}
	ls.evidence(reason, detail, err)
}

func (ls *linkSession) beginWorker() bool {
	ls.workerMu.Lock()
	defer ls.workerMu.Unlock()
	if ls.closing.Load() {
		return false
	}
	ls.workerWG.Add(1)
	return true
}

func (ls *linkSession) start() {
	if ls == nil {
		return
	}
	ls.startOnce.Do(func() {
		ls.ctrlMu.Lock()
		ctrl := ls.ctrl
		ls.ctrlMu.Unlock()
		if ctrl != nil {
			if !ls.beginWorker() {
				return
			}
			go func() {
				defer ls.workerWG.Done()
				ls.readControl(ctrl)
			}()
		}
		if !ls.beginWorker() {
			return
		}
		go func() {
			defer ls.workerWG.Done()
			ls.acceptLoop()
		}()
	})
}

func (ls *linkSession) acceptLoop() {
	for {
		conn, err := ls.ys.Accept()
		if err != nil {
			if !ls.closing.Load() {
				ls.reportEvidence(SessionCarrierLost, "carrier_accept_failed", err)
			}
			return
		}
		if !ls.beginWorker() {
			_ = conn.Close()
			return
		}
		go func() {
			defer ls.workerWG.Done()
			ls.dispatch(conn)
		}()
	}
}

func (ls *linkSession) wrap(conn net.Conn, onWriteFail func(error)) net.Conn {
	return &boundedConn{Conn: conn, logger: ls.logger, onWriteFail: onWriteFail}
}

func (ls *linkSession) controlWriteFailed(err error) {
	if isConnectionWriteTimeout(err) {
		ls.reportEvidence(SessionCarrierLost, "control_connection_write_timeout", err)
		return
	}
	ls.reportEvidence(SessionSpineLost, "control_spine_write_failed", err)
}

func (ls *linkSession) dispatch(conn net.Conn) {
	var header streamHeader
	if err := readLaneJSON(conn, &header); err != nil {
		_ = conn.Close()
		return
	}
	switch header.Kind {
	case streamControl:
		conn = ls.wrap(conn, ls.controlWriteFailed)
		ls.ctrlMu.Lock()
		if ls.ctrl != nil {
			ls.ctrlMu.Unlock()
			_ = conn.Close()
			ls.reportEvidence(SessionProtocolViolation, "duplicate_control_spine", errors.New("duplicate control stream"))
			return
		}
		ls.ctrl = conn
		ls.ctrlMu.Unlock()
		ls.readControl(conn)
	case streamActor:
		if ls.onActor == nil {
			_ = conn.Close()
			return
		}
		// Actor substreams carry the per-write transport budget so a peer that
		// stops reading kills only its own stream; the failure is local and
		// never session evidence. Lane substreams are byte pumps and get NO
		// budget: their timing belongs to the initiating context alone.
		ls.onActor(ls.wrap(conn, nil))
	case streamLane:
		if ls.onLane == nil {
			_ = conn.Close()
			return
		}
		ls.onLane(conn)
	default:
		if ls.logger != nil {
			ls.logger.Warn("link.unknown_stream_kind", "kind", string(header.Kind))
		}
		_ = conn.Close()
	}
}

func (ls *linkSession) readControl(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if ls.closing.Load() {
				return
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
				errors.Is(err, net.ErrClosed) || errors.Is(err, yamux.ErrSessionShutdown) {
				ls.reportEvidence(SessionSpineLost, "control_spine_read_failed", err)
			} else {
				ls.reportEvidence(SessionProtocolViolation, "control_spine_decode_failed", err)
			}
			return
		}
		switch peekControlKind(raw) {
		case ctrlProbe:
			frame, err := decodeControl(raw)
			if err != nil || frame.Probe == nil {
				ls.reportEvidence(SessionProtocolViolation, "malformed_liveness_probe", err)
				return
			}
			reply, _ := encodeControl(controlFrame{
				Kind:       ctrlProbeReply,
				ProbeReply: &ProbeReply{Nonce: frame.Probe.Nonce},
			})
			if err := ls.sendControl(reply); err != nil {
				return
			}
			continue
		case ctrlProbeReply:
			frame, err := decodeControl(raw)
			if err != nil || frame.ProbeReply == nil {
				ls.reportEvidence(SessionProtocolViolation, "malformed_liveness_reply", err)
				return
			}
			ls.probeMu.Lock()
			matched := frame.ProbeReply.Nonce != "" && frame.ProbeReply.Nonce == ls.probe
			sentAt := ls.probeAt
			if matched {
				ls.probe = ""
				ls.probeAt = time.Time{}
			}
			ls.probeMu.Unlock()
			if matched && ls.onProbe != nil {
				if ls.logger != nil {
					ls.logger.Debug("link.session_probe_reply", "round_trip", time.Since(sentAt))
				}
				ls.onProbe()
			}
			continue
		}
		if ls.onControl == nil {
			continue
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					ls.reportEvidence(SessionLocalFault, "control_router_panic", fmt.Errorf("panic: %v", recovered))
				}
			}()
			ls.onControl(append([]byte(nil), raw...))
		}()
	}
}

func (ls *linkSession) submitControlTask(task func(), busy func()) bool {
	if ls == nil {
		if busy != nil {
			busy()
		}
		return false
	}
	return ls.controlTasks.submit(task, busy)
}

func (ls *linkSession) sendControl(payload []byte) error {
	if ls == nil {
		return errLinkClosed
	}
	ls.ctrlMu.Lock()
	defer ls.ctrlMu.Unlock()
	if ls.ctrl == nil || ls.closing.Load() {
		return errLinkClosed
	}
	buf := make([]byte, 0, len(payload)+1)
	buf = append(buf, payload...)
	buf = append(buf, '\n')
	_, err := ls.ctrl.Write(buf)
	return err
}

func (ls *linkSession) sendProbe(nonce string) error {
	if nonce == "" {
		return errors.New("link: empty probe nonce")
	}
	raw, err := encodeControl(controlFrame{Kind: ctrlProbe, Probe: &Probe{Nonce: nonce}})
	if err != nil {
		return err
	}
	ls.probeMu.Lock()
	if ls.probe != "" {
		ls.probeMu.Unlock()
		return nil
	}
	ls.probe = nonce
	ls.probeAt = time.Now()
	ls.probeMu.Unlock()
	if err := ls.sendControl(raw); err != nil {
		ls.probeMu.Lock()
		if ls.probe == nonce {
			ls.probe = ""
			ls.probeAt = time.Time{}
		}
		ls.probeMu.Unlock()
		return err
	}
	return nil
}

func (ls *linkSession) openStream(ctx context.Context) (net.Conn, error) {
	return ls.openTagged(ctx, streamActor)
}

func (ls *linkSession) openLane(ctx context.Context) (net.Conn, error) {
	return ls.openTagged(ctx, streamLane)
}

// openTagged gives the whole non-cancellable yamux open attempt to one
// background owner. Caller cancellation only abandons the wait; the worker
// still reaches one of delivery, late-close, or error while retaining its seat.
func (ls *linkSession) openTagged(ctx context.Context, kind streamKind) (net.Conn, error) {
	if ls == nil || ls.closing.Load() {
		return nil, errLinkClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case ls.openSeats <- struct{}{}:
		ls.openInFlight.Add(1)
	default:
		if ls.logger != nil {
			ls.logger.Warn("link.open_capacity_busy",
				"kind", kind, "in_flight", ls.openInFlight.Load())
		}
		return nil, ErrOpenBusy
	}
	if !ls.beginWorker() {
		<-ls.openSeats
		ls.openInFlight.Add(-1)
		return nil, errLinkClosed
	}
	result := make(chan openResult)
	callerGone := make(chan struct{})
	go func() {
		defer func() {
			<-ls.openSeats
			ls.openInFlight.Add(-1)
			ls.workerWG.Done()
		}()
		conn, err := ls.ys.Open()
		if err == nil {
			err = writeStreamHeader(conn, kind)
			if err != nil {
				_ = conn.Close()
				conn = nil
			} else {
				switch kind {
				case streamControl:
					conn = ls.wrap(conn, ls.controlWriteFailed)
				case streamActor:
					conn = ls.wrap(conn, nil)
				}
				// streamLane stays unwrapped: byte-pump timing belongs to
				// the initiating context alone.
			}
		}
		if err != nil && isConnectionWriteTimeout(err) {
			ls.reportEvidence(SessionCarrierLost, "open_stream_connection_write_timeout", err)
		}
		opened := openResult{conn: conn, err: err}
		select {
		case result <- opened:
		case <-callerGone:
			if opened.conn != nil {
				_ = opened.conn.Close()
				ls.lateClosed.Add(1)
				if ls.logger != nil {
					ls.logger.Info("link.open_late_closed",
						"kind", kind, "late_closed", ls.lateClosed.Load())
				}
			}
		}
	}()
	select {
	case opened := <-result:
		return opened.conn, opened.err
	case <-ctx.Done():
		close(callerGone)
		return nil, ctx.Err()
	}
}

func isConnectionWriteTimeout(err error) bool {
	if errors.Is(err, yamux.ErrConnectionWriteTimeout) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (ls *linkSession) drainControlTasks(timeout time.Duration) (bool, int64) {
	if ls == nil {
		return true, 0
	}
	return ls.controlTasks.drain(timeout)
}

func (ls *linkSession) closeCarrier() error {
	if ls == nil || ls.ys == nil {
		return nil
	}
	var closeErr error
	ls.closeOnce.Do(func() {
		ls.workerMu.Lock()
		ls.closing.Store(true)
		ls.workerMu.Unlock()
		closeErr = ls.ys.Close()
	})
	return closeErr
}

func (ls *linkSession) waitWorkers(timeout time.Duration) bool {
	if ls == nil {
		return true
	}
	done := make(chan struct{})
	go func() {
		ls.workerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (ls *linkSession) closed() <-chan struct{} {
	if ls == nil || ls.ys == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return ls.ys.CloseChan()
}

func (ls *linkSession) openCounts() (inFlight, lateClosed int64) {
	if ls == nil {
		return 0, 0
	}
	return ls.openInFlight.Load(), ls.lateClosed.Load()
}
