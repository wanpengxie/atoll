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
	return writeStreamJSON(w, streamHeader{Kind: kind})
}

// streamHeaderReadTimeout bounds the ONE header read at the head of every
// substream (期11 review #F). Without it, a peer that opens a substream and
// then never writes its header (a half-open connection, a peer bug) wedges the
// dispatch goroutine reading it forever. Session probes prove only the control
// spine, so a stuck child still needs its own admission bound. A header is tens
// of bytes, so any healthy peer sends it near-instantly; this only ever fires on
// a genuinely stuck/half-open stream. The deadline is CLEARED the moment the
// header is read (readStreamJSON's defer), so it never bounds what follows on
// the same stream.
//
// A var (not a const) SOLELY so a test can shorten it to prove the bounded
// close without waiting out a real 30s; production never reassigns it.
var streamHeaderReadTimeout = 30 * time.Second

// writeStreamJSON / readStreamJSON are the substream header's own tiny framing:
// a single newline-terminated JSON value, exactly once per stream, before any
// other traffic. Newline-terminated (not length-prefixed) is sufficient because
// this message is sent by exactly one side at a fixed protocol step — no
// interleaving, no need for a byte-exact framer.
func writeStreamJSON(w io.Writer, v any) error {
	if dl, ok := w.(interface{ SetWriteDeadline(t time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(streamWriteBudget))
		defer func() { _ = dl.SetWriteDeadline(time.Time{}) }()
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

// readStreamJSON reads EXACTLY through the terminating '\n' and no further —
// deliberately NOT a bufio.Reader/json.Decoder wrapping r directly: both read
// AHEAD into their own internal buffer past the delimiter (a decoder has no
// length prefix to bound its read to), silently swallowing whatever follows on
// the SAME stream. A byte-at-a-time scan is the simplest way to guarantee zero
// over-read; headers here are tens of bytes, so the per-byte Read call cost is
// noise.
func readStreamJSON(r io.Reader, v any) error {
	// Bound the header read against a half-open / never-writing peer (#F). Set
	// on entry, CLEARED on return (defer) so anything following on the SAME
	// stream inherits no deadline. Only streams that carry a deadline API
	// (net.Conn / yamux stream) are bounded; a plain io.Reader (test buffer)
	// simply skips it.
	if dl, ok := r.(interface{ SetReadDeadline(t time.Time) error }); ok {
		_ = dl.SetReadDeadline(time.Now().Add(streamHeaderReadTimeout))
		defer func() { _ = dl.SetReadDeadline(time.Time{}) }()
	}
	var buf []byte
	one := make([]byte, 1)
	for {
		n, err := r.Read(one)
		if n == 1 {
			if one[0] == '\n' {
				return json.Unmarshal(buf, v)
			}
			buf = append(buf, one[0])
		}
		if err != nil {
			if err == io.EOF && len(buf) > 0 {
				return json.Unmarshal(buf, v)
			}
			return fmt.Errorf("link: read stream header: %w", err)
		}
	}
}

type boundedConn struct {
	net.Conn
	logger      *slog.Logger
	onWriteFail func(error)
	// budget overrides streamWriteBudget when non-zero (test injection).
	budget time.Duration
}

func (c *boundedConn) Write(payload []byte) (int, error) {
	budget := c.budget
	if budget == 0 {
		budget = streamWriteBudget
	}
	_ = c.Conn.SetWriteDeadline(time.Now().Add(budget))
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
	// abandonAfter overrides controlTaskAbandonAfter when non-zero (tests).
	abandonAfter time.Duration

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
		abandonAfter := p.abandonAfter
		if abandonAfter == 0 {
			abandonAfter = controlTaskAbandonAfter
		}
		done := make(chan struct{})
		timer := time.AfterFunc(abandonAfter, func() {
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
		// A zombie already counted itself into abandoned when its 45s timer
		// fired but still holds an active seat — count each task once:
		// abandoned + the stuck tasks not yet accounted (active minus
		// zombies).
		stuck := p.active.Load() - p.zombies.Load()
		if stuck < 0 {
			stuck = 0
		}
		return false, p.abandoned.Load() + stuck
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
	onProbe func(),
	evidence func(SessionEndReason, string, error),
	logger *slog.Logger,
) *linkSession {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ls := &linkSession{
		ys: ys, onControl: onControl, onActor: onActor,
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
	onProbe func(),
	evidence func(SessionEndReason, string, error),
	logger *slog.Logger,
) (*linkSession, error) {
	ys, err := yamux.Client(newWSByteStream(ws), linkYamuxConfig(logger))
	if err != nil {
		return nil, err
	}
	ls := newLinkSession(ys, onControl, nil, onProbe, evidence, logger)
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
	onProbe func(),
	evidence func(SessionEndReason, string, error),
	logger *slog.Logger,
) (*linkSession, error) {
	ys, err := yamux.Server(newWSByteStream(ws), linkYamuxConfig(logger))
	if err != nil {
		return nil, err
	}
	return newLinkSession(ys, onControl, onActor, onProbe, evidence, logger), nil
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

// Lock-ordering invariant: controlWriteFailed can run inside sendControl's
// ctrlMu hold (boundedConn.Write calls it synchronously), and reportEvidence
// resolves into beginSeal which takes the registry lock — so the only legal
// order is ctrlMu → registry.mu. Nothing under the registry lock may ever
// call sendControl or take ctrlMu.
func (ls *linkSession) controlWriteFailed(err error) {
	if isConnectionWriteTimeout(err) {
		ls.reportEvidence(SessionCarrierLost, "control_connection_write_timeout", err)
		return
	}
	if ls.carrierClosed() {
		ls.reportEvidence(SessionCarrierLost, "carrier_closed_control_write", err)
		return
	}
	ls.reportEvidence(SessionSpineLost, "control_spine_write_failed", err)
}

// carrierClosed observes the multiplexer's own close level without blocking.
func (ls *linkSession) carrierClosed() bool {
	if ls == nil || ls.ys == nil {
		return true
	}
	select {
	case <-ls.ys.CloseChan():
		return true
	default:
		return false
	}
}

func (ls *linkSession) dispatch(conn net.Conn) {
	var header streamHeader
	if err := readStreamJSON(conn, &header); err != nil {
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
		// never session evidence.
		ls.onActor(ls.wrap(conn, nil))
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
			switch {
			case errors.Is(err, yamux.ErrSessionShutdown) || ls.carrierClosed():
				// The whole carrier is gone; the control stream's EOF is a
				// symptom, not a separate spine death — attribute precisely.
				ls.reportEvidence(SessionCarrierLost, "carrier_closed_control_read", err)
			case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
				errors.Is(err, net.ErrClosed):
				ls.reportEvidence(SessionSpineLost, "control_spine_read_failed", err)
			default:
				ls.reportEvidence(SessionProtocolViolation, "control_spine_decode_failed", err)
			}
			return
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

func (ls *linkSession) handleProbe(probe *Probe) {
	reply, _ := encodeControl(controlFrame{
		Kind: ctrlProbeReply, ProbeReply: &ProbeReply{Nonce: probe.Nonce},
	})
	_ = ls.sendControl(reply)
}

func (ls *linkSession) handleProbeReply(reply *ProbeReply) {
	ls.probeMu.Lock()
	matched := reply.Nonce == ls.probe
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
