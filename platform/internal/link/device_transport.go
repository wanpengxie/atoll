package link

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	ProtocolVersion         = 3
	StreamHeaderReadTimeout = 30 * time.Second
	LaneRPCTimeout          = 20 * time.Second
	streamWriteBudget       = 10 * time.Second
	maxStreamHeaderBytes    = 64 << 10
	maxControlFrameBytes    = 1 << 24
)

var (
	errLinkClosed           = errors.New("link: closed")
	ErrProtocolVersion      = errors.New("link: unsupported carrier protocol")
	ErrLaneRPCTimeout       = errors.New("link: lane RPC timeout")
	errControlFrameTooLarge = errors.New("link: control frame too large")
)

func writeStreamJSON(w io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > maxStreamHeaderBytes {
		return fmt.Errorf("link: stream header too large: %d > %d", len(payload), maxStreamHeaderBytes)
	}
	payload = append(payload, '\n')
	if deadline, ok := w.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = deadline.SetWriteDeadline(time.Now().Add(streamWriteBudget))
		defer deadline.SetWriteDeadline(time.Time{})
	}
	_, err = w.Write(payload)
	return err
}

func marshalControlJSON(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxControlFrameBytes {
		return nil, fmt.Errorf("%w: %d > %d", errControlFrameTooLarge, len(payload), maxControlFrameBytes)
	}
	return append(payload, '\n'), nil
}

func readStreamJSON(r io.Reader, value any) error {
	if deadline, ok := r.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = deadline.SetReadDeadline(time.Now().Add(StreamHeaderReadTimeout))
		defer deadline.SetReadDeadline(time.Time{})
	}
	var payload []byte
	one := make([]byte, 1)
	for {
		n, err := r.Read(one)
		if n == 1 {
			if one[0] == '\n' {
				return decodeStrictJSON(payload, value)
			}
			payload = append(payload, one[0])
			if len(payload) > maxStreamHeaderBytes {
				return errors.New("link: stream header too large")
			}
		}
		if err != nil {
			return err
		}
	}
}

func decodeStrictJSON(payload []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("link: trailing JSON frame data")
	}
	return nil
}

// boundedJSONDecoder owns one newline-delimited control stream. It never lets
// json.Decoder read ahead across frame boundaries and never retains more than
// max bytes of attacker-controlled input for one frame.
type boundedJSONDecoder struct {
	reader *bufio.Reader
	max    int
}

func newBoundedJSONDecoder(reader io.Reader, max int) *boundedJSONDecoder {
	return &boundedJSONDecoder{reader: bufio.NewReader(reader), max: max}
}

func (d *boundedJSONDecoder) Decode(value any) error {
	if d == nil || d.reader == nil || d.max <= 0 {
		return errors.New("link: invalid control decoder")
	}
	payload := make([]byte, 0, min(d.max, 4<<10))
	for {
		fragment, err := d.reader.ReadSlice('\n')
		complete := len(fragment) > 0 && fragment[len(fragment)-1] == '\n'
		if complete {
			fragment = fragment[:len(fragment)-1]
		}
		if len(fragment) > d.max-len(payload) {
			return fmt.Errorf("%w: limit %d", errControlFrameTooLarge, d.max)
		}
		payload = append(payload, fragment...)
		if complete {
			return decodeStrictJSON(payload, value)
		}
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return err
		}
	}
}

// actorStreamConn is the actor stream's single write owner. Every complete IPC
// frame reaches this Write exactly once: Codec serializes the frame, then this
// owner holds the stream-wide deadline across that one transport write. A
// failed or timed-out actor write closes only this actor stream.
type actorStreamConn struct {
	net.Conn
	logger *slog.Logger

	writeMu sync.Mutex
	// budget overrides streamWriteBudget in tests.
	budget time.Duration
}

func newActorStreamConn(conn net.Conn, logger *slog.Logger) *actorStreamConn {
	return &actorStreamConn{Conn: conn, logger: logger}
}

func (c *actorStreamConn) Write(payload []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	budget := c.budget
	if budget <= 0 {
		budget = streamWriteBudget
	}
	_ = c.Conn.SetWriteDeadline(time.Now().Add(budget))
	n, err := c.Conn.Write(payload)
	_ = c.Conn.SetWriteDeadline(time.Time{})
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("link.actor_stream_write_failed", "error", err)
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
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	cfg.LogOutput = yamuxLogWriter{logger: logger}
	return cfg
}

type CarrierGeneration string
type LaneGeneration string

type DeviceStreamKind string

const (
	DeviceStreamCarrier     DeviceStreamKind = "carrier"
	DeviceStreamLaneControl DeviceStreamKind = "lane_control"
	DeviceStreamActor       DeviceStreamKind = "actor"
)

type DeviceStreamHeader struct {
	Kind         DeviceStreamKind `json:"kind"`
	ProtoVersion int              `json:"proto_version,omitempty"`
	Channel      channel.ID       `json:"channel,omitempty"`
	LaneGen      LaneGeneration   `json:"lane_gen,omitempty"`
}

func (h DeviceStreamHeader) Validate() error {
	switch h.Kind {
	case DeviceStreamCarrier:
		if h.ProtoVersion <= 0 || h.Channel != "" || h.LaneGen != "" {
			return errors.New("link: malformed carrier stream header")
		}
	case DeviceStreamLaneControl, DeviceStreamActor:
		if h.ProtoVersion != 0 || h.Channel == "" || h.LaneGen == "" {
			return fmt.Errorf("link: malformed %s stream header", h.Kind)
		}
	default:
		return fmt.Errorf("link: unknown stream kind %q", h.Kind)
	}
	return nil
}

type CarrierClass string

const (
	CarrierTerminal  CarrierClass = "terminal"
	CarrierRetryable CarrierClass = "retryable"
)

type SpineFrame struct {
	Kind       string            `json:"kind"`
	DaemonID   string            `json:"daemon_id,omitempty"`
	CarrierGen CarrierGeneration `json:"carrier_gen,omitempty"`
	Class      CarrierClass      `json:"class,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	Channel    channel.ID        `json:"channel,omitempty"`
	State      string            `json:"state,omitempty"`
	Nonce      string            `json:"nonce,omitempty"`
}

func (f SpineFrame) Validate() error {
	switch f.Kind {
	case SpineCarrierAccept:
		if f.DaemonID == "" || f.CarrierGen == "" || f.Class != "" ||
			f.Reason != "" || f.Channel != "" || f.State != "" || f.Nonce != "" {
			return errors.New("link: malformed carrier_accept")
		}
	case SpineCarrierReject:
		if (f.Class != CarrierTerminal && f.Class != CarrierRetryable) || f.Reason == "" ||
			f.DaemonID != "" || f.CarrierGen != "" || f.Channel != "" ||
			f.State != "" || f.Nonce != "" {
			return errors.New("link: malformed carrier_reject")
		}
	case SpineCompartmentState:
		if f.Channel == "" || f.DaemonID != "" || f.CarrierGen != "" ||
			f.Class != "" || f.Nonce != "" {
			return errors.New("link: compartment_state channel required")
		}
		switch f.State {
		case "building", "ready", "fault", "gone":
		default:
			return errors.New("link: invalid compartment state")
		}
	case SpineCompartmentClose:
		if f.Channel == "" || f.DaemonID != "" || f.CarrierGen != "" ||
			f.Class != "" || f.Reason != "" || f.State != "" || f.Nonce != "" {
			return errors.New("link: compartment_close channel required")
		}
	case SpineProbe, SpineProbeReply:
		if f.Nonce == "" || f.DaemonID != "" || f.CarrierGen != "" ||
			f.Class != "" || f.Reason != "" || f.Channel != "" || f.State != "" {
			return errors.New("link: probe nonce required")
		}
	default:
		return fmt.Errorf("link: unknown spine frame kind %q", f.Kind)
	}
	return nil
}

// LaneFrame is the closed lane-control vocabulary. Lane lifecycle is never
// represented here: the stream itself is the lane.
type LaneFrame struct {
	Kind      string `json:"kind"`
	RequestID string `json:"request_id,omitempty"`

	PlanReply          *PlanReply           `json:"plan_reply,omitempty"`
	AllocRequest       *AllocRequest        `json:"alloc_request,omitempty"`
	AllocReply         *AllocReply          `json:"alloc_reply,omitempty"`
	Committed          *Committed           `json:"committed,omitempty"`
	CommittedReply     *CommittedReply      `json:"committed_reply,omitempty"`
	ReclaimAck         *ReclaimAck          `json:"reclaim_ack,omitempty"`
	ReclaimAckReply    *ReclaimAckReply     `json:"reclaim_ack_reply,omitempty"`
	ReconcilePull      *ReconcilePull       `json:"reconcile_pull,omitempty"`
	ReconcilePullReply *ReconcilePullReply  `json:"reconcile_pull_reply,omitempty"`
	ReclaimRequest     *ReclaimRequest      `json:"reclaim_request,omitempty"`
	ReclaimReply       *ReclaimReply        `json:"reclaim_reply,omitempty"`
	ResolveCoord       *ResolveCoordRequest `json:"resolve_coord,omitempty"`
	ResolveCoordReply  *ResolveCoordReply   `json:"resolve_coord_reply,omitempty"`
}

const (
	LanePlanPull           = "plan_pull"
	LanePlanReply          = "plan_reply"
	LanePlanPoke           = "plan_poke"
	LaneAllocRequest       = "alloc_request"
	LaneAllocReply         = "alloc_reply"
	LaneCommitted          = "committed"
	LaneCommittedReply     = "committed_reply"
	LaneReclaimAck         = "reclaim_ack"
	LaneReclaimAckReply    = "reclaim_ack_reply"
	LaneReconcilePull      = "reconcile_pull"
	LaneReconcilePullReply = "reconcile_pull_reply"
	LaneReclaimRequest     = "reclaim_request"
	LaneReclaimReply       = "reclaim_reply"
	LaneResolveCoord       = "resolve_coord"
	LaneResolveCoordReply  = "resolve_coord_reply"
)

func PlanLaneReply(requestID string, actors []platform.PlanActor, err error) LaneFrame {
	reply := &PlanReply{Actors: actors}
	if err != nil {
		reply.Error = err.Error()
	}
	return LaneFrame{Kind: LanePlanReply, RequestID: requestID, PlanReply: reply}
}

func (f LaneFrame) Validate() error {
	payloads := f.payloadCount()
	switch f.Kind {
	case LanePlanPull:
		if payloads != 0 {
			return errors.New("link: plan_pull carries an unexpected payload")
		}
		return requiredControlField("plan_pull.request_id", f.RequestID)
	case LanePlanReply:
		if f.PlanReply == nil || payloads != 1 {
			return errors.New("link: plan_reply payload required")
		}
		if err := requiredControlField("plan_reply.request_id", f.RequestID); err != nil {
			return err
		}
		return f.PlanReply.validate()
	case LanePlanPoke:
		if f.RequestID != "" || payloads != 0 {
			return errors.New("link: malformed plan_poke")
		}
		return nil
	case LaneAllocRequest:
		if f.AllocRequest == nil || payloads != 1 {
			return errors.New("link: alloc_request payload required")
		}
		return validateRequestID(f.RequestID, f.AllocRequest.RequestID, f.AllocRequest.validate())
	case LaneAllocReply:
		if f.AllocReply == nil || payloads != 1 {
			return errors.New("link: alloc_reply payload required")
		}
		return validateRequestID(f.RequestID, f.AllocReply.RequestID, f.AllocReply.validate())
	case LaneCommitted:
		if f.Committed == nil || payloads != 1 {
			return errors.New("link: committed payload required")
		}
		return validateRequestID(f.RequestID, f.Committed.RequestID, f.Committed.validate())
	case LaneCommittedReply:
		if f.CommittedReply == nil || payloads != 1 {
			return errors.New("link: committed_reply payload required")
		}
		return validateRequestID(f.RequestID, f.CommittedReply.RequestID, f.CommittedReply.validate())
	case LaneReclaimAck:
		if f.ReclaimAck == nil || payloads != 1 {
			return errors.New("link: reclaim_ack payload required")
		}
		return validateRequestID(f.RequestID, f.ReclaimAck.RequestID, f.ReclaimAck.validate())
	case LaneReclaimAckReply:
		if f.ReclaimAckReply == nil || payloads != 1 {
			return errors.New("link: reclaim_ack_reply payload required")
		}
		return validateRequestID(f.RequestID, f.ReclaimAckReply.RequestID, f.ReclaimAckReply.validate())
	case LaneReconcilePull:
		if f.ReconcilePull == nil || payloads != 1 {
			return errors.New("link: reconcile_pull payload required")
		}
		return validateRequestID(f.RequestID, f.ReconcilePull.RequestID, f.ReconcilePull.validate())
	case LaneReconcilePullReply:
		if f.ReconcilePullReply == nil || payloads != 1 {
			return errors.New("link: reconcile_pull_reply payload required")
		}
		return validateRequestID(f.RequestID, f.ReconcilePullReply.RequestID, f.ReconcilePullReply.validate())
	case LaneReclaimRequest:
		if f.ReclaimRequest == nil || payloads != 1 {
			return errors.New("link: reclaim_request payload required")
		}
		return validateRequestID(f.RequestID, f.ReclaimRequest.RequestID, f.ReclaimRequest.validate())
	case LaneReclaimReply:
		if f.ReclaimReply == nil || payloads != 1 {
			return errors.New("link: reclaim_reply payload required")
		}
		return validateRequestID(f.RequestID, f.ReclaimReply.RequestID, f.ReclaimReply.validate())
	case LaneResolveCoord:
		if f.ResolveCoord == nil || payloads != 1 {
			return errors.New("link: resolve_coord payload required")
		}
		return validateRequestID(f.RequestID, f.ResolveCoord.RequestID, f.ResolveCoord.validate())
	case LaneResolveCoordReply:
		if f.ResolveCoordReply == nil || payloads != 1 {
			return errors.New("link: resolve_coord_reply payload required")
		}
		return validateRequestID(f.RequestID, f.ResolveCoordReply.RequestID, f.ResolveCoordReply.validate())
	default:
		return fmt.Errorf("link: unknown lane frame kind %q", f.Kind)
	}
}

func (f LaneFrame) payloadCount() int {
	count := 0
	for _, present := range []bool{
		f.PlanReply != nil, f.AllocRequest != nil, f.AllocReply != nil,
		f.Committed != nil, f.CommittedReply != nil, f.ReclaimAck != nil,
		f.ReclaimAckReply != nil, f.ReconcilePull != nil, f.ReconcilePullReply != nil,
		f.ReclaimRequest != nil, f.ReclaimReply != nil, f.ResolveCoord != nil,
		f.ResolveCoordReply != nil,
	} {
		if present {
			count++
		}
	}
	return count
}

func validateRequestID(outer, inner string, validation error) error {
	if validation != nil {
		return validation
	}
	if outer == "" || outer != inner {
		return errors.New("link: lane request ids do not match")
	}
	return nil
}

const (
	SpineCarrierAccept    = "carrier_accept"
	SpineCarrierReject    = "carrier_reject"
	SpineCompartmentState = "compartment_state"
	SpineCompartmentClose = "compartment_close"
	SpineProbe            = "probe"
	SpineProbeReply       = "probe_reply"
)

type rawCarrier struct {
	session      *yamux.Session
	spine        net.Conn
	spineDecoder *boundedJSONDecoder
	logger       *slog.Logger

	spineSend sync.Mutex
	sealed    atomic.Bool
	closeOnce sync.Once
	done      chan struct{}

	streamWorkerMu sync.Mutex
	streamWorkers  sync.WaitGroup
}

func newRawCarrier(session *yamux.Session, spine net.Conn, logger *slog.Logger) *rawCarrier {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	c := &rawCarrier{
		session: session, spine: spine,
		spineDecoder: newBoundedJSONDecoder(spine, maxControlFrameBytes), logger: logger,
		done: make(chan struct{}),
	}
	return c
}

func (c *rawCarrier) sendSpine(frame SpineFrame) error {
	if c == nil || c.sealed.Load() {
		return errLinkClosed
	}
	payload, err := marshalControlJSON(frame)
	if err != nil {
		return err
	}
	c.spineSend.Lock()
	_ = c.spine.SetWriteDeadline(time.Now().Add(streamWriteBudget))
	_, err = c.spine.Write(payload)
	_ = c.spine.SetWriteDeadline(time.Time{})
	c.spineSend.Unlock()
	if err != nil {
		_ = c.Close()
	}
	return err
}

func (c *rawCarrier) readSpine(frame *SpineFrame) error {
	return c.spineDecoder.Decode(frame)
}

func (c *rawCarrier) open(ctx context.Context, header DeviceStreamHeader) (net.Conn, error) {
	if c == nil || c.sealed.Load() {
		return nil, errLinkClosed
	}
	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result)
	go func() {
		conn, err := c.session.Open()
		opened := result{conn: conn, err: err}
		select {
		case ch <- opened:
		case <-ctx.Done():
			if conn != nil {
				_ = conn.Close()
			}
		case <-c.done:
			if conn != nil {
				_ = conn.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errLinkClosed
	case result := <-ch:
		if result.err != nil {
			return nil, result.err
		}
		if err := writeStreamJSON(result.conn, header); err != nil {
			_ = result.conn.Close()
			return nil, err
		}
		return result.conn, nil
	}
}

func (c *rawCarrier) accept() (net.Conn, DeviceStreamHeader, error) {
	conn, err := c.acceptRaw()
	if err != nil {
		return nil, DeviceStreamHeader{}, err
	}
	return c.adoptAccepted(conn)
}

func (c *rawCarrier) acceptRaw() (net.Conn, error) {
	if c == nil || c.sealed.Load() {
		return nil, errLinkClosed
	}
	return c.session.Accept()
}

func (c *rawCarrier) adoptAccepted(conn net.Conn) (net.Conn, DeviceStreamHeader, error) {
	var header DeviceStreamHeader
	if err := readStreamJSON(conn, &header); err != nil {
		_ = conn.Close()
		return nil, header, err
	}
	if header.Kind == DeviceStreamActor {
		conn = newActorStreamConn(conn, c.logger)
	}
	return conn, header, nil
}

func (c *rawCarrier) beginStreamWorker() bool {
	c.streamWorkerMu.Lock()
	defer c.streamWorkerMu.Unlock()
	if c.sealed.Load() {
		return false
	}
	c.streamWorkers.Add(1)
	return true
}

// serveStreams keeps Accept independent from per-stream header and actor
// handshake progress. Each accepted stream is parsed by a tracked worker; a
// half-open stream therefore cannot block admission of its siblings. Closing
// the carrier unblocks the workers, and this method joins all of them before
// returning to the carrier supervisor.
func (c *rawCarrier) serveStreams(handle func(net.Conn, DeviceStreamHeader)) error {
	for {
		conn, err := c.acceptRaw()
		if err != nil {
			c.streamWorkers.Wait()
			return err
		}
		if !c.beginStreamWorker() {
			_ = conn.Close()
			c.streamWorkers.Wait()
			return errLinkClosed
		}
		go func() {
			defer c.streamWorkers.Done()
			conn, header, err := c.adoptAccepted(conn)
			if err != nil {
				return
			}
			if handle == nil {
				_ = conn.Close()
				return
			}
			handle(conn, header)
		}()
	}
}

func (c *rawCarrier) Close() error {
	if c == nil {
		return nil
	}
	var err error
	c.closeOnce.Do(func() {
		c.streamWorkerMu.Lock()
		c.sealed.Store(true)
		c.streamWorkerMu.Unlock()
		close(c.done)
		if c.spine != nil {
			err = c.spine.Close()
		}
		if c.session != nil {
			err = errors.Join(err, c.session.Close())
		}
	})
	return err
}

func (c *rawCarrier) Done() <-chan struct{} {
	if c == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return c.done
}

type ServerCarrier struct{ *rawCarrier }
type ClientCarrier struct{ *rawCarrier }

func (c *ServerCarrier) SendSpine(frame SpineFrame) error  { return c.sendSpine(frame) }
func (c *ServerCarrier) ReadSpine(frame *SpineFrame) error { return c.readSpine(frame) }
func (c *ClientCarrier) SendSpine(frame SpineFrame) error  { return c.sendSpine(frame) }
func (c *ClientCarrier) ReadSpine(frame *SpineFrame) error { return c.readSpine(frame) }

func AcceptDeviceCarrier(ws *websocket.Conn, logger *slog.Logger) (*ServerCarrier, error) {
	session, err := yamux.Server(newWSByteStream(ws), linkYamuxConfig(logger))
	if err != nil {
		return nil, err
	}
	conn, err := session.Accept()
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	var header DeviceStreamHeader
	if err := readStreamJSON(conn, &header); err != nil {
		_ = conn.Close()
		_ = session.Close()
		return nil, err
	}
	if header.Kind != DeviceStreamCarrier || header.Channel != "" || header.LaneGen != "" {
		_ = conn.Close()
		_ = session.Close()
		return nil, errors.New("link: malformed carrier stream header")
	}
	if header.ProtoVersion != ProtocolVersion {
		carrier := &ServerCarrier{newRawCarrier(session, conn, logger)}
		return carrier, fmt.Errorf("%w: got %d, want %d",
			ErrProtocolVersion, header.ProtoVersion, ProtocolVersion)
	}
	return &ServerCarrier{newRawCarrier(session, conn, logger)}, nil
}

func DialDeviceCarrier(ctx context.Context, serverURL, bearer string, logger *slog.Logger) (*ClientCarrier, *http.Response, error) {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+bearer)
	ws, response, err := websocket.DefaultDialer.DialContext(ctx, serverURL, headers)
	if err != nil {
		return nil, response, err
	}
	session, err := yamux.Client(newWSByteStream(ws), linkYamuxConfig(logger))
	if err != nil {
		_ = ws.Close()
		return nil, response, err
	}
	spine, err := session.Open()
	if err != nil {
		_ = session.Close()
		return nil, response, err
	}
	if err := writeStreamJSON(spine, DeviceStreamHeader{
		Kind: DeviceStreamCarrier, ProtoVersion: ProtocolVersion,
	}); err != nil {
		_ = spine.Close()
		_ = session.Close()
		return nil, response, err
	}
	return &ClientCarrier{newRawCarrier(session, spine, logger)}, response, nil
}

func NewCarrierGeneration() CarrierGeneration {
	id, _ := uuid.NewV7()
	return CarrierGeneration(id.String())
}

func NewLaneGeneration() LaneGeneration {
	id, _ := uuid.NewV7()
	return LaneGeneration(id.String())
}

func (c *ServerCarrier) OpenLane(ctx context.Context, chID channel.ID, generation LaneGeneration) (*LaneStream, error) {
	conn, err := c.open(ctx, DeviceStreamHeader{
		Kind: DeviceStreamLaneControl, Channel: chID, LaneGen: generation,
	})
	if err != nil {
		return nil, err
	}
	return newLaneStream(c.rawCarrier, chID, generation, conn), nil
}

func (c *ClientCarrier) OpenActor(ctx context.Context, chID channel.ID, generation LaneGeneration) (net.Conn, error) {
	conn, err := c.open(ctx, DeviceStreamHeader{
		Kind: DeviceStreamActor, Channel: chID, LaneGen: generation,
	})
	if err != nil {
		return nil, err
	}
	return newActorStreamConn(conn, c.logger), nil
}

func (c *ServerCarrier) AcceptStream() (net.Conn, DeviceStreamHeader, error) {
	return c.accept()
}

func (c *ClientCarrier) AcceptStream() (net.Conn, DeviceStreamHeader, error) {
	return c.accept()
}

func (c *ServerCarrier) ServeStreams(handle func(net.Conn, DeviceStreamHeader)) error {
	return c.serveStreams(handle)
}

func (c *ClientCarrier) ServeStreams(handle func(net.Conn, DeviceStreamHeader)) error {
	return c.serveStreams(handle)
}

type LaneStream struct {
	carrier *rawCarrier
	Channel channel.ID
	Gen     LaneGeneration
	conn    net.Conn
	decoder *boundedJSONDecoder

	sendMu       sync.Mutex
	retired      atomic.Bool
	retire       sync.Once
	collect      sync.Once
	done         chan struct{}
	physicalDone chan struct{}
	onRetire     func(*LaneStream)
}

func newLaneStream(carrier *rawCarrier, chID channel.ID, generation LaneGeneration, conn net.Conn) *LaneStream {
	return &LaneStream{
		carrier: carrier, Channel: chID, Gen: generation, conn: conn,
		decoder: newBoundedJSONDecoder(conn, maxControlFrameBytes),
		done:    make(chan struct{}), physicalDone: make(chan struct{}),
	}
}

func AdoptLane(carrier *ClientCarrier, header DeviceStreamHeader, conn net.Conn) (*LaneStream, error) {
	if carrier == nil || conn == nil || header.Validate() != nil ||
		header.Kind != DeviceStreamLaneControl {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, errors.New("link: malformed lane stream")
	}
	return newLaneStream(carrier.rawCarrier, header.Channel, header.LaneGen, conn), nil
}

func (l *LaneStream) SetRetire(fn func(*LaneStream)) { l.onRetire = fn }

func (l *LaneStream) Send(value any) error {
	if l == nil || l.retired.Load() {
		return errLinkClosed
	}
	payload, err := marshalControlJSON(value)
	if err != nil {
		return err
	}
	l.sendMu.Lock()
	_ = l.conn.SetWriteDeadline(time.Now().Add(streamWriteBudget))
	_, err = l.conn.Write(payload)
	_ = l.conn.SetWriteDeadline(time.Time{})
	l.sendMu.Unlock()
	if err != nil {
		l.RetireLogical()
	}
	return err
}

func (l *LaneStream) Decode(value any) error {
	if l == nil || l.retired.Load() {
		return errLinkClosed
	}
	err := l.decoder.Decode(value)
	if err != nil {
		l.RetireLogical()
	}
	return err
}

func (l *LaneStream) RetireLogical() {
	if l == nil {
		return
	}
	l.retire.Do(func() {
		l.retired.Store(true)
		if l.onRetire != nil {
			l.onRetire(l)
		}
		close(l.done)
		// A yamux local Close does not wake this side's blocked Read. The
		// reader is the lane's physical supervisor, so retire only breaks that
		// Read; the reader itself performs CollectPhysical after it wakes.
		_ = l.conn.SetReadDeadline(time.Unix(1, 0))
	})
}

// CollectPhysical is called exactly once by the lane's existing reader after
// logical retirement. It is intentionally separate from RetireLogical: Close
// may wait for yamux's send path and is never part of a live value decision.
func (l *LaneStream) CollectPhysical() {
	if l == nil {
		return
	}
	l.collect.Do(func() {
		_ = l.conn.Close()
		close(l.physicalDone)
	})
}

func (l *LaneStream) Retired() bool                 { return l == nil || l.retired.Load() }
func (l *LaneStream) Done() <-chan struct{}         { return l.done }
func (l *LaneStream) PhysicalDone() <-chan struct{} { return l.physicalDone }
func (l *LaneStream) Conn() io.ReadWriteCloser      { return l.conn }
