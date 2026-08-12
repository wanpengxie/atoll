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
	ProtocolVersion           = 3
	StreamHeaderReadTimeout   = 30 * time.Second
	LaneRPCTimeout            = 20 * time.Second
	streamWriteBudget         = 10 * time.Second
	maxStreamHeaderBytes      = 64 << 10
	maxControlFrameBytes      = 1 << 24
	maxStreamAdmissionWorkers = 64
	maxStreamOpenAttempts     = 32
)

var (
	errLinkClosed           = errors.New("link: closed")
	ErrProtocolVersion      = errors.New("link: unsupported carrier protocol")
	ErrLaneRPCTimeout       = errors.New("link: lane RPC timeout")
	errControlFrameTooLarge = errors.New("link: control frame too large")
	errOpenBusy             = errors.New("link: open capacity busy")
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
	failed  atomic.Bool
	// budget overrides streamWriteBudget in tests.
	budget time.Duration
}

func newActorStreamConn(conn net.Conn, logger *slog.Logger) *actorStreamConn {
	return &actorStreamConn{Conn: conn, logger: logger}
}

func wakeActorStreamReader(conn net.Conn) {
	if conn != nil {
		_ = conn.SetReadDeadline(time.Unix(1, 0))
	}
}

func failActorStream(conn net.Conn) {
	if owner, ok := conn.(*actorStreamConn); ok {
		owner.failed.Store(true)
		wakeActorStreamReader(owner.Conn)
		return
	}
	wakeActorStreamReader(conn)
}

func (c *actorStreamConn) Write(payload []byte) (int, error) {
	if c == nil || c.failed.Load() {
		return 0, errLinkClosed
	}
	c.writeMu.Lock()
	if c.failed.Load() {
		c.writeMu.Unlock()
		return 0, errLinkClosed
	}
	budget := c.budget
	if budget <= 0 {
		budget = streamWriteBudget
	}
	_ = c.Conn.SetWriteDeadline(time.Now().Add(budget))
	n, err := c.Conn.Write(payload)
	_ = c.Conn.SetWriteDeadline(time.Time{})
	if err != nil {
		c.failed.Store(true)
	}
	c.writeMu.Unlock()
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("link.actor_stream_write_failed", "error", err)
		}
		// The established actor reader is this stream's physical supervisor.
		// Wake it after publishing the failed level; it owns Close, which may
		// wait for yamux's connection write timeout.
		wakeActorStreamReader(c.Conn)
	}
	return n, err
}

func (c *actorStreamConn) Close() error {
	if c == nil {
		return nil
	}
	c.failed.Store(true)
	return c.Conn.Close()
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
	// DeviceStreamStorage is a lane's storage sibling: the stream that carries
	// the server's file metadata instructions and nothing else. It is
	// its own stream because its consumer is the one reader that legitimately
	// blocks — those instructions end in filesystem syscalls no context can
	// recall — and a queue must ride a single physical dependency: a dead disk
	// may freeze the storage stream, never the plan traffic beside it. It
	// carries its lane's generation and has no identity of its own: it is
	// admitted only against that exact lane and dies with it.
	DeviceStreamStorage DeviceStreamKind = "storage"
	DeviceStreamActor   DeviceStreamKind = "actor"
	// DeviceStreamExchange carries one file-byte redemption. It is an exact-
	// lane child and therefore always carries channel plus lane generation.
	DeviceStreamExchange DeviceStreamKind = "exchange"
)

type DeviceStreamHeader struct {
	Kind         DeviceStreamKind `json:"kind"`
	ProtoVersion int              `json:"proto_version,omitempty"`
	Channel      channel.ID       `json:"channel,omitempty"`
	ChannelName  string           `json:"channel_name,omitempty"`
	LaneGen      LaneGeneration   `json:"lane_gen,omitempty"`
}

func (h DeviceStreamHeader) Validate() error {
	switch h.Kind {
	case DeviceStreamCarrier:
		if h.ProtoVersion <= 0 || h.Channel != "" || h.ChannelName != "" || h.LaneGen != "" {
			return errors.New("link: malformed carrier stream header")
		}
	case DeviceStreamLaneControl:
		if h.ProtoVersion != 0 || h.Channel == "" || h.LaneGen == "" || h.ChannelName == "" {
			return fmt.Errorf("link: malformed %s stream header", h.Kind)
		}
	case DeviceStreamStorage, DeviceStreamActor, DeviceStreamExchange:
		if h.ProtoVersion != 0 || h.Channel == "" || h.ChannelName != "" || h.LaneGen == "" {
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

// SpineFrame is the closed carrier-spine vocabulary.
//
// Compartment existence is NOT a command here: the device converges its own
// compartment set onto a complete authoritative snapshot it pulls, exactly as
// actor bodies converge onto AcceptFullDesired. The spine therefore carries a
// contentless poke plus one request/reply pair, and never a per-coordinate
// teardown command, a device-state report, or an acknowledgement.
type SpineFrame struct {
	Kind       string            `json:"kind"`
	DaemonID   string            `json:"daemon_id,omitempty"`
	CarrierGen CarrierGeneration `json:"carrier_gen,omitempty"`
	Class      CarrierClass      `json:"class,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	Nonce      string            `json:"nonce,omitempty"`

	// Serve/Unknown carry the compartment plan reply. Every channel the space
	// directory enumerated lands in exactly one of them; a channel in neither
	// no longer exists and the device retires its compartment. The server only
	// answers when it can enumerate the directory completely — a partial
	// snapshot would make the device tear down compartments that must live.
	Serve   []channel.ID `json:"serve,omitempty"`
	Unknown []channel.ID `json:"unknown,omitempty"`
}

func (f SpineFrame) Validate() error {
	switch f.Kind {
	case SpineCarrierAccept:
		if f.DaemonID == "" || f.CarrierGen == "" || f.Class != "" ||
			f.Reason != "" || f.Nonce != "" || len(f.Serve) > 0 || len(f.Unknown) > 0 {
			return errors.New("link: malformed carrier_accept")
		}
	case SpineCarrierReject:
		if (f.Class != CarrierTerminal && f.Class != CarrierRetryable) || f.Reason == "" ||
			f.DaemonID != "" || f.CarrierGen != "" || f.Nonce != "" ||
			len(f.Serve) > 0 || len(f.Unknown) > 0 {
			return errors.New("link: malformed carrier_reject")
		}
	case SpineCompartmentPlanPoke:
		if f.DaemonID != "" || f.CarrierGen != "" || f.Class != "" ||
			f.Reason != "" || f.Nonce != "" || len(f.Serve) > 0 || len(f.Unknown) > 0 {
			return errors.New("link: compartment_plan_poke carries no payload")
		}
	case SpineCompartmentPlanPull:
		if f.Nonce == "" || f.DaemonID != "" || f.CarrierGen != "" ||
			f.Class != "" || f.Reason != "" || len(f.Serve) > 0 || len(f.Unknown) > 0 {
			return errors.New("link: compartment_plan_pull nonce required")
		}
	case SpineCompartmentPlanReply:
		if f.Nonce == "" || f.DaemonID != "" || f.CarrierGen != "" ||
			f.Class != "" || f.Reason != "" {
			return errors.New("link: malformed compartment_plan_reply")
		}
		if err := validatePlanSet(f.Serve, f.Unknown); err != nil {
			return err
		}
	case SpineProbe, SpineProbeReply:
		if f.Nonce == "" || f.DaemonID != "" || f.CarrierGen != "" ||
			f.Class != "" || f.Reason != "" || len(f.Serve) > 0 || len(f.Unknown) > 0 {
			return errors.New("link: probe nonce required")
		}
	default:
		return fmt.Errorf("link: unknown spine frame kind %q", f.Kind)
	}
	return nil
}

// validatePlanSet holds the snapshot to the one property the device relies on
// when it retires a compartment: every named channel appears exactly once, so
// "named in neither list" unambiguously means the channel no longer exists.
func validatePlanSet(serve, unknown []channel.ID) error {
	seen := make(map[channel.ID]struct{}, len(serve)+len(unknown))
	for _, list := range [][]channel.ID{serve, unknown} {
		for _, id := range list {
			if id == "" {
				return errors.New("link: compartment plan names an empty channel")
			}
			if _, dup := seen[id]; dup {
				return fmt.Errorf("link: compartment plan names %q twice", id)
			}
			seen[id] = struct{}{}
		}
	}
	return nil
}

// LaneFrame is the closed lane-control vocabulary. Lane lifecycle is never
// represented here: the stream itself is the lane.
type LaneFrame struct {
	Kind      string `json:"kind"`
	RequestID string `json:"request_id,omitempty"`

	PlanReply   *PlanReply   `json:"plan_reply,omitempty"`
	FileRequest *FileRequest `json:"file_request,omitempty"`
	FileReply   *FileReply   `json:"file_reply,omitempty"`
}

const (
	LanePlanPull    = "plan_pull"
	LanePlanReply   = "plan_reply"
	LanePlanPoke    = "plan_poke"
	LaneFileRequest = "file_request"
	LaneFileReply   = "file_reply"
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
	case LaneFileRequest:
		if f.FileRequest == nil || payloads != 1 {
			return errors.New("link: file_request payload required")
		}
		return validateRequestID(f.RequestID, f.FileRequest.RequestID, f.FileRequest.validate())
	case LaneFileReply:
		if f.FileReply == nil || payloads != 1 {
			return errors.New("link: file_reply payload required")
		}
		return validateRequestID(f.RequestID, f.FileReply.RequestID, f.FileReply.validate())
	default:
		return fmt.Errorf("link: unknown lane frame kind %q", f.Kind)
	}
}

func (f LaneFrame) payloadCount() int {
	count := 0
	for _, present := range []bool{
		f.PlanReply != nil, f.FileRequest != nil, f.FileReply != nil,
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
	SpineCarrierAccept        = "carrier_accept"
	SpineCarrierReject        = "carrier_reject"
	SpineCompartmentPlanPoke  = "compartment_plan_poke"
	SpineCompartmentPlanPull  = "compartment_plan_pull"
	SpineCompartmentPlanReply = "compartment_plan_reply"
	SpineProbe                = "probe"
	SpineProbeReply           = "probe_reply"
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

	streamWorkerMu    sync.Mutex
	streamWorkers     sync.WaitGroup
	streamWorkerSlots chan struct{}

	// Outbound counterpart of streamWorkerSlots: how many stream opens this
	// carrier may have in flight at once. openInFlight is the seat count made
	// readable, so a carrier at its ceiling is visible rather than merely slow.
	streamOpenSlots chan struct{}
	openInFlight    atomic.Int64
}

func newRawCarrier(session *yamux.Session, spine net.Conn, logger *slog.Logger) *rawCarrier {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	c := &rawCarrier{
		session: session, spine: spine,
		spineDecoder: newBoundedJSONDecoder(spine, maxControlFrameBytes), logger: logger,
		done:              make(chan struct{}),
		streamWorkerSlots: make(chan struct{}, maxStreamAdmissionWorkers),
		streamOpenSlots:   make(chan struct{}, maxStreamOpenAttempts),
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
	// A seat is taken before the worker starts and released by the worker, never
	// by this call. Caller cancellation only abandons the wait: the library open
	// the worker started is still in flight and may still produce a stream, so
	// releasing on return would make the ceiling a fiction.
	//
	// A full pool is refused outright rather than queued. Queueing would move
	// the goroutines from inside the library to a waiting line without reducing
	// their number, and both callers already converge by retry — the device
	// retries its outbound slot, the host reopens the lane on its next scan.
	select {
	case c.streamOpenSlots <- struct{}{}:
		c.openInFlight.Add(1)
	default:
		c.logger.Warn("link.open_capacity_busy",
			"kind", header.Kind, "channel", header.Channel,
			"in_flight", c.openInFlight.Load(), "capacity", maxStreamOpenAttempts)
		return nil, errOpenBusy
	}
	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result)
	go func() {
		defer func() {
			c.openInFlight.Add(-1)
			<-c.streamOpenSlots
		}()
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
	select {
	case c.streamWorkerSlots <- struct{}{}:
		c.streamWorkers.Add(1)
		return true
	default:
		return false
	}
}

func (c *rawCarrier) endStreamWorker() {
	<-c.streamWorkerSlots
	c.streamWorkers.Done()
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
			if c.sealed.Load() {
				c.streamWorkers.Wait()
				return errLinkClosed
			}
			// Refusing is correct; refusing silently is not. A carrier sitting
			// at its admission ceiling looks exactly like a slow peer from the
			// outside, and the outbound ceiling says so too.
			c.logger.Warn("link.stream_admission_busy",
				"capacity", maxStreamAdmissionWorkers)
			continue
		}
		go func() {
			defer c.endStreamWorker()
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

func (c *ServerCarrier) OpenLane(ctx context.Context, chID channel.ID, channelName string, generation LaneGeneration) (*LaneStream, error) {
	conn, err := c.open(ctx, DeviceStreamHeader{
		Kind: DeviceStreamLaneControl, Channel: chID, ChannelName: channelName, LaneGen: generation,
	})
	if err != nil {
		return nil, err
	}
	return newLaneStream(c.rawCarrier, chID, channelName, generation, conn), nil
}

// OpenStorage opens the storage sibling of an admitted lane, keyed by that
// lane's generation. The device opens it — the same direction as actor
// streams — because the device is the only side that knows the moment
// generation G became its current lane, so the pair cannot race: by the time
// this is called, the server's lane G already exists or was already retired,
// and the server's admission answers exactly and finally either way.
func (c *ClientCarrier) OpenStorage(ctx context.Context, chID channel.ID, generation LaneGeneration) (*LaneStream, error) {
	conn, err := c.open(ctx, DeviceStreamHeader{
		Kind: DeviceStreamStorage, Channel: chID, LaneGen: generation,
	})
	if err != nil {
		return nil, err
	}
	return newLaneStream(c.rawCarrier, chID, "", generation, conn), nil
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

func (c *ClientCarrier) OpenExchange(ctx context.Context, chID channel.ID, generation LaneGeneration) (net.Conn, error) {
	return c.open(ctx, DeviceStreamHeader{Kind: DeviceStreamExchange, Channel: chID, LaneGen: generation})
}

func (c *ServerCarrier) OpenExchange(ctx context.Context, chID channel.ID, generation LaneGeneration) (net.Conn, error) {
	return c.open(ctx, DeviceStreamHeader{Kind: DeviceStreamExchange, Channel: chID, LaneGen: generation})
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
	carrier     *rawCarrier
	Channel     channel.ID
	ChannelName string
	Gen         LaneGeneration
	conn        net.Conn
	decoder     *boundedJSONDecoder

	sendMu       sync.Mutex
	retired      atomic.Bool
	retire       sync.Once
	collect      sync.Once
	done         chan struct{}
	physicalDone chan struct{}
	onRetire     func(*LaneStream)
}

func newLaneStream(carrier *rawCarrier, chID channel.ID, channelName string, generation LaneGeneration, conn net.Conn) *LaneStream {
	return &LaneStream{
		carrier: carrier, Channel: chID, ChannelName: channelName, Gen: generation, conn: conn,
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
	return newLaneStream(carrier.rawCarrier, header.Channel, header.ChannelName, header.LaneGen, conn), nil
}

// AdoptStorage wraps a device-opened storage stream on the server side.
func AdoptStorage(carrier *ServerCarrier, header DeviceStreamHeader, conn net.Conn) (*LaneStream, error) {
	if carrier == nil || conn == nil || header.Validate() != nil ||
		header.Kind != DeviceStreamStorage {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, errors.New("link: malformed storage stream")
	}
	return newLaneStream(carrier.rawCarrier, header.Channel, "", header.LaneGen, conn), nil
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
		close(l.done)
		// A yamux local Close does not wake this side's blocked Read. The
		// reader is the lane's physical supervisor, so retire only breaks that
		// Read; the reader itself performs CollectPhysical after it wakes.
		_ = l.conn.SetReadDeadline(time.Unix(1, 0))
		// The injected callback runs last: everything above must happen even
		// if it panics or parks, because sync.Once never re-runs its body, and
		// a retire whose done never closed strands every waiter forever.
		// rawCarrier.Close orders its own finishers the same way.
		if l.onRetire != nil {
			l.onRetire(l)
		}
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
