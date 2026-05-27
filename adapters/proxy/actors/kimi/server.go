package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
)

const (
	DefaultWSPort         = 10086
	DefaultWSFallbackPort = 10089
	wsListenHost          = "127.0.0.1"
	wsEndpointPath        = "/ws"

	defaultPingInterval    = 5 * time.Second
	defaultMissedPongLimit = 3
	defaultWriteTimeout    = 5 * time.Second
)

var (
	ErrExtensionDisconnected = errors.New("kimi chrome extension disconnected")
	ErrServerClosed          = errors.New("kimi ws server closed")
	ErrUnknownTool           = errors.New("unknown kimi tool")
)

type ToolError struct {
	Message string
}

func (e *ToolError) Error() string {
	if e == nil || e.Message == "" {
		return "kimi tool failed"
	}
	return e.Message
}

type Server struct {
	listener   net.Listener
	httpServer *http.Server
	addr       string
	port       int

	requestID       func() string
	pingInterval    time.Duration
	missedPongLimit int

	mu               sync.Mutex
	session          *serverSession
	extensionVersion string
	pending          map[string]chan toolResult

	closed    chan struct{}
	closeOnce sync.Once
}

type serverOptions struct {
	Host              string
	Port              int
	FallbackPort      int
	PingInterval      time.Duration
	MissedPongLimit   int
	RequestID         func() string
	ReadHeaderTimeout time.Duration
}

type serverSession struct {
	conn *websocket.Conn

	writeMu sync.Mutex
	mu      sync.Mutex
	missed  int

	closed chan struct{}
	once   sync.Once
}

type wsFrame struct {
	Type                string          `json:"type"`
	RequestID           string          `json:"requestId,omitempty"`
	ResponseToRequestID string          `json:"responseToRequestId,omitempty"`
	Payload             json.RawMessage `json:"payload,omitempty"`
}

type toolCallPayload struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

type toolResultPayload struct {
	Data  json.RawMessage `json:"data"`
	Error string          `json:"error"`
}

type helloPayload struct {
	ExtensionVersion string `json:"extensionVersion"`
}

type toolResult struct {
	data json.RawMessage
	err  error
}

func StartServer(ctx context.Context, opts serverOptions) (*Server, error) {
	host := strings.TrimSpace(opts.Host)
	if host == "" {
		host = wsListenHost
	}
	port := opts.Port
	fallbackPort := opts.FallbackPort
	if fallbackPort == 0 {
		fallbackPort = DefaultWSFallbackPort
	}
	ln, boundPort, err := listenWithFallback(host, port, fallbackPort)
	if err != nil {
		return nil, err
	}
	requestID := opts.RequestID
	if requestID == nil {
		requestID = func() string { return "kimi-" + uuid.NewString() }
	}
	pingInterval := opts.PingInterval
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	missedPongLimit := opts.MissedPongLimit
	if missedPongLimit <= 0 {
		missedPongLimit = defaultMissedPongLimit
	}

	s := &Server{
		listener:        ln,
		addr:            ln.Addr().String(),
		port:            boundPort,
		requestID:       requestID,
		pingInterval:    pingInterval,
		missedPongLimit: missedPongLimit,
		pending:         map[string]chan toolResult{},
		closed:          make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc(wsEndpointPath, s.handleWS)
	readHeaderTimeout := opts.ReadHeaderTimeout
	if readHeaderTimeout <= 0 {
		readHeaderTimeout = 5 * time.Second
	}
	s.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = s.Shutdown(context.Background())
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(shutdownCtx)
	}()
	return s, nil
}

func (s *Server) Addr() string {
	if s == nil {
		return ""
	}
	return s.addr
}

func (s *Server) Port() int {
	if s == nil {
		return 0
	}
	return s.port
}

func (s *Server) HasConnectedExtension() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	sess := s.session
	s.mu.Unlock()
	return sess != nil && !sess.isClosed()
}

func (s *Server) ExtensionVersion() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.extensionVersion
}

func (s *Server) CallTool(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	name = strings.TrimSpace(name)
	if !isKimiToolName(name) {
		return nil, fmt.Errorf("%w: %s", ErrUnknownTool, name)
	}
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}

	requestID := s.requestID()
	wait := make(chan toolResult, 1)
	s.mu.Lock()
	select {
	case <-s.closed:
		s.mu.Unlock()
		return nil, ErrServerClosed
	default:
	}
	sess := s.session
	if sess == nil || sess.isClosed() {
		s.mu.Unlock()
		return nil, ErrExtensionDisconnected
	}
	s.pending[requestID] = wait
	s.mu.Unlock()
	defer s.removePending(requestID)

	payload, err := json.Marshal(toolCallPayload{Name: name, Args: args})
	if err != nil {
		return nil, err
	}
	writeCtx, cancel := context.WithTimeout(ctx, defaultWriteTimeout)
	defer cancel()
	if err := sess.writeJSON(writeCtx, wsFrame{
		Type:      "tool_call",
		RequestID: requestID,
		Payload:   payload,
	}); err != nil {
		return nil, fmt.Errorf("write tool_call: %w", err)
	}

	select {
	case got := <-wait:
		if got.err != nil {
			return nil, got.err
		}
		if len(got.data) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return got.data, nil
	case <-sess.closed:
		return nil, ErrExtensionDisconnected
	case <-s.closed:
		return nil, ErrServerClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var shutdownErr error
	s.closeOnce.Do(func() {
		close(s.closed)
		s.mu.Lock()
		sess := s.session
		s.session = nil
		s.extensionVersion = ""
		pending := s.pending
		s.pending = map[string]chan toolResult{}
		s.mu.Unlock()

		if sess != nil {
			sess.close()
		}
		failPending(pending, ErrServerClosed)
		if s.httpServer != nil {
			shutdownErr = s.httpServer.Shutdown(ctx)
		}
	})
	return shutdownErr
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	conn.SetReadLimit(8 << 20)
	sess := &serverSession{conn: conn, closed: make(chan struct{})}
	s.replaceSession(sess)
	s.handleSession(r.Context(), sess)
}

func (s *Server) handleSession(ctx context.Context, sess *serverSession) {
	defer s.unregisterSession(sess, ErrExtensionDisconnected)
	keepaliveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.keepaliveLoop(keepaliveCtx, sess)

	for {
		var frame wsFrame
		if err := wsjson.Read(ctx, sess.conn, &frame); err != nil {
			return
		}
		switch frame.Type {
		case "hello":
			if err := s.handleHello(ctx, sess, frame.Payload); err != nil {
				return
			}
		case "pong":
			sess.markPong()
		case "tool_result":
			s.deliverToolResult(frame)
		default:
		}
	}
}

func (s *Server) handleHello(ctx context.Context, sess *serverSession, raw json.RawMessage) error {
	var payload helloPayload
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}
	s.mu.Lock()
	if s.session == sess {
		s.extensionVersion = strings.TrimSpace(payload.ExtensionVersion)
	}
	s.mu.Unlock()
	writeCtx, cancel := context.WithTimeout(ctx, defaultWriteTimeout)
	defer cancel()
	return sess.writeJSON(writeCtx, wsFrame{Type: "hello_ack"})
}

func (s *Server) deliverToolResult(frame wsFrame) {
	requestID := strings.TrimSpace(frame.ResponseToRequestID)
	if requestID == "" {
		return
	}
	var payload toolResultPayload
	if len(frame.Payload) > 0 {
		if err := json.Unmarshal(frame.Payload, &payload); err != nil {
			s.deliverPending(requestID, toolResult{err: err})
			return
		}
	}
	if payload.Error != "" {
		s.deliverPending(requestID, toolResult{err: &ToolError{Message: payload.Error}})
		return
	}
	data := payload.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	s.deliverPending(requestID, toolResult{data: data})
}

func (s *Server) deliverPending(requestID string, result toolResult) {
	s.mu.Lock()
	ch := s.pending[requestID]
	delete(s.pending, requestID)
	s.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- result:
	default:
	}
}

func (s *Server) replaceSession(sess *serverSession) {
	s.mu.Lock()
	prev := s.session
	pending := s.pending
	s.pending = map[string]chan toolResult{}
	s.session = sess
	s.extensionVersion = ""
	s.mu.Unlock()

	if prev != nil {
		prev.close()
	}
	failPending(pending, ErrExtensionDisconnected)
}

func (s *Server) unregisterSession(sess *serverSession, err error) {
	s.mu.Lock()
	if s.session != sess {
		s.mu.Unlock()
		sess.close()
		return
	}
	s.session = nil
	s.extensionVersion = ""
	pending := s.pending
	s.pending = map[string]chan toolResult{}
	s.mu.Unlock()

	sess.close()
	failPending(pending, err)
}

func (s *Server) removePending(requestID string) {
	s.mu.Lock()
	delete(s.pending, requestID)
	s.mu.Unlock()
}

func (s *Server) keepaliveLoop(ctx context.Context, sess *serverSession) {
	if s.pingInterval <= 0 {
		return
	}
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.closed:
			return
		case <-ticker.C:
			if missed := sess.markPing(); missed > s.missedPongLimit {
				sess.close()
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, defaultWriteTimeout)
			err := sess.writeJSON(writeCtx, wsFrame{Type: "ping"})
			cancel()
			if err != nil {
				sess.close()
				return
			}
		}
	}
}

func (s *serverSession) writeJSON(ctx context.Context, v any) error {
	select {
	case <-s.closed:
		return ErrExtensionDisconnected
	default:
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	select {
	case <-s.closed:
		return ErrExtensionDisconnected
	default:
	}
	return wsjson.Write(ctx, s.conn, v)
}

func (s *serverSession) markPing() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.missed++
	return s.missed
}

func (s *serverSession) markPong() {
	s.mu.Lock()
	s.missed = 0
	s.mu.Unlock()
}

func (s *serverSession) close() {
	s.once.Do(func() {
		close(s.closed)
		_ = s.conn.CloseNow()
	})
}

func (s *serverSession) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func failPending(pending map[string]chan toolResult, err error) {
	for _, ch := range pending {
		select {
		case ch <- toolResult{err: err}:
		default:
		}
	}
}

func listenWithFallback(host string, port, fallbackPort int) (net.Listener, int, error) {
	if port < 0 || port > 65535 {
		return nil, 0, fmt.Errorf("kimi ws listen port %d out of range", port)
	}
	if fallbackPort < 0 || fallbackPort > 65535 {
		return nil, 0, fmt.Errorf("kimi ws fallback port %d out of range", fallbackPort)
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err == nil {
		return ln, listenerPort(ln), nil
	}
	if port == 0 || !isAddrInUse(err) {
		return nil, 0, fmt.Errorf("kimi ws listen %s:%d: %w", host, port, err)
	}
	for next := fallbackPort; next <= 65535; next++ {
		ln, err = net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(next)))
		if err == nil {
			return ln, listenerPort(ln), nil
		}
		if isAddrInUse(err) {
			continue
		}
		return nil, 0, fmt.Errorf("kimi ws listen %s:%d: %w", host, next, err)
	}
	return nil, 0, fmt.Errorf("kimi ws listen: no free port from %d to 65535", fallbackPort)
}

func listenerPort(ln net.Listener) int {
	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		return addr.Port
	}
	return 0
}

func isAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE) || strings.Contains(strings.ToLower(err.Error()), "address already in use")
}
