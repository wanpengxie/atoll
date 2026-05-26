package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

const LocalListenHost = "127.0.0.1"

type localWebSocketAcceptor interface {
	AcceptLocalWebSocket(ctx context.Context, conn *websocket.Conn) error
}

type LocalListener struct {
	listener net.Listener
	server   *http.Server
	log      Logger
}

type localHelloFrame struct {
	FrameType string `json:"frame_type,omitempty"`
	ActorID   string `json:"actor_id,omitempty"`
}

func StartLocalListener(ctx context.Context, port int, registry *Registry, log Logger) (*LocalListener, error) {
	if registry == nil {
		return nil, fmt.Errorf("proxy local listen: registry required")
	}
	if log == nil {
		log = noopLogger{}
	}
	addr := localListenAddr(port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("proxy local listen: %s unavailable: %w; remediation: stop the process using that port or start coagent-proxy with --port <free-port>", addr, err)
	}
	ll := &LocalListener{listener: ln, log: log}
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		go ll.handleConn(ctx, registry, ws)
	})
	ll.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := ll.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("proxy local listen serve: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ll.Shutdown(shutdownCtx)
	}()
	log.Printf("proxy local listen ready: %s", ln.Addr().String())
	return ll, nil
}

func (l *LocalListener) Addr() net.Addr {
	if l == nil || l.listener == nil {
		return nil
	}
	return l.listener.Addr()
}

func (l *LocalListener) Shutdown(ctx context.Context) error {
	if l == nil || l.server == nil {
		return nil
	}
	return l.server.Shutdown(ctx)
}

func (l *LocalListener) handleConn(ctx context.Context, registry *Registry, ws *websocket.Conn) {
	var hello localHelloFrame
	_ = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := ws.ReadJSON(&hello); err != nil {
		closeLocalWS(ws, websocket.ClosePolicyViolation, "hello frame required")
		return
	}
	_ = ws.SetReadDeadline(time.Time{})
	actorID := actor.ActorID(strings.TrimSpace(hello.ActorID))
	if actorID == "" {
		closeLocalWS(ws, websocket.ClosePolicyViolation, "actor_id required")
		return
	}
	mod, ok := registry.Get(actorID)
	if !ok {
		closeLocalWS(ws, websocket.ClosePolicyViolation, "actor not enabled")
		return
	}
	acceptor, ok := mod.(localWebSocketAcceptor)
	if !ok {
		closeLocalWS(ws, websocket.ClosePolicyViolation, "actor does not accept local ws")
		return
	}
	if err := acceptor.AcceptLocalWebSocket(ctx, ws); err != nil {
		l.log.Printf("proxy local ws actor=%s closed: %v", actorID, err)
	}
}

func localListenAddr(port int) string {
	return net.JoinHostPort(LocalListenHost, strconv.Itoa(port))
}

func closeLocalWS(ws *websocket.Conn, code int, reason string) {
	raw, _ := json.Marshal(map[string]string{"error": reason})
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, string(raw)), time.Now().Add(time.Second))
	_ = ws.Close()
}
