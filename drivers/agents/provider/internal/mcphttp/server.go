// Package mcphttp exposes one generation-scoped Streamable HTTP MCP endpoint.
// It is a transport only: execution still terminates at the worker ToolPort.
package mcphttp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/mcpcodec"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
)

const maxRequestBytes = 8 << 20

type Snapshot func() mcpcodec.InvokeFunc

type Server struct {
	listener net.Listener
	http     *http.Server
	codec    *mcpcodec.Server
	token    string
	slots    chan struct{}
	once     sync.Once
	done     chan struct{}
	logger   *slog.Logger
}

func Start(life context.Context, surface toolsurface.Surface, snapshot Snapshot, logger *slog.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		_ = listener.Close()
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{listener: listener, codec: mcpcodec.New(life, surface), token: hex.EncodeToString(tokenBytes), slots: make(chan struct{}, 16), done: make(chan struct{}), logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) { s.serve(snapshot, w, r) })
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		defer close(s.done)
		_ = s.http.Serve(listener)
	}()
	go func() {
		<-life.Done()
		s.Close()
	}()
	return s, nil
}

func (s *Server) Config() json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{
		toolsurface.ClaudeServer: map[string]any{
			"type":    "http",
			"url":     "http://" + s.listener.Addr().String() + "/mcp",
			"headers": map[string]string{"Authorization": "Bearer " + s.token},
		},
	}})
	return raw
}

func (s *Server) serve(snapshot Snapshot, w http.ResponseWriter, r *http.Request) {
	if !s.validOrigin(r.Header.Get("Origin")) {
		http.Error(w, "forbidden origin", http.StatusForbidden)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+s.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil || !json.Valid(raw) {
		http.Error(w, "invalid JSON-RPC body", http.StatusBadRequest)
		return
	}
	method := methodOf(raw)
	s.logger.Debug("claude.mcp_http", "method", method, "protocol_version", r.Header.Get("MCP-Protocol-Version"))
	if method != "notifications/cancelled" {
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		default:
			s.write(w, mcpcodec.BusyResponse(raw))
			return
		}
	}
	var invoke mcpcodec.InvokeFunc
	if snapshot != nil {
		invoke = snapshot()
	}
	response := s.codec.Handle(r.Context(), raw, invoke)
	if response == nil {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	s.write(w, response)
}

func (s *Server) validOrigin(origin string) bool {
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	return u.Host == s.listener.Addr().String()
}

func (s *Server) write(w http.ResponseWriter, response json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

func methodOf(raw json.RawMessage) string {
	var request struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal(raw, &request)
	return request.Method
}

func (s *Server) Close() {
	s.once.Do(func() {
		s.codec.Close()
		_ = s.listener.Close()
		_ = s.http.Close()
	})
}

func (s *Server) Done() <-chan struct{} { return s.done }
