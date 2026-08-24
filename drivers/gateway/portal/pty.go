package portal

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/platform/terminal"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// The terminal leg is its own WebSocket, deliberately NOT the ledger feed's.
//
// /ws is one serialized writer pump over a single channel: a build's scrolling
// output would share that pump with every ledger frame, and a viewer that fell
// behind would stall the feed. That is the same judgment the data plane
// already made ("字节恒不进控制帧……为了背压与队头阻塞").
//
// This is a WORKAROUND, not the right shape: the device leg carries many
// stream kinds over ONE carrier because it has yamux, and the browser leg has
// no multiplexer to do the same. When it gets one these two collapse into one
// connection. Tracked in .dalek/pm/browser-leg-multiplexing-pending.md.
const ptyPath = "/pty"

const (
	ptyWriteWait  = 10 * time.Second
	ptyPongWait   = 60 * time.Second
	ptyPingPeriod = 25 * time.Second
)

type ptyClientMsg struct {
	Type string `json:"type"`
	Data string `json:"data,omitempty"`
	Cols uint16 `json:"cols,omitempty"`
	Rows uint16 `json:"rows,omitempty"`
}

func (p *Portal) ptyUpgrader() websocket.Upgrader {
	return websocket.Upgrader{
		// Same-origin discipline, identical to /ws: this endpoint
		// authenticates by cookie, so an absent Origin (curl, a native
		// client) is allowed but a cross-origin one is refused.
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true
			}
			u, err := url.Parse(origin)
			if err != nil {
				return false
			}
			return u.Host == r.Host
		},
	}
}

func (p *Portal) pty(w http.ResponseWriter, r *http.Request) {
	if p.cfg.Terminals == nil {
		writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), "terminal line unavailable")
		return
	}
	principal, ok := p.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, string(codeNotAuthenticated), "invalid session")
		return
	}
	chID := channel.ID(r.URL.Query().Get("channel_id"))
	if chID == "" {
		writeError(w, http.StatusBadRequest, string(codeInvalidArgs), "channel_id is required")
		return
	}
	// Same eligibility gate as the byte leg: membership in THIS channel, on
	// every open. The judgment is "may you open a session here", never "may
	// you run this command" — see terminal-line-design.md §7.
	caller, member, err := p.cfg.Gateway.SubjectIn(r.Context(), principal, chID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), "channel eligibility unavailable — retry")
		return
	}
	if !member {
		writeError(w, http.StatusForbidden, string(codePermissionDenied), "no eligibility for channel")
		return
	}
	// 口子恒单向 (§5): only a human opens a terminal. An agent with a pty
	// would bypass per-call authorization and per-call accounting, which is
	// the exact opposite of why this line exists.
	if !isHuman(caller) {
		writeError(w, http.StatusForbidden, string(codePermissionDenied), "only a human may open a terminal")
		return
	}

	cols := uint16(atoiDefault(r.URL.Query().Get("cols"), 80))
	rows := uint16(atoiDefault(r.URL.Query().Get("rows"), 24))
	sessionID := r.URL.Query().Get("session")

	var (
		sess   *terminal.Session
		stream <-chan []byte
	)
	if sessionID != "" {
		// Reattach within the grace window: the shell kept running, and the
		// viewer picks up the live stream from NOW. Output produced while
		// away is gone by design (§4.3-2) — nothing was buffered.
		sess, stream, err = p.cfg.Terminals.Attach(sessionID, caller)
		if err != nil {
			status := http.StatusNotFound
			code := codeNotFound
			switch {
			case errors.Is(err, terminal.ErrNotOwner):
				status, code = http.StatusForbidden, codePermissionDenied
			case errors.Is(err, terminal.ErrBusy):
				status, code = http.StatusConflict, codeInvalidArgs
			}
			writeError(w, status, string(code), err.Error())
			return
		}
	} else {
		sessionID = newSessionID()
		sess, err = p.cfg.Terminals.Open(r.Context(), sessionID, chID, caller, cols, rows)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), err.Error())
			return
		}
		sess, stream, err = p.cfg.Terminals.Attach(sessionID, caller)
		if err != nil {
			p.cfg.Terminals.Close(sessionID)
			writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), err.Error())
			return
		}
	}

	upgrader := p.ptyUpgrader()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already answered the request. Detaching (not closing) keeps
		// the shell alive for the grace window, which is what a client that
		// immediately retries needs.
		p.cfg.Terminals.Detach(sess)
		return
	}
	p.servePTY(ws, sess, stream, sessionID)
}

func (p *Portal) servePTY(ws *websocket.Conn, sess *terminal.Session, stream <-chan []byte, sessionID string) {
	defer func() {
		// Detach, NOT Close: losing the viewer恒不杀进程 (§4.4). The manager
		// starts the grace clock; if nobody returns within it, it reaps.
		p.cfg.Terminals.Detach(sess)
		_ = ws.Close()
	}()

	var writeMu sync.Mutex
	send := func(t int, b []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = ws.SetWriteDeadline(time.Now().Add(ptyWriteWait))
		return ws.WriteMessage(t, b)
	}
	// The session id goes out first so a client that lost its connection can
	// come back to the same shell.
	hello, _ := json.Marshal(map[string]string{"type": "ready", "session": sessionID})
	if err := send(websocket.TextMessage, hello); err != nil {
		return
	}

	done := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(done) }) }

	go func() {
		defer finish()
		ping := time.NewTicker(ptyPingPeriod)
		defer ping.Stop()
		for {
			select {
			case b, ok := <-stream:
				if !ok {
					return
				}
				// Terminal bytes ride as binary: they are not text and must
				// never be mangled by UTF-8 validation.
				if err := send(websocket.BinaryMessage, b); err != nil {
					return
				}
			case <-ping.C:
				writeMu.Lock()
				_ = ws.SetWriteDeadline(time.Now().Add(ptyWriteWait))
				err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(ptyWriteWait))
				writeMu.Unlock()
				if err != nil {
					return
				}
			case <-done:
				return
			}
		}
	}()

	_ = ws.SetReadDeadline(time.Now().Add(ptyPongWait))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(ptyPongWait))
	})
	defer finish()
	for {
		typ, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		_ = ws.SetReadDeadline(time.Now().Add(ptyPongWait))
		if typ == websocket.BinaryMessage {
			if err := sess.Write(data); err != nil {
				return
			}
			continue
		}
		var msg ptyClientMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "input":
			if err := sess.Write([]byte(msg.Data)); err != nil {
				return
			}
		case "resize":
			// Window size is low-frequency and needs to be reliable, so it is
			// a control message rather than an escape smuggled into the byte
			// stream (§4).
			if err := sess.Resize(msg.Cols, msg.Rows); err != nil {
				return
			}
		case "close":
			p.cfg.Terminals.Close(sessionID)
			return
		}
	}
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 || n > 1<<16-1 {
		return def
	}
	return n
}

// newSessionID mints an opaque id. It only needs to be unguessable enough that
// a session cannot be reattached by chance; ownership is checked separately
// against the caller, so this is not a bearer token.
func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

// isHuman reads the kind off a three-segment actor id (<kind>:<seed>:<stamp>).
// The kind is structural, not a claim: the runtime minted this id, so reading
// it back is not trusting the caller.
func isHuman(id actor.ActorID) bool {
	parts := strings.Split(string(id), ":")
	if len(parts) != 3 {
		return false
	}
	kind, ok := actor.ParseKind(parts[0])
	return ok && kind == actor.KindHuman
}
