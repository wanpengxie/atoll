package portal

import (
	"context"
	"crypto/rand"
	"encoding/binary"
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

// 终端腿是一条 WebSocket——**一条**，不是一个终端一条。
//
// 它恒不与账本 feed 合并：/ws 是一条串行的写泵，一次构建的滚屏会和每一帧账本
// 共用那个泵，落后的读者会把 feed 拖住。这与数据面早就下过的同一个判断一致
// （"字节恒不进控制帧……为了背压与队头阻塞"）。
//
// 但"每开一个终端就多一条 WS"是另一回事，那恒不成立：开十个频道就是十条连接，
// 十份 ping/pong、十次握手、十份鉴权。设备腿之所以只要一条 carrier，是因为它有
// yamux；浏览器腿没有多路复用器，那就在这一条 WS 上自己做一个最小的：
//
//	客户端 → 服务端   {"type":"open","id":N,"channel_id":...,"session":...,"cols":..,"rows":..}
//	                  {"type":"resize","id":N,"cols":..,"rows":..}
//	                  {"type":"close","id":N}
//	                  二进制  [id:uint32 BE][输入字节…]
//	服务端 → 客户端   {"type":"ready","id":N,"session":...}
//	                  {"type":"error","id":N,"code":...,"detail":...}
//	                  {"type":"exit","id":N,"reason":...}
//	                  二进制  [id:uint32 BE][输出字节…]
//
// 流 id 由客户端分配：这样 open 恒不需要一个额外的 ref 字段来对答案。
const ptyPath = "/pty"

const (
	ptyWriteWait  = 10 * time.Second
	ptyPongWait   = 60 * time.Second
	ptyPingPeriod = 25 * time.Second
	// ptyStreamHeader 是二进制帧前面那 4 个字节的流 id。
	ptyStreamHeader = 4
	// ptyMaxStreams 是一条连接上同时开着的终端数上限。恒有界：一个坏掉的或者
	// 恶意的客户端恒不该能靠一条连接把节点上的 shell 开满。
	ptyMaxStreams = 32
	// ptyReplayChunk 是回放切块的大小。回放最大 MaxReplay，一次性一帧发出去会
	// 让写泵在这一帧上卡住整条连接（此刻它是所有终端共用的）。
	ptyReplayChunk = 32 << 10
)

type ptyClientMsg struct {
	Type      string `json:"type"`
	ID        uint32 `json:"id"`
	ChannelID string `json:"channel_id,omitempty"`
	Session   string `json:"session,omitempty"`
	Device    string `json:"device,omitempty"`
	Data      string `json:"data,omitempty"`
	Cols      uint16 `json:"cols,omitempty"`
	Rows      uint16 `json:"rows,omitempty"`
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
	// 握手只认"你是谁"。**"你能不能在这个频道开终端"恒是每次 open 各判各的**——
	// 一条连接现在承载多个频道的终端，握手时判一次就恒不够了。
	principal, ok := p.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, string(codeNotAuthenticated), "invalid session")
		return
	}
	upgrader := p.ptyUpgrader()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// 连接的生命周期就是这些终端的 attach 生命周期：连接一断，本连接上所有的
	// open/attach 请求恒即作废。
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	p.servePTY(ctx, ws, principal)
}

// ptyStream is one terminal riding the shared connection.
type ptyStream struct {
	id      uint32
	session *terminal.Session
	stop    chan struct{}
	once    sync.Once
}

func (s *ptyStream) halt() { s.once.Do(func() { close(s.stop) }) }

func (p *Portal) servePTY(ctx context.Context, ws *websocket.Conn, principal string) {
	var writeMu sync.Mutex
	send := func(t int, b []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = ws.SetWriteDeadline(time.Now().Add(ptyWriteWait))
		return ws.WriteMessage(t, b)
	}
	sendJSON := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return send(websocket.TextMessage, b)
	}
	sendBytes := func(id uint32, b []byte) error {
		frame := make([]byte, ptyStreamHeader+len(b))
		binary.BigEndian.PutUint32(frame[:ptyStreamHeader], id)
		copy(frame[ptyStreamHeader:], b)
		return send(websocket.BinaryMessage, frame)
	}

	var (
		mu      sync.Mutex
		streams = map[uint32]*ptyStream{}
	)
	dropAll := func() {
		mu.Lock()
		live := make([]*ptyStream, 0, len(streams))
		for _, st := range streams {
			live = append(live, st)
		}
		streams = map[uint32]*ptyStream{}
		mu.Unlock()
		for _, st := range live {
			st.halt()
			// Detach, NOT Close: losing the viewer恒不杀进程 (§4.4). The manager
			// starts the grace clock; if nobody returns within it, it reaps.
			p.cfg.Terminals.Detach(st.session)
		}
	}
	defer func() {
		dropAll()
		_ = ws.Close()
	}()

	done := make(chan struct{})
	var doneOnce sync.Once
	finish := func() { doneOnce.Do(func() { close(done) }) }
	defer finish()

	go func() {
		ping := time.NewTicker(ptyPingPeriod)
		defer ping.Stop()
		for {
			select {
			case <-ping.C:
				writeMu.Lock()
				_ = ws.SetWriteDeadline(time.Now().Add(ptyWriteWait))
				err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(ptyWriteWait))
				writeMu.Unlock()
				if err != nil {
					finish()
					return
				}
			case <-done:
				return
			}
		}
	}()

	openStream := func(msg ptyClientMsg) {
		if msg.ID == 0 {
			_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codeInvalidArgs, "detail": "stream id must be non-zero"})
			return
		}
		mu.Lock()
		_, exists := streams[msg.ID]
		count := len(streams)
		mu.Unlock()
		if exists {
			_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codeInvalidArgs, "detail": "stream id already in use"})
			return
		}
		if count >= ptyMaxStreams {
			_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codeInvalidArgs, "detail": "too many terminals on one connection"})
			return
		}
		chID := channel.ID(msg.ChannelID)
		if chID == "" {
			_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codeInvalidArgs, "detail": "channel_id is required"})
			return
		}
		// 与字节腿同一道闸：**每一次 open 都判**这个频道的成员资格。判的恒是
		// "你能不能在这里开一个会话"，恒不是"你能不能跑这条命令"——见 §7。
		caller, member, err := p.cfg.Gateway.SubjectIn(ctx, principal, chID)
		if err != nil {
			_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codeUnavailable, "detail": "channel eligibility unavailable — retry"})
			return
		}
		if !member {
			_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codePermissionDenied, "detail": "no eligibility for channel"})
			return
		}
		// 口子恒单向 (§5): only a human opens a terminal.
		if !isHuman(caller) {
			_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codePermissionDenied, "detail": "only a human may open a terminal"})
			return
		}

		cols, rows := msg.Cols, msg.Rows
		if cols == 0 {
			cols = 80
		}
		if rows == 0 {
			rows = 24
		}
		sessionID := msg.Session
		var att *terminal.Attachment
		var sess *terminal.Session
		if sessionID != "" {
			sess, att, err = p.cfg.Terminals.Attach(sessionID, chID, caller)
			if err != nil {
				code := codeNotFound
				if errors.Is(err, terminal.ErrNotOwner) {
					code = codePermissionDenied
				}
				_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": code, "detail": err.Error()})
				return
			}
		} else {
			sessionID = newSessionID()
			sess, err = p.cfg.Terminals.Open(ctx, sessionID, chID, caller, msg.Device, cols, rows)
			if err != nil {
				_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codeUnavailable, "detail": err.Error()})
				return
			}
			sess, att, err = p.cfg.Terminals.Attach(sessionID, chID, caller)
			if err != nil {
				p.cfg.Terminals.Close(sessionID)
				_ = sendJSON(map[string]any{"type": "error", "id": msg.ID, "code": codeUnavailable, "detail": err.Error()})
				return
			}
		}

		st := &ptyStream{id: msg.ID, session: sess, stop: make(chan struct{})}
		mu.Lock()
		streams[msg.ID] = st
		mu.Unlock()

		if err := sendJSON(map[string]any{"type": "ready", "id": msg.ID, "session": sessionID}); err != nil {
			return
		}
		// 屏幕先于直播。快照与订阅是同一把锁内取的（terminal.Attachment），
		// 所以这里恒不会丢掉、也恒不会重复中间那一段。
		for off := 0; off < len(att.Replay); off += ptyReplayChunk {
			end := off + ptyReplayChunk
			if end > len(att.Replay) {
				end = len(att.Replay)
			}
			if err := sendBytes(msg.ID, att.Replay[off:end]); err != nil {
				return
			}
		}
		_ = sess.Resize(cols, rows)
		// 回放还原不了全屏程序自己画的那一屏，它们靠 SIGWINCH 重画。
		_ = sess.Redraw()

		go func() {
			for {
				select {
				case b, ok := <-att.Bytes:
					if !ok {
						return
					}
					// Terminal bytes ride as binary: they are not text and must
					// never be mangled by UTF-8 validation.
					if err := sendBytes(st.id, b); err != nil {
						finish()
						return
					}
				case <-st.stop:
					return
				case <-done:
					return
				}
			}
		}()

		// 会话真的结束时必须说出来——而且恒只关这一条流，**恒不关整条连接**：
		// 别的频道的终端还在上面跑着。客户端凭这条 exit 才分得清"shell 退出了"
		// 和"网断了"，否则它会一直重连，把人送进一个它从没要过的新 shell。
		go func() {
			select {
			case <-sess.Ended():
				reason := sess.EndReason()
				if reason == "" {
					reason = "session ended"
				}
				_ = sendJSON(map[string]any{"type": "exit", "id": st.id, "reason": reason})
				mu.Lock()
				delete(streams, st.id)
				mu.Unlock()
				st.halt()
				p.cfg.Terminals.Detach(sess)
			case <-st.stop:
			case <-done:
			}
		}()
	}

	_ = ws.SetReadDeadline(time.Now().Add(ptyPongWait))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(ptyPongWait))
	})
	for {
		typ, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		_ = ws.SetReadDeadline(time.Now().Add(ptyPongWait))
		if typ == websocket.BinaryMessage {
			if len(data) < ptyStreamHeader {
				continue
			}
			id := binary.BigEndian.Uint32(data[:ptyStreamHeader])
			mu.Lock()
			st := streams[id]
			mu.Unlock()
			if st == nil {
				continue
			}
			// 一条流写不下去恒不该拖垮整条连接上别的终端。
			if err := st.session.Write(data[ptyStreamHeader:]); err != nil {
				st.halt()
			}
			continue
		}
		var msg ptyClientMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "open":
			openStream(msg)
		case "resize":
			mu.Lock()
			st := streams[msg.ID]
			mu.Unlock()
			if st != nil {
				// Window size is low-frequency and needs to be reliable, so it
				// is a control message rather than an escape smuggled into the
				// byte stream (§4).
				_ = st.session.Resize(msg.Cols, msg.Rows)
			}
		case "detach":
			// 收起分屏/切走频道：viewer 走人，shell 恒不死（§4.4）。
			mu.Lock()
			st := streams[msg.ID]
			delete(streams, msg.ID)
			mu.Unlock()
			if st != nil {
				st.halt()
				p.cfg.Terminals.Detach(st.session)
			}
		case "close":
			mu.Lock()
			st := streams[msg.ID]
			delete(streams, msg.ID)
			mu.Unlock()
			if st != nil {
				st.halt()
				p.cfg.Terminals.Close(st.session.ID)
			}
		}
	}
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
