// Package web is the gateway's first connector (design §5.3): the web方言 car
// (byte↔IR translation for a browser ws), holding NO session state or游标 — those
// live in the gateway core (drivers/gateway). It upgrades the ws, runs the single
// writer pump + reader loop, and drives the gateway Session. internal 墙圈禁: the
// only surface is Connector.ServeWeb.
package web

import (
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	// wsWriteWait bounds one ws write so a stuck peer cannot pin the single writer
	// pump forever (the connector owns its wire deadline independently of lane sizing).
	wsWriteWait = 10 * time.Second
	// wsPongWait is the read deadline; each pong renews it, reclaiming a half-open
	// conn within one window.
	wsPongWait = 60 * time.Second
	// wsPingPeriod pings strictly more often than the read deadline.
	wsPingPeriod = (wsPongWait * 9) / 10
)

// Connector is the web方言 adapter over a gateway. One per process; the assembly
// root constructs it with the gateway and injects ServeWeb behind a local interface.
type Connector struct {
	gw              *gateway.Gateway
	contractVersion string
	boot            string
	upgrader        websocket.Upgrader
}

// New builds a web connector over gw. The contract version and the server
// world identity (boot) are supplied by the assembly root; boot may be empty
// for hosts without an install identity.
func New(gw *gateway.Gateway, contractVersion, boot string) *Connector {
	if contractVersion == "" {
		panic("web connector: contract version is required")
	}
	return &Connector{
		gw: gw, contractVersion: contractVersion, boot: boot,
		upgrader: websocket.Upgrader{
			// Same-origin ws (cross-site hijacking defense): /ws authenticates by
			// cookie, so an absent Origin (non-browser client) is allowed but a
			// cross-origin one is refused.
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				if origin == "" {
					return true
				}
				u, err := url.Parse(origin)
				return err == nil && u.Host == r.Host
			},
		},
	}
}

// ServeWeb upgrades one authenticated connection and runs the gateway session
// (连接模型勘误期: 连接即人). The portal has only resolved session→principal —
// there is NO connection-level channel ACL, because channel eligibility is a
// per-frame/per-batch fact the gateway resolves live (户籍 ∪ 读资格 via the injected
// EntitlementResolver). The opening frame MUST be an attach, but it names no channel:
// it just hands over the游标表 (a multi-key since map). Then the writer pump drains
// the lane (feed for ALL the person's合法频道 + receipts) and the reader loop drives每
// upstream business frame — each carrying its own channel_id — onto that channel's
// subject cell.
func (c *Connector) ServeWeb(w http.ResponseWriter, r *http.Request, principal string) {
	ws, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	ws.SetReadLimit(subjectgate.MaxFrameBytes)
	_ = ws.SetReadDeadline(time.Now().Add(wsPongWait))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	// Opening frame: attach (channel-blind — just the游标表 handoff).
	_, data, err := ws.ReadMessage()
	if err != nil {
		return
	}
	f, perr := subjectgate.ParseUpstreamFrame(data)
	if perr != nil || f.Type != subjectgate.FrameAttach {
		writeErr(ws, f.Ref, "attach", subjectgate.CodeBadPayload, "opening frame must be a valid attach")
		return
	}
	var ap subjectgate.AttachPayload
	if err := f.DecodePayload(&ap); err != nil {
		writeErr(ws, f.Ref, "attach", subjectgate.CodeBadPayload, err.Error())
		return
	}

	sess, aerr := c.gw.Attach(principal, parseSince(ap))
	if aerr != nil {
		writeErr(ws, f.Ref, "attach", subjectgate.CodeUnavailable, "gateway unavailable")
		return
	}
	defer sess.Close()

	// Single writer pump (feed + receipts + keepalive all funnel through the lane,
	// gorilla forbids concurrent writes).
	go c.writerPump(ws, sess)
	// Unblock the reader on teardown (writer failure / seal / disconnect).
	go func() {
		<-sess.Done()
		_ = ws.SetReadDeadline(time.Now())
	}()

	// Attach receipt carries version discovery and is sent before feed backfill.
	// Sent before the feed backfill so it is not interleaved behind it.
	receipt, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, f.Ref, subjectgate.AttachReceipt{ContractVersion: c.contractVersion, Boot: c.boot})
	sess.Send(receipt)
	sess.StartFeed()

	// Reader loop: the SINGLE ws reader. detach is整删 (no client-visible unbind);
	// every business frame names its own channel_id and returns a receipt-or-error.
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		fr, perr := subjectgate.ParseUpstreamFrame(data)
		if perr != nil {
			sess.Send(errFrame(fr.Ref, string(fr.Type), subjectgate.CodeBadPayload, perr.Error()))
			continue
		}
		sess.Send(sess.Upstream(fr))
	}
}

func (c *Connector) writerPump(ws *websocket.Conn, sess *gateway.Session) {
	down := sess.Down()
	done := sess.Done()
	ping := time.NewTicker(wsPingPeriod)
	defer ping.Stop()
	for {
		select {
		case <-done:
			return
		case b := <-down:
			_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := ws.WriteMessage(websocket.TextMessage, b); err != nil {
				sess.Close()
				return
			}
		case <-ping.C:
			_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
				sess.Close()
				return
			}
		}
	}
}

// parseSince converts the attach since map into the per-channel cursor vector
// (连接模型勘误期: multi-key is legal — a connection carries游标 for ALL its channels;
// a key with no eligibility is harmless and silently dropped downstream).
func parseSince(ap subjectgate.AttachPayload) map[channel.ID]int64 {
	if len(ap.Since) == 0 {
		return nil
	}
	out := make(map[channel.ID]int64, len(ap.Since))
	for k, v := range ap.Since {
		out[channel.ID(k)] = v
	}
	return out
}

func errFrame(ref, frameType, code, detail string) subjectgate.Frame {
	return subjectgate.NewErrorFrame(ref, frameType, code, detail)
}

// writeErr writes one error frame directly to the ws (pre-session teardown paths).
func writeErr(ws *websocket.Conn, ref, frameType, code, detail string) {
	b, err := errFrame(ref, frameType, code, detail).Marshal()
	if err != nil {
		return
	}
	_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
	_ = ws.WriteMessage(websocket.TextMessage, b)
}
