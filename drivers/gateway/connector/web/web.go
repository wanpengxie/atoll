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
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	// wsWriteWait bounds one ws write so a stuck peer cannot pin the single writer
	// pump forever (照 ws.go wsWriteWait 10s == LaneWriteTimeoutMs).
	wsWriteWait = 10 * time.Second
	// wsPongWait is the read deadline; each pong renews it, reclaiming a half-open
	// conn within one window.
	wsPongWait = 60 * time.Second
	// wsPingPeriod pings strictly more often than the read deadline.
	wsPingPeriod = (wsPongWait * 9) / 10
)

// Connector is the web方言 adapter over a gateway. One per process; the assembly
// root constructs it with the gateway and the app injects ServeWeb behind an
// app-defined interface (app → drivers is forbidden, so cmd/server bridges).
type Connector struct {
	gw       *gateway.Gateway
	upgrader websocket.Upgrader
}

// New builds a web connector over gw.
func New(gw *gateway.Gateway) *Connector {
	return &Connector{
		gw: gw,
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

// ServeWeb upgrades one authenticated connection and runs the gateway session. The
// app membrane has already resolved session→principal + channel ACL (workspace
// membership = tail eligibility); the gateway resolves channel membership (write
// eligibility) inside Attach. The opening frame MUST be an attach naming this
// channel; then the reader loop drives每 upstream frame onto the subject's cell and
// the writer pump drains the lane (feed + receipts) to the wire.
func (c *Connector) ServeWeb(w http.ResponseWriter, r *http.Request, home *platform.Home, chID channel.ID, principal string) {
	ws, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	ws.SetReadLimit(platform.MaxFrameBytes)
	_ = ws.SetReadDeadline(time.Now().Add(wsPongWait))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	// Opening frame: attach.
	_, data, err := ws.ReadMessage()
	if err != nil {
		return
	}
	f, perr := platform.ParseFrame(data)
	if perr != nil || f.Type != platform.FrameAttach {
		writeErr(ws, "attach", platform.CodeBadPayload, "opening frame must be a valid attach")
		return
	}
	var ap platform.AttachPayload
	if err := f.DecodePayload(&ap); err != nil {
		writeErr(ws, "attach", platform.CodeBadPayload, err.Error())
		return
	}
	if ap.ChannelID != "" && channel.ID(ap.ChannelID) != chID {
		writeErr(ws, "attach", platform.CodeBadPayload, "attach channel_id does not match the authenticated channel")
		return
	}
	since, serr := parseSince(ap, chID)
	if serr != "" {
		writeErr(ws, "attach", platform.CodeBadPayload, serr)
		return
	}

	sess, gen, aerr := c.gw.Attach(r.Context(), home, chID, principal, since)
	if aerr != nil {
		writeErr(ws, "attach", platform.CodeUnavailable, "channel unavailable")
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

	// Attach receipt (before the feed backfill so it is not interleaved behind it).
	receipt, _ := platform.NewFrame(platform.FrameReceipt, gen, f.Ref, platform.AttachReceipt{BindingGen: gen})
	sess.Send(receipt)
	sess.StartFeed()

	// Reader loop: the SINGLE ws reader.
	ctx := r.Context()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		fr, perr := platform.ParseFrame(data)
		if perr != nil {
			sess.Send(errFrame("", platform.CodeBadPayload, perr.Error()))
			continue
		}
		// detach returns an empty frame (表② detach receipt = "—"): send nothing, the
		// seal + teardown IS the result.
		if resp := sess.Upstream(ctx, fr); resp.Type != "" {
			sess.Send(resp)
		}
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

// parseSince validates the attach since map (裁决7: today a single-element map keyed
// by this channel; a multi-key payload is refused bad_payload) and returns the
// per-channel cursor vector.
func parseSince(ap platform.AttachPayload, chID channel.ID) (map[channel.ID]int64, string) {
	if len(ap.Since) == 0 {
		return nil, ""
	}
	if len(ap.Since) > 1 {
		return nil, "since map may carry only this connection's channel (today 单元素)"
	}
	out := make(map[channel.ID]int64, 1)
	for k, v := range ap.Since {
		if k != string(chID) {
			return nil, "since key must be the attached channel"
		}
		out[channel.ID(k)] = v
	}
	return out, ""
}

func errFrame(frameType, code, detail string) platform.Frame {
	f, _ := platform.NewFrame(platform.FrameError, 0, "", platform.ErrorPayload{Frame: frameType, Code: code, Detail: detail})
	return f
}

// writeErr writes one error frame directly to the ws (pre-session teardown paths).
func writeErr(ws *websocket.Conn, frameType, code, detail string) {
	b, err := errFrame(frameType, code, detail).Marshal()
	if err != nil {
		return
	}
	_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
	_ = ws.WriteMessage(websocket.TextMessage, b)
}
