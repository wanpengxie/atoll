// Package web is the gateway's first connector (design §5.3): the web方言 car
// (byte↔IR translation for a browser ws), holding NO session state or游标 — those
// live in the gateway core (drivers/gateway). It upgrades the ws, runs the single
// writer pump + reader loop, and drives the gateway Session. internal 墙圈禁: the
// only surface is Connector.ServeWeb.
package web

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
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
	if ap.HistoryProtocol != subjectgate.FrameVersion {
		writeErr(ws, f.Ref, "attach", subjectgate.CodeBadPayload, "history_protocol must match wire version")
		return
	}

	if ap.Generation == 0 {
		writeErr(ws, f.Ref, "attach", subjectgate.CodeBadPayload, "generation must be positive")
		return
	}
	sess, aerr := c.gw.Attach(principal, parseSince(ap), ap.Generation)
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

	// Attach receipt carries only membership and lightweight history metadata.
	// PrimeFeed installs commit subscriptions first; PrepareHistoryMetadata then
	// captures each head and advances the live cursor to it. LaunchFeed therefore
	// starts strictly after the snapshot while every later commit remains visible.
	if !sess.PrimeFeed() {
		return
	}
	historyMeta := sess.PrepareHistoryMetadata(channel.ID(ap.Focus))
	routes, complete := sess.MembershipSnapshot()
	entries := make([]subjectgate.MembershipEntry, 0, len(routes))
	for _, route := range routes {
		entries = append(entries, subjectgate.MembershipEntry{ChannelID: string(route.Channel), ActorID: string(route.SubjectID)})
	}
	slices.SortFunc(entries, func(a, b subjectgate.MembershipEntry) int { return strings.Compare(a.ChannelID, b.ChannelID) })
	sess.SetLabel(ap.Label)
	receipt, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, f.Ref, subjectgate.AttachReceipt{
		Session:             sess.ID(),
		Label:               sess.Label(),
		ContractVersion:     c.contractVersion,
		Boot:                c.boot,
		Memberships:         entries,
		MembershipsComplete: complete,
		HistoryMeta:         historyMeta,
	})
	sess.Send(receipt)
	sess.LaunchFeed()

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
		sess.Dispatch(fr)
	}
}

func (c *Connector) writerPump(ws *websocket.Conn, sess *gateway.Session) {
	live := sess.LiveDown()
	history := sess.HistoryDown()
	backfill := sess.BackfillDown()
	done := sess.Done()
	ping := time.NewTicker(wsPingPeriod)
	defer ping.Stop()
	var pendingBackfill []byte
	var pendingHistory []byte
	for {
		var b []byte
		// Keepalive is checked at every frame boundary as well as while idle;
		// a continuous backfill must not starve pings until the pong deadline.
		select {
		case <-done:
			return
		case <-ping.C:
			_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
				sess.Close()
				return
			}
			continue
		default:
		}
		if pendingHistory != nil {
			select {
			case <-done:
				return
			case b = <-live:
			default:
				b = pendingHistory
				pendingHistory = nil
			}
		} else if pendingBackfill != nil {
			// A backfill frame selected while live was empty is only a candidate.
			// Re-check both higher lanes at the next frame boundary.
			select {
			case <-done:
				return
			case b = <-live:
			case pendingHistory = <-history:
				continue
			default:
				b = pendingBackfill
				pendingBackfill = nil
			}
		} else {
			select {
			case <-done:
				return
			case b = <-live:
			default:
				select {
				case <-done:
					return
				case b = <-live:
				case pendingHistory = <-history:
					continue
				default:
					select {
					case <-done:
						return
					case b = <-live:
					case pendingHistory = <-history:
						continue
					case pendingBackfill = <-backfill:
						continue
					case <-ping.C:
						_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
						if err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
							sess.Close()
							return
						}
						continue
					}
				}
			}
		}
		_ = ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
		if err := ws.WriteMessage(websocket.TextMessage, b); err != nil {
			sess.Close()
			return
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
