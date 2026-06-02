package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

// gateway is the client/SDK ingress: it exposes the routes the coagentsdk
// expects (cursor / messages / actors / ws) over the single-channel home. It is
// the external-client half of the v2 truth-flip — a client POSTs a request into
// truth and tails the committed envelopes (responses/events) over a WS push.
type gateway struct {
	home      *channelhost.ChannelHome
	channelID channel.ID
}

const clientRequestTTLMs int64 = 30_000 // SDK requests carry no expiry; R5 default.

var gwUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

const sessionCookieName = "coagent_session"

// mount registers the SDK routes on mux.
func (g *gateway) mount(mux *http.ServeMux) {
	mux.HandleFunc("/api/channels/", g.routeChannel)
	mux.HandleFunc("/ws", g.handleWS)
}

// routeChannel dispatches /api/channels/{id}/{cursor|messages|actors}.
func (g *gateway) routeChannel(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/channels/")
	id, tail, ok := strings.Cut(rest, "/")
	if !ok || channel.ID(id) != g.channelID {
		http.Error(w, "unknown channel", http.StatusNotFound)
		return
	}
	switch {
	case tail == "cursor" && r.Method == http.MethodGet:
		g.handleCursor(w, r)
	case tail == "messages" && r.Method == http.MethodPost:
		g.handleMessages(w, r)
	case tail == "actors" && r.Method == http.MethodGet:
		g.handleActors(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (g *gateway) handleCursor(w http.ResponseWriter, r *http.Request) {
	seq, err := g.home.MaxSeq(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"last_received_seq": seq})
}

func (g *gateway) handleMessages(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID       string          `json:"id"`
		Type     string          `json:"type"`
		Kind     string          `json:"kind"`
		Payload  json.RawMessage `json:"payload"`
		Audience []string        `json:"audience"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sender := g.clientSender(r)
	// The client is an actor too — register it so its request sender-authenticates.
	_ = g.home.Registry().Insert(r.Context(), actor.Record{ID: sender, Kind: actor.KindAgent})

	now := time.Now().UnixMilli()
	exp := now + clientRequestTTLMs
	aud := make(message.Audience, 0, len(body.Audience))
	for _, a := range body.Audience {
		aud = append(aud, actor.ActorID(a))
	}
	env := &message.Envelope{
		ID: message.ID(body.ID), TS: now, ChannelID: g.channelID,
		Kind: message.Kind(body.Kind), Type: body.Type,
		Sender: message.Sender{Kind: actor.KindAgent, ID: sender}, Audience: aud,
		Payload: body.Payload, ExpiresAt: &exp,
	}
	cctx := harness.CtxWithCaller(r.Context(), harness.CallerContext{ActorID: sender, ChannelID: g.channelID, AllowProvidedSenderKind: true})
	res, err := g.home.Dispatch(cctx, env)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"message_id": string(res.MessageID), "accepted": res.RejectReason == "",
		"reject_reason": string(res.RejectReason), "reject_detail": res.RejectDetail,
	})
}

func (g *gateway) handleActors(w http.ResponseWriter, r *http.Request) {
	rows, err := g.home.Registry().ListActive(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	actors := make([]map[string]any, 0, len(rows))
	for _, rec := range rows {
		rd := rec.Readiness.Normalize()
		actors = append(actors, map[string]any{
			"actor_id": string(rec.ID), "kind": string(rec.Kind), "binding": string(rec.Binding),
			"ready": rd.IsReady(), "ready_reason": rd.Reason,
		})
	}
	writeJSON(w, map[string]any{"channel_id": string(g.channelID), "actors": actors})
}

// handleWS upgrades a client subscription and tails committed envelopes forward
// from the client's cursor, pushing each as a wsPushFrame. The push hub signals
// "new commits"; the tail reads by seq so nothing is missed.
func (g *gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	ws, err := gwUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = ws.Close() }()

	var sub struct {
		Type      string `json:"type"`
		ChannelID string `json:"channel_id"`
		SinceSeq  int64  `json:"since_seq"`
	}
	if _, raw, err := ws.ReadMessage(); err != nil {
		return
	} else if err := json.Unmarshal(raw, &sub); err != nil || sub.Type != "subscribe" || channel.ID(sub.ChannelID) != g.channelID {
		return
	}

	ctx := r.Context()
	sig, unsub := g.home.Subscribe()
	defer unsub()
	last := sub.SinceSeq
	for {
		envs, err := g.home.ReadAfterSeq(ctx, last, 256)
		if err != nil {
			return
		}
		for i := range envs {
			env := envs[i]
			raw, mErr := json.Marshal(env)
			if mErr != nil {
				continue
			}
			frame, _ := json.Marshal(map[string]any{
				"type": "message", "channel_id": string(g.channelID), "seq": env.Seq, "envelope": json.RawMessage(raw),
			})
			if err := ws.WriteMessage(websocket.TextMessage, frame); err != nil {
				return
			}
			last = env.Seq
		}
		select {
		case <-sig:
		case <-ctx.Done():
			return
		}
	}
}

// clientSender resolves the client's actor id from the session cookie (a single
// api-key style identity); empty → a shared "web-client".
func (g *gateway) clientSender(r *http.Request) actor.ActorID {
	if c, err := r.Cookie(sessionCookieName); err == nil && c.Value != "" {
		return actor.ActorID(c.Value)
	}
	return "web-client"
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
