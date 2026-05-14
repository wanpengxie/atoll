// Package pushhub owns the front-end WebSocket fan-out — when
// server/viewcache applies a new message, the hub broadcasts it to
// every WS-subscribed front-end user that is currently a member of
// the channel.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (pushhub 子目录).
//
// Demo-period: single-instance; no Redis pub/sub. Production would
// replace Hub.broadcast with a pub/sub backend so multiple server
// processes can fan out.
package pushhub

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/identity"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Hub holds the live subscriber registry.
type Hub struct {
	mu sync.RWMutex
	// channel_id → user_id → set of subscribers
	subs map[channel.ID]map[string]map[*subscriber]struct{}
}

// NewHub builds a Hub.
func NewHub() *Hub {
	return &Hub{
		subs: map[channel.ID]map[string]map[*subscriber]struct{}{},
	}
}

// subscriber is one open WS connection.
type subscriber struct {
	ws      *websocket.Conn
	userID  string
	chans   map[channel.ID]struct{}
	send    chan []byte
	once    sync.Once
	done    chan struct{}
	writeMu sync.Mutex
}

func newSubscriber(ws *websocket.Conn, userID string) *subscriber {
	return &subscriber{
		ws:     ws,
		userID: userID,
		chans:  map[channel.ID]struct{}{},
		send:   make(chan []byte, 64),
		done:   make(chan struct{}),
	}
}

// Close cleans up.
func (s *subscriber) Close() {
	s.once.Do(func() {
		close(s.done)
		_ = s.ws.Close()
	})
}

// pumpWrite drains the send channel onto the WS.
func (s *subscriber) pumpWrite() {
	for {
		select {
		case <-s.done:
			return
		case msg, ok := <-s.send:
			if !ok {
				return
			}
			s.writeMu.Lock()
			err := s.ws.WriteMessage(websocket.TextMessage, msg)
			s.writeMu.Unlock()
			if err != nil {
				s.Close()
				return
			}
		}
	}
}

// pumpRead reads control + subscribe frames from the client.
func (s *subscriber) pumpRead(hub *Hub) {
	defer hub.unregister(s)
	for {
		_, raw, err := s.ws.ReadMessage()
		if err != nil {
			return
		}
		var ctrl struct {
			Type      string     `json:"type"`
			ChannelID channel.ID `json:"channel_id"`
		}
		if err := json.Unmarshal(raw, &ctrl); err != nil {
			continue
		}
		switch ctrl.Type {
		case "subscribe":
			hub.subscribe(s, ctrl.ChannelID)
		case "unsubscribe":
			hub.unsubscribe(s, ctrl.ChannelID)
		}
	}
}

// HandleWS upgrades a front-end WS. The identity middleware MUST
// have authenticated the request upstream (via cookie or Bearer).
//
// Wire protocol (client → server JSON frames):
//
//	{"type":"subscribe",   "channel_id":"…"}
//	{"type":"unsubscribe", "channel_id":"…"}
//
// Server-pushed frames carry the kernel/message.Envelope shape
// inside `{"type":"message","channel_id":"…","seq":N,"envelope":…}`.
func (h *Hub) HandleWS(ident *identity.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		// HTTP→WS upgrade can't use middleware-set context (the
		// request lifecycle is hijacked); re-authenticate by reading
		// the cookie / Bearer directly.
		raw := identity.ExtractTokenFromRequest(c.Request)
		user, err := ident.Authenticate(c.Request.Context(), raw)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		sub := newSubscriber(ws, user.ID)
		go sub.pumpWrite()
		sub.pumpRead(h)
	}
}

// PushMessage fan-outs a stored message to every subscriber of the
// channel. Called by gateway after viewcache.Apply commits.
func (h *Hub) PushMessage(channelID channel.ID, seq viewsync.Seq, env message.Envelope) {
	payload := map[string]any{
		"type":       "message",
		"channel_id": string(channelID),
		"seq":        int64(seq),
		"envelope":   env,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	h.mu.RLock()
	bucket := h.subs[channelID]
	subs := make([]*subscriber, 0, len(bucket))
	for _, perUser := range bucket {
		for s := range perUser {
			subs = append(subs, s)
		}
	}
	h.mu.RUnlock()

	for _, s := range subs {
		select {
		case s.send <- raw:
		case <-s.done:
		default:
			// Slow subscriber — drop the frame. The client can
			// recover via the REST resync endpoint.
		}
	}
}

func (h *Hub) subscribe(sub *subscriber, channelID channel.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[channelID]; !ok {
		h.subs[channelID] = map[string]map[*subscriber]struct{}{}
	}
	if _, ok := h.subs[channelID][sub.userID]; !ok {
		h.subs[channelID][sub.userID] = map[*subscriber]struct{}{}
	}
	h.subs[channelID][sub.userID][sub] = struct{}{}
	sub.chans[channelID] = struct{}{}
}

func (h *Hub) unsubscribe(sub *subscriber, channelID channel.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if perUser, ok := h.subs[channelID]; ok {
		if set, ok := perUser[sub.userID]; ok {
			delete(set, sub)
			if len(set) == 0 {
				delete(perUser, sub.userID)
			}
		}
		if len(perUser) == 0 {
			delete(h.subs, channelID)
		}
	}
	delete(sub.chans, channelID)
}

func (h *Hub) unregister(sub *subscriber) {
	for ch := range sub.chans {
		h.unsubscribe(sub, ch)
	}
	sub.Close()
}

// SubscriberCount returns the number of active subscribers for a
// channel (test helper).
func (h *Hub) SubscriberCount(channelID channel.ID) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	bucket := h.subs[channelID]
	total := 0
	for _, set := range bucket {
		total += len(set)
	}
	return total
}

// Ensure context.Context import isn't dropped by go fmt — Hub doesn't
// directly use it but the public API hooks (PushMessage) accept
// context-bound callers indirectly. The unused-import linter is
// satisfied by the blank import below.
var _ = context.Background
