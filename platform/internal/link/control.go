package link

import (
	"encoding/json"
	"fmt"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

// Declaration is the actor identity a daemon ships on attach so the home can
// register it into membership (the daemon holds NO truth — registration is
// home-side). Type/catalog is domain, not link wire (type non-first-class):
// link carries only the structural triple the registry needs.
type Declaration struct {
	ActorID actor.ActorID `json:"actor_id"`
	Kind    actor.Kind    `json:"kind"`
	Binding actor.Binding `json:"binding"`
}

// AttachRequest is the stream-0 control message a daemon sends ONCE to join a
// channel home: the party identity + the actor streams it will open. It carries
// NO credential — authentication is an app-layer concern resolved on the WS
// upgrade (the URL's ?key= query) before the connection ever reaches the
// Acceptor; the link layer is auth-agnostic (concept doc §3.2: "Link 不关心对端
// 是什么，差异只在 ResolveFunc"). A credential field here would be a dead leak of
// an app concern into the wire vocabulary.
type AttachRequest struct {
	ComputeID    string        `json:"compute_id"`
	Declarations []Declaration `json:"declarations"`
}

// AttachReply is the home's stream-0 response: the assigned channel and the
// accept verdict.
type AttachReply struct {
	ChannelID channel.ID `json:"channel_id"`
	Accepted  bool       `json:"accepted"`
	Reason    string     `json:"reason,omitempty"`
}

// controlKind tags one stream-0 control payload (the link control plane is
// JSON; actor streams are native ipc). Exactly two messages: attach and its
// reply — the whole party-level negotiation.
type controlKind string

const (
	ctrlAttach      controlKind = "attach"
	ctrlAttachReply controlKind = "attach_reply"
)

// controlFrame is the stream-0 envelope: one kind, one optional payload each.
type controlFrame struct {
	Kind        controlKind    `json:"kind"`
	Attach      *AttachRequest `json:"attach,omitempty"`
	AttachReply *AttachReply   `json:"attach_reply,omitempty"`
}

func encodeControl(f controlFrame) ([]byte, error) { return json.Marshal(f) }

func decodeControl(b []byte) (controlFrame, error) {
	var f controlFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return controlFrame{}, fmt.Errorf("link: decode control: %w", err)
	}
	return f, nil
}

// ---------------------------------------------------------------------------
// wsConn — gorilla *websocket.Conn as a wireConn (binary-message transport)
// ---------------------------------------------------------------------------

// wsConn adapts a gorilla WebSocket to the link wireConn: every mux frame is one
// binary message. WriteMessage callers are serialised by linkConn, so no extra
// write mutex is needed here.
type wsConn struct {
	ws *websocket.Conn
}

func (c *wsConn) ReadMessage() ([]byte, error) {
	_, b, err := c.ws.ReadMessage()
	return b, err
}

func (c *wsConn) WriteMessage(b []byte) error {
	return c.ws.WriteMessage(websocket.BinaryMessage, b)
}

func (c *wsConn) Close() error { return c.ws.Close() }

var _ wireConn = (*wsConn)(nil)
