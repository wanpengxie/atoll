package link

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// AttachRequest is the stream-0 control message a daemon sends to join a
// channel home. Actor intent is pulled separately as one full Plan snapshot;
// attach carries no actor declarations or incremental lifecycle state. It
// carries NO credential —
// authentication is an app-layer concern resolved on the WS upgrade (the URL's
// ?key= query) before the connection ever reaches the Acceptor; the link layer
// is auth-agnostic (it does not care who the peer is, only its ResolveFunc
// differs). A credential field here would be a dead leak of an app concern into
// the wire vocabulary.
type AttachRequest struct {
	Proto int `json:"proto"`
}

// AttachReply is the home's stream-0 response: the assigned channel and the
// accept verdict.
type AttachReply struct {
	ChannelID  channel.ID        `json:"channel_id"`
	Generation SessionGeneration `json:"generation"`
	Accepted   bool              `json:"accepted"`
	Reason     string            `json:"reason,omitempty"`
	// DaemonID is the authenticated compute id the app bound to this link before
	// handing it to Home. The peer never supplies an identity claim on the link
	// protocol; this reply lets it key local resource ownership by the server's
	// authoritative identity.
	DaemonID string `json:"daemon_id"`
}

// controlKind tags one stream-0 control payload (the link control plane is
// JSON; actor streams are native ipc). Exactly two messages: attach and its
// reply — the whole party-level negotiation.
type controlKind string

const (
	ctrlAttach      controlKind = "attach"
	ctrlAttachReply controlKind = "attach_reply"
	ctrlPlanPull    controlKind = "plan_pull"
	ctrlPlanReply   controlKind = "plan_reply"
	ctrlPlanPoke    controlKind = "plan_poke"
	ctrlProbe       controlKind = "session_probe"
	ctrlProbeReply  controlKind = "session_probe_reply"
)

type PlanPull struct{}

type Probe struct {
	Nonce string `json:"nonce"`
}

type ProbeReply struct {
	Nonce string `json:"nonce"`
}

type PlanReply struct {
	Actors []platform.PlanActor `json:"actors"`
	Error  string               `json:"error,omitempty"`
}

// encodePlanPoke emits the deliberately empty, sole-key level-wake frame.
func encodePlanPoke() []byte { return []byte(`{"kind":"plan_poke"}`) }

// validPlanPoke accepts EXACTLY the one-field shape encodePlanPoke emits
// (len==1 check). The two functions must change together: adding a field to
// the encoder without relaxing this check makes every poke a protocol
// violation that kills the session.
func validPlanPoke(raw []byte) bool {
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || len(value) != 1 {
		return false
	}
	kind, ok := value["kind"]
	if !ok {
		return false
	}
	var decoded controlKind
	return json.Unmarshal(kind, &decoded) == nil && decoded == ctrlPlanPoke
}

// controlFrame is the stream-0 envelope: one kind, one optional payload each.
type controlFrame struct {
	RequestID   string         `json:"request_id,omitempty"`
	Kind        controlKind    `json:"kind"`
	Attach      *AttachRequest `json:"attach,omitempty"`
	AttachReply *AttachReply   `json:"attach_reply,omitempty"`
	PlanPull    *PlanPull      `json:"plan_pull,omitempty"`
	PlanReply   *PlanReply     `json:"plan_reply,omitempty"`
	Probe       *Probe         `json:"probe,omitempty"`
	ProbeReply  *ProbeReply    `json:"probe_reply,omitempty"`
}

func encodeControl(f controlFrame) ([]byte, error) { return json.Marshal(f) }

func decodeControl(b []byte) (controlFrame, error) {
	var f controlFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return controlFrame{}, fmt.Errorf("link: decode control: %w", err)
	}
	return f, nil
}

func peekControlKind(raw []byte) controlKind {
	var head struct {
		Kind controlKind `json:"kind"`
	}
	_ = json.Unmarshal(raw, &head)
	return head.Kind
}

func waitGroupWithin(group *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
