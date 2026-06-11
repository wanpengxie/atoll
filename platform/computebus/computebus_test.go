package computebus

import (
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	frames := []Frame{
		{Type: FrameAttach, Attach: &AttachRequest{
			APIKey: "key-1", ComputeID: "c1",
			Declarations: []AttachDeclaration{{ActorID: "xhs", Kind: actor.KindTool, Binding: actor.BindingEmbedded}},
		}},
		{Type: FrameAttachReply, Reply: &AttachReply{ChannelID: channel.ID("ch-1"), Accepted: true}},
		{Type: FrameHeartbeat, Beat: &Heartbeat{ComputeID: "c1"}},
		{Type: FrameDispatch, Dispatch: &DispatchFrame{Target: "xhs", Envelope: &message.Envelope{ID: "e1"}}},
		{Type: FrameEmit, EmitID: "emit-1", Emit: &EmitFrame{Source: "xhs", Envelope: &message.Envelope{ID: "e2"}}},
		{Type: FrameEmitAck, Ack: &EmitAck{EmitID: "emit-1", MessageID: "e2"}},
		{Type: FrameDeath, Death: &DeathFrame{Actor: "xhs", Cause: "panic"}},
	}
	for _, f := range frames {
		b, err := Encode(f)
		if err != nil {
			t.Fatalf("encode %s: %v", f.Type, err)
		}
		got, err := Decode(b)
		if err != nil {
			t.Fatalf("decode %s: %v", f.Type, err)
		}
		if got.Type != f.Type {
			t.Fatalf("type = %q, want %q", got.Type, f.Type)
		}
	}
}

func TestDecodeInvalid(t *testing.T) {
	_, err := Decode([]byte(`not json`))
	if err == nil {
		t.Fatal("invalid json must error")
	}
}

func TestAttachDeclarationNoTypes(t *testing.T) {
	d := AttachDeclaration{ActorID: "a", Kind: actor.KindTool, Binding: actor.BindingEmbedded}
	b, _ := Encode(Frame{Type: FrameAttach, Attach: &AttachRequest{
		APIKey: "k", ComputeID: "c", Declarations: []AttachDeclaration{d},
	}})
	got, _ := Decode(b)
	decl := got.Attach.Declarations[0]
	if decl.ActorID != "a" || decl.Kind != actor.KindTool {
		t.Fatalf("decl = %+v", decl)
	}
}
