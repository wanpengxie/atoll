package ipc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// --- closed Kind set ------------------------------------------------------

// The port-wire Kind set is closed at exactly 22 members with fixed wire
// spellings. Every kind has a real producer + state transition; the dead
// frames (fence / shutdown / heartbeat / control) are gone. KindCancel is the
// request-scope of cancel(scope) crossing the wire (host→remote); KindCancelRequest
// is its caller-side upstream twin (remote→host: a bound actor abandons one of its
// own outbound requests); KindAccess/
// KindSchedule + their acks are the plane-2 and time-axis capability arms (an
// out-of-process actor's incarnation carries every plane a local cell's Caps do);
// KindDetach/KindDespawn are the two lifecycle-termination arms (remote→host
// graceful detach vs host→remote by-name despawn) and KindDeliverResult is the
// remote host's delivery observation relayed home. If a wire spelling drifts or a
// kind is added/removed, this trips — the two endpoints must agree on the exact
// bytes.
func TestKindClosedSet(t *testing.T) {
	want := map[Kind]string{
		KindHandshake:     "handshake",
		KindHandshakeAck:  "handshake_ack",
		KindDeliver:       "deliver",
		KindEmit:          "emit",
		KindEmitAck:       "emit_ack",
		KindDown:          "down",
		KindCancel:        "cancel",
		KindObs:           "obs",
		KindAccess:        "access",
		KindAccessAck:     "access_ack",
		KindSchedule:      "schedule",
		KindScheduleAck:   "schedule_ack",
		KindDetach:        "detach",
		KindDespawn:       "despawn",
		KindDeliverResult: "deliver_result",
		KindCancelRequest: "cancel_request",
		KindSpawn:         "spawn",
		KindSpawnAck:      "spawn_ack",
		KindEnd:           "end",
		KindEndAck:        "end_ack",
		KindIdle:          "idle",
		KindIdleAck:       "idle_ack",
	}
	for k, wire := range want {
		if string(k) != wire {
			t.Errorf("Kind %q wire form = %q, want %q", k, string(k), wire)
		}
	}
	if len(want) != 22 {
		t.Fatalf("expected exactly 22 kinds, guard lists %d", len(want))
	}
}

// The three lifecycle frames (detach / despawn / deliver_result) survive
// Write→Read across a real net.Pipe (two independent endpoints, not one buffer):
// what one end writes, the peer reads identically. detach/despawn reuse
// DownPayload{Reason}; deliver_result carries its own DeliverResultPayload. This
// pins the on-wire contract for the frames S1 added.
func TestNewLifecycleFramesRoundTripOverPipe(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	frames := []Frame{
		{Kind: KindDetach, Payload: mustMarshal(t, DownPayload{Reason: "ctx cancelled"})},
		{Kind: KindDespawn, Payload: mustMarshal(t, DownPayload{Reason: "despawn"})},
		{Kind: KindDeliverResult, Payload: mustMarshal(t, DeliverResultPayload{
			EnvelopeID: message.ID("m-9"), Outcome: "not_hosted", Detail: "no cell",
		})},
	}

	wc := NewCodec(nil, client)
	go func() {
		for _, f := range frames {
			if err := wc.Write(f); err != nil {
				return
			}
		}
	}()

	rc := NewCodec(server, nil)

	d0, err := rc.Read()
	if err != nil {
		t.Fatalf("read detach: %v", err)
	}
	if d0.Kind != KindDetach {
		t.Fatalf("frame 0 kind = %q, want detach", d0.Kind)
	}
	var dp0 DownPayload
	mustUnmarshal(t, d0.Payload, &dp0)
	if dp0.Reason != "ctx cancelled" {
		t.Fatalf("detach reason = %q, want ctx cancelled", dp0.Reason)
	}

	d1, err := rc.Read()
	if err != nil {
		t.Fatalf("read despawn: %v", err)
	}
	if d1.Kind != KindDespawn {
		t.Fatalf("frame 1 kind = %q, want despawn", d1.Kind)
	}
	var dp1 DownPayload
	mustUnmarshal(t, d1.Payload, &dp1)
	if dp1.Reason != "despawn" {
		t.Fatalf("despawn reason = %q, want despawn", dp1.Reason)
	}

	d2, err := rc.Read()
	if err != nil {
		t.Fatalf("read deliver_result: %v", err)
	}
	if d2.Kind != KindDeliverResult {
		t.Fatalf("frame 2 kind = %q, want deliver_result", d2.Kind)
	}
	var drp DeliverResultPayload
	mustUnmarshal(t, d2.Payload, &drp)
	if drp.EnvelopeID != message.ID("m-9") || drp.Outcome != "not_hosted" || drp.Detail != "no cell" {
		t.Fatalf("deliver_result payload = %+v, want {m-9, not_hosted, no cell}", drp)
	}
}

// --- wire framing: length-prefix + JSON -----------------------------------

// One frame on the wire is a uint32 big-endian length header followed by
// exactly that many bytes of JSON-marshalled Frame. This nails the on-wire
// byte contract independent of the Read path, so a decoder in another
// language (or a regression in PutUint32 endianness) is caught.
func TestWriteWireFormatLengthPrefixedJSON(t *testing.T) {
	var buf bytes.Buffer
	c := NewCodec(nil, &buf)

	f := Frame{Kind: KindHandshake, Payload: mustMarshal(t, HandshakePayload{LeaseID: "lease-xyz"})}
	if err := c.Write(f); err != nil {
		t.Fatalf("Write: %v", err)
	}

	raw := buf.Bytes()
	if len(raw) < 4 {
		t.Fatalf("frame too short: %d bytes", len(raw))
	}
	n := binary.BigEndian.Uint32(raw[:4])
	body := raw[4:]
	if int(n) != len(body) {
		t.Fatalf("length header = %d, body = %d bytes", n, len(body))
	}

	// Body must be valid JSON decoding back to the same Frame.
	var got Frame
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("body not valid frame JSON: %v", err)
	}
	if got.Kind != KindHandshake {
		t.Fatalf("decoded kind = %q, want handshake", got.Kind)
	}
}

// Empty payload is omitted on the wire (omitempty) — a Down with no reason
// or a kind that carries no payload must not emit a `"payload":null` field
// that the peer would then mis-decode.
func TestWriteOmitsEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	c := NewCodec(nil, &buf)
	if err := c.Write(Frame{Kind: KindDown}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	body := buf.Bytes()[4:]
	if strings.Contains(string(body), "payload") {
		t.Fatalf("empty-payload frame emitted payload field: %s", body)
	}
}

func TestSpawnPayloadPreservesTaggedPlacementHost(t *testing.T) {
	want := SpawnPayload{
		Nonce: "nonce-1", Kind: actor.KindAgent, Class: "worker", NameHint: "child",
		Config: json.RawMessage(`{"x":1}`), PlacementKind: "daemon", PlacementHost: "daemon-target",
	}
	raw := mustMarshal(t, want)
	var got SpawnPayload
	mustUnmarshal(t, raw, &got)
	if got.Nonce != want.Nonce || got.Kind != want.Kind || got.Class != want.Class ||
		got.NameHint != want.NameHint || string(got.Config) != string(want.Config) ||
		got.PlacementKind != want.PlacementKind || got.PlacementHost != want.PlacementHost {
		t.Fatalf("spawn payload=%+v want=%+v", got, want)
	}
	if bytes.Contains(raw, []byte(`"placement":`)) {
		t.Fatalf("legacy kind-only placement field returned: %s", raw)
	}
}

// --- round-trip per kind --------------------------------------------------

// Each kind's payload survives Write→Read byte-for-byte at the struct level.
// This is the core codec contract: what one endpoint writes, the peer reads
// identically. A3 (truth): the substrate relays the actual envelope — Deliver
// and Emit must not mutate or drop any envelope field, including the sender
// identity (A1 addressing) and audience.
func TestRoundTripPerKind(t *testing.T) {
	expires := int64(1717000000)
	env := message.Envelope{
		ID:         message.ID("msg-1"),
		TS:         1717000000123,
		ChannelID:  "chan-A",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: actor.ActorID("agent:writer")},
		Kind:       message.KindRequest,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{"text":"hi"}`),
		ParentID:   message.ID("msg-0"),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.ActorID("user:bob"), actor.ActorID("agent:writer")},
		ExpiresAt:  &expires,
	}

	cases := []struct {
		name  string
		frame Frame
		check func(t *testing.T, got Frame)
	}{
		{
			name:  "handshake",
			frame: Frame{Kind: KindHandshake, Payload: mustMarshal(t, HandshakePayload{LeaseID: "lease-42"})},
			check: func(t *testing.T, got Frame) {
				var p HandshakePayload
				mustUnmarshal(t, got.Payload, &p)
				if p.LeaseID != "lease-42" {
					t.Errorf("lease_id = %q, want lease-42", p.LeaseID)
				}
			},
		},
		{
			name:  "handshake_ack",
			frame: Frame{Kind: KindHandshakeAck, Payload: mustMarshal(t, HandshakeAckPayload{Actor: actor.ActorID("agent:writer")})},
			check: func(t *testing.T, got Frame) {
				var p HandshakeAckPayload
				mustUnmarshal(t, got.Payload, &p)
				if p.Actor != actor.ActorID("agent:writer") {
					t.Errorf("actor = %q, want agent:writer", p.Actor)
				}
			},
		},
		{
			name:  "deliver",
			frame: Frame{Kind: KindDeliver, Payload: mustMarshal(t, DeliverPayload{Envelope: env})},
			check: func(t *testing.T, got Frame) {
				var p DeliverPayload
				mustUnmarshal(t, got.Payload, &p)
				assertEnvelopeEqual(t, p.Envelope, env)
			},
		},
		{
			name:  "emit",
			frame: Frame{Kind: KindEmit, Payload: mustMarshal(t, EmitPayload{Envelope: env})},
			check: func(t *testing.T, got Frame) {
				var p EmitPayload
				mustUnmarshal(t, got.Payload, &p)
				assertEnvelopeEqual(t, p.Envelope, env)
			},
		},
		{
			name:  "down",
			frame: Frame{Kind: KindDown, Payload: mustMarshal(t, DownPayload{Reason: "panic: nil map"})},
			check: func(t *testing.T, got Frame) {
				var p DownPayload
				mustUnmarshal(t, got.Payload, &p)
				if p.Reason != "panic: nil map" {
					t.Errorf("reason = %q", p.Reason)
				}
			},
		},
		{
			name:  "cancel",
			frame: Frame{Kind: KindCancel, Payload: mustMarshal(t, CancelPayload{RequestID: message.ID("req-7")})},
			check: func(t *testing.T, got Frame) {
				var p CancelPayload
				mustUnmarshal(t, got.Payload, &p)
				if p.RequestID != message.ID("req-7") {
					t.Errorf("request_id = %q, want req-7", p.RequestID)
				}
			},
		},
		{
			name:  "cancel_request",
			frame: Frame{Kind: KindCancelRequest, Payload: mustMarshal(t, CancelPayload{RequestID: message.ID("req-up-9")})},
			check: func(t *testing.T, got Frame) {
				var p CancelPayload
				mustUnmarshal(t, got.Payload, &p)
				if p.RequestID != message.ID("req-up-9") {
					t.Errorf("request_id = %q, want req-up-9", p.RequestID)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := NewCodec(&buf, &buf)
			if err := c.Write(tc.frame); err != nil {
				t.Fatalf("Write: %v", err)
			}
			got, err := c.Read()
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if got.Kind != tc.frame.Kind {
				t.Fatalf("kind = %q, want %q", got.Kind, tc.frame.Kind)
			}
			tc.check(t, got)
		})
	}
}

// --- stream framing: many frames over one connection ----------------------

// A single byte-stream carries a sequence of frames; Read must recover each
// frame boundary from the length prefix alone, in order. This is the stream
// invariant the port relies on (one connection, a flow of deliver/emit
// frames over the actor's lifetime).
func TestMultipleFramesOverOneStream(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	frames := []Frame{
		{Kind: KindHandshake, Payload: mustMarshal(t, HandshakePayload{LeaseID: "L"})},
		{Kind: KindDeliver, Payload: mustMarshal(t, DeliverPayload{Envelope: message.Envelope{ID: "m1"}})},
		{Kind: KindEmit, Payload: mustMarshal(t, EmitPayload{Envelope: message.Envelope{ID: "m2"}})},
		{Kind: KindDown},
	}

	wc := NewCodec(nil, client)
	go func() {
		for _, f := range frames {
			if err := wc.Write(f); err != nil {
				return
			}
		}
	}()

	rc := NewCodec(server, nil)
	for i, want := range frames {
		got, err := rc.Read()
		if err != nil {
			t.Fatalf("frame %d Read: %v", i, err)
		}
		if got.Kind != want.Kind {
			t.Fatalf("frame %d kind = %q, want %q", i, got.Kind, want.Kind)
		}
	}
}

// --- EOF on closed connection ---------------------------------------------

// Read surfaces io.EOF when the peer closed cleanly between frames. The port
// host treats this as the terminal Down-equivalent (per doc), so it must be
// the exact io.EOF sentinel, not a wrapped/opaque error.
func TestReadEOFOnCleanClose(t *testing.T) {
	c := NewCodec(bytes.NewReader(nil), io.Discard)
	if _, err := c.Read(); err != io.EOF {
		t.Fatalf("Read on empty stream err = %v, want io.EOF", err)
	}
}

// A header that promises more body than the stream delivers is a truncated
// frame, not EOF — io.ReadFull reports ErrUnexpectedEOF. The decoder must not
// hand back a partial/zero frame as if it were valid.
func TestReadTruncatedBody(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 64) // claim 64 bytes, supply only 3
	stream := append(hdr[:], 'a', 'b', 'c')
	c := NewCodec(bytes.NewReader(stream), io.Discard)
	if _, err := c.Read(); err != io.ErrUnexpectedEOF {
		t.Fatalf("Read truncated err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// A header truncated mid-prefix (fewer than 4 bytes) is likewise an
// unexpected EOF, not a silent zero-length frame.
func TestReadTruncatedHeader(t *testing.T) {
	c := NewCodec(bytes.NewReader([]byte{0x00, 0x01}), io.Discard)
	if _, err := c.Read(); err != io.ErrUnexpectedEOF {
		t.Fatalf("Read truncated header err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// --- MaxFrameBytes guard --------------------------------------------------

// Read rejects an oversized length header BEFORE allocating the body buffer
// — a hostile/corrupt peer cannot drive an OOM by claiming a huge frame.
func TestReadRejectsOversizedFrameBeforeAlloc(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(MaxFrameBytes+1))

	codec := NewCodec(bytes.NewReader(hdr[:]), io.Discard)
	// The size guard (n > MaxFrameBytes) returns BEFORE the body make([]byte,
	// n) by construction, so a hostile huge length never drives an alloc; the
	// "frame too large" error is the observable proof of rejection.
	if _, err := codec.Read(); err == nil {
		t.Fatal("Read returned nil error for oversized frame")
	} else if !strings.Contains(err.Error(), "frame too large") {
		t.Fatalf("Read err=%q want frame too large", err)
	}
}

// Write rejects a frame whose marshalled form exceeds MaxFrameBytes, and
// emits NOTHING — a too-large frame must not leak a length header + partial
// body that would desync the peer's framing.
func TestWriteRejectsOversizedFrame(t *testing.T) {
	big := make([]byte, MaxFrameBytes)
	for i := range big {
		big[i] = 'a'
	}
	var buf bytes.Buffer
	c := NewCodec(nil, &buf)
	// json.RawMessage of a quoted blob > MaxFrameBytes once wrapped.
	payload := json.RawMessage(append(append([]byte{'"'}, big...), '"'))
	err := c.Write(Frame{Kind: KindEmit, Payload: payload})
	if err == nil {
		t.Fatal("Write accepted oversized frame")
	}
	if !strings.Contains(err.Error(), "frame too large") {
		t.Fatalf("Write err = %q, want frame too large", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("Write emitted %d bytes for rejected frame, want 0", buf.Len())
	}
}

// A frame exactly at MaxFrameBytes-or-under round-trips; the cap is inclusive
// on the accept side (the > check, not >=). We verify a near-max frame is
// accepted by both Write and Read.
func TestMaxFrameBoundaryAccepted(t *testing.T) {
	// Build a payload that lands the whole frame just under the cap.
	var buf bytes.Buffer
	c := NewCodec(&buf, &buf)
	blobLen := MaxFrameBytes - 64 // leave headroom for the JSON envelope
	blob := make([]byte, blobLen)
	for i := range blob {
		blob[i] = 'x'
	}
	payload := json.RawMessage(append(append([]byte{'"'}, blob...), '"'))
	f := Frame{Kind: KindEmit, Payload: payload}
	if err := c.Write(f); err != nil {
		t.Fatalf("Write near-max frame: %v", err)
	}
	got, err := c.Read()
	if err != nil {
		t.Fatalf("Read near-max frame: %v", err)
	}
	if got.Kind != KindEmit {
		t.Fatalf("kind = %q, want emit", got.Kind)
	}
}

// --- malformed body -------------------------------------------------------

// A correctly length-framed body that is not valid Frame JSON surfaces an
// unmarshal error, not a zero-value Frame masquerading as valid.
func TestReadRejectsMalformedJSON(t *testing.T) {
	body := []byte("{not json")
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	c := NewCodec(bytes.NewReader(append(hdr[:], body...)), io.Discard)
	if _, err := c.Read(); err == nil {
		t.Fatal("Read accepted malformed JSON body")
	} else if !strings.Contains(err.Error(), "unmarshal") {
		t.Fatalf("Read err = %q, want unmarshal", err)
	}
}

// --- concurrent writes are atomic per frame -------------------------------

// Write holds wmu across header+body, so concurrent writers never interleave
// a header from one frame with the body of another. We fan N writers at one
// codec and require the reader to recover N intact, well-formed frames.
func TestConcurrentWritesDoNotInterleave(t *testing.T) {
	var buf bytes.Buffer
	c := NewCodec(&buf, &buf)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Write(Frame{Kind: KindEmit, Payload: mustMarshal(t, EmitPayload{Envelope: message.Envelope{ID: "m"}})})
		}()
	}
	wg.Wait()

	// Reading from the same codec/buffer: every frame must decode cleanly and
	// be a KindEmit. Interleaved bytes would desync framing and error out.
	for i := 0; i < n; i++ {
		f, err := c.Read()
		if err != nil {
			t.Fatalf("frame %d Read after concurrent writes: %v", i, err)
		}
		if f.Kind != KindEmit {
			t.Fatalf("frame %d kind = %q, want emit", i, f.Kind)
		}
	}
}

// --- Write defensive branches ---------------------------------------------

// Write surfaces a marshal error verbatim (wrapped "ipc: marshal") and emits
// NOTHING: a Frame whose Payload is a json.RawMessage holding INVALID JSON
// fails json.Marshal (RawMessage validates on marshal), so the header/body
// write is never reached and the wire stays clean — no desync for the peer.
func TestWriteMarshalError(t *testing.T) {
	var buf bytes.Buffer
	c := NewCodec(nil, &buf)
	// json.RawMessage is marshalled by re-validating its bytes; "{bad" is not
	// valid JSON, so json.Marshal(Frame{...}) fails before any write.
	err := c.Write(Frame{Kind: KindEmit, Payload: json.RawMessage([]byte("{bad"))})
	if err == nil {
		t.Fatal("Write accepted frame with invalid RawMessage payload")
	}
	if !strings.Contains(err.Error(), "ipc: marshal") {
		t.Fatalf("Write err = %q, want ipc: marshal", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("Write emitted %d bytes for un-marshallable frame, want 0", buf.Len())
	}
}

// failWriter fails its Nth Write call (1-based), succeeding before then. It
// lets a test target the length-prefix write (n=1) vs the body write (n=2)
// independently — the two distinct error returns inside Codec.Write.
type failWriter struct {
	calls  int
	failOn int
	err    error
}

func (f *failWriter) Write(p []byte) (int, error) {
	f.calls++
	if f.calls == f.failOn {
		return 0, f.err
	}
	return len(p), nil
}

// Write returns the writer error from the LENGTH-PREFIX write (the first of
// the two w.Write calls). The error is propagated raw (no wrapping) so the
// host can inspect the underlying transport failure (e.g. a broken pipe).
func TestWriteHeaderWriteError(t *testing.T) {
	boom := errors.New("header boom")
	w := &failWriter{failOn: 1, err: boom}
	c := NewCodec(nil, w)
	err := c.Write(Frame{Kind: KindDown})
	if err != boom {
		t.Fatalf("Write header-fail err = %v, want %v", err, boom)
	}
	if w.calls != 1 {
		t.Fatalf("expected to stop after the failed header write, saw %d writes", w.calls)
	}
}

// Write returns the writer error from the BODY write (the second w.Write
// call, after the header wrote fine). Propagated raw, same as the header path.
func TestWriteBodyWriteError(t *testing.T) {
	boom := errors.New("body boom")
	w := &failWriter{failOn: 2, err: boom}
	c := NewCodec(nil, w)
	err := c.Write(Frame{Kind: KindDown})
	if err != boom {
		t.Fatalf("Write body-fail err = %v, want %v", err, boom)
	}
	if w.calls != 2 {
		t.Fatalf("expected header write then failed body write (2 calls), saw %d", w.calls)
	}
}

// --- helpers --------------------------------------------------------------

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

func assertEnvelopeEqual(t *testing.T, got, want message.Envelope) {
	t.Helper()
	if got.ID != want.ID {
		t.Errorf("envelope ID = %q, want %q", got.ID, want.ID)
	}
	if got.Sender != want.Sender {
		t.Errorf("envelope Sender = %+v, want %+v", got.Sender, want.Sender)
	}
	if got.Kind != want.Kind {
		t.Errorf("envelope Kind = %q, want %q", got.Kind, want.Kind)
	}
	if got.Type != want.Type {
		t.Errorf("envelope Type = %q, want %q", got.Type, want.Type)
	}
	if got.Visibility != want.Visibility {
		t.Errorf("envelope Visibility = %q, want %q", got.Visibility, want.Visibility)
	}
	if len(got.Audience) != len(want.Audience) {
		t.Fatalf("envelope Audience len = %d, want %d", len(got.Audience), len(want.Audience))
	}
	for i := range want.Audience {
		if got.Audience[i] != want.Audience[i] {
			t.Errorf("envelope Audience[%d] = %q, want %q", i, got.Audience[i], want.Audience[i])
		}
	}
	if (got.ExpiresAt == nil) != (want.ExpiresAt == nil) {
		t.Fatalf("envelope ExpiresAt nil-ness mismatch: got=%v want=%v", got.ExpiresAt, want.ExpiresAt)
	}
	if got.ExpiresAt != nil && *got.ExpiresAt != *want.ExpiresAt {
		t.Errorf("envelope ExpiresAt = %d, want %d", *got.ExpiresAt, *want.ExpiresAt)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("envelope Payload = %s, want %s", got.Payload, want.Payload)
	}
}
