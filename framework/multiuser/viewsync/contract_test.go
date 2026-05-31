package viewsync

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestPushAckFramePayloadFields locks the JSON wire-form keys for the
// Push and Ack payloads against L1 §8.3 row 1 + row 2 spec.
func TestPushAckFramePayloadFields(t *testing.T) {
	t.Parallel()

	push := PushFrame{
		ChannelID:    "chan-A",
		Seq:          42,
		MessageID:    "m-42",
		OwnerEpoch:   1,
		FencingToken: "tok-A",
		Envelope: message.Envelope{
			ID:         "m-42",
			TS:         1,
			ChannelID:  "chan-A",
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent-1"},
			Kind:       message.KindEvent,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{"agent:channel-agent"},
		},
	}
	if got := jsonKeys(t, push); !equalSlices(got, []string{"channel_id", "envelope", "fencing_token", "message_id", "owner_epoch", "seq"}) {
		t.Errorf("PushFrame keys = %v, want [channel_id envelope fencing_token message_id owner_epoch seq]", got)
	}

	ack := AckFrame{ChannelID: "chan-A", LastReceivedSeq: 7, Accepted: true}
	if got := jsonKeys(t, ack); !equalSlices(got, []string{"accepted", "channel_id", "last_received_seq"}) {
		t.Errorf("AckFrame keys = %v, want [accepted channel_id last_received_seq]", got)
	}

	rreq := ResyncRequest{ChannelID: "chan-A", SinceSeq: 2, UntilSeq: 5}
	if got := jsonKeys(t, rreq); !equalSlices(got, []string{"channel_id", "since_seq", "until_seq"}) {
		t.Errorf("ResyncRequest keys = %v, want [channel_id since_seq until_seq]", got)
	}

	rresp := ResyncResponse{ChannelID: "chan-A", SinceSeq: 2, UntilSeq: 5, Messages: []ResyncMessage{}}
	if got := jsonKeys(t, rresp); !equalSlices(got, []string{"channel_id", "messages", "since_seq", "until_seq"}) {
		t.Errorf("ResyncResponse keys = %v, want [channel_id messages since_seq until_seq]", got)
	}
}

func TestAckFrame_RejectReason_RoundTrip(t *testing.T) {
	t.Parallel()

	in := AckFrame{
		ChannelID:    "chan-A",
		Accepted:     false,
		RejectReason: RejectReasonMuxOwnerEpochStale,
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if got := jsonKeys(t, in); !equalSlices(got, []string{"accepted", "channel_id", "last_received_seq", "reject_reason"}) {
		t.Fatalf("AckFrame reject keys=%v", got)
	}
	var out AckFrame
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.RejectReason != RejectReasonMuxOwnerEpochStale || out.Accepted {
		t.Fatalf("round trip ack=%+v", out)
	}
}

// TestApplyStateMachineCoverage exercises the L1 §8.4 server apply rule
// branches (in-order / out-of-order / duplicate / resync-overlap) by
// driving a faithful in-memory implementation of the Receiver contract.
//
// The implementation here is a reference state machine matching the
// pseudocode in L1 §8.4 — it is NOT what runtime/server uses (that one
// is sqlite-backed). Its role is to lock the contract: the test asserts
// the cursor advances exactly when L1 §8.4 says it should.
func TestApplyStateMachineCoverage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		seqs     []Seq // arrival order
		want     ApplyOutcome
		wantSeq  Seq
		wantRows int
	}{
		{
			name:     "in-order — frames arrive 1..3 contiguous",
			seqs:     []Seq{1, 2, 3},
			want:     ApplyOutcomeContiguous,
			wantSeq:  3,
			wantRows: 3,
		},
		{
			name:     "duplicate — seq <= last_received_seq",
			seqs:     []Seq{1, 2, 1, 2}, // 1,2 then resends
			want:     ApplyOutcomeDuplicate,
			wantSeq:  2,
			wantRows: 2,
		},
		{
			name:     "gap — seq > last_received_seq + 1, cursor not advanced",
			seqs:     []Seq{1, 3}, // 2 missing
			want:     ApplyOutcomeGap,
			wantSeq:  1, // stays at last contiguous
			wantRows: 2, // both rows persisted
		},
		{
			name:     "resync-overlap — gap filled later, cursor catches up via buffered drain",
			seqs:     []Seq{1, 3, 4, 2}, // gap then live then gap-fill
			want:     ApplyOutcomeContiguous,
			wantSeq:  4, // after seq=2 arrives, drain catches 3+4
			wantRows: 4,
		},
		{
			name:     "resync during live push — interleaved fills + lives all wedge into contiguous",
			seqs:     []Seq{1, 5, 2, 3, 4}, // gap + drain + remaining
			want:     ApplyOutcomeContiguous,
			wantSeq:  5,
			wantRows: 5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRefReceiver(channel.ID("chan-A"))
			var lastResult ApplyResult
			for _, s := range tc.seqs {
				res, err := r.Apply(context.Background(), mkPush(channel.ID("chan-A"), s))
				if err != nil {
					t.Fatalf("Apply(seq=%d): %v", s, err)
				}
				lastResult = res
			}
			if lastResult.Outcome != tc.want {
				t.Errorf("final outcome = %s, want %s", lastResult.Outcome, tc.want)
			}
			if lastResult.LastReceivedSeq != tc.wantSeq {
				t.Errorf("final cursor = %d, want %d", lastResult.LastReceivedSeq, tc.wantSeq)
			}
			if got := len(r.rows); got != tc.wantRows {
				t.Errorf("rows persisted = %d, want %d", got, tc.wantRows)
			}
		})
	}
}

// TestResyncerInterfaceShape asserts the Resyncer interface advertises
// the two methods L1 §8.5 says — ServeResync (daemon-side) +
// RequestResync (server-side).
func TestResyncerInterfaceShape(t *testing.T) {
	t.Parallel()

	var r Resyncer = &fakeResyncer{}
	resp, err := r.ServeResync(context.Background(), ResyncRequest{
		ChannelID: "chan-A", SinceSeq: 2, UntilSeq: 4,
	})
	if err != nil {
		t.Fatalf("ServeResync: %v", err)
	}
	if resp.SinceSeq != 2 || resp.UntilSeq != 4 {
		t.Errorf("interval echo: got [%d,%d], want [2,4]", resp.SinceSeq, resp.UntilSeq)
	}
	if err := r.RequestResync(context.Background(), "chan-A", 2, 4); err != nil {
		t.Errorf("RequestResync: %v", err)
	}
}

// TestPusherInterfaceShape asserts Pusher exposes EnqueuePush +
// AckReceived per L1 §8.6.
func TestPusherInterfaceShape(t *testing.T) {
	t.Parallel()

	var p Pusher = &fakePusher{}
	if err := p.EnqueuePush(context.Background(), PushFrame{ChannelID: "c", Seq: 1, MessageID: "m"}); err != nil {
		t.Fatalf("EnqueuePush: %v", err)
	}
	if err := p.AckReceived(context.Background(), AckFrame{ChannelID: "c", LastReceivedSeq: 1}); err != nil {
		t.Fatalf("AckReceived: %v", err)
	}
}

// ---------------------------------------------------------------------------
// reference Receiver (in-memory) — locks L1 §8.4 apply rules
// ---------------------------------------------------------------------------

type refReceiver struct {
	channelID channel.ID
	rows      map[Seq]PushFrame
	cursor    LastReceivedSeq
}

func newRefReceiver(id channel.ID) *refReceiver {
	return &refReceiver{
		channelID: id,
		rows:      make(map[Seq]PushFrame),
	}
}

// Apply implements the L1 §8.4 server apply rule. Each branch maps to
// exactly one ApplyOutcome value.
func (r *refReceiver) Apply(_ context.Context, frame PushFrame) (ApplyResult, error) {
	if frame.ChannelID != r.channelID {
		return ApplyResult{}, errors.New("channel mismatch")
	}
	// INSERT OR IGNORE — duplicate seq just drops.
	if _, exists := r.rows[frame.Seq]; !exists {
		r.rows[frame.Seq] = frame
	}

	switch {
	case frame.Seq <= r.cursor:
		return ApplyResult{Outcome: ApplyOutcomeDuplicate, LastReceivedSeq: r.cursor}, nil
	case frame.Seq == r.cursor+1:
		// Contiguous: advance cursor + drain buffered contiguous frames.
		r.cursor = frame.Seq
		for {
			if _, ok := r.rows[r.cursor+1]; !ok {
				break
			}
			r.cursor++
		}
		return ApplyResult{Outcome: ApplyOutcomeContiguous, LastReceivedSeq: r.cursor}, nil
	default:
		// Gap: row persisted, cursor unchanged.
		return ApplyResult{Outcome: ApplyOutcomeGap, LastReceivedSeq: r.cursor}, nil
	}
}

func mkPush(channelID channel.ID, seq Seq) PushFrame {
	return PushFrame{
		ChannelID: channelID,
		Seq:       seq,
		MessageID: "m",
		Envelope: message.Envelope{
			ID:         "env",
			TS:         1,
			ChannelID:  channelID,
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "a"},
			Kind:       message.KindEvent,
			Type:       "agent.text",
			Payload:    json.RawMessage(`{}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{"agent:channel-agent"},
		},
	}
}

// ---------------------------------------------------------------------------
// fakes for interface-shape assertions
// ---------------------------------------------------------------------------

type fakePusher struct{}

func (fakePusher) EnqueuePush(context.Context, PushFrame) error { return nil }
func (fakePusher) AckReceived(context.Context, AckFrame) error  { return nil }

type fakeResyncer struct{}

func (fakeResyncer) ServeResync(_ context.Context, req ResyncRequest) (ResyncResponse, error) {
	return ResyncResponse{ChannelID: req.ChannelID, SinceSeq: req.SinceSeq, UntilSeq: req.UntilSeq, Messages: []ResyncMessage{}}, nil
}
func (fakeResyncer) RequestResync(_ context.Context, _ channel.ID, _ Seq, _ Seq) error {
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func jsonKeys(t *testing.T, v any) []string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
