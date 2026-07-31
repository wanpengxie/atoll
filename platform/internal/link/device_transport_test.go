package link

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

type blockingLaneConn struct {
	readStarted  chan struct{}
	readWake     chan struct{}
	closeStarted chan struct{}
	closeRelease chan struct{}
	readOnce     sync.Once
	wakeOnce     sync.Once
	closeOnce    sync.Once
}

func newBlockingLaneConn() *blockingLaneConn {
	return &blockingLaneConn{
		readStarted: make(chan struct{}), readWake: make(chan struct{}),
		closeStarted: make(chan struct{}), closeRelease: make(chan struct{}),
	}
}

func (c *blockingLaneConn) Read([]byte) (int, error) {
	c.readOnce.Do(func() { close(c.readStarted) })
	<-c.readWake
	return 0, errors.New("read deadline")
}
func (*blockingLaneConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *blockingLaneConn) Close() error {
	c.closeOnce.Do(func() { close(c.closeStarted) })
	<-c.closeRelease
	return nil
}
func (*blockingLaneConn) LocalAddr() net.Addr         { return testAddr("local") }
func (*blockingLaneConn) RemoteAddr() net.Addr        { return testAddr("remote") }
func (*blockingLaneConn) SetDeadline(time.Time) error { return nil }
func (c *blockingLaneConn) SetReadDeadline(time.Time) error {
	c.wakeOnce.Do(func() { close(c.readWake) })
	return nil
}
func (*blockingLaneConn) SetWriteDeadline(time.Time) error { return nil }

func TestRetireLaneLogicalDecisionPrecedesPhysicalClose(t *testing.T) {
	conn := newBlockingLaneConn()
	lane := newLaneStream(nil, channel.ID("a"), LaneGeneration("g1"), conn)
	var retired atomic.Bool
	lane.SetRetire(func(exact *LaneStream) {
		if exact != lane {
			t.Errorf("retire callback got a different lane")
		}
		retired.Store(true)
	})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		var frame LaneFrame
		_ = lane.Decode(&frame)
		lane.RetireLogical()
		lane.CollectPhysical()
	}()
	<-conn.readStarted

	started := time.Now()
	lane.RetireLogical()
	if time.Since(started) > 50*time.Millisecond {
		t.Fatal("logical retirement waited for physical Close")
	}
	if !retired.Load() || !lane.Retired() {
		t.Fatal("logical lane authority was not removed synchronously")
	}
	select {
	case <-lane.Done():
	default:
		t.Fatal("logical done level was not published")
	}
	<-conn.closeStarted
	select {
	case <-lane.PhysicalDone():
		t.Fatal("physical completion published before blocked Close returned")
	default:
	}
	close(conn.closeRelease)
	select {
	case <-lane.PhysicalDone():
	case <-time.After(time.Second):
		t.Fatal("physical collector did not complete")
	}
	<-readerDone
}

func TestLaneDecoderPreservesBackToBackFrames(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	lane := newLaneStream(nil, "a", "g1", left)
	defer func() {
		lane.RetireLogical()
		lane.CollectPhysical()
	}()
	go func() {
		first, _ := json.Marshal(LaneFrame{Kind: LanePlanPoke, RequestID: "one"})
		second, _ := json.Marshal(LaneFrame{Kind: LanePlanPoke, RequestID: "two"})
		payload := append(append(first, '\n'), append(second, '\n')...)
		_, _ = right.Write(payload)
	}()
	var first, second LaneFrame
	if err := lane.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := lane.Decode(&second); err != nil {
		t.Fatal(err)
	}
	if first.RequestID != "one" || second.RequestID != "two" {
		t.Fatalf("decoded frames out of order: %#v %#v", first, second)
	}
}

func TestRetireLaneIsExactObjectAndIdempotent(t *testing.T) {
	firstLocal, firstRemote := net.Pipe()
	secondLocal, secondRemote := net.Pipe()
	defer firstRemote.Close()
	defer secondRemote.Close()
	g1 := newLaneStream(nil, "a", "g1", firstLocal)
	g2 := newLaneStream(nil, "a", "g2", secondLocal)
	current := g2
	g1.SetRetire(func(exact *LaneStream) {
		if current == exact {
			current = nil
		}
	})
	g1.RetireLogical()
	g1.RetireLogical()
	if current != g2 || g2.Retired() {
		t.Fatal("late g1 retirement changed g2")
	}
	g1.CollectPhysical()
	g2.RetireLogical()
	g2.CollectPhysical()
}

func TestLane_StaleGenerationFenced(t *testing.T) {
	binding := &laneActorBinding{current: func() bool { return false }}
	if err := binding.Deliver(&message.Envelope{}); !errors.Is(err, errLinkClosed) {
		t.Fatalf("stale lane delivery=%v, want closed fence", err)
	}
	// A stale cancel is deliberately dropped before it can reach the endpoint.
	binding.CancelRequest("stale-request")
}

func TestClosedWireVocabularyRejectsSmuggledPayloads(t *testing.T) {
	if err := (DeviceStreamHeader{
		Kind: DeviceStreamActor, ProtoVersion: ProtocolVersion,
		Channel: "a", LaneGen: "g1",
	}).Validate(); err == nil {
		t.Fatal("actor header accepted a carrier-only protocol field")
	}
	if err := (SpineFrame{
		Kind: SpineCompartmentPlanPoke, Reason: "smuggled",
	}).Validate(); err == nil {
		t.Fatal("compartment_plan_poke accepted a payload")
	}
	// A channel named twice would make "absent from both lists" ambiguous, and
	// absence is the only thing that makes the device retire a compartment.
	if err := (SpineFrame{
		Kind: SpineCompartmentPlanReply, Nonce: "n",
		Serve: []channel.ID{"a"}, Unknown: []channel.ID{"a"},
	}).Validate(); err == nil {
		t.Fatal("compartment plan named one channel in both lists")
	}
	if err := (SpineFrame{
		Kind: SpineCompartmentPlanReply, Nonce: "n", Serve: []channel.ID{""},
	}).Validate(); err == nil {
		t.Fatal("compartment plan named an empty channel")
	}
	if err := (LaneFrame{
		Kind: LaneAllocRequest, RequestID: "outer",
		AllocRequest: &AllocRequest{RequestID: "inner", Coord: "coord"},
	}).Validate(); err == nil {
		t.Fatal("lane frame accepted mismatched correlation ids")
	}
	if err := (LaneFrame{
		Kind: LanePlanPoke, PlanReply: &PlanReply{},
	}).Validate(); err == nil {
		t.Fatal("plan_poke accepted an unrelated payload")
	}
}

func TestCarrier_MixedProtocolVersionRejected(t *testing.T) {
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).
			Upgrade(w, r, nil)
		if err != nil {
			result <- err
			return
		}
		_, err = AcceptDeviceCarrier(ws, nil)
		result <- err
	}))
	defer server.Close()
	ws, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	session, err := yamux.Client(newWSByteStream(ws), linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	spine, err := session.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStreamJSON(spine, DeviceStreamHeader{
		Kind: DeviceStreamCarrier, ProtoVersion: ProtocolVersion - 1,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrProtocolVersion) {
			t.Fatalf("mixed-version verdict=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("mixed-version carrier was not rejected")
	}
}

type serialWriteConn struct {
	active atomic.Int32
	max    atomic.Int32
}

func (*serialWriteConn) Read([]byte) (int, error) { return 0, context.Canceled }
func (c *serialWriteConn) Write(p []byte) (int, error) {
	active := c.active.Add(1)
	for {
		max := c.max.Load()
		if active <= max || c.max.CompareAndSwap(max, active) {
			break
		}
	}
	time.Sleep(time.Millisecond)
	c.active.Add(-1)
	return len(p), nil
}
func (*serialWriteConn) Close() error                     { return nil }
func (*serialWriteConn) LocalAddr() net.Addr              { return testAddr("local") }
func (*serialWriteConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (*serialWriteConn) SetDeadline(time.Time) error      { return nil }
func (*serialWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (*serialWriteConn) SetWriteDeadline(time.Time) error { return nil }

func TestLaneConcurrentWritersUseOneDeadlineOwner(t *testing.T) {
	conn := &serialWriteConn{}
	lane := newLaneStream(nil, "a", "g1", conn)
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := lane.Send(LaneFrame{Kind: LanePlanPoke}); err != nil {
				t.Errorf("send: %v", err)
			}
		}()
	}
	group.Wait()
	if got := conn.max.Load(); got != 1 {
		t.Fatalf("concurrent writes entered the stream %d at a time", got)
	}
	lane.RetireLogical()
	lane.CollectPhysical()
}

func TestBoundedControlDecoderRejectsOversizedFrameAndPreservesBoundaries(t *testing.T) {
	const limit = 64
	decoder := newBoundedJSONDecoder(strings.NewReader(
		"{\"value\":\"one\"}\n{\"value\":\"two\"}\n",
	), limit)
	var first, second struct {
		Value string `json:"value"`
	}
	if err := decoder.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&second); err != nil {
		t.Fatal(err)
	}
	if first.Value != "one" || second.Value != "two" {
		t.Fatalf("decoded frames = %q, %q", first.Value, second.Value)
	}

	oversized := newBoundedJSONDecoder(
		strings.NewReader(strings.Repeat("x", limit+1)+"\n"), limit,
	)
	var value any
	if err := oversized.Decode(&value); !errors.Is(err, errControlFrameTooLarge) {
		t.Fatalf("oversized frame error = %v, want %v", err, errControlFrameTooLarge)
	}
	if maxControlFrameBytes != 1<<24 {
		t.Fatalf("production control frame limit = %d, want 16MiB", maxControlFrameBytes)
	}
	conn := &serialWriteConn{}
	carrier := newRawCarrier(nil, conn, nil)
	lane := newLaneStream(carrier, "a", "g1", conn)
	if carrier.spineDecoder.max != maxControlFrameBytes ||
		lane.decoder.max != maxControlFrameBytes {
		t.Fatalf("production decoder limits: spine=%d lane=%d want=%d",
			carrier.spineDecoder.max, lane.decoder.max, maxControlFrameBytes)
	}
}

type failingSpineConn struct {
	onClose func()
}

func (*failingSpineConn) Read([]byte) (int, error) {
	return 0, context.Canceled
}
func (*failingSpineConn) Write([]byte) (int, error) {
	return 0, errors.New("injected spine write failure")
}
func (c *failingSpineConn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}
func (*failingSpineConn) LocalAddr() net.Addr              { return testAddr("local") }
func (*failingSpineConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (*failingSpineConn) SetDeadline(time.Time) error      { return nil }
func (*failingSpineConn) SetReadDeadline(time.Time) error  { return nil }
func (*failingSpineConn) SetWriteDeadline(time.Time) error { return nil }

func TestSpineWriteFailureClosesAfterReleasingSendLock(t *testing.T) {
	conn := &failingSpineConn{}
	carrier := newRawCarrier(nil, conn, nil)
	var closeSawUnlocked atomic.Bool
	conn.onClose = func() {
		if carrier.spineSend.TryLock() {
			closeSawUnlocked.Store(true)
			carrier.spineSend.Unlock()
		}
	}

	if err := carrier.sendSpine(SpineFrame{Kind: SpineProbe, Nonce: "probe"}); err == nil {
		t.Fatal("injected spine write failure reported success")
	}
	if !closeSawUnlocked.Load() {
		t.Fatal("sendSpine called Close while holding spineSend")
	}
	if !carrier.sealed.Load() {
		t.Fatal("spine write failure did not close the carrier")
	}
}
