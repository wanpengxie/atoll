package gateway

import (
	"sync"
	"sync/atomic"
)

// Lane constants (build spec §S3, 照 ws.go outbound/wsWriteWait/慢消费者三常数
// 逐字平移). The lane is the十字's north face: one活连接's瞬时 outbound queue.
const (
	// LaneCapacity bounds the outbound queue depth. A slow reader that lets the
	// queue fill past this is disconnected rather than allowed to pin memory
	// unboundedly (满策略 = 断连, 照 ws.go outbound cap 64).
	LaneCapacity = 64
	// LaneWriteTimeoutMs is how long one downstream write may block a drainer
	// before the lane is torn down (照 ws.go wsWriteWait 10s). Law条 F: this is
	// strictly < the arm seal join budget (ArmSealJoinTimeout) so a slow lane
	// dies BEFORE it can drag out a detach seal.
	LaneWriteTimeoutMs = 10_000
)

// lane is one connection's outbound queue (§5.6 lane column). The gateway read
// pump enqueues serialized downstream frames; the connector's writer goroutine
// drains out to the wire. Full → the enqueue is DROPPED and the lane SEALS
// ITSELF (满 → 自封断连, see push); the drop is counted (obs记账, DoD-11).
type lane struct {
	out    chan []byte
	closed chan struct{}
	once   sync.Once

	// cursor is this lane's read position (map<channel, seq> — 游标在设备,
	// server 零持久化). Touched only by the read pump goroutine, so no lock.
	cursor *cursor

	dropped atomic.Int64
}

func newLane(cur *cursor) *lane {
	return &lane{
		out:    make(chan []byte, LaneCapacity),
		closed: make(chan struct{}),
		cursor: cur,
	}
}

// push enqueues one serialized downstream frame. ok=false means the lane is
// closed — including by THIS call: a full queue seals the lane itself (满 →
// 自封断连). A dropped frame is a hole in the device's tail that only a
// reconnect-with-cursor can heal, so the lane is doomed the moment one drops;
// making the overflow CLOSE the lane keeps that verdict structural — the
// drainer unblocks and the connector tears the session down off Done() even if
// a push caller forgot to act on ok=false (断连权威在 lane，不在调用方约定).
func (l *lane) push(b []byte) (ok bool) {
	select {
	case <-l.closed:
		return false
	default:
	}
	select {
	case l.out <- b:
		return true
	case <-l.closed:
		return false
	default:
		l.dropped.Add(1)
		l.close()
		return false
	}
}

// drain returns the next queued frame, or ok=false when the lane closes (the
// connector's writer goroutine selects on this + its own write deadline).
func (l *lane) drain() (<-chan []byte, <-chan struct{}) { return l.out, l.closed }

// close idempotently tears the lane down (unblocks any drainer / pusher).
func (l *lane) close() { l.once.Do(func() { close(l.closed) }) }

// isClosed reports whether the lane has been torn down.
func (l *lane) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

// DroppedCount reports how many downstream frames this lane dropped to a full
// queue over its lifetime (obs记账, DoD-11 三断言之一).
func (l *lane) DroppedCount() int64 { return l.dropped.Load() }
