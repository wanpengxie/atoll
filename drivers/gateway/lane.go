package gateway

import (
	"sync"
)

// Lane constants (build spec §S3, 照 ws.go outbound/wsWriteWait/慢消费者三常数
// 逐字平移). The lane is the十字's north face: one活连接's瞬时 outbound queue.
const (
	// laneCapacity bounds the outbound queue depth. A temporarily full queue applies
	// backpressure to its producers; the connector's bounded writer owns the actual
	// slow-peer verdict and closes the Session on its wire deadline.
	laneCapacity = 64
	// backfillLaneCapacity deliberately stays shallow: history readers wait here
	// instead of occupying the live lane or growing unbounded memory.
	backfillLaneCapacity = 16
	// historyLaneCapacity is the interactive/page reservoir lane. It remains
	// bounded, but is distinct from attach backfill so an upward read in the
	// focused channel cannot sit behind every background channel's initial tail.
	historyLaneCapacity = 16
)

// lane is one connection's outbound queue (§5.6 lane column). The gateway read
// pump enqueues serialized downstream frames; the connector's writer goroutine
// drains out to the wire. Full is not itself evidence of a slow peer: the producer
// waits for room while memory remains bounded. A connector write failure/deadline or
// Session.Close seals the lane and releases every blocked producer.
type lane struct {
	live     chan []byte
	history  chan []byte
	backfill chan []byte
	closed   chan struct{}
	once     sync.Once

	// cursor is this lane's read position (map<channel, seq> — 游标在设备,
	// server 零持久化). Touched only by the read pump goroutine, so no lock.
	cursor *cursor
}

func newLane(cur *cursor) *lane {
	return &lane{
		live:     make(chan []byte, laneCapacity),
		history:  make(chan []byte, historyLaneCapacity),
		backfill: make(chan []byte, backfillLaneCapacity),
		closed:   make(chan struct{}),
		cursor:   cur,
	}
}

// push enqueues one serialized downstream frame with bounded-memory backpressure.
// A full queue blocks rather than guessing that a runnable writer is slow; the
// connector's existing wire deadline makes the slow-consumer decision and closes the
// Session. Closing the lane releases a blocked push with ok=false, so Gateway.Close
// never waits for queue space.
func (l *lane) push(b []byte) (ok bool) { return l.pushTo(l.live, b) }

func (l *lane) pushBackfill(b []byte) (ok bool) { return l.pushTo(l.backfill, b) }

func (l *lane) pushHistory(b []byte) (ok bool) { return l.pushTo(l.history, b) }

func (l *lane) pushTo(dst chan<- []byte, b []byte) (ok bool) {
	select {
	case <-l.closed:
		return false
	default:
	}
	select {
	case dst <- b:
		return true
	case <-l.closed:
		return false
	}
}

// close idempotently tears the lane down (unblocks any drainer / pusher).
func (l *lane) close() { l.once.Do(func() { close(l.closed) }) }
