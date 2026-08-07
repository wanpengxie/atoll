package base

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const (
	ResumeSeedKey        = "agent.resume-seed"
	persistRetryInitial  = 100 * time.Millisecond
	persistRetryMax      = 5 * time.Second
	persistLoudThreshold = 5
)

const ObsCheckpointDrop actorrt.ObsKind = "agentbase.checkpoint_drop"

func readSeed(sys actorbase.Sys) []byte {
	out, err := sys.State().Get(resource.ResourceID(ResumeSeedKey))
	if err != nil || !out.Found || len(out.Value) == 0 {
		return nil
	}
	return append([]byte(nil), out.Value...)
}

// persistCoordinator prevents an older retrying checkpoint from overwriting a
// newer value. Puts are serialized only for the tiny state write; retry waits
// never hold the lock.
type persistCoordinator struct {
	mu  sync.Mutex
	seq uint64
}

func (p *persistCoordinator) submit(sys actorbase.Sys, key string, value []byte) {
	if key == "" {
		key = ResumeSeedKey
	}
	value = append([]byte(nil), value...)
	p.mu.Lock()
	p.seq++
	seq := p.seq
	p.mu.Unlock()
	go func() {
		delay, failures := persistRetryInitial, 0
		for {
			p.mu.Lock()
			if seq != p.seq {
				p.mu.Unlock()
				return
			}
			out, err := sys.State().Put(resource.ResourceID(key), value)
			p.mu.Unlock()
			if err == nil && out.Accepted() {
				return
			}
			failures++
			if failures == persistLoudThreshold || failures%persistLoudThreshold == 0 {
				publishCheckpointDrop(sys, key, failures, out, err)
			}
			if !waitPersistTimer(sys.Life(), delay) {
				return
			}
			if delay < persistRetryMax {
				delay *= 2
				if delay > persistRetryMax {
					delay = persistRetryMax
				}
			}
		}
	}()
}

type persistWait func(context.Context, time.Duration) bool

func waitPersistTimer(ctx context.Context, delay time.Duration) bool {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func persistLoop(sys actorbase.Sys, key string, value []byte, wait persistWait) {
	delay := persistRetryInitial
	failures := 0
	for {
		out, err := sys.State().Put(resource.ResourceID(key), value)
		if err == nil && out.Accepted() {
			return
		}
		failures++
		if failures == persistLoudThreshold || failures%persistLoudThreshold == 0 {
			publishCheckpointDrop(sys, key, failures, out, err)
		}
		if !wait(sys.Life(), delay) || sys.Life().Err() != nil {
			return
		}
		if delay < persistRetryMax {
			delay *= 2
			if delay > persistRetryMax {
				delay = persistRetryMax
			}
		}
	}
}

func publishCheckpointDrop(sys actorbase.Sys, key string, failures int, out accessdoor.Outcome, err error) {
	detail := map[string]any{"key": key, "consecutive_failures": failures, "reject_reason": string(out.RejectReason)}
	if err != nil {
		detail["error"] = err.Error()
	}
	val, _ := json.Marshal(detail)
	_ = sys.PublishObs(ObsCheckpointDrop, val)
}
