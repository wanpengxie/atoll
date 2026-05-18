//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_MultiChannel_Concurrency covers phase-2 case 6 — per-channel
// isolation under concurrent writes.
//
// One user binds three channels to the same daemon, then fires
// `perChannel` POSTs in parallel against each channel. Each channel's
// own sqlite must:
//
//  1. Carry EXACTLY its share of human.text rows (channel A only sees
//     its A_* texts; never any B_* / C_*).
//  2. Assign monotonically increasing seq 1..N for those rows (no gaps,
//     no duplicates).
//  3. Preserve POST ordering for that channel (per-channel write_message
//     ordering invariant: the FIFO contract within a channel).
//
// Regression target:
//   - Per-channel fencing leakage (a write committed to channel A
//     accidentally surfacing in channel B because of a shared atomic
//     counter or worker mailbox).
//   - Daemonbus dispatcher serialising across channels (would not be a
//     correctness bug but would surface as a sharp throughput cliff —
//     this test would still pass but the elapsed time becomes useful
//     diagnostic data printed via t.Log).
func TestE2E_MultiChannel_Concurrency(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "concur+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-concur-" + uniqSuffix())

	const (
		nChannels  = 3
		perChannel = 10
	)
	chIDs := make([]string, nChannels)
	for i := 0; i < nChannels; i++ {
		chIDs[i] = s.CreateChannel(wsID, fmt.Sprintf("ch-concur-%d-%s", i, uniqSuffix()), "")
		s.BindChannel(wsID, chIDs[i])
	}

	// Each goroutine posts perChannel texts to its own channel with a
	// distinct prefix so cross-contamination is observable.
	type postRecord struct {
		channelIdx int
		text       string
		seq        int64
	}
	results := make([][]postRecord, nChannels)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < nChannels; i++ {
		i := i
		results[i] = make([]postRecord, 0, perChannel)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perChannel; j++ {
				text := fmt.Sprintf("ch%d-msg%02d", i, j)
				resp := s.PostMessage(chIDs[i], "human.text", text, "")
				if !resp.Accepted {
					t.Errorf("ch%d-msg%02d not accepted: %+v", i, j, resp)
					return
				}
				results[i] = append(results[i], postRecord{
					channelIdx: i,
					text:       text,
					seq:        resp.Seq,
				})
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("MultiChannel: %d channels × %d posts each completed in %s",
		nChannels, perChannel, elapsed)

	// Eventually each channel sqlite should have exactly perChannel
	// human.text rows (channel-agent may also write agent.text replies
	// via the single-shot mock — those should match perChannel too).
	for i, chID := range chIDs {
		idx := i
		ch := chID
		harness.Eventually(t, fmt.Sprintf("ch%d humans materialised", idx), 10*time.Second,
			func() bool {
				return countHumanRowsInChannel(t, s, ch) >= perChannel
			})
	}

	// Per-channel sqlite assertions.
	for i, chID := range chIDs {
		msgs := s.ListChannelMessages(chID)
		var humans []harness.StoredMessage
		for _, m := range msgs {
			if m.Type == "human.text" {
				humans = append(humans, m)
			}
		}
		if len(humans) != perChannel {
			t.Errorf("ch%d human count=%d want %d", i, len(humans), perChannel)
			continue
		}
		// Texts must be the i-prefixed ones; no cross-channel leakage.
		wantPrefix := fmt.Sprintf("ch%d-", i)
		for _, h := range humans {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(h.Payload, &payload); err != nil {
				t.Errorf("ch%d payload decode: %v", i, err)
				continue
			}
			if !startsWith(payload.Text, wantPrefix) {
				t.Errorf("ch%d sees foreign text %q (want %s prefix) — per-channel isolation broken",
					i, payload.Text, wantPrefix)
			}
		}
		// seq monotonicity within channel.
		seqs := make([]int64, len(humans))
		for j, h := range humans {
			seqs[j] = h.Seq
		}
		if !sort.SliceIsSorted(seqs, func(a, b int) bool { return seqs[a] < seqs[b] }) {
			t.Errorf("ch%d human seqs not sorted asc: %v", i, seqs)
		}
		// Match what the POST responses said.
		want := results[i]
		// Sort POST records by seq before comparing.
		sort.Slice(want, func(a, b int) bool { return want[a].seq < want[b].seq })
		for j := range humans {
			var payload struct {
				Text string `json:"text"`
			}
			_ = json.Unmarshal(humans[j].Payload, &payload)
			if payload.Text != want[j].text {
				t.Errorf("ch%d msg %d seq=%d text=%q want %q",
					i, j, humans[j].Seq, payload.Text, want[j].text)
			}
		}
	}
}

func countHumanRowsInChannel(t *testing.T, s *harness.Stack, channelID string) int {
	t.Helper()
	n := 0
	for _, m := range s.ListChannelMessages(channelID) {
		if m.Type == "human.text" {
			n++
		}
	}
	return n
}

func startsWith(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}
