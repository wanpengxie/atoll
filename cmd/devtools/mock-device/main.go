// Command mock-device is a standalone WS client that stands in for a real
// browser-extension device, so an adapter's end-to-end path can be exercised
// without a real browser. It connects to a live adapter's PRIVATE device
// endpoint, reads down-frames (commands the adapter pushes), and replies with
// canned ok up-frames keyed by cmd.
//
// The transport machine (dial, reconnect backoff, read/write loop) is
// adapter-neutral and shared. What differs per adapter is ONLY the canned
// business result — that lives in a per-adapter profile selected by --adapter.
//
// Wire (mirrors each adapter's wire.go — the adapter's PRIVATE device language):
//
//	down (adapter → device): {correlation_id, cmd, params}
//	up   (device → adapter): {correlation_id, ok, result}        on success
//	                         {correlation_id, ok:false, error}   on failure
//
// Usage:
//
//	go run ./cmd/devtools/mock-device --adapter xhs              # default :8090
//	go run ./cmd/devtools/mock-device --adapter kimi             # default :8091
//	go run ./cmd/devtools/mock-device --adapter kimi --addr 127.0.0.1:9000
//
// The endpoint is keyless (the adapter trusts loopback), so no credential flag.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/actors/kimi"
	"github.com/wanpengxie/ActOS/actors/xhs"
)

// downFrame is the command the adapter pushes down to the device.
type downFrame struct {
	CorrelationID string          `json:"correlation_id"`
	Cmd           string          `json:"cmd"`
	Params        json.RawMessage `json:"params"`
}

// upFrame is the reply the device sends back up.
type upFrame struct {
	CorrelationID string          `json:"correlation_id"`
	OK            bool            `json:"ok"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         *upError        `json:"error,omitempty"`
}

type upError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// profile is one adapter's device persona: where its endpoint listens by default
// and how it answers each command. The transport loop is identical across
// profiles; only reply differs.
type profile struct {
	defaultAddr string
	reply       func(down downFrame) upFrame
}

// profiles enumerates the adapters this mock can stand in for. The default addr
// is pulled from the adapter package itself (single source of truth — no drift).
var profiles = map[string]profile{
	"xhs":  {defaultAddr: xhs.DefaultListenAddr, reply: xhsReply},
	"kimi": {defaultAddr: kimi.DefaultListenAddr, reply: kimiReply},
}

// okResult builds an ok up-frame carrying result for the given down-frame.
func okResult(down downFrame, result map[string]any) upFrame {
	raw, _ := json.Marshal(result)
	return upFrame{CorrelationID: down.CorrelationID, OK: true, Result: raw}
}

// xhsReply answers the xhs adapter's outward device verbs (see actors/xhs/types.go):
// search / publish / get-note / get-my-recent. An unknown cmd still gets an ok
// reply with an empty result, so the adapter never stalls.
func xhsReply(down downFrame) upFrame {
	var result map[string]any
	switch down.Cmd {
	case "search":
		result = map[string]any{
			"results": []map[string]any{{"note_id": "n1", "title": "mock"}},
		}
	case "publish":
		result = map[string]any{
			"status":  "completed",
			"note_id": "mock1",
			"url":     "https://www.xiaohongshu.com/explore/mock1",
		}
	case "get-note":
		result = map[string]any{
			"note": map[string]any{"note_id": "n1", "title": "mock", "content": "mock body"},
		}
	case "get-my-recent":
		result = map[string]any{
			"notes": []map[string]any{{"note_id": "n1", "title": "mock"}},
		}
	default:
		result = map[string]any{}
	}
	return okResult(down, result)
}

// kimiReply answers the kimi adapter's 13 browser-primitive actions (see
// actors/kimi/types.go). screenshot / save_as_pdf return a LOCAL file path (the
// device writes to disk; bytes do not cross the wire). An unknown cmd still gets
// an ok reply with an empty result, so the adapter never stalls.
func kimiReply(down downFrame) upFrame {
	var result map[string]any
	switch down.Cmd {
	case "navigate":
		result = map[string]any{"url": "https://example.com", "title": "Example"}
	case "find_tab":
		result = map[string]any{"tab": map[string]any{"id": 1, "url": "https://example.com"}}
	case "snapshot":
		result = map[string]any{"snapshot": "<html mock>"}
	case "click", "fill", "upload", "close_tab", "close_session":
		result = map[string]any{"ok": true}
	case "evaluate":
		result = map[string]any{"value": "mock-eval-result"}
	case "screenshot":
		result = map[string]any{"path": "/tmp/kimi-mock-screenshot.png"}
	case "save_as_pdf":
		result = map[string]any{"path": "/tmp/kimi-mock.pdf"}
	case "network":
		result = map[string]any{"requests": []map[string]any{}}
	case "list_tabs":
		result = map[string]any{
			"tabs": []map[string]any{{"id": 1, "url": "https://example.com", "title": "Example"}},
		}
	default:
		result = map[string]any{}
	}
	return okResult(down, result)
}

func main() {
	adapter := flag.String("adapter", "", "which adapter to mock: "+adapterList())
	addr := flag.String("addr", "", "adapter device endpoint host:port (default: the adapter's own default)")
	flag.Parse()

	prof, ok := profiles[*adapter]
	if !ok {
		log.Fatalf("mock-device: --adapter must be one of %s (got %q)", adapterList(), *adapter)
	}
	endpoint := *addr
	if endpoint == "" {
		endpoint = prof.defaultAddr
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	u := url.URL{Scheme: "ws", Host: endpoint, Path: "/device"}

	const initialBackoff = 500 * time.Millisecond
	const maxBackoff = 30 * time.Second
	backoff := initialBackoff

	for ctx.Err() == nil {
		err := runOnce(ctx, u.String(), prof.reply, func() { backoff = initialBackoff })
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			log.Printf("disconnected: %v — reconnecting in %s", err, backoff)
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	log.Print("shutdown")
}

// runOnce dials the endpoint and serves down-frames until the connection drops
// or ctx is cancelled. On a successful dial it calls onConnected so the caller
// can reset its reconnect backoff: a long-lived connection that finally drops
// should retry from the initial delay, not from the historically accumulated one.
func runOnce(ctx context.Context, wsURL string, reply func(downFrame) upFrame, onConnected func()) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	onConnected()
	log.Printf("connected to %s", wsURL)

	// Close the conn when ctx is cancelled so the blocking ReadJSON unwinds.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		var down downFrame
		if err := conn.ReadJSON(&down); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		log.Printf("down: correlation_id=%s cmd=%s params=%s",
			down.CorrelationID, down.Cmd, string(down.Params))

		up := reply(down)
		if err := conn.WriteJSON(up); err != nil {
			return err
		}
		log.Printf("up:   correlation_id=%s ok=%t", up.CorrelationID, up.OK)
	}
}

// adapterList returns the sorted, comma-joined set of known adapter names (for
// flag help + the unknown-adapter error).
func adapterList() string {
	names := make([]string, 0, len(profiles))
	for n := range profiles {
		names = append(names, n)
	}
	sort.Strings(names)
	return fmt.Sprintf("{%s}", strings.Join(names, "|"))
}
