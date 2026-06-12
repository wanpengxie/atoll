// Command mock-xhs-device is a standalone WS client that stands in for the real
// xhs browser extension. It connects to a live xhs adapter's private device
// endpoint, reads down-frames (commands the adapter pushes), and replies with
// canned ok up-frames keyed by cmd. It is a manual smoke tool: point it at a
// running daemon's tool:xhs cell and exercise xhs.* requests end to end without
// a real browser.
//
// Wire (mirrors actors/xhs/wire.go — the adapter's PRIVATE device language):
//
//	down (adapter → device): {correlation_id, cmd, params}
//	up   (device → adapter): {correlation_id, ok, result}        on success
//	                         {correlation_id, ok:false, error}   on failure
//
// Usage:
//
//	go run ./cmd/mock-xhs-device --addr 127.0.0.1:8090
//
// The endpoint is keyless (the adapter trusts loopback), so no credential flag.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/url"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
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

// CannedReply builds the canned ok up-frame the mock device returns for a given
// down-frame. The cmd verbs are the adapter's outward device language (see
// actors/xhs/types.go): search / publish / get-note / get-my-recent. An unknown
// cmd still gets an ok reply with an empty result, so the adapter never stalls.
//
// It is exported (and deliberately tiny) so the live integration test can drive
// the exact same device behaviour through the real WS path.
func CannedReply(down downFrame) upFrame {
	var result map[string]any
	switch down.Cmd {
	case "search":
		result = map[string]any{
			"results": []map[string]any{
				{"note_id": "n1", "title": "mock"},
			},
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
			"notes": []map[string]any{
				{"note_id": "n1", "title": "mock"},
			},
		}
	default:
		result = map[string]any{}
	}
	raw, _ := json.Marshal(result)
	return upFrame{CorrelationID: down.CorrelationID, OK: true, Result: raw}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "adapter device endpoint host:port")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	u := url.URL{
		Scheme: "ws",
		Host:   *addr,
		Path:   "/device",
	}

	const initialBackoff = 500 * time.Millisecond
	const maxBackoff = 30 * time.Second
	backoff := initialBackoff

	for ctx.Err() == nil {
		err := runOnce(ctx, u.String(), func() { backoff = initialBackoff })
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
func runOnce(ctx context.Context, wsURL string, onConnected func()) error {
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

		up := CannedReply(down)
		if err := conn.WriteJSON(up); err != nil {
			return err
		}
		log.Printf("up:   correlation_id=%s ok=%t", up.CorrelationID, up.OK)
	}
}
