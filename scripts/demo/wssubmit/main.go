// Command wssubmit is the demo's websocket leg. Pure curl cannot speak ws, so
// scripts/demo-curl.sh shells out here for the live half of the closed loop:
// attach (contract version comes back in the receipt), one submit with a
// self-minted idempotency id + intent riding the agent-message payload, then
// wait on the live feed until the scripted agent's kind=response lands.
// On success it prints the minted message id to stdout for the paged-read
// verification back in the shell script.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type frame struct {
	V       int             `json:"v"`
	Type    string          `json:"frame_type"`
	Ref     string          `json:"ref,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func main() {
	base := flag.String("base", "http://127.0.0.1:8832", "server base URL")
	token := flag.String("token", "", "bearer token")
	channelID := flag.String("channel", "", "channel id")
	actor := flag.String("actor", "", "audience actor id")
	msgType := flag.String("msgtype", "human.message", "message type (the addressed actor's vocabulary)")
	text := flag.String("text", "hello", "message text")
	flag.Parse()
	if *token == "" || *channelID == "" || *actor == "" {
		log.Fatal("wssubmit: -token, -channel and -actor are required")
	}

	wsURL := strings.Replace(*base, "http", "ws", 1) + "/ws"
	hdr := http.Header{"Authorization": {"Bearer " + *token}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, hdr)
	if err != nil {
		log.Fatalf("wssubmit: dial %s: %v", wsURL, err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	send := func(typ, ref string, payload any) {
		raw, err := json.Marshal(payload)
		if err != nil {
			log.Fatalf("wssubmit: marshal %s payload: %v", typ, err)
		}
		b, _ := json.Marshal(frame{V: 2, Type: typ, Ref: ref, Payload: raw})
		if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
			log.Fatalf("wssubmit: write %s: %v", typ, err)
		}
	}
	read := func() (frame, map[string]any) {
		_, b, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("wssubmit: read: %v", err)
		}
		var f frame
		if err := json.Unmarshal(b, &f); err != nil {
			log.Fatalf("wssubmit: frame decode: %v", err)
		}
		var p map[string]any
		_ = json.Unmarshal(f.Payload, &p)
		return f, p
	}

	// attach → receipt(ref, contract_version): the FSM's only handshake form.
	send("attach", "attach-1", map[string]any{"since": map[string]int64{*channelID: 0}})
	f, p := read()
	if f.Type != "receipt" || f.Ref != "attach-1" {
		log.Fatalf("wssubmit: attach answer = %+v %v", f, p)
	}
	log.Printf("wssubmit: attached, contract_version=%v", p["contract_version"])

	// One submit: self-minted id (idempotency key), intent inside the payload
	// (agent vocabulary — never an envelope field).
	msgID := uuid.NewString()
	send("submit", "demo-1", map[string]any{
		"channel_id": *channelID, "id": msgID, "msg_type": *msgType,
		"kind": "request", "audience": []string{*actor},
		"payload": map[string]any{"text": *text, "intent": "steer"},
	})

	for {
		f, p := read()
		switch f.Type {
		case "receipt":
			if f.Ref == "demo-1" {
				log.Printf("wssubmit: submit acked message_id=%v", p["message_id"])
			}
		case "error":
			log.Fatalf("wssubmit: error frame: %v", p)
		case "feed":
			// Only OUR request's answer counts: response pairing is parent_id ==
			// the submitted message id (any other kind=response — replayed
			// history, tool terminals — must not end the demo).
			env, _ := p["envelope"].(map[string]any)
			if env != nil && env["kind"] == "response" && env["parent_id"] == msgID {
				log.Printf("wssubmit: agent response landed (correlation=%v)", env["correlation_id"])
				fmt.Println(msgID)
				return
			}
		}
	}
}
