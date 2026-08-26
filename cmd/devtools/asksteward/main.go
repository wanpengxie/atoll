// asksteward logs in as root on a running node, finds the c0 steward and
// sends it one agent.ask, printing everything the channel says back. It is a
// developer smoke tool for the agent line (prompt injection, tool injection),
// not a user-facing command.
//
//	go run ./cmd/devtools/asksteward --addr 127.0.0.1:38419 --password <root pw> "who are you?"
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wanpengxie/atoll/platform/subjectgate"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:38419", "node http address")
	password := flag.String("password", os.Getenv("ATOLL_ROOT_PASSWORD"), "root password (or ATOLL_ROOT_PASSWORD)")
	channelID := flag.String("channel", "c0", "channel id")
	target := flag.String("actor", "", "target actor id (default: the channel's first agent member)")
	msgType := flag.String("type", "agent.ask", "request type")
	timeout := flag.Duration("timeout", 5*time.Minute, "how long to wait for the final answer")
	flag.Parse()
	text := strings.Join(flag.Args(), " ")
	if text == "" {
		log.Fatal("usage: asksteward [flags] <text>")
	}
	base := "http://" + *addr
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 30 * time.Second}
	login := map[string]string{"email": "root@atoll.local", "password": *password}
	raw, _ := json.Marshal(login)
	resp, err := client.Post(base+"/api/identity/login", "application/json", bytes.NewReader(raw))
	if err != nil {
		log.Fatalf("login: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("login: status=%d body=%s", resp.StatusCode, body)
	}
	u, _ := url.Parse(base)
	var cookies []string
	for _, c := range jar.Cookies(u) {
		cookies = append(cookies, c.Name+"="+c.Value)
	}
	headers := http.Header{}
	headers.Set("Cookie", strings.Join(cookies, "; "))
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+*addr+"/ws", headers)
	if err != nil {
		log.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()
	acks := make(chan map[string]any, 64)
	feed := make(chan map[string]any, 1024)
	go func() {
		for {
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err != nil {
				close(feed)
				return
			}
			switch frame["frame_type"] {
			case "receipt", "error":
				acks <- frame
			case "feed":
				if p, ok := frame["payload"].(map[string]any); ok {
					feed <- p
				}
			}
		}
	}()
	send := func(ref string, frameType string, payload any) map[string]any {
		frame := map[string]any{"v": subjectgate.FrameVersion, "frame_type": frameType, "ref": ref, "payload": payload}
		if err := conn.WriteJSON(frame); err != nil {
			log.Fatalf("write %s: %v", ref, err)
		}
		deadline := time.After(15 * time.Second)
		for {
			select {
			case ack := <-acks:
				if ack["ref"] == ref {
					return ack
				}
			case <-deadline:
				log.Fatalf("no ack for %s", ref)
			}
		}
	}
	if ack := send("attach", "attach", map[string]any{
		"since": map[string]int64{}, "focus": *channelID, "history_protocol": subjectgate.FrameVersion,
	}); ack["frame_type"] != "receipt" {
		log.Fatalf("attach rejected: %v", ack)
	}
	submit := func(ref, typ, audience string, payload any) string {
		ack := send(ref, "submit", map[string]any{"channel_id": *channelID, "msg_type": typ, "kind": "request", "visibility": "public", "payload": payload, "audience": []string{audience}})
		if ack["frame_type"] == "error" {
			log.Fatalf("%s rejected: %v", ref, ack["payload"])
		}
		receipt, _ := ack["payload"].(map[string]any)
		id, _ := receipt["message_id"].(string)
		return id
	}
	await := func(parent string, timeout time.Duration, onOther func(map[string]any)) map[string]any {
		deadline := time.After(timeout)
		for {
			select {
			case item, ok := <-feed:
				if !ok {
					log.Fatal("ws closed")
				}
				env, _ := item["envelope"].(map[string]any)
				if env == nil {
					continue
				}
				if env["parent_id"] == parent && env["kind"] == "response" {
					p, _ := env["payload"].(map[string]any)
					if p["status"] == "completed" || p["status"] == "failed" {
						return env
					}
				}
				if onOther != nil {
					onOther(env)
				}
			case <-deadline:
				log.Fatalf("no terminal within %s", timeout)
			}
		}
	}
	actorID := *target
	if actorID == "" {
		id := submit("members", "system.member.list", "system", map[string]any{})
		env := await(id, 15*time.Second, nil)
		p, _ := env["payload"].(map[string]any)
		raw, _ := json.Marshal(p)
		var reply struct {
			Actors []struct {
				ID   string `json:"id"`
				Kind string `json:"kind"`
			} `json:"actors"`
		}
		_ = json.Unmarshal(raw, &reply)
		for _, m := range reply.Actors {
			if m.Kind == "agent" {
				actorID = m.ID
				break
			}
		}
		if actorID == "" {
			log.Fatalf("no agent member in %s; member.list reply=%s", *channelID, raw)
		}
	}
	fmt.Printf("→ %s %s: %s\n", *msgType, actorID, text)
	start := time.Now()
	id := submit("ask", *msgType, actorID, map[string]any{"text": text})
	env := await(id, *timeout, func(other map[string]any) {
		sender, _ := other["sender"].(map[string]any)
		typ, _ := other["type"].(string)
		if strings.HasPrefix(typ, "agent.") || other["kind"] == "response" {
			p, _ := json.Marshal(other["payload"])
			if len(p) > 400 {
				p = append(p[:400], []byte("…")...)
			}
			fmt.Printf("  · [%5.1fs] %s %s %s\n", time.Since(start).Seconds(), sender["id"], typ, p)
		}
	})
	p, _ := json.MarshalIndent(env["payload"], "", "  ")
	fmt.Printf("← [%5.1fs] %s\n%s\n", time.Since(start).Seconds(), env["type"], p)
}
