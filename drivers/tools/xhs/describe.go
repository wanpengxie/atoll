package xhs

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
)

// describe.go is the actor.describe self-answer catalog — discovery is the
// actor answering live (no external catalog). The shape mirrors echo's, scaled
// to the four xhs business types.

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: "xhs", Interfaces: []string{"actor"},
		Words: map[string]introspect.WordSpec{
			TypePublish: {
				Description:  "Publish a note to Xiaohongshu. Long-running (image upload + post).",
				InputSchema:  json.RawMessage(`{"type":"object","required":["title","content"],"properties":{"title":{"type":"string"},"content":{"type":"string"},"images":{"type":"array","items":{"type":"string"}},"tags":{"type":"array","items":{"type":"string"}}}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"status":{"type":"string"},"note_id":{"type":"string"},"url":{"type":"string"}}}`),
				ErrorCodes:   []string{"device_offline", "timeout"},
				Examples:     []json.RawMessage{json.RawMessage(`{"title":"旅行","content":"正文","images":["img/cover.png"],"tags":["旅行"]}`)},
			},
			TypeSearch: {
				Description:  "Search Xiaohongshu by keyword.",
				InputSchema:  json.RawMessage(`{"type":"object","required":["keyword"],"properties":{"keyword":{"type":"string"},"limit":{"type":"integer"}}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"results":{"type":"array","items":{"type":"object"}}}}`),
			},
			TypeNoteFetch: {
				Description:  "Fetch one note. Locate by url, or by note_id + xsec_token.",
				InputSchema:  json.RawMessage(`{"type":"object","properties":{"url":{"type":"string"},"note_id":{"type":"string"},"xsec_token":{"type":"string"}}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"note":{"type":"object"}}}`),
			},
			TypeListenSet: {
				Description:  "Move this adapter's private device endpoint to a new listen address. Binds the new address first; on failure the previous listener keeps serving. Survives restart. The endpoint is keyless, so whatever can reach the address can drive the plugin — a wildcard bind (0.0.0.0, ::) is refused.",
				InputSchema:  json.RawMessage(`{"type":"object","required":["listen_addr"],"properties":{"listen_addr":{"type":"string","description":"host:port, e.g. 127.0.0.1:8090 or 100.64.0.2:8090"}}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"desired_addr":{"type":"string"},"actual_addr":{"type":"string"},"online":{"type":"boolean"},"loopback":{"type":"boolean"},"persisted":{"type":"boolean"}}}`),
				ErrorCodes:   []string{"invalid_args", "bind_failed"},
				Examples:     []json.RawMessage{json.RawMessage(`{"listen_addr":"127.0.0.1:8090"}`)},
			},
			TypeListenGet: {
				Description:  "Report this adapter's device endpoint: the address asked for, the address actually bound, and whether a plugin is attached right now.",
				InputSchema:  json.RawMessage(`{"type":"object","properties":{}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"desired_addr":{"type":"string"},"actual_addr":{"type":"string"},"online":{"type":"boolean"},"loopback":{"type":"boolean"}}}`),
			},
			TypeRecentFetch: {
				Description:  "Fetch the connected account's recent notes.",
				InputSchema:  json.RawMessage(`{"type":"object","properties":{"limit":{"type":"integer"}}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"notes":{"type":"array","items":{"type":"object"}}}}`),
			},
		},
	}
}
