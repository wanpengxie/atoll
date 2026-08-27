package kimi

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
)

// describe.go is the actor.describe self-answer catalog — discovery is the
// actor answering live (no external catalog). kimi exposes one work word,
// kimi.command, whose input schema closes the browser-primitive action set,
// plus the two endpoint words every plugin adapter answers about itself.

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: "kimi", Interfaces: []string{"actor"},
		Words: map[string]introspect.WordSpec{
			TypeCommand: {
				Description:  "Forward one Kimi WebBridge browser command to the user's Chrome extension. The device verb is the payload's `action` (one of 13 primitives); `args` is forwarded verbatim.",
				InputSchema:  json.RawMessage(`{"type":"object","required":["action"],"properties":{"action":{"type":"string","enum":["navigate","find_tab","snapshot","click","fill","evaluate","screenshot","network","upload","save_as_pdf","list_tabs","close_tab","close_session"]},"args":{"type":"object"}}}`),
				OutputSchema: json.RawMessage(`{"type":"object"}`),
				ErrorCodes:   []string{"device_offline", "invalid_action", "timeout"},
				Examples:     []json.RawMessage{json.RawMessage(`{"action":"navigate","args":{"url":"https://example.com"}}`)},
			},
			TypeListenSet: {
				Description:  "Move this adapter's private device endpoint to a new listen address. Binds the new address first; on failure the previous listener keeps serving. Survives restart. The endpoint is keyless, so whatever can reach the address can drive the plugin — a wildcard bind (0.0.0.0, ::) is refused.",
				InputSchema:  json.RawMessage(`{"type":"object","required":["listen_addr"],"properties":{"listen_addr":{"type":"string","description":"host:port, e.g. 127.0.0.1:10086 or 100.64.0.2:10086"}}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"desired_addr":{"type":"string"},"actual_addr":{"type":"string"},"online":{"type":"boolean"},"loopback":{"type":"boolean"},"persisted":{"type":"boolean"}}}`),
				ErrorCodes:   []string{"invalid_args", "bind_failed"},
				Examples:     []json.RawMessage{json.RawMessage(`{"listen_addr":"127.0.0.1:10086"}`)},
			},
			TypeListenGet: {
				Description:  "Report this adapter's device endpoint: the address asked for, the address actually bound, and whether a plugin is attached right now.",
				InputSchema:  json.RawMessage(`{"type":"object","properties":{}}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"desired_addr":{"type":"string"},"actual_addr":{"type":"string"},"online":{"type":"boolean"},"loopback":{"type":"boolean"}}}`),
			},
		},
	}
}
