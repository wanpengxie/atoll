package kimi

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
)

// describe.go is the actor.describe self-answer catalog — discovery is the
// actor answering live (no external catalog). kimi exposes one word,
// kimi.command, whose input schema closes the browser-primitive action set.

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
		},
	}
}
