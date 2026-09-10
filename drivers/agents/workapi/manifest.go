// Package workapi owns the work-aware Agent interface: its wire vocabulary,
// strict decoding, schemas, and discoverable manifest. It is a driver-family
// contract, not part of Atoll's kernel protocol layer.
package workapi

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
)

func Manifest(class string, capabilities map[string]bool) introspect.Manifest {
	caps := cloneCapabilities(capabilities)
	caps[CapabilityWorkProtocolV1] = true
	return introspect.Manifest{
		Class:        class,
		Interfaces:   []string{"actor", "agent"},
		Capabilities: caps,
		Words: map[string]introspect.WordSpec{
			TypeAsk: {
				Description:  "Create one addressable Agent work. Use delivery=wait for a foreground answer; use delivery=receipt plus a stable submission_key to decouple the reply from execution. Work addresses and deduplication last only for the current Controller process; after restart submit a new request. related_work_id starts an explicit branch from that visible work's latest durable context checkpoint; it does not merge results back automatically. The response supplies work_id and executable next requests.",
				InputSchema:  json.RawMessage(AskInputSchema),
				OutputSchema: json.RawMessage(AskOutputSchema),
				ErrorCodes:   []string{"invalid_args", "capacity", "limit_exceeded", "submission_conflict", "work_not_found", "context_unavailable", "ledger_unavailable"},
				Examples: []json.RawMessage{
					json.RawMessage(`{"text":"explain the failure"}`),
					json.RawMessage(`{"text":"run the migration","delivery":"receipt","submission_key":"client-job-17"}`),
				},
			},
			TypeStatus: {
				Description:  "Inspect or page works in this Agent instance without driving, retrying, or changing them. Selected status exposes each input's accepted/assigned/included disposition without repeating its body. Use submission_key to look up a lost receipt for the same caller within the current Controller process; follow the opaque stable next_cursor and actor-authored next[].",
				InputSchema:  json.RawMessage(StatusInputSchema),
				OutputSchema: json.RawMessage(StatusOutputSchema),
				ErrorCodes:   []string{"invalid_args", "work_not_found", "permission_denied"},
				Examples: []json.RawMessage{
					json.RawMessage(`{"work_id":"w-opaque"}`),
					json.RawMessage(`{"submission_key":"client-job-17"}`),
				},
			},
			TypeResult: {
				Description:  "Read one work result held by the current Controller process. state=open is not a successful empty result: follow the returned agent.result request later, or use the returned targeted interrupt request.",
				InputSchema:  json.RawMessage(ResultInputSchema),
				OutputSchema: json.RawMessage(ResultOutputSchema),
				ErrorCodes:   []string{"invalid_args", "work_not_found", "permission_denied"},
				Examples:     []json.RawMessage{json.RawMessage(`{"work_id":"w-opaque"}`)},
			},
			TypeSteer: {
				Description:  "Append text to one addressed work. The input may join the current episode at its next safe boundary or become a continuation; included=false does not mean it was lost. Reuse operation_key only for an identical retry.",
				InputSchema:  json.RawMessage(WorkTextSteerInputSchema),
				OutputSchema: json.RawMessage(ControlOutputSchema),
				ErrorCodes:   []string{"invalid_args", "unsupported_scope", "work_not_found", "work_closed", "operation_conflict", "limit_exceeded", "ledger_unavailable"},
				Examples: []json.RawMessage{
					json.RawMessage(`{"work_id":"w-opaque","text":"preserve the old API","operation_key":"input-4"}`),
				},
			},
			TypeInterrupt: {
				Description:  "Request stop for one work. The empty form stops open work Agent-wide; never use it as a fallback after a targeted failure. operation_key is supported only with work_id.",
				InputSchema:  json.RawMessage(InterruptInputSchema),
				OutputSchema: json.RawMessage(ControlOutputSchema),
				ErrorCodes:   []string{"invalid_args", "unsupported_scope", "work_not_found", "operation_conflict", "limit_exceeded", "ledger_unavailable"},
				Examples: []json.RawMessage{
					json.RawMessage(`{"work_id":"w-opaque","operation_key":"stop-2"}`),
					json.RawMessage(`{}`),
				},
			},
		},
	}
}

func cloneCapabilities(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in)+1)
	for name, enabled := range in {
		out[name] = enabled
	}
	return out
}
