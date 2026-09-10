package native

import (
	"encoding/json"
	"slices"
	"testing"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
)

func TestSessionWordsPublishTheirOwnInputContracts(t *testing.T) {
	want := map[string]struct {
		properties []string
		required   []string
	}{
		agentproto.TypeSessionList:    {properties: nil, required: nil},
		agentproto.TypeSessionGet:     {properties: []string{"session"}, required: nil},
		agentproto.TypeSessionArchive: {properties: []string{"session"}, required: nil},
		agentproto.TypeSessionReset:   {properties: []string{"session"}, required: nil},
		agentproto.TypeSessionRename:  {properties: []string{"name", "session"}, required: []string{"name"}},
		agentproto.TypeSessionSync:    {properties: []string{"after", "from_session", "session", "through"}, required: []string{"from_session"}},
	}
	manifest := Manifest()
	for word, expected := range want {
		t.Run(word, func(t *testing.T) {
			var schema struct {
				Properties map[string]any `json:"properties"`
				Required   []string       `json:"required"`
			}
			if err := json.Unmarshal(manifest.Words[word].InputSchema, &schema); err != nil {
				t.Fatal(err)
			}
			var output map[string]any
			if err := json.Unmarshal(manifest.Words[word].OutputSchema, &output); err != nil || output["type"] != "object" {
				t.Fatalf("invalid output schema: %s (%v)", manifest.Words[word].OutputSchema, err)
			}
			properties := make([]string, 0, len(schema.Properties))
			for name := range schema.Properties {
				properties = append(properties, name)
			}
			slices.Sort(properties)
			if !slices.Equal(properties, expected.properties) || !slices.Equal(schema.Required, expected.required) {
				t.Fatalf("properties=%v required=%v", properties, schema.Required)
			}
		})
	}
}

func TestParameterlessSessionWordsRejectUnrelatedFields(t *testing.T) {
	for _, word := range []string{
		agentproto.TypeSessionList,
		agentproto.TypeSessionGet,
		agentproto.TypeSessionArchive,
		agentproto.TypeSessionReset,
	} {
		t.Run(word, func(t *testing.T) {
			sys := newTestSys(newTestState())
			c := &controller{data: newWorkTable(), sessions: map[string]*session{"s": {ID: "s"}}}
			c.handleSession(sys, testRequest("request", word, map[string]any{"session_id": "s", "name": "not-for-this-word"}))
			if sys.fails["request"] != "invalid_args" {
				t.Fatalf("failure=%q", sys.fails["request"])
			}
		})
	}
}

func TestSessionListRejectsAnIgnoredSessionSelector(t *testing.T) {
	sys := newTestSys(newTestState())
	c := &controller{data: newWorkTable()}
	c.handleSession(sys, testRequest("request", agentproto.TypeSessionList, map[string]any{"session": "ignored"}))
	if sys.fails["request"] != "invalid_args" {
		t.Fatalf("failure=%q", sys.fails["request"])
	}
}
