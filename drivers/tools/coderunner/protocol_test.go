package coderunner

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestProtocolFramesRoundTrip(t *testing.T) {
	frames := []any{
		startFrame{Op: "start", Program: "data:x", Args: json.RawMessage(`{"x":1}`), Actors: map[string]string{"echo": "tool:echo:1"}, Self: "tool:runner:1", Channel: "c", RequestID: "r"},
		answerFrame{Op: "answer", ID: 7, OK: true, Payload: json.RawMessage(`{"ok":1}`)},
		answerFrame{Op: "answer", ID: 8, Error: &callError{Code: "bad", Detail: "no"}},
		nodeFrame{Op: "call", ID: 9, Target: "echo", Type: "echo.say", Input: json.RawMessage(`{"x":1}`), DeadlineMS: 10},
		nodeFrame{Op: "log", Stream: "log", Text: "hello"},
		nodeFrame{Op: "progress", Status: "processing", Value: json.RawMessage(`{"n":1}`)},
		nodeFrame{Op: "result", Value: json.RawMessage(`{"done":true}`)},
		nodeFrame{Op: "error", Kind: "exception", Message: "x", Stack: "s"},
	}
	for _, frame := range frames {
		raw, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		out := reflect.New(reflect.TypeOf(frame))
		if err := json.Unmarshal(raw, out.Interface()); err != nil {
			t.Fatalf("round trip %T: %v", frame, err)
		}
	}
}
