package workapi

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestDecodeAskRequiresSubmissionKeyOnlyForReceipt(t *testing.T) {
	wait, err := DecodeAsk([]byte(`{"text":" answer ","delivery":"wait"}`))
	if err != nil || wait.Text != " answer " || wait.Delivery != DeliveryWait {
		t.Fatalf("wait = %+v, %v", wait, err)
	}
	if _, err := DecodeAsk([]byte(`{"text":"run","delivery":"receipt"}`)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("receipt without key error = %v", err)
	}
	receipt, err := DecodeAsk([]byte(`{"text":"run","delivery":"receipt","submission_key":"submit-1"}`))
	if err != nil || receipt.SubmissionKey != "submit-1" {
		t.Fatalf("receipt = %+v, %v", receipt, err)
	}
}

func TestDecodeAskDefaultsToWaitAndRejectsUnknownFields(t *testing.T) {
	got, err := DecodeAsk([]byte(`{"text":"hello"}`))
	if err != nil || got.Delivery != DeliveryWait {
		t.Fatalf("ask = %+v, %v", got, err)
	}
	if _, err := DecodeAsk([]byte(`{"text":"hello","owner":"forged"}`)); !errors.Is(err, ErrUnknownField) {
		t.Fatalf("unknown error = %v", err)
	}
}

func TestDecodeSteerSeparatesDestinationWorkFromBufferedTarget(t *testing.T) {
	got, err := DecodeSteer([]byte(`{"work_id":"w1","target":"request-7"}`))
	if err != nil || got.WorkID != "w1" || got.Target != "request-7" {
		t.Fatalf("steer = %+v, %v", got, err)
	}
	for _, raw := range []string{`{"target":"request-7"}`, `{"all":true,"work_id":"w1"}`, `{"view_id":"view:v","text":"x"}`} {
		if _, err := DecodeSteer([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{
		`{"work_id":"w1","text":"x","target":"request-7"}`,
		`{"work_id":"w1","target":"request-7","expected_turn_id":"turn"}`,
	} {
		if _, err := DecodeSteer([]byte(raw)); !errors.Is(err, ErrInvalidPayload) {
			t.Errorf("DecodeSteer(%s) error = %v", raw, err)
		}
	}
}

func TestDecodeInterruptEmptyMeansAgentScope(t *testing.T) {
	got, err := DecodeInterrupt([]byte(`{}`))
	if err != nil || got.WorkID != "" {
		t.Fatalf("interrupt = %+v, %v", got, err)
	}
	targeted, err := DecodeInterrupt([]byte(`{"work_id":"w2","operation_key":"stop-1"}`))
	if err != nil || targeted.WorkID != "w2" {
		t.Fatalf("targeted = %+v, %v", targeted, err)
	}
}

func TestDecodeStatusHasAtMostOneSelector(t *testing.T) {
	list, err := DecodeStatus([]byte(`{}`))
	if err != nil || list.Limit != 20 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if _, err := DecodeStatus([]byte(`{"work_id":"w1","submission_key":"s1"}`)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("multiple selector error = %v", err)
	}
	if _, err := DecodeStatus([]byte(`{"request_id":"remote-request"}`)); !errors.Is(err, ErrUnknownField) {
		t.Fatalf("receiver-local request selector error = %v", err)
	}
	if _, err := DecodeStatus([]byte(`{"work_id":"w1","limit":5}`)); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("selected pagination error = %v", err)
	}
	selected, err := DecodeStatus([]byte(`{"work_id":"w1"}`))
	if err != nil || selected.Limit != 0 {
		t.Fatalf("selected status acquired list default: %+v, %v", selected, err)
	}
}

func TestSchemasAreJSONDocuments(t *testing.T) {
	for name, schema := range map[string]string{
		"ask": AskInputSchema, "status": StatusInputSchema, "result": ResultInputSchema,
		"steer": SteerInputSchema, "work steer": WorkTextSteerInputSchema, "interrupt": InterruptInputSchema,
		"ask output": AskOutputSchema, "status output": StatusOutputSchema, "result output": ResultOutputSchema, "control output": ControlOutputSchema,
	} {
		var doc map[string]any
		if err := json.Unmarshal([]byte(schema), &doc); err != nil {
			t.Errorf("%s schema: %v", name, err)
		}
	}
}

func TestClosedEnums(t *testing.T) {
	if _, ok := ParseDelivery("later"); ok {
		t.Fatal("unknown delivery accepted")
	}
	if _, ok := ParseWorkState("waiting"); ok {
		t.Fatal("stage accepted as state")
	}
	if _, ok := ParseOutcome("unknown"); ok {
		t.Fatal("unknown outcome accepted")
	}
}
