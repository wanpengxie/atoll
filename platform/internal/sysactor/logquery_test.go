package sysactor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestLogQueryOrdinaryWordDispatchAndDiscovery(t *testing.T) {
	doc, ok := systemManifest().Words[message.TypeSystemLogQuery]
	if !ok || len(doc.Examples) < 3 || len(doc.OutputSchema) == 0 || len(doc.ErrorCodes) == 0 {
		t.Fatal("query must describe agent usage and failures")
	}
	for _, example := range doc.Examples {
		var req channelspec.LogQueryRequest
		if err := json.Unmarshal(example, &req); err != nil {
			t.Fatal(err)
		}
		if err := req.Validate(); err != nil {
			t.Fatalf("example %s: %v", example, err)
		}
	}
	called := false
	s := New(Deps{QueryLog: func(ctx context.Context, req channelspec.LogQueryRequest) (channelspec.LogQueryResponse, error) {
		called = true
		if req.Text != "actor channel" || req.Limit != 5 {
			t.Fatalf("query changed: %+v", req)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("reader must have a time budget")
		}
		return channelspec.LogQueryResponse{HeadSeq: 42, Turns: []channelspec.LogQueryTurn{}}, nil
	}})
	sys := &failSys{}
	s.handle(sys, requestMsg("q", message.TypeSystemLogQuery, []byte(`{"text":"actor channel","limit":5}`)))
	if !called || len(sys.fails) != 0 || len(sys.replies) != 1 || sys.replies[0].v.(channelspec.LogQueryResponse).HeadSeq != 42 {
		t.Fatalf("dispatch: %+v", sys)
	}
}

func TestLogQueryRejectsMalformedAndAmbiguousRequests(t *testing.T) {
	for _, body := range []string{
		`{}`, `{"text":" "}`, `{"text":"x","channel_id":"elsewhere"}`, `{"text":"x","sql":"SELECT *"}`,
		`{"text":"x","limit":21}`, `{"text":"x","before_seq":-1}`, `{"text":"x","sender_kind":"admin"}`,
		`{"text":"x","from_ts":100,"to_ts":100}`, `{"read_seq":1,"text":"x"}`, `{"read_seq":1,"limit":1}`,
		`{"text":"x","offset":20}`, `{"read_seq":-1}`, `{"read_seq":1,"offset":-1}`, `{"text":"x","limit":1.5}`,
		`{"group_by":"unknown"}`, `{"group_by":"sender","limit":1}`, `{"group_by":"sender","read_seq":1}`,
		`{"around_seq":2,"text":"x"}`, `{"around_seq":2,"participant":"agent:x:1"}`, `{"around_seq":2,"group_by":"sender"}`,
		`{"around_seq":2,"read_seq":2}`, `{"around_seq":2,"radius":11}`, `{"around_seq":2,"head_seq":1}`,
		`{"around_seq":2,"before_seq":3}`, `{"around_seq":2,"after_seq":1}`, `{"text":"x","after_seq":3}`, `{"text":"x","radius":1}`,
		`{"text":"x","view":"all"}`, `{"read_seq":1,"read_id":"x"}`, `{"read_id":"x","related_to":"y"}`,
		`{"related_to":"x","offset":1}`, `{"related_to":"x","text":"q"}`, `{"around_seq":1,"read_id":"x"}`,
	} {
		sys := &failSys{}
		s := New(Deps{QueryLog: func(context.Context, channelspec.LogQueryRequest) (channelspec.LogQueryResponse, error) {
			t.Fatal("invalid request reached reader")
			return channelspec.LogQueryResponse{}, nil
		}})
		s.handle(sys, requestMsg("q", message.TypeSystemLogQuery, []byte(body)))
		if len(sys.fails) != 1 || sys.fails[0].code != "invalid_args" {
			t.Fatalf("%s: %+v", body, sys.fails)
		}
	}
}

func TestLogQueryFailureCodes(t *testing.T) {
	for _, test := range []struct {
		err  error
		code string
	}{
		{channelspec.ErrLogMessageNotFound, "not_found"}, {context.DeadlineExceeded, "query_timeout"}, {errors.New("offline"), "provider_failed"},
		{context.Canceled, "cancelled"},
	} {
		sys := &failSys{}
		s := New(Deps{QueryLog: func(context.Context, channelspec.LogQueryRequest) (channelspec.LogQueryResponse, error) {
			return channelspec.LogQueryResponse{}, test.err
		}})
		s.handle(sys, requestMsg("q", message.TypeSystemLogQuery, []byte(`{"read_seq":1}`)))
		if len(sys.fails) != 1 || sys.fails[0].code != test.code {
			t.Fatalf("%v: %+v", test.err, sys.fails)
		}
	}
	sys := &failSys{}
	New(Deps{}).handle(sys, requestMsg("q", message.TypeSystemLogQuery, []byte(`{"text":"x"}`)))
	if len(sys.fails) != 1 || sys.fails[0].code != "provider_failed" {
		t.Fatalf("missing reader: %+v", sys.fails)
	}
}
