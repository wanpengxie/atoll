package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	coagentsdk "github.com/wanpengxie/ActOS/sdk"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

type gwEcho struct{ mctx *behavior.ModuleContext }

func (m *gwEcho) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name: "echo", ActorID: "echo", Binding: actor.BindingEmbedded,
		Types: []string{"echo.say"}, MaxPendingMs: 30_000,
	}
}
func (m *gwEcho) Init(_ context.Context, mctx *behavior.ModuleContext) error {
	m.mctx = mctx
	return nil
}
func (m *gwEcho) Shutdown(context.Context) error                   { return nil }
func (m *gwEcho) OnExternalCallback(context.Context, []byte) error { return nil }
func (m *gwEcho) Handle(ctx context.Context, env *message.Envelope) error {
	_, err := m.mctx.Respond(ctx, behavior.CorrelationKey(env.ID), env.Payload, behavior.RespondOptions{})
	return err
}

// TestGatewaySDK_EndToEnd proves the client-push + SDK-endpoint alignment (P0
// #2/#3): the REAL coagentsdk client Calls a hosted adapter against the v2 server
// — fetchCursor + POST /messages + WS subscribe/tail — and receives the response
// pushed back over the WS. Before this the server mounted only /compute /ingress
// (every SDK call 404'd) and committed envelopes never reached an external client.
func TestGatewaySDK_EndToEnd(t *testing.T) {
	ctx := context.Background()
	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := home.InstallEmbeddedAdapter(ctx, &gwEcho{}); err != nil {
		t.Fatalf("install: %v", err)
	}

	mux := http.NewServeMux()
	gw := &gateway{home: home, channelID: "ch"}
	gw.mount(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := &coagentsdk.Client{BaseURL: srv.URL}
	res, err := client.Call(ctx, coagentsdk.CallRequest{
		ChannelID: "ch", ActorID: "echo", Type: "echo.say",
		Payload: json.RawMessage(`{"msg":"hello"}`), Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("sdk Call failed — gateway/client-push broken: %v", err)
	}
	if !res.OK {
		t.Fatalf("sdk Call not OK: %+v", res)
	}
}
