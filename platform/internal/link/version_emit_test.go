package link_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// versionGateMinter is a deliberately tiny truth gate. It records the version
// the link welded at handshake time and judges that immutable stamp against the
// authority version at each Write. That makes this test sensitive to the exact
// bug it guards: refreshing an already-running remote incarnation to the latest
// declaration would incorrectly let its old code keep authoring after upgrade.
type versionGateMinter struct {
	mu      sync.Mutex
	current int64
	minted  []int64
}

func (m *versionGateMinter) Mint(_ actor.ActorID, _ actor.Kind, _ channel.ID, birthVersion int64) harness.Pen {
	m.mu.Lock()
	m.minted = append(m.minted, birthVersion)
	m.mu.Unlock()
	return versionGatePen{minter: m, birthVersion: birthVersion}
}

func (m *versionGateMinter) setCurrent(version int64) {
	m.mu.Lock()
	m.current = version
	m.mu.Unlock()
}

func (m *versionGateMinter) versions() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int64(nil), m.minted...)
}

type versionGatePen struct {
	minter       *versionGateMinter
	birthVersion int64
}

func (p versionGatePen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.minter.mu.Lock()
	current := p.minter.current
	p.minter.mu.Unlock()
	if p.birthVersion != current {
		return harness.WriteResult{
			MessageID:    env.ID,
			RejectReason: harness.HarnessAuthorVersionStale,
		}, nil
	}
	return harness.WriteResult{MessageID: env.ID, Seq: 1}, nil
}

func TestDaemonEmitKeepsHandshakeBirthVersionAcrossDeclarationUpgrade(t *testing.T) {
	rt, _ := actorrt.New(actorrt.Config{})
	t.Cleanup(rt.StopAll)
	auth := newTestAuthorities()
	minter := &versionGateMinter{current: 1}
	acc := newTestAcceptor(t, link.Config{
		Minter: minter, Runtime: rt, ChannelID: testChannelID,
		Declarations: auth, Authority: auth, CanAttach: func(context.Context, string) error { return nil }, PortIndex: auth,
		ActorLock: func(actor.ActorID) func() { return func() {} },
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, "daemon-version")
	}))
	t.Cleanup(func() { _ = acc.Close(); srv.Close() })

	const id actor.ActorID = "agent:version-weld"
	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], []link.Declaration{{
		ActorID: id, Kind: actor.KindAgent,
		Binding: actor.BindingRuntimeInboundViaRelay, Version: 1,
	}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	arms, err := d.OpenStream(context.Background(), id, 1, "", func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.StartStream(id)

	// Upgrade authority without rebuilding the already-open incarnation. Its
	// next emit must still carry birth version 1 and be rejected as stale.
	auth.mu.Lock()
	row := auth.rows[id]
	row.CurrentDeclVersion = 2
	auth.rows[id] = row
	auth.mu.Unlock()
	minter.setCurrent(2)

	for i := 0; i < 3; i++ {
		res, err := arms.Pen.Write(context.Background(), &message.Envelope{
			ID: message.ID(fmt.Sprintf("old-code-write-%d", i)), Kind: message.KindEvent, Type: "version.probe",
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.RejectReason != harness.HarnessAuthorVersionStale {
			t.Fatalf("old incarnation emit %d verdict=%+v, want %s", i, res, harness.HarnessAuthorVersionStale)
		}
	}
	if got := minter.versions(); len(got) != 3 || got[0] != 1 || got[1] != 1 || got[2] != 1 {
		t.Fatalf("minted birth versions=%v, want immutable handshake version [1 1 1]", got)
	}
}

var _ harness.Minter = (*versionGateMinter)(nil)
var _ harness.Pen = versionGatePen{}
