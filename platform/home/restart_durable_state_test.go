package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	restartStateClass = "restart-state-holder"
	restartStateDecl  = "decl:restart-state"
	restartStateKey   = resource.ResourceID("boot-log")
)

// restartStateReport is one incarnation's account of its own private state at
// birth: what it found, and what it left behind for the next life.
type restartStateReport struct {
	actorID actor.ActorID
	found   bool
	read    string
	wrote   string
	err     string
}

type restartStateFixture struct {
	mark    string
	reports chan restartStateReport
}

func newRestartStateFixture(mark string) *restartStateFixture {
	return &restartStateFixture{mark: mark, reports: make(chan restartStateReport, 4)}
}

func (f *restartStateFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != restartStateClass {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return f.proc(), nil
	}}}, true
}

func (f *restartStateFixture) proc() actorbase.Proc {
	return func(sys actorbase.Sys) error {
		report := restartStateReport{actorID: sys.Self()}
		out, err := sys.State().Get(restartStateKey)
		switch {
		case err != nil:
			report.err = "get: " + err.Error()
		case out.Accepted():
			report.found, report.read = out.Found, string(out.Value)
		case out.RejectReason != access.ResourceNotFound:
			report.err = "get rejected: " + string(out.RejectReason)
		}
		if report.err == "" {
			next := f.mark
			if report.found {
				var previous string
				if err := json.Unmarshal([]byte(report.read), &previous); err != nil {
					report.err = "decode carried state: " + err.Error()
				} else {
					next = previous + "|" + f.mark
				}
			}
			if report.err == "" {
				raw, err := json.Marshal(next)
				if err != nil {
					report.err = "encode state: " + err.Error()
				} else if put, err := sys.State().Put(restartStateKey, raw); err != nil {
					report.err = "put: " + err.Error()
				} else if !put.Accepted() {
					report.err = "put rejected: " + string(put.RejectReason)
				} else {
					report.wrote = string(raw)
				}
			}
		}
		f.reports <- report
		<-sys.Life().Done()
		return nil
	}
}

func restartStateDeclaration() DeclareRequest {
	return DeclareRequest{
		SourceDeclID: restartStateDecl, Kind: actor.KindAgent,
		Class: restartStateClass, Placement: storespec.NewServerPlacement(),
		CreatedAt: time.Now().UnixMilli(),
	}
}

// T5. A declared record's private state lives in the durable locus, so it is
// the actor's memory ACROSS lives, not just within one. The proof has to be
// end to end: the second life is a genuinely new Home over the same db, the
// actor reads through its own ordinary State() arm, and what it reads is what
// the previous life wrote — under the same ActorID, in the same durable row.
func TestDurableActorStateSurvivesAHomeRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("restart-state")
	ctx := context.Background()

	first := newRestartStateFixture("first")
	h1, err := Open(Config{
		ChannelID:             channelID,
		DBPath:                dbPath,
		CompositionResolver:   first,
		IntroductionResolver:  inertIntroductionResolver{},
		ReconcileInterval:     time.Hour,
		Bootstrap:             true,
		BootstrapDeclarations: []DeclareRequest{restartStateDeclaration()},
	})
	if err != nil {
		t.Fatal(err)
	}
	born := restartRecv(t, "the first life to settle its state", first.reports)
	if born.err != "" {
		t.Fatalf("first life: %s", born.err)
	}
	if born.found {
		t.Fatalf("a brand new record already carried state %q", born.read)
	}
	if born.wrote != `"first"` {
		t.Fatalf("first life wrote %q", born.wrote)
	}
	if err := h1.closeInternal("test-restart"); err != nil {
		t.Fatalf("close first Home: %v", err)
	}

	second := newRestartStateFixture("second")
	h2, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  second,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		MustExistDB:          true,
	})
	if err != nil {
		t.Fatalf("restart Open: %v", err)
	}
	t.Cleanup(func() { _ = h2.closeInternal("test") })

	reborn := restartRecv(t, "the second life to read its carried state", second.reports)
	if reborn.err != "" {
		t.Fatalf("second life: %s", reborn.err)
	}
	if reborn.actorID != born.actorID {
		t.Fatalf("restart produced a different identity: %s → %s", born.actorID, reborn.actorID)
	}
	if !reborn.found || reborn.read != `"first"` {
		t.Fatalf("second life read found=%v value=%q, want the first life's %q",
			reborn.found, reborn.read, `"first"`)
	}
	if reborn.wrote != `"first|second"` {
		t.Fatalf("second life wrote %q", reborn.wrote)
	}

	// The physical claim: one durable row, under the record's own id, holding
	// the accumulated value. Home lends no leaf port, so the locus is read
	// through a second handle the test opens itself.
	durable := openDurableStateReader(t, channelID, dbPath)
	value, exists, err := durable.Read(ctx, born.actorID, restartStateKey)
	if err != nil || !exists {
		t.Fatalf("durable state row after restart: exists=%v err=%v", exists, err)
	}
	if string(value) != `"first|second"` {
		t.Fatalf("durable state row = %s", value)
	}
}
