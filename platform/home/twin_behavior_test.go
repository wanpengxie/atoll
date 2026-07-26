package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	twinParentClass = "twin-parent"
	twinChildClass  = "twin-child"
)

// twinReport is what one twin body reports about the capabilities it exercised.
// The two twins run the SAME body code; only their record's storage home
// differs (the parent is a declared durable record, the child is a forked entry
// record), so any asymmetry in this report is a leak of the classification.
type twinReport struct {
	id      actor.ActorID
	child   actor.ActorID
	state   string
	timerOK bool
	emitted message.ID
	err     error
}

type twinResolver struct{ reports chan twinReport }

func (r twinResolver) BuildClass(
	_ channel.ID,
	id actor.ActorID,
	class string,
	config json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != twinParentClass && class != twinChildClass {
		return platform.ActorFactory{}, false
	}
	reports := r.reports
	forks := class == twinParentClass
	var peer actor.ActorID
	if len(config) > 0 {
		var parsed struct {
			Peer actor.ActorID `json:"peer"`
		}
		if err := json.Unmarshal(config, &parsed); err == nil {
			peer = parsed.Peer
		}
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			report := twinReport{id: id}
			fail := func(err error) {
				if err != nil && report.err == nil {
					report.err = err
				}
			}
			if forks {
				child, err := sys.Fork(message.ID("fork:"+string(id)), actorcaps.ForkSpec{
					Kind: actor.KindAgent, Class: twinChildClass, NameHint: "twin",
					Config: []byte(`{"peer":"` + string(id) + `"}`),
				})
				fail(err)
				report.child, peer = child, child
			}
			_, err := sys.State().Put("k", []byte(`"v"`))
			fail(err)
			out, err := sys.State().Get("k")
			fail(err)
			report.state = string(out.Value)
			if _, err := sys.After(time.Hour, "twin.tick", nil); err != nil {
				fail(err)
			} else {
				report.timerOK = true
			}
			if peer != "" {
				msgID, err := sys.Emit("twin.hello", map[string]string{"from": string(id)}, peer)
				fail(err)
				report.emitted = msgID
			}
			reports <- report
			<-sys.Life().Done()
			return nil
		}, nil
	}}}, true
}

// The whole point of the refactor: a forked ENTRY record and a declared DURABLE
// record are one species. They run identical body code and get identical
// capability, collaboration and lifecycle behaviour. The single place their
// storage home is allowed to show is invisible to them: which physical locus
// their private state landed in.
func TestEntryAndDurableActorsAreBehaviourallyIdentical(t *testing.T) {
	ctx := context.Background()
	reports := make(chan twinReport, 4)
	h, err := Open(Config{
		ChannelID:            "twin-home",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  twinResolver{reports: reports},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    10 * time.Millisecond,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "twin", Kind: actor.KindAgent, Class: twinParentClass,
			Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })

	seen := map[actor.ActorID]twinReport{}
	for len(seen) < 2 {
		select {
		case report := <-reports:
			seen[report.id] = report
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of 2 twins reported: %+v", len(seen), seen)
		}
	}

	var parent, child twinReport
	for _, report := range seen {
		if report.child != "" {
			parent = report
		} else {
			child = report
		}
	}
	if parent.id == "" || child.id == "" || parent.child != child.id {
		t.Fatalf("twins did not pair up: %+v", seen)
	}

	// Capability + collaboration parity: same verbs, same results, no arm is
	// degraded or absent on either side.
	for name, report := range map[string]twinReport{"durable": parent, "entry": child} {
		if report.err != nil {
			t.Fatalf("%s twin errored: %v", name, report.err)
		}
		if report.state != `"v"` {
			t.Fatalf("%s twin state round-trip = %q, want %q", name, report.state, `"v"`)
		}
		if !report.timerOK {
			t.Fatalf("%s twin could not arm a timer", name)
		}
		if report.emitted == "" {
			t.Fatalf("%s twin could not write to the conversation", name)
		}
	}

	// The one asymmetry, and it is invisible from inside: the declared record's
	// private state lives in the durable locus, the entry record's does not.
	if _, exists, err := h.cs.Assembly.State.Read(ctx, parent.id, "k"); err != nil || !exists {
		t.Fatalf("durable twin state row: exists=%v err=%v", exists, err)
	}
	if _, exists, err := h.cs.Assembly.State.Read(ctx, child.id, "k"); err != nil || exists {
		t.Fatalf("entry twin state leaked into the durable locus: exists=%v err=%v", exists, err)
	}

	// Lifecycle parity: one command, same shape, same effect on both.
	for name, id := range map[string]actor.ActorID{"durable": parent.id, "entry": child.id} {
		if active, err := h.actors.IsActive(ctx, id); err != nil || !active {
			t.Fatalf("%s twin active=%v err=%v before terminal", name, active, err)
		}
		if err := removeThroughSysOp(h, ctx, id); err != nil {
			t.Fatalf("%s twin terminal: %v", name, err)
		}
		if active, err := h.actors.IsActive(ctx, id); err != nil || active {
			t.Fatalf("%s twin active=%v err=%v after terminal", name, active, err)
		}
	}
}
