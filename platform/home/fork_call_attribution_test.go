package home

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
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
	forkAttrParentClass = "fork-attr-parent"
	forkAttrChildClass  = "fork-attr-child"
	forkAttrProbeType   = "fork.attr.probe"
	forkAttrChildren    = 4
)

type forkAttrSlot struct {
	Slot int `json:"slot"`
}

// forkAttrAnswer is what a child puts in its reply payload. Every field is an
// attribution coordinate the parent can cross-check against the one it
// addressed: a mis-routed request or a mis-attributed reply breaks at least one
// of them.
type forkAttrAnswer struct {
	From      string `json:"from"`
	Slot      int    `json:"slot"`
	AskedSlot int    `json:"asked_slot"`
	RequestID string `json:"req_id"`
	Status    string `json:"status"`
}

// forkAttrExchange is one parent→child Call/Wait round trip as the PARENT saw
// it: who it addressed, and the reply envelope it actually got back.
type forkAttrExchange struct {
	slot   int
	target actor.ActorID
	reply  message.Envelope
	err    string
}

type forkAttrReport struct {
	parent    actor.ActorID
	children  []actor.ActorID
	exchanges []forkAttrExchange
	err       string
}

// forkAttrFixture is the out-of-band channel pair the two bodies report on.
// ready is how a child announces its body is running, so the parent addresses
// it only once delivery can actually land (the substrate drops a delivery aimed
// at an actor with no endpoint yet — it never queues it).
type forkAttrFixture struct {
	ready   chan actor.ActorID
	reports chan forkAttrReport
}

func (f *forkAttrFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, config json.RawMessage,
) (platform.ActorFactory, bool) {
	switch class {
	case forkAttrParentClass:
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return f.parentProc(), nil
		}}}, true
	case forkAttrChildClass:
		var slot forkAttrSlot
		if len(config) > 0 {
			_ = json.Unmarshal(config, &slot)
		}
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return f.childProc(slot.Slot), nil
		}}}, true
	}
	return platform.ActorFactory{}, false
}

func (f *forkAttrFixture) parentProc() actorbase.Proc {
	return func(sys actorbase.Sys) error {
		report := forkAttrReport{parent: sys.Self()}
		fail := func(format string, args ...any) error {
			report.err = fmt.Sprintf(format, args...)
			f.reports <- report
			<-sys.Life().Done()
			return nil
		}

		// Every child is forked off the SAME incarnation, concurrently.
		ids := make([]actor.ActorID, forkAttrChildren)
		errs := make([]error, forkAttrChildren)
		var wg sync.WaitGroup
		for slot := range forkAttrChildren {
			wg.Add(1)
			go func(slot int) {
				defer wg.Done()
				cfg, err := json.Marshal(forkAttrSlot{Slot: slot})
				if err != nil {
					errs[slot] = err
					return
				}
				ids[slot], errs[slot] = sys.Fork(
					message.ID(fmt.Sprintf("fork:%s:%d", sys.Self(), slot)),
					actorcaps.ForkSpec{
						Kind:     actor.KindAgent,
						Class:    forkAttrChildClass,
						NameHint: fmt.Sprintf("kid%d", slot),
						Config:   cfg,
					})
			}(slot)
		}
		wg.Wait()
		for slot, err := range errs {
			if err != nil {
				return fail("fork slot %d: %v", slot, err)
			}
		}
		report.children = append(report.children, ids...)

		pending := map[actor.ActorID]struct{}{}
		for _, id := range ids {
			pending[id] = struct{}{}
		}
		for len(pending) > 0 {
			select {
			case id := <-f.ready:
				delete(pending, id)
			case <-sys.Life().Done():
				return fail("life ended with %d children still silent", len(pending))
			case <-time.After(restartWaitBudget):
				return fail("%d children never reported ready", len(pending))
			}
		}

		for slot, child := range ids {
			exchange := forkAttrExchange{slot: slot, target: child}
			ticket, err := sys.Call(child, forkAttrProbeType, forkAttrSlot{Slot: slot})
			if err != nil {
				exchange.err = err.Error()
				report.exchanges = append(report.exchanges, exchange)
				continue
			}
			answer, err := ticket.Wait(sys.Life(), restartWaitBudget)
			if err != nil {
				exchange.err = err.Error()
			} else {
				exchange.reply = answer.Envelope
			}
			report.exchanges = append(report.exchanges, exchange)
		}
		f.reports <- report
		<-sys.Life().Done()
		return nil
	}
}

func (f *forkAttrFixture) childProc(slot int) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		f.ready <- sys.Self()
		return actorbase.Serve(map[string]actorbase.Handler{
			forkAttrProbeType: func(_ context.Context, msg actorbase.Msg) (any, error) {
				var asked forkAttrSlot
				if err := json.Unmarshal(msg.Payload, &asked); err != nil {
					return nil, err
				}
				return forkAttrAnswer{
					From:      string(sys.Self()),
					Slot:      slot,
					AskedSlot: asked.Slot,
					RequestID: string(msg.ID),
				}, nil
			},
		})(sys)
	}
}

// T2. One incarnation forks four children CONCURRENTLY and then holds a
// Call/Wait round trip with each. The point is attribution: every reply must
// carry the addressed child as its author, answer the request that child
// actually handled, and come back to the caller alone. A crossed wire anywhere
// in fork → mint → route → reply shows up as one of these coordinates naming
// the wrong actor.
func TestForkedSiblingsAnswerTheirOwnCallsWithTheirOwnIdentity(t *testing.T) {
	fixture := &forkAttrFixture{
		ready:   make(chan actor.ActorID, forkAttrChildren*2),
		reports: make(chan forkAttrReport, 2),
	}
	h, err := Open(Config{
		ChannelID:            "fork-attr",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  fixture,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl:fork-attr-parent", Kind: actor.KindAgent,
			Class: forkAttrParentClass, Placement: storespec.NewServerPlacement(),
			CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	report := restartRecv(t, "the parent's fork/call report", fixture.reports)
	if report.err != "" {
		t.Fatalf("parent body: %s", report.err)
	}
	if len(report.children) != forkAttrChildren {
		t.Fatalf("forked %d children, want %d", len(report.children), forkAttrChildren)
	}

	distinct := map[actor.ActorID]int{}
	for slot, child := range report.children {
		if child == "" {
			t.Fatalf("fork slot %d minted an empty id", slot)
		}
		if previous, clash := distinct[child]; clash {
			t.Fatalf("fork slots %d and %d minted the same id %s", previous, slot, child)
		}
		distinct[child] = slot
		if child == report.parent {
			t.Fatalf("fork slot %d handed back the parent's own id", slot)
		}
	}

	if len(report.exchanges) != forkAttrChildren {
		t.Fatalf("held %d exchanges, want %d", len(report.exchanges), forkAttrChildren)
	}
	for _, exchange := range report.exchanges {
		if exchange.err != "" {
			t.Fatalf("slot %d call to %s: %s", exchange.slot, exchange.target, exchange.err)
		}
		reply := exchange.reply
		if reply.Kind != message.KindResponse || reply.Type != forkAttrProbeType {
			t.Fatalf("slot %d reply kind/type = %q/%q", exchange.slot, reply.Kind, reply.Type)
		}
		// Author attribution: the reply is welded to the child that was asked.
		if reply.Sender.ID != exchange.target || reply.Sender.Kind != actor.KindAgent {
			t.Fatalf("slot %d reply sender = %+v, want %s/%s",
				exchange.slot, reply.Sender, exchange.target, actor.KindAgent)
		}
		// Target attribution: the reply comes back to the caller and no one else.
		if len(reply.Audience) != 1 || reply.Audience[0] != report.parent {
			t.Fatalf("slot %d reply audience = %v, want [%s]",
				exchange.slot, reply.Audience, report.parent)
		}
		var answer forkAttrAnswer
		if err := json.Unmarshal(reply.Payload, &answer); err != nil {
			t.Fatalf("slot %d reply payload %s: %v", exchange.slot, reply.Payload, err)
		}
		if answer.From != string(exchange.target) {
			t.Fatalf("slot %d was answered by %s, not the addressed %s",
				exchange.slot, answer.From, exchange.target)
		}
		if answer.Slot != exchange.slot || answer.AskedSlot != exchange.slot {
			t.Fatalf("slot %d reached a child holding slot %d and reading slot %d",
				exchange.slot, answer.Slot, answer.AskedSlot)
		}
		// The reply closes exactly the request that child handled.
		if answer.RequestID == "" || message.ID(answer.RequestID) != reply.ParentID {
			t.Fatalf("slot %d reply parent=%q, child handled request %q",
				exchange.slot, reply.ParentID, answer.RequestID)
		}
		if answer.Status != string(message.StatusCompleted) {
			t.Fatalf("slot %d reply status = %q", exchange.slot, answer.Status)
		}
	}

	// The forked siblings are members in their own right, and they are FORK
	// births: no declaration produced them.
	declared, err := h.controller.DeclaredReconcileList()
	if err != nil {
		t.Fatal(err)
	}
	declaredIDs := map[actor.ActorID]struct{}{}
	for _, instance := range declared {
		declaredIDs[instance.ID] = struct{}{}
	}
	for _, child := range report.children {
		active, err := h.controller.IsActive(ctx, child)
		if err != nil || !active {
			t.Fatalf("forked child %s active=%v err=%v", child, active, err)
		}
		facts, found, err := h.controller.ActorFacts(ctx, child)
		if err != nil || !found || facts.Kind != actor.KindAgent || facts.Principal != "" {
			t.Fatalf("forked child %s facts=%+v found=%v err=%v", child, facts, found, err)
		}
		if _, declaredBirth := declaredIDs[child]; declaredBirth {
			t.Fatalf("forked child %s surfaced as a declared instance", child)
		}
	}
}
