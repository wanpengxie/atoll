package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

const (
	restartTimerClass        = "restart-timer-worker"
	restartTimerDecl         = "decl:restart-timer"
	restartTimerType         = "restart.timer.tick"
	restartTimerFollowupType = "restart.timer.followup"
	restartTimerPayload      = `{"round":1}`
)

type restartTimerArmed struct {
	actorID actor.ActorID
	timerID schedule.TimerID
	err     string
}

type restartTimerFire struct {
	actorID  actor.ActorID
	env      message.Envelope
	followup message.ID
	err      string
}

// restartTimerFixture builds ONE class. arm is the only thing that differs
// between the two Home lives in the restart test: the second life must not arm
// a fresh timer, so whatever fires there can only be the row the first life
// left behind.
type restartTimerFixture struct {
	arm     bool
	started chan actor.ActorID
	armed   chan restartTimerArmed
	fired   chan restartTimerFire
}

func newRestartTimerFixture(arm bool) *restartTimerFixture {
	return &restartTimerFixture{
		arm:     arm,
		started: make(chan actor.ActorID, 4),
		armed:   make(chan restartTimerArmed, 4),
		fired:   make(chan restartTimerFire, 4),
	}
}

func (f *restartTimerFixture) BuildClass(
	_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != restartTimerClass {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return f.proc(), nil
	}}}, true
}

func (f *restartTimerFixture) proc() actorbase.Proc {
	return func(sys actorbase.Sys) error {
		if f.arm {
			id, err := sys.After(
				time.Hour, restartTimerType,
				json.RawMessage(restartTimerPayload), schedule.TimerHomeDurable)
			armed := restartTimerArmed{actorID: sys.Self(), timerID: id}
			if err != nil {
				armed.err = err.Error()
			}
			f.armed <- armed
		}
		f.started <- sys.Self()
		for {
			msg, err := sys.Recv()
			if err != nil {
				return err
			}
			if msg.Type != restartTimerType {
				continue
			}
			fire := restartTimerFire{actorID: sys.Self(), env: msg.Envelope}
			// A fire that only lands in the log proves nothing about the actor
			// being driven by it. Doing real work back into the channel does.
			spec, err := behavior.EventSpecJSON(restartTimerFollowupType,
				map[string]string{"timer": string(msg.ID)}, sys.Self())
			var followup message.ID
			if err == nil {
				followup, err = sys.Emit(spec)
			}
			if err != nil {
				fire.err = err.Error()
			}
			fire.followup = followup
			f.fired <- fire
		}
	}
}

func restartTimerDeclaration() DeclareRequest {
	return DeclareRequest{
		SourceDeclID: restartTimerDecl, Kind: actor.KindAgent,
		Class: restartTimerClass, Placement: storespec.NewServerPlacement(),
		CreatedAt: time.Now().UnixMilli(),
	}
}

// openRestartTimerStoreReader opens a second handle on an already-open channel
// db to read the durable timer table directly. A claim about WHERE a pending
// row lives has to look at the locus itself: Home hands out no timer port after
// Open.
func openRestartTimerStoreReader(
	t *testing.T,
	channelID channel.ID,
	dbPath string,
) timerspec.TimerStore {
	t.Helper()
	cs, err := runtime.OpenChannel(
		context.Background(), channelID, dbPath,
		runtime.OpenChannelOptions{MustExist: true},
	)
	if err != nil {
		t.Fatalf("open durable timer reader: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs.Assembly.Timers
}

func restartTimerFireAssertions(
	t *testing.T,
	fire restartTimerFire,
	armed restartTimerArmed,
) {
	t.Helper()
	if fire.err != "" {
		t.Fatalf("fired body could not do its follow-up work: %s", fire.err)
	}
	env := fire.env
	if env.ID != message.ID("timer:"+string(armed.timerID)) {
		t.Fatalf("fire envelope id = %q, want the deterministic id of timer %q",
			env.ID, armed.timerID)
	}
	if env.Kind != message.KindEvent || env.Type != restartTimerType {
		t.Fatalf("fire kind/type = %q/%q", env.Kind, env.Type)
	}
	if string(env.Payload) != restartTimerPayload {
		t.Fatalf("fire payload = %s, want %s (carried verbatim)", env.Payload, restartTimerPayload)
	}
	if len(env.Audience) != 1 || env.Audience[0] != armed.actorID {
		t.Fatalf("fire audience = %v, want the self-targeted [%s]", env.Audience, armed.actorID)
	}
	if env.Sender.ID != armed.actorID || env.Sender.Kind != actor.KindAgent {
		t.Fatalf("fire sender = %+v, want the arming author %s", env.Sender, armed.actorID)
	}
	if fire.followup == "" {
		t.Fatal("the fired body committed no follow-up work")
	}
}

// T3. After(..., TimerHomeDurable) is the durable-home arm of the schedule
// verb. Payload pass-through is not the interesting half — this drives one all
// the way to a REAL fire: the engine's due sweep, the fire sink's author
// admission, the harness write, the delivery pump, and the arming actor's own
// mailbox.
func TestDurableHomeTimerReallyFiresBackIntoItsAuthor(t *testing.T) {
	clock := newRestartShiftClock()
	fixture := newRestartTimerFixture(true)
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	h, err := Open(Config{
		ChannelID:             "restart-timer-inline",
		DBPath:                dbPath,
		CompositionResolver:   fixture,
		IntroductionResolver:  inertIntroductionResolver{},
		ReconcileInterval:     time.Hour,
		Clock:                 clock,
		Bootstrap:             true,
		BootstrapDeclarations: []DeclareRequest{restartTimerDeclaration()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	armed := restartRecv(t, "the durable timer to be armed", fixture.armed)
	if armed.err != "" || armed.timerID == "" {
		t.Fatalf("After(TimerHomeDurable) = %q err=%s", armed.timerID, armed.err)
	}
	restartRecv(t, "the arming body to reach its mailbox", fixture.started)

	// It is a DURABLE-home timer: the row is in the channel's timer table, not
	// in the engine's process memory.
	timers := openRestartTimerStoreReader(t, "restart-timer-inline", dbPath)
	fireAt, pending, err := timers.NextFireAt(ctx)
	if err != nil || !pending {
		t.Fatalf("durable timer row after arming: pending=%v err=%v", pending, err)
	}
	if fireAt <= time.Now().UnixMilli() {
		t.Fatalf("timer armed for %d, which is already due — the test would prove nothing", fireAt)
	}

	clock.jump(2 * time.Hour)
	fire := restartRecv(t, "the durable timer to fire", fixture.fired)
	restartTimerFireAssertions(t, fire, armed)
	if fire.actorID != armed.actorID {
		t.Fatalf("timer fired into %s, armed by %s", fire.actorID, armed.actorID)
	}

	row, found, err := h.query.LatestBySenderAndType(ctx, armed.actorID, restartTimerFollowupType)
	if err != nil || !found || row.Envelope.ID != fire.followup {
		t.Fatalf("follow-up work row: found=%v id=%q err=%v", found, row.Envelope.ID, err)
	}
	restartEventually(t, "the fired durable row to be retired", func() bool {
		_, stillPending, err := timers.NextFireAt(ctx)
		return err == nil && !stillPending
	})
}

// T4. The durable timer's whole reason to exist is that it outlives the process
// that armed it. One Home arms it and dies without firing; a second Home over
// the same db picks the row up on its own, fires it into the restored identity,
// and that identity does a new round of work off it.
func TestDurableTimerArmedBeforeARestartFiresInTheNextHome(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	const channelID = channel.ID("restart-timer-cycle")
	ctx := context.Background()

	first := newRestartTimerFixture(true)
	firstClock := newRestartShiftClock()
	h1, err := Open(Config{
		ChannelID:             channelID,
		DBPath:                dbPath,
		CompositionResolver:   first,
		IntroductionResolver:  inertIntroductionResolver{},
		ReconcileInterval:     time.Hour,
		Clock:                 firstClock,
		Bootstrap:             true,
		BootstrapDeclarations: []DeclareRequest{restartTimerDeclaration()},
	})
	if err != nil {
		t.Fatal(err)
	}
	armed := restartRecv(t, "the durable timer to be armed", first.armed)
	if armed.err != "" || armed.timerID == "" {
		t.Fatalf("After(TimerHomeDurable) = %q err=%s", armed.timerID, armed.err)
	}
	restartRecv(t, "the arming body to reach its mailbox", first.started)
	if _, found, err := h1.query.LatestBySenderAndType(
		ctx, armed.actorID, restartTimerFollowupType); err != nil || found {
		t.Fatalf("the timer already fired in its own Home: found=%v err=%v", found, err)
	}
	if err := h1.closeInternal("test-restart"); err != nil {
		t.Fatalf("close first Home: %v", err)
	}

	// The pending row is the only thing crossing the restart.
	timers := openRestartTimerStoreReader(t, channelID, dbPath)
	if _, pending, err := timers.NextFireAt(ctx); err != nil || !pending {
		t.Fatalf("durable timer row did not survive the restart: pending=%v err=%v", pending, err)
	}

	second := newRestartTimerFixture(false)
	secondClock := newRestartShiftClock()
	h2, err := Open(Config{
		ChannelID:            channelID,
		DBPath:               dbPath,
		CompositionResolver:  second,
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Clock:                secondClock,
		MustExistDB:          true,
	})
	if err != nil {
		t.Fatalf("restart Open: %v", err)
	}
	t.Cleanup(func() { _ = h2.closeInternal("test") })

	restarted := restartRecv(t, "the restored body to reach its mailbox", second.started)
	if restarted != armed.actorID {
		t.Fatalf("restored identity = %s, armed by %s", restarted, armed.actorID)
	}
	select {
	case unexpected := <-second.armed:
		t.Fatalf("the second life armed its own timer %q — the test would prove nothing",
			unexpected.timerID)
	default:
	}

	secondClock.jump(2 * time.Hour)
	fire := restartRecv(t, "the carried-over durable timer to fire", second.fired)
	restartTimerFireAssertions(t, fire, armed)
	if fire.actorID != armed.actorID {
		t.Fatalf("timer fired into %s, armed by %s", fire.actorID, armed.actorID)
	}

	row, found, err := h2.query.LatestBySenderAndType(ctx, armed.actorID, restartTimerFollowupType)
	if err != nil || !found || row.Envelope.ID != fire.followup {
		t.Fatalf("post-restart follow-up work row: found=%v id=%q err=%v",
			found, row.Envelope.ID, err)
	}
	restartEventually(t, "the fired durable row to be retired", func() bool {
		_, stillPending, err := timers.NextFireAt(ctx)
		return err == nil && !stillPending
	})
}
