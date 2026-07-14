package actorrt

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

func TestSealRejectsSpawnAndForkWithoutRunningBuilders(t *testing.T) {
	rt, _ := New(Config{})
	parent, _, err := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
	if err != nil {
		t.Fatal(err)
	}
	rt.Seal()
	rt.Seal()

	spawnBuilt := false
	if _, built, err := rt.SpawnIfAbsent("new", actor.KindAgent, func(Incarnation) Actor {
		spawnBuilt = true
		return newRecordActor()
	}); !errors.Is(err, ErrRuntimeSealed) || built {
		t.Fatalf("SpawnIfAbsent after Seal = built %v, err %v", built, err)
	}
	if spawnBuilt {
		t.Fatal("sealed SpawnIfAbsent ran builder")
	}

	forkBuilt := false
	if _, err := rt.Fork(parent, "child", actor.KindAgent, func(Incarnation) Actor {
		forkBuilt = true
		return newRecordActor()
	}); !errors.Is(err, ErrRuntimeSealed) {
		t.Fatalf("Fork after Seal err = %v", err)
	}
	if forkBuilt {
		t.Fatal("sealed Fork ran builder")
	}
	if _, ok := rt.Stat("new"); ok {
		t.Fatal("sealed spawn entered population")
	}
	if _, ok := rt.Stat("child"); ok {
		t.Fatal("sealed fork entered population")
	}
}

func TestSealWinsSpawnAndForkBuildStraddles(t *testing.T) {
	t.Run("SpawnIfAbsent", func(t *testing.T) {
		rt, _ := New(Config{})
		entered, release := make(chan struct{}), make(chan struct{})
		result := make(chan error, 1)
		go func() {
			_, _, err := rt.SpawnIfAbsent("new", actor.KindAgent, func(Incarnation) Actor {
				close(entered)
				<-release
				return newRecordActor()
			})
			result <- err
		}()
		<-entered
		rt.Seal()
		close(release)
		if err := <-result; !errors.Is(err, ErrRuntimeSealed) {
			t.Fatalf("err = %v", err)
		}
		if _, ok := rt.Stat("new"); ok {
			t.Fatal("straddling spawn entered sealed runtime")
		}
	})
	t.Run("Fork", func(t *testing.T) {
		rt, _ := New(Config{})
		parent, _, _ := rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
		entered, release := make(chan struct{}), make(chan struct{})
		result := make(chan error, 1)
		go func() {
			_, err := rt.Fork(parent, "child", actor.KindAgent, func(Incarnation) Actor {
				close(entered)
				<-release
				return newRecordActor()
			})
			result <- err
		}()
		<-entered
		rt.Seal()
		close(release)
		if err := <-result; !errors.Is(err, ErrRuntimeSealed) {
			t.Fatalf("err = %v", err)
		}
		if _, ok := rt.Stat("child"); ok {
			t.Fatal("straddling fork entered sealed runtime")
		}
	})
}

func TestSealWinsAttachAfterHandshakeBeforeRegistration(t *testing.T) {
	rt, _ := New(Config{})
	host, remote := net.Pipe()
	codec := ipc.NewCodec(remote, remote)
	peerDone := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "lease"})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			peerDone <- err
			return
		}
		frame, err := codec.Read()
		if err == nil && frame.Kind != ipc.KindHandshakeAck {
			err = errors.New("wrong handshake ack")
		}
		peerDone <- err
	}()
	prepared, err := rt.PrepareHandshake(context.Background(), host, Sinks{Emit: nopEmit}, staticResolve("remote"), nil, nil, func(Incarnation) {})
	if err != nil {
		t.Fatal(err)
	}
	rt.Seal()
	if _, err := prepared.Commit(func() bool { return true }); !errors.Is(err, ErrRuntimeSealed) {
		t.Fatalf("Attach err = %v", err)
	}
	if _, ok := rt.Stat("remote"); ok {
		t.Fatal("straddling attach entered sealed runtime")
	}
	select {
	case err := <-peerDone:
		if err == nil {
			t.Fatal("sealed Attach sent an ACK")
		}
	case <-time.After(time.Second):
		t.Fatal("sealed Attach did not close uncommitted stream")
	}
}

func TestPreparedAttachIsInvisibleAndStopAllReapsIt(t *testing.T) {
	rt, deliver := New(Config{ZombieGrace: time.Second})

	host, remote := net.Pipe()
	codec := ipc.NewCodec(remote, remote)
	allowRead := make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "lease"})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			peerDone <- err
			return
		}
		<-allowRead
		_, err := codec.Read()
		peerDone <- err
	}()
	prepared, err := rt.PrepareHandshake(context.Background(), host, Sinks{Emit: nopEmit}, staticResolve("remote"), nil, nil, func(Incarnation) {})
	if err != nil {
		t.Fatal(err)
	}

	attachDone := make(chan error, 1)
	go func() {
		_, err := prepared.Commit(func() bool { return true })
		attachDone <- err
	}()
	waitPreparedCandidate(t, rt, "remote")

	if ids := rt.LiveIDs(); len(ids) != 0 {
		t.Fatalf("prepared port visible in LiveIDs: %v", ids)
	}
	if _, ok := rt.Stat("remote"); ok {
		t.Fatal("prepared port visible in Stat")
	}
	if _, ok := rt.CurrentIncarnation("remote"); ok {
		t.Fatal("prepared port visible through CurrentIncarnation")
	}
	result, err := deliver.Deliver([]actor.ActorID{"remote"}, env("before-ack"))
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Per["remote"]; got != NotHosted {
		t.Fatalf("deliver to prepared port = %v, want NotHosted", got)
	}

	rt.StopAll()
	close(allowRead)
	if err := <-attachDone; err == nil {
		t.Fatal("Attach committed after StopAll won during ACK preparation")
	}
	select {
	case err := <-peerDone:
		if err == nil {
			t.Fatal("peer received ACK after StopAll")
		}
	case <-time.After(time.Second):
		t.Fatal("prepared peer was not closed by StopAll")
	}
	waitZombiesZero(t, rt, time.Second)
	if got := rt.LeakedTotal(); got != 0 {
		t.Fatalf("LeakedTotal = %d, want 0", got)
	}
}

func TestDespawnIDCancelsPreparedCandidateButReportsNoLiveBody(t *testing.T) {
	rt, _ := New(Config{})

	host, remote := net.Pipe()
	codec := ipc.NewCodec(remote, remote)
	allowRead := make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "lease"})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			peerDone <- err
			return
		}
		<-allowRead
		_, err := codec.Read()
		peerDone <- err
	}()
	prepared, err := rt.PrepareHandshake(context.Background(), host, Sinks{Emit: nopEmit}, staticResolve("remote"), nil, nil, func(Incarnation) {})
	if err != nil {
		t.Fatal(err)
	}
	attachDone := make(chan error, 1)
	go func() {
		_, err := prepared.Commit(func() bool { return true })
		attachDone <- err
	}()
	waitPreparedCandidate(t, rt, "remote")

	if stoppedLive := rt.DespawnID("remote"); stoppedLive {
		t.Fatal("DespawnID reported a prepared-only candidate as a live embodiment")
	}
	close(allowRead)
	if err := <-attachDone; err == nil {
		t.Fatal("candidate committed after DespawnID cancelled it")
	}
	select {
	case err := <-peerDone:
		if err == nil {
			t.Fatal("cancelled candidate delivered an ACK")
		}
	case <-time.After(time.Second):
		t.Fatal("candidate peer was not closed")
	}
	if _, live := rt.Stat("remote"); live {
		t.Fatal("cancelled candidate became live")
	}
	waitZombiesZero(t, rt, time.Second)
}

func waitPreparedCandidate(t *testing.T, rt *Runtime, id actor.ActorID) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		rt.mu.Lock()
		_, ok := rt.prepared[id]
		rt.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("prepared candidate %q was not registered", id)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPrepareHandshakeSendsNoAckAndAbortLeavesNoRuntimeState(t *testing.T) {
	rt, _ := New(Config{})
	host, remote := net.Pipe()
	codec := ipc.NewCodec(remote, remote)
	peerRead := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "lease"})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			peerRead <- err
			return
		}
		_, err := codec.Read()
		peerRead <- err
	}()

	prepared, err := rt.PrepareHandshake(context.Background(), host, Sinks{Emit: nopEmit}, staticResolve("remote"), nil, nil, func(Incarnation) {})
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared.ID(); got != "remote" {
		t.Fatalf("prepared ID = %q, want remote", got)
	}
	select {
	case err := <-peerRead:
		t.Fatalf("peer read completed before Commit/Abort: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if len(rt.LiveIDs()) != 0 {
		t.Fatal("parsed handshake entered Runtime before Commit")
	}
	prepared.Abort()
	select {
	case err := <-peerRead:
		if err == nil {
			t.Fatal("Abort emitted an ACK")
		}
	case <-time.After(time.Second):
		t.Fatal("Abort did not close the handshake transport")
	}
	if len(rt.LiveIDs()) != 0 || len(rt.Zombies()) != 0 {
		t.Fatalf("Abort left runtime state: live=%v zombies=%v", rt.LiveIDs(), rt.Zombies())
	}
}

func TestStopAllSealsRuntime(t *testing.T) {
	rt, _ := New(Config{})
	rt.StopAll()
	if _, _, err := rt.SpawnIfAbsent("new", actor.KindAgent, static(newRecordActor())); !errors.Is(err, ErrRuntimeSealed) {
		t.Fatalf("SpawnIfAbsent after StopAll err = %v", err)
	}
}
