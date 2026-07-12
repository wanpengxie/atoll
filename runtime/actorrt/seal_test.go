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
	parked, release := make(chan struct{}), make(chan struct{})
	rt.attachStraddleHook = func() { close(parked); <-release }
	host, remote := net.Pipe()
	codec := ipc.NewCodec(remote, remote)
	acked := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "lease"})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			acked <- err
			return
		}
		frame, err := codec.Read()
		if err == nil && frame.Kind != ipc.KindHandshakeAck {
			err = errors.New("wrong handshake ack")
		}
		acked <- err
	}()
	result := make(chan error, 1)
	go func() {
		_, err := rt.Attach(context.Background(), host, Sinks{Emit: nopEmit}, staticResolve("remote"), nil, nil)
		result <- err
	}()
	<-parked
	if err := <-acked; err != nil {
		t.Fatal(err)
	}
	rt.Seal()
	close(release)
	if err := <-result; !errors.Is(err, ErrRuntimeSealed) {
		t.Fatalf("Attach err = %v", err)
	}
	if _, ok := rt.Stat("remote"); ok {
		t.Fatal("straddling attach entered sealed runtime")
	}
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := codec.Read(); err == nil {
		t.Fatal("sealed Attach did not close acknowledged stream")
	}
}

func TestStopAllSealsRuntime(t *testing.T) {
	rt, _ := New(Config{})
	rt.StopAll()
	if _, _, err := rt.SpawnIfAbsent("new", actor.KindAgent, static(newRecordActor())); !errors.Is(err, ErrRuntimeSealed) {
		t.Fatalf("SpawnIfAbsent after StopAll err = %v", err)
	}
}
