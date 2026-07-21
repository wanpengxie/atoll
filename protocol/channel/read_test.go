package channel

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestReaderIdentityIsModeExclusive(t *testing.T) {
	if !(Reader{Mode: ReaderMember, ActorID: actor.ActorID("agent:one")}).Valid() {
		t.Fatal("actor-only member reader rejected")
	}
	if (Reader{Mode: ReaderMember, ActorID: actor.ActorID("agent:one"), Principal: "login"}).Valid() {
		t.Fatal("member reader accepted a login principal")
	}
	if !(Reader{Mode: ReaderObserver, Principal: "login"}).Valid() {
		t.Fatal("principal-only observer reader rejected")
	}
	if (Reader{Mode: ReaderObserver, ActorID: actor.ActorID("agent:one"), Principal: "login"}).Valid() {
		t.Fatal("observer reader accepted an actor identity")
	}
}
