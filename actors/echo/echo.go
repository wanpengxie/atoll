// Package echo provides a minimal actor for dev/test — the actorbase-spec-v1
// §4 S5a "concept budget" consumer: a bare Proc loop over sys.Recv() proving
// the verb table alone (Recv/Reply/Fail) suffices for the simplest possible
// actor, with zero extra concepts riding along.
package echo

import (
	"fmt"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
)

const DefaultActorID actor.ActorID = "echo"

// TypeSay is the one request type this actor understands: echo the request
// payload back unchanged in a completed response.
const TypeSay = "echo.say"

const actorDoc = "Minimal dev/test actor: echo.say replies with its request " +
	"payload unchanged; any other type fails type_unsupported."

// New is this actor's actorbase.Def constructor (registry.Constructor's New
// func, spec §1.6: zero parameters, spec/deps live in the closure that built
// Def — echo has neither, so New is just run itself, no closure needed).
func New() (actorbase.Proc, error) {
	return run, nil
}

// Def is the actorbase.Def this actor registers under.
func Def() actorbase.Def {
	return actorbase.Def{Doc: actorDoc, New: New}
}

// run is the Proc body: entry = birth, return = death (spec §1.6). It is a
// bare loop, not Serve's routes-table sugar — S5a's point is that the raw
// contract alone (no framework layer) is already sufficient here.
func run(sys actorbase.Sys) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		switch msg.Type {
		case TypeSay:
			_, _ = sys.Reply(msg, msg.Payload)
		default:
			_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("echo actor does not handle %s", msg.Type))
		}
	}
}
