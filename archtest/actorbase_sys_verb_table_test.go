package archtest

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
)

// sysVerbTable is the EXACT method set of actorbase.Sys — the whole table, in
// one place, as source text a reviewer can read against the interface.
//
// The count (16 today) is deliberately NOT the assertion. The load-bearing
// judgement is "one verb per column of the capability face, and no parallel,
// suffixed twin of any column": a table that grows a genuinely new atom is a
// one-line edit here plus a new atom in the surjection sweep, while a table
// that grows RespondEnvelope beside Reply — a second way to say the same word,
// differing only in which ledger authorises it — is precisely what this test
// exists to make impossible to land quietly.
var sysVerbTable = []string{
	// Pen — response writes (the Msg in hand names the request; MsgOrigin, not
	// a second verb, names which ledger authorises the write).
	"Reply", "Fail", "Progress",
	// Pen — writes that take on no closure obligation, and the one that does.
	"Emit", "Post", "Call",
	// Access plane (two handles, each with its own method set — the surjection
	// sweep is what enumerates those).
	"Resource", "State",
	// Schedule.
	"After", "CancelTimer",
	// Lifecycle.
	"Fork", "End",
	// Actor context / observability.
	"Self", "PublishObs",
	// Input stream + process life.
	"Recv", "Life",
}

// parallelVerbSuffixes are the shapes a re-split of the table would take. They
// are named separately from the exact-set check so the failure message says
// WHAT went wrong rather than only that the set moved: a verb ending in
// Identity or Envelope is a column being served by two methods again, with the
// real distinction (which authority, which ctx) hidden inside a method name
// instead of being a caller-visible fact.
var parallelVerbSuffixes = []string{"Identity", "Envelope"}

// TestActorbaseSysHasExactlyTheDeclaredVerbTable is the "no parallel verbs"
// lock. It is a DIFFERENT assertion from
// TestActorbaseSysIsSurjectiveOverCapabilityFace and neither subsumes the
// other — the two are the two directions of the same map:
//
//   - the surjection sweep proves the table is not MISSING anything: every
//     semantic atom of the closed capability face (Pen kinds × terminal status,
//     access.Operation values, schedule/lifecycle/context atoms) is reachable
//     through some Sys verb. It asks MethodByName and is satisfied by a
//     superset.
//   - this test proves the table has nothing EXTRA: the method set is exactly
//     the enumerated one. It is satisfied only by equality.
//
// A surjection assertion can never catch a second verb for an atom that is
// already covered — which is the failure mode that actually happened here (a
// parallel Identity-suffixed drive face grew alongside the table and no
// archtest went red for it, three audits running). Hence a separate test, not
// a stricter version of the old one.
func TestActorbaseSysHasExactlyTheDeclaredVerbTable(t *testing.T) {
	sysType := reflect.TypeOf((*actorbase.Sys)(nil)).Elem()

	want := map[string]bool{}
	for _, name := range sysVerbTable {
		if want[name] {
			t.Fatalf("sysVerbTable lists %q twice — the table is the assertion, keep it a set", name)
		}
		want[name] = true
	}

	got := map[string]bool{}
	for i := 0; i < sysType.NumMethod(); i++ {
		got[sysType.Method(i).Name] = true
	}

	var v []string
	for name := range got {
		if !want[name] {
			v = append(v, fmt.Sprintf(
				"Sys has method %q, which is NOT in the declared verb table — if it is a genuinely new capability-face atom, add it here AND to the surjection sweep's atom map; if it is a second way to say a word the table already has, it does not belong on Sys",
				name))
		}
		for _, suffix := range parallelVerbSuffixes {
			if name != suffix && strings.HasSuffix(name, suffix) {
				v = append(v, fmt.Sprintf(
					"Sys verb %q is a %s-suffixed twin — a column served by two methods, with the real distinction (which ledger authorises the write, which ctx it runs under) buried in a method name. Put the distinction on the data (see actorbase.MsgOrigin), not in a parallel verb",
					name, suffix))
			}
		}
	}
	for name := range want {
		if !got[name] {
			v = append(v, fmt.Sprintf(
				"the verb table declares %q but Sys has no such method — a removed verb must be removed from this table in the same change that removes it from the interface",
				name))
		}
	}
	sort.Strings(v)
	failViolations(t, "actorbase.Sys exact verb table (surjection proves nothing is missing; this proves nothing is doubled)", v)

	// A count check would be redundant given the set equality above; it is
	// asserted anyway because the two can only disagree if want/got stopped
	// being sets, and that would silently weaken everything above it.
	if sysType.NumMethod() != len(sysVerbTable) {
		t.Fatalf("Sys has %d methods but the table declares %d", sysType.NumMethod(), len(sysVerbTable))
	}
}
