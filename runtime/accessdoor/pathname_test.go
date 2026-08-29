package accessdoor

import (
	"errors"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

const testRoot = "/srv/atoll/daemons/daemon-a/channels/c0.c"

func pathDoor(t *testing.T) *door {
	t.Helper()
	return &door{deps: Deps{
		Registry:      &fakeRegistry{},
		Drivers:       DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority:     &fakeMembership{lookupFound: true, lookupHost: "daemon-a"},
		State:         &fakeStateStore{},
		ChannelID:     "c",
		ChannelName:   "c0.c",
		StorageMounts: directMounts{root: testRoot},
	}}
}

// An actor working inside the channel directory names files the way its own
// process sees them. That name has to become an address before anything can be
// authorized or routed, and the door is the one place that already knows both
// halves it takes — which device the caller sits on, and which channel this is.
func TestADeviceLocalPathBecomesTheChannelAddress(t *testing.T) {
	got, err := pathDoor(t).normalizeFileName(t.Context(), "agent:a", resource.ResourceID(testRoot+"/docs/report.txt"))
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if want := resource.ResourceID("daemon://daemon-a/c/docs/report.txt"); got != want {
		t.Fatalf("normalize = %q, want %q", got, want)
	}
}

// Cleaning happens before the boundary is judged, so a path that walks out and
// back in is the file it resolves to, not a refusal.
func TestAPathThatWalksOutAndBackInIsTheFileItResolvesTo(t *testing.T) {
	got, err := pathDoor(t).normalizeFileName(t.Context(), "agent:a", resource.ResourceID(testRoot+"/docs/../docs/report.txt"))
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if want := resource.ResourceID("daemon://daemon-a/c/docs/report.txt"); got != want {
		t.Fatalf("normalize = %q, want %q", got, want)
	}
}

// The refusal carries the boundary. Told only "outside", a caller has to
// discover where the line is by probing paths — and the caller here is
// increasingly a model, which probes confidently and at length.
func TestARefusedPathNamesTheBoundary(t *testing.T) {
	_, err := pathDoor(t).normalizeFileName(t.Context(), "agent:a", "/etc/passwd")
	var outside *PathOutsideChannelError
	if !errors.As(err, &outside) {
		t.Fatalf("err = %v, want PathOutsideChannelError", err)
	}
	if len(outside.Roots) != 1 || outside.Roots[0] != testRoot {
		t.Fatalf("roots = %q, want [%q]", outside.Roots, testRoot)
	}
}

// Escapes are judged on whole segments and after cleaning, so neither a
// sibling that shares a prefix nor a dot-dot walk gets in. The channel
// directory itself is a real path but names no file.
func TestOnlyRealFilesUnderTheChannelDirectoryResolve(t *testing.T) {
	for _, raw := range []string{
		testRoot + "-other/report.txt",            // shares a prefix, different directory
		testRoot + "/../daemon-b/channels/c0.c/x", // walks out
		testRoot,       // the directory itself
		testRoot + "/", // same, spelled with a slash
		"/srv/atoll/daemons/daemon-a/channels/c0.d", // a different channel's directory
	} {
		_, err := pathDoor(t).normalizeFileName(t.Context(), "agent:a", resource.ResourceID(raw))
		var outside *PathOutsideChannelError
		if !errors.As(err, &outside) {
			t.Fatalf("%q: err = %v, want PathOutsideChannelError", raw, err)
		}
	}
}

// Widening what the door accepts must not reinterpret what it already
// accepted: an address is already complete, and an opaque kv id is not a path
// at all.
func TestNamesThatAreAlreadyCompletePassThroughUntouched(t *testing.T) {
	for _, raw := range []string{
		"daemon://daemon-a/c/docs/report.txt",
		"config",
		"some/kv/key",
		"",
	} {
		got, err := pathDoor(t).normalizeFileName(t.Context(), "agent:a", resource.ResourceID(raw))
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if string(got) != raw {
			t.Fatalf("%q normalized to %q; nothing but an absolute path may be rewritten", raw, got)
		}
	}
}

// A path names its device by lying under that device's directory, so the same
// string is the same file whoever says it. In particular a browser — which runs
// on no device at all — can still name one. An earlier revision resolved
// against the caller's own placement and so refused every path an unplaced
// caller gave, which is both less useful and wrong: it made the answer depend
// on who asked rather than on what was asked about.
func TestAPathResolvesTheSameForACallerThatIsOnNoDevice(t *testing.T) {
	d := pathDoor(t)
	d.deps.Authority = &fakeMembership{isMember: true} // a member, placed nowhere
	got, err := d.normalizeFileName(t.Context(), "human:root:1", resource.ResourceID(testRoot+"/docs/report.txt"))
	if err != nil {
		t.Fatalf("normalize for an unplaced caller: %v", err)
	}
	if want := resource.ResourceID("daemon://daemon-a/c/docs/report.txt"); got != want {
		t.Fatalf("normalize = %q, want %q", got, want)
	}
}

// Which devices a channel has, and where they keep their directories, is not
// something a non-member gets to map out by submitting paths and reading the
// refusals.
func TestANonMemberLearnsNothingAboutTheChannelsDirectories(t *testing.T) {
	d := pathDoor(t)
	d.deps.Authority = &fakeMembership{}
	_, err := d.normalizeFileName(t.Context(), "human:stranger:1", resource.ResourceID(testRoot+"/docs/report.txt"))
	if err == nil {
		t.Fatal("a non-member resolved a path")
	}
	if strings.Contains(err.Error(), testRoot) {
		t.Fatalf("refusal leaked a channel directory to a non-member: %v", err)
	}
}

// Two devices claiming the same directory names no single file. It cannot
// happen while daemon ids are unique and sit in the layout, but picking one
// silently is how the wrong file gets read for years.
func TestAPathUnderTwoDevicesIsAnsweredNotGuessed(t *testing.T) {
	d := pathDoor(t)
	d.deps.StorageMounts = directMounts{root: testRoot, others: []StorageMount{
		{DaemonID: "daemon-b", Name: "laptop-b", Online: true, Root: testRoot},
	}}
	_, err := d.normalizeFileName(t.Context(), "agent:a", resource.ResourceID(testRoot+"/docs/report.txt"))
	var ambiguous *PathAmbiguousError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("err = %v, want PathAmbiguousError", err)
	}
	if len(ambiguous.Hosts) != 2 {
		t.Fatalf("hosts = %q, want both devices named", ambiguous.Hosts)
	}
}

// An offline device reports no directory, so a path on it cannot be told from a
// path that belongs nowhere. The refusal says so and names the way out that
// needs no directory at all.
func TestWithNoReachableDeviceThePathIsRefusedWithTheWayOut(t *testing.T) {
	d := pathDoor(t)
	d.deps.StorageMounts = directMounts{root: ""} // attached, but nothing reported
	_, err := d.normalizeFileName(t.Context(), "agent:a", resource.ResourceID(testRoot+"/docs/report.txt"))
	var outside *PathOutsideChannelError
	if !errors.As(err, &outside) {
		t.Fatalf("err = %v, want PathOutsideChannelError", err)
	}
	if !strings.Contains(outside.Error(), "daemon://") {
		t.Fatalf("refusal offers no way out: %v", outside)
	}
}

// A listing prefix may name the channel directory itself — there it means
// "everything", which is the one place the empty remainder is an answer.
func TestAListingPrefixMayNameTheChannelDirectory(t *testing.T) {
	got, err := pathDoor(t).normalizeFilePrefix(t.Context(), "agent:a", testRoot)
	if err != nil {
		t.Fatalf("normalize prefix: %v", err)
	}
	if want := "daemon://daemon-a/c/"; got != want {
		t.Fatalf("prefix = %q, want %q", got, want)
	}
}

// Open is the file byte verb, so it may refuse a name invoke cannot: a bare
// "a.pdf" is a legal opaque kv id, but as a file name it has no base — the
// caller has a shell and moves. Letting it fall through would answer
// resource_not_found, which reads as "that file is gone".
func TestOpenRefusesARelativeNameInsteadOfCallingItMissing(t *testing.T) {
	d := pathDoor(t)
	h := boundHandle{door: d, caller: "agent:a", authority: accessAuthority("agent:a")}
	_, _, err := h.Open(t.Context(), "a.pdf", access.OpRead)
	var relative *PathRelativeError
	if !errors.As(err, &relative) {
		t.Fatalf("err = %v, want PathRelativeError", err)
	}
}
