package accessdoor

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// A caller that lives inside a channel directory names files the way its own
// process sees them — an absolute path on its own machine. That name has to
// become a ResourceID before it can be authorized or routed, and this is where.
//
// It is here rather than at either end of a message on purpose. The sender
// cannot be asked to do it (it would have to know the device it sits on, and
// every actor that forwarded a name would have to know too), and the receiver
// cannot (it does not know the sender's machine). The door already resolves the
// channel's mounts on every call, which is exactly what completing the name
// requires — so completing it anywhere else means shipping that somewhere it is
// not already needed.
//
// The pivot is the PATH, not the caller. A path names its device by lying under
// that device's channel directory: the same string is the same file whoever
// says it, so a browser — which runs on no device — can still name one. An
// earlier revision resolved against the caller's own placement instead, which
// made an unplaced caller unable to name any file and, worse, made the answer
// depend on who asked.
//
// The relative form is NOT handled here, and cannot be: a bare "a.pdf" is
// indistinguishable from a kv resource id, which is a legitimate name for this
// same door. Rejecting it belongs to a file-only entry point — see
// boundHandle.Open, which is the file byte verb and may be strict.

var errNoStorageMounts = errors.New("accessdoor: storage mounts unavailable")

// PathOutsideChannelError names an absolute path that lies under no reachable
// device of this channel. The known directories travel with it because a caller
// told only "outside" has to discover the boundary by probing, and a model
// probing a filesystem boundary is a long, confident, wrong walk.
//
// It also covers a path on a device that is currently offline, which cannot be
// told apart from a path that belongs nowhere: recognizing it would need that
// device's directory, and only the device can say what that is. So the message
// says what to do instead — name the file by address, which needs no directory
// and answers host_offline honestly.
type PathOutsideChannelError struct {
	Path  string
	Roots []string
}

// Error carries the whole recovery, not a terse condition. These errors exist
// to be read by the caller that hit them, and the caller is increasingly a
// model — which acts on the words it is given and invents the rest. Keeping the
// guidance here rather than at the boundary that renders it also means the
// wording lives next to the condition that produces it, so there is one place
// to be right and no importer needs to learn what a path is.
func (e *PathOutsideChannelError) Error() string {
	if len(e.Roots) == 0 {
		return e.Path + " cannot be resolved: no device of this channel is reachable right now, " +
			"so there is no directory to measure it against. Name the file by its " +
			resourcespec.DaemonScheme + ":// address instead."
	}
	return e.Path + " is not in this channel's storage. Reachable channel directories are: " +
		strings.Join(e.Roots, ", ") + ". If the file is on a device that is currently offline, " +
		"name it by its " + resourcespec.DaemonScheme + ":// address instead — that needs no directory. " +
		"This is not a permission refusal, so retrying this path unchanged will not help."
}

// PathAmbiguousError names a path lying under more than one device's channel
// directory. It cannot happen while daemon ids are unique and appear in the
// layout, but picking one silently is how a wrong file gets read for years, so
// the collision is answered rather than resolved.
type PathAmbiguousError struct {
	Path  string
	Hosts []string
}

func (e *PathAmbiguousError) Error() string {
	return e.Path + " lies under the channel directory of more than one device (" +
		strings.Join(e.Hosts, ", ") + "), so it names no single file. Use the " +
		resourcespec.DaemonScheme + ":// address of the one you mean."
}

// PathRelativeError names a path that is neither an address nor absolute. It
// cannot be completed: a caller with a shell changes its working directory, so
// there is no base this side can supply.
type PathRelativeError struct{ Path string }

func (e *PathRelativeError) Error() string {
	return "give the absolute path instead of " + e.Path +
		": you change your working directory, so this side has no base to resolve a relative path against."
}

// looksAbsolute reports whether raw is a device-local absolute path rather than
// an address or an opaque resource id.
func looksAbsolute(raw string) bool {
	return strings.HasPrefix(raw, "/")
}

func isFileAddress(raw string) bool {
	return strings.HasPrefix(raw, resourcespec.DaemonScheme+"://")
}

// normalizeFileName turns a device-local absolute path into the canonical file
// address. Addresses and opaque ids pass through untouched — this widens what
// the door accepts, it never reinterprets what it already accepted.
func (d *door) normalizeFileName(ctx context.Context, caller actor.ActorID, id resource.ResourceID) (resource.ResourceID, error) {
	raw := string(id)
	if !looksAbsolute(raw) {
		return id, nil
	}
	mount, rel, err := d.mountHolding(ctx, caller, raw)
	if err != nil {
		return "", err
	}
	if rel == "" {
		// The channel directory itself: a real path, but not a file. Naming it
		// as one is the same mistake as naming a path outside, and the same
		// answer tells the caller where it stands.
		return "", d.outsideError(ctx, raw)
	}
	address, err := resourcespec.FormatFileAddress(resourcespec.FileAddress{
		Scheme: resourcespec.DaemonScheme, Host: mount.Name,
		Channel: d.deps.ChannelName, Path: rel,
	})
	if err != nil {
		return "", err
	}
	return resource.ResourceID(address), nil
}

// normalizeFilePrefix is normalizeFileName for a listing prefix. The channel
// directory itself is a legal prefix here — it means "everything" — which is
// the one place the empty remainder is an answer rather than a mistake.
func (d *door) normalizeFilePrefix(ctx context.Context, caller actor.ActorID, prefix string) (string, error) {
	if !looksAbsolute(prefix) {
		return prefix, nil
	}
	mount, rel, err := d.mountHolding(ctx, caller, prefix)
	if err != nil {
		return "", err
	}
	return resourcespec.DaemonScheme + "://" + mount.Name + "/" + d.deps.ChannelName + "/" + rel, nil
}

// mountHolding finds the one device whose channel directory contains abs, and
// the portion of abs below it. Membership is checked first: which devices this
// channel has, and where they keep their directories, is not something a
// non-member gets to probe by submitting paths.
func (d *door) mountHolding(ctx context.Context, caller actor.ActorID, abs string) (StorageMount, string, error) {
	if _, err := d.authorizeMember(ctx, caller); err != nil {
		return StorageMount{}, "", err
	}
	mounts, err := d.reachableMounts(ctx)
	if err != nil {
		return StorageMount{}, "", err
	}
	var holder StorageMount
	var rel string
	var hosts []string
	for _, mount := range mounts {
		portion, ok := underRoot(mount.Root, abs)
		if !ok {
			continue
		}
		hosts = append(hosts, mount.Name)
		holder, rel = mount, portion
	}
	switch len(hosts) {
	case 0:
		return StorageMount{}, "", d.outsideError(ctx, abs)
	case 1:
		return holder, rel, nil
	default:
		sort.Strings(hosts)
		return StorageMount{}, "", &PathAmbiguousError{Path: abs, Hosts: hosts}
	}
}

// reachableMounts is every bound device that has reported its channel
// directory. An offline device reports nothing, so it simply is not here — see
// PathOutsideChannelError for why that is answered rather than guessed at.
func (d *door) reachableMounts(ctx context.Context) ([]StorageMount, error) {
	if d.deps.StorageMounts == nil {
		return nil, errNoStorageMounts
	}
	all, err := d.deps.StorageMounts.ListStorageMounts(ctx, d.deps.ChannelID)
	if err != nil {
		return nil, err
	}
	out := make([]StorageMount, 0, len(all))
	for _, mount := range all {
		if mount.Online && mount.Root != "" && mount.Name != "" {
			out = append(out, mount)
		}
	}
	return out, nil
}

func (d *door) outsideError(ctx context.Context, abs string) error {
	mounts, err := d.reachableMounts(ctx)
	if err != nil {
		return err
	}
	roots := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		roots = append(roots, mount.Root)
	}
	sort.Strings(roots)
	return &PathOutsideChannelError{Path: abs, Roots: roots}
}

// underRoot reports the portion of abs that lies below root. Both are cleaned
// first so "/a/b/../b/c" and a trailing slash on the root do not decide the
// answer; the boundary is compared on whole segments, so "/a/bc" is not under
// "/a/b". The root itself answers ("", true) — inside, but naming no file;
// callers that need a file reject the empty portion themselves.
func underRoot(root, abs string) (string, bool) {
	root, abs = path.Clean(root), path.Clean(abs)
	if root == "" || root == "." || !strings.HasPrefix(root, "/") {
		return "", false
	}
	if abs == root {
		return "", true
	}
	if !strings.HasPrefix(abs, root+"/") {
		return "", false
	}
	return strings.TrimPrefix(abs, root+"/"), true
}

// LocalPath renders a channel-relative path back as the device sees it. A
// caller that spoke in device-local paths is answered in them, so every answer
// it gets can be handed straight back to a shell without a translation step it
// has to perform itself.
func LocalPath(root, rel string) string {
	if root == "" {
		return rel
	}
	return path.Join(path.Clean(root), rel)
}
