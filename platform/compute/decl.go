package compute

import (
	"encoding/json"
	"io"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// ActorFactorySource resolves one body's factory at BUILD time, from the class
// and config the Host's own desired carries — the same shape the server host
// uses. There is no plan-generation snapshot to consult: the spec in the build
// input IS the generation (it came off this Host's desired), so a newer plan
// cannot feed an older in-flight build by construction, and a row this daemon
// cannot build fails alone, at its own build, with its own log — it holds no
// other actor's update hostage.
//
// The eager two-ledger shape this replaces (pre-built factory table, exact
// generation lookup, whole-plan last-known-good) protected nothing real: a
// plan-superseded body is already truth-dead — home refuses its stale-attempt
// writes and tears down its route — so rejecting a whole plan to "keep the old
// one running" kept zombies running and held healthy rows back with them.
type ActorFactorySource interface {
	BuildClass(
		id actor.ActorID,
		class string,
		config json.RawMessage,
	) (def platform.ActorFactory, ok bool)
}

// CompartmentResources are the channel-scoped physical resources constructed
// as one transaction when the first lane for a coordinate arrives.
type CompartmentResources struct {
	Factories       ActorFactorySource
	LocalFileOpener LocalFileOpener
	Close           func() error
}

type CompartmentBuilder func(
	channelID string,
	workspaceDir string,
) (CompartmentResources, error)

// LocalFileOpener mirrors platform/internal/link.LocalFileOpener's exact
// method set (期11 spec §5/§3.4's "daemon 本地颁 os.Root 子句柄") — a
// SEPARATE named interface (not an alias) purely so drivers/devicehost's
// wiring code reads against platform's own public vocabulary rather than
// reaching into platform/internal/link (which it cannot import); Go's
// structural interface typing makes the two directly interchangeable at
// the compartment builder boundary with no adapter needed.
type LocalFileOpener interface {
	OpenRead(path string) (io.ReadSeekCloser, error)
	OpenWrite(path string) (accessdoor.WriteHandle, error)
	Create(path string) error
	Delete(path string) error
	Stat(path string) (FileInfo, bool, error)
	List(prefix string) ([]FileInfo, error)
}

type FileInfo struct {
	Path string
	Size int64
	// ModifiedAt is Unix milliseconds, zero when the device reported none.
	ModifiedAt int64
}
