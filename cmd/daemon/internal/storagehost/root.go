package storagehost

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

// pathSegmentPattern is the shared charset assert for every value this
// package ever turns into a filesystem path segment (channelID, coord) —
// §8.4's red line ("opaque id 永不当路径...channelID同律:用作路径段前assert
// uuid/charset"). Deliberately excludes '.' (so "." and ".." are structurally
// unrepresentable, not merely blocked by a separate special-case check) and
// '/' (no nesting via a single segment). Both this package's inputs are
// substrate-generated day-1 (coord: resourcespec.GenerateCoord's hex(sha256);
// channelID: whatever shape the app layer mints) — "substrate-generated" is
// not itself a trust argument (§8.4 asserts regardless, "路径全substrate自
// 生成,无不可信输入面" describes provenance, not a reason to skip the assert).
var pathSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,200}$`)

// assertPathSegment enforces pathSegmentPattern, returning a distinguishable
// error (never silently truncating or escaping) — the assert point §8.4
// requires "before first use as a path segment", exercised by every
// constructor below before any filepath.Join.
func assertPathSegment(kind, value string) error {
	if !pathSegmentPattern.MatchString(value) {
		return fmt.Errorf("storagehost: %s %q is not a legal path segment (must match %s)", kind, value, pathSegmentPattern.String())
	}
	return nil
}

// channelRoot is one channel's resource tree — an os.Root confined to
// <workspaceRoot>/resources/<channelID> (§4.2's layout), with the live/ and
// staging/ subdirectories pre-created. Every filesystem-touching method in
// this package operates through the Root handle, never a bare os.* call
// against a manually-joined path — os.Root's own structural confinement
// (chroot-style, symlinks may not escape) is the mechanism §8.6's "本地句柄
// 无防伪...结构性防越界" and design doc C2's "禁复用 device.resolvePath 的
// Clean+HasPrefix 形" both point at.
type channelRoot struct {
	root *os.Root
}

// liveDir / stagingDir are the two subdirectory names under a channelRoot —
// constants, not configuration: §4.2's "布局第一版即定形" pins this layout,
// changing it is a migration, not a config knob.
const (
	liveDir    = "live"
	stagingDir = "staging"
)

// openChannelRoot asserts channelID's charset, then opens (creating if
// necessary) <workspaceRoot>/resources/<channelID>/{live,staging} as one
// os.Root-confined tree — the per-channel resource root §4.2 defines,
// SIBLING to (never nested under) <workspaceRoot>/<channelID>'s device
// workspace tree.
func openChannelRoot(workspaceRoot, channelID string) (*channelRoot, error) {
	if err := assertPathSegment("channelID", channelID); err != nil {
		return nil, err
	}
	base := filepath.Join(workspaceRoot, "resources", channelID)
	if err := os.MkdirAll(filepath.Join(base, liveDir), 0o700); err != nil {
		return nil, fmt.Errorf("storagehost: mkdir live root for channel %q: %w", channelID, err)
	}
	if err := os.MkdirAll(filepath.Join(base, stagingDir), 0o700); err != nil {
		return nil, fmt.Errorf("storagehost: mkdir staging root for channel %q: %w", channelID, err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return nil, fmt.Errorf("storagehost: open channel resource root %q: %w", base, err)
	}
	return &channelRoot{root: root}, nil
}

// livePath / stagingPath assert coord's charset then return the Root-relative
// path (NOT a joined absolute path — every caller passes this straight to an
// os.Root method, which is what actually enforces the confinement; this
// package never does its own Clean+HasPrefix containment check, per C2's
// explicit prohibition).
func livePath(coord string) (string, error) {
	if err := assertPathSegment("coord", coord); err != nil {
		return "", err
	}
	return filepath.Join(liveDir, coord), nil
}

// stagingPath builds one staging entry's Root-relative path from a coord and
// a caller-supplied unique suffix (§3.5's "staging 临时名每写唯一" —
// mktemp-style, so two concurrent writes for the same coord never collide or
// interleave). The suffix itself is not charset-asserted here (callers mint
// it via a uuid, always safe) but the join still goes through coord's own
// assert.
func stagingPath(coord, suffix string) (string, error) {
	if err := assertPathSegment("coord", coord); err != nil {
		return "", err
	}
	if err := assertPathSegment("staging suffix", suffix); err != nil {
		return "", err
	}
	return filepath.Join(stagingDir, coord+"-"+suffix), nil
}

func (c *channelRoot) Close() error { return c.root.Close() }

// fsyncDir fsyncs the directory at c's root-relative relDir (liveDir or
// stagingDir — this package's only two directory entries ever mutated, both
// flat: every coord is a DIRECT child, never nested, root.go's own doc) —
// 期11 S3's parent-directory durability (transfer-lifecycle-spec.md §3's #7):
// a file's own fsync (streamer.go's Commit) durably persists its BYTES, but
// the directory ENTRY that makes a rename/mkdir/touch visible again after a
// crash is a SEPARATE piece of metadata the containing directory itself
// owns — POSIX's classic "fsync the parent too" rule. Without this, a crash
// between rename()/Mkdir()/Create() returning and the directory's own
// eventual writeback can resurrect the OLD (pre-rename/nonexistent) state on
// reboot even though the file's own bytes were already durable.
func fsyncDir(c *channelRoot, relDir string) error {
	f, err := c.root.Open(relDir)
	if err != nil {
		return fmt.Errorf("storagehost: fsync dir open %q: %w", relDir, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("storagehost: fsync dir %q: %w", relDir, err)
	}
	return nil
}
