package accessdoor

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ErrNoStoragePlacement is placement chain ④'s zero-candidate honest reject
// (期11 spec §4.3/§0: "0台在线...门明确报错，K8s unschedulable同款") — a Go
// error (not an access verdict): the channel simply has no physical
// substrate to provision file bytes on right now, the same class of failure
// as an assembly defect, never a caller-authorization decision.
var ErrNoStoragePlacement = errors.New("accessdoor: no online storage daemon available for this channel")

// ErrAmbiguousStoragePlacement is chain ④'s >1-candidate honest reject
// (§4.3: ">1台且无一属创建者" — "placement 不明确"). Distinguishable from
// ErrNoStoragePlacement (0 vs >1) because the two failure MODES differ
// operationally even though both are "cannot place": zero is a genuine
// capacity gap, ambiguity is a policy gap (② — owner-level affinity — would
// resolve it, day-1 it is deferred, §4.3).
var ErrAmbiguousStoragePlacement = errors.New("accessdoor: ambiguous storage placement (multiple online daemons, none is the creator's host)")

// choosePlacement runs the file-kind placement policy chain (期11 spec
// §4.3's "policy 链，coral 正推"): creator affinity at HOST level (①), then
// (② owner-level affinity — deferred whole to the human-inbound debt this
// data flow shares, §4.3's own words: "此数据流与human接线同源，day-1不实现")
// skipped entirely, then the unique-online-candidate fallback (③), then an
// honest reject (④). It NEVER falls back to server placement and NEVER
// silently picks among multiple candidates — both are named red lines
// (§4.3: "绝不回落server、绝不隐式挑盘").
func (d *door) choosePlacement(ctx context.Context, caller actor.ActorID) (string, error) {
	if d.deps.StorageMounts == nil {
		return "", fmt.Errorf("accessdoor: file kind placement routing not wired (Deps.StorageMounts is nil)")
	}

	mounts, err := d.deps.StorageMounts.ListStorageDaemons(ctx, d.deps.ChannelID)
	if err != nil {
		return "", err
	}

	// ① creator affinity · host level: the creator itself is daemon-hosted, and
	// that SAME daemon is a live (online) storage mount for this channel — the
	// creator's own workspace is the natural place for its own file bytes to
	// land (§4.3's "创建者daemon-hosted→落其宿主daemon").
	row, found, err := d.deps.Authority.LookupActive(ctx, caller)
	if err != nil {
		return "", err
	}
	host := ""
	if found && row.Placement.Kind == storespec.PlacementDaemon {
		host = row.Placement.Host
	}
	if found && host != "" {
		for _, m := range mounts {
			if m.DaemonID == host && m.Online {
				return host, nil
			}
		}
	}

	// ② owner-level creator affinity: deferred whole (§4.3) — skipped.

	// ③ unique online candidate: creator has no usable affinity above, so if
	// exactly one online storage daemon serves this channel, use it.
	var online []StorageMount
	for _, m := range mounts {
		if m.Online {
			online = append(online, m)
		}
	}
	if len(online) == 1 {
		return online[0].DaemonID, nil
	}

	// ④ honest reject: zero candidates is a capacity gap; more than one with no
	// resolvable affinity is a policy gap (would-be ②'s job).
	if len(online) == 0 {
		return "", fmt.Errorf("%w: channel %q", ErrNoStoragePlacement, d.deps.ChannelID)
	}
	return "", fmt.Errorf("%w: channel %q has %d online storage daemons", ErrAmbiguousStoragePlacement, d.deps.ChannelID, len(online))
}
