package accessdoor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type door struct{ deps Deps }

func (d *door) fileAddress(id resource.ResourceID) (resourcespec.FileAddress, bool, error) {
	raw := string(id)
	if !strings.HasPrefix(raw, resourcespec.DaemonScheme+"://") {
		return resourcespec.FileAddress{}, false, nil
	}
	address, err := resourcespec.ParseFileAddress(raw)
	if err != nil {
		return resourcespec.FileAddress{}, true, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if d.deps.ChannelName == "" || address.Channel != d.deps.ChannelName {
		return resourcespec.FileAddress{}, true, fmt.Errorf("%w: file address names a different channel", ErrMalformed)
	}
	return address, true, nil
}

func (d *door) storageMount(ctx context.Context, host string) (StorageMount, error) {
	if d.deps.StorageMounts == nil {
		return StorageMount{}, errors.New("accessdoor: storage mounts unavailable")
	}
	mount, found, err := d.deps.StorageMounts.ResolveStorageDaemon(ctx, d.deps.ChannelID, host)
	if err != nil {
		return StorageMount{}, err
	}
	if !found {
		return StorageMount{}, fmt.Errorf("accessdoor: daemon %q is not bound to channel", host)
	}
	if !mount.Online {
		return StorageMount{}, NewHostOfflineError(host)
	}
	return mount, nil
}

// authorizeMember is the file plane's membership check: inside the membrane
// every member reads and writes alike, so being an active member IS the right
// (the same rule effectiveOps states for objects). It answers with the facts
// rather than one field because a transfer that will be finished on another
// connection also has to record whose transfer it is.
func (d *door) authorizeMember(ctx context.Context, caller actor.ActorID) (storespec.ResourceActorFacts, error) {
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return storespec.ResourceActorFacts{}, err
	}
	if !facts.Active {
		return storespec.ResourceActorFacts{}, ErrFileCapabilityUnavailable
	}
	return facts, nil
}

func (d *door) resolveFileRoute(ctx context.Context, caller actor.ActorID, id resource.ResourceID, mode access.Operation) (*FileRoute, error) {
	address, file, err := d.fileAddress(id)
	if err != nil {
		return nil, err
	}
	if !file {
		return nil, fmt.Errorf("%w: file address required", ErrMalformed)
	}
	facts, err := d.authorizeMember(ctx, caller)
	if err != nil {
		return nil, err
	}
	mount, err := d.storageMount(ctx, address.Host)
	if err != nil {
		return nil, err
	}
	if facts.PreferredStorageHost != "" && facts.PreferredStorageHost == mount.DaemonID {
		return &FileRoute{Path: address.Path, Mode: mode, Redeem: FileRedeemLocal}, nil
	}
	if d.deps.TransferControl == nil {
		return nil, errors.New("accessdoor: transfer control unavailable")
	}
	token, err := d.deps.TransferControl.IssueTransfer(ctx, TransferSpec{
		Address: id, HostID: mount.DaemonID, HostName: address.Host,
		Mode: mode, Caller: caller, Principal: facts.Principal,
	})
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, errors.New("accessdoor: empty transfer ticket")
	}
	return &FileRoute{Token: token, Mode: mode, Redeem: FileRedeemRemote}, nil
}

func (d *door) driver(kind resourcespec.ResourceKind) (resourcespec.Driver, error) {
	drv, ok := d.deps.Drivers[kind]
	if !ok {
		return nil, errors.New("accessdoor: no driver for kind " + string(kind))
	}
	return drv, nil
}

func (d *door) invoke(ctx context.Context, caller actor.ActorID, op access.Operation, id resource.ResourceID, args []byte) (Outcome, error) {
	address, file, addressErr := d.fileAddress(id)
	if addressErr != nil {
		return Outcome{}, addressErr
	}
	if file {
		if _, err := d.authorizeMember(ctx, caller); err != nil {
			return Outcome{RejectReason: access.AccessDenied}, nil
		}
		switch op {
		case access.OpRead, access.OpWrite:
			route, err := d.resolveFileRoute(ctx, caller, id, op)
			if err != nil {
				return Outcome{}, err
			}
			return Outcome{Route: route}, nil
		case access.OpDelete:
			if d.deps.Files == nil {
				return Outcome{}, errors.New("accessdoor: file control unavailable")
			}
			mount, err := d.storageMount(ctx, address.Host)
			if err != nil {
				return Outcome{}, err
			}
			if err := d.deps.Files.Delete(ctx, mount.DaemonID, address.Path); err != nil {
				return executeFailure(ctx, err)
			}
			return Outcome{}, nil
		}
	}

	meta, exists, err := d.deps.Registry.Resolve(ctx, id)
	if err != nil {
		return Outcome{}, err
	}
	if !exists {
		return Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return Outcome{}, err
	}
	if !effectiveOps(caller, facts.Active, facts.Owner, meta.CreatedBy)[op] {
		return Outcome{RejectReason: access.AccessDenied}, nil
	}
	drv, err := d.driver(meta.Kind)
	if err != nil {
		return Outcome{}, err
	}
	switch op {
	case access.OpRead:
		val, found, err := drv.Read(ctx, id)
		if err != nil {
			return executeFailure(ctx, err)
		}
		return Outcome{Value: val, Found: found}, nil
	case access.OpWrite:
		if err := drv.Write(ctx, id, args); err != nil {
			return executeFailure(ctx, err)
		}
		return Outcome{}, nil
	case access.OpDelete:
		if err := drv.Delete(ctx, id); err != nil {
			return executeFailure(ctx, err)
		}
		if err := d.deps.Registry.Delete(ctx, id); err != nil {
			return executeFailure(ctx, err)
		}
		return Outcome{}, nil
	default:
		return Outcome{}, ErrMalformed
	}
}
