// Package spacetool implements the per-channel syscall actor. It owns no
// judgement: every accepted word is decoded into the lagoon contract and sent
// through the c0 corridor.
package spacetool

import (
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/message"
)

func Def(binder lagoon.SpaceOpsBinder) actorbase.Def {
	return actorbase.Def{Doc: "channel-zero registry operations", New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error { return serve(sys, binder) }, nil
	}}
}

func serve(sys actorbase.Sys, binder lagoon.SpaceOpsBinder) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return nil
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		handle(sys, binder, msg)
	}
}

func decode(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &lagoon.Error{Code: lagoon.CodeInvalidArgs, Detail: "invalid JSON payload"}
	}
	return nil
}

func fail(sys actorbase.Sys, msg actorbase.Msg, err error) {
	var le *lagoon.Error
	if errors.As(err, &le) {
		_, _ = sys.Fail(msg, string(le.Code), le.Detail)
		return
	}
	_, _ = sys.Fail(msg, "internal_error", err.Error())
}

func handle(sys actorbase.Sys, binder lagoon.SpaceOpsBinder, msg actorbase.Msg) {
	if binder == nil {
		fail(sys, msg, errors.New("space-tool unavailable"))
		return
	}
	ops, queries := binder.Bind(lagoon.SubmitIn{Source: msg.ChannelID, Sender: msg.Sender.ID, RequestID: string(msg.ID)})
	var value any
	var err error
	switch lagoon.Word(msg.Type) {
	case lagoon.WordChannelCreate:
		var p lagoon.ChannelCreate
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.CreateChannel(msg.Ctx(), p)
		}
	case lagoon.WordChannelRetire:
		var p lagoon.ChannelRetire
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.RetireChannel(msg.Ctx(), p)
		}
	case lagoon.WordPrincipalRetire:
		var p lagoon.PrincipalRetire
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.RetirePrincipal(msg.Ctx(), p)
		}
	case lagoon.WordCredentialSet:
		var p lagoon.CredentialSet
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.SetCredential(msg.Ctx(), p)
		}
	case lagoon.WordDeclRegister:
		var p lagoon.DeclRegister
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.RegisterDecl(msg.Ctx(), p)
		}
	case lagoon.WordDeclEdit:
		var p lagoon.DeclEdit
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.EditDecl(msg.Ctx(), p)
		}
	case lagoon.WordDeclRevoke:
		var p lagoon.DeclRevoke
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.RevokeDecl(msg.Ctx(), p)
		}
	case lagoon.WordOverlaySet:
		var p lagoon.OverlaySet
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.SetOverlay(msg.Ctx(), p)
		}
	case lagoon.WordOverlayClear:
		var p lagoon.OverlayClear
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.ClearOverlay(msg.Ctx(), p)
		}
	case lagoon.WordDeviceMint:
		var p lagoon.DeviceMint
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.MintDevice(msg.Ctx(), p)
		}
	case lagoon.WordDeviceClaim:
		var p lagoon.DeviceClaim
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.ClaimDevice(msg.Ctx(), p)
		}
	case lagoon.WordDeviceRetire:
		var p lagoon.DeviceRetire
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.RetireDevice(msg.Ctx(), p)
		}
	case lagoon.WordDeviceAttach:
		var p lagoon.DeviceBinding
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.AttachDevice(msg.Ctx(), p)
		}
	case lagoon.WordDeviceDetach:
		var p lagoon.DeviceBinding
		if err = decode(msg.Payload, &p); err == nil {
			value, err = ops.DetachDevice(msg.Ctx(), p)
		}
	case lagoon.WordChannelList:
		var p lagoon.ChannelList
		if err = decode(msg.Payload, &p); err == nil {
			value, err = queries.ListChannels(msg.Ctx(), p)
		}
	case lagoon.WordChannelGet:
		var p lagoon.ChannelGet
		if err = decode(msg.Payload, &p); err == nil {
			value, err = queries.GetChannel(msg.Ctx(), p)
		}
	case lagoon.WordChannelCandidates:
		var p lagoon.ChannelCandidates
		if err = decode(msg.Payload, &p); err == nil {
			value, err = queries.ListCandidates(msg.Ctx(), p)
		}
	case lagoon.WordDeclList:
		value, err = queries.ListDecls(msg.Ctx())
	case lagoon.WordDeviceList:
		value, err = queries.ListDevices(msg.Ctx())
	case lagoon.WordPrincipalMe:
		value, err = queries.Me(msg.Ctx())
	default:
		err = &lagoon.Error{Code: lagoon.CodeInvalidArgs, Detail: "unsupported space operation"}
	}
	if err != nil {
		fail(sys, msg, err)
		return
	}
	_, _ = sys.Reply(msg, value)
}
