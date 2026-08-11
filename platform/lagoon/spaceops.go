package lagoon

import (
	"context"
	"encoding/json"
)

type spaceOpsBinder struct{ submitter Submitter }
type boundSpaceOps struct {
	submitter Submitter
	in        SubmitIn
}

func NewSpaceOps(submitter Submitter) SpaceOpsBinder { return spaceOpsBinder{submitter: submitter} }
func (b spaceOpsBinder) Bind(in SubmitIn) (SpaceOps, SpaceQueries) {
	ops := &boundSpaceOps{submitter: b.submitter, in: in}
	return ops, ops
}

func (o *boundSpaceOps) call(ctx context.Context, word Word, payload any, out any) error {
	o.in.Word = word
	o.in.Payload = payload
	reply, err := o.submitter.Submit(ctx, o.in)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(reply.Value)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
func (o *boundSpaceOps) CreateChannel(ctx context.Context, p ChannelCreate) (v ChannelRow, e error) {
	e = o.call(ctx, WordChannelCreate, p, &v)
	return
}
func (o *boundSpaceOps) RetireChannel(ctx context.Context, p ChannelRetire) (v ChannelRow, e error) {
	e = o.call(ctx, WordChannelRetire, p, &v)
	return
}
func (o *boundSpaceOps) RetirePrincipal(ctx context.Context, p PrincipalRetire) (v PrincipalRow, e error) {
	e = o.call(ctx, WordPrincipalRetire, p, &v)
	return
}
func (o *boundSpaceOps) SetCredential(ctx context.Context, p CredentialSet) (v CredentialReply, e error) {
	e = o.call(ctx, WordCredentialSet, p, &v)
	return
}
func (o *boundSpaceOps) RegisterDecl(ctx context.Context, p DeclRegister) (v DeclRow, e error) {
	e = o.call(ctx, WordDeclRegister, p, &v)
	return
}
func (o *boundSpaceOps) EditDecl(ctx context.Context, p DeclEdit) (v DeclRow, e error) {
	e = o.call(ctx, WordDeclEdit, p, &v)
	return
}
func (o *boundSpaceOps) RevokeDecl(ctx context.Context, p DeclRevoke) (v DeclRow, e error) {
	e = o.call(ctx, WordDeclRevoke, p, &v)
	return
}
func (o *boundSpaceOps) SetOverlay(ctx context.Context, p OverlaySet) (v OverlayRow, e error) {
	e = o.call(ctx, WordOverlaySet, p, &v)
	return
}
func (o *boundSpaceOps) ClearOverlay(ctx context.Context, p OverlayClear) (v Confirmation, e error) {
	e = o.call(ctx, WordOverlayClear, p, &v)
	return
}
func (o *boundSpaceOps) MintDevice(ctx context.Context, p DeviceMint) (v DeviceRow, e error) {
	e = o.call(ctx, WordDeviceMint, p, &v)
	return
}
func (o *boundSpaceOps) ClaimDevice(ctx context.Context, p DeviceClaim) (v DeviceRow, e error) {
	e = o.call(ctx, WordDeviceClaim, p, &v)
	return
}
func (o *boundSpaceOps) RetireDevice(ctx context.Context, p DeviceRetire) (v DeviceRow, e error) {
	e = o.call(ctx, WordDeviceRetire, p, &v)
	return
}
func (o *boundSpaceOps) AttachDevice(ctx context.Context, p DeviceBinding) (v BindingRow, e error) {
	e = o.call(ctx, WordDeviceAttach, p, &v)
	return
}
func (o *boundSpaceOps) DetachDevice(ctx context.Context, p DeviceBinding) (v Confirmation, e error) {
	e = o.call(ctx, WordDeviceDetach, p, &v)
	return
}
func (o *boundSpaceOps) ListChannels(ctx context.Context, p ChannelList) (v []ChannelRow, e error) {
	e = o.call(ctx, WordChannelList, p, &v)
	return
}
func (o *boundSpaceOps) GetChannel(ctx context.Context, p ChannelGet) (v ChannelRow, e error) {
	e = o.call(ctx, WordChannelGet, p, &v)
	return
}
func (o *boundSpaceOps) ListCandidates(ctx context.Context, p ChannelCandidates) (v []PrincipalRow, e error) {
	e = o.call(ctx, WordChannelCandidates, p, &v)
	return
}
func (o *boundSpaceOps) ListDecls(ctx context.Context) (v []DeclRow, e error) {
	e = o.call(ctx, WordDeclList, struct{}{}, &v)
	return
}
func (o *boundSpaceOps) ListDevices(ctx context.Context) (v []DeviceRow, e error) {
	e = o.call(ctx, WordDeviceList, struct{}{}, &v)
	return
}
func (o *boundSpaceOps) Me(ctx context.Context) (v PrincipalRow, e error) {
	e = o.call(ctx, WordPrincipalMe, struct{}{}, &v)
	return
}

var _ SpaceOps = (*boundSpaceOps)(nil)
var _ SpaceQueries = (*boundSpaceOps)(nil)
