package transit

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/framework/devicetransit"
	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
)

// DeviceTransit is the daemon-side implementation of
// framework/devicetransit.DeviceTransit — the runtime_inbound_via_relay binding's
// transport.
//
// Direction map (impl-layer2 §5.3):
//
//   - DeviceTransit.Send packages an adapter→device payload into a
//     daemonbus `device_transit.recv` frame (daemon → server →
//     device, §5.3.2 outbound).
//   - Inbound `device_transit.send` frames (device → server →
//     daemon, §5.3.1 inbound) are routed by the daemon's frame
//     dispatcher to OnRecv, which fans out to the adapter Manager
//     via the OnRecvFn callback (wired in cmd/daemon).
type DeviceTransit struct {
	client   *Client
	frameID  FrameIDGen
	onRecvFn func(ctx context.Context, frame devicetransit.SendFrame) error
	onAckFn  func(ctx context.Context, frame devicetransit.AckFrame) error
}

// DeviceTransitConfig wires a DeviceTransit.
type DeviceTransitConfig struct {
	Client  *Client
	FrameID FrameIDGen
	OnRecv  func(ctx context.Context, frame devicetransit.SendFrame) error
	OnAck   func(ctx context.Context, frame devicetransit.AckFrame) error
}

// NewDeviceTransit builds a DeviceTransit.
func NewDeviceTransit(cfg DeviceTransitConfig) (*DeviceTransit, error) {
	if cfg.Client == nil {
		return nil, errors.New("transit: DeviceTransitConfig.Client nil")
	}
	if cfg.FrameID == nil {
		return nil, errors.New("transit: DeviceTransitConfig.FrameID nil")
	}
	return &DeviceTransit{
		client:   cfg.Client,
		frameID:  cfg.FrameID,
		onRecvFn: cfg.OnRecv,
		onAckFn:  cfg.OnAck,
	}, nil
}

// Send implements devicetransit.DeviceTransit — packages an
// adapter→device SendFrame into a `device_transit.recv` daemonbus
// frame (impl-layer2 §5.3.2 outbound: daemon → server → device). The
// returned frame_id is the one the underlying daemonbus frame carries
// (also what ack/error frames reference).
func (d *DeviceTransit) Send(ctx context.Context, frame devicetransit.SendFrame) (devicetransit.FrameID, error) {
	fid := d.frameID()
	if err := d.client.Send(ctx, fid, daemonbus.FrameTypeDeviceTransitRecv, frame); err != nil {
		return "", err
	}
	return devicetransit.FrameID(fid), nil
}

// Ack implements devicetransit.DeviceTransit (daemon-side outgoing ack, e.g.
// daemon ACKs a device_transit.send it processed successfully).
func (d *DeviceTransit) Ack(ctx context.Context, frame devicetransit.AckFrame) error {
	return d.client.Send(ctx, d.frameID(), daemonbus.FrameTypeDeviceTransitAck, frame)
}

// DispatchIncoming routes a device_transit.* frame to the configured
// callbacks. Called by the daemon's main frame loop. Per impl-layer2
// §5.3.1, only `device_transit.send` arrives at the daemon (device →
// adapter inbound direction); ack frames remain bidirectional.
func (d *DeviceTransit) DispatchIncoming(ctx context.Context, frame daemonbus.Frame) error {
	switch frame.FrameKind {
	case daemonbus.FrameTypeDeviceTransitSend:
		var payload devicetransit.SendFrame
		if err := DecodePayload(frame, &payload); err != nil {
			return fmt.Errorf("transit: decode device_transit.send: %w", err)
		}
		ack := devicetransit.AckFrame{
			CorrelationFrameID: devicetransit.FrameID(frame.FrameID),
			Result:             devicetransit.AckAccepted,
		}
		if d.onRecvFn == nil {
			ack.Result = devicetransit.AckRejectedRetryable
			ack.Reason = "receiver_unavailable"
			ack.Detail = "device transit receiver is not wired"
			return d.Ack(ctx, ack)
		}
		if err := d.onRecvFn(ctx, payload); err != nil {
			var ackErr *devicetransit.AckError
			if errors.As(err, &ackErr) {
				ack.Result = ackErr.Result
				if ack.Result == "" {
					ack.Result = devicetransit.AckRejectedRetryable
				}
				ack.Reason = ackErr.Reason
				ack.Detail = ackErr.Detail
			} else {
				ack.Result = devicetransit.AckRejectedRetryable
				ack.Reason = "receiver_error"
				ack.Detail = err.Error()
			}
			return d.Ack(ctx, ack)
		}
		return d.Ack(ctx, ack)
	case daemonbus.FrameTypeDeviceTransitAck:
		if d.onAckFn == nil {
			return nil
		}
		var payload devicetransit.AckFrame
		if err := DecodePayload(frame, &payload); err != nil {
			return fmt.Errorf("transit: decode device_transit.ack: %w", err)
		}
		return d.onAckFn(ctx, payload)
	}
	return fmt.Errorf("transit: DeviceTransit got non-device frame: %s", frame.FrameKind)
}
