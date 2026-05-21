package transit

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
)

// DeviceTransit is the daemon-side implementation of
// kernel/devicetransit.DeviceTransit — the runtime_inbound_via_relay binding's
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
	client    *Client
	frameID   FrameIDGen
	onRecvFn  func(ctx context.Context, frame devicetransit.SendFrame) error
	onAckFn   func(ctx context.Context, frame devicetransit.AckFrame) error
	onErrorFn func(ctx context.Context, frame devicetransit.ErrorFrame) error
}

// DeviceTransitConfig wires a DeviceTransit.
type DeviceTransitConfig struct {
	Client  *Client
	FrameID FrameIDGen
	OnRecv  func(ctx context.Context, frame devicetransit.SendFrame) error
	OnAck   func(ctx context.Context, frame devicetransit.AckFrame) error
	OnError func(ctx context.Context, frame devicetransit.ErrorFrame) error
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
		client:    cfg.Client,
		frameID:   cfg.FrameID,
		onRecvFn:  cfg.OnRecv,
		onAckFn:   cfg.OnAck,
		onErrorFn: cfg.OnError,
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

// Error implements devicetransit.DeviceTransit.
func (d *DeviceTransit) Error(ctx context.Context, frame devicetransit.ErrorFrame) error {
	return d.client.Send(ctx, d.frameID(), daemonbus.FrameTypeDeviceTransitError, frame)
}

// DispatchIncoming routes a device_transit.* frame to the configured
// callbacks. Called by the daemon's main frame loop. Per impl-layer2
// §5.3.1, only `device_transit.send` arrives at the daemon (device →
// adapter inbound direction); ack / error frames remain bidirectional.
func (d *DeviceTransit) DispatchIncoming(ctx context.Context, frame daemonbus.Frame) error {
	switch frame.FrameKind {
	case daemonbus.FrameTypeDeviceTransitSend:
		if d.onRecvFn == nil {
			return nil
		}
		var payload devicetransit.SendFrame
		if err := DecodePayload(frame, &payload); err != nil {
			return fmt.Errorf("transit: decode device_transit.send: %w", err)
		}
		return d.onRecvFn(ctx, payload)
	case daemonbus.FrameTypeDeviceTransitAck:
		if d.onAckFn == nil {
			return nil
		}
		var payload devicetransit.AckFrame
		if err := DecodePayload(frame, &payload); err != nil {
			return fmt.Errorf("transit: decode device_transit.ack: %w", err)
		}
		return d.onAckFn(ctx, payload)
	case daemonbus.FrameTypeDeviceTransitError:
		if d.onErrorFn == nil {
			return nil
		}
		var payload devicetransit.ErrorFrame
		if err := DecodePayload(frame, &payload); err != nil {
			return fmt.Errorf("transit: decode device_transit.error: %w", err)
		}
		return d.onErrorFn(ctx, payload)
	}
	return fmt.Errorf("transit: DeviceTransit got non-device frame: %s", frame.FrameKind)
}
