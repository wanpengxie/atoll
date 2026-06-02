// Package localdevice is the local device host on an attached compute — it
// collapses the v1 two-hop device relay into the compute's own process. It runs
// the local transport (xhs browser ws listen / kimi 127.0.0.1 HTTP) and feeds
// device frames into the hosting adapterActor cell via the host's Ask seam, and
// provides the transport-backed behavior.ExternalRequestFunc injected into
// relay adapters (proxyfacade) so ctx.ForwardExternalRequest reaches the device.
//
// Port from: adapters/proxy/daemon/local_xhs.go + adapters/device/xhs edge +
// framework/devicetransit (the v1 relay, collapsed to one local hop).
//
// Depends on runtime + lib + wire + adapters (concrete device modules via cmd).
package localdevice
