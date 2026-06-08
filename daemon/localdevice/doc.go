// Package localdevice is the local device transit on an attached compute: it
// bridges a relay adapter cell to a local external device (e.g. an xhs browser
// or a kimi local worker). The cell-to-device half is the Forward seam; the
// device-to-cell half is Callback, which routes the device's frame back to the
// owning cell via the host's Deliver path.
//
// Depends on kernel + runtime/actorrt. MUST NOT import server.
package localdevice
