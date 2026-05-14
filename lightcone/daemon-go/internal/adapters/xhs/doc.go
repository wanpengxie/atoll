// Package xhs hosts the M1.3-T14 daemon-side adapter for the
// xhs-creator template. It plugs into the daemon-go adapter framework
// (pkg/adapter, T13) and provides the protocol translation between v4
// request envelopes (kind=request, type=xhs.*) and the existing M1.2
// Chrome extension WebSocket protocol (frame: `{type:'command',
// correlation_id, cmd, params}` plus HTTP callback at
// /api/device/{deviceId}/callback).
//
// Type declarations (L4 §2.1):
//
//   - xhs.publish        request/response  (note publish)
//   - xhs.search         request/response  (keyword search)
//   - xhs.note.fetch     request/response  (single note fetch)
//   - xhs.recent.fetch   request/response  (recent published list)
//   - xhs.cookie.sync    request/response  (cookie refresh)
//   - xhs.note.archived  event              (agent-emitted; no daemon-side
//     translation — declared so the
//     adapter actor owns the type)
//
// Lifecycle wiring (production):
//
//	mgr, _ := adapter.NewManager(adapter.ManagerConfig{
//	    DB:      channelDB,
//	    Deps:    harnessDeps,
//	    Modules: map[string]adapter.Module{"xhs": xhs.New(xhs.Config{
//	        DeviceClient: realWS,        // pkg/devices.Server-backed impl
//	        DeviceID:     defaultDevice, // overridable via payload.device_id
//	    })},
//	})
//	_ = mgr.Install(ctx)
//	_ = mgr.BootRecoverTimers(ctx)
//	go mgr.RunGC(ctx)
//
// Callback wiring: see callback_http.go — HandleCallback materialises
// the framework `Manager.OnExternalCallback("xhs", body)` entry from a
// `POST /api/device/{deviceId}/callback` request.
//
// Tests inject `MockDeviceClient` (device_client.go) so they exercise
// the full request → push → callback → respond cycle without needing
// a real Chrome extension or daemon ws server.
package xhs
