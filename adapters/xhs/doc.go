// Package xhs is the M1.6-T2 in_process scaffold for the xhs adapter.
//
// Why a parallel package next to adapters/device/xhs?
//
//	adapters/device/xhs/  — full via_server_transit binding (T3) — Init
//	                        requires DeviceTransit which T2 cannot wire
//	                        because server/devicebus + Chrome extension
//	                        protocol upgrades are T3 scope.
//	adapters/xhs/         — T2 in_process scaffold — Handle synchronously
//	                        emits a success terminal via ctx.Respond so
//	                        acceptance #2 ("xhs.publish request → framework
//	                        handle() → mock 路径 emit response success")
//	                        can land without device-edge dependencies.
//
// The scaffold owns the same canonical actor id (tool:xhs-adapter) and
// declares the same six envelope types as the device adapter — the only
// difference is the actor.Binding, which T2 keeps as in_process so the
// daemon composition root can install it without DeviceTransit. T3
// replaces this scaffold by registering adapters/device/xhs as the bound
// module for tool:xhs-adapter instead.
package xhs
