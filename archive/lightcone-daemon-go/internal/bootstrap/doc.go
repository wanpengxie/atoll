// Package bootstrap implements the channel-create 9-step saga + reconcile
// loop (L2 §1.4.7) for the daemon-go protocol baseline.
//
// Entry points:
//
//	Saga.ChannelCreate(ctx, params)
//	    Drive the 9-step bootstrap saga. Idempotent on create_request_id:
//	    re-sending the same id after success returns the existing
//	    channel_id; re-sending while in_progress returns
//	    ErrBootstrapInProgress; re-sending after rolled_back returns
//	    ErrBootstrapRolledBack so the caller switches id.
//
//	Saga.Reconcile(ctx)
//	    Scan bootstrap_registry WHERE status='in_progress' on daemon
//	    startup; verify workdir + channel sqlite integrity; on integrity
//	    failure rollback (delete workdir + UPDATE status='rolled_back');
//	    on success retry step 8a (INSERT OR IGNORE channel_created event)
//	    + step 8b (UPDATE status='completed').
//
//	Saga.ListChannels(ctx)
//	    Return every status='completed' channel row; used by the server
//	    reconcile API (daemon:list_channels).
//
//	NewHandler(saga)
//	    HTTP wrapper for `POST /api/channel/create` (L2 §3.6.1
//	    daemon_rpc binding error mapping).
//
// The 9 steps follow the L2 §1.4.7 table verbatim. Steps 3-8a run inside
// a single channel-local IMMEDIATE transaction so that any failure
// produces a clean compensation path (ROLLBACK + os.RemoveAll(workdir)
// + UPDATE bootstrap_registry status='rolled_back').
package bootstrap
