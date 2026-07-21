package store_test

import "testing"

// TestChannelStoresCloseIsIdempotent pins the two-phase close contract: after
// a successful Close, further Close calls are nil no-ops. Previously the
// second call re-ran the WAL checkpoint against the already-dead handle and
// failed with "database is closed", which broke every retrying caller
// upstream (Home's post-teardown store-close retry and ChannelHost.Close's
// drain could never converge).
func TestChannelStoresCloseIsIdempotent(t *testing.T) {
	cs := openTestChannel(t)
	if err := cs.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("repeat Close = %v, want nil", err)
	}
}
