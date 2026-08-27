package boot

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/registry"
)

// Nothing on this node keeps the password in plaintext — the credential is a
// bcrypt hash and the installer shows the value once and forgets it. That makes
// booting again with a new password the ONLY way back in, so it is pinned here.

func installFixture(t *testing.T, password string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "channels")
	res, err := Ensure(context.Background(), Config{
		ChannelDir:         dir,
		RootPassword:       password,
		ResolveClassConfig: registry.ResolveDefaultConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Installed {
		t.Fatal("fixture did not install")
	}
	return dir
}

func rootLoginWorks(t *testing.T, channelDir, password string) bool {
	t.Helper()
	db, err := openSQLite(filepath.Join(filepath.Dir(channelDir), registryDBName), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var hash string
	if err := db.QueryRowContext(context.Background(),
		`SELECT secret_hash FROM credentials WHERE principal_id=? AND kind='password'`,
		channelspec.RootPrincipalID).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if hash == password {
		t.Fatal("the credential is stored in plaintext")
	}
	return bcryptMatches(hash, password)
}

// Booting an existing install with a different password replaces it. This is
// the recovery path: forget the password, boot again with a new one, log in.
func TestBootingAgainWithANewPasswordReplacesIt(t *testing.T) {
	dir := installFixture(t, "first-password")

	res, err := Ensure(context.Background(), Config{
		ChannelDir:         dir,
		RootPassword:       "second-password",
		ResolveClassConfig: registry.ResolveDefaultConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Installed {
		t.Fatal("a second boot re-ran the install; it must only reseed the credential")
	}
	if !res.PasswordReseeded {
		t.Fatal("the password changed but the boot did not report it")
	}
	if !rootLoginWorks(t, dir, "second-password") {
		t.Fatal("the new password does not work")
	}
	if rootLoginWorks(t, dir, "first-password") {
		t.Fatal("the old password still works")
	}
}

// Re-booting with the SAME password must be a no-op, not a rotation. A value
// left sitting in the environment would otherwise rewrite the credential on
// every restart, and an event worth reporting would start happening constantly
// and mean nothing.
func TestBootingAgainWithTheSamePasswordChangesNothing(t *testing.T) {
	dir := installFixture(t, "same-password")

	res, err := Ensure(context.Background(), Config{
		ChannelDir:         dir,
		RootPassword:       "same-password",
		ResolveClassConfig: registry.ResolveDefaultConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.PasswordReseeded {
		t.Fatal("an unchanged password was reported as a rotation")
	}
	if !rootLoginWorks(t, dir, "same-password") {
		t.Fatal("the password stopped working")
	}
}

// The ordinary restart — no password given at all — must leave the credential
// exactly where it is rather than clearing it.
func TestBootingWithNoPasswordLeavesTheCredentialAlone(t *testing.T) {
	dir := installFixture(t, "kept-password")

	if _, err := Ensure(context.Background(), Config{
		ChannelDir:         dir,
		ResolveClassConfig: registry.ResolveDefaultConfig,
	}); err != nil {
		t.Fatal(err)
	}
	if !rootLoginWorks(t, dir, "kept-password") {
		t.Fatal("an ordinary restart lost the password")
	}
}

// A credential row that is gone — removed, or never written by an older install
// — is repaired by the same path rather than needing a different one.
func TestAMissingCredentialIsReseededRatherThanFatal(t *testing.T) {
	dir := installFixture(t, "doomed-password")

	db, err := openSQLite(filepath.Join(filepath.Dir(dir), registryDBName), true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(),
		`DELETE FROM credentials WHERE principal_id=?`, channelspec.RootPrincipalID); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	res, err := Ensure(context.Background(), Config{
		ChannelDir:         dir,
		RootPassword:       "recovered-password",
		ResolveClassConfig: registry.ResolveDefaultConfig,
	})
	if err != nil {
		t.Fatalf("a missing credential was fatal instead of repairable: %v", err)
	}
	if !res.PasswordReseeded {
		t.Fatal("the repair was not reported")
	}
	if !rootLoginWorks(t, dir, "recovered-password") {
		t.Fatal("the repaired password does not work")
	}
}
