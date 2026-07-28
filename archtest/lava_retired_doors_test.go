package archtest

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestLavaRetiredDoorsAbsentFromProduction is the wall in front of the 03B
// lava-flow anchor removals: each entry below was a production-dead door that
// only tests kept alive. The doors are deleted; production must never regrow
// them. Test files are skipped — serveLedger's occupancy probe legitimately
// lives on as an in-package test helper.
func TestLavaRetiredDoorsAbsentFromProduction(t *testing.T) {
	retired := []string{
		"func Bootstrap" + "Owner(", // platform/home bridge; use Config.BootstrapOwnerPrincipal
		"admit" + "ChannelOwner",    // its Home method, died with the bridge
		"Ref" + "Correlation(",      // protocol/channel derivation, production caller removed
		"Message" + "Correlation(",  // protocol/channel derivation, production caller removed
		"serveLedger) " + "len()",   // occupancy probe is test-only now (serve_test.go)
	}
	for _, root := range []string{"../app", "../cmd", "../drivers", "../lib", "../platform", "../protocol", "../runtime"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := readFile(path)
			if err != nil {
				return err
			}
			for _, symbol := range retired {
				if strings.Contains(string(b), symbol) {
					t.Errorf("%s contains retired door %q", filepath.ToSlash(path), symbol)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLavaRetiredDoorGuardFixtures(t *testing.T) {
	retired := []struct {
		pattern string
		fixture string
	}{
		{"func Bootstrap" + "Owner(", "func BootstrapOwner(ctx context.Context) {}"},
		{"admit" + "ChannelOwner", "h.admitChannelOwner(ctx, principal)"},
		{"Ref" + "Correlation(", "channel.RefCorrelation(ref)"},
		{"Message" + "Correlation(", "channel.MessageCorrelation(id)"},
		{"serveLedger) " + "len()", "func (l *serveLedger) len() int { return 0 }"},
	}
	for _, tc := range retired {
		if !strings.Contains(tc.fixture, tc.pattern) {
			t.Errorf("guard pattern %q does not trip its fixture %q", tc.pattern, tc.fixture)
		}
	}
}
