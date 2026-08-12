package store

import (
	"database/sql"
	"testing"

	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
)

type principalScanner struct{ kind string }

func (s principalScanner) Scan(dest ...any) error {
	*dest[0].(*string) = "principal"
	*dest[1].(*string) = s.kind
	*dest[2].(*sql.NullString) = sql.NullString{}
	*dest[3].(*sql.NullString) = sql.NullString{}
	*dest[4].(*regspec.PrincipalStatus) = regspec.PrincipalPresent
	*dest[5].(*int64) = 1
	return nil
}

func TestScanPrincipalAcceptsOnlyHumanAndAgentKinds(t *testing.T) {
	for _, kind := range []actor.Kind{actor.KindHuman, actor.KindAgent} {
		row, err := scanPrincipal(principalScanner{kind: string(kind)})
		if err != nil || row.Kind != kind {
			t.Fatalf("kind %q scanned as (%q,%v)", kind, row.Kind, err)
		}
	}
	for _, kind := range []actor.Kind{actor.KindTool, actor.KindSystem} {
		if _, err := scanPrincipal(principalScanner{kind: string(kind)}); err == nil {
			t.Fatalf("principal kind %q was accepted", kind)
		}
	}
}
