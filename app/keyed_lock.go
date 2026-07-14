package app

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/channel"
)

type keyedLockSet struct {
	mu sync.Mutex
	m  map[string]*keyedLockEntry
}

type keyedLockEntry struct {
	mu   sync.Mutex
	refs int
}

func newKeyedLockSet() *keyedLockSet { return &keyedLockSet{m: map[string]*keyedLockEntry{}} }

func (s *keyedLockSet) lock(key string) func() {
	s.mu.Lock()
	e := s.m[key]
	if e == nil {
		e = &keyedLockEntry{}
		s.m[key] = e
	}
	e.refs++
	s.mu.Unlock()
	e.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Unlock()
			s.mu.Lock()
			e.refs--
			if e.refs == 0 && s.m[key] == e {
				delete(s.m, key)
			}
			s.mu.Unlock()
		})
	}
}

type appDaemonAuthority struct{ app *App }

func (a appDaemonAuthority) LockAndValidate(ctx context.Context, daemonID string, chID channel.ID) (func(), error) {
	if a.app == nil || a.app.daemonLocks == nil {
		return nil, errors.New("app: daemon authority unavailable")
	}
	release := a.app.daemonLocks.lock(daemonID)
	var n int
	err := a.app.db.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM daemons d JOIN daemon_channels dc ON dc.daemon_id=d.id
		WHERE d.id=? AND dc.channel_id=?`, daemonID, string(chID)).Scan(&n)
	if err != nil || n != 1 {
		release()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("app: daemon or channel binding no longer exists")
	}
	return release, nil
}
