package app

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// ProcessDB owns the production app database together with both process
// exclusion locks. Close shuts the SQL pool first and releases locks last.
type ProcessDB struct {
	DB      *sql.DB
	sidecar *os.File
	inode   *os.File
	once    sync.Once
	err     error
}

func (p *ProcessDB) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		if p.DB != nil {
			p.err = p.DB.Close()
		}
		for _, f := range []*os.File{p.inode, p.sidecar} {
			if f == nil {
				continue
			}
			p.err = errors.Join(p.err, syscall.Flock(int(f.Fd()), syscall.LOCK_UN), f.Close())
		}
	})
	return p.err
}

// OpenProcessDB is the stop-the-world production opener. init=true creates a
// brand-new database and refuses an existing path; init=false upgrades an
// existing database and refuses a missing path. Exclusion is acquired before
// OpenDB can execute any DDL.
func OpenProcessDB(path string, init bool) (_ *ProcessDB, retErr error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	var canonical string
	if init {
		if _, err := os.Lstat(abs); err == nil {
			return nil, fmt.Errorf("app: --init refuses existing database %s", abs)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
		if err != nil {
			return nil, fmt.Errorf("app: resolve database parent: %w", err)
		}
		canonical = filepath.Join(parent, filepath.Base(abs))
	} else {
		if _, err := os.Stat(abs); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("app: database does not exist (use --init for a new install): %s", abs)
			}
			return nil, err
		}
		canonical, err = filepath.EvalSymlinks(abs)
		if err != nil {
			return nil, fmt.Errorf("app: resolve database: %w", err)
		}
	}

	p := &ProcessDB{}
	defer func() {
		if retErr != nil {
			_ = p.Close()
		}
	}()
	p.sidecar, err = os.OpenFile(canonical+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(p.sidecar.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("app: database already locked: %w", err)
	}
	if init {
		f, err := os.OpenFile(canonical, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		_ = f.Close()
	}
	p.inode, err = os.OpenFile(canonical, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(p.inode.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("app: database inode already locked: %w", err)
	}
	p.DB, err = OpenDB(canonical)
	if err != nil {
		return nil, err
	}
	return p, nil
}
