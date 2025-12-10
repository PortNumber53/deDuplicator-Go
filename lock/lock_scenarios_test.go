package lock

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLockPreventsConcurrentAcquire(t *testing.T) {
	base := os.Getenv("WORKSPACE")
	if base == "" {
		base = t.TempDir()
	} else {
		base = filepath.Join(base, "tmp-lock-test")
		if err := os.MkdirAll(base, 0755); err != nil {
			t.Fatalf("create workspace tmp dir: %v", err)
		}
	}
	tDir, err := os.MkdirTemp(base, "locks-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	newLock := func(name string) *Lock {
		return &Lock{
			path: filepath.Join(tDir, name+".lock"),
			flow: name,
		}
	}

	l := newLock("migrate-test")
	if err := l.Acquire(); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	defer l.Release()

	other := newLock("migrate-test")
	if err := other.Acquire(); err == nil {
		t.Fatalf("expected second acquire to fail while lock is held")
	}
}
