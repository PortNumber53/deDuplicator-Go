package lock

import "testing"

func TestLockPreventsConcurrentAcquire(t *testing.T) {
	lockPath := "migrate-test"
	l := New(lockPath)
	if err := l.Acquire(); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	defer l.Release()

	other := New(lockPath)
	if err := other.Acquire(); err == nil {
		t.Fatalf("expected second acquire to fail while lock is held")
	}
}
