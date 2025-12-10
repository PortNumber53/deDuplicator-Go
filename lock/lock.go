package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

const fallbackLockDir = "/tmp/deduplicator"

type Lock struct {
	path string
	flow string
	file *os.File
}

// New creates a new Lock instance
func New(flow string) *Lock {
	if flow == "" {
		panic("flow name cannot be empty")
	}
	path := filepath.Join(lockDir(), fmt.Sprintf("%s.lock", flow))
	return &Lock{
		path: path,
		flow: flow,
	}
}

// isProcessRunning checks if a process with the given PID is running
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	
	// On Unix systems, FindProcess always succeeds, so we need to send
	// signal 0 to check if the process actually exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// cleanStaleLock removes the lock file if the process is no longer running
func (l *Lock) cleanStaleLock() error {
	content, err := os.ReadFile(l.path)
	if err != nil {
		return err
	}

	pid, err := strconv.Atoi(string(content))
	if err != nil {
		// If we can't read the PID, the lock file is invalid
		return os.Remove(l.path)
	}

	if !isProcessRunning(pid) {
		return os.Remove(l.path)
	}

	return fmt.Errorf("process is still running (PID: %d)", pid)
}

// Acquire tries to acquire the lock
func (l *Lock) Acquire() error {
	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(l.path), 0755); err != nil {
		return fmt.Errorf("failed to create lock directory: %v", err)
	}

	// Try to create the lock file
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			// Lock file exists, check if it's stale
			if err := l.cleanStaleLock(); err != nil {
				return fmt.Errorf("another instance of %s flow is already running: %v", l.flow, err)
			}
			// Stale lock was cleaned, try to acquire again
			return l.Acquire()
		}
		return fmt.Errorf("failed to create lock file: %v", err)
	}

	// Write PID to lock file
	if _, err := fmt.Fprintf(f, "%d", os.Getpid()); err != nil {
		f.Close()
		os.Remove(l.path)
		return fmt.Errorf("failed to write PID to lock file: %v", err)
	}

	l.file = f
	return nil
}

// Release releases the lock
func (l *Lock) Release() error {
	if l.file != nil {
		l.file.Close()
		if err := os.Remove(l.path); err != nil {
			return fmt.Errorf("failed to remove lock file: %v", err)
		}
	}
	return nil
}

// MustAcquire creates a new Lock instance and acquires it, panicking on error
func MustAcquire(flow string) *Lock {
	l := New(flow)
	if err := l.Acquire(); err != nil {
		panic(fmt.Sprintf("failed to acquire %s lock: %v", flow, err))
	}
	return l
}

// lockDir resolves the lock directory, allowing override via DEDUPLICATOR_LOCK_DIR.
func lockDir() string {
	if custom := os.Getenv("DEDUPLICATOR_LOCK_DIR"); custom != "" {
		return custom
	}
	return fallbackLockDir
}
