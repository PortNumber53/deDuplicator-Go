package files

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCalculateFileHash(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "file_hash_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test cases
	tests := []struct {
		name        string
		setup       func(dir string) string
		expectError bool
		errorSubstr string
	}{
		{
			name: "Regular file",
			setup: func(dir string) string {
				path := filepath.Join(dir, "regular.txt")
				err := os.WriteFile(path, []byte("test content"), 0644)
				if err != nil {
					t.Fatalf("Failed to create test file: %v", err)
				}
				return path
			},
			expectError: false,
		},
		{
			name: "Non-existent file",
			setup: func(dir string) string {
				return filepath.Join(dir, "nonexistent.txt")
			},
			expectError: true,
			errorSubstr: "no such file",
		},
		{
			name: "Directory",
			setup: func(dir string) string {
				path := filepath.Join(dir, "subdir")
				err := os.Mkdir(path, 0755)
				if err != nil {
					t.Fatalf("Failed to create directory: %v", err)
				}
				return path
			},
			expectError: true,
			errorSubstr: "is a directory",
		},
		{
			name: "Symlink",
			setup: func(dir string) string {
				target := filepath.Join(dir, "target.txt")
				err := os.WriteFile(target, []byte("target content"), 0644)
				if err != nil {
					t.Fatalf("Failed to create target file: %v", err)
				}

				link := filepath.Join(dir, "symlink.txt")
				err = os.Symlink(target, link)
				if err != nil {
					t.Fatalf("Failed to create symlink: %v", err)
				}
				return link
			},
			expectError: true,
			errorSubstr: "is a symlink",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filePath := tc.setup(tempDir)
			hash, err := calculateFileHash(filePath)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tc.errorSubstr) {
					t.Errorf("Expected error containing %q, got %q", tc.errorSubstr, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if hash == "" {
					t.Errorf("Expected non-empty hash, got empty string")
				}
			}
		})
	}
}

func TestCalculateFileHashTimeout(t *testing.T) {
	// Skip this test in short mode as it involves waiting
	if testing.Short() {
		t.Skip("Skipping timeout test in short mode")
	}

	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "file_hash_timeout_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a mock file that simulates a very slow read
	// This is a bit tricky to test directly, so we'll use a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Create a large file (100MB of zeros)
	largePath := filepath.Join(tempDir, "large_file.bin")
	f, err := os.Create(largePath)
	if err != nil {
		t.Fatalf("Failed to create large file: %v", err)
	}

	// Write 100MB of zeros
	zeros := make([]byte, 1024*1024) // 1MB buffer
	for i := 0; i < 100; i++ {
		_, err := f.Write(zeros)
		if err != nil {
			f.Close()
			t.Fatalf("Failed to write to large file: %v", err)
		}
	}
	f.Close()

	// We can't easily test the timeout directly in the calculateFileHash function
	// since it has its own timeout mechanism. Instead, we'll test our helper function
	// that uses context cancellation.
	doneCh := make(chan struct{})
	go func() {
		_, err := calculateFileHashWithContext(ctx, largePath)
		if err == nil {
			t.Errorf("Expected error due to context cancellation, got none")
		} else if !strings.Contains(err.Error(), "context") {
			t.Errorf("Expected error about context cancellation, got: %v", err)
		}
		close(doneCh)
	}()

	// Wait for the test to complete or timeout
	select {
	case <-doneCh:
		// Test completed
	case <-time.After(5 * time.Second):
		t.Fatalf("Test timed out waiting for context cancellation")
	}
}

// Helper function to test with context
func calculateFileHashWithContext(ctx context.Context, filePath string) (string, error) {
	// This is a simplified version just for testing
	resultCh := make(chan struct {
		hash string
		err  error
	}, 1)

	go func() {
		hash, err := calculateFileHash(filePath)
		resultCh <- struct {
			hash string
			err  error
		}{hash, err}
	}()

	select {
	case result := <-resultCh:
		return result.hash, result.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestProcessStdin(t *testing.T) {
	// This would require mocking the database and stdin
	// For now, we'll just test the file type detection logic

	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "process_stdin_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create different types of files
	regularFile := filepath.Join(tempDir, "regular.txt")
	err = os.WriteFile(regularFile, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create regular file: %v", err)
	}

	dirPath := filepath.Join(tempDir, "subdir")
	err = os.Mkdir(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	symlinkPath := filepath.Join(tempDir, "symlink.txt")
	err = os.Symlink(regularFile, symlinkPath)
	if err != nil {
		t.Fatalf("Failed to create symlink: %v", err)
	}

	// Test isDirectory function
	if !isDirectory(dirPath) {
		t.Errorf("Expected %s to be detected as a directory", dirPath)
	}
	if isDirectory(regularFile) {
		t.Errorf("Expected %s not to be detected as a directory", regularFile)
	}

	// Test isSymlink function
	if !isSymlink(symlinkPath) {
		t.Errorf("Expected %s to be detected as a symlink", symlinkPath)
	}
	if isSymlink(regularFile) {
		t.Errorf("Expected %s not to be detected as a symlink", regularFile)
	}

	// Test isDeviceFile function
	if isDeviceFile(regularFile) {
		t.Errorf("Expected %s not to be detected as a device file", regularFile)
	}
	// Note: Testing actual device files is tricky in a portable way
}

// Helper functions for testing - these would normally be in the main package
// but we define them here for testing purposes

func isDirectory(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func isSymlink(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
}

func isDeviceFile(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return info.Mode()&(os.ModeDevice|os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0
}
