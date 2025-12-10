package files

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestProcessStdinWithMockDB(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

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

	// Set up expectations for the hostname query
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Failed to get hostname: %v", err)
	}

	// Set up expectations for the host query
	hostRows := sqlmock.NewRows([]string{"name"}).
		AddRow("testhost")
	mock.ExpectQuery("SELECT name FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(hostname).
		WillReturnRows(hostRows)

	// Set up expectations for the transaction
	mock.ExpectBegin()

	// Set up expectations for the prepared statement
	mock.ExpectPrepare("INSERT INTO files").
		ExpectExec().
		WithArgs(regularFile, "testhost", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Set up expectations for the transaction commit
	mock.ExpectCommit()

	// Create a pipe to simulate stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}

	// Save original stdin and restore it after the test
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
	}()

	// Write test data to the pipe
	go func() {
		defer w.Close()
		w.Write([]byte(regularFile + "\n"))
		w.Write([]byte(dirPath + "\n"))
		w.Write([]byte(symlinkPath + "\n"))
	}()

	// Call the function
	err = ProcessStdin(context.Background(), db)
	if err != nil {
		t.Errorf("ProcessStdin returned error: %v", err)
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestProcessStdinHostNotFound(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Get the hostname
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Failed to get hostname: %v", err)
	}

	// Set up expectations for the host query to return no rows
	mock.ExpectQuery("SELECT name FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(hostname).
		WillReturnError(sql.ErrNoRows)

	// Create a pipe to simulate stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}

	// Save original stdin and restore it after the test
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
	}()

	// Write test data to the pipe
	go func() {
		defer w.Close()
		w.Write([]byte("/path/to/file.txt\n"))
	}()

	// Call the function
	err = ProcessStdin(context.Background(), db)

	// Verify the error
	if err == nil {
		t.Error("Expected error for non-existent host, got nil")
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestProcessStdinEmptyInput(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Get the hostname
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Failed to get hostname: %v", err)
	}

	// Set up expectations for the host query
	hostRows := sqlmock.NewRows([]string{"name"}).
		AddRow("testhost")
	mock.ExpectQuery("SELECT name FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(hostname).
		WillReturnRows(hostRows)

	// Even for empty input, the transaction is started
	mock.ExpectBegin()

	// Prepare statement is created
	mock.ExpectPrepare("INSERT INTO files").
		WillReturnError(nil)

	// Transaction is committed
	mock.ExpectCommit()

	// Create a pipe to simulate stdin with no input
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}
	w.Close() // Close immediately to simulate empty input

	// Save original stdin and restore it after the test
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
	}()

	// Call the function
	err = ProcessStdin(context.Background(), db)
	if err != nil {
		t.Errorf("ProcessStdin returned error for empty input: %v", err)
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestFileTypeDetection(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "file_type_test")
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

	// Test cases
	tests := []struct {
		name      string
		path      string
		isDir     bool
		isSymlink bool
		isDevice  bool
	}{
		{
			name:      "Regular file",
			path:      regularFile,
			isDir:     false,
			isSymlink: false,
			isDevice:  false,
		},
		{
			name:      "Directory",
			path:      dirPath,
			isDir:     true,
			isSymlink: false,
			isDevice:  false,
		},
		{
			name:      "Symlink",
			path:      symlinkPath,
			isDir:     false,
			isSymlink: true,
			isDevice:  false,
		},
		{
			name:      "Non-existent file",
			path:      filepath.Join(tempDir, "nonexistent.txt"),
			isDir:     false,
			isSymlink: false,
			isDevice:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Get file info using Lstat
			info, err := os.Lstat(tc.path)
			if os.IsNotExist(err) {
				// For non-existent files, we expect all checks to be false
				if tc.isDir || tc.isSymlink || tc.isDevice {
					t.Errorf("Expected all type checks to be false for non-existent file")
				}
				return
			}
			if err != nil {
				t.Fatalf("Failed to get file info: %v", err)
			}

			// Check directory type
			isDir := info.IsDir()
			if isDir != tc.isDir {
				t.Errorf("Expected isDir=%v, got %v", tc.isDir, isDir)
			}

			// Check symlink type
			isSymlink := info.Mode()&os.ModeSymlink != 0
			if isSymlink != tc.isSymlink {
				t.Errorf("Expected isSymlink=%v, got %v", tc.isSymlink, isSymlink)
			}

			// Check device type
			isDevice := info.Mode()&(os.ModeDevice|os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0
			if isDevice != tc.isDevice {
				t.Errorf("Expected isDevice=%v, got %v", tc.isDevice, isDevice)
			}
		})
	}
}

// TestProcessStdinWithMockDBNoHost tests the ProcessStdin function when no host is found
func TestProcessStdinWithMockDBNoHost(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Get the hostname
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Failed to get hostname: %v", err)
	}

	// Set up expectations for the host query with no results
	mock.ExpectQuery("SELECT name FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(hostname).
		WillReturnError(sql.ErrNoRows)

	// Create a pipe to simulate stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}

	// Save original stdin and restore it after the test
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
	}()

	// Write test data to the pipe
	go func() {
		defer w.Close()
		w.Write([]byte("/path/to/file.txt\n"))
	}()

	// Call the function
	err = ProcessStdin(context.Background(), db)

	// Verify the error
	if err == nil {
		t.Error("Expected error for non-existent host, got nil")
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

// TestProcessStdinWithMockDBError tests the ProcessStdin function with a database error
func TestProcessStdinWithMockDBError(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Get the hostname
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Failed to get hostname: %v", err)
	}

	// Set up expectations for the host query with an error
	mock.ExpectQuery("SELECT name FROM hosts WHERE LOWER\\(hostname\\) = LOWER\\(\\$1\\)").
		WithArgs(hostname).
		WillReturnError(fmt.Errorf("database error"))

	// Create a pipe to simulate stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}

	// Save original stdin and restore it after the test
	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
	}()

	// Write test data to the pipe
	go func() {
		defer w.Close()
		w.Write([]byte("/path/to/file.txt\n"))
	}()

	// Call the function
	err = ProcessStdin(context.Background(), db)

	// Verify the error
	if err == nil {
		t.Error("Expected error for database error, got nil")
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}
