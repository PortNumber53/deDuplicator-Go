package files

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPruneNonExistentFiles(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "prune_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	testFilePath := filepath.Join(tempDir, "existing.txt")
	err = os.WriteFile(testFilePath, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

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

	// Set up expectations for the host details query
	hostDetailsRows := sqlmock.NewRows([]string{"name", "root_path"}).
		AddRow("testhost", tempDir)
	mock.ExpectQuery("SELECT name, root_path FROM hosts WHERE name = \\$1").
		WithArgs("testhost").
		WillReturnRows(hostDetailsRows)

	// Set up expectations for the count query
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(3)
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("testhost").
		WillReturnRows(countRows)

	// Set up expectations for the files query
	fileRows := sqlmock.NewRows([]string{"id", "path"}).
		AddRow(1, "existing.txt").
		AddRow(2, "nonexistent.txt").
		AddRow(3, "symlink.txt")
	mock.ExpectQuery("SELECT id, path").
		WithArgs("testhost").
		WillReturnRows(fileRows)

	// Set up expectations for transaction
	mock.ExpectBegin()

	// Set up expectations for the delete statement
	mock.ExpectPrepare("DELETE FROM files WHERE id = \\$1").
		ExpectExec().
		WithArgs(2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Set up expectations for the delete statement for symlinks
	mock.ExpectExec("DELETE FROM files").
		WithArgs(3).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Set up expectations for transaction commit
	mock.ExpectCommit()

	// Call the function
	err = PruneNonExistentFiles(context.Background(), db, PruneOptions{})
	if err != nil {
		t.Errorf("PruneNonExistentFiles returned error: %v", err)
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestPruneNonExistentFilesHostNotFound(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New()
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

	// Call the function
	err = PruneNonExistentFiles(context.Background(), db, PruneOptions{})

	// Verify the error
	if err == nil {
		t.Error("Expected error for non-existent host, got nil")
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestPruneNonExistentFilesDeviceFiles(t *testing.T) {
	// Create a new mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock database: %v", err)
	}
	defer db.Close()

	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "prune_device_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	testFilePath := filepath.Join(tempDir, "regular.txt")
	err = os.WriteFile(testFilePath, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

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

	// Set up expectations for the host details query
	hostDetailsRows := sqlmock.NewRows([]string{"name", "root_path"}).
		AddRow("testhost", tempDir)
	mock.ExpectQuery("SELECT name, root_path FROM hosts WHERE name = \\$1").
		WithArgs("testhost").
		WillReturnRows(hostDetailsRows)

	// Set up expectations for the count query
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(2)
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("testhost").
		WillReturnRows(countRows)

	// Set up expectations for the files query
	fileRows := sqlmock.NewRows([]string{"id", "path"}).
		AddRow(1, "regular.txt").
		AddRow(2, "device.pipe") // This would be a device file in real life
	mock.ExpectQuery("SELECT id, path").
		WithArgs("testhost").
		WillReturnRows(fileRows)

	// Set up expectations for transaction
	mock.ExpectBegin()

	// Set up expectations for the prepared statement
	mock.ExpectPrepare("DELETE FROM files WHERE id = \\$1").
		ExpectExec().
		WithArgs(2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Set up expectations for transaction commit
	mock.ExpectCommit()

	// Call the function
	err = PruneNonExistentFiles(context.Background(), db, PruneOptions{})
	if err != nil {
		t.Errorf("PruneNonExistentFiles returned error: %v", err)
	}

	// Verify all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}
