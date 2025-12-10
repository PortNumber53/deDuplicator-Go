package db

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMigrateDatabaseAppliesPendingFiles(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cwd, _ := os.Getwd()
	if err := os.Chdir(".."); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS migrations`).WillReturnResult(sqlmock.NewResult(0, 1))

	// Four .up.sql files exist in migrations/
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT EXISTS`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectBegin()
		mock.ExpectExec(`(?s).*`).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`INSERT INTO migrations`).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
	}

	if err := MigrateDatabase(db); err != nil {
		t.Fatalf("MigrateDatabase error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRollbackLastMigrationRunsDownFile(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cwd, _ := os.Getwd()
	if err := os.Chdir(".."); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	mock.ExpectQuery(`SELECT filename`).WillReturnRows(sqlmock.NewRows([]string{"filename"}).AddRow("000004_add_root_folder_to_files.up.sql"))
	mock.ExpectBegin()
	mock.ExpectExec(`(?s).*`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM migrations`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := RollbackLastMigration(db); err != nil {
		t.Fatalf("RollbackLastMigration error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestStatusMigrationsFlagsMissingFiles(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cwd, _ := os.Getwd()
	if err := os.Chdir(".."); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	mock.ExpectQuery(`SELECT filename FROM migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"filename"}).
			AddRow("000001_init.up.sql").
			AddRow("000999_missing.up.sql"))

	var buf bytes.Buffer
	origStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	if err := StatusMigrations(db); err != nil {
		t.Fatalf("StatusMigrations error: %v", err)
	}

	_ = w.Close()
	os.Stdout = origStdout
	_, _ = buf.ReadFrom(r)
	out := buf.String()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	if !strings.Contains(out, "missing in code") {
		t.Fatalf("expected missing migration to be flagged, output: %s", out)
	}
}
