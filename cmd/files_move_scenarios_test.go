package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMoveDupesTargetRequired(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	if err := HandleFiles(context.Background(), db, []string{"move-dupes"}); err == nil {
		t.Fatalf("expected error when --target is missing")
	}
}

func TestHashRejectsConflictingHashModeFlags(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	err = HandleFiles(context.Background(), db, []string{"hash", "--first-chunk", "--full-hash"})
	if err == nil {
		t.Fatalf("expected conflicting hash mode error")
	}
	if !strings.Contains(err.Error(), "--first-chunk and --full-hash cannot be used together") {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected database access: %v", err)
	}
}
