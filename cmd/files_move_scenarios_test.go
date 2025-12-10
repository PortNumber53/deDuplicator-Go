package cmd

import (
	"context"
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
