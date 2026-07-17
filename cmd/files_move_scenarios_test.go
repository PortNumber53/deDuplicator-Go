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

func TestMirrorGroupRequiresGroupName(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	if err := HandleFiles(context.Background(), db, []string{"mirror-group"}); err == nil {
		t.Fatalf("expected error when group name is missing")
	}
}

func TestHashUpgradeRejectsArguments(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	if err := HandleFiles(context.Background(), db, []string{"hash-upgrade", "extra"}); err == nil {
		t.Fatalf("expected error when hash-upgrade receives an argument")
	}
}
