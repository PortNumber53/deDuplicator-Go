package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = orig
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func TestManageServerAddLowercasesAndRejectsDuplicates(t *testing.T) {
	t.Run("adds host with lowercased hostname", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectExec("INSERT INTO hosts").
			WithArgs("Backup1", "backup1.local", "10.0.0.5", "", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		if err := HandleManage(db, []string{"server-add", "Backup1", "--hostname", "Backup1.LOCAL", "--ip", "10.0.0.5"}); err != nil {
			t.Fatalf("HandleManage server-add error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("duplicate host surfaces error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectExec("INSERT INTO hosts").
			WillReturnError(os.ErrExist)

		if err := HandleManage(db, []string{"server-add", "Backup1", "--hostname", "backup1.local"}); err == nil {
			t.Fatalf("expected duplicate error")
		}
	})
}

func TestManageServerEditPreservesUnspecifiedFields(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	now := time.Now()
	mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE name = \\$1").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "Backup1", "backup1.local", "10.0.0.5", "/data", []byte(`{}`), now))

	mock.ExpectExec("UPDATE hosts SET name = \\$2, hostname = \\$3, ip = \\$4, root_path = \\$5, settings = \\$6 WHERE name = \\$1").
		WithArgs("Backup1", "Backup1", "backup1.lan", "10.0.0.5", "/data", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := HandleManage(db, []string{"server-edit", "Backup1", "--hostname", "backup1.lan"}); err != nil {
		t.Fatalf("HandleManage server-edit error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestManageServerListEmptyShowsGuidance(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts ORDER BY name").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}))

	output := captureStdout(t, func() {
		if err := HandleManage(db, []string{"server-list"}); err != nil {
			t.Fatalf("HandleManage server-list error: %v", err)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	if !strings.Contains(output, "No servers found") {
		t.Fatalf("expected guidance when empty, got: %s", output)
	}
}

func TestManagePathAddAndEditUpdateSettings(t *testing.T) {
	t.Run("path-add writes mapping", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		now := time.Now()
		mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE name = \\$1").
			WithArgs("Backup1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
				AddRow(1, "Backup1", "backup1.local", "10.0.0.5", "/data", []byte(`{}`), now))

		mock.ExpectExec("UPDATE hosts SET name = \\$2, hostname = \\$3, ip = \\$4, root_path = \\$5, settings = \\$6 WHERE name = \\$1").
			WithArgs("Backup1", "Backup1", "backup1.local", "10.0.0.5", "/data", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		if err := HandleManage(db, []string{"path-add", "Backup1", "photos", "/data/photos"}); err != nil {
			t.Fatalf("HandleManage path-add error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("path-edit rewrites mapping", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		now := time.Now()
		mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE name = \\$1").
			WithArgs("Backup1").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
				AddRow(1, "Backup1", "backup1.local", "10.0.0.5", "/data", []byte(`{"paths":{"photos":"/data/photos"}}`), now))

		mock.ExpectExec("UPDATE hosts SET name = \\$2, hostname = \\$3, ip = \\$4, root_path = \\$5, settings = \\$6 WHERE name = \\$1").
			WithArgs("Backup1", "Backup1", "backup1.local", "10.0.0.5", "/data", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		if err := HandleManage(db, []string{"path-edit", "Backup1", "photos", "/mnt/photos"}); err != nil {
			t.Fatalf("HandleManage path-edit error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestManagePathDeleteMissingIsReported(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	now := time.Now()
	mock.ExpectQuery("SELECT id, name, hostname, ip, root_path, settings, created_at FROM hosts WHERE name = \\$1").
		WithArgs("Backup1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
			AddRow(1, "Backup1", "backup1.local", "10.0.0.5", "/data", []byte(`{"paths":{"photos":"/data/photos"}}`), now))

	output := captureStdout(t, func() {
		if err := HandleManage(db, []string{"path-delete", "Backup1", "docs"}); err != nil {
			t.Fatalf("HandleManage path-delete error: %v", err)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	if !strings.Contains(output, "not found") {
		t.Fatalf("expected missing path message, got: %s", output)
	}
}
