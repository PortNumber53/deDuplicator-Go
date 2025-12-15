package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigINISupportsDefaultSectionAndNoSection(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "config.ini")
	if err := os.WriteFile(cfgPath, []byte(`
# No section: treated as [default]
host=192.168.0.10
port=54180
user=prod_user
password=prod_pass
name=prod_db
local_migrate_lock_dir=/var/lock/deduplicator

[rabbitmq]
host=rabbit.local
port=5679
vhost=/dedupe
user=ruser
password=rpass
queue=dedup_backup

[logging]
log_file=/var/log/dedupe/dedupe.log
error_log_file=/var/log/dedupe/error.log
`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Clear relevant env vars for test
	keys := []string{
		"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME",
		"LOCAL_MIGRATE_LOCK_DIR", "DEDUPLICATOR_LOCK_DIR",
		"RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_VHOST", "RABBITMQ_USER", "RABBITMQ_PASSWORD", "RABBITMQ_QUEUE",
		"LOG_FILE", "ERROR_LOG_FILE",
	}
	orig := map[string]string{}
	for _, k := range keys {
		orig[k] = os.Getenv(k)
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if orig[k] == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, orig[k])
			}
		}
	})

	loadConfigINI(cfgPath)

	if got := os.Getenv("DB_HOST"); got != "192.168.0.10" {
		t.Fatalf("DB_HOST=%q, want %q", got, "192.168.0.10")
	}
	if got := os.Getenv("DB_PORT"); got != "54180" {
		t.Fatalf("DB_PORT=%q, want %q", got, "54180")
	}
	if got := os.Getenv("DB_USER"); got != "prod_user" {
		t.Fatalf("DB_USER=%q, want %q", got, "prod_user")
	}
	if got := os.Getenv("DB_PASSWORD"); got != "prod_pass" {
		t.Fatalf("DB_PASSWORD=%q, want %q", got, "prod_pass")
	}
	if got := os.Getenv("DB_NAME"); got != "prod_db" {
		t.Fatalf("DB_NAME=%q, want %q", got, "prod_db")
	}
	if got := os.Getenv("LOCAL_MIGRATE_LOCK_DIR"); got != "/var/lock/deduplicator" {
		t.Fatalf("LOCAL_MIGRATE_LOCK_DIR=%q, want %q", got, "/var/lock/deduplicator")
	}

	if got := os.Getenv("RABBITMQ_HOST"); got != "rabbit.local" {
		t.Fatalf("RABBITMQ_HOST=%q, want %q", got, "rabbit.local")
	}
	if got := os.Getenv("RABBITMQ_PORT"); got != "5679" {
		t.Fatalf("RABBITMQ_PORT=%q, want %q", got, "5679")
	}
	if got := os.Getenv("RABBITMQ_VHOST"); got != "/dedupe" {
		t.Fatalf("RABBITMQ_VHOST=%q, want %q", got, "/dedupe")
	}
	if got := os.Getenv("RABBITMQ_USER"); got != "ruser" {
		t.Fatalf("RABBITMQ_USER=%q, want %q", got, "ruser")
	}
	if got := os.Getenv("RABBITMQ_PASSWORD"); got != "rpass" {
		t.Fatalf("RABBITMQ_PASSWORD=%q, want %q", got, "rpass")
	}
	if got := os.Getenv("RABBITMQ_QUEUE"); got != "dedup_backup" {
		t.Fatalf("RABBITMQ_QUEUE=%q, want %q", got, "dedup_backup")
	}

	if got := os.Getenv("LOG_FILE"); got != "/var/log/dedupe/dedupe.log" {
		t.Fatalf("LOG_FILE=%q, want %q", got, "/var/log/dedupe/dedupe.log")
	}
	if got := os.Getenv("ERROR_LOG_FILE"); got != "/var/log/dedupe/error.log" {
		t.Fatalf("ERROR_LOG_FILE=%q, want %q", got, "/var/log/dedupe/error.log")
	}
}

func TestLoadConfigINIDoesNotOverrideExistingEnv(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "config.ini")
	if err := os.WriteFile(cfgPath, []byte(`
[database]
host=config-host
port=5433
`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	origHost := os.Getenv("DB_HOST")
	origPort := os.Getenv("DB_PORT")
	t.Cleanup(func() {
		if origHost == "" {
			_ = os.Unsetenv("DB_HOST")
		} else {
			_ = os.Setenv("DB_HOST", origHost)
		}
		if origPort == "" {
			_ = os.Unsetenv("DB_PORT")
		} else {
			_ = os.Setenv("DB_PORT", origPort)
		}
	})

	_ = os.Setenv("DB_HOST", "env-host")
	_ = os.Setenv("DB_PORT", "9999")

	loadConfigINI(cfgPath)

	if got := os.Getenv("DB_HOST"); got != "env-host" {
		t.Fatalf("DB_HOST=%q, want %q", got, "env-host")
	}
	if got := os.Getenv("DB_PORT"); got != "9999" {
		t.Fatalf("DB_PORT=%q, want %q", got, "9999")
	}
}
