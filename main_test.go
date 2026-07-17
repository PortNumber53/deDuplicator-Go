package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func preserveEnv(t *testing.T, keys ...string) {
	t.Helper()
	orig := map[string]string{}
	for _, key := range keys {
		orig[key] = os.Getenv(key)
		_ = os.Unsetenv(key)
	}
	t.Cleanup(func() {
		for _, key := range keys {
			if orig[key] == "" {
				_ = os.Unsetenv(key)
			} else {
				_ = os.Setenv(key, orig[key])
			}
		}
	})
}

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
hostname=book16
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

	keys := []string{
		"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME",
		"DEDUPLICATOR_HOSTNAME", "LOCAL_MIGRATE_LOCK_DIR", "DEDUPLICATOR_LOCK_DIR",
		"RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_VHOST", "RABBITMQ_USER", "RABBITMQ_PASSWORD", "RABBITMQ_QUEUE",
		"LOG_FILE", "ERROR_LOG_FILE",
	}
	preserveEnv(t, keys...)

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
	if got := os.Getenv("DEDUPLICATOR_HOSTNAME"); got != "book16" {
		t.Fatalf("DEDUPLICATOR_HOSTNAME=%q, want %q", got, "book16")
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

	preserveEnv(t, "DB_HOST", "DB_PORT")

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

func TestLoadConfigFilesDoesNotErrorWhenOptionalFilesAreMissing(t *testing.T) {
	tmp := t.TempDir()
	loaded, err := loadConfigFiles(
		configFile{path: filepath.Join(tmp, "system.ini"), kind: configFileINI},
		configFile{path: filepath.Join(tmp, "user.ini"), kind: configFileINI},
		configFile{path: filepath.Join(tmp, ".env"), kind: configFileDotenv},
	)
	if err != nil {
		t.Fatalf("loadConfigFiles returned error for missing optional files: %v", err)
	}
	if loaded {
		t.Fatal("expected no config files to be loaded")
	}
}

func TestLoadConfigFilesReadsSystemUserThenDotenv(t *testing.T) {
	tmp := t.TempDir()
	systemPath := filepath.Join(tmp, "system.ini")
	userPath := filepath.Join(tmp, "user.ini")
	envPath := filepath.Join(tmp, ".env")
	if err := os.WriteFile(systemPath, []byte("[database]\nhost=system-host\n"), 0644); err != nil {
		t.Fatalf("write system config: %v", err)
	}
	if err := os.WriteFile(userPath, []byte("[database]\nhost=user-host\nport=15432\n"), 0644); err != nil {
		t.Fatalf("write user config: %v", err)
	}
	if err := os.WriteFile(envPath, []byte("DB_HOST=dotenv-host\n"), 0644); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	preserveEnv(t, "DB_HOST", "DB_PORT")

	loaded, err := loadConfigFiles(
		configFile{path: systemPath, kind: configFileINI},
		configFile{path: userPath, kind: configFileINI},
		configFile{path: envPath, kind: configFileDotenv},
	)
	if err != nil {
		t.Fatalf("loadConfigFiles returned error: %v", err)
	}
	if !loaded {
		t.Fatal("expected a config file to be loaded")
	}
	if got := os.Getenv("DB_HOST"); got != "system-host" {
		t.Fatalf("DB_HOST=%q, want system-host", got)
	}
	if got := os.Getenv("DB_PORT"); got != "15432" {
		t.Fatalf("DB_PORT=%q, want 15432", got)
	}
}

func TestDefaultConfigFilesIncludeRequestedLocations(t *testing.T) {
	files := defaultConfigFiles()
	paths := configFilePaths(files)

	if len(paths) < 2 {
		t.Fatalf("expected at least system config and .env paths, got %#v", paths)
	}
	if paths[0] != "/etc/dedupe/config.ini" {
		t.Fatalf("first config path=%q, want /etc/dedupe/config.ini", paths[0])
	}
	if paths[len(paths)-1] != ".env" {
		t.Fatalf("last config path=%q, want .env", paths[len(paths)-1])
	}
	foundUserConfig := false
	for _, path := range paths {
		if strings.HasSuffix(path, filepath.Join(".config", "dedupe", "config.ini")) {
			foundUserConfig = true
			break
		}
	}
	if !foundUserConfig {
		t.Fatalf("expected ~/.config/dedupe/config.ini in %#v", paths)
	}
}

func TestMissingConfigRejectionSkipsHelpAndHonorsEnv(t *testing.T) {
	keys := []string{
		"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME",
		"RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_VHOST", "RABBITMQ_USER", "RABBITMQ_PASSWORD", "RABBITMQ_QUEUE",
	}
	preserveEnv(t, keys...)

	if shouldRejectMissingConfig([]string{"deduplicator", "files", "prune", "--help"}, false) {
		t.Fatal("help should not require config files")
	}
	if !shouldRejectMissingConfig([]string{"deduplicator", "files", "prune"}, false) {
		t.Fatal("non-help command without config should be rejected")
	}

	_ = os.Setenv("DB_HOST", "db.example")
	if shouldRejectMissingConfig([]string{"deduplicator", "files", "prune"}, false) {
		t.Fatal("configured environment should satisfy startup config")
	}
}
