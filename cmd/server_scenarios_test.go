package cmd

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestResolveServerHostScopeUsesFriendlyNameOrEnvOverride(t *testing.T) {
	t.Run("uses explicit friendly name", func(t *testing.T) {
		database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer database.Close()

		now := time.Now()
		mock.ExpectQuery(`(?s)SELECT id, name, hostname, COALESCE\(ip, ''\), root_path, settings, created_at\s+FROM hosts\s+WHERE LOWER\(name\) = LOWER\(\$1\) OR LOWER\(hostname\) = LOWER\(\$1\)`).
			WithArgs("Brain").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
				AddRow(1, "Brain", "brain.local", "", "", []byte(`{}`), now))

		scope, err := resolveServerHostScope(database, "Brain", "book16.local")
		if err != nil {
			t.Fatalf("resolveServerHostScope: %v", err)
		}
		if scope.Hostname != "brain.local" || scope.AllHosts {
			t.Fatalf("unexpected scope: %#v", scope)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("uses env override before local hostname", func(t *testing.T) {
		database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer database.Close()

		t.Setenv("DEDUPLICATOR_SERVER_HOST", "Pinky")
		now := time.Now()
		mock.ExpectQuery(`(?s)SELECT id, name, hostname, COALESCE\(ip, ''\), root_path, settings, created_at\s+FROM hosts\s+WHERE LOWER\(name\) = LOWER\(\$1\) OR LOWER\(hostname\) = LOWER\(\$1\)`).
			WithArgs("Pinky").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "hostname", "ip", "root_path", "settings", "created_at"}).
				AddRow(2, "Pinky", "pinky.local", "", "", []byte(`{}`), now))

		scope, err := resolveServerHostScope(database, "", "book16.local")
		if err != nil {
			t.Fatalf("resolveServerHostScope: %v", err)
		}
		if scope.Hostname != "pinky.local" || scope.AllHosts {
			t.Fatalf("unexpected scope: %#v", scope)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("unknown local hostname becomes all-host read-only scope", func(t *testing.T) {
		database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer database.Close()

		mock.ExpectQuery(`(?s)SELECT id, name, hostname, COALESCE\(ip, ''\), root_path, settings, created_at\s+FROM hosts\s+WHERE LOWER\(name\) = LOWER\(\$1\) OR LOWER\(hostname\) = LOWER\(\$1\)`).
			WithArgs("book16").
			WillReturnError(sql.ErrNoRows)

		scope, err := resolveServerHostScope(database, "", "book16")
		if err != nil {
			t.Fatalf("resolveServerHostScope: %v", err)
		}
		if !scope.AllHosts || scope.Name != "all hosts" {
			t.Fatalf("unexpected scope: %#v", scope)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("unknown explicit host is an error", func(t *testing.T) {
		database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer database.Close()

		mock.ExpectQuery(`(?s)SELECT id, name, hostname, COALESCE\(ip, ''\), root_path, settings, created_at\s+FROM hosts\s+WHERE LOWER\(name\) = LOWER\(\$1\) OR LOWER\(hostname\) = LOWER\(\$1\)`).
			WithArgs("Missing").
			WillReturnError(sql.ErrNoRows)

		if _, err := resolveServerHostScope(database, "Missing", "book16"); err == nil {
			t.Fatalf("expected explicit host lookup to fail")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestServerLocalHostnameUsesConfigOverride(t *testing.T) {
	t.Setenv("DEDUPLICATOR_HOSTNAME", "book16")

	hostname, err := serverLocalHostname()
	if err != nil {
		t.Fatalf("serverLocalHostname: %v", err)
	}
	if hostname != "book16" {
		t.Fatalf("hostname=%q, want book16", hostname)
	}
}

func TestServerHealthAndDeleteAreReadOnlyForRemoteHost(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	server := newDeduplicatorHTTPServerWithOptions(deduplicatorHTTPServerOptions{
		db:                   database,
		hostname:             "brain.local",
		localHostname:        "book16.local",
		deleteEnabled:        false,
		deleteDisabledReason: "delete disabled for remote dev host",
	})

	healthReq := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	healthResp := httptest.NewRecorder()
	server.routes().ServeHTTP(healthResp, healthReq)
	if healthResp.Code != http.StatusOK {
		t.Fatalf("health status = %d, body = %s", healthResp.Code, healthResp.Body.String())
	}
	var health healthResponse
	if err := json.Unmarshal(healthResp.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health.DeleteEnabled {
		t.Fatalf("expected delete to be disabled: %#v", health)
	}

	deleteReq := httptest.NewRequest(http.MethodPost, "/api/files/9/delete", nil)
	deleteResp := httptest.NewRecorder()
	server.routes().ServeHTTP(deleteResp, deleteReq)
	if deleteResp.Code != http.StatusForbidden {
		t.Fatalf("delete status = %d, body = %s", deleteResp.Code, deleteResp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db call: %v", err)
	}
}

func TestServerSearchAllHostsWhenLocalHostIsNotIndexed(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	root := t.TempDir()
	mock.ExpectQuery(`(?s)SELECT id, path, COALESCE\(root_folder, ''\), hostname, size, COALESCE\(hash, ''\), last_hashed_at\s+FROM files\s+WHERE LOWER\(path\) LIKE \$1\s+OR LOWER\(COALESCE\(root_folder, ''\) \|\| '/' \|\| path\) LIKE \$1\s+ORDER BY hostname ASC, id DESC\s+LIMIT \$2`).
		WithArgs("%future%", 100).
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder", "hostname", "size", "hash", "last_hashed_at"}).
			AddRow(8, "movies/Future.mkv", root, "brain.local", int64(99), "hash", nil))

	server := newDeduplicatorHTTPServerWithOptions(deduplicatorHTTPServerOptions{
		db:            database,
		allHosts:      true,
		localHostname: "book16",
		deleteEnabled: false,
	})
	request := httptest.NewRequest(http.MethodGet, "/api/search?q=Future", nil)
	response := httptest.NewRecorder()

	server.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var results []fileSearchResult
	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(results) != 1 || results[0].Hostname != "brain.local" {
		t.Fatalf("unexpected results: %#v", results)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestServerSearchReturnsPartialPathMatchesForLocalHost(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	root := t.TempDir()
	hashedAt := time.Now().UTC()
	mock.ExpectQuery(`(?s)SELECT id, path, COALESCE\(root_folder, ''\), hostname, size, COALESCE\(hash, ''\), last_hashed_at\s+FROM files\s+WHERE LOWER\(hostname\) = LOWER\(\$1\).*LOWER\(path\) LIKE \$2.*LIMIT \$3`).
		WithArgs("brain.local", "%future%", 25).
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder", "hostname", "size", "hash", "last_hashed_at"}).
			AddRow(7, "movies/Back to the Future.mkv", root, "brain.local", int64(42), "abc123", hashedAt))

	server := newDeduplicatorHTTPServer(database, "brain.local", "")
	request := httptest.NewRequest(http.MethodGet, "/api/search?q=Future&limit=25", nil)
	response := httptest.NewRecorder()

	server.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var results []fileSearchResult
	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result, got %#v", results)
	}
	if results[0].ID != 7 || results[0].FullPath != filepath.Join(root, "movies/Back to the Future.mkv") {
		t.Fatalf("unexpected result: %#v", results[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestServerSearchRequiresQuery(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	server := newDeduplicatorHTTPServer(database, "brain.local", "")
	request := httptest.NewRequest(http.MethodGet, "/api/search?q=", nil)
	response := httptest.NewRecorder()

	server.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := strings.TrimSpace(response.Body.String()); got != "[]" {
		t.Fatalf("expected empty JSON array, got %s", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected db call: %v", err)
	}
}

func TestServerDeleteRemovesFileThenDBRow(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	root := t.TempDir()
	relPath := "movies/delete-me.mkv"
	fullPath := filepath.Join(root, relPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(fullPath, []byte("duplicate"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	mock.ExpectQuery(`(?s)SELECT id, path, COALESCE\(root_folder, ''\), hostname, size, COALESCE\(hash, ''\), last_hashed_at\s+FROM files\s+WHERE id = \$1 AND LOWER\(hostname\) = LOWER\(\$2\)`).
		WithArgs(9, "brain.local").
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder", "hostname", "size", "hash", "last_hashed_at"}).
			AddRow(9, relPath, root, "brain.local", int64(9), "hash", nil))
	mock.ExpectExec(`DELETE FROM files WHERE id = \$1 AND LOWER\(hostname\) = LOWER\(\$2\)`).
		WithArgs(9, "brain.local").
		WillReturnResult(sqlmock.NewResult(0, 1))

	server := newDeduplicatorHTTPServer(database, "brain.local", "")
	request := httptest.NewRequest(http.MethodPost, "/api/files/9/delete", nil)
	response := httptest.NewRecorder()

	server.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(fullPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected file deletion, stat err = %v", err)
	}
	var result deleteFileResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !result.RemovedFile || !result.RemovedDB || result.AlreadyMissing {
		t.Fatalf("unexpected delete response: %#v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestServerDeleteMissingFileStillRemovesDBRow(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	root := t.TempDir()
	relPath := "movies/missing.mkv"
	mock.ExpectQuery(`(?s)SELECT id, path, COALESCE\(root_folder, ''\), hostname, size, COALESCE\(hash, ''\), last_hashed_at\s+FROM files\s+WHERE id = \$1 AND LOWER\(hostname\) = LOWER\(\$2\)`).
		WithArgs(10, "brain.local").
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder", "hostname", "size", "hash", "last_hashed_at"}).
			AddRow(10, relPath, root, "brain.local", nil, "", nil))
	mock.ExpectExec(`DELETE FROM files WHERE id = \$1 AND LOWER\(hostname\) = LOWER\(\$2\)`).
		WithArgs(10, "brain.local").
		WillReturnResult(sqlmock.NewResult(0, 1))

	server := newDeduplicatorHTTPServer(database, "brain.local", "")
	request := httptest.NewRequest(http.MethodPost, "/api/files/10/delete", nil)
	response := httptest.NewRecorder()

	server.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var result deleteFileResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !result.AlreadyMissing || result.RemovedFile || !result.RemovedDB {
		t.Fatalf("unexpected delete response: %#v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestServerDeleteRejectsUnsafeIndexedPathsBeforeDBDelete(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rootFolder string
		path       string
	}{
		{name: "missing root", rootFolder: "", path: "movie.mkv"},
		{name: "absolute path", rootFolder: t.TempDir(), path: "/etc/passwd"},
		{name: "escaping path", rootFolder: t.TempDir(), path: "../outside.mkv"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer database.Close()

			mock.ExpectQuery(`(?s)SELECT id, path, COALESCE\(root_folder, ''\), hostname, size, COALESCE\(hash, ''\), last_hashed_at\s+FROM files\s+WHERE id = \$1 AND LOWER\(hostname\) = LOWER\(\$2\)`).
				WithArgs(11, "brain.local").
				WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder", "hostname", "size", "hash", "last_hashed_at"}).
					AddRow(11, tc.path, tc.rootFolder, "brain.local", nil, "", nil))

			server := newDeduplicatorHTTPServer(database, "brain.local", "")
			request := httptest.NewRequest(http.MethodPost, "/api/files/11/delete", nil)
			response := httptest.NewRecorder()

			server.routes().ServeHTTP(response, request)

			if response.Code != http.StatusConflict {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestServerDeleteRejectsNonRegularFilesBeforeDBDelete(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "directory"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	mock.ExpectQuery(`(?s)SELECT id, path, COALESCE\(root_folder, ''\), hostname, size, COALESCE\(hash, ''\), last_hashed_at\s+FROM files\s+WHERE id = \$1 AND LOWER\(hostname\) = LOWER\(\$2\)`).
		WithArgs(12, "brain.local").
		WillReturnRows(sqlmock.NewRows([]string{"id", "path", "root_folder", "hostname", "size", "hash", "last_hashed_at"}).
			AddRow(12, "directory", root, "brain.local", nil, "", nil))

	server := newDeduplicatorHTTPServer(database, "brain.local", "")
	request := httptest.NewRequest(http.MethodPost, "/api/files/12/delete", nil)
	response := httptest.NewRecorder()

	server.routes().ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(root, "directory")); err != nil {
		t.Fatalf("directory should not be removed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestParseSearchLimitBounds(t *testing.T) {
	if got := parseSearchLimit(""); got != 100 {
		t.Fatalf("default limit = %d", got)
	}
	if got := parseSearchLimit("-5"); got != 100 {
		t.Fatalf("negative limit = %d", got)
	}
	if got := parseSearchLimit("9999"); got != maxSearchLimit {
		t.Fatalf("max limit = %d", got)
	}
}

func TestServerDeleteMissingRowReturnsNotFound(t *testing.T) {
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()

	mock.ExpectQuery(`(?s)SELECT id, path, COALESCE\(root_folder, ''\), hostname, size, COALESCE\(hash, ''\), last_hashed_at\s+FROM files\s+WHERE id = \$1 AND LOWER\(hostname\) = LOWER\(\$2\)`).
		WithArgs(404, "brain.local").
		WillReturnError(sql.ErrNoRows)

	server := newDeduplicatorHTTPServer(database, "brain.local", "")
	request := httptest.NewRequest(http.MethodPost, "/api/files/404/delete", nil)
	response := httptest.NewRecorder()

	server.routes().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSafeIndexedPathRequiresAbsoluteRoot(t *testing.T) {
	if _, err := safeIndexedPath("relative/root", "movie.mkv"); err == nil {
		t.Fatalf("expected relative root to be rejected")
	}
}

func TestDisplayServerURL(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want string
	}{
		{addr: ":19111", want: "http://localhost:19111"},
		{addr: "0.0.0.0:9090", want: "http://localhost:9090"},
		{addr: "[::]:7070", want: "http://localhost:7070"},
		{addr: "127.0.0.1:6060", want: "http://127.0.0.1:6060"},
	} {
		if got := displayServerURL(tc.addr); got != tc.want {
			t.Fatalf("displayServerURL(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

func TestViteDevCORSAllowsConfiguredDevPort(t *testing.T) {
	for _, origin := range []string{
		"http://localhost:19110",
		"http://127.0.0.1:19110",
		"http://192.168.1.10:19110",
	} {
		if !isAllowedViteDevOrigin(origin) {
			t.Fatalf("expected origin to be allowed: %s", origin)
		}
	}

	for _, origin := range []string{
		"http://localhost:5173",
		"http://localhost:19111",
		"http://localhost:19110/path",
	} {
		if isAllowedViteDevOrigin(origin) {
			t.Fatalf("expected origin to be rejected: %s", origin)
		}
	}
}
