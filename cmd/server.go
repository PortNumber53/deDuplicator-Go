package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	dedupdb "deduplicator/db"
)

const (
	defaultServerAddr    = ":19111"
	localServerUIDir     = "web/dist"
	installedServerUIDir = "/usr/local/share/deduplicator/web"
	maxSearchLimit       = 500
	viteDevPort          = "19110"
)

type deduplicatorHTTPServer struct {
	db                   *sql.DB
	hostname             string
	allHosts             bool
	uiDir                string
	localHostname        string
	deleteEnabled        bool
	deleteDisabledReason string
}

type fileSearchResult struct {
	ID           int        `json:"id"`
	Path         string     `json:"path"`
	RootFolder   string     `json:"rootFolder"`
	FullPath     string     `json:"fullPath"`
	Hostname     string     `json:"hostname"`
	Size         *int64     `json:"size,omitempty"`
	Hash         string     `json:"hash,omitempty"`
	LastHashedAt *time.Time `json:"lastHashedAt,omitempty"`
}

type deleteFileResponse struct {
	ID             int    `json:"id"`
	Path           string `json:"path"`
	RootFolder     string `json:"rootFolder"`
	FullPath       string `json:"fullPath"`
	RemovedFile    bool   `json:"removedFile"`
	AlreadyMissing bool   `json:"alreadyMissing"`
	RemovedDB      bool   `json:"removedDb"`
}

type apiError struct {
	Error string `json:"error"`
}

type healthResponse struct {
	Status               string `json:"status"`
	Hostname             string `json:"hostname"`
	AllHosts             bool   `json:"allHosts"`
	LocalHostname        string `json:"localHostname"`
	DeleteEnabled        bool   `json:"deleteEnabled"`
	DeleteDisabledReason string `json:"deleteDisabledReason,omitempty"`
}

type serverHostScope struct {
	Name     string
	Hostname string
	AllHosts bool
}

// HandleServer runs the HTTP server mode for file search and deletion.
func HandleServer(ctx context.Context, database *sql.DB, args []string) error {
	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	addr := serverCmd.String("addr", defaultServerAddr, "HTTP listen address")
	uiDir := serverCmd.String("ui-dir", "", "Directory containing the built Vite UI")
	hostOverride := serverCmd.String("host", "", "Friendly host name or hostname to serve (default: current OS hostname or DEDUPLICATOR_SERVER_HOST)")
	if err := serverCmd.Parse(args); err != nil {
		return fmt.Errorf("error parsing server flags: %v", err)
	}
	if serverCmd.NArg() != 0 {
		return fmt.Errorf("server does not accept positional arguments")
	}

	localHostname, err := serverLocalHostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %v", err)
	}
	scope, err := resolveServerHostScope(database, *hostOverride, localHostname)
	if err != nil {
		return err
	}

	resolvedUIDir := resolveServerUIDir(*uiDir)
	deleteEnabled := !scope.AllHosts && strings.EqualFold(scope.Hostname, localHostname)
	deleteDisabledReason := ""
	if scope.AllHosts {
		deleteDisabledReason = fmt.Sprintf("delete disabled because local hostname %s is not registered; serving all hosts in read-only mode", localHostname)
	} else if !deleteEnabled {
		deleteDisabledReason = fmt.Sprintf("delete disabled because this process is running on %s but serving indexed host %s", localHostname, scope.Hostname)
	}
	handler := newDeduplicatorHTTPServerWithOptions(deduplicatorHTTPServerOptions{
		db:                   database,
		hostname:             scope.Hostname,
		allHosts:             scope.AllHosts,
		uiDir:                resolvedUIDir,
		localHostname:        localHostname,
		deleteEnabled:        deleteEnabled,
		deleteDisabledReason: deleteDisabledReason,
	}).routes()
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	fmt.Printf("Deduplicator server listening on %s for host %s using UI %s\n", displayServerURL(*addr), scope.Name, resolvedUIDir)
	if deleteDisabledReason != "" {
		fmt.Printf("Warning: %s\n", deleteDisabledReason)
	}
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type deduplicatorHTTPServerOptions struct {
	db                   *sql.DB
	hostname             string
	allHosts             bool
	uiDir                string
	localHostname        string
	deleteEnabled        bool
	deleteDisabledReason string
}

func newDeduplicatorHTTPServer(database *sql.DB, hostname, uiDir string) *deduplicatorHTTPServer {
	return newDeduplicatorHTTPServerWithOptions(deduplicatorHTTPServerOptions{
		db:            database,
		hostname:      hostname,
		uiDir:         uiDir,
		localHostname: hostname,
		deleteEnabled: true,
	})
}

func newDeduplicatorHTTPServerWithOptions(opts deduplicatorHTTPServerOptions) *deduplicatorHTTPServer {
	if opts.localHostname == "" {
		opts.localHostname = opts.hostname
	}
	return &deduplicatorHTTPServer{
		db:                   opts.db,
		hostname:             strings.ToLower(opts.hostname),
		allHosts:             opts.allHosts,
		uiDir:                opts.uiDir,
		localHostname:        strings.ToLower(opts.localHostname),
		deleteEnabled:        opts.deleteEnabled,
		deleteDisabledReason: opts.deleteDisabledReason,
	}
}

func resolveServerHostScope(database *sql.DB, requestedHost, localHostname string) (serverHostScope, error) {
	requestedHost = strings.TrimSpace(requestedHost)
	if requestedHost == "" {
		requestedHost = strings.TrimSpace(os.Getenv("DEDUPLICATOR_SERVER_HOST"))
	}
	explicitHost := requestedHost != ""
	if requestedHost == "" {
		requestedHost = localHostname
	}

	host, err := findServerHost(database, requestedHost)
	if err != nil {
		if explicitHost {
			return serverHostScope{}, fmt.Errorf("failed to find host %q: %v", requestedHost, err)
		}
		return serverHostScope{
			Name:     "all hosts",
			Hostname: "",
			AllHosts: true,
		}, nil
	}
	return serverHostScope{
		Name:     host.Name,
		Hostname: host.Hostname,
	}, nil
}

func serverLocalHostname() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("DEDUPLICATOR_HOSTNAME")); configured != "" {
		return configured, nil
	}
	return os.Hostname()
}

func findServerHost(database *sql.DB, nameOrHostname string) (*dedupdb.Host, error) {
	host := &dedupdb.Host{}
	err := database.QueryRow(`
		SELECT id, name, hostname, COALESCE(ip, ''), root_path, settings, created_at
		FROM hosts
		WHERE LOWER(name) = LOWER($1) OR LOWER(hostname) = LOWER($1)
		ORDER BY CASE WHEN LOWER(name) = LOWER($1) THEN 0 ELSE 1 END
		LIMIT 1
	`, nameOrHostname).Scan(&host.ID, &host.Name, &host.Hostname, &host.IP, &host.RootPath, &host.Settings, &host.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("host not found by name or hostname: %s", nameOrHostname)
	}
	return host, err
}

func (s *deduplicatorHTTPServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/search", s.handleSearch)
	mux.HandleFunc("/api/files/", s.handleFileAction)
	mux.HandleFunc("/", s.handleUI)
	return withViteDevCORS(mux)
}

func (s *deduplicatorHTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, healthResponse{
		Status:               "ok",
		Hostname:             s.hostname,
		AllHosts:             s.allHosts,
		LocalHostname:        s.localHostname,
		DeleteEnabled:        s.deleteEnabled,
		DeleteDisabledReason: s.deleteDisabledReason,
	})
}

func (s *deduplicatorHTTPServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, http.StatusOK, []fileSearchResult{})
		return
	}

	limit := parseSearchLimit(r.URL.Query().Get("limit"))
	results, err := s.searchFiles(r.Context(), query, limit)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *deduplicatorHTTPServer) handleFileAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id, action, ok := parseFileActionPath(r.URL.Path)
	if !ok || action != "delete" {
		writeAPIError(w, http.StatusNotFound, "unknown file action")
		return
	}
	if !s.deleteEnabled {
		writeAPIError(w, http.StatusForbidden, s.deleteDisabledReason)
		return
	}

	response, err := s.deleteIndexedFile(r.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, sql.ErrNoRows) {
			status = http.StatusNotFound
		} else if errors.Is(err, errUnsafeFilePath) || errors.Is(err, errUnsupportedFileType) {
			status = http.StatusConflict
		}
		writeAPIError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *deduplicatorHTTPServer) searchFiles(ctx context.Context, query string, limit int) ([]fileSearchResult, error) {
	pattern := "%" + strings.ToLower(query) + "%"
	if s.allHosts {
		return s.searchFilesAcrossHosts(ctx, pattern, limit)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, path, COALESCE(root_folder, ''), hostname, size, COALESCE(hash, ''), last_hashed_at
		FROM files
		WHERE LOWER(hostname) = LOWER($1)
		  AND (
			LOWER(path) LIKE $2
			OR LOWER(COALESCE(root_folder, '') || '/' || path) LIKE $2
		  )
		ORDER BY id DESC
		LIMIT $3
	`, s.hostname, pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []fileSearchResult{}
	for rows.Next() {
		result, err := scanFileSearchResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *deduplicatorHTTPServer) searchFilesAcrossHosts(ctx context.Context, pattern string, limit int) ([]fileSearchResult, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, path, COALESCE(root_folder, ''), hostname, size, COALESCE(hash, ''), last_hashed_at
		FROM files
		WHERE LOWER(path) LIKE $1
		   OR LOWER(COALESCE(root_folder, '') || '/' || path) LIKE $1
		ORDER BY hostname ASC, id DESC
		LIMIT $2
	`, pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []fileSearchResult{}
	for rows.Next() {
		result, err := scanFileSearchResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *deduplicatorHTTPServer) deleteIndexedFile(ctx context.Context, id int) (deleteFileResponse, error) {
	var row fileSearchResult
	var size sql.NullInt64
	var hash string
	var lastHashedAt sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT id, path, COALESCE(root_folder, ''), hostname, size, COALESCE(hash, ''), last_hashed_at
		FROM files
		WHERE id = $1 AND LOWER(hostname) = LOWER($2)
	`, id, s.hostname).Scan(&row.ID, &row.Path, &row.RootFolder, &row.Hostname, &size, &hash, &lastHashedAt)
	if err != nil {
		return deleteFileResponse{}, err
	}

	fullPath, err := safeIndexedPath(row.RootFolder, row.Path)
	if err != nil {
		return deleteFileResponse{}, err
	}

	response := deleteFileResponse{
		ID:         row.ID,
		Path:       row.Path,
		RootFolder: row.RootFolder,
		FullPath:   fullPath,
	}

	info, err := os.Lstat(fullPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return deleteFileResponse{}, fmt.Errorf("failed to inspect file %s: %w", fullPath, err)
		}
		response.AlreadyMissing = true
	} else {
		if !info.Mode().IsRegular() {
			return deleteFileResponse{}, fmt.Errorf("%w: refusing to delete non-regular file %s", errUnsupportedFileType, fullPath)
		}
		if err := os.Remove(fullPath); err != nil {
			return deleteFileResponse{}, fmt.Errorf("failed to delete file %s: %w", fullPath, err)
		}
		response.RemovedFile = true
	}

	result, err := s.db.ExecContext(ctx, `DELETE FROM files WHERE id = $1 AND LOWER(hostname) = LOWER($2)`, id, s.hostname)
	if err != nil {
		return deleteFileResponse{}, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return deleteFileResponse{}, err
	}
	response.RemovedDB = rowsAffected > 0
	return response, nil
}

func scanFileSearchResult(scanner interface {
	Scan(dest ...any) error
}) (fileSearchResult, error) {
	var result fileSearchResult
	var size sql.NullInt64
	var hash string
	var lastHashedAt sql.NullTime
	if err := scanner.Scan(&result.ID, &result.Path, &result.RootFolder, &result.Hostname, &size, &hash, &lastHashedAt); err != nil {
		return fileSearchResult{}, err
	}
	if size.Valid {
		result.Size = &size.Int64
	}
	if hash != "" {
		result.Hash = hash
	}
	if lastHashedAt.Valid {
		result.LastHashedAt = &lastHashedAt.Time
	}
	if fullPath, err := safeIndexedPath(result.RootFolder, result.Path); err == nil {
		result.FullPath = fullPath
	}
	return result, nil
}

var (
	errUnsafeFilePath      = errors.New("unsafe indexed file path")
	errUnsupportedFileType = errors.New("unsupported file type")
)

func safeIndexedPath(rootFolder, path string) (string, error) {
	rootFolder = strings.TrimSpace(rootFolder)
	path = strings.TrimSpace(path)
	if rootFolder == "" || path == "" {
		return "", fmt.Errorf("%w: root_folder and path are required", errUnsafeFilePath)
	}
	if !filepath.IsAbs(rootFolder) {
		return "", fmt.Errorf("%w: root_folder must be absolute", errUnsafeFilePath)
	}
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("%w: indexed path must be relative to root_folder", errUnsafeFilePath)
	}

	root := filepath.Clean(rootFolder)
	cleanRel := filepath.Clean(path)
	if cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: indexed path escapes root_folder", errUnsafeFilePath)
	}

	fullPath := filepath.Join(root, cleanRel)
	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errUnsafeFilePath, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: indexed path escapes root_folder", errUnsafeFilePath)
	}
	return fullPath, nil
}

func parseSearchLimit(raw string) int {
	if raw == "" {
		return 100
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 100
	}
	if limit > maxSearchLimit {
		return maxSearchLimit
	}
	return limit
}

func parseFileActionPath(path string) (int, string, bool) {
	trimmed := strings.Trim(strings.TrimPrefix(path, "/api/files/"), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return 0, "", false
	}
	id, err := strconv.Atoi(parts[0])
	if err != nil || id <= 0 {
		return 0, "", false
	}
	return id, parts[1], true
}

func resolveServerUIDir(requested string) string {
	if strings.TrimSpace(requested) != "" {
		return requested
	}
	if fileExists(filepath.Join(localServerUIDir, "index.html")) {
		return localServerUIDir
	}
	return installedServerUIDir
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func displayServerURL(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "http://localhost" + addr
	}
	if strings.HasPrefix(addr, "0.0.0.0:") {
		return "http://localhost:" + strings.TrimPrefix(addr, "0.0.0.0:")
	}
	if strings.HasPrefix(addr, "[::]:") {
		return "http://localhost:" + strings.TrimPrefix(addr, "[::]:")
	}
	return "http://" + addr
}

func withViteDevCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if isAllowedViteDevOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Add("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isAllowedViteDevOrigin(origin string) bool {
	if origin == "" {
		return false
	}
	for _, prefix := range []string{"http://", "https://"} {
		if !strings.HasPrefix(origin, prefix) {
			continue
		}
		hostPort := strings.TrimPrefix(origin, prefix)
		host, port, ok := strings.Cut(hostPort, ":")
		if !ok || port != viteDevPort || host == "" {
			return false
		}
		return !strings.Contains(host, "/")
	}
	return false
}

func (s *deduplicatorHTTPServer) handleUI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	indexPath := filepath.Join(s.uiDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<html><body><h1>Deduplicator UI not built</h1><p>Run npm --prefix web install and npm --prefix web run build, or start the Vite dev server with npm --prefix web run dev.</p></body></html>`))
		return
	}

	cleanPath := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/"))
	requested := filepath.Join(s.uiDir, cleanPath)
	if cleanPath != "." {
		if rel, err := filepath.Rel(s.uiDir, requested); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			if info, err := os.Stat(requested); err == nil && !info.IsDir() {
				http.ServeFile(w, r, requested)
				return
			}
		}
	}
	http.ServeFile(w, r, indexPath)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, apiError{Error: message})
}
