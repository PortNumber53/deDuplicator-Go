package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"encoding/json"

	_ "github.com/lib/pq"
)

type Host struct {
	ID        int
	Name      string
	Hostname  string
	IP        string
	RootPath  string          // deprecated, keep for now
	Settings  json.RawMessage // stores paths and other settings as JSON
	CreatedAt time.Time
}

type HostPaths struct {
	Paths map[string]string `json:"paths"` // friendly name -> absolute path
}

// GetPaths returns the paths from the host's settings JSON
func (h *Host) GetPaths() (map[string]string, error) {
	if h.Settings == nil || len(h.Settings) == 0 {
		return map[string]string{}, nil
	}
	var hp HostPaths
	err := json.Unmarshal(h.Settings, &hp)
	if err != nil {
		return nil, err
	}
	if hp.Paths == nil {
		hp.Paths = map[string]string{}
	}
	return hp.Paths, nil
}

// SetPaths sets the paths in the host's settings JSON
func (h *Host) SetPaths(paths map[string]string) error {
	hp := HostPaths{Paths: paths}
	settings, err := json.Marshal(hp)
	if err != nil {
		return err
	}
	h.Settings = settings
	return nil
}

func CreateDatabase(db *sql.DB, force bool) error {
	if force {
		fmt.Println("Force flag enabled: dropping existing tables...")
		_, err := db.Exec(`DROP TABLE IF EXISTS files; DROP TABLE IF EXISTS hosts`)
		if err != nil {
			return fmt.Errorf("error dropping tables: %v", err)
		}
	}

	fmt.Println("Creating hosts table...")
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS hosts (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			hostname TEXT NOT NULL,
			ip TEXT,
			root_path TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("error creating hosts table: %v", err)
	}

	fmt.Println("Creating files table...")
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			id SERIAL PRIMARY KEY,
			path TEXT NOT NULL,
			hostname TEXT NOT NULL,
			hash TEXT,
			size BIGINT,
			last_hashed_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(path, hostname),
			FOREIGN KEY (hostname) REFERENCES hosts(hostname)
		)
	`)
	if err != nil {
		return fmt.Errorf("error creating files table: %v", err)
	}

	fmt.Println("Database setup completed successfully")
	return nil
}

// AddHost adds a new host to the database
func AddHost(db *sql.DB, name, hostname, ip, rootPath string, settings json.RawMessage) error {
	settings = ensureSettings(settings)
	_, err := db.Exec(`
		INSERT INTO hosts (name, hostname, ip, root_path, settings)
		VALUES ($1, $2, $3, $4, $5)
	`, name, strings.ToLower(hostname), ip, rootPath, settings)
	return err
} // Note: for backward compatibility, rootPath can be provided as ""

// UpdateHost updates an existing host in the database
func UpdateHost(db *sql.DB, oldName, newName, hostname, ip, rootPath string, settings json.RawMessage) error {
	settings = ensureSettings(settings)
	result, err := db.Exec(`
		UPDATE hosts
		SET name = $2, hostname = $3, ip = $4, root_path = $5, settings = $6
		WHERE name = $1
	`, oldName, newName, strings.ToLower(hostname), ip, rootPath, settings)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("host not found: %s", oldName)
	}
	return nil
}

// ensureSettings guarantees we always write valid JSON (defaults to {}).
func ensureSettings(settings json.RawMessage) json.RawMessage {
	if len(settings) == 0 {
		return json.RawMessage(`{}`)
	}
	return settings
}

// DeleteHost deletes a host from the database
func DeleteHost(db *sql.DB, name string) error {
	result, err := db.Exec(`DELETE FROM hosts WHERE name = $1`, name)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("host not found: %s", name)
	}
	return nil
}

// GetHostByHostname retrieves a host by hostname (case-insensitive)
func GetHostByHostname(db *sql.DB, hostname string) (*Host, error) {
	host := &Host{}
	err := db.QueryRow(`
		SELECT id, name, hostname, ip, root_path, settings, created_at
		FROM hosts WHERE LOWER(hostname) = LOWER($1)
	`, hostname).Scan(&host.ID, &host.Name, &host.Hostname, &host.IP, &host.RootPath, &host.Settings, &host.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("host not found by hostname: %s", hostname)
	}
	return host, err
}

// GetHost retrieves a host by name
func GetHost(db *sql.DB, name string) (*Host, error) {
	host := &Host{}
	err := db.QueryRow(`
		SELECT id, name, hostname, ip, root_path, settings, created_at
		FROM hosts WHERE name = $1
	`, name).Scan(&host.ID, &host.Name, &host.Hostname, &host.IP, &host.RootPath, &host.Settings, &host.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("host not found: %s", name)
	}
	return host, err
}

// ListHosts returns all hosts in the database
func ListHosts(db *sql.DB) ([]Host, error) {
	rows, err := db.Query(`
		SELECT id, name, hostname, ip, root_path, settings, created_at
		FROM hosts ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hosts []Host
	for rows.Next() {
		var host Host
		err := rows.Scan(&host.ID, &host.Name, &host.Hostname, &host.IP, &host.RootPath, &host.Settings, &host.CreatedAt)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}
	return hosts, rows.Err()
}

func Connect(host, port, user, password, dbname string) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		host, port, user, dbname)
	if password != "" {
		connStr += fmt.Sprintf(" password=%s", password)
	}

	return sql.Open("postgres", connStr)
}
