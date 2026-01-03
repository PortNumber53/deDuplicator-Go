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

type PathGroup struct {
	ID          int
	Name        string
	Description string
	MinCopies   int
	MaxCopies   *int // nullable
	CreatedAt   time.Time
}

type PathGroupMember struct {
	ID           int
	GroupID      int
	HostName     string
	FriendlyPath string
	Priority     int
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

// CreatePathGroup creates a new path group
func CreatePathGroup(db *sql.DB, name, description string, minCopies int, maxCopies *int) error {
	_, err := db.Exec(`
		INSERT INTO path_groups (name, description, min_copies, max_copies)
		VALUES ($1, $2, $3, $4)
	`, name, description, minCopies, maxCopies)
	return err
}

// GetPathGroup retrieves a path group by name
func GetPathGroup(db *sql.DB, name string) (*PathGroup, error) {
	group := &PathGroup{}
	err := db.QueryRow(`
		SELECT id, name, description, min_copies, max_copies, created_at
		FROM path_groups WHERE name = $1
	`, name).Scan(&group.ID, &group.Name, &group.Description, &group.MinCopies, &group.MaxCopies, &group.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("path group not found: %s", name)
	}
	return group, err
}

// ListPathGroups returns all path groups
func ListPathGroups(db *sql.DB) ([]PathGroup, error) {
	rows, err := db.Query(`
		SELECT id, name, description, min_copies, max_copies, created_at
		FROM path_groups ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []PathGroup
	for rows.Next() {
		var group PathGroup
		err := rows.Scan(&group.ID, &group.Name, &group.Description, &group.MinCopies, &group.MaxCopies, &group.CreatedAt)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

// DeletePathGroup deletes a path group
func DeletePathGroup(db *sql.DB, name string) error {
	result, err := db.Exec(`DELETE FROM path_groups WHERE name = $1`, name)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("path group not found: %s", name)
	}
	return nil
}

// AddPathToGroup adds a path to a group
func AddPathToGroup(db *sql.DB, groupName, hostName, friendlyPath string, priority int) error {
	// First get the group ID
	var groupID int
	err := db.QueryRow(`SELECT id FROM path_groups WHERE name = $1`, groupName).Scan(&groupID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("path group not found: %s", groupName)
		}
		return err
	}

	// Check if host exists
	var exists bool
	err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM hosts WHERE name = $1)`, hostName).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("host not found: %s", hostName)
	}

	// Check if the host has this friendly path
	host, err := GetHost(db, hostName)
	if err != nil {
		return err
	}
	paths, err := host.GetPaths()
	if err != nil {
		return err
	}
	if _, ok := paths[friendlyPath]; !ok {
		return fmt.Errorf("friendly path '%s' not found on host '%s'", friendlyPath, hostName)
	}

	// Add to group
	_, err = db.Exec(`
		INSERT INTO path_group_members (group_id, host_name, friendly_path, priority)
		VALUES ($1, $2, $3, $4)
	`, groupID, hostName, friendlyPath, priority)
	return err
}

// RemovePathFromGroup removes a path from its group
func RemovePathFromGroup(db *sql.DB, hostName, friendlyPath string) error {
	result, err := db.Exec(`
		DELETE FROM path_group_members
		WHERE host_name = $1 AND friendly_path = $2
	`, hostName, friendlyPath)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("path not found in any group: %s:%s", hostName, friendlyPath)
	}
	return nil
}

// ListGroupMembers returns all members of a path group
func ListGroupMembers(db *sql.DB, groupName string) ([]PathGroupMember, error) {
	rows, err := db.Query(`
		SELECT pgm.id, pgm.group_id, pgm.host_name, pgm.friendly_path, pgm.priority
		FROM path_group_members pgm
		JOIN path_groups pg ON pgm.group_id = pg.id
		WHERE pg.name = $1
		ORDER BY pgm.priority, pgm.host_name, pgm.friendly_path
	`, groupName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []PathGroupMember
	for rows.Next() {
		var member PathGroupMember
		err := rows.Scan(&member.ID, &member.GroupID, &member.HostName, &member.FriendlyPath, &member.Priority)
		if err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

// GetGroupForPath returns the group that contains a specific path
func GetGroupForPath(db *sql.DB, hostName, friendlyPath string) (*PathGroup, error) {
	group := &PathGroup{}
	err := db.QueryRow(`
		SELECT pg.id, pg.name, pg.description, pg.min_copies, pg.max_copies, pg.created_at
		FROM path_groups pg
		JOIN path_group_members pgm ON pg.id = pgm.group_id
		WHERE pgm.host_name = $1 AND pgm.friendly_path = $2
	`, hostName, friendlyPath).Scan(&group.ID, &group.Name, &group.Description, &group.MinCopies, &group.MaxCopies, &group.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no group found for path %s:%s", hostName, friendlyPath)
	}
	return group, err
}
