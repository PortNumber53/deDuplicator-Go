package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type Host struct {
	ID        int
	Name      string
	Hostname  string
	IP        string
	RootPath  string
	CreatedAt time.Time
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
			host TEXT NOT NULL,
			hash TEXT,
			size BIGINT,
			last_hashed_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(path, host),
			FOREIGN KEY (host) REFERENCES hosts(name)
		)
	`)
	if err != nil {
		return fmt.Errorf("error creating files table: %v", err)
	}

	fmt.Println("Database setup completed successfully")
	return nil
}

// AddHost adds a new host to the database
func AddHost(db *sql.DB, name, hostname, ip, rootPath string) error {
	_, err := db.Exec(`
		INSERT INTO hosts (name, hostname, ip, root_path)
		VALUES ($1, $2, $3, $4)
	`, name, hostname, ip, rootPath)
	return err
}

// UpdateHost updates an existing host in the database
func UpdateHost(db *sql.DB, name, hostname, ip, rootPath string) error {
	result, err := db.Exec(`
		UPDATE hosts 
		SET hostname = $2, ip = $3, root_path = $4
		WHERE name = $1
	`, name, hostname, ip, rootPath)
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

// GetHost retrieves a host by name
func GetHost(db *sql.DB, name string) (*Host, error) {
	host := &Host{}
	err := db.QueryRow(`
		SELECT id, name, hostname, ip, root_path, created_at 
		FROM hosts WHERE name = $1
	`, name).Scan(&host.ID, &host.Name, &host.Hostname, &host.IP, &host.RootPath, &host.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("host not found: %s", name)
	}
	return host, err
}

// ListHosts returns all hosts in the database
func ListHosts(db *sql.DB) ([]Host, error) {
	rows, err := db.Query(`
		SELECT id, name, hostname, ip, root_path, created_at 
		FROM hosts ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hosts []Host
	for rows.Next() {
		var host Host
		err := rows.Scan(&host.ID, &host.Name, &host.Hostname, &host.IP, &host.RootPath, &host.CreatedAt)
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
