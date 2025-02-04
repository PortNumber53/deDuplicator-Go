package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func CreateDatabase(db *sql.DB, force bool) error {
	if force {
		fmt.Println("Force flag enabled: dropping existing table...")
		_, err := db.Exec(`DROP TABLE IF EXISTS files`)
		if err != nil {
			return fmt.Errorf("error dropping table: %v", err)
		}
	}

	fmt.Println("Creating files table...")
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			id SERIAL PRIMARY KEY,
			path TEXT NOT NULL,
			host TEXT NOT NULL,
			hash TEXT,
			size BIGINT,
			last_hashed_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(path, host)
		)
	`)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	fmt.Println("Database setup completed successfully")
	return nil
}

func Connect(host, port, user, password, dbname string) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", 
		host, port, user, dbname)
	if password != "" {
		connStr += fmt.Sprintf(" password=%s", password)
	}

	return sql.Open("postgres", connStr)
}
