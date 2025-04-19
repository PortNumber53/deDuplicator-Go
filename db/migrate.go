package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MigrateDatabase handles database migrations
func MigrateDatabase(db *sql.DB) error {
	log.Println("Running database migrations...")
	start := time.Now()

	// First, ensure migrations table exists
	if err := createMigrationsTable(db); err != nil {
		return fmt.Errorf("error creating migrations table: %v", err)
	}

	// Get list of migration files
	files, err := filepath.Glob("migrations/*.sql")
	if err != nil {
		return fmt.Errorf("error finding migration files: %v", err)
	}

	// Apply each migration
	for _, file := range files {
		filename := filepath.Base(file)
		if strings.HasSuffix(filename, ".up.sql") {
			applied, err := isMigrationApplied(db, filename)
			if err != nil {
				return fmt.Errorf("error checking migration status: %v", err)
			}

			if !applied {
				if err := applyMigration(db, file, filename); err != nil {
					return fmt.Errorf("error applying migration %s: %v", filename, err)
				}
			}
		}
	}

	log.Printf("Database migrations completed successfully (took %dms)", time.Since(start).Milliseconds())
	return nil
}

func createMigrationsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS migrations (
			id SERIAL PRIMARY KEY,
			filename VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`
	_, err := db.Exec(query)
	return err
}

func isMigrationApplied(db *sql.DB, filename string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM migrations WHERE filename = $1)`
	err := db.QueryRow(query, filename).Scan(&exists)
	return exists, err
}

func applyMigration(db *sql.DB, filepath, filename string) error {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Apply the migration
	if _, err := tx.Exec(string(content)); err != nil {
		return err
	}

	// Record the migration
	if _, err := tx.Exec(`INSERT INTO migrations (filename) VALUES ($1)`, filename); err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Applied migration: %s", filename)
	return nil
}

// RollbackLastMigration rolls back the last migration
func RollbackLastMigration(db *sql.DB) error {
	log.Println("Rolling back last migration...")
	start := time.Now()

	// Get the last applied migration
	var filename string
	err := db.QueryRow(`
		SELECT filename 
		FROM migrations 
		ORDER BY applied_at DESC 
		LIMIT 1
	`).Scan(&filename)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Println("No migrations to roll back")
			return nil
		}
		return fmt.Errorf("error getting last migration: %v", err)
	}

	// Get the corresponding down migration file
	downFile := strings.Replace(filename, ".up.sql", ".down.sql", 1)
	content, err := os.ReadFile(filepath.Join("migrations", downFile))
	if err != nil {
		return fmt.Errorf("error reading down migration %s: %v", downFile, err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Apply the down migration
	if _, err := tx.Exec(string(content)); err != nil {
		return err
	}

	// Remove the migration record
	if _, err := tx.Exec(`DELETE FROM migrations WHERE filename = $1`, filename); err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Migration rollback completed successfully (took %dms)", time.Since(start).Milliseconds())
	return nil
}

// StatusMigrations prints the status of all migrations (applied, pending, or missing)
func StatusMigrations(db *sql.DB) error {
	// List all .up.sql migration files
	files, err := filepath.Glob("migrations/*.up.sql")
	if err != nil {
		return fmt.Errorf("error finding migration files: %v", err)
	}

	// Query all applied migrations from DB
	rows, err := db.Query(`SELECT filename FROM migrations`)
	if err != nil {
		return fmt.Errorf("error querying migrations table: %v", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var fname string
		if err := rows.Scan(&fname); err != nil {
			return fmt.Errorf("error scanning migration row: %v", err)
		}
		applied[fname] = true
	}

	fmt.Println("Migration Status:")
	fmt.Println("------------------")
	// Track which applied migrations are missing from code
	appliedButMissing := make([]string, 0)
	for fname := range applied {
		found := false
		for _, f := range files {
			if filepath.Base(f) == fname {
				found = true
				break
			}
		}
		if !found {
			appliedButMissing = append(appliedButMissing, fname)
		}
	}

	for _, file := range files {
		base := filepath.Base(file)
		if applied[base] {
			fmt.Printf("[applied] %s\n", base)
		} else {
			fmt.Printf("[pending] %s\n", base)
		}
	}
	for _, missing := range appliedButMissing {
		fmt.Printf("[missing in code] %s\n", missing)
	}

	return nil
}

// ResetDatabase drops all tables and reapplies all migrations
func ResetDatabase(db *sql.DB) error {
	log.Println("Resetting database...")
	start := time.Now()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Drop all tables
	_, err = tx.Exec(`
		DO $$ DECLARE
			r RECORD;
		BEGIN
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
				EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
			END LOOP;
		END $$;
	`)
	if err != nil {
		return fmt.Errorf("error dropping tables: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	// Run migrations
	if err := MigrateDatabase(db); err != nil {
		return fmt.Errorf("error running migrations: %v", err)
	}

	log.Printf("Database reset completed successfully (took %dms)", time.Since(start).Milliseconds())
	return nil
}
