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
