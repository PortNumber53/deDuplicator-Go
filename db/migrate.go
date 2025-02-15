package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// MigrateDatabase handles database migrations
func MigrateDatabase(db *sql.DB) error {
	log.Println("Running database migrations...")

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("could not create database driver: %v", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		"postgres",
		driver,
	)
	if err != nil {
		return fmt.Errorf("could not create migrate instance: %v", err)
	}

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			log.Println("No migrations to run")
			return nil
		}
		return fmt.Errorf("could not run migrations: %v", err)
	}

	log.Println("Database migrations completed successfully")
	return nil
}

// RollbackLastMigration rolls back the last migration
func RollbackLastMigration(db *sql.DB) error {
	log.Println("Rolling back last migration...")

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("could not create database driver: %v", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		"postgres",
		driver,
	)
	if err != nil {
		return fmt.Errorf("could not create migrate instance: %v", err)
	}

	if err := m.Steps(-1); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			log.Println("No migrations to roll back")
			return nil
		}
		return fmt.Errorf("could not roll back migration: %v", err)
	}

	log.Println("Migration rollback completed successfully")
	return nil
}

// ResetDatabase drops all tables and reapplies all migrations
func ResetDatabase(db *sql.DB) error {
	log.Println("Resetting database...")

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("could not create database driver: %v", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		"postgres",
		driver,
	)
	if err != nil {
		return fmt.Errorf("could not create migrate instance: %v", err)
	}

	if err := m.Drop(); err != nil {
		return fmt.Errorf("could not drop database: %v", err)
	}

	if err := m.Up(); err != nil {
		return fmt.Errorf("could not run migrations: %v", err)
	}

	log.Println("Database reset completed successfully")
	return nil
}
