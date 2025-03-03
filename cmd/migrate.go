package cmd

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"

	"deduplicator/db"
)

// HandleMigrate handles database migration commands
func HandleMigrate(database *sql.DB, args []string) error {
	if len(args) < 1 {
		cmd := FindCommand("migrate")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
		return nil
	}

	// Check for help flag
	if args[0] == "help" || args[0] == "--help" {
		cmd := FindCommand("migrate")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
	}

	// Check for help flag in subcommands
	if len(args) > 1 && (args[1] == "help" || args[1] == "--help") {
		cmd := FindCommand("migrate")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
	}

	subcommand := args[0]
	switch subcommand {
	case "up":
		return db.MigrateDatabase(database)
	case "down":
		return db.RollbackLastMigration(database)
	case "reset":
		return db.ResetDatabase(database)
	case "status":
		driver, err := postgres.WithInstance(database, &postgres.Config{})
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

		version, dirty, err := m.Version()
		if err != nil {
			if errors.Is(err, migrate.ErrNilVersion) {
				fmt.Println("No migrations have been applied")
				return nil
			}
			return fmt.Errorf("could not get migration version: %v", err)
		}

		fmt.Printf("Current migration version: %d (dirty: %v)\n", version, dirty)
		return nil
	default:
		return fmt.Errorf("unknown migrate subcommand: %s", subcommand)
	}
}
