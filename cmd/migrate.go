package cmd

import (
	"database/sql"
	"fmt"

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
		return db.StatusMigrations(database)
	default:
		return fmt.Errorf("unknown migrate subcommand: %s", subcommand)
	}
}
