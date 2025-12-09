package cmd

import (
	"database/sql"
	"fmt"

	"deduplicator/db"
)

// HandleMigrate handles database migration commands
func HandleMigrate(database *sql.DB, args []string) error {
	verbose, trimmedArgs := extractMigrateFlags(args)
	if len(trimmedArgs) < 1 {
		cmd := FindCommand("migrate")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
		return nil
	}

	// Check for help flag
	if trimmedArgs[0] == "help" || trimmedArgs[0] == "--help" {
		cmd := FindCommand("migrate")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
	}

	// Check for help flag in subcommands
	if len(trimmedArgs) > 1 && (trimmedArgs[1] == "help" || trimmedArgs[1] == "--help") {
		cmd := FindCommand("migrate")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
	}

	subcommand := trimmedArgs[0]

	if verbose {
		info := currentDBInfo()
		fmt.Printf("VERBOSE: migrate %s (db=%s@%s:%s/%s)\n", subcommand, info.User, info.Host, info.Port, info.Name)
	}

	switch subcommand {
	case "up":
		if err := db.MigrateDatabase(database); err != nil {
			return wrapMigrateErr(verbose, err)
		}
		return nil
	case "down":
		if err := db.RollbackLastMigration(database); err != nil {
			return wrapMigrateErr(verbose, err)
		}
		return nil
	case "reset":
		if err := db.ResetDatabase(database); err != nil {
			return wrapMigrateErr(verbose, err)
		}
		return nil
	case "status":
		if err := db.StatusMigrations(database); err != nil {
			return wrapMigrateErr(verbose, err)
		}
		return nil
	default:
		return fmt.Errorf("unknown migrate subcommand: %s", subcommand)
	}
}

// extractMigrateFlags removes migrate-level flags (currently --verbose/-v) and
// returns the flag state along with the remaining args.
func extractMigrateFlags(args []string) (bool, []string) {
	verbose := false
	remaining := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "--verbose" || arg == "-v" {
			verbose = true
			continue
		}
		remaining = append(remaining, arg)
	}
	return verbose, remaining
}

func wrapMigrateErr(verbose bool, err error) error {
	if !verbose {
		return err
	}
	info := currentDBInfo()
	return fmt.Errorf("%v (db=%s@%s:%s/%s)", err, info.User, info.Host, info.Port, info.Name)
}
