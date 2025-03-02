package cmd

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	"deduplicator/files"
)

// HandleFiles handles file-related commands
func HandleFiles(ctx context.Context, database *sql.DB, args []string) error {
	if len(args) == 0 || args[0] == "help" {
		cmd := FindCommand("files")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
		return fmt.Errorf("files command requires a subcommand: find, list-dupes, move-dupes, or hash")
	}

	switch args[0] {
	case "find":
		// Parse find command flags
		findCmd := flag.NewFlagSet("find", flag.ExitOnError)
		findHost := findCmd.String("host", "", "Host to find files for (defaults to current host)")

		if err := findCmd.Parse(args[1:]); err != nil {
			return fmt.Errorf("error parsing find command flags: %v", err)
		}

		hostName := *findHost
		if hostName == "" {
			// Get hostname for current machine
			hostname, err := os.Hostname()
			if err != nil {
				return fmt.Errorf("error getting hostname: %v", err)
			}

			// Convert hostname to lowercase for consistency
			hostname = strings.ToLower(hostname)

			// Find host in database by hostname (case-insensitive)
			err = database.QueryRow(`
				SELECT name 
				FROM hosts 
				WHERE LOWER(hostname) = LOWER($1)
			`, hostname).Scan(&hostName)
			if err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("no host found for hostname %s, please add it using 'deduplicator manage add' or specify --host", hostname)
				}
				return fmt.Errorf("error finding host: %v", err)
			}
		}

		return files.FindFiles(ctx, database, files.FindOptions{
			Host: hostName,
		})

	case "hash":
		// Parse hash command flags
		hashCmd := flag.NewFlagSet("hash", flag.ExitOnError)
		force := hashCmd.Bool("force", false, "Rehash files even if they already have a hash")
		renew := hashCmd.Bool("renew", false, "Recalculate hashes older than 1 week")
		retryProblematic := hashCmd.Bool("retry-problematic", false, "Retry files that previously timed out")
		// count parameter is defined but not used in the HashFiles function
		_ = hashCmd.Int("count", 0, "Process only N files (0 = unlimited)")

		if err := hashCmd.Parse(args[1:]); err != nil {
			return fmt.Errorf("error parsing hash command flags: %v", err)
		}

		// Get hostname for current machine
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("error getting hostname: %v", err)
		}

		// Convert hostname to lowercase for consistency
		hostname = strings.ToLower(hostname)

		// Find host in database by hostname (case-insensitive)
		var hostName string
		err = database.QueryRow(`
			SELECT name 
			FROM hosts 
			WHERE LOWER(hostname) = LOWER($1)
		`, hostname).Scan(&hostName)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no host found for hostname %s, please add it using 'deduplicator manage add'", hostname)
			}
			return fmt.Errorf("error finding host: %v", err)
		}

		return files.HashFiles(ctx, database, files.HashOptions{
			Host:             hostName,
			Refresh:          *force,
			Renew:            *renew,
			RetryProblematic: *retryProblematic,
		})

	case "list-dupes":
		// Parse command flags
		cmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		count := cmd.Int("count", 0, "Limit the number of duplicate groups to show (0 = no limit)")
		minSize := cmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")
		destDir := cmd.String("dest", "", "Directory to move duplicates to (if specified)")
		run := cmd.Bool("run", false, "Actually move files (default is dry-run)")
		stripPrefix := cmd.String("strip-prefix", "", "Remove this prefix from paths when moving files")
		ignoreDestDir := cmd.Bool("ignore-dest", true, "Ignore files that are already in the destination directory")

		if err := cmd.Parse(args[1:]); err != nil {
			return fmt.Errorf("error parsing command flags: %v", err)
		}

		// Get hostname for current machine
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("error getting hostname: %v", err)
		}

		// Convert hostname to lowercase for consistency
		hostname = strings.ToLower(hostname)

		// Find host in database by hostname (case-insensitive)
		var hostName string
		err = database.QueryRow(`
			SELECT name 
			FROM hosts 
			WHERE LOWER(hostname) = LOWER($1)
		`, hostname).Scan(&hostName)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no host found for hostname %s, please add it using 'deduplicator manage add'", hostname)
			}
			return fmt.Errorf("error finding host: %v", err)
		}

		var parsedMinSize int64
		if *minSize != "" {
			var err error
			parsedMinSize, err = files.ParseSize(*minSize)
			if err != nil {
				fmt.Printf("Error parsing min-size: %v\n", err)
				os.Exit(1)
			}
		}

		// If dest directory is specified, use DedupFiles, otherwise use FindDuplicates
		if *destDir != "" {
			// Warn if --run is not specified
			if !*run {
				fmt.Println("Note: Running in dry-run mode. Use --run to actually move files.")
			}

			return files.DedupFiles(ctx, database, files.DedupeOptions{
				DryRun:        !*run,
				DestDir:       *destDir,
				StripPrefix:   *stripPrefix,
				Count:         *count,
				IgnoreDestDir: *ignoreDestDir,
				MinSize:       parsedMinSize,
			})
		} else {
			return files.FindDuplicates(ctx, database, files.DuplicateListOptions{
				Count:   *count,
				MinSize: parsedMinSize,
			})
		}

	case "move-dupes":
		// Parse command flags
		cmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		count := cmd.Int("count", 0, "Limit the number of duplicate groups to show (0 = no limit)")
		minSize := cmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")
		target := cmd.String("target", "", "Target directory to move duplicates to (required)")
		dryRun := cmd.Bool("dry-run", false, "Show what would be moved without making changes")

		if err := cmd.Parse(args[1:]); err != nil {
			return fmt.Errorf("error parsing command flags: %v", err)
		}

		if *target == "" {
			return fmt.Errorf("--target is required for move-dupes command")
		}

		// Get hostname for current machine
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("error getting hostname: %v", err)
		}

		// Convert hostname to lowercase for consistency
		hostname = strings.ToLower(hostname)

		// Find host in database by hostname (case-insensitive)
		var hostName string
		err = database.QueryRow(`
			SELECT name 
			FROM hosts 
			WHERE LOWER(hostname) = LOWER($1)
		`, hostname).Scan(&hostName)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no host found for hostname %s, please add it using 'deduplicator manage add'", hostname)
			}
			return fmt.Errorf("error finding host: %v", err)
		}

		var parsedMinSize int64
		if *minSize != "" {
			var err error
			parsedMinSize, err = files.ParseSize(*minSize)
			if err != nil {
				fmt.Printf("Error parsing min-size: %v\n", err)
				os.Exit(1)
			}
		}

		return files.MoveDuplicates(ctx, database, files.DuplicateListOptions{
			Count:   *count,
			MinSize: parsedMinSize,
		}, files.MoveOptions{
			TargetDir: *target,
			DryRun:    *dryRun,
		})

	default:
		return fmt.Errorf("unknown files subcommand: %s", args[0])
	}
}
