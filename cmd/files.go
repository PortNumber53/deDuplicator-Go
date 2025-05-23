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
	fmt.Printf("DEBUG: HandleFiles called with args: %v\n", args)
	var err error
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		cmd := FindCommand("files")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
		return fmt.Errorf("files command requires a subcommand: find, list-dupes, move-dupes, hash, prune, or import")
	}

	switch args[0] {
	case "import":
		importCmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		sourcePath := importCmd.String("source", "", "Source directory to import files from (required)")
		serverName := importCmd.String("server", "", "Target server to import files to (required)")
		friendlyPath := importCmd.String("path", "", "Target friendly path on the server to import files to (required)")
		importRemoveSource := importCmd.Bool("remove-source", false, "Remove source files after successful import")
		importDryRun := importCmd.Bool("dry-run", false, "Show what would be imported without making changes")
		importCount := importCmd.Int("count", 0, "Limit the number of files to process (0 = no limit)")
		err = importCmd.Parse(args[1:])
		if err != nil {
			return fmt.Errorf("error parsing command flags: %v", err)
		}
		if *sourcePath == "" || *serverName == "" || *friendlyPath == "" {
			fmt.Println("Usage: files import --source DIR --server NAME --path FRIENDLY [options]")
			return fmt.Errorf("--source, --server, and --path are required")
		}
		err = files.ImportFiles(ctx, database, files.ImportOptions{
			SourcePath:   *sourcePath,
			HostName:     *serverName,
			FriendlyPath: *friendlyPath,
			RemoveSource: *importRemoveSource,
			DryRun:       *importDryRun,
			Count:        *importCount,
		})
		if err != nil {
			fmt.Printf("Import error: %v\n", err)
		}
		return err

	case "prune":
		pruneCmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		pruneBatchSize := pruneCmd.Int("batch-size", 0, "Number of deletions per transaction commit (default: 250)")
		err = pruneCmd.Parse(args[1:])
		if err != nil {
			return fmt.Errorf("error parsing prune flags: %v", err)
		}
		pruneOpts := files.PruneOptions{BatchSize: *pruneBatchSize}
		err = files.PruneNonExistentFiles(ctx, database, pruneOpts)
		if err != nil {
			fmt.Printf("Prune error: %v\n", err)
		}
		return err

	case "find":
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				cmd := FindCommand("files find")
				if cmd != nil {
					ShowCommandHelp(*cmd)
					return nil
				}
				break
			}
		}

		// Parse find command flags
		findCmd := flag.NewFlagSet("find", flag.ExitOnError)
		findHost := findCmd.String("host", "", "Host to find files for (defaults to current host)")
		findPath := findCmd.String("path", "", "Friendly path name to find files for (optional)")

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
					return fmt.Errorf("no host found for hostname %s, please add it using 'deduplicator manage add' or specify --server", hostname)
				}
				return fmt.Errorf("error finding host: %v", err)
			}
		}

		return files.FindFiles(ctx, database, files.FindOptions{
			Server: hostName,
			Path:   *findPath,
		})

	case "hash":
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				cmd := FindCommand("files hash")
				if cmd != nil {
					ShowCommandHelp(*cmd)
					return nil
				}
				break
			}
		}

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
			Server:           hostName,
			Refresh:          *force,
			Renew:            *renew,
			RetryProblematic: *retryProblematic,
		})

	case "list-dupes":
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				cmd := FindCommand("files list-dupes")
				if cmd != nil {
					ShowCommandHelp(*cmd)
					return nil
				}
				break
			}
		}

		// Parse command flags
		cmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		count := cmd.Int("count", 0, "Limit the number of duplicate groups to show (0 = no limit)")
		minSize := cmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")
		destDir := cmd.String("dest", "", "Directory to move duplicates to (if specified)")
		run := cmd.Bool("run", false, "Actually move files (default is dry-run)")
		stripPrefix := cmd.String("strip-prefix", "", "Remove this prefix from paths when moving files")
		ignoreDestDir := cmd.Bool("ignore-dest", true, "Ignore files that are already in the destination directory")

		err = cmd.Parse(args[1:])
		if err != nil {
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
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				cmd := FindCommand("files move-dupes")
				if cmd != nil {
					ShowCommandHelp(*cmd)
					return nil
				}
				break
			}
		}

		// Parse command flags
		moveDupesCmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		target := moveDupesCmd.String("target", "", "Target directory to move duplicates to (required)")

		err = moveDupesCmd.Parse(args[1:])
		if err != nil {
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
		}

		importCmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		sourcePath := importCmd.String("source", "", "Source directory to import files from (required)")
		serverName := importCmd.String("server", "", "Target server to import files to (required)")
		friendlyPath := importCmd.String("path", "", "Target friendly path on the server to import files to (required)")
		importRemoveSource := importCmd.Bool("remove-source", false, "Remove source files after successful import")
		importDryRun := importCmd.Bool("dry-run", false, "Show what would be imported without making changes")
		importCount := importCmd.Int("count", 0, "Limit the number of files to process (0 = no limit)")
		err = importCmd.Parse(args[1:])
		if err != nil {
			return fmt.Errorf("error parsing command flags: %v", err)
		}
		if *sourcePath == "" || *serverName == "" || *friendlyPath == "" {
			fmt.Println("Usage: files import --source DIR --server NAME --path FRIENDLY [options]")
			return fmt.Errorf("--source, --server, and --path are required")
		}
		err = files.ImportFiles(ctx, database, files.ImportOptions{
			SourcePath:   *sourcePath,
			HostName:     *serverName,
			FriendlyPath: *friendlyPath,
			RemoveSource: *importRemoveSource,
			DryRun:       *importDryRun,
			Count:        *importCount,
		})
		if err != nil {
			fmt.Printf("Import error: %v\n", err)
		}
		return err

	case "mirror":
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				cmd := FindCommand("files mirror")
				if cmd != nil {
					ShowCommandHelp(*cmd)
					return nil
				}
				break
			}
		}

		if len(args) < 2 {
			return fmt.Errorf("files mirror requires a friendly path argument")
		}
		var friendlyPath string
		friendlyPath = args[1]
		return files.MirrorFriendlyPath(ctx, database, friendlyPath)

	default:
		return fmt.Errorf("unknown files subcommand: %s", args[0])
	}
}
