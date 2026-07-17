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

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *repeatedStringFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("value cannot be empty")
	}
	*f = append(*f, value)
	return nil
}

// HandleFiles handles file-related commands
func HandleFiles(ctx context.Context, database *sql.DB, args []string) error {
	var err error
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		cmd := FindCommand("files")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
		return fmt.Errorf("files command requires a subcommand: find, list-dupes, move-dupes, hash, hash-upgrade, prune, import, mirror, mirror-group, or dedupe-group")
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
		duplicateDir := importCmd.String("duplicate", "", "Move duplicate files to this directory instead of skipping them")
		importAge := importCmd.Int("age", 0, "Only import files older than this many minutes")
		err = importCmd.Parse(args[1:])
		if err != nil {
			return fmt.Errorf("error parsing command flags: %v", err)
		}
		if *sourcePath == "" || *serverName == "" || *friendlyPath == "" {
			fmt.Println("Import files from a source directory into the database")
			fmt.Println("")
			fmt.Println("Usage: files import --source DIR --server NAME --path FRIENDLY [options]")
			fmt.Println("")
			fmt.Println("Required flags:")
			fmt.Println("  --source string   Source directory to import files from")
			fmt.Println("  --server string   Target server name where files will be stored")
			fmt.Println("  --path string     Friendly path on the target server")
			fmt.Println("")
			fmt.Println("Options:")
			fmt.Println("  --duplicate string   Move duplicate files to this directory instead of skipping")
			fmt.Println("  --remove-source      Remove source files after successful import")
			fmt.Println("  --dry-run            Show what would be imported without making changes")
			fmt.Println("  --count int          Limit the number of files to process (0 = no limit, default: 0)")
			fmt.Println("  --age int            Only import files older than this many minutes")
			return fmt.Errorf("--source, --server, and --path are required")
		}
		err = files.ImportFiles(ctx, database, files.ImportOptions{
			SourcePath:   *sourcePath,
			HostName:     *serverName,
			FriendlyPath: *friendlyPath,
			RemoveSource: *importRemoveSource,
			DryRun:       *importDryRun,
			Count:        *importCount,
			DuplicateDir: *duplicateDir,
			Age:          *importAge,
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
		serverFlag := findCmd.String("server", "", "Host to find files for (defaults to current host)")
		pathNameFlag := findCmd.String("path", "", "Friendly path name to search within (optional)")

		err = findCmd.Parse(args[1:])
		if err != nil {
			return fmt.Errorf("error parsing find command flags: %v", err)
		}

		var serverToUse string
		if *serverFlag != "" {
			serverToUse = *serverFlag
		} else {
			// Default to current host if --server is not provided
			osHostname, err := os.Hostname()
			if err != nil {
				return fmt.Errorf("error getting current OS hostname: %v", err)
			}
			// Find the friendly server name from the database based on the OS hostname
			err = database.QueryRowContext(ctx, `SELECT name FROM hosts WHERE LOWER(hostname) = LOWER($1)`, strings.ToLower(osHostname)).Scan(&serverToUse)
			if err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("no host found in database for OS hostname '%s'. Please add it using 'manage server-add' or specify --server.", osHostname)
				}
				return fmt.Errorf("error querying host from database for OS hostname '%s': %v", osHostname, err)
			}
		}

		findOpts := files.FindOptions{
			Server: serverToUse,
		}

		if *pathNameFlag != "" {
			findOpts.Path = *pathNameFlag
		}

		// Call the actual find function from the files package
		err = files.FindFiles(ctx, database, findOpts)
		if err != nil {
			return fmt.Errorf("error executing find: %v", err)
		}
		return nil

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
		force := hashCmd.Bool("force", false, "Rehash selected files even if they already have a hash")
		renew := hashCmd.Bool("renew", false, "Recalculate hashes older than 1 week")
		retryProblematic := hashCmd.Bool("retry-problematic", false, "Retry files that previously timed out")
		fullHash := hashCmd.Bool("full-hash", false, "Hash full contents for all eligible files")
		largeFirst := hashCmd.Bool("large-first", false, "Process larger files before smaller files")
		var priorityPaths repeatedStringFlag
		hashCmd.Var(&priorityPaths, "path", "Friendly path or absolute root folder to process first (can be repeated)")
		_ = hashCmd.Int("count", 0, "Process only N files (0 = unlimited)")

		if err := hashCmd.Parse(args[1:]); err != nil {
			fmt.Printf("Error: failed to parse hash command flags: %v\n", err)
			return err
		}
		// Get hostname for current machine
		hostname, err := os.Hostname()
		if err != nil {
			fmt.Printf("Error: failed to get hostname: %v\n", err)
			return err
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
				fmt.Printf("Error: no host found for hostname '%s'. Please add it using 'deduplicator manage add'.\n", hostname)
				return err
			}
			fmt.Printf("Error: failed to find host in database: %v\n", err)
			return err
		}

		fmt.Printf("Hashing files for host: %s\n", hostName)
		err = files.HashFiles(ctx, database, files.HashOptions{
			Server:           hostName,
			Refresh:          *force,
			Renew:            *renew,
			RetryProblematic: *retryProblematic,
			FullHash:         *fullHash,
			LargeFirst:       *largeFirst,
			Paths:            []string(priorityPaths),
		})
		if err != nil {
			if strings.Contains(err.Error(), "no files need hashing") || strings.Contains(err.Error(), "No files need hashing") {
				fmt.Println("No files need hashing.")
				return nil
			}
			fmt.Printf("Error: %v\n", err)
			return err
		}
		fmt.Println("Hashing completed successfully.")
		return nil

	case "hash-upgrade":
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				cmd := FindCommand("files hash-upgrade")
				if cmd != nil {
					ShowCommandHelp(*cmd)
					return nil
				}
				break
			}
		}

		hashUpgradeCmd := flag.NewFlagSet("hash-upgrade", flag.ExitOnError)
		if err := hashUpgradeCmd.Parse(args[1:]); err != nil {
			return fmt.Errorf("error parsing hash-upgrade flags: %v", err)
		}
		if hashUpgradeCmd.NArg() != 0 {
			return fmt.Errorf("hash-upgrade does not accept arguments")
		}

		hostname, err := os.Hostname()
		if err != nil {
			fmt.Printf("Error: failed to get hostname: %v\n", err)
			return err
		}
		hostname = strings.ToLower(hostname)

		var hostName string
		err = database.QueryRow(`
				SELECT name
				FROM hosts
				WHERE LOWER(hostname) = LOWER($1)
			`, hostname).Scan(&hostName)
		if err != nil {
			if err == sql.ErrNoRows {
				fmt.Printf("Error: no host found for hostname '%s'. Please add it using 'deduplicator manage add'.\n", hostname)
				return err
			}
			fmt.Printf("Error: failed to find host in database: %v\n", err)
			return err
		}

		fmt.Printf("Upgrading recent hashes for host: %s\n", hostName)
		return files.UpgradeRecentHashes(ctx, database, files.HashUpgradeOptions{
			Server: hostName,
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
		dryRun := moveDupesCmd.Bool("dry-run", false, "Show what would be moved without making changes")
		count := moveDupesCmd.Int("count", 0, "Limit the number of duplicate sets to process (0 = no limit)")
		minSize := moveDupesCmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")

		err = moveDupesCmd.Parse(args[1:])
		if err != nil {
			return fmt.Errorf("error parsing command flags: %v", err)
		}

		if *target == "" {
			return fmt.Errorf("--target is required for move-dupes command")
		}

		var parsedMinSize int64
		if *minSize != "" {
			parsedMinSize, err = files.ParseSize(*minSize)
			if err != nil {
				return fmt.Errorf("error parsing min-size: %v", err)
			}
		}

		// Create move options
		moveOpts := files.MoveOptions{
			TargetDir: *target,
			DryRun:    *dryRun,
			Count:     *count,
		}

		// Call MoveDuplicates with the appropriate options
		dupOpts := files.DuplicateListOptions{
			Count:   *count,
			MinSize: parsedMinSize,
		}

		return files.MoveDuplicates(ctx, database, dupOpts, moveOpts)

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

	case "mirror-group":
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				cmd := FindCommand("files mirror-group")
				if cmd != nil {
					ShowCommandHelp(*cmd)
					return nil
				}
				break
			}
		}

		if len(args) < 2 {
			return fmt.Errorf("mirror-group requires a group name argument")
		}

		mirrorGroupCmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		dryRun := mirrorGroupCmd.Bool("dry-run", false, "Show what would be mirrored without transferring files")
		if err := mirrorGroupCmd.Parse(args[2:]); err != nil {
			return fmt.Errorf("error parsing mirror-group flags: %v", err)
		}

		return files.MirrorGroup(ctx, database, files.GroupMirrorOptions{
			GroupName: args[1],
			DryRun:    *dryRun,
		})

	case "dedupe-group":
		// Check for help flag
		for _, arg := range args[1:] {
			if arg == "--help" || arg == "help" {
				fmt.Println("Usage: deduplicator files dedupe-group <group name> [options]")
				fmt.Println("\nOptions:")
				fmt.Println("  --balance-mode <mode>  Balance mode: priority (default), equal, capacity")
				fmt.Println("  --respect-limits       Honor min/max copy limits from group settings")
				fmt.Println("  --dry-run              Show what would be done without making changes")
				fmt.Println("  --min-size <bytes>     Only process files larger than this size")
				fmt.Println("  --count <n>            Limit the number of duplicate groups to process")
				fmt.Println("  --run                  Actually perform the deduplication (opposite of dry-run)")
				return nil
			}
		}

		if len(args) < 2 {
			return fmt.Errorf("dedupe-group requires a group name argument")
		}

		groupName := args[1]
		balanceMode := "priority"
		respectLimits := false
		dryRun := true
		minSize := int64(0)
		count := 0

		for i := 2; i < len(args); i++ {
			switch args[i] {
			case "--balance-mode":
				if i+1 < len(args) {
					balanceMode = args[i+1]
					i++
				}
			case "--respect-limits":
				respectLimits = true
			case "--dry-run":
				dryRun = true
			case "--run":
				dryRun = false
			case "--min-size":
				if i+1 < len(args) {
					fmt.Sscanf(args[i+1], "%d", &minSize)
					i++
				}
			case "--count":
				if i+1 < len(args) {
					fmt.Sscanf(args[i+1], "%d", &count)
					i++
				}
			}
		}

		opts := files.GroupDedupeOptions{
			GroupName:     groupName,
			BalanceMode:   balanceMode,
			RespectLimits: respectLimits,
			DryRun:        dryRun,
			MinSize:       minSize,
			Count:         count,
		}

		return files.DeduplicateByGroup(ctx, database, opts)

	default:
		return fmt.Errorf("unknown files subcommand: %s", args[0])
	}
}
