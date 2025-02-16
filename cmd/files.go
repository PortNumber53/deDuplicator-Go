package cmd

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	"deduplicator/files"
)

// HandleFiles handles file-related commands
func HandleFiles(ctx context.Context, database *sql.DB, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("files command requires a subcommand: find or list")
	}

	switch args[1] {
	case "find":
		if len(args) < 3 {
			return fmt.Errorf("find command requires a host name")
		}
		hostName := args[2]
		return files.FindFiles(ctx, database, files.FindOptions{
			Host: hostName,
		})
	case "list":
		// Parse list command flags
		listCmd := flag.NewFlagSet("list", flag.ExitOnError)
		listHost := listCmd.String("host", "", "Specific host to check for duplicates")
		listAllHosts := listCmd.Bool("all-hosts", false, "Check duplicates across all hosts")
		listCount := listCmd.Int("count", 0, "Limit the number of duplicate groups to show (0 = no limit)")
		listMinSize := listCmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")

		if err := listCmd.Parse(args[2:]); err != nil {
			return fmt.Errorf("error parsing list command flags: %v", err)
		}

		if *listHost != "" && *listAllHosts {
			fmt.Println("Error: Cannot specify both --host and --all-hosts")
			os.Exit(1)
		}

		var minSize int64
		if *listMinSize != "" {
			var err error
			minSize, err = files.ParseSize(*listMinSize)
			if err != nil {
				fmt.Printf("Error parsing min-size: %v\n", err)
				os.Exit(1)
			}
		}

		return files.FindDuplicates(ctx, database, files.DuplicateListOptions{
			Host:     *listHost,
			AllHosts: *listAllHosts,
			Count:    *listCount,
			MinSize:  minSize,
			Colors: files.ColorOptions{
				HeaderColor: "\033[33m", // Yellow
				FileColor:   "\033[90m", // Dark gray
				ResetColor:  "\033[0m",  // Reset
			},
		})
	default:
		return fmt.Errorf("unknown files subcommand: %s", args[1])
	}
}
