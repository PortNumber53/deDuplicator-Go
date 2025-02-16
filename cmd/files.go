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
	if len(args) == 0 || args[0] == "help" {
		cmd := FindCommand("files")
		if cmd != nil {
			ShowCommandHelp(*cmd)
			return nil
		}
		return fmt.Errorf("files command requires a subcommand: find or list")
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

			// Find host in database by hostname
			err = database.QueryRow(`
				SELECT name 
				FROM hosts 
				WHERE hostname = $1
			`, hostname).Scan(&hostName)
			if err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add' or specify --host", hostname)
				}
				return fmt.Errorf("error finding host: %v", err)
			}
		}

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

		if err := listCmd.Parse(args[1:]); err != nil {
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
		return fmt.Errorf("unknown files subcommand: %s", args[0])
	}
}
