package cmd

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	"deduplicator/files"
	"log"
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

	case "list", "move-dupes":
		// Parse command flags
		cmd := flag.NewFlagSet(args[0], flag.ExitOnError)
		host := cmd.String("host", "", "Specific host to check for duplicates")
		allHosts := cmd.Bool("all-hosts", false, "Check duplicates across all hosts")
		count := cmd.Int("count", 0, "Limit the number of duplicate groups to show (0 = no limit)")
		minSize := cmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")

		// Additional flags for move-dupes
		var target string
		var dryRun bool
		if args[0] == "move-dupes" {
			cmd.StringVar(&target, "target", "", "Target directory to move duplicates to (required)")
			cmd.BoolVar(&dryRun, "dry-run", false, "Show what would be moved without making changes")
		}

		if err := cmd.Parse(args[1:]); err != nil {
			return fmt.Errorf("error parsing command flags: %v", err)
		}

		if args[0] == "move-dupes" && target == "" {
			return fmt.Errorf("--target is required for move-dupes command")
		}

		if *host != "" && *allHosts {
			fmt.Println("Error: Cannot specify both --host and --all-hosts")
			os.Exit(1)
		}

		// If no host specified and not all hosts, use current hostname
		if *host == "" && !*allHosts {
			// Get hostname for current machine
			hostname, err := os.Hostname()
			if err != nil {
				return fmt.Errorf("error getting hostname: %v", err)
			}
			hostname = strings.ToLower(hostname)
			log.Printf("Looking up host for hostname: %s", hostname)

			// Find host in database by hostname (case-insensitive)
			err = database.QueryRow(`
				SELECT hostname
				FROM hosts
				WHERE LOWER(hostname) = LOWER($1)
			`, hostname).Scan(host)
			if err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add' or specify --host", hostname)
				}
				return fmt.Errorf("error finding host: %v", err)
			}
			log.Printf("Found host: %s", *host)
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

		opts := files.DuplicateListOptions{
			Host:     *host,
			AllHosts: *allHosts,
			Count:    *count,
			MinSize:  parsedMinSize,
			Colors: files.ColorOptions{
				HeaderColor: "\033[33m", // Yellow
				FileColor:   "\033[90m", // Dark gray
				ResetColor:  "\033[0m",  // Reset
			},
		}

		if args[0] == "list" {
			return files.FindDuplicates(ctx, database, opts)
		} else {
			return files.MoveDuplicates(ctx, database, opts, files.MoveOptions{
				TargetDir: target,
				DryRun:    dryRun,
			})
		}

	default:
		return fmt.Errorf("unknown files subcommand: %s", args[0])
	}
}
