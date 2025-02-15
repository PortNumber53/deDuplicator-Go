package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"
	"github.com/joho/godotenv"

	"deduplicator/db"
	"deduplicator/files"
	"deduplicator/lock"
	"deduplicator/mq"
)

// VERSION represents the current version of the deduplicator tool
const VERSION = "1.0.0"

// Command represents a subcommand with its description and usage
type Command struct {
	Name        string
	Description string
	Usage       string
	Help        string   // Detailed help text
	Examples    []string // Example usages
}

// Available commands
var commands = []Command{
	{
		Name:        "migrate",
		Description: "Run database migrations",
		Usage:       "migrate [up|down|reset|status]",
		Help: `Manage database migrations for schema changes.

Subcommands:
  up     - Apply all pending migrations
  down   - Roll back the last applied migration
  reset  - Drop all tables and reapply migrations
  status - Show current migration status

The migrations are applied in order based on the numeric prefix of the migration files.`,
		Examples: []string{
			"dedupe migrate up",
			"dedupe migrate down",
			"dedupe migrate reset",
			"dedupe migrate status",
		},
	},
	{
		Name:        "createdb",
		Description: "Initialize or recreate the database schema (deprecated, use migrate instead)",
		Usage:       "createdb [--force]",
		Help: `Initialize or recreate the database schema.

Options:
  --force  Force recreation of tables by dropping existing ones

Note: This command is deprecated. Please use 'migrate up' instead.`,
		Examples: []string{
			"dedupe createdb",
			"dedupe createdb --force",
		},
	},
	{
		Name:        "manage",
		Description: "Manage backup hosts (add/edit/delete/list)",
		Usage:       "manage [add|edit|delete|list] [options]",
		Help: `Manage backup hosts in the system.

Subcommands:
  list           - List all registered hosts
  add            - Add a new host
  edit           - Edit an existing host
  delete         - Remove a host

Arguments for add/edit:
  <name>         - Unique identifier for the host
  <hostname>     - DNS hostname or IP address
  <ip>           - IP address (optional)
  <root_path>    - Base directory for file scanning`,
		Examples: []string{
			"dedupe manage list",
			"dedupe manage add myhost example.com 192.168.1.100 /data",
			"dedupe manage edit myhost newhost.com 192.168.1.101 /backup",
			"dedupe manage delete myhost",
		},
	},
	{
		Name:        "update",
		Description: "Process file paths from stdin and update the database",
		Usage:       "update < file_list.txt",
		Help: `Update the database with file paths from standard input.

Each line from stdin should contain a single file path. The paths will be
associated with the current host and stored in the database for deduplication.`,
		Examples: []string{
			"find /data -type f | dedupe update",
			"cat file_list.txt | dedupe update",
		},
	},
	{
		Name:        "hash",
		Description: "Calculate and update file hashes in the database",
		Usage:       "hash [--force] [--count N]",
		Help: `Calculate and store file hashes for deduplication.

Options:
  --force        Rehash files even if they already have a hash
  --count N      Process only N files (0 = unlimited)

Files are hashed using SHA256 for reliable duplicate detection.`,
		Examples: []string{
			"dedupe hash",
			"dedupe hash --force",
			"dedupe hash --count 1000",
		},
	},
	{
		Name:        "list",
		Description: "List duplicate files",
		Usage:       "list [--host HOST] [--all-hosts] [--count N]",
		Help: `List duplicate files in the system.

Options:
  --host HOST    Only show duplicates for specific host
  --all-hosts    Show duplicates across all hosts
  --count N      Limit output to N duplicate groups (0 = unlimited)

Files are considered duplicates if they have the same hash value.`,
		Examples: []string{
			"dedupe list",
			"dedupe list --host myserver",
			"dedupe list --all-hosts",
			"dedupe list --count 10",
		},
	},
	{
		Name:        "prune",
		Description: "Remove entries for files that no longer exist",
		Usage:       "prune [--host HOST] [--all-hosts]",
		Help: `Remove database entries for files that no longer exist on disk.

Options:
  --host HOST    Only prune files from specific host
  --all-hosts    Prune files across all hosts (requires --i-am-sure)

This command helps keep the database in sync with the actual filesystem.`,
		Examples: []string{
			"dedupe prune",
			"dedupe prune --host myserver",
			"dedupe prune --all-hosts --i-am-sure",
		},
	},
	{
		Name:        "organize",
		Description: "Organize duplicate files by moving them",
		Usage:       "organize [--host HOST] [--all-hosts] [--run] [--move DIR] [--strip-prefix PREFIX]",
		Help: `Organize duplicate files by moving them to a new location.

Options:
  --host HOST          Only organize files from specific host
  --all-hosts         Organize files across all hosts
  --run               Actually move files (default is dry-run)
  --move DIR          Move duplicates to this directory
  --strip-prefix PREFIX  Remove prefix from paths when moving

By default, this runs in dry-run mode and only shows what would be done.`,
		Examples: []string{
			"dedupe organize --move /backup/dupes",
			"dedupe organize --host myserver --run",
			"dedupe organize --all-hosts --strip-prefix /data",
		},
	},
	{
		Name:        "dedupe",
		Description: "Move duplicate files to a destination directory",
		Usage:       "dedupe --dest DIR [--run] [--strip-prefix PREFIX] [--count N]",
		Help: `Move duplicate files to a destination directory.

Options:
  --dest DIR          Directory to move duplicates to (required)
  --run              Actually move files (default is dry-run)
  --strip-prefix PREFIX  Remove prefix from paths when moving
  --count N          Process only N duplicate groups (0 = unlimited)
  --ignore-dest      Ignore files already in destination (default: true)

By default, this runs in dry-run mode and only shows what would be done.`,
		Examples: []string{
			"dedupe dedupe --dest /backup/dupes",
			"dedupe dedupe --dest /backup/dupes --run",
			"dedupe dedupe --dest /backup/dupes --strip-prefix /data",
		},
	},
	{
		Name:        "listen",
		Description: "Listen for version update messages from RabbitMQ",
		Usage:       "listen",
		Help: `Listen for version update messages from RabbitMQ.

This command connects to RabbitMQ and waits for version update notifications.
When a new version is published, the process will exit gracefully.

Requires RabbitMQ environment variables to be set.`,
		Examples: []string{
			"dedupe listen",
		},
	},
	{
		Name:        "queue version",
		Description: "Publish a version update message to notify running instances",
		Usage:       "queue version [--version VERSION]",
		Help: `Publish a version update message to notify running instances.

Options:
  --version VERSION   Version number to publish (defaults to current version)

This command publishes a message to RabbitMQ that will notify all listening
instances to shut down gracefully.

Requires RabbitMQ environment variables to be set.`,
		Examples: []string{
			"dedupe queue version",
			"dedupe queue version --version 1.1.0",
		},
	},
}

func printUsage() {
	fmt.Printf("Deduplicator %s - A tool for finding and managing duplicate files\n\n", VERSION)
	fmt.Println("Usage:")
	fmt.Println("  dedupe <command> [options]\n")
	fmt.Println("Available Commands:")

	// Find the longest command name for padding
	maxLen := 0
	for _, cmd := range commands {
		if len(cmd.Name) > maxLen {
			maxLen = len(cmd.Name)
		}
	}

	// Print each command with aligned descriptions
	for _, cmd := range commands {
		fmt.Printf("  %-*s  %s\n", maxLen, cmd.Name, cmd.Description)
	}

	fmt.Println("\nDetailed Usage:")
	for _, cmd := range commands {
		fmt.Printf("  dedupe %s\n", cmd.Usage)
	}

	fmt.Println("\nEnvironment Variables:")
	fmt.Println("  DB_HOST          PostgreSQL host (default: localhost)")
	fmt.Println("  DB_PORT          PostgreSQL port (default: 5432)")
	fmt.Println("  DB_USER          PostgreSQL user (default: postgres)")
	fmt.Println("  DB_PASSWORD      PostgreSQL password")
	fmt.Println("  DB_NAME          PostgreSQL database name (default: deduplicator)")
	fmt.Println("  RABBITMQ_HOST    RabbitMQ host (optional)")
	fmt.Println("  RABBITMQ_PORT    RabbitMQ port (default: 5672)")
	fmt.Println("  RABBITMQ_VHOST   RabbitMQ vhost")
	fmt.Println("  RABBITMQ_USER    RabbitMQ username")
	fmt.Println("  RABBITMQ_PASSWORD RabbitMQ password")
	fmt.Println("  RABBITMQ_QUEUE   RabbitMQ queue name (default: dedup_backup)")
}

func showCommandHelp(cmd Command) {
	fmt.Printf("\nCommand: %s - %s\n\n", cmd.Name, cmd.Description)
	fmt.Printf("Usage:\n  dedupe %s\n\n", cmd.Usage)
	fmt.Println(cmd.Help)
	if len(cmd.Examples) > 0 {
		fmt.Println("\nExamples:")
		for _, example := range cmd.Examples {
			fmt.Printf("  %s\n", example)
		}
	}
	fmt.Println()
}

func findCommand(name string) *Command {
	for _, cmd := range commands {
		if cmd.Name == name {
			return &cmd
		}
	}
	return nil
}

func handleManage(dbConn *sql.DB, args []string) error {
	if len(args) < 1 {
		cmd := findCommand("manage")
		if cmd != nil {
			showCommandHelp(*cmd)
			return nil
		}
		return fmt.Errorf("manage command requires a subcommand: add, edit, delete, or list")
	}

	if args[0] == "help" {
		cmd := findCommand("manage")
		if cmd != nil {
			showCommandHelp(*cmd)
			return nil
		}
	}

	subcommand := args[0]
	switch subcommand {
	case "list":
		hosts, err := db.ListHosts(dbConn)
		if err != nil {
			return fmt.Errorf("error listing hosts: %v", err)
		}
		if len(hosts) == 0 {
			fmt.Println("No hosts found. Use 'dedupe manage add' to add a host.")
			return nil
		}
		fmt.Printf("%-20s %-30s %-15s %s\n", "NAME", "HOSTNAME", "IP", "ROOT PATH")
		fmt.Println(strings.Repeat("-", 80))
		for _, host := range hosts {
			fmt.Printf("%-20s %-30s %-15s %s\n", host.Name, host.Hostname, host.IP, host.RootPath)
		}
		return nil

	case "add", "edit":
		if len(args) != 5 {
			fmt.Printf("Usage: dedupe manage %s <name> <hostname> <ip> <root_path>\n", subcommand)
			fmt.Printf("\nExample:\n  dedupe manage %s myhost example.com 192.168.1.100 /data\n", subcommand)
			return nil
		}
		name, hostname, ip, rootPath := args[1], args[2], args[3], args[4]

		if subcommand == "add" {
			err := db.AddHost(dbConn, name, hostname, ip, rootPath)
			if err != nil {
				return fmt.Errorf("error adding host: %v", err)
			}
			fmt.Printf("Host '%s' added successfully\n", name)
		} else {
			err := db.UpdateHost(dbConn, name, hostname, ip, rootPath)
			if err != nil {
				return fmt.Errorf("error updating host: %v", err)
			}
			fmt.Printf("Host '%s' updated successfully\n", name)
		}
		return nil

	case "delete":
		if len(args) != 2 {
			fmt.Println("Usage: dedupe manage delete <name>")
			fmt.Println("\nExample:\n  dedupe manage delete myhost")
			return nil
		}
		name := args[1]
		err := db.DeleteHost(dbConn, name)
		if err != nil {
			return fmt.Errorf("error deleting host: %v", err)
		}
		fmt.Printf("Host '%s' deleted successfully\n", name)
		return nil

	default:
		return fmt.Errorf("unknown subcommand: %s", subcommand)
	}
}

func handleMigrate(database *sql.DB, args []string) error {
	if len(args) < 1 {
		cmd := findCommand("migrate")
		if cmd != nil {
			showCommandHelp(*cmd)
			return nil
		}
		return fmt.Errorf("migrate command requires a subcommand: up, down, or reset")
	}

	if args[0] == "help" {
		cmd := findCommand("migrate")
		if cmd != nil {
			showCommandHelp(*cmd)
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

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Check for help command
	if os.Args[1] == "help" {
		if len(os.Args) == 2 {
			printUsage()
			os.Exit(0)
		}
		cmd := findCommand(os.Args[2])
		if cmd != nil {
			showCommandHelp(*cmd)
			os.Exit(0)
		}
		fmt.Printf("Unknown command: %s\n", os.Args[2])
		os.Exit(1)
	}

	// Check if command exists and if help is requested
	if len(os.Args) > 2 && os.Args[2] == "help" {
		cmd := findCommand(os.Args[1])
		if cmd != nil {
			showCommandHelp(*cmd)
			os.Exit(0)
		}
	}

	// Create context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Command line flags
	migrateCmd := flag.NewFlagSet("migrate", flag.ExitOnError)

	createdbCmd := flag.NewFlagSet("createdb", flag.ExitOnError)
	createdbForce := createdbCmd.Bool("force", false, "Force recreation of tables")

	updateCmd := flag.NewFlagSet("update", flag.ExitOnError)

	hashCmd := flag.NewFlagSet("hash", flag.ExitOnError)
	hashForce := hashCmd.Bool("force", false, "Force rehash of all files")
	hashCount := hashCmd.Int("count", 0, "Number of files to process")

	listCmd := flag.NewFlagSet("list", flag.ExitOnError)
	listHost := listCmd.String("host", "", "Specific host to check for duplicates")
	listAllHosts := listCmd.Bool("all-hosts", false, "Check duplicates across all hosts")
	listCount := listCmd.Int("count", 0, "Limit the number of duplicate groups to show (0 = no limit)")

	listenCmd := flag.NewFlagSet("listen", flag.ExitOnError)

	queueVersionCmd := flag.NewFlagSet("version", flag.ExitOnError)
	queueVersionValue := queueVersionCmd.String("version", VERSION, "Version number to publish (defaults to current version)")

	pruneCmd := flag.NewFlagSet("prune", flag.ExitOnError)
	pruneHost := pruneCmd.String("host", "", "Specific host to prune files from")
	pruneAllHosts := pruneCmd.Bool("all-hosts", false, "Prune files across all hosts")
	pruneIAmSure := pruneCmd.Bool("i-am-sure", false, "") // Hidden flag required for all-hosts pruning

	organizeCmd := flag.NewFlagSet("organize", flag.ExitOnError)
	organizeHost := organizeCmd.String("host", "", "Specific host to organize files from")
	organizeAllHosts := organizeCmd.Bool("all-hosts", false, "Organize files across all hosts")
	organizeRun := organizeCmd.Bool("run", false, "Actually move the files (default is dry-run)")
	organizeMove := organizeCmd.String("move", "", "Move conflicting files to this directory, preserving their structure")
	organizeStripPrefix := organizeCmd.String("strip-prefix", "", "Remove this prefix from paths when moving files")

	dedupeCmd := flag.NewFlagSet("dedupe", flag.ExitOnError)
	dedupeDest := dedupeCmd.String("dest", "", "Directory to move duplicate files to (required)")
	dedupeRun := dedupeCmd.Bool("run", false, "Actually move the files (default is dry-run)")
	dedupeStripPrefix := dedupeCmd.String("strip-prefix", "", "Remove this prefix from paths when moving files")
	dedupeCount := dedupeCmd.Int("count", 0, "Limit the number of duplicate groups to process (0 = no limit)")
	dedupeIgnoreDest := dedupeCmd.Bool("ignore-dest", true, "Ignore files that are already in the destination directory")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal, initiating graceful shutdown...")
		cancel()
	}()

	// Handle commands that don't need database access
	switch os.Args[1] {
	case "listen":
		listenCmd.Parse(os.Args[2:])
		handleListen(ctx)
		return
	case "queue":
		if len(os.Args) < 3 {
			fmt.Println("Expected 'version' subcommand for queue command")
			os.Exit(1)
		}

		switch os.Args[2] {
		case "version":
			queueVersionCmd.Parse(os.Args[3:])
			if *queueVersionValue == "" {
				fmt.Println("Error: --version is required")
				queueVersionCmd.PrintDefaults()
				os.Exit(1)
			}
			handleQueueVersion(ctx, *queueVersionValue)
			return
		default:
			fmt.Printf("Unknown queue subcommand: %s\n", os.Args[2])
			os.Exit(1)
		}
	}

	// Set up RabbitMQ connection for other commands if environment variables are set
	var rabbit *mq.RabbitMQ
	if os.Getenv("RABBITMQ_HOST") != "" {
		var err error
		rabbit, err = mq.NewRabbitMQ(VERSION)
		if err != nil {
			log.Printf("Warning: Failed to connect to RabbitMQ: %v", err)
		} else {
			defer rabbit.Close()

			// Start listening for version updates
			shutdown := rabbit.ListenForUpdates(ctx)

			// Handle version update messages
			go func() {
				<-shutdown
				log.Println("Received version update notification, shutting down...")
				cancel() // Cancel context to initiate graceful shutdown
			}()
		}
	}

	// Acquire flow-specific lock before proceeding
	// Only acquire lock for commands that modify the database
	var lockFile *lock.Lock
	switch os.Args[1] {
	case "migrate":
		lockFile = lock.MustAcquire("migrate")
		defer lockFile.Release()
	case "createdb":
		lockFile = lock.MustAcquire("createdb")
		defer lockFile.Release()
	case "update":
		lockFile = lock.MustAcquire("update")
		defer lockFile.Release()
	case "hash":
		lockFile = lock.MustAcquire("hash")
		defer lockFile.Release()
	case "prune":
		err := pruneCmd.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}
		lockFile = lock.MustAcquire("prune")
		defer lockFile.Release()
	case "organize":
		err := organizeCmd.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}
	case "dedupe":
		err := dedupeCmd.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}
	}

	// Database connection parameters
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}

	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		dbUser = "postgres"
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "deduplicator"
	}

	dbPassword := os.Getenv("DB_PASSWORD")

	// Connect to database
	database, err := db.Connect(dbHost, dbPort, dbUser, dbPassword, dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// Parse subcommands
	var cmdErr error
	switch os.Args[1] {
	case "migrate":
		migrateCmd.Parse(os.Args[2:])
		cmdErr = handleMigrate(database, os.Args[2:])
	case "createdb":
		log.Println("Warning: createdb command is deprecated, please use 'migrate up' instead")
		createdbCmd.Parse(os.Args[2:])
		cmdErr = db.CreateDatabase(database, *createdbForce)
	case "update":
		updateCmd.Parse(os.Args[2:])
		cmdErr = files.ProcessStdin(database)
	case "hash":
		hashCmd.Parse(os.Args[2:])
		cmdErr = files.UpdateHashes(database, *hashForce, *hashCount)
	case "list":
		listCmd.Parse(os.Args[2:])
		if *listHost != "" && *listAllHosts {
			fmt.Println("Error: Cannot specify both --host and --all-hosts")
			os.Exit(1)
		}
		cmdErr = files.FindDuplicates(database, files.ListOptions{
			Host:     *listHost,
			AllHosts: *listAllHosts,
			Count:    *listCount,
			Colors: files.ColorOptions{
				HeaderColor: "\033[33m", // Yellow
				FileColor:   "\033[90m", // Dark gray
				ResetColor:  "\033[0m",  // Reset
			},
		})
	case "prune":
		pruneCmd.Parse(os.Args[2:])
		if *pruneHost != "" && *pruneAllHosts {
			fmt.Println("Error: Cannot specify both --host and --all-hosts")
			os.Exit(1)
		}
		cmdErr = files.PruneNonExistentFiles(database, files.PruneOptions{
			Host:     *pruneHost,
			AllHosts: *pruneAllHosts,
			IAmSure:  *pruneIAmSure,
		})
	case "organize":
		organizeCmd.Parse(os.Args[2:])
		if *organizeHost != "" && *organizeAllHosts {
			fmt.Println("Error: Cannot specify both --host and --all-hosts")
			os.Exit(1)
		}
		cmdErr = files.OrganizeDuplicates(database, files.OrganizeOptions{
			Host:            *organizeHost,
			AllHosts:        *organizeAllHosts,
			DryRun:          !*organizeRun,
			ConflictMoveDir: *organizeMove,
			StripPrefix:     *organizeStripPrefix,
		})
	case "dedupe":
		dedupeCmd.Parse(os.Args[2:])
		if *dedupeDest == "" {
			fmt.Println("Error: --dest is required")
			os.Exit(1)
		}
		cmdErr = files.DedupFiles(database, files.DedupeOptions{
			DryRun:        !*dedupeRun,
			DestDir:       *dedupeDest,
			StripPrefix:   *dedupeStripPrefix,
			Count:         *dedupeCount,
			IgnoreDestDir: *dedupeIgnoreDest,
		})
	case "manage":
		cmdErr = handleManage(database, os.Args[2:])
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}

	if cmdErr != nil {
		log.Fatal(cmdErr)
	}
}

// handleListen handles the listen command
func handleListen(ctx context.Context) {
	if os.Getenv("RABBITMQ_HOST") == "" {
		log.Fatal("RABBITMQ_HOST environment variable is not set")
	}

	rabbit, err := mq.NewRabbitMQ(VERSION)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbit.Close()

	log.Printf("Listening for messages on queue %s (current version: %s)...",
		os.Getenv("RABBITMQ_QUEUE"), VERSION)
	shutdown := rabbit.ListenForUpdates(ctx)

	// Wait for either a shutdown signal or message
	select {
	case <-ctx.Done():
		log.Println("Context cancelled, shutting down...")
	case <-shutdown:
		log.Println("Received version update notification, shutting down...")
	}
}

// handleQueueVersion handles the queue version command
func handleQueueVersion(ctx context.Context, version string) {
	if version == VERSION {
		log.Printf("Publishing current version: %s", VERSION)
	} else {
		log.Printf("Warning: Publishing version %s which differs from current version %s",
			version, VERSION)
	}

	if os.Getenv("RABBITMQ_HOST") == "" {
		log.Fatal("RABBITMQ_HOST environment variable is not set")
	}

	rabbit, err := mq.NewRabbitMQ(VERSION)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbit.Close()

	if err := rabbit.PublishVersionUpdate(ctx, version); err != nil {
		log.Fatalf("Failed to publish version update: %v", err)
	}
}
