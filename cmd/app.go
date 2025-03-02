package cmd

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"deduplicator/db"
	"deduplicator/files"
	"deduplicator/lock"
	"deduplicator/mq"
)

// App represents the main application
type App struct {
	version string
	rabbit  *mq.RabbitMQ
	db      *sql.DB
}

// NewApp creates a new App instance
func NewApp(version string) *App {
	return &App{
		version: version,
	}
}

// HandleCommand processes and executes a command
func (a *App) HandleCommand(ctx context.Context, args []string) error {
	if len(args) < 2 {
		PrintUsage(a.version)
		return fmt.Errorf("no command provided")
	}

	// Check for help command
	if args[1] == "help" {
		if len(args) == 2 {
			PrintUsage(a.version)
			return nil
		}
		command := FindCommand(args[2])
		if command != nil {
			ShowCommandHelp(*command)
			return nil
		}
		return fmt.Errorf("unknown command: %s", args[2])
	}

	// Check if command exists and if help is requested
	if len(args) > 2 && args[2] == "help" {
		command := FindCommand(args[1])
		if command != nil {
			ShowCommandHelp(*command)
			return nil
		}
	}

	// Initialize RabbitMQ if needed
	if args[1] == "listen" || args[1] == "queue" {
		if os.Getenv("RABBITMQ_HOST") != "" {
			var err error
			a.rabbit, err = mq.NewRabbitMQ(a.version)
			if err != nil {
				log.Printf("Warning: Failed to connect to RabbitMQ: %v", err)
			} else {
				defer a.rabbit.Close()
				// Start listening for version updates in background
				if args[1] == "listen" {
					shutdown := a.rabbit.ListenForUpdates(ctx)
					go func() {
						select {
						case <-ctx.Done():
							return
						case <-shutdown:
							log.Println("Received version update notification, initiating graceful shutdown...")
							return
						}
					}()
				}
			}
		} else {
			return fmt.Errorf("RabbitMQ connection required but RABBITMQ_HOST not configured")
		}
	}

	// Handle commands that don't need database access
	switch args[1] {
	case "listen":
		<-ctx.Done() // Just wait for shutdown since we're already listening
		return nil
	case "queue":
		if len(args) < 3 {
			return fmt.Errorf("expected 'version' subcommand for queue command")
		}

		switch args[2] {
		case "version":
			if a.rabbit == nil {
				return fmt.Errorf("RabbitMQ connection not available")
			}
			return HandleQueueVersion(ctx, a.rabbit, a.version, a.version)
		default:
			return fmt.Errorf("unknown queue subcommand: %s", args[2])
		}
	}

	// Acquire flow-specific lock before proceeding
	var lockFile *lock.Lock
	switch args[1] {
	case "migrate", "createdb", "update", "hash", "prune":
		lockFile = lock.MustAcquire(args[1])
		defer lockFile.Release()
	}

	// Connect to database
	if err := a.connectDB(); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	defer a.db.Close()

	// Execute command
	switch args[1] {
	case "migrate":
		return HandleMigrate(a.db, args[2:])
	case "createdb":
		log.Println("Warning: createdb command is deprecated, please use 'migrate up' instead")
		return db.CreateDatabase(a.db, false) // TODO: Add force flag support
	case "update":
		return files.ProcessStdin(ctx, a.db)
	case "hash":
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to get hostname: %v", err)
		}

		// Convert hostname to lowercase for consistency
		hostname = strings.ToLower(hostname)

		var hostName string
		err = a.db.QueryRow(`
			SELECT name 
			FROM hosts 
			WHERE LOWER(hostname) = LOWER($1)
		`, hostname).Scan(&hostName)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
			}
			return err
		}

		// Parse hash command flags
		flags := CreateFlagSets(a.version)
		hashCmd := flags["hash"]
		if err := hashCmd.Parse(args[2:]); err != nil {
			return fmt.Errorf("error parsing hash command flags: %v", err)
		}

		forceFlag := hashCmd.Lookup("force")
		renewFlag := hashCmd.Lookup("renew")
		retryFlag := hashCmd.Lookup("retry-problematic")

		return files.HashFiles(ctx, a.db, files.HashOptions{
			Host:             hostName,
			Refresh:          forceFlag != nil && forceFlag.Value.(flag.Getter).Get().(bool),
			Renew:            renewFlag != nil && renewFlag.Value.(flag.Getter).Get().(bool),
			RetryProblematic: retryFlag != nil && retryFlag.Value.(flag.Getter).Get().(bool),
		})
	case "list":
		// Parse list command flags
		flags := CreateFlagSets(a.version)
		listCmd := flags["list"]
		if err := listCmd.Parse(args[2:]); err != nil {
			return fmt.Errorf("error parsing list command flags: %v", err)
		}

		countFlag := listCmd.Lookup("count")
		minSizeFlag := listCmd.Lookup("min-size")

		// Parse count
		var count int
		if countFlag != nil {
			count = countFlag.Value.(flag.Getter).Get().(int)
		}

		// Parse min-size
		var minSize int64
		if minSizeFlag != nil && minSizeFlag.Value.String() != "" {
			var err error
			minSize, err = files.ParseSize(minSizeFlag.Value.String())
			if err != nil {
				return fmt.Errorf("invalid min-size value: %v", err)
			}
		}

		return files.FindDuplicates(ctx, a.db, files.DuplicateListOptions{
			Count:   count,
			MinSize: minSize,
		})
	case "prune":
		// Parse prune command flags
		flags := CreateFlagSets(a.version)
		pruneCmd := flags["prune"]
		if err := pruneCmd.Parse(args[2:]); err != nil {
			return fmt.Errorf("error parsing prune command flags: %v", err)
		}

		return files.PruneNonExistentFiles(ctx, a.db, files.PruneOptions{})
	case "problematic":
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to get hostname: %v", err)
		}

		// Convert hostname to lowercase for consistency
		hostname = strings.ToLower(hostname)

		var hostName string
		err = a.db.QueryRow(`
			SELECT name 
			FROM hosts 
			WHERE LOWER(hostname) = LOWER($1)
		`, hostname).Scan(&hostName)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
			}
			return err
		}

		return files.ListProblematicFiles(ctx, a.db, hostName)
	case "organize":
		// Parse organize command flags
		flags := CreateFlagSets(a.version)
		organizeCmd := flags["organize"]
		if err := organizeCmd.Parse(args[2:]); err != nil {
			return fmt.Errorf("error parsing organize command flags: %v", err)
		}

		runFlag := organizeCmd.Lookup("run")
		moveFlag := organizeCmd.Lookup("move")
		stripPrefixFlag := organizeCmd.Lookup("strip-prefix")

		return files.OrganizeDuplicates(ctx, a.db, files.OrganizeOptions{
			DryRun:          runFlag == nil || !runFlag.Value.(flag.Getter).Get().(bool),
			ConflictMoveDir: moveFlag.Value.String(),
			StripPrefix:     stripPrefixFlag.Value.String(),
		})
	case "dedupe":
		// Parse dedupe command flags
		flags := CreateFlagSets(a.version)
		dedupeCmd := flags["dedupe"]
		if err := dedupeCmd.Parse(args[2:]); err != nil {
			return fmt.Errorf("error parsing dedupe command flags: %v", err)
		}

		destFlag := dedupeCmd.Lookup("dest")
		if destFlag == nil || destFlag.Value.String() == "" {
			return fmt.Errorf("--dest is required for dedupe command")
		}

		runFlag := dedupeCmd.Lookup("run")
		stripPrefixFlag := dedupeCmd.Lookup("strip-prefix")
		countFlag := dedupeCmd.Lookup("count")
		ignoreDestFlag := dedupeCmd.Lookup("ignore-dest")
		minSizeFlag := dedupeCmd.Lookup("min-size")

		// Parse min-size if provided
		var parsedMinSize int64
		if minSizeFlag != nil && minSizeFlag.Value.String() != "" {
			var err error
			parsedMinSize, err = files.ParseSize(minSizeFlag.Value.String())
			if err != nil {
				return fmt.Errorf("error parsing min-size: %v", err)
			}
		}

		return files.DedupFiles(ctx, a.db, files.DedupeOptions{
			DryRun:        runFlag == nil || !runFlag.Value.(flag.Getter).Get().(bool),
			DestDir:       destFlag.Value.String(),
			StripPrefix:   stripPrefixFlag.Value.String(),
			Count:         countFlag.Value.(flag.Getter).Get().(int),
			IgnoreDestDir: ignoreDestFlag == nil || ignoreDestFlag.Value.(flag.Getter).Get().(bool),
			MinSize:       parsedMinSize,
		})
	case "manage":
		return HandleManage(a.db, args[2:])
	case "files":
		return HandleFiles(ctx, a.db, args[2:])
	default:
		return fmt.Errorf("unknown command: %s", args[1])
	}
}

// connectDB establishes a connection to the database
func (a *App) connectDB() error {
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

	var err error
	a.db, err = db.Connect(dbHost, dbPort, dbUser, dbPassword, dbName)
	return err
}
