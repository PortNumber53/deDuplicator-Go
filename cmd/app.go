package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

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

		var hostName string
		err = a.db.QueryRow(`
			SELECT name 
			FROM hosts 
			WHERE hostname = $1
		`, hostname).Scan(&hostName)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no host found for hostname %s, please add it using 'dedupe manage add'", hostname)
			}
			return err
		}

		return files.HashFiles(ctx, a.db, files.HashOptions{
			Host:    hostName,
			Refresh: false, // TODO: Add flag support
			Renew:   false,
		})
	case "list":
		return files.FindDuplicates(ctx, a.db, files.DuplicateListOptions{
			Host:     "", // TODO: Add flag support
			AllHosts: false,
			Count:    0,
			MinSize:  0,
			Colors: files.ColorOptions{
				HeaderColor: "\033[33m", // Yellow
				FileColor:   "\033[90m", // Dark gray
				ResetColor:  "\033[0m",  // Reset
			},
		})
	case "prune":
		return files.PruneNonExistentFiles(ctx, a.db, files.PruneOptions{
			Host:     "", // TODO: Add flag support
			AllHosts: false,
			IAmSure:  false,
		})
	case "organize":
		return files.OrganizeDuplicates(ctx, a.db, files.OrganizeOptions{
			Host:            "", // TODO: Add flag support
			AllHosts:        false,
			DryRun:          true,
			ConflictMoveDir: "",
			StripPrefix:     "",
		})
	case "dedupe":
		return files.DedupFiles(ctx, a.db, files.DedupeOptions{
			DryRun:        true, // TODO: Add flag support
			DestDir:       "",
			StripPrefix:   "",
			Count:         0,
			IgnoreDestDir: true,
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
