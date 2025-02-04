package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	"deduplicator/db"
	"deduplicator/files"
	"deduplicator/lock"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	// Command line flags
	createdbCmd := flag.NewFlagSet("createdb", flag.ExitOnError)
	createdbForce := createdbCmd.Bool("force", false, "Force recreation of tables")

	updateCmd := flag.NewFlagSet("update", flag.ExitOnError)

	hashCmd := flag.NewFlagSet("hash", flag.ExitOnError)
	hashForce := hashCmd.Bool("force", false, "Force rehash of all files")
	hashCount := hashCmd.Int("count", 0, "Number of files to process")

	listCmd := flag.NewFlagSet("list", flag.ExitOnError)
	listHost := listCmd.String("host", "", "Specific host to check for duplicates")
	listAllHosts := listCmd.Bool("all-hosts", false, "Check duplicates across all hosts")

	pruneCmd := flag.NewFlagSet("prune", flag.ExitOnError)
	pruneHost := pruneCmd.String("host", "", "Specific host to prune files from")
	pruneAllHosts := pruneCmd.Bool("all-hosts", false, "Prune files across all hosts")
	pruneIAmSure := pruneCmd.Bool("i-am-sure", false, "") // Hidden flag required for all-hosts pruning

	if len(os.Args) < 2 {
		fmt.Println("Expected 'createdb', 'update', 'hash', 'list' or 'prune' subcommands")
		os.Exit(1)
	}

	// Acquire flow-specific lock before proceeding
	// Only acquire lock for commands that modify the database
	var lockFile *lock.Lock
	switch os.Args[1] {
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
	case "createdb":
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
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}

	if cmdErr != nil {
		log.Fatal(cmdErr)
	}
}
