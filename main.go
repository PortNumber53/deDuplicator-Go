package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	_ "github.com/golang-migrate/migrate/v4/source/file"

	"deduplicator/cmd"
	"deduplicator/logging"
)

// VERSION represents the current version of the deduplicator tool
const VERSION = "1.3.5"

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	logging.InitLoggers()

	// Check for help or version flags
	if len(os.Args) > 1 {
		if os.Args[1] == "--help" || os.Args[1] == "-h" {
			cmd.PrintUsage(VERSION)
			return
		} else if os.Args[1] == "--version" || os.Args[1] == "-v" {
			fmt.Printf("Deduplicator %s\n", VERSION)
			return
		}
	}

	// Create context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logging.InfoLogger.Println("Received shutdown signal, initiating graceful shutdown...")
		cancel()
	}()

	// Create and run application
	app := cmd.NewApp(VERSION)
	logging.InfoLogger.Printf("DEBUG: os.Args = %v", os.Args)
	if err := app.HandleCommand(ctx, os.Args); err != nil {
		logging.ErrorLogger.Fatal(err)
	}
}
