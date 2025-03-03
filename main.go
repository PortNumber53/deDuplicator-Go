package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"deduplicator/cmd"
)

// VERSION represents the current version of the deduplicator tool
const VERSION = "1.2.1"

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

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
		log.Println("Received shutdown signal, initiating graceful shutdown...")
		cancel()
	}()

	// Create and run application
	app := cmd.NewApp(VERSION)
	if err := app.HandleCommand(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}
