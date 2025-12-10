package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/joho/godotenv"

	_ "github.com/golang-migrate/migrate/v4/source/file"

	"deduplicator/cmd"
	"deduplicator/logging"
)

// VERSION represents the current version of the deduplicator tool
const VERSION = "1.3.5"

func main() {
	loadConfigINI("/etc/dedupe/config.ini")

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

// loadConfigINI reads a simple INI-style config and sets DB_* env vars if unset.
func loadConfigINI(path string) {
	f, err := os.Open(path)
	if err != nil {
		// Silent if missing; warn only on unexpected errors
		if !os.IsNotExist(err) {
			log.Printf("Warning: Error opening config %s: %v", path, err)
		}
		return
	}
	defer f.Close()

	type dbCfg struct {
		host     string
		port     string
		user     string
		password string
		name     string
	}
	cfg := dbCfg{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") || line == "[database]" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		switch key {
		case "url":
			if u, err := url.Parse(val); err == nil {
				if u.Hostname() != "" {
					cfg.host = u.Hostname()
				}
				if u.Port() != "" {
					cfg.port = u.Port()
				}
				if u.User != nil {
					if u.User.Username() != "" {
						cfg.user = u.User.Username()
					}
					if p, ok := u.User.Password(); ok {
						cfg.password = p
					}
				}
				if strings.TrimPrefix(u.Path, "/") != "" {
					cfg.name = strings.TrimPrefix(u.Path, "/")
				}
			}
		case "host":
			cfg.host = val
		case "port":
			cfg.port = val
		case "user":
			cfg.user = val
		case "password":
			cfg.password = val
		case "name", "dbname":
			cfg.name = val
		}
	}
	if err := sc.Err(); err != nil {
		log.Printf("Warning: Error reading config %s: %v", path, err)
	}

	// Only set env vars if not already set
	if os.Getenv("DB_HOST") == "" && cfg.host != "" {
		os.Setenv("DB_HOST", cfg.host)
	}
	if os.Getenv("DB_PORT") == "" && cfg.port != "" {
		os.Setenv("DB_PORT", cfg.port)
	}
	if os.Getenv("DB_USER") == "" && cfg.user != "" {
		os.Setenv("DB_USER", cfg.user)
	}
	if os.Getenv("DB_PASSWORD") == "" && cfg.password != "" {
		os.Setenv("DB_PASSWORD", cfg.password)
	}
	if os.Getenv("DB_NAME") == "" && cfg.name != "" {
		os.Setenv("DB_NAME", cfg.name)
	}
}
