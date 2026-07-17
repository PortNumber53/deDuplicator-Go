package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/joho/godotenv"

	_ "github.com/golang-migrate/migrate/v4/source/file"

	"deduplicator/cmd"
	"deduplicator/logging"
)

// VERSION represents the current version of the deduplicator tool
const VERSION = "1.4.3"

const (
	systemConfigPath = "/etc/dedupe/config.ini"
	dotenvPath       = ".env"
)

func main() {
	configFiles := defaultConfigFiles()
	loadedConfig, err := loadConfigFiles(configFiles...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
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

	if shouldRejectMissingConfig(os.Args, loadedConfig) {
		logging.ErrorLogger.Fatalf("no configuration file found: expected one of %s", strings.Join(configFilePaths(configFiles), ", "))
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

type configFileKind int

const (
	configFileINI configFileKind = iota
	configFileDotenv
)

type configFile struct {
	path string
	kind configFileKind
}

func defaultConfigFiles() []configFile {
	files := []configFile{
		{path: systemConfigPath, kind: configFileINI},
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		files = append(files, configFile{
			path: filepath.Join(home, ".config", "dedupe", "config.ini"),
			kind: configFileINI,
		})
	}
	files = append(files, configFile{path: dotenvPath, kind: configFileDotenv})
	return files
}

func configFilePaths(files []configFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.path)
	}
	return paths
}

func loadConfigFiles(files ...configFile) (bool, error) {
	loaded := false

	for _, file := range files {
		switch file.kind {
		case configFileINI:
			iniLoaded, err := loadConfigINI(file.path)
			if err != nil {
				return false, err
			}
			loaded = loaded || iniLoaded
		case configFileDotenv:
			if err := loadDotenv(file.path); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return false, err
				}
			} else {
				loaded = true
			}
		}
	}
	return loaded, nil
}

func loadDotenv(path string) error {
	if _, err := os.Stat(path); err != nil {
		return err
	}
	return godotenv.Load(path)
}

func shouldRejectMissingConfig(args []string, loadedConfig bool) bool {
	if loadedConfig || hasConfigEnvironment() || isHelpOrVersionRequest(args) {
		return false
	}
	return len(args) > 1
}

func isHelpOrVersionRequest(args []string) bool {
	for i, arg := range args {
		if i == 0 {
			continue
		}
		switch arg {
		case "--help", "-h", "help", "--version", "-v":
			return true
		}
	}
	return false
}

func hasConfigEnvironment() bool {
	for _, key := range []string{
		"DB_HOST",
		"DB_PORT",
		"DB_USER",
		"DB_PASSWORD",
		"DB_NAME",
		"RABBITMQ_HOST",
		"RABBITMQ_PORT",
		"RABBITMQ_VHOST",
		"RABBITMQ_USER",
		"RABBITMQ_PASSWORD",
		"RABBITMQ_QUEUE",
	} {
		if os.Getenv(key) != "" {
			return true
		}
	}
	return false
}

// loadConfigINI reads a simple INI-style config and sets env vars if unset.
//
// Supported sections:
// - [default] (and lines before any section): misc settings and DB fallbacks
// - [database]
// - [rabbitmq]
// - [logging]
func loadConfigINI(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("error opening config %s: %w", path, err)
	}
	defer f.Close()

	type dbCfg struct {
		host     string
		port     string
		user     string
		password string
		name     string
	}
	type rabbitCfg struct {
		host     string
		port     string
		vhost    string
		user     string
		password string
		queue    string
	}
	type loggingCfg struct {
		logFile      string
		errorLogFile string
	}

	cfg := dbCfg{}
	rmq := rabbitCfg{}
	logCfg := loggingCfg{}
	configuredHostname := ""
	lockDir := ""
	localMigrateLockDir := ""

	section := "default" // also covers lines before any [section]
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")))
			if section == "" {
				section = "default"
			}
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])

		switch section {
		case "database", "default", "":
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
			case "hostname":
				configuredHostname = val
			case "deduplicator_lock_dir":
				lockDir = val
			case "local_migrate_lock_dir":
				localMigrateLockDir = val
			}
		case "rabbitmq":
			switch key {
			case "host":
				rmq.host = val
			case "port":
				rmq.port = val
			case "vhost":
				rmq.vhost = val
			case "user", "username":
				rmq.user = val
			case "password":
				rmq.password = val
			case "queue":
				rmq.queue = val
			}
		case "logging":
			switch key {
			case "log_file":
				logCfg.logFile = val
			case "error_log_file":
				logCfg.errorLogFile = val
			}
		}
	}
	if err := sc.Err(); err != nil {
		return true, fmt.Errorf("error reading config %s: %w", path, err)
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

	if os.Getenv("DEDUPLICATOR_HOSTNAME") == "" && configuredHostname != "" {
		os.Setenv("DEDUPLICATOR_HOSTNAME", configuredHostname)
	}
	if os.Getenv("DEDUPLICATOR_LOCK_DIR") == "" && lockDir != "" {
		os.Setenv("DEDUPLICATOR_LOCK_DIR", lockDir)
	}
	if os.Getenv("LOCAL_MIGRATE_LOCK_DIR") == "" && localMigrateLockDir != "" {
		os.Setenv("LOCAL_MIGRATE_LOCK_DIR", localMigrateLockDir)
	}

	if os.Getenv("RABBITMQ_HOST") == "" && rmq.host != "" {
		os.Setenv("RABBITMQ_HOST", rmq.host)
	}
	if os.Getenv("RABBITMQ_PORT") == "" && rmq.port != "" {
		os.Setenv("RABBITMQ_PORT", rmq.port)
	}
	if os.Getenv("RABBITMQ_VHOST") == "" && rmq.vhost != "" {
		os.Setenv("RABBITMQ_VHOST", rmq.vhost)
	}
	if os.Getenv("RABBITMQ_USER") == "" && rmq.user != "" {
		os.Setenv("RABBITMQ_USER", rmq.user)
	}
	if os.Getenv("RABBITMQ_PASSWORD") == "" && rmq.password != "" {
		os.Setenv("RABBITMQ_PASSWORD", rmq.password)
	}
	if os.Getenv("RABBITMQ_QUEUE") == "" && rmq.queue != "" {
		os.Setenv("RABBITMQ_QUEUE", rmq.queue)
	}

	if os.Getenv("LOG_FILE") == "" && logCfg.logFile != "" {
		os.Setenv("LOG_FILE", logCfg.logFile)
	}
	if os.Getenv("ERROR_LOG_FILE") == "" && logCfg.errorLogFile != "" {
		os.Setenv("ERROR_LOG_FILE", logCfg.errorLogFile)
	}

	return true, nil
}
