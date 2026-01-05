package cmd

import "fmt"

// PrintUsage prints the main usage information
func PrintUsage(version string) {
	fmt.Printf("Deduplicator %s - A tool for finding and managing duplicate files\n\n", version)
	fmt.Println("Usage:")
	fmt.Println("Usage: deduplicator <command> [options]")
	fmt.Println("Available Commands:")

	// Find the longest command name for padding
	maxLen := 0
	for _, cmd := range Commands {
		if len(cmd.Name) > maxLen {
			maxLen = len(cmd.Name)
		}
	}

	// Print each command with aligned descriptions
	for _, cmd := range Commands {
		fmt.Printf("  %-*s  %s\n", maxLen, cmd.Name, cmd.Description)
	}

	fmt.Println("\nDetailed Usage:")
	for _, cmd := range Commands {
		fmt.Printf("  deduplicator %s\n", cmd.Usage)
	}

	fmt.Println("\nEnvironment Variables:")
	fmt.Println("  DB_HOST          PostgreSQL host (default: localhost)")
	fmt.Println("  DB_PORT          PostgreSQL port (default: 5432)")
	fmt.Println("  DB_USER          PostgreSQL user (default: postgres)")
	fmt.Println("  DB_PASSWORD      PostgreSQL password")
	fmt.Println("  DB_NAME          PostgreSQL database name (default: deduplicator)")
	fmt.Println("  DB_URL           Optional database URL (overrides individual DB_* values when used by your scripts)")
	fmt.Println("  RABBITMQ_HOST    RabbitMQ host (optional)")
	fmt.Println("  RABBITMQ_PORT    RabbitMQ port (default: 5672)")
	fmt.Println("  RABBITMQ_VHOST   RabbitMQ vhost")
	fmt.Println("  RABBITMQ_USER    RabbitMQ username")
	fmt.Println("  RABBITMQ_PASSWORD RabbitMQ password")
	fmt.Println("  RABBITMQ_QUEUE   RabbitMQ queue name (default: dedup_backup)")
	fmt.Println("  DEDUPLICATOR_LOCK_DIR    Override lock directory for flow locks")
	fmt.Println("  LOCAL_MIGRATE_LOCK_DIR   Override lock directory for local migration lock")
	fmt.Println("  LOG_FILE         Log file path (default: /var/log/dedupe/dedupe.log)")
	fmt.Println("  ERROR_LOG_FILE   Error log file path (default: /var/log/dedupe/error.log)")
}

// ShowCommandHelp shows detailed help for a specific command
func ShowCommandHelp(cmd Command) {
	fmt.Printf("\nCommand: %s - %s\n\n", cmd.Name, cmd.Description)
	fmt.Printf("Usage:\n  deduplicator %s\n\n", cmd.Usage)
	fmt.Println(cmd.Help)
	if len(cmd.Examples) > 0 {
		fmt.Println("\nExamples:")
		for _, example := range cmd.Examples {
			fmt.Printf("  %s\n", example)
		}
	}
	fmt.Println()
}
