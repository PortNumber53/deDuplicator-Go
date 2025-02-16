package cmd

import (
	"database/sql"
	"log"
	"os"

	"deduplicator/db"
)

// SetupDatabase creates a database connection using environment variables
func SetupDatabase() *sql.DB {
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

	return database
}
