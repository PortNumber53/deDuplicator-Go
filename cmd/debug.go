package cmd

import (
	"os"
)

type dbInfo struct {
	Host string
	Port string
	User string
	Name string
}

// currentDBInfo mirrors the defaults used by connectDB for easier diagnostics.
func currentDBInfo() dbInfo {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = "postgres"
	}
	name := os.Getenv("DB_NAME")
	if name == "" {
		name = "deduplicator"
	}
	return dbInfo{
		Host: host,
		Port: port,
		User: user,
		Name: name,
	}
}
