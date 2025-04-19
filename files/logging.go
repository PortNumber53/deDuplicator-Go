package files

import (
	"log"
	"os"
)

var (
	InfoLogger  *log.Logger
	ErrorLogger *log.Logger
)

// InitLoggers initializes loggers for info and error logs, using environment variables LOG_FILE and ERROR_LOG_FILE.
func InitLoggers() {
	logFile := os.Getenv("LOG_FILE")
	errorLogFile := os.Getenv("ERROR_LOG_FILE")

	var infoHandle, errorHandle *os.File
	var err error

	if logFile != "" {
		infoHandle, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Warning: Could not open LOG_FILE %s: %v. Logging to stderr.", logFile, err)
			infoHandle = os.Stderr
		}
	} else {
		infoHandle = os.Stderr
	}

	if errorLogFile != "" {
		errorHandle, err = os.OpenFile(errorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Warning: Could not open ERROR_LOG_FILE %s: %v. Logging errors to stderr.", errorLogFile, err)
			errorHandle = os.Stderr
		}
	} else {
		errorHandle = os.Stderr
	}

	InfoLogger = log.New(infoHandle, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger = log.New(errorHandle, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}
