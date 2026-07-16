package files

import (
	"io"
	"log"

	"deduplicator/logging"
)

func init() {
	logging.InfoLogger = log.New(io.Discard, "", 0)
	logging.ErrorLogger = log.New(io.Discard, "", 0)
}
