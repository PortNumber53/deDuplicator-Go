package files

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
)

// FindDuplicates finds and displays duplicate files
func FindDuplicates(ctx context.Context, db *sql.DB, opts DuplicateListOptions) error {
	// Get hostname for current machine
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting hostname: %v", err)
	}

	// Convert hostname to lowercase for consistency
	hostname = strings.ToLower(hostname)

	// Find duplicate groups
	groups, err := FindDuplicateGroups(ctx, db, hostname, opts.MinSize, opts.Count)
	if err != nil {
		return err
	}

	// Print the results
	PrintDuplicateGroups(groups)
	return nil
}
