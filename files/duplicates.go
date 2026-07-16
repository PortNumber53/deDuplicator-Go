package files

import (
	"context"
	"database/sql"
)

// FindDuplicates finds and displays duplicate files
func FindDuplicates(ctx context.Context, db *sql.DB, opts DuplicateListOptions) error {
	groups, err := FindDuplicateGroups(ctx, db, "", opts.MinSize, opts.Count)
	if err != nil {
		return err
	}

	// Print the results
	PrintDuplicateGroups(groups)
	return nil
}
