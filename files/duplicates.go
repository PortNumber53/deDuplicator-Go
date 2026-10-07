package files

import (
	"context"
	"database/sql"
)

// FindDuplicates finds and displays duplicate files
func FindDuplicates(ctx context.Context, db *sql.DB, opts DuplicateListOptions) (resultErr error) {
	defer cancellationResult(ctx, &resultErr)
	if err := ctx.Err(); err != nil {
		return err
	}
	groups, err := FindDuplicateGroups(ctx, db, "", opts.MinSize, opts.Count)
	if err != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return err
	}

	// Print the results
	_, err = printDuplicateGroupsContext(ctx, groups)
	return err
}
