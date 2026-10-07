package files

import "context"

// cancellationResult preserves a shared shutdown cause across driver and subprocess errors.
func cancellationResult(ctx context.Context, result *error) {
	if err := ctx.Err(); err != nil {
		*result = err
	}
}
