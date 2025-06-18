package files

// ColorOptions represents color settings for output
type ColorOptions struct {
	HeaderColor string
	FileColor   string
	ResetColor  string
}

// DuplicateListOptions represents options for listing duplicate files
type DuplicateListOptions struct {
	Count   int   // Limit the number of duplicate groups to show (0 = no limit)
	MinSize int64 // Minimum file size to consider
}

// DedupeOptions represents options for the dedupe command
type DedupeOptions struct {
	DryRun        bool   // If true, only show what would be done without making changes
	DestDir       string // Directory to move duplicate files to
	StripPrefix   string // Remove this prefix from paths when moving files
	Count         int    // Limit the number of duplicate groups to process (0 = no limit)
	IgnoreDestDir bool   // If true, ignore files that are already in the destination directory
	MinSize       int64  // Minimum file size to consider
}

// ImportOptions represents options for the import command
type ImportOptions struct {
	SourcePath   string // Source directory to import files from
	HostName     string // Target hostname to import files to
	FriendlyPath string // Target friendly path on the server to import files to
	RemoveSource bool   // If true, remove source files after successful import
	DryRun       bool   // If true, only show what would be done without making changes
	Count        int    // Limit the number of files to process (0 = no limit)
	DuplicateDir string // If non-empty, move duplicate files to this directory instead of skipping
}

// MoveOptions represents options for moving duplicate files
type MoveOptions struct {
	TargetDir string // Directory to move duplicates to
	DryRun    bool   // If true, only show what would be done
	Count     int    // Limit the number of duplicate groups to process (0 = no limit)
}

// PruneOptions represents options for the prune command


// HashOptions represents options for the hash command
type HashOptions struct {
	Server           string
	Refresh          bool // hash all files regardless of existing hash
	Renew            bool // hash files with hashes older than 1 week
	RetryProblematic bool // retry files that previously timed out
}

// FindOptions represents options for the find command
type FindOptions struct {
	Server      string
	Path        string // Optional friendly path to filter on
	MinimumSize int64 // Minimum file size to consider
	NumWorkers  int   // Number of worker goroutines to use
}
