package cmd

import (
	"flag"

	"github.com/spf13/cobra"
)

// CreateFlagSets creates and returns all command flag sets
func CreateFlagSets(version string) map[string]*flag.FlagSet {
	flagSets := make(map[string]*flag.FlagSet)

	// Root command flags
	rootCmd := flag.NewFlagSet("root", flag.ContinueOnError)
	rootCmd.Bool("help", false, "Show help")
	rootCmd.Bool("version", false, "Show version")
	flagSets["root"] = rootCmd

	// Scan command flags
	scanCmd := flag.NewFlagSet("scan", flag.ContinueOnError)
	scanCmd.Bool("help", false, "Show help for scan command")
	scanCmd.Bool("recursive", true, "Scan directories recursively")
	scanCmd.Bool("follow-symlinks", false, "Follow symbolic links")
	scanCmd.String("min-size", "1B", "Minimum file size to consider (e.g. 10KB, 1MB)")
	scanCmd.String("exclude", "", "Exclude files matching pattern (glob)")
	scanCmd.Bool("verbose", false, "Verbose output")
	flagSets["scan"] = scanCmd

	// Files command flags
	filesCmd := flag.NewFlagSet("files", flag.ContinueOnError)
	filesCmd.Bool("help", false, "Show help for files command")
	flagSets["files"] = filesCmd

	// Files list-dupes command flags
	filesListDupesCmd := flag.NewFlagSet("files-list-dupes", flag.ContinueOnError)
	filesListDupesCmd.Bool("help", false, "Show help for files list-dupes command")
	filesListDupesCmd.String("dest", "", "Directory to move duplicate files to")
	filesListDupesCmd.Bool("run", false, "Actually move files (without this flag, it's a dry run)")
	filesListDupesCmd.String("strip-prefix", "", "Strip this prefix from paths when moving")
	filesListDupesCmd.Int("count", 0, "Limit to top N duplicate groups (by size)")
	filesListDupesCmd.Bool("ignore-dest", true, "Ignore files in destination directory")
	filesListDupesCmd.String("min-size", "1B", "Minimum file size to consider (e.g. 10KB, 1MB)")
	flagSets["files-list-dupes"] = filesListDupesCmd

	// Files move-dupes command flags
	filesMoveCmd := flag.NewFlagSet("files-move-dupes", flag.ContinueOnError)
	filesMoveCmd.Bool("help", false, "Show help for files move-dupes command")
	filesMoveCmd.String("target", "", "Target directory to move duplicates to (required)")
	filesMoveCmd.Bool("dry-run", false, "Show what would be moved without making changes")
	flagSets["files-move-dupes"] = filesMoveCmd

	// Files hash command flags
	filesHashCmd := flag.NewFlagSet("files-hash", flag.ContinueOnError)
	filesHashCmd.Bool("help", false, "Show help for files hash command")
	filesHashCmd.Bool("force", false, "Rehash files even if they already have a hash")
	filesHashCmd.Bool("renew", false, "Recalculate hashes older than 1 week")
	filesHashCmd.Bool("retry-problematic", false, "Retry files that previously timed out")
	filesHashCmd.Int("count", 0, "Process only N files (0 = unlimited)")
	flagSets["files-hash"] = filesHashCmd

	// Manage command flags
	manageCmd := flag.NewFlagSet("manage", flag.ContinueOnError)
	manageCmd.Bool("help", false, "Show help for manage command")
	flagSets["manage"] = manageCmd

	return flagSets
}

func addPruneFlags(cmd *cobra.Command) {
	// No flags needed for prune command
}
