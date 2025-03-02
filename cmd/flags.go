package cmd

import (
	"flag"

	"github.com/spf13/cobra"
)

// CreateFlagSets creates and returns all command flag sets
func CreateFlagSets(version string) map[string]*flag.FlagSet {
	flags := make(map[string]*flag.FlagSet)

	// Migrate command flags
	migrateCmd := flag.NewFlagSet("migrate", flag.ExitOnError)
	flags["migrate"] = migrateCmd

	// Createdb command flags
	createdbCmd := flag.NewFlagSet("createdb", flag.ExitOnError)
	createdbCmd.Bool("force", false, "Force recreation of tables")
	flags["createdb"] = createdbCmd

	// Update command flags
	updateCmd := flag.NewFlagSet("update", flag.ExitOnError)
	flags["update"] = updateCmd

	// Hash command flags
	hashCmd := flag.NewFlagSet("hash", flag.ExitOnError)
	hashCmd.Bool("force", false, "Force rehash of all files")
	hashCmd.Bool("renew", false, "Recalculate hashes older than 1 week")
	hashCmd.Bool("retry-problematic", false, "Retry files that previously timed out")
	flags["hash"] = hashCmd

	// List command flags
	listCmd := flag.NewFlagSet("list", flag.ExitOnError)
	listCmd.Int("count", 0, "Limit the number of duplicate groups to show (0 = no limit)")
	listCmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")
	flags["list"] = listCmd

	// Listen command flags
	listenCmd := flag.NewFlagSet("listen", flag.ExitOnError)
	flags["listen"] = listenCmd

	// Queue version command flags
	queueVersionCmd := flag.NewFlagSet("version", flag.ExitOnError)
	queueVersionCmd.String("version", version, "Version number to publish (defaults to current version)")
	flags["queue-version"] = queueVersionCmd

	// Prune command flags
	pruneCmd := flag.NewFlagSet("prune", flag.ExitOnError)
	flags["prune"] = pruneCmd

	// Organize command flags
	organizeCmd := flag.NewFlagSet("organize", flag.ExitOnError)
	organizeCmd.Bool("run", false, "Actually move the files (default is dry-run)")
	organizeCmd.String("move", "", "Move conflicting files to this directory, preserving their structure")
	organizeCmd.String("strip-prefix", "", "Remove this prefix from paths when moving files")
	flags["organize"] = organizeCmd

	// Dedupe command flags
	dedupeCmd := flag.NewFlagSet("dedupe", flag.ExitOnError)
	dedupeCmd.String("dest", "", "Directory to move duplicate files to (required)")
	dedupeCmd.Bool("run", false, "Actually move the files (default is dry-run)")
	dedupeCmd.String("strip-prefix", "", "Remove this prefix from paths when moving files")
	dedupeCmd.Int("count", 0, "Limit the number of duplicate groups to process (0 = no limit)")
	dedupeCmd.Bool("ignore-dest", true, "Ignore files that are already in the destination directory")
	dedupeCmd.String("min-size", "", "Minimum file size to consider (e.g., \"1M\", \"1.5G\", \"500K\")")
	flags["dedupe"] = dedupeCmd

	return flags
}

func addPruneFlags(cmd *cobra.Command) {
	// No flags needed for prune command
}
