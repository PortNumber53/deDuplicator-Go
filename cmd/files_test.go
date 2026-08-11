package cmd

import "testing"

func TestParseGroupDedupeOptionsEnablesVerbose(t *testing.T) {
	opts, err := parseGroupDedupeOptions("family", []string{
		"--count", "1",
		"--min-size", "10G",
		"--verbose",
	})
	if err != nil {
		t.Fatalf("parseGroupDedupeOptions error: %v", err)
	}
	if !opts.Verbose {
		t.Fatal("verbose = false, want true")
	}
	if opts.GroupName != "family" || opts.Count != 1 || opts.MinSize != 10*1024*1024*1024 {
		t.Fatalf("unexpected options: %+v", opts)
	}
	if !opts.DryRun {
		t.Fatal("dry run should remain the default")
	}
}
