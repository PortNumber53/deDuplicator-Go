package files

import (
	"os"
	"strings"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

type progressDisplayPolicy struct {
	Visible bool
	Color   bool
}

func currentProgressDisplayPolicy() progressDisplayPolicy {
	return progressDisplayPolicyFor(term.IsTerminal(int(os.Stderr.Fd())), os.Getenv)
}

func progressDisplayPolicyFor(isTerminal bool, getenv func(string) string) progressDisplayPolicy {
	ci := getenv("CI") != "" || getenv("JENKINS_URL") != "" || getenv("BUILD_NUMBER") != ""
	visible := isTerminal && !ci
	color := visible && getenv("NO_COLOR") == "" && !strings.EqualFold(getenv("TERM"), "dumb")
	return progressDisplayPolicy{Visible: visible, Color: color}
}

func newProgressBar(max int64, description string, options ...progressbar.Option) *progressbar.ProgressBar {
	policy := currentProgressDisplayPolicy()
	theme := progressbar.Theme{
		Saucer:        "=",
		SaucerHead:    ">",
		SaucerPadding: " ",
		BarStart:      "[",
		BarEnd:        "]",
	}
	if policy.Color {
		description = "[cyan]" + description + "[reset]"
		theme.Saucer = "[green]=[reset]"
		theme.SaucerHead = "[green]>[reset]"
	}

	baseOptions := []progressbar.Option{
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionSetVisibility(policy.Visible),
		progressbar.OptionEnableColorCodes(policy.Color),
		progressbar.OptionUseANSICodes(policy.Visible),
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetTheme(theme),
	}
	return progressbar.NewOptions64(max, append(baseOptions, options...)...)
}

func finishProgressLine() {
	if currentProgressDisplayPolicy().Visible {
		_, _ = os.Stderr.WriteString("\n")
	}
}
