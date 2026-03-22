package version

import "fmt"

var (
	// Version is the semantic version string (for example: v1.2.3).
	// It is injected at build time via -ldflags.
	Version = "dev"

	// Commit is the git commit SHA for the build.
	Commit = "none"

	// Date is the UTC build date in RFC3339 format.
	Date = "unknown"
)

// String returns a readable version summary for CLI output.
func String() string {
	return fmt.Sprintf("%s (commit=%s, date=%s)", Version, Commit, Date)
}
