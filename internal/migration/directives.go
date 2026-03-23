package migration

import (
	"regexp"
	"strings"
)

var dependsOnRegex = regexp.MustCompile(`^--\s*schemata:depends-on\s+(\S+)\s*$`)

// parseDirectives scans lines from the start of SQL content for schemata directives.
// Returns version strings referenced by "-- schemata:depends-on <version>" directives.
// Stops scanning after the first non-comment, non-blank line.
func parseDirectives(sql string) []string {
	var deps []string
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "--") {
			break
		}
		if matches := dependsOnRegex.FindStringSubmatch(trimmed); len(matches) == 2 {
			deps = append(deps, matches[1])
		}
	}
	return deps
}
