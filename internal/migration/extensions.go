package migration

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

var createExtensionRegex = regexp.MustCompile(`(?i)CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"([^"]+)"|(\S+))`)

// FindExtensionsInMigrations scans migration SQL for CREATE EXTENSION statements
// and returns a deduplicated, sorted list of extension names found.
func FindExtensionsInMigrations(migrations []Migration) ([]string, error) {
	seen := make(map[string]bool)

	for i := range migrations {
		if migrations[i].SQL == "" {
			if err := migrations[i].LoadSQL(); err != nil {
				return nil, fmt.Errorf("failed to load migration %s: %w", migrations[i].Version, err)
			}
		}

		matches := createExtensionRegex.FindAllStringSubmatch(migrations[i].SQL, -1)
		for _, match := range matches {
			// match[1] is quoted name, match[2] is unquoted name
			name := match[1]
			if name == "" {
				name = match[2]
			}
			// Strip trailing semicolons or whitespace from unquoted names
			name = strings.TrimRight(name, "; \t")
			if name != "" {
				seen[name] = true
			}
		}
	}

	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

// VersionBefore returns a version string that sorts before the earliest
// migration version. Handles both SQL format (YYYYMMDDHHMMSS) and
// moo format (YYYY-MM-DD-name).
func VersionBefore(earliest string) string {
	// Try SQL format: YYYYMMDDHHMMSS (14 digits)
	if len(earliest) == 14 {
		if t, err := time.Parse("20060102150405", earliest); err == nil {
			return t.Add(-1 * time.Second).Format("20060102150405")
		}
	}

	// Try moo format: YYYY-MM-DD-name (date prefix is 10 chars)
	if len(earliest) >= 10 {
		dateStr := earliest[:10]
		if t, err := time.Parse("2006-01-02", dateStr); err == nil {
			// Use the day before, at end of day
			return t.Add(-1 * time.Second).Format("20060102150405")
		}
	}

	// Fallback: use current time
	return generateVersion()
}
