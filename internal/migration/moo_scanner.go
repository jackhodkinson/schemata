package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var mooFilenameRegex = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}-.+)\.(txt|yml)$`)

// MooScanner scans a directory for moo-postgresql format migration files.
type MooScanner struct {
	directory string
}

// NewMooScanner creates a new moo-format migration scanner.
func NewMooScanner(directory string) *MooScanner {
	return &MooScanner{directory: directory}
}

// Scan finds all moo-format migration files in the directory.
// SQL and DependsOn are populated eagerly during scan.
func (s *MooScanner) Scan() ([]Migration, error) {
	if _, err := os.Stat(s.directory); os.IsNotExist(err) {
		return []Migration{}, nil
	}

	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		matches := mooFilenameRegex.FindStringSubmatch(entry.Name())
		if len(matches) != 3 {
			continue
		}

		version := matches[1] // full filename stem
		// Name is the slug after the YYYY-MM-DD- prefix
		name := version
		if len(version) > 11 {
			name = version[11:] // strip "YYYY-MM-DD-"
		}

		filePath := filepath.Join(s.directory, entry.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", entry.Name(), err)
		}

		sql, depends, err := parseMooFile(string(content))
		if err != nil {
			return nil, fmt.Errorf("failed to parse migration file %s: %w", entry.Name(), err)
		}

		m := Migration{
			Version:  version,
			Name:     name,
			FilePath: filePath,
			SQL:      sql,
		}
		if depends != "" {
			m.DependsOn = []string{depends}
		}

		migrations = append(migrations, m)
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// parseMooFile parses the content of a moo-format migration file.
// Returns the SQL content and the depends reference (empty string if root).
func parseMooFile(content string) (sql string, depends string, err error) {
	lines := strings.Split(content, "\n")
	inApply := false
	var sqlLines []string

	for _, line := range lines {
		if inApply {
			// Strip 2-space indent from Apply block lines
			if strings.HasPrefix(line, "  ") {
				sqlLines = append(sqlLines, line[2:])
			} else if strings.TrimSpace(line) == "" {
				sqlLines = append(sqlLines, "")
			} else {
				// Non-indented, non-empty line after Apply — stop
				sqlLines = append(sqlLines, line)
			}
			continue
		}

		if strings.HasPrefix(line, "Depends:") {
			depends = strings.TrimSpace(strings.TrimPrefix(line, "Depends:"))
			continue
		}

		if strings.HasPrefix(line, "Apply:") {
			inApply = true
			continue
		}
	}

	if !inApply {
		return "", "", fmt.Errorf("missing Apply: field")
	}

	// Trim trailing empty lines
	for len(sqlLines) > 0 && strings.TrimSpace(sqlLines[len(sqlLines)-1]) == "" {
		sqlLines = sqlLines[:len(sqlLines)-1]
	}

	return strings.Join(sqlLines, "\n"), depends, nil
}
