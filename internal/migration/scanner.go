package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Migration represents a single migration file
type Migration struct {
	Version  string // Timestamp prefix (YYYYMMDDHHMMSS)
	Name     string // Human-readable name
	FilePath string // Full path to the migration file
	SQL      string // SQL content (loaded on demand)
}

// Scanner scans a directory for migration files
type Scanner struct {
	directory string
}

// NewScanner creates a new migration scanner
func NewScanner(directory string) *Scanner {
	return &Scanner{directory: directory}
}

// Scan finds all migration files in the directory
func (s *Scanner) Scan() ([]Migration, error) {
	// Ensure directory exists
	if _, err := os.Stat(s.directory); os.IsNotExist(err) {
		// Directory doesn't exist, return empty list
		return []Migration{}, nil
	}

	// Read directory
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Check if file matches migration naming pattern
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		migration, err := parseMigrationFilename(entry.Name())
		if err != nil {
			// Not a valid migration file, skip
			continue
		}

		migration.FilePath = filepath.Join(s.directory, entry.Name())
		migrations = append(migrations, migration)
	}

	// Sort by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// LoadSQL loads the SQL content of a migration
func (m *Migration) LoadSQL() error {
	if m.SQL != "" {
		return nil // Already loaded
	}

	content, err := os.ReadFile(m.FilePath)
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}

	m.SQL = string(content)
	return nil
}

// Migration filename format: YYYYMMDDHHMMSS-name.sql
var migrationFilenameRegex = regexp.MustCompile(`^(\d{14})-(.+)\.sql$`)

// parseMigrationFilename parses a migration filename
func parseMigrationFilename(filename string) (Migration, error) {
	matches := migrationFilenameRegex.FindStringSubmatch(filename)
	if len(matches) != 3 {
		return Migration{}, fmt.Errorf("invalid migration filename format: %s", filename)
	}

	return Migration{
		Version: matches[1],
		Name:    matches[2],
	}, nil
}

// ValidateMigrationName checks if a name is valid for a migration
func ValidateMigrationName(name string) error {
	if name == "" {
		return fmt.Errorf("migration name cannot be empty")
	}

	// Check for invalid characters
	invalidChars := regexp.MustCompile(`[^a-zA-Z0-9_\-\s]`)
	if invalidChars.MatchString(name) {
		return fmt.Errorf("migration name contains invalid characters (only alphanumeric, underscore, hyphen, and space allowed)")
	}

	return nil
}

// ToKebabCase converts a string to kebab-case for use in filenames
func ToKebabCase(s string) string {
	// Replace spaces and underscores with hyphens
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")

	// Convert to lowercase
	s = strings.ToLower(s)

	// Remove consecutive hyphens
	multiHyphen := regexp.MustCompile(`-+`)
	s = multiHyphen.ReplaceAllString(s, "-")

	// Trim hyphens from start and end
	s = strings.Trim(s, "-")

	return s
}
