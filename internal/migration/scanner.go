package migration

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Migration represents a single migration file
type Migration struct {
	Version       string   // Timestamp prefix (YYYYMMDDHHMMSS)
	Name          string   // Human-readable name
	FilePath      string   // Full path to the migration file
	SQL           string   // SQL content (loaded on demand)
	Statements    []string // Parsed top-level SQL statements
	Checksum      string   // SHA-256 of the exact source file bytes
	ExecutionMode string   // transactional or non_transactional
	DependsOn     []string // Versions this migration depends on (parsed from directives)

	sourceBytes               []byte
	authoritativeSQL          string
	authoritativeDependencies []string
	dependenciesAuthoritative bool
}

const (
	ExecutionModeTransactional    = "transactional"
	ExecutionModeNonTransactional = "non_transactional"
)

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
	for i := 1; i < len(migrations); i++ {
		if migrations[i-1].Version == migrations[i].Version {
			return nil, fmt.Errorf("duplicate migration version %s in %q and %q",
				migrations[i].Version, migrations[i-1].FilePath, migrations[i].FilePath)
		}
	}

	return migrations, nil
}

// LoadSQL loads the SQL content of a migration
func (m *Migration) LoadSQL() error {
	if m.sourceBytes != nil {
		if m.SQL != m.authoritativeSQL {
			return fmt.Errorf("migration SQL changed after its authoritative source was loaded")
		}
	} else if m.SQL != "" || m.FilePath == "" {
		m.setAuthoritativeSource([]byte(m.SQL), m.SQL)
	} else {
		content, err := os.ReadFile(m.FilePath)
		if err != nil {
			return fmt.Errorf("failed to read migration file: %w", err)
		}
		m.SQL = string(content)
		m.setAuthoritativeSource(content, m.SQL)
	}

	// Always derive the public checksum. Never trust a caller-supplied or stale
	// value when deciding what immutable identity will be recorded.
	m.Checksum = migrationChecksum(m.sourceBytes)
	directives, err := parseMigrationDirectives(m.SQL)
	if err != nil {
		return err
	}
	if m.dependenciesAuthoritative {
		m.DependsOn = cloneDependencies(m.authoritativeDependencies)
	} else {
		m.DependsOn = directives.DependsOn
	}
	// Execution policy is part of the checksummed source and is never trusted
	// from a mutable, caller-supplied struct field.
	m.ExecutionMode = directives.ExecutionMode
	return m.prepareStatements()
}

func (m *Migration) setAuthoritativeSource(source []byte, sql string) {
	m.sourceBytes = make([]byte, len(source))
	copy(m.sourceBytes, source)
	m.authoritativeSQL = sql
}

func (m *Migration) setAuthoritativeDependencies(dependencies []string) {
	m.authoritativeDependencies = cloneDependencies(dependencies)
	m.DependsOn = cloneDependencies(dependencies)
	m.dependenciesAuthoritative = true
}

func cloneDependencies(dependencies []string) []string {
	cloned := make([]string, len(dependencies))
	copy(cloned, dependencies)
	return cloned
}

func migrationChecksum(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
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
