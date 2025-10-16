package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Generator creates new migration files
type Generator struct {
	directory string
}

// NewGenerator creates a new migration generator
func NewGenerator(directory string) *Generator {
	return &Generator{directory: directory}
}

// Generate creates a new migration file with the given SQL content
func (g *Generator) Generate(name, sql string) (*Migration, error) {
	// Validate migration name
	if err := ValidateMigrationName(name); err != nil {
		return nil, err
	}

	// Ensure migrations directory exists
	if err := os.MkdirAll(g.directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create migrations directory: %w", err)
	}

	// Generate version (timestamp)
	version := generateVersion()

	// Convert name to kebab-case
	kebabName := ToKebabCase(name)

	// Create filename
	filename := fmt.Sprintf("%s-%s.sql", version, kebabName)
	filePath := filepath.Join(g.directory, filename)

	// Check if file already exists
	if _, err := os.Stat(filePath); err == nil {
		return nil, fmt.Errorf("migration file already exists: %s", filename)
	}

	// Write migration file
	if err := os.WriteFile(filePath, []byte(sql), 0644); err != nil {
		return nil, fmt.Errorf("failed to write migration file: %w", err)
	}

	migration := &Migration{
		Version:  version,
		Name:     kebabName,
		FilePath: filePath,
		SQL:      sql,
	}

	return migration, nil
}

// CreateEmpty creates an empty migration file (for manual migrations)
func (g *Generator) CreateEmpty(name string) (*Migration, error) {
	return g.Generate(name, "-- TODO: Add migration SQL here\n")
}

// generateVersion creates a timestamp-based version string
func generateVersion() string {
	return time.Now().Format("20060102150405")
}
