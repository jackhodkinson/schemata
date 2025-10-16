package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationFilenameParsing(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		wantVersion string
		wantName    string
		wantErr     bool
	}{
		{
			name:        "valid migration",
			filename:    "20231015120530-add-users-table.sql",
			wantVersion: "20231015120530",
			wantName:    "add-users-table",
			wantErr:     false,
		},
		{
			name:        "valid with underscores",
			filename:    "20231015120530-add_email_column.sql",
			wantVersion: "20231015120530",
			wantName:    "add_email_column",
			wantErr:     false,
		},
		{
			name:     "missing extension",
			filename: "20231015120530-migration",
			wantErr:  true,
		},
		{
			name:     "wrong extension",
			filename: "20231015120530-migration.txt",
			wantErr:  true,
		},
		{
			name:     "invalid version format",
			filename: "2023-10-15-migration.sql",
			wantErr:  true,
		},
		{
			name:     "no name part",
			filename: "20231015120530.sql",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mig, err := parseMigrationFilename(tt.filename)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, mig.Version)
			assert.Equal(t, tt.wantName, mig.Name)
		})
	}
}

func TestScanner(t *testing.T) {
	// Create temp directory with test migrations
	tmpDir, err := os.MkdirTemp("", "migrations-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test migration files
	migrations := []struct {
		filename string
		content  string
	}{
		{"20231015120530-first-migration.sql", "CREATE TABLE users (id INT);"},
		{"20231015130000-second-migration.sql", "ALTER TABLE users ADD COLUMN email TEXT;"},
		{"20231016090000-third-migration.sql", "CREATE INDEX idx_users_email ON users(email);"},
		{"README.md", "This is not a migration"}, // Should be ignored
	}

	for _, mig := range migrations {
		path := filepath.Join(tmpDir, mig.filename)
		err := os.WriteFile(path, []byte(mig.content), 0644)
		require.NoError(t, err)
	}

	// Test scanner
	scanner := NewScanner(tmpDir)
	found, err := scanner.Scan()
	require.NoError(t, err)

	// Should find 3 migrations (README.md should be ignored)
	assert.Len(t, found, 3)

	// Check sorting (should be in version order)
	assert.Equal(t, "20231015120530", found[0].Version)
	assert.Equal(t, "first-migration", found[0].Name)

	assert.Equal(t, "20231015130000", found[1].Version)
	assert.Equal(t, "second-migration", found[1].Name)

	assert.Equal(t, "20231016090000", found[2].Version)
	assert.Equal(t, "third-migration", found[2].Name)
}

func TestScannerNonexistentDirectory(t *testing.T) {
	scanner := NewScanner("/nonexistent/directory")
	migrations, err := scanner.Scan()

	require.NoError(t, err) // Should not error on nonexistent dir
	assert.Empty(t, migrations)
}

func TestMigrationLoadSQL(t *testing.T) {
	// Create temp file
	tmpDir, err := os.MkdirTemp("", "migrations-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	content := "CREATE TABLE test (id INT);"
	filename := "20231015120530-test.sql"
	path := filepath.Join(tmpDir, filename)
	err = os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)

	// Create migration object
	mig := Migration{
		Version:  "20231015120530",
		Name:     "test",
		FilePath: path,
	}

	// Load SQL
	err = mig.LoadSQL()
	require.NoError(t, err)
	assert.Equal(t, content, mig.SQL)

	// Loading again should be no-op
	err = mig.LoadSQL()
	require.NoError(t, err)
	assert.Equal(t, content, mig.SQL)
}

func TestToKebabCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"add users table", "add-users-table"},
		{"Add_Users_Table", "add-users-table"},
		{"add   multiple   spaces", "add-multiple-spaces"},
		{"already-kebab-case", "already-kebab-case"},
		{"MixedCASE", "mixedcase"},
		{"--leading-trailing--", "leading-trailing"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ToKebabCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateMigrationName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple name", "add users table", false},
		{"valid with underscores", "add_users_table", false},
		{"valid with hyphens", "add-users-table", false},
		{"empty name", "", true},
		{"invalid characters", "add@users#table", true},
		{"valid alphanumeric", "add123users", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMigrationName(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
