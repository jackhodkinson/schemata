package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMooFile(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		wantSQL     string
		wantDepends string
		wantErr     string
	}{
		{
			name: "complete file with all fields",
			content: `Description: Add scheduling tables
Created: 2026-02-27 13:37:40.081085713 UTC
Depends: 2026-02-25-rack-and-slot-service-status
Apply: |
  CREATE TABLE production.grid_schedule (
    task_id uuid PRIMARY KEY
  );`,
			wantSQL: "CREATE TABLE production.grid_schedule (\n  task_id uuid PRIMARY KEY\n);",
			wantDepends: "2026-02-25-rack-and-slot-service-status",
		},
		{
			name: "root migration with empty depends",
			content: `Description: New epoch for DB migration
Created: 2026-02-26 12:00:00.000000000 UTC
Depends:
Apply: |
  CREATE SCHEMA analytics;
  CREATE SCHEMA finance;`,
			wantSQL:     "CREATE SCHEMA analytics;\nCREATE SCHEMA finance;",
			wantDepends: "",
		},
		{
			name: "multi-statement SQL block",
			content: `Description: Add columns
Created: 2026-03-01 12:00:00.000000000 UTC
Depends: 2026-02-26-epoch
Apply: |
  ALTER TABLE rack
    ADD COLUMN in_service boolean;

  UPDATE rack SET in_service = true;

  ALTER TABLE rack
    ALTER COLUMN in_service SET NOT NULL;`,
			wantSQL:     "ALTER TABLE rack\n  ADD COLUMN in_service boolean;\n\nUPDATE rack SET in_service = true;\n\nALTER TABLE rack\n  ALTER COLUMN in_service SET NOT NULL;",
			wantDepends: "2026-02-26-epoch",
		},
		{
			name: "missing Apply field",
			content: `Description: Bad migration
Created: 2026-03-01 12:00:00.000000000 UTC
Depends:`,
			wantErr: "missing Apply: field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, depends, err := parseMooFile(tt.content)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantDepends, depends)
		})
	}
}

func TestMooScanner(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "moo-migrations-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test migration files
	files := map[string]string{
		"2026-02-26-epoch.txt": `Description: New epoch
Created: 2026-02-26 12:00:00.000000000 UTC
Depends:
Apply: |
  CREATE SCHEMA analytics;`,

		"2026-02-27-add-tables.yml": `Description: Add scheduling tables
Created: 2026-02-27 13:37:40.081085713 UTC
Depends: 2026-02-26-epoch
Apply: |
  CREATE TABLE production.grid_schedule (
    task_id uuid PRIMARY KEY
  );`,

		"2026-03-04-add-log.txt": `Description: Add log table
Created: 2026-03-04 12:31:01.854591153 UTC
Depends: 2026-02-27-add-tables
Apply: |
  CREATE TABLE log.grid_schedule (
    task_id uuid PRIMARY KEY
  );`,

		"README.md":              "Not a migration",
		"20231015120530-test.sql": "CREATE TABLE test (id INT);",
	}

	for name, content := range files {
		err := os.WriteFile(filepath.Join(tmpDir, name), []byte(content), 0644)
		require.NoError(t, err)
	}

	scanner := NewMooScanner(tmpDir)
	migrations, err := scanner.Scan()
	require.NoError(t, err)

	// Should find 3 moo files, ignore README.md and .sql
	require.Len(t, migrations, 3)

	// Sorted by version (filename stem)
	assert.Equal(t, "2026-02-26-epoch", migrations[0].Version)
	assert.Equal(t, "2026-02-27-add-tables", migrations[1].Version)
	assert.Equal(t, "2026-03-04-add-log", migrations[2].Version)

	// Name is slug portion after date
	assert.Equal(t, "epoch", migrations[0].Name)
	assert.Equal(t, "add-tables", migrations[1].Name)
	assert.Equal(t, "add-log", migrations[2].Name)

	// SQL is populated eagerly
	assert.Contains(t, migrations[0].SQL, "CREATE SCHEMA analytics;")
	assert.Contains(t, migrations[1].SQL, "CREATE TABLE production.grid_schedule")
	assert.Contains(t, migrations[2].SQL, "CREATE TABLE log.grid_schedule")

	// DependsOn populated from Depends field
	assert.Empty(t, migrations[0].DependsOn)
	assert.Equal(t, []string{"2026-02-26-epoch"}, migrations[1].DependsOn)
	assert.Equal(t, []string{"2026-02-27-add-tables"}, migrations[2].DependsOn)

	// LoadSQL is a no-op since SQL is already set
	err = migrations[0].LoadSQL()
	require.NoError(t, err)
}

func TestMooScanner_NonexistentDirectory(t *testing.T) {
	scanner := NewMooScanner("/nonexistent/directory")
	migrations, err := scanner.Scan()
	require.NoError(t, err)
	assert.Empty(t, migrations)
}

func TestMooScanner_EmptyDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "moo-empty-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	scanner := NewMooScanner(tmpDir)
	migrations, err := scanner.Scan()
	require.NoError(t, err)
	assert.Empty(t, migrations)
}

func TestMooScanner_DependencyOrdering(t *testing.T) {
	// Create files where date order differs from dependency order
	// (temporal inversion — like the real circiuthub migrations)
	tmpDir, err := os.MkdirTemp("", "moo-ordering-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	files := map[string]string{
		// March file depends on April file (temporal inversion)
		"2026-03-18-shipment-eta.txt": `Description: Shipment ETA
Created: 2026-03-18 12:00:00.000000000 UTC
Depends: 2026-03-19-operator-station
Apply: |
  ALTER TABLE shipment ADD COLUMN eta timestamptz;`,

		"2026-03-19-operator-station.txt": `Description: Operator station
Created: 2026-03-19 12:00:00.000000000 UTC
Depends:
Apply: |
  CREATE TABLE operator_station (id serial PRIMARY KEY);`,
	}

	for name, content := range files {
		err := os.WriteFile(filepath.Join(tmpDir, name), []byte(content), 0644)
		require.NoError(t, err)
	}

	scanner := NewMooScanner(tmpDir)
	migrations, err := scanner.Scan()
	require.NoError(t, err)
	require.Len(t, migrations, 2)

	// Scanner sorts by version (lexicographic), so March comes first
	assert.Equal(t, "2026-03-18-shipment-eta", migrations[0].Version)
	assert.Equal(t, "2026-03-19-operator-station", migrations[1].Version)

	// But topo sort should put operator-station first (it's the root)
	sorted, err := topoSortMigrations(migrations)
	require.NoError(t, err)
	assert.Equal(t, "2026-03-19-operator-station", sorted[0].Version)
	assert.Equal(t, "2026-03-18-shipment-eta", sorted[1].Version)
}
