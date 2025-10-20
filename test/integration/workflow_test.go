package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndToEndWorkflow tests the complete workflow:
// 1. Parse schema.sql
// 2. Generate migrations
// 3. Apply migrations to target
// 4. Verify target matches schema
func TestEndToEndWorkflow(t *testing.T) {
	ctx := context.Background()

	// Connect to dev and target databases
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	targetConn := &config.DBConnection{URL: strPtr(targetDBURL)}

	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	targetPool, err := db.Connect(ctx, targetConn)
	require.NoError(t, err)
	defer targetPool.Close()

	// Clean up both databases
	cleanup(t, ctx, devPool, targetPool)
	defer cleanup(t, ctx, devPool, targetPool)

	// Create temporary test directory
	tmpDir := t.TempDir()
	migrationsDir := filepath.Join(tmpDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	require.NoError(t, err)

	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err = os.WriteFile(schemaFile, []byte(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			content TEXT,
			published BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		);

		CREATE INDEX idx_posts_user_id ON posts(user_id);
	`), 0644)
	require.NoError(t, err)

	// Step 1: Parse schema.sql
	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err, "should parse schema.sql")
	assert.Greater(t, len(desiredSchema), 0, "should have parsed objects")

	// Debug: check what was parsed
	for key, obj := range desiredSchema {
		if tbl, ok := obj.Payload.(schema.Table); ok && tbl.Name == "posts" {
			t.Logf("Parsed posts table: %d foreign keys", len(tbl.ForeignKeys))
			for i, fk := range tbl.ForeignKeys {
				t.Logf("  FK %d: %s cols=%v -> %s.%s(%v)", i, fk.Name, fk.Cols, fk.Ref.Schema, fk.Ref.Table, fk.Ref.Cols)
			}
		}
		t.Logf("Parsed object: %v", key)
	}

	// Step 2: Generate initial migration (dev DB is empty, so diff against it)
	// Start with empty actual schema (dev DB is clean)
	actualSchema := make(schema.SchemaObjectMap)

	// Compute diff
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err)

	// Generate DDL for the diff
	ddlGen := planner.NewDDLGenerator()
	ddl, err := ddlGen.GenerateDDL(diff, desiredSchema)
	require.NoError(t, err)
	assert.NotEmpty(t, ddl, "should generate DDL for initial schema")
	t.Logf("Generated DDL:\n%s", ddl)

	// Write migration file
	timestamp := time.Now().Format("20060102150405")
	migrationFile := filepath.Join(migrationsDir, timestamp+"-initial-schema.sql")
	err = os.WriteFile(migrationFile, []byte(ddl), 0644)
	require.NoError(t, err)

	// Step 3: Apply migration to target
	scanner := migration.NewScanner(migrationsDir)
	migrations, err := scanner.Scan()
	require.NoError(t, err)
	require.Len(t, migrations, 1, "should find 1 migration")

	applier := migration.NewApplier(targetPool, false)
	opts := migration.ApplyOptions{
		DryRun:          false,
		ContinueOnError: false,
	}
	err = applier.Apply(ctx, migrations, opts)
	require.NoError(t, err, "should apply migrations to target")

	// Step 4: Verify target matches schema
	targetCatalog := db.NewCatalog(targetPool)
	targetObjects, err := targetCatalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "schemata"})
	require.NoError(t, err)

	// Verify tables exist
	foundUsers := false
	foundPosts := false
	for _, obj := range targetObjects {
		if tbl, ok := obj.(schema.Table); ok {
			if tbl.Name == "users" {
				foundUsers = true
				assert.NotNil(t, tbl.PrimaryKey, "users should have primary key")
			}
			if tbl.Name == "posts" {
				foundPosts = true
				assert.Len(t, tbl.ForeignKeys, 1, "posts should have foreign key to users")
			}
		}
	}

	assert.True(t, foundUsers, "should create users table")
	assert.True(t, foundPosts, "should create posts table")
}

// TestDiffWorkflow tests the diff command workflow
func TestDiffWorkflow(t *testing.T) {
	ctx := context.Background()

	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	// Clean up
	cleanup(t, ctx, devPool, nil)
	defer cleanup(t, ctx, devPool, nil)

	// Create initial schema in dev database
	_, err = devPool.Exec(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL
		);
	`)
	require.NoError(t, err)

	// Create desired schema file with additional column
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err = os.WriteFile(schemaFile, []byte(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT NOT NULL
		);
	`), 0644)
	require.NoError(t, err)

	// Parse desired schema
	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err)

	// Extract actual schema
	catalog := db.NewCatalog(devPool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema"})
	require.NoError(t, err)

	// Build actual schema map
	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	// Compute diff
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err)

	// Should detect the missing column
	assert.False(t, diff.IsEmpty(), "should detect differences")
	assert.Greater(t, len(diff.ToAlter), 0, "should have alter operations")

	// Generate DDL
	ddlGen := planner.NewDDLGenerator()
	ddl, err := ddlGen.GenerateDDL(diff, desiredSchema)
	require.NoError(t, err)
	assert.Contains(t, ddl, "ALTER TABLE", "should generate ALTER TABLE statement")
	assert.Contains(t, ddl, "ADD COLUMN name", "should add name column")
}

// TestGenerateWorkflow tests the generate command workflow
func TestGenerateWorkflow(t *testing.T) {
	ctx := context.Background()

	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	cleanup(t, ctx, devPool, nil)
	defer cleanup(t, ctx, devPool, nil)

	tmpDir := t.TempDir()
	migrationsDir := filepath.Join(tmpDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	require.NoError(t, err)

	// Create initial migration
	migration1 := filepath.Join(migrationsDir, "20230101120000-initial.sql")
	err = os.WriteFile(migration1, []byte(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL
		);
	`), 0644)
	require.NoError(t, err)

	// Apply existing migration to dev
	scanner := migration.NewScanner(migrationsDir)
	migrations, err := scanner.Scan()
	require.NoError(t, err)

	applier := migration.NewApplier(devPool, false)
	opts := migration.ApplyOptions{
		DryRun:          false,
		ContinueOnError: false,
	}
	err = applier.Apply(ctx, migrations, opts)
	require.NoError(t, err)

	// Create updated schema file
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err = os.WriteFile(schemaFile, []byte(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL,
			username TEXT UNIQUE
		);
	`), 0644)
	require.NoError(t, err)

	// Parse new schema
	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err)

	// Extract current dev schema
	catalog := db.NewCatalog(devPool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "schemata"})
	require.NoError(t, err)

	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	// Compute diff
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err)

	assert.False(t, diff.IsEmpty(), "should detect schema changes")

	// Generate new migration
	ddlGen := planner.NewDDLGenerator()
	ddl, err := ddlGen.GenerateDDL(diff, desiredSchema)
	require.NoError(t, err)

	// Write new migration
	migration2 := filepath.Join(migrationsDir, "20230101130000-add-username.sql")
	err = os.WriteFile(migration2, []byte(ddl), 0644)
	require.NoError(t, err)

	// Verify migration was created
	migrations2, err := scanner.Scan()
	require.NoError(t, err)
	assert.Len(t, migrations2, 2, "should have 2 migrations")
}

// TestMigrateWithPreflightCheck tests migrate command with preflight check
func TestMigrateWithPreflightCheck(t *testing.T) {
	ctx := context.Background()

	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	targetConn := &config.DBConnection{URL: strPtr(targetDBURL)}

	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	targetPool, err := db.Connect(ctx, targetConn)
	require.NoError(t, err)
	defer targetPool.Close()

	cleanup(t, ctx, devPool, targetPool)
	defer cleanup(t, ctx, devPool, targetPool)

	tmpDir := t.TempDir()
	migrationsDir := filepath.Join(tmpDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	require.NoError(t, err)

	schemaFile := filepath.Join(tmpDir, "schema.sql")

	// Create schema file
	err = os.WriteFile(schemaFile, []byte(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL
		);
	`), 0644)
	require.NoError(t, err)

	// Create matching migration
	migration1 := filepath.Join(migrationsDir, "20230101120000-create-users.sql")
	err = os.WriteFile(migration1, []byte(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL
		);
	`), 0644)
	require.NoError(t, err)

	// Apply migrations to dev to verify sync
	scanner := migration.NewScanner(migrationsDir)
	migrations, err := scanner.Scan()
	require.NoError(t, err)

	devApplier := migration.NewApplier(devPool, false)
	opts := migration.ApplyOptions{
		DryRun:          false,
		ContinueOnError: false,
	}
	err = devApplier.Apply(ctx, migrations, opts)
	require.NoError(t, err)

	// Parse schema
	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err)

	// Extract dev schema after migrations
	catalog := db.NewCatalog(devPool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "schemata"})
	require.NoError(t, err)

	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	// Verify they match (preflight check passes)
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err)

	// Debug: show what differences were found
	if !diff.IsEmpty() {
		t.Logf("Diff is NOT empty:")
		t.Logf("  ToCreate: %d objects", len(diff.ToCreate))
		for _, key := range diff.ToCreate {
			t.Logf("    CREATE: %v", key)
		}
		t.Logf("  ToDrop: %d objects", len(diff.ToDrop))
		for _, key := range diff.ToDrop {
			t.Logf("    DROP: %v", key)
		}
		t.Logf("  ToAlter: %d objects", len(diff.ToAlter))
		for _, alter := range diff.ToAlter {
			t.Logf("    ALTER: %v", alter.Key)
			for _, change := range alter.Changes {
				t.Logf("      - %s", change)
			}
		}
	}

	assert.True(t, diff.IsEmpty(), "migrations should be in sync with schema.sql")

	// Now apply to target
	targetApplier := migration.NewApplier(targetPool, false)
	err = targetApplier.Apply(ctx, migrations, opts)
	require.NoError(t, err)

	// Verify target has the table
	var tableExists bool
	err = targetPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'users'
		)
	`).Scan(&tableExists)
	require.NoError(t, err)
	assert.True(t, tableExists, "target should have users table")
}

// TestALTEROperations tests various ALTER TABLE operations
func TestALTEROperations(t *testing.T) {
	ctx := context.Background()

	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	cleanup(t, ctx, devPool, nil)
	defer cleanup(t, ctx, devPool, nil)

	// Create initial table
	_, err = devPool.Exec(ctx, `
		CREATE TABLE test_alters (
			id SERIAL PRIMARY KEY,
			old_column TEXT
		);
	`)
	require.NoError(t, err)

	tests := []struct {
		name           string
		desiredSchema  string
		expectedDDL    []string
		notExpectedDDL []string
	}{
		{
			name: "add column",
			desiredSchema: `
				CREATE TABLE test_alters (
					id SERIAL PRIMARY KEY,
					old_column TEXT,
					new_column TEXT NOT NULL DEFAULT 'default'
				);
			`,
			expectedDDL: []string{
				"ALTER TABLE",
				"ADD COLUMN new_column",
				"NOT NULL",
				"DEFAULT",
			},
		},
		{
			name: "drop column",
			desiredSchema: `
				CREATE TABLE test_alters (
					id SERIAL PRIMARY KEY
				);
			`,
			expectedDDL: []string{
				"ALTER TABLE",
				"DROP COLUMN old_column",
			},
		},
		{
			name: "alter column type",
			desiredSchema: `
				CREATE TABLE test_alters (
					id SERIAL PRIMARY KEY,
					old_column INTEGER
				);
			`,
			expectedDDL: []string{
				"ALTER TABLE",
				"ALTER COLUMN old_column TYPE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			schemaFile := filepath.Join(tmpDir, "schema.sql")
			err := os.WriteFile(schemaFile, []byte(tt.desiredSchema), 0644)
			require.NoError(t, err)

			// Parse desired schema
			p := parser.NewParser()
			desiredSchema, err := p.ParseFile(schemaFile)
			require.NoError(t, err)

			// Extract actual schema
			catalog := db.NewCatalog(devPool)
			actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "schemata"})
			require.NoError(t, err)

			actualSchema, err := buildObjectMapFromObjects(actualObjects)
			require.NoError(t, err)

			// Compute diff
			d := differ.NewDiffer()
			diff, err := d.Diff(desiredSchema, actualSchema)
			require.NoError(t, err)

			// Generate DDL
			ddlGen := planner.NewDDLGenerator()
			ddl, err := ddlGen.GenerateDDL(diff, desiredSchema)
			require.NoError(t, err)

			// Verify expected DDL fragments
			for _, expected := range tt.expectedDDL {
				assert.Contains(t, ddl, expected, "DDL should contain: %s", expected)
			}

			// Verify unexpected DDL fragments
			for _, notExpected := range tt.notExpectedDDL {
				assert.NotContains(t, ddl, notExpected, "DDL should not contain: %s", notExpected)
			}
		})
	}
}

// Helper functions

func cleanup(t *testing.T, ctx context.Context, devPool, targetPool *db.Pool) {
	t.Helper()

	if devPool != nil {
		_, _ = devPool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS users CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS posts CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS comments CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS test_alters CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS test_migration_table CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS test_dryrun_table CASCADE")
	}

	if targetPool != nil {
		_, _ = targetPool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
		_, _ = targetPool.Exec(ctx, "DROP TABLE IF EXISTS users CASCADE")
		_, _ = targetPool.Exec(ctx, "DROP TABLE IF EXISTS posts CASCADE")
		_, _ = targetPool.Exec(ctx, "DROP TABLE IF EXISTS comments CASCADE")
	}
}
