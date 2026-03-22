//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

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

// --- test harness ---

// testDB wraps a pool with helpers for integration tests.
type testDB struct {
	pool *db.Pool
}

func newTestDB(t *testing.T, url string) *testDB {
	t.Helper()
	ctx := context.Background()
	conn := &config.DBConnection{URL: &url}
	pool, err := db.Connect(ctx, conn)
	require.NoError(t, err, "failed to connect to %s", url)
	t.Cleanup(func() { pool.Close() })
	return &testDB{pool: pool}
}

// reset drops everything: public schema + schemata tracking schema.
func (d *testDB) reset(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	require.NoError(t, err)
	err = resetPublicSchema(ctx, d.pool)
	require.NoError(t, err)
}

// execSQL runs raw SQL against the database.
func (d *testDB) execSQL(t *testing.T, sql string) {
	t.Helper()
	_, err := d.pool.Exec(context.Background(), sql)
	require.NoError(t, err, "failed to exec SQL:\n%s", truncate(sql, 200))
}

// extractSchema returns the current schema from the database as an object map.
func (d *testDB) extractSchema(t *testing.T) schema.SchemaObjectMap {
	t.Helper()
	catalog := db.NewCatalog(d.pool)
	objects, err := catalog.ExtractAllObjects(
		context.Background(),
		[]string{"public"},
		[]string{"pg_catalog", "information_schema", "pg_toast", "schemata"},
	)
	require.NoError(t, err, "failed to extract catalog")
	m, err := buildObjectMapFromObjects(objects)
	require.NoError(t, err, "failed to build object map")
	return m
}

// tableExists returns true if the given table exists in public schema.
func (d *testDB) tableExists(t *testing.T, table string) bool {
	t.Helper()
	var exists bool
	err := d.pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)
	`, table).Scan(&exists)
	require.NoError(t, err)
	return exists
}

// columnExists returns true if the given column exists on the table.
func (d *testDB) columnExists(t *testing.T, table, column string) bool {
	t.Helper()
	var exists bool
	err := d.pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
		)
	`, table, column).Scan(&exists)
	require.NoError(t, err)
	return exists
}

// appliedVersions returns the list of applied migration versions.
func (d *testDB) appliedVersions(t *testing.T) []string {
	t.Helper()
	tracker := db.NewMigrationTracker(d.pool)
	v, err := tracker.GetAppliedVersions(context.Background())
	require.NoError(t, err)
	return v
}

// --- pipeline helpers ---

// parseSQL parses a schema string and returns the object map.
func parseSQL(t *testing.T, sql string) schema.SchemaObjectMap {
	t.Helper()
	p := parser.NewParser()
	m, err := p.ParseSQL(sql)
	require.NoError(t, err, "failed to parse SQL")
	return m
}

// diffSchemas computes the diff between desired and actual.
func diffSchemas(t *testing.T, desired, actual schema.SchemaObjectMap) *differ.Diff {
	t.Helper()
	d := differ.NewDiffer()
	diff, err := d.Diff(desired, actual)
	require.NoError(t, err, "failed to compute diff")
	return diff
}

// generateDDL generates DDL for a diff. Returns empty string if diff is empty.
// If actual is provided, it's used for correct DROP ordering.
func generateDDL(t *testing.T, diff *differ.Diff, desired schema.SchemaObjectMap, actual ...schema.SchemaObjectMap) string {
	t.Helper()
	if diff.IsEmpty() {
		return ""
	}
	gen := planner.NewDDLGenerator()
	ddl, err := gen.GenerateDDL(diff, desired, actual...)
	require.NoError(t, err, "failed to generate DDL")
	return ddl
}

// logDiff logs a non-empty diff for debugging.
func logDiff(t *testing.T, diff *differ.Diff) {
	t.Helper()
	if diff.IsEmpty() {
		return
	}
	for _, k := range diff.ToCreate {
		t.Logf("  + CREATE %s %s.%s", k.Kind, k.Schema, k.Name)
	}
	for _, k := range diff.ToDrop {
		t.Logf("  - DROP %s %s.%s", k.Kind, k.Schema, k.Name)
	}
	for _, a := range diff.ToAlter {
		t.Logf("  ~ ALTER %s %s.%s: %v", a.Key.Kind, a.Key.Schema, a.Key.Name, a.Changes)
	}
}

// evolve performs one schema evolution step:
//  1. Parse desiredSQL
//  2. Diff against current DB state
//  3. Generate DDL migration
//  4. Apply migration to DB
//  5. Verify DB now matches desired (round-trip check)
//
// Returns the generated DDL for inspection.
func evolve(t *testing.T, d *testDB, stepName, desiredSQL string) string {
	t.Helper()

	desired := parseSQL(t, desiredSQL)
	actual := d.extractSchema(t)
	diff := diffSchemas(t, desired, actual)

	if diff.IsEmpty() {
		t.Fatalf("step %q: expected schema differences but diff was empty", stepName)
	}

	ddl := generateDDL(t, diff, desired, actual)
	require.NotEmpty(t, ddl, "step %q: generated DDL should not be empty", stepName)
	t.Logf("step %q: applying DDL (%d bytes)", stepName, len(ddl))

	d.execSQL(t, ddl)

	// Round-trip verification: extract again and diff must be empty
	actualAfter := d.extractSchema(t)
	verifyDiff := diffSchemas(t, desired, actualAfter)
	if !verifyDiff.IsEmpty() {
		t.Logf("step %q: round-trip verification FAILED — residual diff:", stepName)
		logDiff(t, verifyDiff)
	}
	assert.True(t, verifyDiff.IsEmpty(),
		"step %q: after applying generated DDL, database should match desired schema exactly", stepName)

	return ddl
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// =============================================================================
// Test 1: Multi-step schema evolution
// =============================================================================

func TestSchemaEvolution_MultiStep(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	// --- V1: initial schema ---
	evolve(t, devDB, "V1: initial tables", `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL REFERENCES users(id),
			title TEXT NOT NULL,
			body TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_posts_user_id ON posts(user_id);
	`)

	// --- V2: add columns, new table, new index ---
	evolve(t, devDB, "V2: add columns + comments table", `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			username TEXT UNIQUE,
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL REFERENCES users(id),
			title TEXT NOT NULL,
			body TEXT,
			is_published BOOLEAN DEFAULT false,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_posts_user_id ON posts(user_id);
		CREATE INDEX idx_posts_published ON posts(is_published) WHERE is_published = true;

		CREATE TABLE comments (
			id SERIAL PRIMARY KEY,
			post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
			user_id INTEGER NOT NULL REFERENCES users(id),
			body TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_comments_post_id ON comments(post_id);
	`)

	// --- V3: drop a column, change a type, add FK on delete behavior ---
	evolve(t, devDB, "V3: drop column + alter type", `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			username TEXT UNIQUE,
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			title VARCHAR(500) NOT NULL,
			is_published BOOLEAN DEFAULT false,
			published_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_posts_user_id ON posts(user_id);
		CREATE INDEX idx_posts_published ON posts(is_published) WHERE is_published = true;

		CREATE TABLE comments (
			id SERIAL PRIMARY KEY,
			post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
			user_id INTEGER NOT NULL REFERENCES users(id),
			body TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_comments_post_id ON comments(post_id);
	`)

	// --- V4: add function + trigger ---
	evolve(t, devDB, "V4: add function + trigger", `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			username TEXT UNIQUE,
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			title VARCHAR(500) NOT NULL,
			is_published BOOLEAN DEFAULT false,
			published_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_posts_user_id ON posts(user_id);
		CREATE INDEX idx_posts_published ON posts(is_published) WHERE is_published = true;

		CREATE TABLE comments (
			id SERIAL PRIMARY KEY,
			post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
			user_id INTEGER NOT NULL REFERENCES users(id),
			body TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_comments_post_id ON comments(post_id);

		CREATE OR REPLACE FUNCTION set_updated_at()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER trg_users_updated_at
			BEFORE UPDATE ON users
			FOR EACH ROW
			EXECUTE FUNCTION set_updated_at();

		CREATE TRIGGER trg_posts_updated_at
			BEFORE UPDATE ON posts
			FOR EACH ROW
			EXECUTE FUNCTION set_updated_at();
	`)

	// --- V5: add enum type, use it in a column, add a new table that uses it ---
	evolve(t, devDB, "V5: add enum + tags table", `
		CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');

		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			username TEXT UNIQUE,
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			title VARCHAR(500) NOT NULL,
			status post_status DEFAULT 'draft',
			is_published BOOLEAN DEFAULT false,
			published_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_posts_user_id ON posts(user_id);
		CREATE INDEX idx_posts_published ON posts(is_published) WHERE is_published = true;

		CREATE TABLE comments (
			id SERIAL PRIMARY KEY,
			post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
			user_id INTEGER NOT NULL REFERENCES users(id),
			body TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_comments_post_id ON comments(post_id);

		CREATE TABLE tags (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL UNIQUE
		);

		CREATE TABLE post_tags (
			post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
			tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
			PRIMARY KEY (post_id, tag_id)
		);

		CREATE OR REPLACE FUNCTION set_updated_at()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER trg_users_updated_at
			BEFORE UPDATE ON users
			FOR EACH ROW
			EXECUTE FUNCTION set_updated_at();

		CREATE TRIGGER trg_posts_updated_at
			BEFORE UPDATE ON posts
			FOR EACH ROW
			EXECUTE FUNCTION set_updated_at();
	`)
}

// =============================================================================
// Test 2: Transaction rollback on migration failure
// =============================================================================

func TestMigrationRollback_OnFailure(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	// Apply a valid initial migration
	applier := migration.NewApplier(devDB.pool, false)
	err := applier.Apply(context.Background(), []migration.Migration{
		{
			Version:  "20240101000000",
			Name:     "init",
			SQL:      "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, val TEXT);",
			FilePath: "/tmp/test",
		},
	}, migration.ApplyOptions{})
	require.NoError(t, err)

	// Verify table exists and insert a row
	devDB.execSQL(t, "INSERT INTO rollback_test (id, val) VALUES (1, 'before')")

	// Apply a migration that will fail partway through.
	// The first statement succeeds, the second references a non-existent table.
	err = applier.Apply(context.Background(), []migration.Migration{
		{
			Version:  "20240101000000",
			Name:     "init",
			SQL:      "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, val TEXT);",
			FilePath: "/tmp/test",
		},
		{
			Version: "20240102000000",
			Name:    "bad-migration",
			SQL: `
				ALTER TABLE rollback_test ADD COLUMN new_col TEXT DEFAULT 'added';
				ALTER TABLE does_not_exist ADD COLUMN boom TEXT;
			`,
			FilePath: "/tmp/test",
		},
	}, migration.ApplyOptions{ContinueOnError: false})
	require.Error(t, err, "migration with invalid SQL should fail")

	// The failed migration's first statement (ADD COLUMN) should have been rolled back.
	assert.False(t, devDB.columnExists(t, "rollback_test", "new_col"),
		"column from failed migration should not exist — transaction should have rolled back")

	// The original data should still be intact.
	var val string
	err = devDB.pool.QueryRow(context.Background(),
		"SELECT val FROM rollback_test WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "before", val, "data should be intact after failed migration")

	// The failed migration version should NOT be recorded.
	versions := devDB.appliedVersions(t)
	assert.Equal(t, []string{"20240101000000"}, versions,
		"only the successful migration version should be recorded")

	// A subsequent valid migration should still work.
	err = applier.Apply(context.Background(), []migration.Migration{
		{
			Version:  "20240101000000",
			Name:     "init",
			SQL:      "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, val TEXT);",
			FilePath: "/tmp/test",
		},
		{
			Version:  "20240103000000",
			Name:     "add-col-properly",
			SQL:      "ALTER TABLE rollback_test ADD COLUMN status TEXT DEFAULT 'ok';",
			FilePath: "/tmp/test",
		},
	}, migration.ApplyOptions{})
	require.NoError(t, err, "valid migration after a failure should succeed")
	assert.True(t, devDB.columnExists(t, "rollback_test", "status"))
}

// =============================================================================
// Test 3: Concurrent migration runners
// =============================================================================

func TestConcurrentMigrationRunners(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	migrations := []migration.Migration{
		{
			Version:  "20240201000000",
			Name:     "create-table-a",
			SQL:      "CREATE TABLE concurrent_a (id INTEGER PRIMARY KEY);",
			FilePath: "/tmp/test",
		},
		{
			Version:  "20240201000001",
			Name:     "create-table-b",
			SQL:      "CREATE TABLE concurrent_b (id INTEGER PRIMARY KEY);",
			FilePath: "/tmp/test",
		},
		{
			Version:  "20240201000002",
			Name:     "create-table-c",
			SQL:      "CREATE TABLE concurrent_c (id INTEGER PRIMARY KEY);",
			FilePath: "/tmp/test",
		},
	}

	// Spin up multiple goroutines all trying to apply the same migrations.
	const numRunners = 5
	var wg sync.WaitGroup
	errs := make([]error, numRunners)

	for i := 0; i < numRunners; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn := &config.DBConnection{URL: strPtr(devDBURL)}
			pool, err := db.Connect(context.Background(), conn)
			if err != nil {
				errs[idx] = err
				return
			}
			defer pool.Close()

			applier := migration.NewApplier(pool, false)
			errs[idx] = applier.Apply(context.Background(), migrations, migration.ApplyOptions{})
		}(i)
	}

	wg.Wait()

	// All runners should complete without error.
	for i, err := range errs {
		assert.NoError(t, err, "runner %d should not fail", i)
	}

	// Each migration should be applied exactly once.
	versions := devDB.appliedVersions(t)
	assert.Equal(t, []string{"20240201000000", "20240201000001", "20240201000002"}, versions,
		"each migration should appear exactly once")

	// Tables should all exist.
	assert.True(t, devDB.tableExists(t, "concurrent_a"))
	assert.True(t, devDB.tableExists(t, "concurrent_b"))
	assert.True(t, devDB.tableExists(t, "concurrent_c"))
}

// =============================================================================
// Test 4: Idempotency — re-running migrate is a no-op
// =============================================================================

func TestMigrateIdempotency(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	schemaSQL := `
		CREATE TABLE idempotent_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX idx_idempotent_name ON idempotent_test(name);
	`

	// First run: evolve from empty to desired.
	evolve(t, devDB, "initial", schemaSQL)

	// Second run: diff should be empty, nothing to do.
	desired := parseSQL(t, schemaSQL)
	actual := devDB.extractSchema(t)
	diff := diffSchemas(t, desired, actual)
	assert.True(t, diff.IsEmpty(), "second diff should be empty — schema is already correct")
}

// =============================================================================
// Test 5: Complex dependency ordering
// =============================================================================

func TestDependencyOrdering_ComplexGraph(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	// Create a schema with a deep dependency chain:
	//   enum → table A (uses enum) → table B (FK to A) → table C (FK to B)
	//   function → trigger on C
	//   index on B
	//   view joining A and B
	schemaSQL := `
		CREATE TYPE priority_level AS ENUM ('low', 'medium', 'high', 'critical');

		CREATE TABLE teams (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			priority priority_level DEFAULT 'medium'
		);

		CREATE TABLE projects (
			id SERIAL PRIMARY KEY,
			team_id INTEGER NOT NULL REFERENCES teams(id),
			name TEXT NOT NULL,
			started_at DATE
		);

		CREATE INDEX idx_projects_team_id ON projects(team_id);

		CREATE TABLE tasks (
			id SERIAL PRIMARY KEY,
			project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
			title TEXT NOT NULL,
			done BOOLEAN DEFAULT false,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_tasks_project_id ON tasks(project_id);
		CREATE INDEX idx_tasks_open ON tasks(project_id) WHERE done = false;

		CREATE OR REPLACE FUNCTION notify_task_created()
		RETURNS TRIGGER AS $$
		BEGIN
			PERFORM pg_notify('task_created', NEW.id::text);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER trg_task_created
			AFTER INSERT ON tasks
			FOR EACH ROW
			EXECUTE FUNCTION notify_task_created();
	`

	// This must generate DDL in a valid execution order and apply cleanly.
	evolve(t, devDB, "complex deps from scratch", schemaSQL)
}

// =============================================================================
// Test 6: Self-referential foreign key
// =============================================================================

func TestSelfReferentialFK(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	schemaSQL := `
		CREATE TABLE categories (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			parent_id INTEGER REFERENCES categories(id)
		);

		CREATE INDEX idx_categories_parent ON categories(parent_id);
	`

	// Self-referential FK should not cause a circular dependency error.
	evolve(t, devDB, "self-referential FK", schemaSQL)

	// Now evolve: add a column to the self-referential table.
	evolve(t, devDB, "add column to self-ref table", `
		CREATE TABLE categories (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			parent_id INTEGER REFERENCES categories(id),
			sort_order INTEGER DEFAULT 0
		);

		CREATE INDEX idx_categories_parent ON categories(parent_id);
	`)
}

// =============================================================================
// Test 7: DDL round-trip for each object type
// =============================================================================

func TestRoundTrip_PerObjectType(t *testing.T) {
	devDB := newTestDB(t, devDBURL)

	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "table with all constraint types",
			sql: `
				CREATE TABLE rt_constrained (
					id SERIAL PRIMARY KEY,
					email TEXT NOT NULL UNIQUE,
					age INTEGER CHECK (age >= 0 AND age < 200),
					status TEXT DEFAULT 'active',
					created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				);
			`,
		},
		{
			name: "table with composite PK",
			sql: `
				CREATE TABLE rt_composite_pk (
					tenant_id INTEGER NOT NULL,
					user_id INTEGER NOT NULL,
					role TEXT NOT NULL DEFAULT 'member',
					PRIMARY KEY (tenant_id, user_id)
				);
			`,
		},
		{
			name: "partial index",
			sql: `
				CREATE TABLE rt_partial_idx (
					id SERIAL PRIMARY KEY,
					email TEXT,
					deleted_at TIMESTAMP
				);
				CREATE INDEX idx_rt_active_emails ON rt_partial_idx(email) WHERE deleted_at IS NULL;
			`,
		},
		{
			name: "expression index",
			sql: `
				CREATE TABLE rt_expr_idx (
					id SERIAL PRIMARY KEY,
					email TEXT NOT NULL
				);
				CREATE INDEX idx_rt_email_lower ON rt_expr_idx(lower(email));
			`,
		},
		{
			name: "enum type",
			sql: `
				CREATE TYPE rt_color AS ENUM ('red', 'green', 'blue');
				CREATE TABLE rt_with_enum (
					id SERIAL PRIMARY KEY,
					color rt_color DEFAULT 'red'
				);
			`,
		},
		{
			name: "function and trigger",
			sql: `
				CREATE TABLE rt_triggered (
					id SERIAL PRIMARY KEY,
					updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				);

				CREATE OR REPLACE FUNCTION rt_set_updated()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.updated_at = CURRENT_TIMESTAMP;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				CREATE TRIGGER trg_rt_updated
					BEFORE UPDATE ON rt_triggered
					FOR EACH ROW
					EXECUTE FUNCTION rt_set_updated();
			`,
		},
		{
			name: "foreign key with ON DELETE CASCADE",
			sql: `
				CREATE TABLE rt_parent (
					id SERIAL PRIMARY KEY,
					name TEXT NOT NULL
				);

				CREATE TABLE rt_child (
					id SERIAL PRIMARY KEY,
					parent_id INTEGER NOT NULL REFERENCES rt_parent(id) ON DELETE CASCADE,
					value TEXT
				);
			`,
		},
		{
			name: "table with SERIAL types",
			sql: `
				CREATE TABLE rt_serials (
					small_id SMALLSERIAL PRIMARY KEY,
					big_ref BIGSERIAL NOT NULL,
					name TEXT
				);
			`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			devDB.reset(t)

			desired := parseSQL(t, tc.sql)
			require.NotEmpty(t, desired, "parser should produce objects")

			// Generate DDL from parsed objects and apply
			diff := diffSchemas(t, desired, make(schema.SchemaObjectMap))
			ddl := generateDDL(t, diff, desired)
			require.NotEmpty(t, ddl)

			devDB.execSQL(t, ddl)

			// Extract from DB and verify round-trip
			actual := devDB.extractSchema(t)
			verifyDiff := diffSchemas(t, desired, actual)
			if !verifyDiff.IsEmpty() {
				t.Logf("round-trip failed for %q — residual diff:", tc.name)
				logDiff(t, verifyDiff)
			}
			assert.True(t, verifyDiff.IsEmpty(),
				"round-trip should produce no diff for %q", tc.name)
		})
	}
}

// =============================================================================
// Test 8: Migration via file-based workflow (scanner + applier)
// =============================================================================

func TestFileBasedMigrationWorkflow(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	targetDB := newTestDB(t, targetDBURL)
	devDB.reset(t)
	targetDB.reset(t)

	tmpDir := t.TempDir()
	migrationsDir := filepath.Join(tmpDir, "migrations")
	require.NoError(t, os.MkdirAll(migrationsDir, 0755))

	// --- Step 1: Generate initial migration against empty dev DB ---
	v1SQL := `
		CREATE TABLE accounts (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			active BOOLEAN DEFAULT true
		);
	`
	desired := parseSQL(t, v1SQL)
	diff := diffSchemas(t, desired, make(schema.SchemaObjectMap))
	ddl := generateDDL(t, diff, desired)

	m1Path := filepath.Join(migrationsDir, "20240301120000-create-accounts.sql")
	require.NoError(t, os.WriteFile(m1Path, []byte(ddl), 0644))

	// --- Step 2: Apply to dev via file-based workflow ---
	scanner := migration.NewScanner(migrationsDir)
	migrations, err := scanner.Scan()
	require.NoError(t, err)
	require.Len(t, migrations, 1)

	devApplier := migration.NewApplier(devDB.pool, false)
	err = devApplier.Apply(context.Background(), migrations, migration.ApplyOptions{})
	require.NoError(t, err)

	// Preflight check: dev DB after migrations should match desired schema
	devActual := devDB.extractSchema(t)
	preflightDiff := diffSchemas(t, desired, devActual)
	assert.True(t, preflightDiff.IsEmpty(), "preflight: dev should match schema after migrations")

	// --- Step 3: Apply same migrations to target ---
	targetApplier := migration.NewApplier(targetDB.pool, false)
	err = targetApplier.Apply(context.Background(), migrations, migration.ApplyOptions{})
	require.NoError(t, err)

	assert.True(t, targetDB.tableExists(t, "accounts"))

	// --- Step 4: Evolve schema, generate second migration ---
	v2SQL := `
		CREATE TABLE accounts (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			active BOOLEAN DEFAULT true
		);
	`
	desiredV2 := parseSQL(t, v2SQL)
	devActual = devDB.extractSchema(t)
	diff2 := diffSchemas(t, desiredV2, devActual)
	require.False(t, diff2.IsEmpty(), "should detect difference for v2")

	ddl2 := generateDDL(t, diff2, desiredV2)
	m2Path := filepath.Join(migrationsDir, "20240302120000-add-email.sql")
	require.NoError(t, os.WriteFile(m2Path, []byte(ddl2), 0644))

	// Rescan and apply all to dev
	migrations2, err := scanner.Scan()
	require.NoError(t, err)
	require.Len(t, migrations2, 2)

	err = devApplier.Apply(context.Background(), migrations2, migration.ApplyOptions{})
	require.NoError(t, err)

	// Apply to target
	err = targetApplier.Apply(context.Background(), migrations2, migration.ApplyOptions{})
	require.NoError(t, err)

	// Both should now have email column
	assert.True(t, devDB.columnExists(t, "accounts", "email"))
	assert.True(t, targetDB.columnExists(t, "accounts", "email"))

	// Both should match desired v2
	for label, d := range map[string]*testDB{"dev": devDB, "target": targetDB} {
		actual := d.extractSchema(t)
		finalDiff := diffSchemas(t, desiredV2, actual)
		if !finalDiff.IsEmpty() {
			t.Logf("%s residual diff:", label)
			logDiff(t, finalDiff)
		}
		assert.True(t, finalDiff.IsEmpty(), "%s DB should match schema v2", label)
	}
}

// =============================================================================
// Test 9: Destructive operations safety
// =============================================================================

func TestDestructiveOps_DropTableInDiff(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	// Create two tables
	devDB.execSQL(t, `
		CREATE TABLE keep_me (id SERIAL PRIMARY KEY, name TEXT);
		CREATE TABLE drop_me (id SERIAL PRIMARY KEY, value TEXT);
	`)

	// Desired schema only has one table
	desired := parseSQL(t, `
		CREATE TABLE keep_me (id SERIAL PRIMARY KEY, name TEXT);
	`)

	actual := devDB.extractSchema(t)
	diff := diffSchemas(t, desired, actual)

	// Should detect drop_me needs to be dropped
	require.False(t, diff.IsEmpty())

	dropFound := false
	for _, key := range diff.ToDrop {
		if key.Kind == schema.TableKind && key.Name == "drop_me" {
			dropFound = true
		}
	}
	assert.True(t, dropFound, "diff should contain DROP for drop_me table")

	// Generated DDL should contain DROP TABLE
	ddl := generateDDL(t, diff, desired)
	assert.Contains(t, ddl, "DROP TABLE", "DDL should contain DROP TABLE")
	assert.Contains(t, ddl, "drop_me", "DDL should reference the table being dropped")
}

func TestDestructiveOps_DropColumnInDiff(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	devDB.execSQL(t, `
		CREATE TABLE col_test (id SERIAL PRIMARY KEY, keep_col TEXT, drop_col TEXT);
	`)

	desired := parseSQL(t, `
		CREATE TABLE col_test (id SERIAL PRIMARY KEY, keep_col TEXT);
	`)

	actual := devDB.extractSchema(t)
	diff := diffSchemas(t, desired, actual)
	require.False(t, diff.IsEmpty())

	ddl := generateDDL(t, diff, desired)
	assert.Contains(t, ddl, "DROP COLUMN", "should generate DROP COLUMN")

	// Apply it and verify
	devDB.execSQL(t, ddl)
	assert.False(t, devDB.columnExists(t, "col_test", "drop_col"))
	assert.True(t, devDB.columnExists(t, "col_test", "keep_col"))
}

// =============================================================================
// Test 10: Cascade behavior with dependent objects
// =============================================================================

func TestCascadeDrop_ViewDependsOnTable(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	// Create a table and a view that depends on it
	devDB.execSQL(t, `
		CREATE TABLE base_table (id SERIAL PRIMARY KEY, name TEXT);
		CREATE VIEW base_view AS SELECT id, name FROM base_table;
	`)

	// Desired schema drops the view but keeps the table
	desired := parseSQL(t, `
		CREATE TABLE base_table (id SERIAL PRIMARY KEY, name TEXT);
	`)

	actual := devDB.extractSchema(t)
	diff := diffSchemas(t, desired, actual)
	require.False(t, diff.IsEmpty())

	ddl := generateDDL(t, diff, desired)
	// Should drop the view (which depends on nothing else that's being kept)
	assert.Contains(t, ddl, "DROP VIEW")

	devDB.execSQL(t, ddl)

	// View gone, table still there
	assert.True(t, devDB.tableExists(t, "base_table"))
	var viewExists bool
	err := devDB.pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.views
			WHERE table_schema = 'public' AND table_name = 'base_view'
		)
	`).Scan(&viewExists)
	require.NoError(t, err)
	assert.False(t, viewExists)
}

// =============================================================================
// Test 11: Empty diff produces no migration
// =============================================================================

func TestNoDiff_NoMigration(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	schemaSQL := `
		CREATE TABLE stable_table (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		);
	`
	devDB.execSQL(t, schemaSQL)

	desired := parseSQL(t, schemaSQL)
	actual := devDB.extractSchema(t)
	diff := diffSchemas(t, desired, actual)

	assert.True(t, diff.IsEmpty(), "identical schema should produce empty diff")
	ddl := generateDDL(t, diff, desired)
	assert.Empty(t, ddl, "empty diff should produce no DDL")
}

// =============================================================================
// Test 12: Large-ish realistic schema from scratch
// =============================================================================

func TestRealisticSchema_FromScratch(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	// A realistic SaaS-like schema with many interconnected objects
	schemaSQL := `
		CREATE TYPE subscription_tier AS ENUM ('free', 'pro', 'enterprise');
		CREATE TYPE invite_status AS ENUM ('pending', 'accepted', 'expired');

		CREATE TABLE organizations (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			slug TEXT NOT NULL UNIQUE,
			tier subscription_tier DEFAULT 'free',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
			email TEXT NOT NULL,
			display_name TEXT,
			is_admin BOOLEAN DEFAULT false,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE (org_id, email)
		);

		CREATE INDEX idx_users_org ON users(org_id);
		CREATE INDEX idx_users_email ON users(email);

		CREATE TABLE invites (
			id SERIAL PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
			email TEXT NOT NULL,
			invited_by INTEGER REFERENCES users(id),
			status invite_status DEFAULT 'pending',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP WITH TIME ZONE
		);

		CREATE INDEX idx_invites_org ON invites(org_id);
		CREATE INDEX idx_invites_pending ON invites(org_id, email) WHERE status = 'pending';

		CREATE TABLE projects (
			id SERIAL PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			description TEXT,
			archived BOOLEAN DEFAULT false,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_projects_org ON projects(org_id);
		CREATE INDEX idx_projects_active ON projects(org_id) WHERE archived = false;

		CREATE TABLE tasks (
			id SERIAL PRIMARY KEY,
			project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
			assignee_id INTEGER REFERENCES users(id),
			title TEXT NOT NULL,
			description TEXT,
			done BOOLEAN DEFAULT false,
			due_date DATE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_tasks_project ON tasks(project_id);
		CREATE INDEX idx_tasks_assignee ON tasks(assignee_id);
		CREATE INDEX idx_tasks_due ON tasks(due_date) WHERE done = false;

		CREATE TABLE comments (
			id SERIAL PRIMARY KEY,
			task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
			author_id INTEGER NOT NULL REFERENCES users(id),
			body TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_comments_task ON comments(task_id);

		CREATE TABLE audit_log (
			id BIGSERIAL PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id),
			user_id INTEGER REFERENCES users(id),
			action TEXT NOT NULL,
			target_type TEXT,
			target_id INTEGER,
			metadata JSONB,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX idx_audit_org ON audit_log(org_id);
		CREATE INDEX idx_audit_created ON audit_log(org_id, created_at DESC);

		CREATE OR REPLACE FUNCTION log_task_change()
		RETURNS TRIGGER AS $$
		BEGIN
			INSERT INTO audit_log (org_id, user_id, action, target_type, target_id)
			SELECT p.org_id, NEW.assignee_id, TG_OP, 'task', NEW.id
			FROM projects p WHERE p.id = NEW.project_id;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER trg_task_audit
			AFTER INSERT OR UPDATE ON tasks
			FOR EACH ROW
			EXECUTE FUNCTION log_task_change();
	`

	evolve(t, devDB, "realistic SaaS schema", schemaSQL)

	// Verify all expected tables exist
	for _, table := range []string{
		"organizations", "users", "invites", "projects",
		"tasks", "comments", "audit_log",
	} {
		assert.True(t, devDB.tableExists(t, table), "table %s should exist", table)
	}
}

// =============================================================================
// Test 13: Migration with all object types dropped and recreated
// =============================================================================

func TestDropAndRecreateEverything(t *testing.T) {
	devDB := newTestDB(t, devDBURL)
	devDB.reset(t)

	v1 := `
		CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');

		CREATE TABLE people (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			mood mood DEFAULT 'neutral'
		);

		CREATE INDEX idx_people_mood ON people(mood);

		CREATE OR REPLACE FUNCTION on_person_insert()
		RETURNS TRIGGER AS $$
		BEGIN
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER trg_placeholder
			AFTER INSERT ON people
			FOR EACH ROW
			EXECUTE FUNCTION on_person_insert();
	`

	evolve(t, devDB, "v1 with all types", v1)

	// V2: completely different schema — everything from v1 should be dropped
	v2 := `
		CREATE TABLE widgets (
			id SERIAL PRIMARY KEY,
			label TEXT NOT NULL,
			weight NUMERIC(10, 2)
		);

		CREATE INDEX idx_widgets_label ON widgets(label);
	`

	desired := parseSQL(t, v2)
	actual := devDB.extractSchema(t)
	diff := diffSchemas(t, desired, actual)

	require.False(t, diff.IsEmpty())
	require.Greater(t, len(diff.ToDrop), 0, "should have objects to drop")
	require.Greater(t, len(diff.ToCreate), 0, "should have objects to create")

	ddl := generateDDL(t, diff, desired, actual)

	// DDL should be executable — this is the real test. Drop order must be correct.
	// The trigger depends on the function; the index depends on the table; etc.
	devDB.execSQL(t, ddl)

	// Verify new schema
	assert.True(t, devDB.tableExists(t, "widgets"))
	assert.False(t, devDB.tableExists(t, "people"))

	// And round-trip
	actualAfter := devDB.extractSchema(t)
	verifyDiff := diffSchemas(t, desired, actualAfter)
	if !verifyDiff.IsEmpty() {
		logDiff(t, verifyDiff)
	}
	assert.True(t, verifyDiff.IsEmpty(), "round-trip after full replacement should be clean")
}

