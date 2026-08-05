//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	devDBURL    = "postgresql://postgres:postgres@localhost:25433/schemata_dev?sslmode=disable"
	targetDBURL = "postgresql://postgres:postgres@localhost:25434/schemata_target?sslmode=disable"
)

func TestDatabaseConnection(t *testing.T) {
	ctx := context.Background()

	// Test connection to dev database
	devConn := &config.DBConnection{
		URL: strPtr(devDBURL),
	}

	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err, "should connect to dev database")
	defer pool.Close()

	// Test ping
	err = pool.Ping(ctx)
	assert.NoError(t, err, "should be able to ping database")

	// Execute a simple query
	var result int
	err = pool.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "should execute query")
	assert.Equal(t, 1, result)
}

func TestMigrationTracking(t *testing.T) {
	ctx := context.Background()

	// Connect to dev database
	devConn := &config.DBConnection{
		URL: strPtr(devDBURL),
	}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	// Clean up any existing schema
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	// Create migration tracker
	tracker := db.NewMigrationTracker(pool)

	// Ensure schema
	err = tracker.EnsureSchema(ctx)
	require.NoError(t, err, "should create schemata schema")

	// Initially should have no applied versions
	versions, err := tracker.GetAppliedVersions(ctx)
	require.NoError(t, err)
	assert.Empty(t, versions)

	// Mark a version as applied
	err = tracker.MarkApplied(ctx, pool, "20231015120530")
	require.NoError(t, err)

	// Should now have one version
	versions, err = tracker.GetAppliedVersions(ctx)
	require.NoError(t, err)
	assert.Len(t, versions, 1)
	assert.Equal(t, "20231015120530", versions[0])

	// Confirm membership by reading applied versions
	assert.Contains(t, versions, "20231015120530")
	assert.NotContains(t, versions, "20231015130000")

	// Test GetPendingVersions
	allVersions := []string{"20231015120530", "20231015130000", "20231016090000"}
	pending, err := tracker.GetPendingVersions(ctx, allVersions)
	require.NoError(t, err)
	assert.Len(t, pending, 2)
	assert.Equal(t, []string{"20231015130000", "20231016090000"}, pending)

	// Mark another version
	err = tracker.MarkApplied(ctx, pool, "20231015130000")
	require.NoError(t, err)

	// Confirm the second version was applied
	versions, err = tracker.GetAppliedVersions(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"20231015120530", "20231015130000"}, versions)

	// Clean up
	_, _ = pool.Exec(ctx, "DROP SCHEMA schemata CASCADE")
}

func TestSessionAdvisoryLockUsesDedicatedConnection(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	const lockName = "schemata:test:dedicated-connection"
	lockKey := db.AdvisoryLockKey(lockName)

	// Keep one observer session checked out for the complete lifecycle. This
	// avoids a false pass from reacquiring the lock-owning session: PostgreSQL
	// advisory locks are re-entrant within the same session.
	observer, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer observer.Release()
	var observerPID int
	require.NoError(t, observer.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&observerPID))

	err = db.WithSessionAdvisoryLock(ctx, pool, lockName, time.Second, func(lockedConn *pgxpool.Conn) error {
		var lockedPID int
		require.NoError(t, lockedConn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&lockedPID))
		require.NotEqual(t, lockedPID, observerPID)

		var acquired bool
		require.NoError(t, observer.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&acquired))
		assert.False(t, acquired, "a second session must not acquire the held lock")
		return nil
	})
	require.NoError(t, err)

	var acquired bool
	require.NoError(t, observer.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&acquired))
	require.True(t, acquired, "lock must be released after the callback")
	_, err = observer.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey)
	require.NoError(t, err)
}

func TestSessionAdvisoryLockHonorsWaitTimeout(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	const lockName = "schemata:test:wait-timeout"
	lockKey := db.AdvisoryLockKey(lockName)

	holder, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer holder.Release()
	_, err = holder.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey)
	require.NoError(t, err)

	callbackCalled := false
	err = db.WithSessionAdvisoryLock(ctx, pool, lockName, 50*time.Millisecond, func(_ *pgxpool.Conn) error {
		callbackCalled = true
		return nil
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, callbackCalled)

	_, err = holder.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey)
	require.NoError(t, err)

	var result int
	require.NoError(t, pool.QueryRow(ctx, "SELECT 1").Scan(&result))
	assert.Equal(t, 1, result, "pool must remain usable after lock wait cancellation")
}

func TestCatalogExtraction(t *testing.T) {
	ctx := context.Background()

	// Connect to target database
	targetConn := &config.DBConnection{
		URL: strPtr(targetDBURL),
	}
	pool, err := db.Connect(ctx, targetConn)
	require.NoError(t, err)
	defer pool.Close()

	// Create a test schema
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	require.NoError(t, err)
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS test_users CASCADE")

	// Extract schema objects
	catalog := db.NewCatalog(pool)
	objects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, nil)
	require.NoError(t, err)

	// Should find the test_users table
	foundTable := false
	for _, obj := range objects {
		if tbl, ok := obj.(schema.Table); ok {
			if tbl.Name == "test_users" {
				foundTable = true
				// Check columns
				assert.GreaterOrEqual(t, len(tbl.Columns), 4, "should have at least 4 columns")

				// Check primary key
				assert.NotNil(t, tbl.PrimaryKey, "should have primary key")
				if tbl.PrimaryKey != nil {
					assert.Len(t, tbl.PrimaryKey.Cols, 1)
				}

				// Check unique constraint
				assert.GreaterOrEqual(t, len(tbl.Uniques), 1, "should have unique constraint on email")

				break
			}
		}
	}

	assert.True(t, foundTable, "should find test_users table")
}

func TestMigrationApplication(t *testing.T) {
	ctx := context.Background()

	// Connect to dev database
	devConn := &config.DBConnection{
		URL: strPtr(devDBURL),
	}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	// Clean up
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS test_migration_table")

	// Create applier
	applier := migration.NewApplier(pool, false)

	// Create test migrations
	migrations := []migration.Migration{
		{
			Version:  "20231015120530",
			Name:     "create-test-table",
			SQL:      "CREATE TABLE test_migration_table (id INT PRIMARY KEY, value TEXT);",
			FilePath: "/tmp/test",
		},
		{
			Version:  "20231015130000",
			Name:     "add-column",
			SQL:      "ALTER TABLE test_migration_table ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
			FilePath: "/tmp/test",
		},
	}

	// Apply migrations
	opts := migration.ApplyOptions{
		DryRun:          false,
		ContinueOnError: false,
	}
	err = applier.Apply(ctx, migrations, opts)
	require.NoError(t, err, "should apply migrations")

	// Verify table exists
	var tableExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'test_migration_table'
		)
	`).Scan(&tableExists)
	require.NoError(t, err)
	assert.True(t, tableExists, "table should exist after migration")

	// Verify column exists
	var columnCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = 'test_migration_table'
	`).Scan(&columnCount)
	require.NoError(t, err)
	assert.Equal(t, 3, columnCount, "should have 3 columns")

	// Verify versions are tracked
	tracker := db.NewMigrationTracker(pool)
	versions, err := tracker.GetAppliedVersions(ctx)
	require.NoError(t, err)
	assert.Len(t, versions, 2)
	assert.Equal(t, []string{"20231015120530", "20231015130000"}, versions)

	// Try to apply again - should be no-op
	err = applier.Apply(ctx, migrations, opts)
	require.NoError(t, err)

	// Clean up
	_, _ = pool.Exec(ctx, "DROP TABLE test_migration_table")
	_, _ = pool.Exec(ctx, "DROP SCHEMA schemata CASCADE")
}

func TestDryRunMode(t *testing.T) {
	ctx := context.Background()

	devConn := &config.DBConnection{
		URL: strPtr(devDBURL),
	}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	// Clean up
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS test_dryrun_table")
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	// Ensure schema exists first
	tracker := db.NewMigrationTracker(pool)
	err = tracker.EnsureSchema(ctx)
	require.NoError(t, err)

	// Create applier with dry run
	applier := migration.NewApplier(pool, true)

	migrations := []migration.Migration{
		{
			Version:  "20231015120530",
			Name:     "create-table",
			SQL:      "CREATE TABLE test_dryrun_table (id INT);",
			FilePath: "/tmp/test",
		},
	}

	// Apply in dry run mode
	opts := migration.ApplyOptions{
		DryRun:          true,
		ContinueOnError: false,
	}
	err = applier.Apply(ctx, migrations, opts)
	require.NoError(t, err)

	// Table should NOT exist
	var tableExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'test_dryrun_table'
		)
	`).Scan(&tableExists)
	require.NoError(t, err)
	assert.False(t, tableExists, "table should not exist in dry run mode")
}

// Helper function
func strPtr(s string) *string {
	return &s
}
