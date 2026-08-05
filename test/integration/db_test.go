//go:build integration
// +build integration

package integration

import (
	"context"
	"strings"
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
	firstMetadata := db.MigrationMetadata{
		Version:        "20231015120530",
		Name:           "first",
		Checksum:       strings.Repeat("a", 64),
		ExecutionMode:  migration.ExecutionModeTransactional,
		StatementCount: 1,
	}
	err = tracker.MarkRunning(ctx, pool, firstMetadata)
	require.NoError(t, err)
	err = tracker.MarkApplied(ctx, pool, firstMetadata)
	require.NoError(t, err)

	// Should now have one version
	versions, err = tracker.GetAppliedVersions(ctx)
	require.NoError(t, err)
	assert.Len(t, versions, 1)
	assert.Equal(t, "20231015120530", versions[0])

	history, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, "first", history[0].Name)
	assert.Equal(t, strings.Repeat("a", 64), history[0].Checksum)
	assert.Equal(t, db.MigrationStatusApplied, history[0].Status)
	assert.Equal(t, 1, history[0].StatementCount)
	assert.Equal(t, 1, history[0].LastConfirmedStatement)
	assert.NotNil(t, history[0].FinishedAt)

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
	secondMetadata := db.MigrationMetadata{
		Version:        "20231015130000",
		Name:           "second",
		Checksum:       strings.Repeat("b", 64),
		ExecutionMode:  migration.ExecutionModeTransactional,
		StatementCount: 1,
	}
	err = tracker.MarkRunning(ctx, pool, secondMetadata)
	require.NoError(t, err)
	err = tracker.MarkApplied(ctx, pool, secondMetadata)
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

func TestDedicatedConnectionIsFreshAndDoesNotConsumeCallerPoolSlot(t *testing.T) {
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(devDBURL)
	require.NoError(t, err)
	config.MaxConns = 1
	callerPool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	pool := &db.Pool{Pool: callerPool}
	defer pool.Close()

	dirty, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = dirty.Exec(ctx, `
		SET search_path = pg_catalog;
		PREPARE contaminated_session AS SELECT 1;
		CREATE TEMP TABLE contaminated_session_table (id integer);
	`)
	require.NoError(t, err)
	dirty.Release()

	err = db.WithDedicatedConnection(ctx, pool, func(conn *pgxpool.Conn) error {
		var searchPath string
		if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&searchPath); err != nil {
			return err
		}
		assert.NotEqual(t, "pg_catalog", searchPath)

		var preparedCount int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE name = 'contaminated_session'").Scan(&preparedCount); err != nil {
			return err
		}
		assert.Zero(t, preparedCount)

		var tempTableExists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass('pg_temp.contaminated_session_table') IS NOT NULL").Scan(&tempTableExists); err != nil {
			return err
		}
		assert.False(t, tempTableExists)
		return nil
	})
	require.NoError(t, err)
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
		DryRun: false,
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

func TestMigrationHistoryRejectsChangedAppliedSource(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS checksum_original")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS checksum_should_not_run")
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS checksum_original, checksum_should_not_run")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	applier := migration.NewApplier(pool, false)
	original := migration.Migration{
		Version: "20231015120530",
		Name:    "immutable-source",
		SQL:     "CREATE TABLE checksum_original (id integer);",
	}
	require.NoError(t, original.LoadSQL())
	require.NoError(t, applier.Apply(ctx, []migration.Migration{original}, migration.ApplyOptions{}))

	changed := migration.Migration{
		Version:       original.Version,
		Name:          original.Name,
		SQL:           "CREATE TABLE checksum_should_not_run (id integer);",
		Checksum:      original.Checksum,
		Statements:    original.Statements,
		ExecutionMode: original.ExecutionMode,
	}

	dryRun := migration.NewApplier(pool, true)
	err = dryRun.Apply(ctx, []migration.Migration{changed}, migration.ApplyOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checksum mismatch")

	err = applier.Apply(ctx, []migration.Migration{changed}, migration.ApplyOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checksum mismatch")

	var exists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.checksum_should_not_run') IS NOT NULL").Scan(&exists))
	assert.False(t, exists, "history drift must be rejected before changed SQL executes")

	err = applier.Apply(ctx, nil, migration.ApplyOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing from the local inventory")
}

func TestMigrationTrackerRejectsLedgerWithMissingConstraints(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, err = pool.Exec(ctx, `
		CREATE SCHEMA schemata;
		CREATE TABLE schemata.version (
			version_num text NOT NULL,
			name text NOT NULL,
			checksum text NOT NULL,
			execution_mode text NOT NULL,
			status text NOT NULL,
			started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			finished_at timestamptz,
			statement_count integer NOT NULL,
			last_confirmed_statement integer NOT NULL DEFAULT 0,
			failed_statement integer,
			error_message text,
			error_code text,
			attempt_count integer NOT NULL DEFAULT 1,
			recovered_at timestamptz,
			recovery_action text
		);
		COMMENT ON TABLE schemata.version IS 'schemata:migration-history:v2';
	`)
	require.NoError(t, err)

	tracker := db.NewMigrationTracker(pool)
	err = tracker.EnsureSchema(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported pre-release migration tracking constraints")
}

func TestMigrationTrackerRejectsLedgerWithForgedConstraintDefinitions(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, err = pool.Exec(ctx, `
		CREATE SCHEMA schemata;
		CREATE TABLE schemata.version (
			version_num text CONSTRAINT version_pkey PRIMARY KEY,
			name text NOT NULL,
			checksum text NOT NULL CONSTRAINT version_checksum_format CHECK (true),
			execution_mode text NOT NULL CONSTRAINT version_execution_mode CHECK (true),
			status text NOT NULL CONSTRAINT version_status CHECK (true),
			started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			finished_at timestamptz,
			statement_count integer NOT NULL CONSTRAINT version_statement_count CHECK (true),
			last_confirmed_statement integer NOT NULL DEFAULT 0,
			failed_statement integer,
			error_message text,
			error_code text,
			attempt_count integer NOT NULL DEFAULT 1 CONSTRAINT version_attempt_count CHECK (true),
			recovered_at timestamptz,
			recovery_action text CONSTRAINT version_recovery_action CHECK (true),
			CONSTRAINT version_last_confirmed_range CHECK (true),
			CONSTRAINT version_failed_statement_range CHECK (true),
			CONSTRAINT version_recovery_pair CHECK (true),
			CONSTRAINT version_status_state CHECK (true)
		);
		COMMENT ON TABLE schemata.version IS 'schemata:migration-history:v2';
	`)
	require.NoError(t, err)

	err = db.NewMigrationTracker(pool).EnsureSchema(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported pre-release migration tracking constraints")
}

func TestMigrationTrackerRejectsBehaviorChangingTrigger(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.keep_old_migration_history()")
	defer pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.keep_old_migration_history()")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	tracker := db.NewMigrationTracker(pool)
	require.NoError(t, tracker.EnsureSchema(ctx))
	_, err = pool.Exec(ctx, `
		CREATE FUNCTION public.keep_old_migration_history() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END $$;
		CREATE TRIGGER keep_old_migration_history
		BEFORE UPDATE ON schemata.version
		FOR EACH ROW EXECUTE FUNCTION public.keep_old_migration_history();
	`)
	require.NoError(t, err)

	err = tracker.EnsureSchema(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported behavior on migration tracking table")
}

func TestMigrationTrackerRejectsInheritance(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	tracker := db.NewMigrationTracker(pool)
	require.NoError(t, tracker.EnsureSchema(ctx))
	_, err = pool.Exec(ctx, "CREATE TABLE schemata.version_child () INHERITS (schemata.version)")
	require.NoError(t, err)

	err = tracker.EnsureSchema(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "children=1")
}

func TestTransactionalMigrationReportsFailingStatementAndRollsBack(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS statement_diagnostics")
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS statement_diagnostics")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	migrations := []migration.Migration{{
		Version: "20231015120530",
		Name:    "statement-diagnostics",
		SQL: `CREATE TABLE statement_diagnostics (id integer);
INSERT INTO missing_statement_diagnostics_table VALUES (1);
ALTER TABLE statement_diagnostics ADD COLUMN value text;`,
		FilePath: "/migrations/20231015120530-statement-diagnostics.sql",
	}}

	err = migration.NewApplier(pool, false).Apply(ctx, migrations, migration.ApplyOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "statement 2 of 3")
	assert.Contains(t, err.Error(), "INSERT INTO missing_statement_diagnostics_table")

	var tableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.statement_diagnostics') IS NOT NULL").Scan(&tableExists))
	assert.False(t, tableExists, "all schema changes must roll back with the failed transaction")

	history, historyErr := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, historyErr)
	assert.Empty(t, history, "the transactional history row must roll back with the migration")
}

func TestMigrationCannotMutateHistoryThroughDynamicSQL(t *testing.T) {
	for _, testCase := range []struct {
		name string
		sql  string
	}{
		{
			name: "dollar quoted dynamic SQL",
			sql: `DO $body$
BEGIN
  EXECUTE 'DELETE FROM schemata.version WHERE version_num = ''20231015120530''';
END
$body$;
CREATE TABLE public.dynamic_history_should_rollback (id integer);`,
		},
		{
			name: "set_config and unqualified delete",
			sql: `SELECT set_config('search_path', 'schemata,public', true);
DELETE FROM version WHERE version_num = '20231015120530';
CREATE TABLE public.dynamic_history_should_rollback (id integer);`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			devConn := &config.DBConnection{URL: strPtr(devDBURL)}
			pool, err := db.Connect(ctx, devConn)
			require.NoError(t, err)
			defer pool.Close()

			_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS public.dynamic_history_should_rollback")
			defer pool.Exec(ctx, "DROP TABLE IF EXISTS public.dynamic_history_should_rollback")
			defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

			applier := migration.NewApplier(pool, false)
			first := migration.Migration{
				Version: "20231015120530",
				Name:    "history-baseline",
				SQL:     "SELECT 1;",
			}
			require.NoError(t, applier.Apply(ctx, []migration.Migration{first}, migration.ApplyOptions{}))

			second := migration.Migration{
				Version: "20231015130000",
				Name:    "history-tamper",
				SQL:     testCase.sql,
			}
			err = applier.Apply(ctx, []migration.Migration{first, second}, migration.ApplyOptions{})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "modified the reserved migration history")

			versions, historyErr := db.NewMigrationTracker(pool).GetAppliedVersions(ctx)
			require.NoError(t, historyErr)
			assert.Equal(t, []string{"20231015120530"}, versions)
			var tableExists bool
			require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.dynamic_history_should_rollback') IS NOT NULL").Scan(&tableExists))
			assert.False(t, tableExists)
		})
	}
}

func TestDeferredConstraintTriggerCannotMutateHistoryAtCommit(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS public.deferred_history_queue")
	_, _ = pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.deferred_history_tamper()")
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS public.deferred_history_queue")
	defer pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.deferred_history_tamper()")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	applier := migration.NewApplier(pool, false)
	first := migration.Migration{
		Version: "20231015120530",
		Name:    "history-baseline",
		SQL:     "SELECT 1;",
	}
	require.NoError(t, applier.Apply(ctx, []migration.Migration{first}, migration.ApplyOptions{}))

	second := migration.Migration{
		Version: "20231015130000",
		Name:    "deferred-history-tamper",
		SQL: `CREATE TABLE public.deferred_history_queue (id integer);
CREATE FUNCTION public.deferred_history_tamper() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
  DELETE FROM schemata.version WHERE version_num = '20231015120530';
  RETURN NEW;
END
$body$;
CREATE CONSTRAINT TRIGGER deferred_history_tamper
AFTER INSERT ON public.deferred_history_queue
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.deferred_history_tamper();
INSERT INTO public.deferred_history_queue VALUES (1);`,
	}
	err = applier.Apply(ctx, []migration.Migration{first, second}, migration.ApplyOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "modified the reserved migration history")

	versions, historyErr := db.NewMigrationTracker(pool).GetAppliedVersions(ctx)
	require.NoError(t, historyErr)
	assert.Equal(t, []string{"20231015120530"}, versions)
	var tableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.deferred_history_queue') IS NOT NULL").Scan(&tableExists))
	assert.False(t, tableExists)
}

func TestCommitTimeHistoryMutationIsNeverReportedAsSuccess(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS public.defer_first, public.defer_second")
	_, _ = pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.defer_first_trigger(), public.defer_second_trigger()")
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS public.defer_first, public.defer_second")
	defer pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.defer_first_trigger(), public.defer_second_trigger()")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	applier := migration.NewApplier(pool, false)
	first := migration.Migration{
		Version: "20231015120530",
		Name:    "history-baseline",
		SQL:     "SELECT 1;",
	}
	require.NoError(t, applier.Apply(ctx, []migration.Migration{first}, migration.ApplyOptions{}))

	second := migration.Migration{
		Version: "20231015130000",
		Name:    "commit-time-history-tamper",
		SQL: `CREATE TABLE public.defer_first (id integer);
CREATE TABLE public.defer_second (id integer);
CREATE FUNCTION public.defer_second_trigger() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
  DELETE FROM schemata.version WHERE version_num = '20231015120530';
  RETURN NEW;
END
$body$;
CREATE CONSTRAINT TRIGGER defer_second_trigger
AFTER INSERT ON public.defer_second
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.defer_second_trigger();
CREATE FUNCTION public.defer_first_trigger() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
  SET CONSTRAINTS ALL DEFERRED;
  INSERT INTO public.defer_second VALUES (NEW.id);
  RETURN NEW;
END
$body$;
CREATE CONSTRAINT TRIGGER defer_first_trigger
AFTER INSERT ON public.defer_first
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.defer_first_trigger();
INSERT INTO public.defer_first VALUES (1);`,
	}
	err = applier.Apply(ctx, []migration.Migration{first, second}, migration.ApplyOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "durable history differs from the verified transaction state")
}

func TestMigrationAppliesSQLStandardBeginAtomicFunction(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.atomic_add_one(integer)")
	defer pool.Exec(ctx, "DROP FUNCTION IF EXISTS public.atomic_add_one(integer)")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")

	migrations := []migration.Migration{{
		Version: "20231015120530",
		Name:    "begin-atomic-function",
		SQL: `CREATE FUNCTION public.atomic_add_one(value integer)
RETURNS integer
LANGUAGE SQL
BEGIN ATOMIC
  SELECT value + 1;
END;`,
	}}
	require.NoError(t, migration.NewApplier(pool, false).Apply(ctx, migrations, migration.ApplyOptions{}))

	var result int
	require.NoError(t, pool.QueryRow(ctx, "SELECT public.atomic_add_one(41)").Scan(&result))
	assert.Equal(t, 42, result)
}

func TestMigrationCancelsWhenRunnerLockBackendDies(t *testing.T) {
	ctx := context.Background()
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer pool.Close()

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS public.lock_loss_should_rollback")
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS public.lock_loss_should_rollback")
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS schemata CASCADE")
	require.NoError(t, db.NewMigrationTracker(pool).EnsureSchema(ctx))

	migrations := []migration.Migration{{
		Version: "20231015120530",
		Name:    "lock-loss",
		SQL: `SELECT pg_sleep(10);
CREATE TABLE public.lock_loss_should_rollback (id integer);`,
	}}
	applyDone := make(chan error, 1)
	go func() {
		applyDone <- migration.NewApplier(pool, false).Apply(ctx, migrations, migration.ApplyOptions{})
	}()

	key := uint64(db.AdvisoryLockKey("schemata:migrations"))
	classID := int64(uint32(key >> 32))
	objectID := int64(uint32(key))
	var lockPID int32
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err = pool.QueryRow(ctx, `
			SELECT pid
			FROM pg_catalog.pg_locks
			WHERE locktype = 'advisory'
			  AND classid = $1::bigint::oid
			  AND objid = $2::bigint::oid
			  AND objsubid = 1
			  AND granted
			LIMIT 1
		`, classID, objectID).Scan(&lockPID)
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	require.NoError(t, err, "runner advisory lock should become visible")

	var terminated bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", lockPID).Scan(&terminated))
	require.True(t, terminated)

	select {
	case err = <-applyDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lost its advisory lock")
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not stop after its runner lock backend died")
	}

	var tableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.lock_loss_should_rollback') IS NOT NULL").Scan(&tableExists))
	assert.False(t, tableExists)
	history, historyErr := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, historyErr)
	assert.Empty(t, history)
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
	// Constructor-level dry-run must remain authoritative even when callers
	// pass zero-value options.
	opts := migration.ApplyOptions{}
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

	var historyExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('schemata.version') IS NOT NULL").Scan(&historyExists))
	assert.False(t, historyExists, "dry run must not create migration history")
}

// Helper function
func strPtr(s string) *string {
	return &s
}
