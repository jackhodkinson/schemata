//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nonTransactionalTestPool(t *testing.T) (*db.Pool, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := db.Connect(ctx, &config.DBConnection{URL: strPtr(devDBURL)})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool, ctx
}

func cleanNonTransactionalFixtures(t *testing.T, ctx context.Context, pool *db.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS schemata CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_success CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_retry CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_retry_gate CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_ambiguous CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_marked CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_fatal CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_cancel CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_fenced CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_partial_index CASCADE;
		DROP TABLE IF EXISTS public.schemata_nt_session_isolation CASCADE;
		DROP TABLE IF EXISTS public.schemata_transactional_fenced CASCADE;
		DROP FUNCTION IF EXISTS public.schemata_nt_set_session_probe();
	`)
	require.NoError(t, err)
}

func waitForMigrationRecord(
	t *testing.T,
	ctx context.Context,
	tracker *db.MigrationTracker,
	executor db.Executor,
	version string,
	predicate func(db.MigrationRecord) bool,
) db.MigrationRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		history, err := tracker.GetHistoryWithExecutor(ctx, executor)
		if err == nil {
			for _, record := range history {
				if record.Version == version && predicate(record) {
					return record
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for migration %s history state", version)
	return db.MigrationRecord{}
}

func waitForActiveMigrationQueryPID(t *testing.T, ctx context.Context, pool *db.Pool, pattern string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var pid int
		err := pool.QueryRow(ctx, `
			SELECT pid
			FROM pg_catalog.pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND state = 'active'
			  AND query LIKE $1
			ORDER BY query_start DESC
			LIMIT 1
		`, pattern).Scan(&pid)
		if err == nil {
			return pid
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for active migration query %q", pattern)
	return 0
}

func TestNonTransactionalMigrationSupportsCreateIndexConcurrently(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000001",
		Name:    "non-transactional-success",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_success (id integer PRIMARY KEY, value text);
CREATE INDEX CONCURRENTLY schemata_nt_success_value_idx ON public.schemata_nt_success (value);`,
	}}

	applier := migration.NewApplier(pool, false)
	require.NoError(t, applier.Apply(ctx, migrations, migration.ApplyOptions{}))

	var indexValid bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT idx.indisvalid
		FROM pg_catalog.pg_index AS idx
		JOIN pg_catalog.pg_class AS cls ON cls.oid = idx.indexrelid
		WHERE cls.relname = 'schemata_nt_success_value_idx'
	`).Scan(&indexValid))
	assert.True(t, indexValid)

	history, err := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, migration.ExecutionModeNonTransactional, history[0].ExecutionMode)
	assert.Equal(t, db.MigrationStatusApplied, history[0].Status)
	assert.Equal(t, 2, history[0].StatementCount)
	assert.Equal(t, 2, history[0].LastConfirmedStatement)
	assert.Equal(t, 1, history[0].AttemptCount)

	// A completed non-transactional migration remains idempotent at the
	// migration runner level and is never executed a second time.
	require.NoError(t, applier.Apply(ctx, migrations, migration.ApplyOptions{}))
}

func TestNonTransactionalMigrationFailureBlocksApplyUntilExplicitRetry(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000002",
		Name:    "non-transactional-retry",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_retry (id integer PRIMARY KEY, value text);
INSERT INTO public.schemata_nt_retry (id, value) VALUES (1, 'before failure');
INSERT INTO public.schemata_nt_retry_gate (id) VALUES (1);
CREATE INDEX CONCURRENTLY schemata_nt_retry_value_idx ON public.schemata_nt_retry (value);`,
	}}

	applier := migration.NewApplier(pool, false)
	err := applier.Apply(ctx, migrations, migration.ApplyOptions{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "statement 3 of 4")
	assert.ErrorContains(t, err, "durably marked failed")

	tracker := db.NewMigrationTracker(pool)
	history, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, db.MigrationStatusFailed, history[0].Status)
	assert.Equal(t, 2, history[0].LastConfirmedStatement)
	require.NotNil(t, history[0].FailedStatement)
	assert.Equal(t, 3, *history[0].FailedStatement)
	require.NotNil(t, history[0].ErrorCode)
	assert.Equal(t, "42P01", *history[0].ErrorCode)

	// Normal deploys never infer whether a partial statement should be rerun.
	err = applier.Apply(ctx, migrations, migration.ApplyOptions{})
	require.ErrorContains(t, err, "incomplete database status")

	_, err = pool.Exec(ctx, "CREATE TABLE public.schemata_nt_retry_gate (id integer PRIMARY KEY)")
	require.NoError(t, err)
	confirmedThrough := 2
	require.NoError(t, applier.Recover(ctx, migrations, migration.RecoveryOptions{
		Version:          migrations[0].Version,
		Action:           migration.RecoveryActionRetry,
		ConfirmedThrough: &confirmedThrough,
	}))

	history, err = tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, db.MigrationStatusApplied, history[0].Status)
	assert.Equal(t, 4, history[0].LastConfirmedStatement)
	assert.Equal(t, 2, history[0].AttemptCount)
	require.NotNil(t, history[0].RecoveryAction)
	assert.Equal(t, db.MigrationRecoveryRetry, *history[0].RecoveryAction)
	assert.NotNil(t, history[0].RecoveredAt)
}

func TestNonTransactionalRetryCanAttestAmbiguousCommittedProgress(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000003",
		Name:    "non-transactional-ambiguous",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_ambiguous (id integer PRIMARY KEY, value integer);
INSERT INTO public.schemata_nt_ambiguous (id, value) VALUES (1, 42);`,
	}}
	require.NoError(t, migrations[0].LoadSQL())

	tracker := db.NewMigrationTracker(pool)
	require.NoError(t, tracker.EnsureSchema(ctx))
	metadata := db.MigrationMetadata{
		Version:        migrations[0].Version,
		Name:           migrations[0].Name,
		Checksum:       migrations[0].Checksum,
		ExecutionMode:  migrations[0].ExecutionMode,
		StatementCount: len(migrations[0].Statements),
	}
	require.NoError(t, tracker.MarkRunning(ctx, pool, metadata))

	// Model a process crash after PostgreSQL committed statement 1 but before
	// the separate progress update reached the ledger.
	_, err := pool.Exec(ctx, migrations[0].Statements[0])
	require.NoError(t, err)

	confirmedThrough := 1
	require.NoError(t, migration.NewApplier(pool, false).Recover(ctx, migrations, migration.RecoveryOptions{
		Version:          migrations[0].Version,
		Action:           migration.RecoveryActionRetry,
		ConfirmedThrough: &confirmedThrough,
	}))

	var value int
	require.NoError(t, pool.QueryRow(ctx, "SELECT value FROM public.schemata_nt_ambiguous WHERE id = 1").Scan(&value))
	assert.Equal(t, 42, value)
}

func TestNonTransactionalMarkAppliedRequiresExplicitAttestation(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000004",
		Name:    "non-transactional-marked",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_marked (id integer PRIMARY KEY);`,
	}}
	require.NoError(t, migrations[0].LoadSQL())

	tracker := db.NewMigrationTracker(pool)
	require.NoError(t, tracker.EnsureSchema(ctx))
	metadata := db.MigrationMetadata{
		Version:        migrations[0].Version,
		Name:           migrations[0].Name,
		Checksum:       migrations[0].Checksum,
		ExecutionMode:  migrations[0].ExecutionMode,
		StatementCount: len(migrations[0].Statements),
	}
	require.NoError(t, tracker.MarkRunning(ctx, pool, metadata))
	_, err := pool.Exec(ctx, migrations[0].Statements[0])
	require.NoError(t, err)
	// Model the final crash window: the statement and its progress marker are
	// durable, but the process dies before changing running to applied.
	require.NoError(t, tracker.MarkStatementConfirmed(ctx, pool, metadata, 1))

	require.NoError(t, migration.NewApplier(pool, false).Recover(ctx, migrations, migration.RecoveryOptions{
		Version: migrations[0].Version,
		Action:  migration.RecoveryActionMarkApplied,
	}))

	history, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, db.MigrationStatusApplied, history[0].Status)
	assert.Equal(t, 1, history[0].LastConfirmedStatement)
	require.NotNil(t, history[0].RecoveryAction)
	assert.Equal(t, db.MigrationRecoveryMarkApplied, *history[0].RecoveryAction)
}

func TestNonTransactionalFatalConnectionLossRemainsAmbiguous(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000005",
		Name:    "non-transactional-fatal",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_fatal (id integer PRIMARY KEY);
SELECT pg_sleep(30);
INSERT INTO public.schemata_nt_fatal (id) VALUES (1);`,
	}}

	result := make(chan error, 1)
	go func() {
		result <- migration.NewApplier(pool, false).Apply(ctx, migrations, migration.ApplyOptions{})
	}()

	tracker := db.NewMigrationTracker(pool)
	waitForMigrationRecord(t, ctx, tracker, pool, migrations[0].Version, func(record db.MigrationRecord) bool {
		return record.Status == db.MigrationStatusRunning && record.LastConfirmedStatement == 1
	})
	pid := waitForActiveMigrationQueryPID(t, ctx, pool, "%pg_sleep(30)%")
	var terminated bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated))
	require.True(t, terminated)

	select {
	case err := <-result:
		require.Error(t, err)
		assert.ErrorContains(t, err, "outcome is ambiguous")
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not stop after its execution backend was terminated")
	}

	history, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, db.MigrationStatusRunning, history[0].Status)
	assert.Equal(t, 1, history[0].LastConfirmedStatement)
	assert.Nil(t, history[0].FailedStatement)
	assert.Nil(t, history[0].ErrorCode)

	err = migration.NewApplier(pool, false).Apply(ctx, migrations, migration.ApplyOptions{})
	require.ErrorContains(t, err, "incomplete database status")
}

func TestNonTransactionalCancellationNeverExecutesLaterStatements(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000006",
		Name:    "non-transactional-cancel",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_cancel (id integer PRIMARY KEY);
SELECT pg_sleep(30);
INSERT INTO public.schemata_nt_cancel (id) VALUES (1);`,
	}}

	applyCtx, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		result <- migration.NewApplier(pool, false).Apply(applyCtx, migrations, migration.ApplyOptions{})
	}()

	tracker := db.NewMigrationTracker(pool)
	waitForMigrationRecord(t, ctx, tracker, pool, migrations[0].Version, func(record db.MigrationRecord) bool {
		return record.Status == db.MigrationStatusRunning && record.LastConfirmedStatement == 1
	})
	_ = waitForActiveMigrationQueryPID(t, ctx, pool, "%pg_sleep(30)%")
	cancel()

	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not stop after cancellation")
	}

	history, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.NotEqual(t, db.MigrationStatusApplied, history[0].Status)
	assert.Equal(t, 1, history[0].LastConfirmedStatement)
	var rows int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM public.schemata_nt_cancel").Scan(&rows))
	assert.Zero(t, rows, "statement after the canceled statement must never execute")
}

func TestNonTransactionalRecoveryWaitsForOrphanExecutionFence(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000007",
		Name:    "non-transactional-fenced",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_fenced (id integer PRIMARY KEY);`,
	}}
	require.NoError(t, migrations[0].LoadSQL())
	tracker := db.NewMigrationTracker(pool)
	require.NoError(t, tracker.EnsureSchema(ctx))
	metadata := db.MigrationMetadata{
		Version:        migrations[0].Version,
		Name:           migrations[0].Name,
		Checksum:       migrations[0].Checksum,
		ExecutionMode:  migrations[0].ExecutionMode,
		StatementCount: len(migrations[0].Statements),
	}
	require.NoError(t, tracker.MarkRunning(ctx, pool, metadata))
	require.Error(t, tracker.MarkApplied(ctx, pool, metadata), "non-transactional completion must require every statement confirmation")

	fenceConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer fenceConn.Release()
	fenceKey := db.AdvisoryLockKey("schemata:migration-transaction")
	_, err = fenceConn.Exec(ctx, "SELECT pg_advisory_lock($1)", fenceKey)
	require.NoError(t, err)
	defer fenceConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", fenceKey) //nolint:errcheck

	confirmedThrough := 0
	recoverCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = migration.NewApplier(pool, false).Recover(recoverCtx, migrations, migration.RecoveryOptions{
		Version:          migrations[0].Version,
		Action:           migration.RecoveryActionRetry,
		ConfirmedThrough: &confirmedThrough,
	})
	require.Error(t, err)

	history, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, db.MigrationStatusRunning, history[0].Status)
	assert.Equal(t, 1, history[0].AttemptCount, "recovery must not transition before it owns the execution fence")
}

func TestNonTransactionalRecoveryWaitsForOrphanActiveStatement(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000010",
		Name:    "non-transactional-active-statement-fence",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_fenced (id integer PRIMARY KEY);`,
	}}
	require.NoError(t, migrations[0].LoadSQL())
	tracker := db.NewMigrationTracker(pool)
	require.NoError(t, tracker.EnsureSchema(ctx))
	metadata := db.MigrationMetadata{
		Version:        migrations[0].Version,
		Name:           migrations[0].Name,
		Checksum:       migrations[0].Checksum,
		ExecutionMode:  migrations[0].ExecutionMode,
		StatementCount: len(migrations[0].Statements),
	}
	require.NoError(t, tracker.MarkRunning(ctx, pool, metadata))

	activeConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer activeConn.Release()
	activeKey := db.AdvisoryLockKey("schemata:migration-active-statement")
	_, err = activeConn.Exec(ctx, "SELECT pg_advisory_lock($1)", activeKey)
	require.NoError(t, err)
	defer activeConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", activeKey) //nolint:errcheck

	confirmedThrough := 0
	recoverCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = migration.NewApplier(pool, false).Recover(recoverCtx, migrations, migration.RecoveryOptions{
		Version:          migrations[0].Version,
		Action:           migration.RecoveryActionRetry,
		ConfirmedThrough: &confirmedThrough,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "active migration statement fence")

	history, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, db.MigrationStatusRunning, history[0].Status)
	assert.Equal(t, 1, history[0].AttemptCount, "recovery must not transition while an orphan statement backend is active")
}

func TestTransactionalMigrationWaitsForOrphanActiveStatement(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	activeConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer activeConn.Release()
	activeKey := db.AdvisoryLockKey("schemata:migration-active-statement")
	_, err = activeConn.Exec(ctx, "SELECT pg_advisory_lock($1)", activeKey)
	require.NoError(t, err)
	defer activeConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", activeKey) //nolint:errcheck

	migrations := []migration.Migration{{
		Version: "20260805000011",
		Name:    "transactional-active-statement-fence",
		SQL:     "CREATE TABLE public.schemata_transactional_fenced (id integer PRIMARY KEY);",
	}}
	applyCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	err = migration.NewApplier(pool, false).Apply(applyCtx, migrations, migration.ApplyOptions{})
	cancel()
	require.Error(t, err)

	var exists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_transactional_fenced') IS NOT NULL").Scan(&exists))
	assert.False(t, exists, "transactional DDL must not overlap an orphan non-transactional backend")
	history, err := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	assert.Empty(t, history, "the blocked transactional attempt must roll back its ledger row")

	var unlocked bool
	require.NoError(t, activeConn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", activeKey).Scan(&unlocked))
	require.True(t, unlocked)

	require.NoError(t, migration.NewApplier(pool, false).Apply(ctx, migrations, migration.ApplyOptions{}))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_transactional_fenced') IS NOT NULL").Scan(&exists))
	assert.True(t, exists)
}

func TestNonTransactionalConcurrentUniqueIndexFailureLeavesInspectableArtifact(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000008",
		Name:    "non-transactional-partial-index",
		SQL: `-- schemata:transaction off
CREATE TABLE public.schemata_nt_partial_index (id integer PRIMARY KEY, value integer NOT NULL);
INSERT INTO public.schemata_nt_partial_index (id, value) VALUES (1, 7), (2, 7);
CREATE UNIQUE INDEX CONCURRENTLY schemata_nt_partial_value_idx ON public.schemata_nt_partial_index (value);`,
	}}

	applier := migration.NewApplier(pool, false)
	err := applier.Apply(ctx, migrations, migration.ApplyOptions{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "statement 3 of 3")

	var indexValid bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT idx.indisvalid
		FROM pg_catalog.pg_index AS idx
		JOIN pg_catalog.pg_class AS cls ON cls.oid = idx.indexrelid
		WHERE cls.relname = 'schemata_nt_partial_value_idx'
	`).Scan(&indexValid))
	assert.False(t, indexValid, "failed CREATE INDEX CONCURRENTLY must be treated as potentially leaving an artifact")

	history, err := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, db.MigrationStatusFailed, history[0].Status)
	assert.Equal(t, 2, history[0].LastConfirmedStatement)

	// Operator inspection found the invalid index and duplicate row. Recovery
	// is safe only after both are explicitly remediated.
	_, err = pool.Exec(ctx, `
		DROP INDEX public.schemata_nt_partial_value_idx;
		DELETE FROM public.schemata_nt_partial_index WHERE id = 2;
	`)
	require.NoError(t, err)
	confirmedThrough := 2
	require.NoError(t, applier.Recover(ctx, migrations, migration.RecoveryOptions{
		Version:          migrations[0].Version,
		Action:           migration.RecoveryActionRetry,
		ConfirmedThrough: &confirmedThrough,
	}))

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT idx.indisvalid
		FROM pg_catalog.pg_index AS idx
		JOIN pg_catalog.pg_class AS cls ON cls.oid = idx.indexrelid
		WHERE cls.relname = 'schemata_nt_partial_value_idx'
	`).Scan(&indexValid))
	assert.True(t, indexValid)
}

func TestNonTransactionalStatementsCannotLeakNestedSessionState(t *testing.T) {
	pool, ctx := nonTransactionalTestPool(t)
	cleanNonTransactionalFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanNonTransactionalFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805000009",
		Name:    "non-transactional-session-isolation",
		SQL: `-- schemata:transaction off
CREATE FUNCTION public.schemata_nt_set_session_probe() RETURNS integer
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM set_config('schemata.session_probe', 'leaked', false);
  RETURN 1;
END
$$;
SELECT public.schemata_nt_set_session_probe();
CREATE TABLE public.schemata_nt_session_isolation AS
SELECT COALESCE(current_setting('schemata.session_probe', true), '<unset>') AS value;`,
	}}

	require.NoError(t, migration.NewApplier(pool, false).Apply(ctx, migrations, migration.ApplyOptions{}))

	var value string
	require.NoError(t, pool.QueryRow(ctx, "SELECT value FROM public.schemata_nt_session_isolation").Scan(&value))
	assert.Equal(t, "<unset>", value, "nested session state must not cross a non-transactional statement boundary")
}
