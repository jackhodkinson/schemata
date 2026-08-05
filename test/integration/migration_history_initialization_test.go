//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMissingMigrationHistoryRequiresExplicitInitialization(t *testing.T) {
	ctx := context.Background()
	pool := migrationHistoryTestPool(t, ctx)
	cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805010000",
		Name:    "history-initialization-guard",
		SQL:     "CREATE TABLE public.schemata_history_initialization_guard (id integer PRIMARY KEY);",
	}}
	applier := migration.NewApplier(pool, false)

	err := applier.Apply(ctx, migrations, migration.ApplyOptions{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrHistoryInitializationRequired))
	assertMissingInitializationFixtures(t, ctx, pool)

	// Dry-run has the same authorization boundary, including when dry-run is
	// selected at construction time.
	dryRun := migration.NewApplier(pool, true)
	err = dryRun.Apply(ctx, migrations, migration.ApplyOptions{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrHistoryInitializationRequired))
	assertMissingInitializationFixtures(t, ctx, pool)

	require.NoError(t, dryRun.Apply(ctx, migrations, migration.ApplyOptions{
		InitializeHistory: true,
	}))
	assertMissingInitializationFixtures(t, ctx, pool)

	require.NoError(t, applier.Apply(ctx, migrations, migration.ApplyOptions{
		InitializeHistory: true,
	}))

	var historyExists, migrationTableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('schemata.version') IS NOT NULL").Scan(&historyExists))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_initialization_guard') IS NOT NULL").Scan(&migrationTableExists))
	assert.True(t, historyExists)
	assert.True(t, migrationTableExists)
}

func TestDryRunWithExistingHistoryDoesNotMutateLedger(t *testing.T) {
	ctx := context.Background()
	pool := migrationHistoryTestPool(t, ctx)
	cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

	migrations := []migration.Migration{
		{
			Version: "20260805010010",
			Name:    "dry-run-existing-baseline",
			SQL:     "CREATE TABLE public.schemata_history_managed_baseline (id integer PRIMARY KEY);",
		},
		{
			Version: "20260805010020",
			Name:    "dry-run-existing-pending",
			SQL:     "CREATE TABLE public.schemata_history_replay_sentinel (id integer PRIMARY KEY);",
		},
	}
	applier := migration.NewApplier(pool, false)
	require.NoError(t, applier.Apply(ctx, migrations[:1], migration.ApplyOptions{
		InitializeHistory: true,
	}))
	tracker := db.NewMigrationTracker(pool)
	before, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)

	require.NoError(t, migration.NewApplier(pool, true).Apply(ctx, migrations, migration.ApplyOptions{}))

	after, err := tracker.GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	assert.Equal(t, before, after, "dry-run must not mutate an existing ledger")
	var pendingTableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_replay_sentinel') IS NOT NULL").Scan(&pendingTableExists))
	assert.False(t, pendingTableExists)
}

func TestLostMigrationHistoryDoesNotReplayWithoutAuthorization(t *testing.T) {
	ctx := context.Background()
	pool := migrationHistoryTestPool(t, ctx)
	cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

	migrations := []migration.Migration{
		{
			Version: "20260805010100",
			Name:    "managed-baseline",
			SQL:     "CREATE TABLE public.schemata_history_managed_baseline (id integer PRIMARY KEY);",
		},
		{
			Version: "20260805010200",
			Name:    "must-not-replay",
			SQL:     "CREATE TABLE public.schemata_history_replay_sentinel (id integer PRIMARY KEY);",
		},
	}
	applier := migration.NewApplier(pool, false)
	require.NoError(t, applier.Apply(ctx, migrations[:1], migration.ApplyOptions{
		InitializeHistory: true,
	}))

	_, err := pool.Exec(ctx, "DROP TABLE schemata.version")
	require.NoError(t, err)

	err = applier.Apply(ctx, migrations, migration.ApplyOptions{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrHistoryInitializationRequired))

	var baselineExists, replayExists, historyExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_managed_baseline') IS NOT NULL").Scan(&baselineExists))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_replay_sentinel') IS NOT NULL").Scan(&replayExists))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('schemata.version') IS NOT NULL").Scan(&historyExists))
	assert.True(t, baselineExists)
	assert.False(t, replayExists)
	assert.False(t, historyExists, "normal apply must not silently recreate a lost ledger")
}

func TestResetAndApplyRefusesBeforeDestructionWithoutAuthorization(t *testing.T) {
	ctx := context.Background()
	pool := migrationHistoryTestPool(t, ctx)
	cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805010210",
		Name:    "reset-authorization-baseline",
		SQL:     "CREATE TABLE public.schemata_history_managed_baseline (id integer PRIMARY KEY);",
	}}
	applier := migration.NewApplier(pool, false)
	require.NoError(t, applier.Apply(ctx, migrations, migration.ApplyOptions{
		InitializeHistory: true,
	}))

	resetCalled := false
	err := applier.ResetAndApply(ctx, migrations, migration.ApplyOptions{}, func(context.Context) error {
		resetCalled = true
		_, resetErr := pool.Exec(ctx, `
			DROP TABLE public.schemata_history_managed_baseline;
			DROP SCHEMA schemata CASCADE;
		`)
		return resetErr
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrHistoryResetAuthorizationRequired))
	assert.False(t, resetCalled, "unauthorized reset must be rejected before its destructive callback")

	var baselineExists, historyExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_managed_baseline') IS NOT NULL").Scan(&baselineExists))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('schemata.version') IS NOT NULL").Scan(&historyExists))
	assert.True(t, baselineExists)
	assert.True(t, historyExists)
	history, historyErr := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, historyErr)
	require.Len(t, history, 1)
	assert.Equal(t, migrations[0].Version, history[0].Version)
}

func TestResetAndApplyKeepsDestructionAndReplayUnderRunnerLock(t *testing.T) {
	ctx := context.Background()
	pool := migrationHistoryTestPool(t, ctx)
	cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805010220",
		Name:    "runner-fenced-reset",
		SQL:     "CREATE TABLE public.schemata_history_managed_baseline (id integer PRIMARY KEY);",
	}}
	require.NoError(t, migrations[0].LoadSQL())
	applier := migration.NewApplier(pool, false)
	require.NoError(t, applier.Apply(ctx, migrations, migration.ApplyOptions{
		InitializeHistory: true,
	}))

	resetEntered := make(chan struct{})
	continueReset := make(chan struct{})
	resetResult := make(chan error, 1)
	go func() {
		resetMigrations := append([]migration.Migration(nil), migrations...)
		resetResult <- applier.ResetAndApply(ctx, resetMigrations, migration.ApplyOptions{
			InitializeHistory: true,
		}, func(resetCtx context.Context) error {
			close(resetEntered)
			select {
			case <-continueReset:
			case <-resetCtx.Done():
				return context.Cause(resetCtx)
			}
			_, err := pool.Exec(resetCtx, `
				DROP TABLE public.schemata_history_managed_baseline;
				DROP SCHEMA schemata CASCADE;
			`)
			return err
		})
	}()
	<-resetEntered

	competingCtx, cancelCompeting := context.WithTimeout(ctx, 150*time.Millisecond)
	competingMigrations := append([]migration.Migration(nil), migrations...)
	err := migration.NewApplier(pool, false).Apply(competingCtx, competingMigrations, migration.ApplyOptions{})
	cancelCompeting()
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "another runner must not cross the reset callback")

	close(continueReset)
	require.NoError(t, <-resetResult)

	var baselineExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_managed_baseline') IS NOT NULL").Scan(&baselineExists))
	assert.True(t, baselineExists)
	history, historyErr := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, historyErr)
	require.Len(t, history, 1)
	assert.Equal(t, migrations[0].Version, history[0].Version)
	assert.Equal(t, db.MigrationStatusApplied, history[0].Status)
}

func TestResetAndApplyAllowsExplicitAppliedInventoryReplacement(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		replacementVersion string
	}{
		{name: "modified migration", replacementVersion: "20260805010230"},
		{name: "deleted migration", replacementVersion: "20260805010231"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			pool := migrationHistoryTestPool(t, ctx)
			cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
			t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

			original := []migration.Migration{{
				Version: "20260805010230",
				Name:    "old-dev-source",
				SQL:     "CREATE TABLE public.schemata_history_managed_baseline (id integer PRIMARY KEY);",
			}}
			applier := migration.NewApplier(pool, false)
			require.NoError(t, applier.Apply(ctx, original, migration.ApplyOptions{
				InitializeHistory: true,
			}))

			replacement := []migration.Migration{{
				Version: testCase.replacementVersion,
				Name:    "replacement-dev-source",
				SQL:     "CREATE TABLE public.schemata_history_replay_sentinel (id integer PRIMARY KEY);",
			}}
			require.NoError(t, replacement[0].LoadSQL())
			require.NoError(t, applier.ResetAndApply(ctx, replacement, migration.ApplyOptions{
				InitializeHistory: true,
			}, func(resetCtx context.Context) error {
				_, err := pool.Exec(resetCtx, `
					DROP TABLE public.schemata_history_managed_baseline;
					DROP SCHEMA schemata CASCADE;
				`)
				return err
			}))

			var baselineExists, replacementExists bool
			require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_managed_baseline') IS NOT NULL").Scan(&baselineExists))
			require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_replay_sentinel') IS NOT NULL").Scan(&replacementExists))
			assert.False(t, baselineExists)
			assert.True(t, replacementExists)

			history, err := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
			require.NoError(t, err)
			require.Len(t, history, 1)
			assert.Equal(t, replacement[0].Version, history[0].Version)
			assert.Equal(t, replacement[0].Checksum, history[0].Checksum)
		})
	}
}

func TestConcurrentMigrationHistoryInitializationIsSerialized(t *testing.T) {
	ctx := context.Background()
	pool := migrationHistoryTestPool(t, ctx)
	cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805010300",
		Name:    "concurrent-history-initialization",
		SQL:     "CREATE TABLE public.schemata_history_concurrent_guard (id integer PRIMARY KEY);",
	}}
	require.NoError(t, migrations[0].LoadSQL())

	start := make(chan struct{})
	results := make(chan error, 2)
	var runners sync.WaitGroup
	for range 2 {
		runners.Add(1)
		go func() {
			defer runners.Done()
			<-start
			runnerMigrations := append([]migration.Migration(nil), migrations...)
			results <- migration.NewApplier(pool, false).Apply(ctx, runnerMigrations, migration.ApplyOptions{
				InitializeHistory: true,
			})
		}()
	}
	close(start)
	runners.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}

	var migrationTableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_concurrent_guard') IS NOT NULL").Scan(&migrationTableExists))
	assert.True(t, migrationTableExists)

	history, err := db.NewMigrationTracker(pool).GetHistoryWithExecutor(ctx, pool)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, migrations[0].Version, history[0].Version)
	assert.Equal(t, db.MigrationStatusApplied, history[0].Status)
}

func TestRecoveryStillRefusesMissingMigrationHistory(t *testing.T) {
	ctx := context.Background()
	pool := migrationHistoryTestPool(t, ctx)
	cleanMigrationHistoryInitializationFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanMigrationHistoryInitializationFixtures(t, ctx, pool) })

	migrations := []migration.Migration{{
		Version: "20260805010400",
		Name:    "missing-history-recovery",
		SQL: `-- schemata:transaction off
			CREATE INDEX CONCURRENTLY schemata_missing_history_recovery_idx
			ON public.schemata_missing_history_recovery_fixture (id);`,
	}}
	require.NoError(t, migrations[0].LoadSQL())

	err := migration.NewApplier(pool, false).Recover(ctx, migrations, migration.RecoveryOptions{
		Version: migrations[0].Version,
		Action:  migration.RecoveryActionMarkApplied,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration history does not exist; there is nothing to recover")

	var historySchemaExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regnamespace('schemata') IS NOT NULL").Scan(&historySchemaExists))
	assert.False(t, historySchemaExists)
}

func migrationHistoryTestPool(t *testing.T, ctx context.Context) *db.Pool {
	t.Helper()
	connection := &config.DBConnection{URL: strPtr(targetDBURL)}
	pool, err := db.Connect(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func cleanMigrationHistoryInitializationFixtures(t *testing.T, ctx context.Context, pool *db.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS schemata CASCADE;
		DROP TABLE IF EXISTS public.schemata_history_initialization_guard;
		DROP TABLE IF EXISTS public.schemata_history_managed_baseline;
		DROP TABLE IF EXISTS public.schemata_history_replay_sentinel;
		DROP TABLE IF EXISTS public.schemata_history_concurrent_guard;
		DROP TABLE IF EXISTS public.schemata_missing_history_recovery_fixture;
	`)
	require.NoError(t, err)
}

func assertMissingInitializationFixtures(t *testing.T, ctx context.Context, pool *db.Pool) {
	t.Helper()
	var historySchemaExists, migrationTableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regnamespace('schemata') IS NOT NULL").Scan(&historySchemaExists))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_history_initialization_guard') IS NOT NULL").Scan(&migrationTableExists))
	assert.False(t, historySchemaExists, "refusal and dry-run must not create the reserved schema")
	assert.False(t, migrationTableExists, "refusal and dry-run must not execute migration SQL")
}
