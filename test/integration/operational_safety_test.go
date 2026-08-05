//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfiguredPostgresTimeoutsApplyToEverySession(t *testing.T) {
	ctx := context.Background()
	conn := &config.DBConnection{URL: strPtr(devDBURL)}
	statementTimeout := config.Duration{Duration: 80 * time.Millisecond}
	lockTimeout := config.Duration{Duration: 60 * time.Millisecond}
	pool, err := db.Connect(
		ctx,
		conn,
		db.WithDatabaseConfig(config.DatabaseConfig{
			StatementTimeout: &statementTimeout,
			LockTimeout:      &lockTimeout,
		}),
	)
	require.NoError(t, err)
	defer pool.Close()

	connections := make([]*pgxpool.Conn, 0, 2)
	for range 2 {
		connection, err := pool.Acquire(ctx)
		require.NoError(t, err)
		connections = append(connections, connection)
	}
	defer func() {
		for _, connection := range connections {
			connection.Release()
		}
	}()

	for _, connection := range connections {
		assertSessionTimeouts(t, ctx, connection, "80ms", "60ms")
	}
	for _, connection := range connections {
		connection.Release()
	}
	connections = nil

	err = db.WithDedicatedConnection(ctx, pool, func(connection *pgxpool.Conn) error {
		assertSessionTimeouts(t, ctx, connection, "80ms", "60ms")
		return nil
	})
	require.NoError(t, err)

	started := time.Now()
	err = pool.QueryRow(ctx, "SELECT pg_sleep(1)").Scan(new(any))
	require.Error(t, err)
	assert.Less(t, time.Since(started), 750*time.Millisecond)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "57014", pgErr.Code)
	assert.Contains(t, pgErr.Message, "statement timeout")
}

func assertSessionTimeouts(
	t *testing.T,
	ctx context.Context,
	connection *pgxpool.Conn,
	wantStatementTimeout string,
	wantLockTimeout string,
) {
	t.Helper()

	var statementTimeout string
	var lockTimeout string
	err := connection.QueryRow(ctx, `SELECT current_setting('statement_timeout'), current_setting('lock_timeout')`).Scan(
		&statementTimeout,
		&lockTimeout,
	)
	require.NoError(t, err)
	assert.Equal(t, wantStatementTimeout, statementTimeout)
	assert.Equal(t, wantLockTimeout, lockTimeout)
}

func TestDefaultPostgresTimeoutsAreFinite(t *testing.T) {
	ctx := context.Background()
	conn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(ctx, conn)
	require.NoError(t, err)
	defer pool.Close()

	var statementTimeoutMilliseconds int64
	var lockTimeoutMilliseconds int64
	err = pool.QueryRow(ctx, `
		SELECT
			(SELECT setting::bigint FROM pg_settings WHERE name = 'statement_timeout'),
			(SELECT setting::bigint FROM pg_settings WHERE name = 'lock_timeout')
	`).Scan(&statementTimeoutMilliseconds, &lockTimeoutMilliseconds)
	require.NoError(t, err)
	assert.Equal(t, db.DefaultStatementTimeout.Milliseconds(), statementTimeoutMilliseconds)
	assert.Equal(t, db.DefaultLockTimeout.Milliseconds(), lockTimeoutMilliseconds)
}

func TestExpectedTargetIdentityAcceptsMatchingDatabaseAndCluster(t *testing.T) {
	ctx := context.Background()
	database, systemIdentifier := readTargetIdentity(t, ctx, devDBURL)
	configPath := t.TempDir() + "/schemata.yaml"
	configYAML := fmt.Sprintf(`
target:
  url: %q
  identity:
    database: %q
    system-identifier: %q
schema: schema.sql
migrations: ./migrations
`, devDBURL, database, systemIdentifier)
	require.NoError(t, os.WriteFile(configPath, []byte(configYAML), 0o600))
	cfg, err := config.Load(configPath)
	require.NoError(t, err)

	pool, err := db.Connect(ctx, cfg.Target)
	require.NoError(t, err)
	defer pool.Close()

	// Hold two sessions concurrently so the per-physical-connection check runs
	// for pool growth, not only for the session used by Connect's Ping.
	first, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer first.Release()
	second, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer second.Release()

	for _, connection := range []*pgxpool.Conn{first, second} {
		var gotDatabase string
		err := connection.QueryRow(ctx, "SELECT current_database()").Scan(&gotDatabase)
		require.NoError(t, err)
		assert.Equal(t, database, gotDatabase)
	}

	err = db.WithDedicatedConnection(ctx, pool, func(connection *pgxpool.Conn) error {
		var gotDatabase string
		if err := connection.QueryRow(ctx, "SELECT current_database()").Scan(&gotDatabase); err != nil {
			return err
		}
		assert.Equal(t, database, gotDatabase)
		return nil
	})
	require.NoError(t, err)
}

func TestExpectedTargetIdentityRejectsMismatches(t *testing.T) {
	ctx := context.Background()
	database, systemIdentifier := readTargetIdentity(t, ctx, devDBURL)
	systemIdentifierValue, err := strconv.ParseUint(systemIdentifier, 10, 64)
	require.NoError(t, err)
	wrongSystemIdentifier := systemIdentifierValue + 1
	if wrongSystemIdentifier == 0 {
		wrongSystemIdentifier = systemIdentifierValue - 1
	}

	tests := []struct {
		name               string
		expectedDatabase   string
		expectedIdentifier string
	}{
		{
			name:               "database",
			expectedDatabase:   database + "_wrong",
			expectedIdentifier: systemIdentifier,
		},
		{
			name:               "system identifier",
			expectedDatabase:   database,
			expectedIdentifier: strconv.FormatUint(wrongSystemIdentifier, 10),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connectCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			pool, err := db.Connect(connectCtx, &config.DBConnection{
				URL: strPtr(devDBURL),
				Identity: &config.DatabaseIdentity{
					Database:         tt.expectedDatabase,
					SystemIdentifier: tt.expectedIdentifier,
				},
			})
			require.Error(t, err)
			assert.Nil(t, pool)
			var mismatch *db.TargetIdentityMismatchError
			require.ErrorAs(t, err, &mismatch)
			assert.Equal(t, database, mismatch.ActualDatabase)
			assert.Equal(t, systemIdentifierValue, mismatch.ActualSystemIdentifier)
		})
	}
}

func readTargetIdentity(t *testing.T, ctx context.Context, databaseURL string) (string, string) {
	t.Helper()

	pool, err := db.Connect(ctx, &config.DBConnection{URL: strPtr(databaseURL)})
	require.NoError(t, err)
	defer pool.Close()

	var database string
	var systemIdentifier string
	err = pool.QueryRow(ctx, `
		SELECT current_database()::text,
		       (pg_catalog.pg_control_system()).system_identifier::text
	`).Scan(&database, &systemIdentifier)
	require.NoError(t, err)
	return database, systemIdentifier
}

func TestConfiguredLockTimeoutBoundsLockWait(t *testing.T) {
	ctx := context.Background()
	conn := &config.DBConnection{URL: strPtr(devDBURL)}
	blockerPool, err := db.Connect(ctx, conn)
	require.NoError(t, err)
	defer blockerPool.Close()

	waiterPool, err := db.Connect(
		ctx,
		conn,
		db.WithTimeouts(5*time.Second, 75*time.Millisecond),
	)
	require.NoError(t, err)
	defer waiterPool.Close()

	tableName := fmt.Sprintf("schemata_lock_timeout_%d", time.Now().UnixNano())
	quotedTableName := pgx.Identifier{tableName}.Sanitize()
	_, err = blockerPool.Exec(ctx, "CREATE TABLE "+quotedTableName+" (id integer)")
	require.NoError(t, err)
	defer func() {
		_, _ = blockerPool.Exec(context.Background(), "DROP TABLE IF EXISTS "+quotedTableName)
	}()

	blocker, err := blockerPool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback(context.Background()) }()
	_, err = blocker.Exec(ctx, "LOCK TABLE "+quotedTableName+" IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	waiter, err := waiterPool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = waiter.Rollback(context.Background()) }()

	started := time.Now()
	_, err = waiter.Exec(ctx, "LOCK TABLE "+quotedTableName+" IN ACCESS EXCLUSIVE MODE")
	require.Error(t, err)
	assert.Less(t, time.Since(started), time.Second)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "55P03", pgErr.Code)
	assert.Contains(t, pgErr.Message, "lock timeout")
}

func TestContextCancellationStopsInFlightPostgresStatement(t *testing.T) {
	conn := &config.DBConnection{URL: strPtr(devDBURL)}
	pool, err := db.Connect(context.Background(), conn)
	require.NoError(t, err)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- pool.QueryRow(ctx, "SELECT pg_sleep(10)").Scan(new(any))
	}()

	time.Sleep(50 * time.Millisecond)
	started := time.Now()
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "expected context cancellation, got %v", err)
		assert.Less(t, time.Since(started), time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("PostgreSQL statement did not stop after context cancellation")
	}
}
