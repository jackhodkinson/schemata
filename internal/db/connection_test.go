package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresTimeoutValue(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		want    string
	}{
		{name: "disabled", timeout: 0, want: "0"},
		{name: "milliseconds", timeout: 250 * time.Millisecond, want: "250"},
		{name: "whole seconds", timeout: 3 * time.Second, want: "3000"},
		{name: "sub millisecond rounds up", timeout: time.Microsecond, want: "1"},
		{name: "remainder rounds up", timeout: time.Millisecond + time.Microsecond, want: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, postgresTimeoutValue(tt.timeout))
		})
	}
}

func TestConnectRejectsInvalidOptionsBeforeOpeningPool(t *testing.T) {
	url := "postgresql://localhost:1/unused"
	conn := &config.DBConnection{URL: &url}

	_, err := Connect(
		context.Background(),
		conn,
		WithTimeouts(-time.Second, time.Second),
	)
	require.ErrorContains(t, err, "statement timeout must not be negative")

	_, err = Connect(
		context.Background(),
		conn,
		WithTimeouts(time.Second, -time.Second),
	)
	require.ErrorContains(t, err, "lock timeout must not be negative")
}

func TestConnectRejectsInvalidIdentityBeforeOpeningPool(t *testing.T) {
	url := "postgresql://localhost:1/unused"
	conn := &config.DBConnection{
		URL: &url,
		Identity: &config.DatabaseIdentity{
			Database: "unused",
		},
	}

	_, err := Connect(context.Background(), conn)
	require.ErrorContains(t, err, "identity.system-identifier must be specified")
}

func TestWithDatabaseConfigOverridesOnlyExplicitTimeouts(t *testing.T) {
	statementTimeout := config.Duration{Duration: 45 * time.Second}
	options := connectOptions{
		statementTimeout: DefaultStatementTimeout,
		lockTimeout:      DefaultLockTimeout,
	}

	err := WithDatabaseConfig(config.DatabaseConfig{
		StatementTimeout: &statementTimeout,
	})(&options)
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, options.statementTimeout)
	assert.Equal(t, DefaultLockTimeout, options.lockTimeout)
}

func TestWithDatabaseConfigPreservesExplicitZero(t *testing.T) {
	disabled := config.Duration{}
	options := connectOptions{
		statementTimeout: DefaultStatementTimeout,
		lockTimeout:      DefaultLockTimeout,
	}

	err := WithDatabaseConfig(config.DatabaseConfig{
		StatementTimeout: &disabled,
		LockTimeout:      &disabled,
	})(&options)
	require.NoError(t, err)
	assert.Zero(t, options.statementTimeout)
	assert.Zero(t, options.lockTimeout)
}

func TestVerifyTargetIdentity(t *testing.T) {
	t.Run("match", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "app", systemIdentifier: "123"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "000123"},
		)
		require.NoError(t, err)
	})

	t.Run("database mismatch", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "wrong", systemIdentifier: "123"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		var mismatch *TargetIdentityMismatchError
		require.ErrorAs(t, err, &mismatch)
		assert.Equal(t, "app", mismatch.ExpectedDatabase)
		assert.Equal(t, "wrong", mismatch.ActualDatabase)
		assert.Equal(t, uint64(123), mismatch.ExpectedSystemIdentifier)
		assert.Equal(t, uint64(123), mismatch.ActualSystemIdentifier)
	})

	t.Run("system identifier mismatch", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "app", systemIdentifier: "456"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		var mismatch *TargetIdentityMismatchError
		require.ErrorAs(t, err, &mismatch)
		assert.Equal(t, uint64(123), mismatch.ExpectedSystemIdentifier)
		assert.Equal(t, uint64(456), mismatch.ActualSystemIdentifier)
	})

	t.Run("invalid server value", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "app", systemIdentifier: "not-a-number"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		require.ErrorContains(t, err, "PostgreSQL returned invalid system identifier")
	})

	t.Run("permission failure", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{err: &pgconn.PgError{
				Code:    "42501",
				Message: "permission denied for function pg_control_system",
			}},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		var permissionErr *TargetIdentityPermissionError
		require.ErrorAs(t, err, &permissionErr)
		assert.Equal(t, "schemata", permissionErr.Role)
	})
}

func TestTargetIdentityPermissionErrorIncludesPreciseRemediation(t *testing.T) {
	err := targetIdentityQueryError(`deploy"role`, &pgconn.PgError{
		Code:    "42501",
		Message: "permission denied for function pg_control_system",
	})

	require.ErrorContains(t, err, "permission denied while reading PostgreSQL system identity")
	assert.Contains(t, err.Error(), `GRANT pg_monitor TO "deploy""role";`)
	assert.Contains(t, err.Error(), `GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "deploy""role";`)
	var permissionErr *TargetIdentityPermissionError
	require.ErrorAs(t, err, &permissionErr)
	assert.Equal(t, `deploy"role`, permissionErr.Role)

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "42501", pgErr.Code)
}

type stubIdentityQuerier struct {
	database         string
	systemIdentifier string
	err              error
}

func (querier stubIdentityQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	return stubIdentityRow(querier)
}

type stubIdentityRow stubIdentityQuerier

func (row stubIdentityRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != 2 {
		return errors.New("expected two identity destinations")
	}
	database, ok := destinations[0].(*string)
	if !ok {
		return errors.New("database destination is not *string")
	}
	systemIdentifier, ok := destinations[1].(*string)
	if !ok {
		return errors.New("system identifier destination is not *string")
	}
	*database = row.database
	*systemIdentifier = row.systemIdentifier
	return nil
}
