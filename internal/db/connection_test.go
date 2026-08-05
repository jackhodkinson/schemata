package db

import (
	"context"
	"testing"
	"time"

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
