package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/config"
)

const (
	// DefaultStatementTimeout bounds any individual PostgreSQL statement when
	// the configuration does not provide an override.
	DefaultStatementTimeout = 15 * time.Minute
	// DefaultLockTimeout prevents an operation from waiting indefinitely for a
	// PostgreSQL lock when the configuration does not provide an override.
	DefaultLockTimeout = 10 * time.Second
)

type connectOptions struct {
	statementTimeout time.Duration
	lockTimeout      time.Duration
}

// ConnectOption customizes PostgreSQL sessions opened by Connect.
type ConnectOption func(*connectOptions) error

// WithTimeouts sets PostgreSQL statement_timeout and lock_timeout for every
// session in the pool. Zero explicitly disables a timeout; negative values are
// rejected.
func WithTimeouts(statementTimeout, lockTimeout time.Duration) ConnectOption {
	return func(options *connectOptions) error {
		if statementTimeout < 0 {
			return fmt.Errorf("statement timeout must not be negative")
		}
		if lockTimeout < 0 {
			return fmt.Errorf("lock timeout must not be negative")
		}

		options.statementTimeout = statementTimeout
		options.lockTimeout = lockTimeout
		return nil
	}
}

// WithDatabaseConfig applies the explicitly configured timeout values while
// retaining finite defaults for omitted values.
func WithDatabaseConfig(databaseConfig config.DatabaseConfig) ConnectOption {
	return func(options *connectOptions) error {
		statementTimeout := options.statementTimeout
		if databaseConfig.StatementTimeout != nil {
			statementTimeout = databaseConfig.StatementTimeout.Duration
		}

		lockTimeout := options.lockTimeout
		if databaseConfig.LockTimeout != nil {
			lockTimeout = databaseConfig.LockTimeout.Duration
		}

		return WithTimeouts(statementTimeout, lockTimeout)(options)
	}
}

// Executor is an interface that both Pool and Tx implement
// This allows functions to work with either a connection pool or a transaction
type Executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Pool wraps a database connection pool
type Pool struct {
	*pgxpool.Pool
}

// TargetIdentityMismatchError reports that a connection reached a database or
// PostgreSQL cluster other than the configured production target.
type TargetIdentityMismatchError struct {
	ExpectedDatabase         string
	ActualDatabase           string
	ExpectedSystemIdentifier uint64
	ActualSystemIdentifier   uint64
}

func (err *TargetIdentityMismatchError) Error() string {
	return fmt.Sprintf(
		"target identity mismatch: expected database %q with PostgreSQL system identifier %d, connected to database %q with system identifier %d",
		err.ExpectedDatabase,
		err.ExpectedSystemIdentifier,
		err.ActualDatabase,
		err.ActualSystemIdentifier,
	)
}

// TargetIdentityPermissionError reports that the connected role cannot read
// the cluster identifier required for target attestation.
type TargetIdentityPermissionError struct {
	Role string
	Err  error
}

func (err *TargetIdentityPermissionError) Error() string {
	quotedRole := pgx.Identifier{err.Role}.Sanitize()
	return fmt.Sprintf(
		"permission denied while reading PostgreSQL system identity: %v\n"+
			"target identity verification requires pg_monitor membership or EXECUTE on pg_catalog.pg_control_system(); a database administrator can run either:\n"+
			"  GRANT pg_monitor TO %s;\n"+
			"  GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO %s;",
		err.Err,
		quotedRole,
		quotedRole,
	)
}

func (err *TargetIdentityPermissionError) Unwrap() error {
	return err.Err
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Connect creates a new connection pool from a DBConnection config. Every
// session receives finite PostgreSQL statement and lock timeouts by default.
func Connect(ctx context.Context, conn *config.DBConnection, opts ...ConnectOption) (*Pool, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection config is nil")
	}
	if err := conn.Validate(); err != nil {
		return nil, fmt.Errorf("invalid connection config: %w", err)
	}
	var expectedIdentity *config.DatabaseIdentity
	if conn.Identity != nil {
		identityCopy := *conn.Identity
		expectedIdentity = &identityCopy
	}

	connStr, err := conn.ToConnectionString()
	if err != nil {
		return nil, fmt.Errorf("failed to build connection string: %w", err)
	}

	options := connectOptions{
		statementTimeout: DefaultStatementTimeout,
		lockTimeout:      DefaultLockTimeout,
	}
	for _, applyOption := range opts {
		if applyOption == nil {
			continue
		}
		if err := applyOption(&options); err != nil {
			return nil, fmt.Errorf("invalid database connection option: %w", err)
		}
	}

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = postgresTimeoutValue(options.statementTimeout)
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = postgresTimeoutValue(options.lockTimeout)
	if expectedIdentity != nil {
		previousAfterConnect := poolConfig.AfterConnect
		poolConfig.AfterConnect = func(ctx context.Context, connection *pgx.Conn) error {
			if previousAfterConnect != nil {
				if err := previousAfterConnect(ctx, connection); err != nil {
					return err
				}
			}
			return verifyTargetIdentity(ctx, connection, connection.Config().User, *expectedIdentity)
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	if expectedIdentity != nil {
		// AfterConnect attests every physical session, including the one used by
		// Ping. Keep this explicit post-Ping gate so Connect itself cannot return
		// a pool without completing identity verification.
		if err := verifyTargetIdentity(ctx, pool, poolConfig.ConnConfig.User, *expectedIdentity); err != nil {
			pool.Close()
			return nil, err
		}
	}

	return &Pool{Pool: pool}, nil
}

func verifyTargetIdentity(
	ctx context.Context,
	querier rowQuerier,
	role string,
	expected config.DatabaseIdentity,
) error {
	expectedSystemIdentifier, err := expected.SystemIdentifierValue()
	if err != nil {
		return fmt.Errorf("invalid expected target identity: %w", err)
	}

	var actualDatabase string
	var actualSystemIdentifierText string
	err = querier.QueryRow(ctx, `
		SELECT
			current_database()::text,
			(pg_catalog.pg_control_system()).system_identifier::text
	`).Scan(&actualDatabase, &actualSystemIdentifierText)
	if err != nil {
		return targetIdentityQueryError(role, err)
	}

	actualSystemIdentifier, err := strconv.ParseUint(actualSystemIdentifierText, 10, 64)
	if err != nil {
		return fmt.Errorf(
			"failed to verify target identity: PostgreSQL returned invalid system identifier %q: %w",
			actualSystemIdentifierText,
			err,
		)
	}

	if actualDatabase != expected.Database || actualSystemIdentifier != expectedSystemIdentifier {
		return &TargetIdentityMismatchError{
			ExpectedDatabase:         expected.Database,
			ActualDatabase:           actualDatabase,
			ExpectedSystemIdentifier: expectedSystemIdentifier,
			ActualSystemIdentifier:   actualSystemIdentifier,
		}
	}

	return nil
}

func targetIdentityQueryError(role string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42501" {
		return &TargetIdentityPermissionError{Role: role, Err: err}
	}

	return fmt.Errorf("failed to query PostgreSQL target identity: %w", err)
}

// postgresTimeoutValue converts a Go duration to PostgreSQL's millisecond GUC
// representation. Positive sub-millisecond remainders round up so a requested
// safety timeout can never become PostgreSQL's special "disabled" value.
func postgresTimeoutValue(timeout time.Duration) string {
	if timeout == 0 {
		return "0"
	}

	milliseconds := timeout / time.Millisecond
	if timeout%time.Millisecond != 0 {
		milliseconds++
	}
	return strconv.FormatInt(int64(milliseconds), 10)
}

// Close closes the connection pool
func (p *Pool) Close() {
	if p.Pool != nil {
		p.Pool.Close()
	}
}
