package db

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	schemaName = "schemata"
	tableName  = "version"

	ensureSchemaLockName    = "schemata:ensure-schema"
	ensureSchemaLockTimeout = 30 * time.Second
)

// MigrationTracker manages migration version tracking in the database
type MigrationTracker struct {
	pool *Pool
}

// NewMigrationTracker creates a new migration tracker
func NewMigrationTracker(pool *Pool) *MigrationTracker {
	return &MigrationTracker{pool: pool}
}

// EnsureSchema creates the schemata schema and version table if they don't exist.
// This is safe to call concurrently from multiple processes.
func (mt *MigrationTracker) EnsureSchema(ctx context.Context) error {
	return WithSessionAdvisoryLock(
		ctx,
		mt.pool,
		ensureSchemaLockName,
		ensureSchemaLockTimeout,
		func(conn *pgxpool.Conn) error {
			return mt.ensureSchema(ctx, conn)
		},
	)
}

func (mt *MigrationTracker) ensureSchema(ctx context.Context, executor Executor) error {
	// PostgreSQL's CREATE SCHEMA IF NOT EXISTS can still race internally, so
	// EnsureSchema serializes this block with a session advisory lock.
	_, err := executor.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Create version table
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			version_num TEXT PRIMARY KEY
		)
	`, schemaName, tableName)

	_, err = executor.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create version table: %w", err)
	}

	return nil
}

// GetAppliedVersions returns all applied migration versions
func (mt *MigrationTracker) GetAppliedVersions(ctx context.Context) ([]string, error) {
	return mt.GetAppliedVersionsWithExecutor(ctx, mt.pool)
}

// GetAppliedVersionsWithExecutor returns applied versions using a specific
// connection or transaction. This is used while a session advisory lock is
// held so history reads cannot escape to another pooled connection.
func (mt *MigrationTracker) GetAppliedVersionsWithExecutor(ctx context.Context, executor Executor) ([]string, error) {
	query := fmt.Sprintf("SELECT version_num FROM %s.%s ORDER BY version_num", schemaName, tableName)

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied versions: %w", err)
	}
	defer rows.Close()

	var versions []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("failed to scan version: %w", err)
		}
		versions = append(versions, version)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating versions: %w", err)
	}

	return versions, nil
}

// MarkApplied marks a migration version as applied
// The executor parameter allows this to run within a transaction
func (mt *MigrationTracker) MarkApplied(ctx context.Context, executor Executor, version string) error {
	query := fmt.Sprintf("INSERT INTO %s.%s (version_num) VALUES ($1)", schemaName, tableName)

	_, err := executor.Exec(ctx, query, version)
	if err != nil {
		return fmt.Errorf("failed to mark version as applied: %w", err)
	}

	return nil
}

// GetPendingVersions returns versions that haven't been applied yet
func (mt *MigrationTracker) GetPendingVersions(ctx context.Context, availableVersions []string) ([]string, error) {
	return mt.GetPendingVersionsWithExecutor(ctx, mt.pool, availableVersions)
}

// GetPendingVersionsWithExecutor computes pending versions using a specific
// connection or transaction.
func (mt *MigrationTracker) GetPendingVersionsWithExecutor(
	ctx context.Context,
	executor Executor,
	availableVersions []string,
) ([]string, error) {
	appliedVersions, err := mt.GetAppliedVersionsWithExecutor(ctx, executor)
	if err != nil {
		return nil, err
	}

	// Create set of applied versions for efficient lookup
	appliedSet := make(map[string]bool)
	for _, v := range appliedVersions {
		appliedSet[v] = true
	}

	// Find pending versions
	var pending []string
	for _, v := range availableVersions {
		if !appliedSet[v] {
			pending = append(pending, v)
		}
	}

	// Sort pending versions
	sort.Strings(pending)

	return pending, nil
}
