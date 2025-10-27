package db

import (
	"context"
	"fmt"
	"sort"
)

const (
	schemaName = "schemata"
	tableName  = "version"
)

// MigrationTracker manages migration version tracking in the database
type MigrationTracker struct {
	pool *Pool
}

// NewMigrationTracker creates a new migration tracker
func NewMigrationTracker(pool *Pool) *MigrationTracker {
	return &MigrationTracker{pool: pool}
}

// EnsureSchema creates the schemata schema and version table if they don't exist
func (mt *MigrationTracker) EnsureSchema(ctx context.Context) error {
	// Create schema
	_, err := mt.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Create version table
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			version_num TEXT PRIMARY KEY
		)
	`, schemaName, tableName)

	_, err = mt.pool.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create version table: %w", err)
	}

	return nil
}

// GetAppliedVersions returns all applied migration versions
func (mt *MigrationTracker) GetAppliedVersions(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf("SELECT version_num FROM %s.%s ORDER BY version_num", schemaName, tableName)

	rows, err := mt.pool.Query(ctx, query)
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

// IsApplied checks if a specific version has been applied
func (mt *MigrationTracker) IsApplied(ctx context.Context, version string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s.%s WHERE version_num = $1)", schemaName, tableName)

	var exists bool
	err := mt.pool.QueryRow(ctx, query, version).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if version is applied: %w", err)
	}

	return exists, nil
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
	appliedVersions, err := mt.GetAppliedVersions(ctx)
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

// RemoveVersion removes a version from the tracking table (for rollback scenarios)
func (mt *MigrationTracker) RemoveVersion(ctx context.Context, version string) error {
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE version_num = $1", schemaName, tableName)

	_, err := mt.pool.Exec(ctx, query, version)
	if err != nil {
		return fmt.Errorf("failed to remove version: %w", err)
	}

	return nil
}

// GetLatestVersion returns the most recently applied version
func (mt *MigrationTracker) GetLatestVersion(ctx context.Context) (string, error) {
	query := fmt.Sprintf("SELECT version_num FROM %s.%s ORDER BY version_num DESC LIMIT 1", schemaName, tableName)

	var version string
	err := mt.pool.QueryRow(ctx, query).Scan(&version)
	if err != nil {
		// No versions applied yet
		return "", nil
	}

	return version, nil
}
