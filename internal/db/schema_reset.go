package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackhodkinson/schemata/internal/sqlrender"
)

const schemaResetCleanupTimeout = 5 * time.Second

// ResetSchemas atomically drops and recreates every non-system schema, then
// removes Schemata's tracking schema so a caller can replay migration history.
// Catalog-derived identifiers are always validated and quoted as data.
func ResetSchemas(ctx context.Context, pool *Pool, allowCascade bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin schema reset transaction: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), schemaResetCleanupTimeout)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	rows, err := tx.Query(ctx, `
		SELECT nspname
		FROM pg_catalog.pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'schemata')
		  AND nspname NOT LIKE 'pg_temp_%'
		  AND nspname NOT LIKE 'pg_toast_temp_%'
		ORDER BY nspname
	`)
	if err != nil {
		return fmt.Errorf("failed to query schemas: %w", err)
	}

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan schema name: %w", err)
		}
		if err := sqlrender.ValidateIdentifier(schemaName); err != nil {
			rows.Close()
			return fmt.Errorf("invalid schema name returned by PostgreSQL during reset: %w", err)
		}
		schemas = append(schemas, schemaName)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading schema rows: %w", err)
	}

	dropMode := "RESTRICT"
	if allowCascade {
		dropMode = "CASCADE"
	}

	for _, schemaName := range schemas {
		quotedSchema := sqlrender.Identifier(schemaName)
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DROP SCHEMA IF EXISTS %s %s",
			quotedSchema,
			dropMode,
		)); err != nil {
			if !allowCascade {
				return fmt.Errorf("failed to drop schema %s: %w\n\nHint: Schema has dependent objects. Use --allow-cascade to drop with CASCADE (this will drop all dependent objects)", schemaName, err)
			}
			return fmt.Errorf("failed to drop schema %s: %w", schemaName, err)
		}

		if _, err := tx.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
			return fmt.Errorf("failed to create schema %s: %w", schemaName, err)
		}
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"DROP SCHEMA IF EXISTS %s %s",
		sqlrender.Identifier("schemata"),
		dropMode,
	)); err != nil {
		if !allowCascade {
			return fmt.Errorf("failed to drop schemata tracking schema: %w\n\nHint: Schema has dependent objects. Use --allow-cascade to drop with CASCADE (this will drop all dependent objects)", err)
		}
		return fmt.Errorf("failed to drop schemata tracking schema: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit schema reset transaction: %w", err)
	}
	return nil
}
