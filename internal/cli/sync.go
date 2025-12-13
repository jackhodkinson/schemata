package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync dev database to match migrations directory",
	Long: `Reset the dev database and replay all migrations.

This command will:
1. Drop all objects from the dev database
2. Apply all migrations from the migrations directory

This is useful when you've deleted/modified migrations and need to
reset your dev database to match.

Examples:
  # Sync dev database
  schemata sync
`,
	RunE: runSync,
}

func runSync(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if cfg.Dev == nil {
		return fmt.Errorf("dev database not configured")
	}

	ctx := context.Background()

	// Connect to dev database
	fmt.Println("Connecting to dev database...")
	pool, err := db.Connect(ctx, cfg.Dev)
	if err != nil {
		return fmt.Errorf("failed to connect to dev database: %w", err)
	}
	defer pool.Close()

	// Drop all objects from dev database
	fmt.Println("Dropping all objects from dev database...")
	if err := dropAllObjects(ctx, pool, allowCascade); err != nil {
		return fmt.Errorf("failed to drop objects: %w", err)
	}

	// Scan migrations
	fmt.Printf("Scanning migrations directory: %s\n", cfg.Migrations)
	scanner := migration.NewScanner(cfg.Migrations)
	migrations, err := scanner.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan migrations: %w", err)
	}

	if len(migrations) == 0 {
		fmt.Println("No migrations found")
		return nil
	}

	fmt.Printf("Found %d migration(s)\n", len(migrations))

	// Apply all migrations
	fmt.Println("Applying migrations...")
	applier := migration.NewApplier(pool, false)
	opts := migration.ApplyOptions{
		DryRun:          false,
		ContinueOnError: false,
	}

	if err := applier.Apply(ctx, migrations, opts); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	fmt.Println("\n✓ Dev database synced successfully")
	return nil
}

// dropAllObjects drops all objects from the dev database by dropping and recreating schemas
func dropAllObjects(ctx context.Context, pool *db.Pool, allowCascade bool) error {
	// Get list of user schemas (excluding system schemas)
	query := `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'schemata')
		  AND nspname NOT LIKE 'pg_temp_%'
		  AND nspname NOT LIKE 'pg_toast_temp_%'
		ORDER BY nspname
	`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return fmt.Errorf("failed to scan schema name: %w", err)
		}
		schemas = append(schemas, schemaName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading schema rows: %w", err)
	}

	// Determine drop mode based on allowCascade flag
	dropMode := "RESTRICT"
	if allowCascade {
		dropMode = "CASCADE"
	}

	// Drop each schema and recreate it
	for _, schemaName := range schemas {
		fmt.Printf("  - Dropping schema: %s\n", schemaName)
		dropSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %s %s", schemaName, dropMode)
		if _, err := pool.Exec(ctx, dropSQL); err != nil {
			if !allowCascade {
				return fmt.Errorf("failed to drop schema %s: %w\n\nHint: Schema has dependent objects. Use --allow-cascade to drop with CASCADE (this will drop all dependent objects)", schemaName, err)
			}
			return fmt.Errorf("failed to drop schema %s: %w", schemaName, err)
		}

		fmt.Printf("  - Recreating schema: %s\n", schemaName)
		createSQL := fmt.Sprintf("CREATE SCHEMA %s", schemaName)
		if _, err := pool.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create schema %s: %w", schemaName, err)
		}
	}

	// Also drop the schemata tracking schema if it exists
	dropTrackingSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS schemata %s", dropMode)
	if _, err := pool.Exec(ctx, dropTrackingSQL); err != nil {
		if !allowCascade {
			return fmt.Errorf("failed to drop schemata tracking schema: %w\n\nHint: Schema has dependent objects. Use --allow-cascade to drop with CASCADE (this will drop all dependent objects)", err)
		}
		return fmt.Errorf("failed to drop schemata tracking schema: %w", err)
	}

	return nil
}
