package cli

import (
	"context"
	"fmt"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/spf13/cobra"
)

var (
	migrateTarget string
	migrateDryRun bool
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply pending migrations to target database",
	Long: `Apply all pending migrations to the target database.

This command will:
1. Scan the migrations directory
2. Apply all pending migrations to the target database

Examples:
  schemata migrate
  schemata migrate --target staging
  schemata migrate --dry-run
`,
	RunE: runMigrate,
}

func init() {
	migrateCmd.Flags().StringVar(&migrateTarget, "target", "", "Target database (required if multiple targets configured)")
	migrateCmd.Flags().BoolVar(&migrateDryRun, "dry-run", false, "Show what would be applied without actually applying")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Load config
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Pre-flight check: ensure migrations are in sync with schema.sql
	fmt.Println("Running pre-flight check (diff --from migrations)...")
	if err := checkMigrationsInSync(ctx, cfg); err != nil {
		return fmt.Errorf("pre-flight check failed: %w\n\nHint: Run 'schemata generate <name>' to create a migration for the differences", err)
	}
	fmt.Println("✓ Migrations are in sync with schema.sql")

	// Determine target connection
	var targetConn *config.DBConnection
	if cfg.Target != nil {
		if migrateTarget != "" {
			return fmt.Errorf("--target flag specified but config only has single target")
		}
		targetConn = cfg.Target
	} else if cfg.Targets != nil {
		if migrateTarget == "" {
			return fmt.Errorf("multiple targets configured, must specify --target flag")
		}
		conn, exists := cfg.Targets[migrateTarget]
		if !exists {
			return fmt.Errorf("target '%s' not found in config", migrateTarget)
		}
		targetConn = &conn
	} else {
		return fmt.Errorf("no target database configured")
	}

	// Connect to target
	fmt.Printf("Connecting to target database...\n")
	targetPool, err := db.Connect(ctx, targetConn)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetPool.Close()

	// Scan migrations
	scanner := migration.NewScanner(cfg.Migrations)
	migrations, err := scanner.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan migrations: %w", err)
	}

	if len(migrations) == 0 {
		fmt.Println("No migrations found")
		return nil
	}

	// Load SQL for each migration
	for i := range migrations {
		if err := migrations[i].LoadSQL(); err != nil {
			return fmt.Errorf("failed to load migration %s: %w", migrations[i].Version, err)
		}
	}

	// Apply migrations
	applier := migration.NewApplier(targetPool, migrateDryRun)
	opts := migration.ApplyOptions{
		DryRun:          migrateDryRun,
		ContinueOnError: false,
	}

	if migrateDryRun {
		fmt.Println("\n=== DRY RUN MODE ===")
	}

	if err := applier.Apply(ctx, migrations, opts); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	if migrateDryRun {
		fmt.Println("\n=== DRY RUN COMPLETE ===")
	} else {
		fmt.Println("\nMigrations applied successfully")
	}

	return nil
}

// checkMigrationsInSync verifies that applying migrations to dev DB results in schema.sql
func checkMigrationsInSync(ctx context.Context, cfg *config.Config) error {
	// Connect to dev database
	if cfg.Dev == nil {
		return fmt.Errorf("no dev database configured")
	}

	devPool, err := db.Connect(ctx, cfg.Dev)
	if err != nil {
		return fmt.Errorf("failed to connect to dev database: %w", err)
	}
	defer devPool.Close()

	// Apply migrations to dev database
	if cfg.Migrations != "" {
		scanner := migration.NewScanner(cfg.Migrations)
		migrations, err := scanner.Scan()
		if err != nil {
			return fmt.Errorf("failed to scan migrations: %w", err)
		}

		if len(migrations) > 0 {
			applier := migration.NewApplier(devPool, false)
			opts := migration.ApplyOptions{
				DryRun:          false,
				ContinueOnError: false,
			}

			if err := applier.Apply(ctx, migrations, opts); err != nil {
				return fmt.Errorf("failed to apply migrations to dev: %w", err)
			}
		}
	}

	// Parse schema file
	schemaFile := cfg.Schema.GetSchemaPath()
	if schemaFile == "" {
		return fmt.Errorf("no schema file configured")
	}

	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	if err != nil {
		return fmt.Errorf("failed to parse schema file '%s': %w", schemaFile, err)
	}

	// Query actual schema from dev database
	catalog := db.NewCatalog(devPool)
	includeSchemas, excludeSchemas := cfg.Schema.GetSchemaFilters()
	actualObjects, err := catalog.ExtractAllObjects(ctx, includeSchemas, excludeSchemas)
	if err != nil {
		return fmt.Errorf("failed to query dev database schema: %w", err)
	}

	// Build object map with hashing
	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	if err != nil {
		return fmt.Errorf("failed to build object map: %w", err)
	}

	// Compute diff
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	if err != nil {
		return fmt.Errorf("failed to compute diff: %w", err)
	}

	// Check if in sync
	if !diff.IsEmpty() {
		return fmt.Errorf("migrations are out of sync with schema.sql:\n  %d to create, %d to drop, %d to alter",
			len(diff.ToCreate), len(diff.ToDrop), len(diff.ToAlter))
	}

	return nil
}
