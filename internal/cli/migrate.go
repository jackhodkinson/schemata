package cli

import (
	"context"
	"fmt"

	"github.com/jackhodkinson/schemata/internal/app"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/spf13/cobra"
)

var (
	migrateTarget string
	migrateDryRun bool
	migrateStep   int
	migrateTo     string
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply pending migrations to target database",
	Long: `Apply pending migrations to the target database.

This command will:
1. Scan the migrations directory
2. Apply pending migrations to the target database

By default, all pending migrations are applied. Use --step or --to to limit:
  --step N     Apply at most N pending migrations
  --to VERSION Apply up to and including VERSION

Examples:
  schemata migrate
  schemata migrate --target staging
  schemata migrate --dry-run
  schemata migrate --step 1
  schemata migrate --to 20231015120530
`,
	RunE: runMigrate,
}

func init() {
	migrateCmd.Flags().StringVar(&migrateTarget, "target", "", "Target database (required if multiple targets configured)")
	migrateCmd.Flags().BoolVar(&migrateDryRun, "dry-run", false, "Show what would be applied without actually applying")
	migrateCmd.Flags().IntVar(&migrateStep, "step", 0, "Apply at most N pending migrations")
	migrateCmd.Flags().StringVar(&migrateTo, "to", "", "Apply pending migrations up to and including VERSION")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	if cmd.Flags().Changed("step") && migrateStep <= 0 {
		return fmt.Errorf("--step must be a positive integer")
	}
	if migrateStep != 0 && migrateTo != "" {
		return fmt.Errorf("--step and --to are mutually exclusive")
	}

	ctx := context.Background()
	service := app.NewService(allowCascade)

	// Load config
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Pre-flight check: ensure migrations are in sync with configured schema path
	fmt.Println("Running pre-flight check (diff --from migrations)...")
	if err := service.CheckMigrationsInSync(ctx, cfg); err != nil {
		return fmt.Errorf("pre-flight check failed: %w\n\nHint: Run 'schemata generate <name>' to create a migration for the differences", err)
	}
	fmt.Println("✓ Migrations are in sync with configured schema path")

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
	migrations, err := service.ScanMigrations(cfg.Migrations.GetDir(), cfg.Migrations.GetFormat())
	if err != nil {
		return err
	}

	if len(migrations) == 0 {
		fmt.Println("No migrations found")
		return nil
	}

	// Apply migrations
	if migrateDryRun {
		fmt.Println("\n=== DRY RUN MODE ===")
	}

	opts := migration.ApplyOptions{
		DryRun:    migrateDryRun,
		Step:      migrateStep,
		ToVersion: migrateTo,
	}
	if err := service.ApplyMigrations(ctx, targetPool, migrations, opts); err != nil {
		return err
	}

	if migrateDryRun {
		fmt.Println("\n=== DRY RUN COMPLETE ===")
	} else {
		fmt.Println("\nMigrations applied successfully")
	}

	return nil
}
