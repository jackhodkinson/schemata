package cli

import (
	"context"
	"fmt"

	"github.com/jackhodkinson/schemata/internal/app"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
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
	migrations, err := service.ScanMigrations(cfg.Migrations)
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

	if err := service.ApplyMigrations(ctx, targetPool, migrations, migrateDryRun); err != nil {
		return err
	}

	if migrateDryRun {
		fmt.Println("\n=== DRY RUN COMPLETE ===")
	} else {
		fmt.Println("\nMigrations applied successfully")
	}

	return nil
}
