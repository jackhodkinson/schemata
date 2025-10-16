package cli

import (
	"context"
	"fmt"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
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
		fmt.Println("\n=== DRY RUN MODE ===\n")
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
