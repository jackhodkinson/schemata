package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
)

var (
	applyTarget string
	applyDev    bool
	applyDryRun bool
)

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply pending migrations (low-level command)",
	Long: `Apply pending migrations to a specific database.

This is a low-level command. Use 'migrate' for normal workflows.

Examples:
  # Apply to target
  schemata apply --target prod

  # Apply to dev database
  schemata apply --dev

  # Dry run
  schemata apply --target prod --dry-run
`,
	RunE: runApply,
}

func init() {
	applyCmd.Flags().StringVar(&applyTarget, "target", "", "Target name to apply migrations to")
	applyCmd.Flags().BoolVar(&applyDev, "dev", false, "Apply migrations to dev database")
	applyCmd.Flags().BoolVar(&applyDryRun, "dry-run", false, "Show what would be applied without executing")
}

func runApply(cmd *cobra.Command, args []string) error {
	// Must specify either --target or --dev
	if applyTarget == "" && !applyDev {
		return fmt.Errorf("must specify either --target or --dev")
	}
	if applyTarget != "" && applyDev {
		return fmt.Errorf("cannot specify both --target and --dev")
	}

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Get target connection
	var targetConn *config.DBConnection
	if applyDev {
		if cfg.Dev == nil {
			return fmt.Errorf("dev database not configured")
		}
		targetConn = cfg.Dev
	} else {
		targetConn, err = cfg.GetTargetByName(applyTarget)
		if err != nil {
			return err
		}
	}

	// Scan migrations
	scanner := migration.NewScanner(cfg.Migrations)
	migrations, err := scanner.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan migrations: %w", err)
	}

	fmt.Printf("Found %d migrations\n", len(migrations))

	// Connect to database
	ctx := context.Background()
	pool, err := db.Connect(ctx, targetConn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Apply migrations
	applier := migration.NewApplier(pool, applyDryRun)
	opts := migration.ApplyOptions{
		DryRun:          applyDryRun,
		ContinueOnError: false,
	}

	if err := applier.Apply(ctx, migrations, opts); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	if applyDryRun {
		fmt.Println("\nDry run completed (no changes were made)")
	} else {
		fmt.Println("\nMigrations applied successfully")
	}

	return nil
}
