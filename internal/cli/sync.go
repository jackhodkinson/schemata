package cli

import (
	"context"
	"fmt"

	"github.com/jackhodkinson/schemata/internal/app"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/spf13/cobra"
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
	service := app.NewService(allowCascade)

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
	if err := service.DropAllObjects(ctx, pool); err != nil {
		return fmt.Errorf("failed to drop objects: %w", err)
	}

	// Scan migrations
	fmt.Printf("Scanning migrations directory: %s\n", cfg.Migrations)
	migrations, err := service.ScanMigrations(cfg.Migrations)
	if err != nil {
		return err
	}

	if len(migrations) == 0 {
		fmt.Println("No migrations found")
		return nil
	}

	fmt.Printf("Found %d migration(s)\n", len(migrations))

	// Apply all migrations
	fmt.Println("Applying migrations...")
	if err := service.ApplyMigrations(ctx, pool, migrations, false); err != nil {
		return err
	}

	fmt.Println("\n✓ Dev database synced successfully")
	return nil
}
