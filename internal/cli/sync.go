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

var syncInitializeHistory bool

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync dev database to match migrations directory",
	Long: `Reset the dev database and replay all migrations.

This command will:
1. Drop all objects from the dev database
2. Apply all migrations from the migrations directory

Because reset deliberately removes and recreates migration history, it always
requires --initialize-history. Validation, reset, ledger recreation, and replay
remain fenced by the migration runner lock.

This is useful when you've deleted/modified migrations and need to
reset your dev database to match.

Examples:
  # Sync dev database
  schemata sync --initialize-history
`,
	RunE: runSync,
}

func init() {
	syncCmd.Flags().BoolVar(&syncInitializeHistory, "initialize-history", false, "Authorize removal and recreation of migration history on the dev database")
}

func runSync(cmd *cobra.Command, args []string) error {
	if !syncInitializeHistory {
		return migration.ErrHistoryResetAuthorizationRequired
	}

	service := app.NewService(allowCascade)

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if cfg.Dev == nil {
		return fmt.Errorf("dev database not configured")
	}

	ctx := cmd.Context()

	// Connect to dev database
	fmt.Println("Connecting to dev database...")
	pool, err := db.Connect(ctx, cfg.Dev, db.WithDatabaseConfig(cfg.Database))
	if err != nil {
		return fmt.Errorf("failed to connect to dev database: %w", err)
	}
	defer pool.Close()

	// Scan migrations
	fmt.Printf("Scanning migrations directory: %s\n", cfg.Migrations.GetDir())
	migrations, err := service.ScanMigrations(cfg.Migrations.GetDir(), cfg.Migrations.GetFormat())
	if err != nil {
		return err
	}

	fmt.Printf("Found %d migration(s)\n", len(migrations))

	// Keep validation, the destructive reset, ledger recreation, and replay
	// behind one migration-runner lock. No other Schemata runner can observe the
	// deliberately missing ledger between reset and initialization.
	applier := migration.NewApplier(pool, false)
	if err := applier.ResetAndApply(ctx, migrations, migration.ApplyOptions{
		InitializeHistory: syncInitializeHistory,
	}, func(resetCtx context.Context) error {
		fmt.Println("Dropping all objects from dev database...")
		if err := service.DropAllObjects(resetCtx, pool); err != nil {
			return fmt.Errorf("failed to drop objects: %w", err)
		}
		fmt.Println("Applying migrations...")
		return nil
	}); err != nil {
		return fmt.Errorf("failed to reset and apply migrations: %w", err)
	}

	fmt.Println("\n✓ Dev database synced successfully")
	return nil
}
