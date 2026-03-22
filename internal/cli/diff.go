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
	diffFrom   string
	diffTarget string
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show schema differences",
	Long: `Show differences between target database and schema path.

Examples:
  # Compare target DB to configured schema path
  schemata diff

  # Compare dev DB (with migrations applied) to configured schema path
  schemata diff --from migrations

  # Compare specific target to configured schema path
  schemata diff --target prod
`,
	RunE: runDiff,
}

func init() {
	diffCmd.Flags().StringVar(&diffFrom, "from", "", "Compare from 'migrations' (apply to dev) instead of target")
	diffCmd.Flags().StringVar(&diffTarget, "target", "", "Target database to use (for multi-target configs)")
}

func runDiff(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	service := app.NewService(allowCascade)

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Determine which database to compare against
	var dbConn *config.DBConnection
	var dbName string

	if diffFrom == "migrations" {
		// Use dev database with migrations applied
		dbConn = cfg.Dev
		dbName = "dev"
	} else {
		// Use target database
		if cfg.Target != nil {
			dbConn = cfg.Target
			dbName = "target"
		} else if len(cfg.Targets) > 0 {
			// Multi-target config
			if diffTarget == "" {
				return fmt.Errorf("multiple targets configured, please specify --target")
			}
			targetCfg, ok := cfg.Targets[diffTarget]
			if !ok {
				return fmt.Errorf("target '%s' not found in config", diffTarget)
			}
			targetCopy := targetCfg
			dbConn = &targetCopy
			dbName = diffTarget
		} else {
			return fmt.Errorf("no target database configured")
		}
	}

	// Connect to database
	pool, err := db.Connect(ctx, dbConn)
	if err != nil {
		return fmt.Errorf("failed to connect to %s database: %w", dbName, err)
	}
	defer pool.Close()

	// If comparing from migrations, apply them first
	if diffFrom == "migrations" {
		migrations, err := service.ScanMigrations(cfg.Migrations)
		if err != nil {
			return err
		}
		if err := service.ApplyMigrations(ctx, pool, migrations, false); err != nil {
			return fmt.Errorf("failed to apply migrations to dev: %w", err)
		}
	}

	// Parse schema path
	schemaPath := cfg.Schema.GetSchemaPath()
	if schemaPath == "" {
		return fmt.Errorf("no schema path configured")
	}

	desiredSchema, err := service.ParseSchemaPath(schemaPath)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Printf("Parsed %d objects from schema path\n", len(desiredSchema))
	}

	// Query actual schema from database
	actualSchema, err := service.ExtractSchemaFromDB(ctx, pool, cfg)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Printf("Queried %d objects from %s database\n", len(actualSchema), dbName)
	}

	// Compute diff
	diff, err := service.ComputeDiff(desiredSchema, actualSchema)
	if err != nil {
		return err
	}

	// Display results
	if diff.IsEmpty() {
		fmt.Println("✓ Schemas are in sync")
		return nil
	}

	fmt.Printf("Schema differences found between %s and %s:\n\n", schemaPath, dbName)

	// Display creates
	if len(diff.ToCreate) > 0 {
		fmt.Printf("Objects to CREATE (%d):\n", len(diff.ToCreate))
		for _, key := range diff.ToCreate {
			fmt.Printf("  + %s: %s.%s\n", key.Kind, key.Schema, key.Name)
		}
		fmt.Println()
	}

	// Display drops
	if len(diff.ToDrop) > 0 {
		fmt.Printf("Objects to DROP (%d):\n", len(diff.ToDrop))
		for _, key := range diff.ToDrop {
			fmt.Printf("  - %s: %s.%s\n", key.Kind, key.Schema, key.Name)
		}
		fmt.Println()
	}

	// Display alters
	if len(diff.ToAlter) > 0 {
		fmt.Printf("Objects to ALTER (%d):\n", len(diff.ToAlter))
		for _, alter := range diff.ToAlter {
			fmt.Printf("  ~ %s: %s.%s\n", alter.Key.Kind, alter.Key.Schema, alter.Key.Name)
			for _, change := range alter.Changes {
				fmt.Printf("      %s\n", change)
			}
		}
		fmt.Println()
	}

	// Generate DDL preview
	fmt.Println("DDL Preview:")
	fmt.Println("---")
	ddl, err := service.GenerateDDL(diff, desiredSchema)
	if err != nil {
		return err
	}
	fmt.Println(ddl)
	fmt.Println("---")

	return ErrDriftDetected
}
