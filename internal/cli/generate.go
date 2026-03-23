package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jackhodkinson/schemata/internal/app"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/spf13/cobra"
)

var generateCmd = &cobra.Command{
	Use:   "generate <migration-name>",
	Short: "Generate a migration from schema changes",
	Long: `Generate a migration file by comparing the schema path to the dev database.

This command will:
1. Apply all existing migrations to the dev database
2. Compare the dev database to your configured schema path
3. Generate DDL for the differences
4. Create a new migration file with the generated DDL

Examples:
  schemata generate 'add users table'
  schemata generate 'add email column to users'
`,
	Args: cobra.ExactArgs(1),
	RunE: runGenerate,
}

func runGenerate(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	migrationName := args[0]
	service := app.NewService(allowCascade)

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Ensure migrations directory exists
	if cfg.Migrations.GetDir() == "" {
		return fmt.Errorf("no migrations directory configured")
	}

	// Ensure schema path exists
	schemaPath := cfg.Schema.GetSchemaPath()
	if schemaPath == "" {
		return fmt.Errorf("no schema path configured")
	}

	// Connect to dev database
	pool, err := db.Connect(ctx, cfg.Dev)
	if err != nil {
		return fmt.Errorf("failed to connect to dev database: %w", err)
	}
	defer pool.Close()

	// Step 1: Apply all existing migrations to dev database
	fmt.Println("Applying existing migrations to dev database...")
	migrations, err := service.ScanMigrations(cfg.Migrations.GetDir(), cfg.Migrations.GetFormat())
	if err != nil {
		return err
	}

	if len(migrations) > 0 {
		if err := service.ApplyMigrations(ctx, pool, migrations, migration.ApplyOptions{}); err != nil {
			return fmt.Errorf("failed to apply migrations to dev: %w", err)
		}
	} else {
		if verbose {
			fmt.Println("No existing migrations to apply")
		}
	}

	// Step 2: Parse schema path to get desired state
	fmt.Printf("Parsing schema path: %s\n", schemaPath)
	desiredSchema, err := service.ParseSchemaPath(schemaPath)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Printf("Parsed %d objects from schema path\n", len(desiredSchema))
	}

	// Step 3: Query actual schema from dev database
	fmt.Println("Querying dev database schema...")
	actualSchema, err := service.ExtractSchemaFromDB(ctx, pool, cfg)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Printf("Queried %d objects from dev database\n", len(actualSchema))
	}

	// Step 4: Compute diff between desired and actual
	fmt.Println("Computing schema differences...")
	diff, err := service.ComputeDiff(desiredSchema, actualSchema)
	if err != nil {
		return err
	}

	// Check if there are any differences
	if diff.IsEmpty() {
		fmt.Println("✓ No schema changes detected. Schema is already in sync.")
		return nil
	}

	// Display summary of changes
	fmt.Printf("\nSchema changes detected:\n")
	if len(diff.ToCreate) > 0 {
		fmt.Printf("  - %d objects to CREATE\n", len(diff.ToCreate))
	}
	if len(diff.ToDrop) > 0 {
		fmt.Printf("  - %d objects to DROP\n", len(diff.ToDrop))
	}
	if len(diff.ToAlter) > 0 {
		fmt.Printf("  - %d objects to ALTER\n", len(diff.ToAlter))
	}
	fmt.Println()

	// Step 5: Generate DDL for the differences
	fmt.Println("Generating DDL...")
	ddl, err := service.GenerateDDL(diff, desiredSchema)
	if err != nil {
		return err
	}

	if ddl == "" {
		fmt.Println("✓ No DDL generated. Schema is already in sync.")
		return nil
	}

	// Step 6: Create migration file
	gen := migration.NewGenerator(cfg.Migrations.GetDir())
	mig, err := gen.Generate(migrationName, ddl)
	if err != nil {
		return fmt.Errorf("failed to create migration file: %w", err)
	}

	// Display success message
	relPath, _ := filepath.Rel(".", mig.FilePath)
	fmt.Printf("\n✓ Migration created: %s\n", relPath)
	fmt.Println("\nMigration content:")
	fmt.Println("---")
	fmt.Println(ddl)
	fmt.Println("---")
	fmt.Printf("\nReview the migration and run 'schemata migrate' to apply it.\n")

	return nil
}
