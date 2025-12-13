package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/spf13/cobra"
)

var generateCmd = &cobra.Command{
	Use:   "generate <migration-name>",
	Short: "Generate a migration from schema changes",
	Long: `Generate a migration file by comparing the schema file to the dev database.

This command will:
1. Apply all existing migrations to the dev database
2. Compare the dev database to your schema.sql file
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

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Ensure migrations directory exists
	if cfg.Migrations == "" {
		return fmt.Errorf("no migrations directory configured")
	}

	// Ensure schema file exists
	schemaFile := cfg.Schema.GetSchemaPath()
	if schemaFile == "" {
		return fmt.Errorf("no schema file configured")
	}

	// Connect to dev database
	pool, err := db.Connect(ctx, cfg.Dev)
	if err != nil {
		return fmt.Errorf("failed to connect to dev database: %w", err)
	}
	defer pool.Close()

	// Step 1: Apply all existing migrations to dev database
	fmt.Println("Applying existing migrations to dev database...")
	scanner := migration.NewScanner(cfg.Migrations)
	migrations, err := scanner.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan migrations: %w", err)
	}

	if len(migrations) > 0 {
		applier := migration.NewApplier(pool, false)
		opts := migration.ApplyOptions{
			DryRun:          false,
			ContinueOnError: false,
		}

		if err := applier.Apply(ctx, migrations, opts); err != nil {
			return fmt.Errorf("failed to apply migrations to dev: %w", err)
		}
	} else {
		if verbose {
			fmt.Println("No existing migrations to apply")
		}
	}

	// Step 2: Parse schema.sql to get desired state
	fmt.Printf("Parsing schema file: %s\n", schemaFile)
	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	if err != nil {
		return fmt.Errorf("failed to parse schema file '%s': %w", schemaFile, err)
	}

	if verbose {
		fmt.Printf("Parsed %d objects from schema file\n", len(desiredSchema))
	}

	// Step 3: Query actual schema from dev database
	fmt.Println("Querying dev database schema...")
	catalog := db.NewCatalog(pool)
	includeSchemas, excludeSchemas := cfg.Schema.GetSchemaFilters()
	actualObjects, err := catalog.ExtractAllObjects(ctx, includeSchemas, excludeSchemas)
	if err != nil {
		return fmt.Errorf("failed to query database schema: %w", err)
	}

	// Build object map with hashing
	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	if err != nil {
		return fmt.Errorf("failed to build object map: %w", err)
	}

	if verbose {
		fmt.Printf("Queried %d objects from dev database\n", len(actualSchema))
	}

	// Step 4: Compute diff between desired and actual
	fmt.Println("Computing schema differences...")
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	if err != nil {
		return fmt.Errorf("failed to compute diff: %w", err)
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
	ddlGen := planner.NewDDLGenerator(planner.WithAllowCascade(allowCascade))
	ddl, err := ddlGen.GenerateDDL(diff, desiredSchema)
	if err != nil {
		return fmt.Errorf("failed to generate DDL: %w", err)
	}

	if ddl == "" {
		fmt.Println("✓ No DDL generated. Schema is already in sync.")
		return nil
	}

	// Step 6: Create migration file
	gen := migration.NewGenerator(cfg.Migrations)
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
