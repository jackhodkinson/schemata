package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/spf13/cobra"
)

var (
	diffFrom   string
	diffTarget string
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show schema differences",
	Long: `Show differences between target database and schema file.

Examples:
  # Compare target DB to schema.sql
  schemata diff

  # Compare dev DB (with migrations applied) to schema.sql
  schemata diff --from migrations

  # Compare specific target to schema.sql
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
		if err := applyMigrationsForDiff(ctx, cfg, pool); err != nil {
			return fmt.Errorf("failed to apply migrations to dev: %w", err)
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

	if verbose {
		fmt.Printf("Parsed %d objects from schema file\n", len(desiredSchema))
	}

	// Query actual schema from database
	catalog := db.NewCatalog(pool)
	includeSchemas, excludeSchemas := cfg.Schema.GetSchemaFilters()
	actualObjects, err := catalog.ExtractAllObjects(ctx, includeSchemas, excludeSchemas)
	if err != nil {
		return fmt.Errorf("failed to query database schema: %w", err)
	}

	// Build object map with hashing (reuse parser's buildObjectMap logic)
	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	if err != nil {
		return fmt.Errorf("failed to build object map: %w", err)
	}

	if verbose {
		fmt.Printf("Queried %d objects from %s database\n", len(actualSchema), dbName)
	}

	// Compute diff
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	if err != nil {
		return fmt.Errorf("failed to compute diff: %w", err)
	}

	// Display results
	if diff.IsEmpty() {
		fmt.Println("✓ Schemas are in sync")
		return nil
	}

	fmt.Printf("Schema differences found between %s and %s:\n\n", schemaFile, dbName)

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
	ddlGen := planner.NewDDLGenerator()
	ddl, err := ddlGen.GenerateDDL(diff, desiredSchema)
	if err != nil {
		return fmt.Errorf("failed to generate DDL: %w", err)
	}
	fmt.Println(ddl)
	fmt.Println("---")

	// Exit with code 1 to indicate differences found
	os.Exit(1)
	return nil
}

func applyMigrationsForDiff(ctx context.Context, cfg *config.Config, pool *db.Pool) error {
	if cfg.Migrations == "" {
		return fmt.Errorf("no migrations directory configured")
	}

	scanner := migration.NewScanner(cfg.Migrations)
	migrations, err := scanner.Scan()
	if err != nil {
		return fmt.Errorf("failed to scan migrations: %w", err)
	}

	if len(migrations) == 0 {
		if verbose {
			fmt.Println("No migrations to apply")
		}
		return nil
	}

	applier := migration.NewApplier(pool, false)
	opts := migration.ApplyOptions{
		DryRun:          false,
		ContinueOnError: false,
	}

	if verbose {
		fmt.Printf("Applying %d migrations to dev database...\n", len(migrations))
	}

	return applier.Apply(ctx, migrations, opts)
}
