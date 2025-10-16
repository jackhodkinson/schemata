package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/planner"
)

var (
	dumpSchema string
	dumpTarget string
)

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump database schema to SQL file",
	Long: `Dump the target database schema to a SQL file.

Examples:
  # Dump using config file
  schemata dump

  # Dump to specific file
  schemata dump --schema my-schema.sql

  # Dump specific target
  schemata dump --target staging
`,
	RunE: runDump,
}

func init() {
	dumpCmd.Flags().StringVar(&dumpSchema, "schema", "", "Schema file path (overrides config)")
	dumpCmd.Flags().StringVar(&dumpTarget, "target", "", "Target name (for multi-target configs)")
}

func runDump(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Get target connection
	var targetConn *config.DBConnection
	if dumpTarget != "" {
		targetConn, err = cfg.GetTargetByName(dumpTarget)
		if err != nil {
			return err
		}
	} else {
		targetConn, err = cfg.GetSingleTarget()
		if err != nil {
			return err
		}
	}

	// Determine schema file path
	schemaPath := dumpSchema
	if schemaPath == "" {
		schemaPath = cfg.Schema.GetSchemaPath()
	}

	fmt.Printf("Dumping database schema to %s...\n", schemaPath)

	// Connect to target database
	ctx := context.Background()
	pool, err := db.Connect(ctx, targetConn)
	if err != nil {
		return fmt.Errorf("failed to connect to target database: %w", err)
	}
	defer pool.Close()

	// Extract schema objects
	catalog := db.NewCatalog(pool)
	includeSchemas, excludeSchemas := cfg.Schema.GetSchemaFilters()
	objects, err := catalog.ExtractAllObjects(ctx, includeSchemas, excludeSchemas)
	if err != nil {
		return fmt.Errorf("failed to extract schema objects: %w", err)
	}

	// Generate DDL
	ddlGen := planner.NewDDLGenerator()
	var ddlStatements []string
	for _, obj := range objects {
		stmt, err := ddlGen.GenerateCreateStatement(obj)
		if err != nil {
			fmt.Printf("Warning: failed to generate DDL for object: %v\n", err)
			continue
		}
		ddlStatements = append(ddlStatements, stmt)
	}

	// Write to file
	ddl := ""
	for _, stmt := range ddlStatements {
		ddl += stmt + "\n\n"
	}

	if err := os.WriteFile(schemaPath, []byte(ddl), 0644); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	fmt.Printf("Successfully dumped %d database objects\n", len(objects))
	return nil
}
