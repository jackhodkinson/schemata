package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/spf13/cobra"
)

var (
	dumpSchema string
	dumpTarget string
)

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump database schema to SQL file(s)",
	Long: `Dump the target database schema to SQL.

If the schema path ends with ".sql", output is written to that single file.

If the schema path does not end with ".sql", it is treated as a directory: the
directory is created if needed, and one "<schema>.sql" file is written per
PostgreSQL schema (for example public.sql, sales.sql).

Examples:
  # Dump using config file
  schemata dump

  # Dump to a single file
  schemata dump --schema my-schema.sql

  # Dump one file per schema into a directory (created if missing)
  schemata dump --schema ./schema

  # Dump specific target
  schemata dump --target staging
`,
	RunE: runDump,
}

func init() {
	dumpCmd.Flags().StringVar(&dumpSchema, "schema", "", "Schema path: .sql file or directory for per-schema .sql files (overrides config)")
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

	// Determine schema path
	schemaPath := dumpSchema
	if schemaPath == "" {
		schemaPath = cfg.Schema.GetSchemaPath()
	}

	fileMode := isDumpSchemaFilePath(schemaPath)
	if err := validateDumpSchemaPath(schemaPath, fileMode); err != nil {
		return err
	}

	if fileMode {
		fmt.Printf("Dumping database schema to file %s...\n", schemaPath)
	} else {
		abs, err := filepath.Abs(schemaPath)
		if err != nil {
			abs = schemaPath
		}
		fmt.Printf("Dumping database schema to directory %s (one .sql file per schema)...\n", abs)
	}

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

	ddlGen := planner.NewDDLGenerator()

	if fileMode {
		if _, err := writeDumpSingleFile(schemaPath, objects, ddlGen); err != nil {
			return err
		}
		fmt.Printf("Successfully dumped %d database objects\n", len(objects))
		return nil
	}

	nFiles, err := writeDumpPerSchemaDir(schemaPath, objects, ddlGen)
	if err != nil {
		return err
	}
	fmt.Printf("Successfully dumped %d database objects into %d schema file(s)\n", len(objects), nFiles)
	return nil
}
