package cli

import (
	"fmt"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/spf13/cobra"
)

var (
	initDev        string
	initTarget     string
	initMigrations string
	initSchema     string
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize schemata configuration",
	Long: `Initialize a new schemata configuration file.

Examples:
  # Initialize with required flags
  schemata init --dev $DEV_URL --target $TARGET_URL --migrations ./migrations --schema schema.sql

  # Initialize with multiple targets
  schemata init --dev $DEV_URL --target prod=$PROD_URL --target staging=$STAGING_URL --migrations ./migrations --schema schema.sql
`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().StringVar(&initDev, "dev", "", "Dev database connection (required)")
	initCmd.Flags().StringVar(&initTarget, "target", "", "Target database connection (required)")
	initCmd.Flags().StringVar(&initMigrations, "migrations", "./migrations", "Migrations directory path")
	initCmd.Flags().StringVar(&initSchema, "schema", "schema.sql", "Schema path (file or directory)")

	initCmd.MarkFlagRequired("dev")
	initCmd.MarkFlagRequired("target")
}

func runInit(cmd *cobra.Command, args []string) error {
	// Build configuration
	cfg := &config.Config{
		Migrations: config.MigrationsConfig{FilePath: &initMigrations},
	}

	// Parse dev connection
	devURL := config.DetectEnvVar(initDev)
	cfg.Dev = &config.DBConnection{
		URL: &devURL,
	}

	// Parse target connection
	targetURL := config.DetectEnvVar(initTarget)
	cfg.Target = &config.DBConnection{
		URL: &targetURL,
	}

	// Set schema
	cfg.Schema = config.SchemaConfig{
		FilePath: &initSchema,
	}

	// Save configuration
	if err := cfg.Save(cfgFile); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	fmt.Printf("Created configuration file: %s\n", cfgFile)
	return nil
}
