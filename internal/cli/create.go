package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/migration"
)

var createCmd = &cobra.Command{
	Use:   "create <migration-name>",
	Short: "Create an empty migration file",
	Long: `Create an empty migration file for manual SQL.

Examples:
  schemata create 'add custom indexes'
  schemata create 'seed data'
`,
	Args: cobra.ExactArgs(1),
	RunE: runCreate,
}

func runCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create migration generator
	generator := migration.NewGenerator(cfg.Migrations.GetDir())

	// Generate empty migration
	mig, err := generator.CreateEmpty(name)
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	fmt.Printf("Created migration: %s\n", mig.FilePath)
	fmt.Println("Edit the file and run 'schemata migrate' to apply it.")

	return nil
}
