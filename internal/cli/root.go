package cli

import (
	"github.com/spf13/cobra"
)

var (
	cfgFile string
	verbose bool
)

var rootCmd = &cobra.Command{
	Use:   "schemata",
	Short: "A declarative postgres migration tool",
	Long: `Schemata is a declarative Postgres schema migration tool.
It allows you to define your schema in raw SQL and automatically
generate migrations from changes to your schema.`,
}

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "schemata.yaml", "config file path")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")

	// Add subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(dumpCmd)
	rootCmd.AddCommand(generateCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(applyCmd)
}
