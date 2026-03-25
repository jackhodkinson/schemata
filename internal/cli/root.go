package cli

import (
	"github.com/jackhodkinson/schemata/internal/version"
	"github.com/spf13/cobra"
)

var (
	cfgFile string
	verbose bool
	allowCascade bool
)

var rootCmd = &cobra.Command{
	Use:   "schemata",
	Short: "A declarative postgres migration tool",
	Long: `Schemata is a declarative Postgres schema migration tool.
It allows you to define your schema in raw SQL and automatically
generate migrations from changes to your schema.`,
	Version: version.String(),
}

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "schemata.yaml", "config file path")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCmd.PersistentFlags().BoolVar(&allowCascade, "allow-cascade", false, "Allow CASCADE drops when generating DDL (dangerous)")

	// Add subcommands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(dumpCmd)
	rootCmd.AddCommand(generateCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(applyCmd)
	rootCmd.AddCommand(syncCmd)
	rootCmd.AddCommand(fixCmd)
}
