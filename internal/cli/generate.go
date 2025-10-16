package cli

import (
	"fmt"

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
	// TODO: Implement generate command
	// This is a complex command that requires:
	// 1. Applying migrations to dev DB
	// 2. Parsing schema.sql
	// 3. Diffing dev DB vs parsed schema
	// 4. Generating DDL
	// 5. Creating migration file

	return fmt.Errorf("generate command not yet implemented")
}
