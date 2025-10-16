package cli

import (
	"fmt"

	"github.com/spf13/cobra"
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
`,
	RunE: runDiff,
}

func runDiff(cmd *cobra.Command, args []string) error {
	// TODO: Implement diff command
	return fmt.Errorf("diff command not yet implemented")
}
