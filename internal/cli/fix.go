package cli

import "github.com/spf13/cobra"

var fixCmd = &cobra.Command{
	Use:   "fix",
	Short: "Fix common migration issues",
}
