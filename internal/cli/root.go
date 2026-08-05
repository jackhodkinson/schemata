package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackhodkinson/schemata/internal/version"
	"github.com/spf13/cobra"
)

var (
	cfgFile      string
	verbose      bool
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

// Execute runs the root command and cancels in-flight work on SIGINT or
// SIGTERM. Once cancellation starts, signal delivery is restored so a second
// signal can terminate a command whose dependency fails to stop promptly.
func Execute() error {
	ctx, stop := newSignalContext(context.Background())
	defer stop()

	go func() {
		<-ctx.Done()
		stop()
	}()

	return ExecuteContext(ctx)
}

// ExecuteContext runs the root command with a caller-provided cancellation
// context. Command implementations must use cmd.Context for blocking work.
func ExecuteContext(ctx context.Context) error {
	return rootCmd.ExecuteContext(ctx)
}

func newSignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
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
