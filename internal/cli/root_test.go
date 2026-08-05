package cli

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestExecuteCancelsCommandOnInterrupt(t *testing.T) {
	commandName := "test-signal-context"
	started := make(chan struct{})
	testCommand := &cobra.Command{
		Use:    commandName,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			close(started)
			<-cmd.Context().Done()
			return context.Cause(cmd.Context())
		},
	}
	rootCmd.AddCommand(testCommand)
	defer rootCmd.RemoveCommand(testCommand)
	rootCmd.SetArgs([]string{commandName})
	defer rootCmd.SetArgs(nil)
	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	defer rootCmd.SetOut(nil)
	defer rootCmd.SetErr(nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- Execute()
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("test command did not start")
	}

	process, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, process.Signal(os.Interrupt))

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("command did not stop after interrupt")
	}
}

func TestExecuteContextPropagatesCancellationToCommand(t *testing.T) {
	commandName := "test-canceled-context"
	testCommand := &cobra.Command{
		Use:    commandName,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return context.Cause(cmd.Context())
		},
	}
	rootCmd.AddCommand(testCommand)
	defer rootCmd.RemoveCommand(testCommand)
	rootCmd.SetArgs([]string{commandName})
	defer rootCmd.SetArgs(nil)
	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	defer rootCmd.SetOut(nil)
	defer rootCmd.SetErr(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ExecuteContext(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
