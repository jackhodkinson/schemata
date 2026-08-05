package cli

import (
	"errors"
	"testing"

	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationApplyingCommandsRequireExplicitHistoryInitializationFlag(t *testing.T) {
	commands := []*cobra.Command{
		applyCmd,
		migrateCmd,
		generateCmd,
		diffCmd,
	}

	for _, command := range commands {
		t.Run(command.Name(), func(t *testing.T) {
			flag := command.Flags().Lookup("initialize-history")
			require.NotNil(t, flag)
			assert.Equal(t, "false", flag.DefValue)
			assert.Contains(t, flag.Usage, "first-time")
		})
	}

	syncFlag := syncCmd.Flags().Lookup("initialize-history")
	require.NotNil(t, syncFlag)
	assert.Equal(t, "false", syncFlag.DefValue)
	assert.Contains(t, syncFlag.Usage, "removal and recreation")

	assert.Nil(t, recoverCmd.Flags().Lookup("initialize-history"), "recovery must never initialize missing history")
}

func TestSyncRefusesBeforeWorkWithoutHistoryResetAuthorization(t *testing.T) {
	original := syncInitializeHistory
	syncInitializeHistory = false
	t.Cleanup(func() { syncInitializeHistory = original })

	err := runSync(&cobra.Command{}, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, migration.ErrHistoryResetAuthorizationRequired))
}

func TestMigrateDryRunPropagatesToDevPreflight(t *testing.T) {
	opts := migratePreflightApplyOptions(true, true)
	assert.True(t, opts.DryRun)
	assert.True(t, opts.InitializeHistory)
}
