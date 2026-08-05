package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/stretchr/testify/require"
)

func TestRequirePinnedProductionTarget(t *testing.T) {
	t.Parallel()

	require.ErrorContains(t, requirePinnedProductionTarget(nil, "migrate"), "requires the selected target to pin")
	require.ErrorContains(t, requirePinnedProductionTarget(&config.DBConnection{}, "apply"), "requires the selected target to pin")
	require.ErrorContains(t, requirePinnedProductionTarget(&config.DBConnection{
		Identity: &config.DatabaseIdentity{Database: "app"},
	}, "migrate"), "invalid selected target identity")
	require.NoError(t, requirePinnedProductionTarget(&config.DBConnection{
		Identity: &config.DatabaseIdentity{
			Database:         "app",
			SystemIdentifier: "7561860200789946402",
		},
	}, "migrate"))
}

func TestTargetMigrationCommandsRejectUnpinnedTargetBeforeOtherWork(t *testing.T) {
	oldConfigFile := cfgFile
	oldApplyTarget, oldApplyDev := applyTarget, applyDev
	oldApplyDryRun, oldApplyInitialize := applyDryRun, applyInitializeHistory
	oldApplyStep, oldApplyTo := applyStep, applyTo
	oldMigrateTarget, oldMigrateDryRun := migrateTarget, migrateDryRun
	oldMigrateInitialize := migrateInitializeHistory
	oldMigrateStep, oldMigrateTo := migrateStep, migrateTo
	t.Cleanup(func() {
		cfgFile = oldConfigFile
		applyTarget, applyDev = oldApplyTarget, oldApplyDev
		applyDryRun, applyInitializeHistory = oldApplyDryRun, oldApplyInitialize
		applyStep, applyTo = oldApplyStep, oldApplyTo
		migrateTarget, migrateDryRun = oldMigrateTarget, oldMigrateDryRun
		migrateInitializeHistory = oldMigrateInitialize
		migrateStep, migrateTo = oldMigrateStep, oldMigrateTo
	})

	cfgFile = filepath.Join(t.TempDir(), "schemata.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(`
dev: postgresql://postgres@localhost:1/dev
target: postgresql://postgres@localhost:1/production
schema: deliberately-missing-schema.sql
migrations: deliberately-missing-migrations
`), 0600))

	applyTarget, applyDev = "target", false
	applyDryRun, applyInitializeHistory = false, false
	applyStep, applyTo = 0, ""
	require.ErrorContains(t, runApply(applyCmd, nil), "requires the selected target to pin")

	migrateTarget, migrateDryRun = "", false
	migrateInitializeHistory = false
	migrateStep, migrateTo = 0, ""
	require.ErrorContains(t, runMigrate(migrateCmd, nil), "requires the selected target to pin")
}
