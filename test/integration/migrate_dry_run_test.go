//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackhodkinson/schemata/internal/app"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateDryRunDevPreflightIsSideEffectFree(t *testing.T) {
	ctx := context.Background()
	connection := &config.DBConnection{URL: strPtr(targetDBURL)}
	pool, err := db.Connect(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS schemata CASCADE;
		DROP TABLE IF EXISTS public.schemata_migrate_dry_run_preflight;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, cleanupErr := pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS schemata CASCADE;
			DROP TABLE IF EXISTS public.schemata_migrate_dry_run_preflight;
		`)
		require.NoError(t, cleanupErr)
	})

	tempDir := t.TempDir()
	migrationsDir := filepath.Join(tempDir, "migrations")
	require.NoError(t, os.Mkdir(migrationsDir, 0o755))
	sql := "CREATE TABLE public.schemata_migrate_dry_run_preflight (id integer PRIMARY KEY);\n"
	require.NoError(t, os.WriteFile(
		filepath.Join(migrationsDir, "20260805010500-dry-run-preflight.sql"),
		[]byte(sql),
		0o600,
	))
	schemaPath := filepath.Join(tempDir, "schema.sql")
	require.NoError(t, os.WriteFile(schemaPath, []byte(sql), 0o600))

	cfg := &config.Config{
		Dev: connection,
		Migrations: config.MigrationsConfig{
			Dir: migrationsDir,
		},
		Schema: config.SchemaConfig{
			File: schemaPath,
		},
	}
	err = app.NewService(false).CheckMigrationsInSync(ctx, cfg, migration.ApplyOptions{
		DryRun:            true,
		InitializeHistory: true,
	})
	require.Error(t, err, "a side-effect-free preflight cannot make an empty dev database match pending migrations")

	var historyExists, tableExists bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('schemata.version') IS NOT NULL").Scan(&historyExists))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('public.schemata_migrate_dry_run_preflight') IS NOT NULL").Scan(&tableExists))
	assert.False(t, historyExists)
	assert.False(t, tableExists)
}
