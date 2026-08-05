//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/jackhodkinson/schemata/internal/app"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/sqlrender"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDropAllObjectsRollsBackEntireResetWhenLaterSchemaFails(t *testing.T) {
	ctx := context.Background()
	pool := syncResetTestPool(t, ctx)
	cleanupSyncResetFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanupSyncResetFixtures(t, ctx, pool) })

	_, err := pool.Exec(ctx, `
		CREATE SCHEMA aaa_schemata_atomic_first;
		COMMENT ON SCHEMA aaa_schemata_atomic_first IS 'must survive rollback';
		CREATE SCHEMA aab_schemata_atomic_blocked;
		CREATE TABLE aab_schemata_atomic_blocked.blocker (id integer PRIMARY KEY);
	`)
	require.NoError(t, err)

	err = app.NewService(false).DropAllObjects(ctx, pool)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aab_schemata_atomic_blocked")

	var firstExists, blockedTableExists bool
	var firstComment *string
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regnamespace('aaa_schemata_atomic_first') IS NOT NULL").Scan(&firstExists))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT pg_catalog.obj_description(
			to_regnamespace('aaa_schemata_atomic_first'),
			'pg_namespace'
		)
	`).Scan(&firstComment))
	require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass('aab_schemata_atomic_blocked.blocker') IS NOT NULL").Scan(&blockedTableExists))
	assert.True(t, firstExists, "an earlier successful DROP/CREATE must roll back")
	require.NotNil(t, firstComment)
	assert.Equal(t, "must survive rollback", *firstComment)
	assert.True(t, blockedTableExists, "the failing schema and its contents must remain")
}

func TestDropAllObjectsQuotesCatalogSchemaIdentifiers(t *testing.T) {
	ctx := context.Background()
	pool := syncResetTestPool(t, ctx)
	cleanupSyncResetFixtures(t, ctx, pool)
	t.Cleanup(func() { cleanupSyncResetFixtures(t, ctx, pool) })

	oddSchema := `odd"; DROP SCHEMA public; --`
	_, err := pool.Exec(ctx, "CREATE SCHEMA "+sqlrender.Identifier(oddSchema))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "CREATE TABLE "+sqlrender.Qualified(oddSchema, "fixture")+" (id integer)")
	require.NoError(t, err)

	require.NoError(t, app.NewService(true).DropAllObjects(ctx, pool))

	var schemaExists, tableExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1
		)
	`, oddSchema).Scan(&schemaExists))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_class AS cls
			JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace
			WHERE nsp.nspname = $1 AND cls.relname = 'fixture'
		)
	`, oddSchema).Scan(&tableExists))
	assert.True(t, schemaExists, "the oddly named schema must be safely recreated")
	assert.False(t, tableExists, "objects inside the reset schema must be removed")
}

func syncResetTestPool(t *testing.T, ctx context.Context) *db.Pool {
	t.Helper()
	connection := &config.DBConnection{URL: strPtr(targetDBURL)}
	pool, err := db.Connect(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func cleanupSyncResetFixtures(t *testing.T, ctx context.Context, pool *db.Pool) {
	t.Helper()
	oddSchema := `odd"; DROP SCHEMA public; --`
	_, err := pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS aaa_schemata_atomic_first CASCADE;
		DROP SCHEMA IF EXISTS aab_schemata_atomic_blocked CASCADE;
	`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+sqlrender.Identifier(oddSchema)+" CASCADE")
	require.NoError(t, err)
}
