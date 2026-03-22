//go:build integration

package cli

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func integrationDBURL() string {
	if u := os.Getenv("SCHEMATA_INTEGRATION_DB"); u != "" {
		return u
	}
	return "postgresql://postgres:postgres@localhost:25433/schemata_dev?sslmode=disable"
}

func connectIntegrationPool(t *testing.T) *db.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := db.Connect(ctx, &config.DBConnection{URL: strPtr(integrationDBURL())})
	if err != nil {
		t.Skipf("integration database not available: %v", err)
	}
	return pool
}

func strPtr(s string) *string { return &s }

// execDumpFile runs a full per-schema SQL file in one round-trip (same as migration applier).
func execDumpFile(ctx context.Context, pool *db.Pool, sql string) error {
	_, err := pool.Exec(ctx, sql)
	return err
}

func sortedSQLFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.EqualFold(filepath.Ext(e.Name()), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = filepath.Join(dir, n)
	}
	return out, nil
}

// The following tests use hand-written per-schema files (same shape as schemata per-schema dump:
// CREATE SCHEMA + objects for that schema). They document that applying files in lexicographic
// filename order is not dependency order: the first file often fails when the referenced schema
// is only created in a later file.

func TestIntegration_LexicographicApplyOrder_FKFirstFileFailsWhenChildSchemaSortsBeforeParent(t *testing.T) {
	ctx := context.Background()
	pool := connectIntegrationPool(t)
	defer pool.Close()

	const a, z = "lo_fk_a", "lo_fk_z"
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+a+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+z+` CASCADE`)

	dir := t.TempDir()
	// Lexicographic order: lo_fk_a.sql before lo_fk_z.sql
	require.NoError(t, os.WriteFile(filepath.Join(dir, a+".sql"), []byte(`
CREATE SCHEMA `+a+`;
CREATE TABLE `+a+`.child (id INT PRIMARY KEY, pid INT NOT NULL REFERENCES `+z+`.parent(id));
`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, z+".sql"), []byte(`
CREATE SCHEMA `+z+`;
CREATE TABLE `+z+`.parent (id INT PRIMARY KEY);
`), 0644))

	files, err := sortedSQLFiles(dir)
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, a+".sql", filepath.Base(files[0]))

	first, err := os.ReadFile(files[0])
	require.NoError(t, err)
	err = execDumpFile(ctx, pool, string(first))
	require.Error(t, err, "child schema file first must fail: referenced schema/table not created yet")
	errStr := strings.ToLower(err.Error())
	assert.True(t,
		strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "schema"),
		"unexpected error: %v", err)

	// Dependency order succeeds: parent schema (z) before child (a), not lexicographic order.
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+a+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+z+` CASCADE`)
	for _, f := range []string{
		filepath.Join(dir, z+".sql"),
		filepath.Join(dir, a+".sql"),
	} {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NoError(t, execDumpFile(ctx, pool, string(b)))
	}
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+a+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+z+` CASCADE`)
}

func TestIntegration_LexicographicApplyOrder_ViewFirstFileFailsWhenViewSchemaSortsBeforeBase(t *testing.T) {
	ctx := context.Background()
	pool := connectIntegrationPool(t)
	defer pool.Close()

	const a, z = "lo_vw_a", "lo_vw_z"
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+a+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+z+` CASCADE`)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, a+".sql"), []byte(`
CREATE SCHEMA `+a+`;
CREATE VIEW `+a+`.v AS SELECT id FROM `+z+`.base;
`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, z+".sql"), []byte(`
CREATE SCHEMA `+z+`;
CREATE TABLE `+z+`.base (id INT PRIMARY KEY);
`), 0644))

	files, err := sortedSQLFiles(dir)
	require.NoError(t, err)
	first, err := os.ReadFile(files[0])
	require.NoError(t, err)
	err = execDumpFile(ctx, pool, string(first))
	require.Error(t, err, "view before base schema should fail")
}

func TestIntegration_LexicographicApplyOrder_FunctionFirstFileFailsWhenBodyReferencesLaterSchema(t *testing.T) {
	ctx := context.Background()
	pool := connectIntegrationPool(t)
	defer pool.Close()

	const a, z = "lo_fn_a", "lo_fn_z"
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+a+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+z+` CASCADE`)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, a+".sql"), []byte(`
CREATE SCHEMA `+a+`;
CREATE FUNCTION `+a+`.f() RETURNS INT LANGUAGE SQL STABLE AS $$SELECT count(*)::int FROM `+z+`.t$$;
`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, z+".sql"), []byte(`
CREATE SCHEMA `+z+`;
CREATE TABLE `+z+`.t (id INT PRIMARY KEY);
`), 0644))

	files, err := sortedSQLFiles(dir)
	require.NoError(t, err)
	first, err := os.ReadFile(files[0])
	require.NoError(t, err)
	err = execDumpFile(ctx, pool, string(first))
	require.Error(t, err, "function body referencing other schema should fail when that schema is applied later")
}

func TestIntegration_LexicographicApplyOrder_CompositeTypeFirstFileFailsWhenTypeDefinedInLaterSchema(t *testing.T) {
	ctx := context.Background()
	pool := connectIntegrationPool(t)
	defer pool.Close()

	const a, z = "lo_ty_a", "lo_ty_z"
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+a+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+z+` CASCADE`)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, a+".sql"), []byte(`
CREATE SCHEMA `+a+`;
CREATE TABLE `+a+`.u (p `+z+`.pair);
`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, z+".sql"), []byte(`
CREATE SCHEMA `+z+`;
CREATE TYPE `+z+`.pair AS (x INT, y INT);
`), 0644))

	files, err := sortedSQLFiles(dir)
	require.NoError(t, err)
	first, err := os.ReadFile(files[0])
	require.NoError(t, err)
	err = execDumpFile(ctx, pool, string(first))
	require.Error(t, err, "table using cross-schema type should fail when type schema is applied later")
}

func TestIntegration_LexicographicApplyOrder_TriggerFirstFileFailsWhenFunctionLivesInLaterSchema(t *testing.T) {
	ctx := context.Background()
	pool := connectIntegrationPool(t)
	defer pool.Close()

	const a, z = "lo_tr_a", "lo_tr_z"
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+a+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+z+` CASCADE`)

	dir := t.TempDir()
	// Table + trigger in schema a reference function in z only. Lex order: a.sql before z.sql.
	require.NoError(t, os.WriteFile(filepath.Join(dir, a+".sql"), []byte(`
CREATE SCHEMA `+a+`;
CREATE TABLE `+a+`.t (id INT PRIMARY KEY);
CREATE TRIGGER tr AFTER INSERT ON `+a+`.t FOR EACH ROW EXECUTE FUNCTION `+z+`.tf();
`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, z+".sql"), []byte(`
CREATE SCHEMA `+z+`;
CREATE FUNCTION `+z+`.tf() RETURNS TRIGGER LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
`), 0644))

	files, err := sortedSQLFiles(dir)
	require.NoError(t, err)
	first, err := os.ReadFile(files[0])
	require.NoError(t, err)
	err = execDumpFile(ctx, pool, string(first))
	require.Error(t, err, "trigger referencing function in other schema should fail when function file is applied later")
}

func TestIntegration_LexicographicApplyOrder_CitextTableBeforeExtensionFileFails(t *testing.T) {
	ctx := context.Background()
	pool := connectIntegrationPool(t)
	defer pool.Close()

	const early = "lo_ext_e"
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+early+` CASCADE`)
	_, _ = pool.Exec(ctx, `DROP EXTENSION IF EXISTS citext CASCADE`)

	dir := t.TempDir()
	// lo_ext_e.sql sorts before public.sql; table needs citext before extension file runs.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "lo_ext_e.sql"), []byte(`
CREATE SCHEMA `+early+`;
CREATE TABLE `+early+`.t (x citext);
`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "public.sql"), []byte(`
CREATE EXTENSION IF NOT EXISTS citext;
`), 0644))

	files, err := sortedSQLFiles(dir)
	require.NoError(t, err)
	require.Len(t, files, 2)
	first, err := os.ReadFile(files[0])
	require.NoError(t, err)
	err = execDumpFile(ctx, pool, string(first))
	require.Error(t, err, "citext column before extension install should fail")
	_, _ = pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS citext`)
}
