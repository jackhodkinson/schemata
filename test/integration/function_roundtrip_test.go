//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFunctionRoundTrip_NoDiffOnFormatting checks the contract that parser output
// and catalog output for the same function must compare equal after normalization.
func TestFunctionRoundTrip_NoDiffOnFormatting(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL

	dbConn := &config.DBConnection{URL: &dbURL}
	pool, err := db.Connect(ctx, dbConn)
	require.NoError(t, err, "failed to connect to integration test database")
	defer pool.Close()

	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err = os.WriteFile(schemaFile, []byte(`
CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$function$;
`), 0644)
	require.NoError(t, err)

	err = cleanAndApplySchema(ctx, pool, schemaFile)
	require.NoError(t, err, "failed to apply schema with function definition")

	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err, "failed to parse desired schema file")

	catalog := db.NewCatalog(pool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err, "failed to extract function from catalog")

	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err, "failed to build actual schema map")

	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err, "failed to diff parser and catalog objects")

	fnKey := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "update_updated_at_column",
		Signature: "()",
	}

	assert.NotContains(t, diff.ToCreate, fnKey, "function should not be recreated after round-trip")
	assert.NotContains(t, diff.ToDrop, fnKey, "function should not be dropped after round-trip")
	for _, alter := range diff.ToAlter {
		assert.NotEqual(t, fnKey, alter.Key, "function should not be altered when only formatting differs")
	}
	assert.True(t, diff.IsEmpty(), "function round-trip should produce an empty diff")
}

// TestFunctionRoundTrip_OutArgIdentityStable ensures parser/catalog agree on
// function identity when OUT parameters are present.
func TestFunctionRoundTrip_OutArgIdentityStable(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL

	dbConn := &config.DBConnection{URL: &dbURL}
	pool, err := db.Connect(ctx, dbConn)
	require.NoError(t, err, "failed to connect to integration test database")
	defer pool.Close()

	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err = os.WriteFile(schemaFile, []byte(`
CREATE OR REPLACE FUNCTION public.add_one_with_out(IN n integer, OUT result integer)
LANGUAGE sql
AS $function$
    SELECT n + 1;
$function$;
`), 0644)
	require.NoError(t, err)

	err = cleanAndApplySchema(ctx, pool, schemaFile)
	require.NoError(t, err, "failed to apply schema with OUT-arg function definition")

	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err, "failed to parse desired schema file")

	catalog := db.NewCatalog(pool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err, "failed to extract function from catalog")

	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err, "failed to build actual schema map")

	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err, "failed to diff parser and catalog objects")

	for _, key := range diff.ToCreate {
		assert.NotEqual(t, "add_one_with_out", key.Name, "OUT-arg function should not show as create")
	}
	for _, key := range diff.ToDrop {
		assert.NotEqual(t, "add_one_with_out", key.Name, "OUT-arg function should not show as drop")
	}
	for _, alter := range diff.ToAlter {
		assert.NotEqual(t, "add_one_with_out", alter.Key.Name, "OUT-arg function should not show as alter")
	}
	assert.True(t, diff.IsEmpty(), "OUT-arg function should round-trip with stable identity")
}

func TestFunctionRoundTrip_ExactCatalogIdentityDefaultsModesAndConfig(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS "Function Types" CASCADE;
		CREATE SCHEMA "Function Types";
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "Function Types" CASCADE`)
		_ = resetPublicSchema(context.Background(), pool)
	})

	source := `
		CREATE TYPE "Function Types"."Odd Type" AS ENUM ('first', 'second');

		CREATE FUNCTION public.overloaded(timestamp with time zone)
		RETURNS text LANGUAGE sql AS 'SELECT $1::text';

		CREATE FUNCTION public.overloaded(timestamp without time zone)
		RETURNS text LANGUAGE sql AS 'SELECT $1::text';

		CREATE FUNCTION public.overloaded("Function Types"."Odd Type"[])
		RETURNS text LANGUAGE sql AS 'SELECT coalesce(array_length($1, 1), 0)::text';

		CREATE FUNCTION public.defaults_probe(
			prefix text,
			payload text DEFAULT 'a,b'::text,
			flags integer[] DEFAULT ARRAY[1, 2]
		)
		RETURNS text
		LANGUAGE sql
		SECURITY DEFINER
		PARALLEL SAFE
		SET search_path TO "$user", pg_catalog, public, "Function Types"
		AS 'SELECT prefix || payload || coalesce(array_length(flags, 1), 0)::text';

		CREATE FUNCTION public.mode_probe(
			IN seed integer,
			INOUT accumulator integer DEFAULT 1,
			OUT rendered text
		)
		LANGUAGE plpgsql
		AS 'BEGIN accumulator := accumulator + seed; rendered := accumulator::text; END';

		CREATE FUNCTION public.table_probe(value "Function Types"."Odd Type")
		RETURNS TABLE (
			"Odd Result" "Function Types"."Odd Type"[],
			observed_at timestamp with time zone
		)
		LANGUAGE sql AS 'SELECT ARRAY[value], clock_timestamp()';

		CREATE FUNCTION public.variadic_probe(VARIADIC items text[])
		RETURNS integer LANGUAGE sql AS 'SELECT cardinality(items)';

		CREATE FUNCTION public.empty_path_probe()
		RETURNS integer LANGUAGE sql SET search_path TO '' AS 'SELECT 1';
	`

	_, err = pool.Exec(ctx, source)
	require.NoError(t, err)

	desired, err := parser.NewParser().ParseSQL(source)
	require.NoError(t, err)
	actualObjects, err := db.NewCatalog(pool).ExtractAllObjects(
		ctx,
		[]string{"public", "Function Types"},
		[]string{"pg_catalog", "information_schema", "pg_toast", "schemata"},
	)
	require.NoError(t, err)
	actual, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	multiwordKey := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "overloaded", Signature: "(timestamp with time zone)"}
	require.Contains(t, actual, multiwordKey)
	qualifiedArrayKey := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "overloaded", Signature: `("Function Types"."Odd Type"[])`}
	require.Contains(t, actual, qualifiedArrayKey)

	defaultsKey := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "defaults_probe", Signature: "(text,text,integer[])"}
	defaultsFunction := actual[defaultsKey].Payload.(schema.Function)
	require.Nil(t, defaultsFunction.Args[0].Default)
	require.NotNil(t, defaultsFunction.Args[1].Default)
	require.NotNil(t, defaultsFunction.Args[2].Default)
	require.Equal(t, []schema.SchemaName{"$user", "pg_catalog", "public", "Function Types"}, defaultsFunction.SearchPath)
	require.True(t, defaultsFunction.SecurityDefiner)

	emptyPathKey := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "empty_path_probe", Signature: "()"}
	emptyPathFunction := actual[emptyPathKey].Payload.(schema.Function)
	require.NotNil(t, emptyPathFunction.SearchPath)
	require.Empty(t, emptyPathFunction.SearchPath)

	diff, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	require.True(t, diff.IsEmpty(), "function catalog round-trip must be exact: creates=%v drops=%v alters=%v", diff.ToCreate, diff.ToDrop, diff.ToAlter)
}

func TestFunctionCatalogRejectsUnmodeledProconfig(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, resetPublicSchema(ctx, pool))
	t.Cleanup(func() { _ = resetPublicSchema(context.Background(), pool) })

	_, err = pool.Exec(ctx, `
		CREATE FUNCTION public.unmodeled_config() RETURNS integer
		LANGUAGE sql
		SECURITY DEFINER
		SET statement_timeout TO '1s'
		AS 'SELECT 1';
	`)
	require.NoError(t, err)

	_, err = db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), `configuration key "statement_timeout" is not modeled`)
}
