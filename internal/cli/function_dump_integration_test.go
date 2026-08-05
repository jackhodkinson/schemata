//go:build integration

package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/stretchr/testify/require"
)

func TestIntegration_FunctionDumpParsesReappliesAndRoundTripsExactly(t *testing.T) {
	ctx := context.Background()
	pool := connectIntegrationPool(t)
	t.Cleanup(pool.Close)

	reset := func() {
		t.Helper()
		_, err := pool.Exec(ctx, `
			DROP SCHEMA IF EXISTS public CASCADE;
			DROP SCHEMA IF EXISTS "Function Dump Types" CASCADE;
			CREATE SCHEMA public;
			GRANT ALL ON SCHEMA public TO postgres;
			GRANT ALL ON SCHEMA public TO public;
			CREATE SCHEMA "Function Dump Types";
		`)
		require.NoError(t, err)
	}
	reset()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS public CASCADE;
			DROP SCHEMA IF EXISTS "Function Dump Types" CASCADE;
			CREATE SCHEMA public;
			GRANT ALL ON SCHEMA public TO postgres;
			GRANT ALL ON SCHEMA public TO public;
		`)
	})

	source := `
		CREATE TYPE "Function Dump Types"."Odd Type" AS ENUM ('first', 'second');

		CREATE FUNCTION public.dump_overload(timestamp with time zone)
		RETURNS text LANGUAGE sql AS 'SELECT $1::text';

		CREATE FUNCTION public.dump_overload(timestamp without time zone)
		RETURNS text LANGUAGE sql AS 'SELECT $1::text';

		CREATE FUNCTION public.dump_overload("Function Dump Types"."Odd Type"[])
		RETURNS text LANGUAGE sql AS 'SELECT coalesce(array_length($1, 1), 0)::text';

		CREATE FUNCTION public.dump_defaults(
			prefix text,
			payload text DEFAULT 'a,b'::text,
			flags integer[] DEFAULT ARRAY[1, 2]
		)
		RETURNS text
		LANGUAGE sql
		STRICT
		SECURITY DEFINER
		PARALLEL SAFE
		SET search_path TO "$user", pg_catalog, public, "Function Dump Types"
		AS 'SELECT prefix || payload || coalesce(array_length(flags, 1), 0)::text';

		CREATE FUNCTION public.dump_modes(
			IN seed integer,
			INOUT accumulator integer DEFAULT 1,
			OUT rendered text
		)
		LANGUAGE plpgsql
		AS 'BEGIN accumulator := accumulator + seed; rendered := accumulator::text; END';

		CREATE FUNCTION public.dump_table(value "Function Dump Types"."Odd Type")
		RETURNS TABLE (
			"Odd Result" "Function Dump Types"."Odd Type"[],
			observed_at timestamp with time zone
		)
		LANGUAGE sql AS 'SELECT ARRAY[value], clock_timestamp()';

		CREATE FUNCTION public.dump_variadic(VARIADIC items text[])
		RETURNS integer LANGUAGE sql AS 'SELECT cardinality(items)';
	`
	_, err := pool.Exec(ctx, source)
	require.NoError(t, err)

	includeSchemas := []string{"public", "Function Dump Types"}
	originalObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, includeSchemas, nil)
	require.NoError(t, err)
	originalMap, err := objectmap.Build(originalObjects)
	require.NoError(t, err)

	dumpPath := filepath.Join(t.TempDir(), "schema.sql")
	files, err := writeDumpSingleFile(dumpPath, originalObjects, planner.NewDDLGenerator())
	require.NoError(t, err)
	require.Equal(t, 1, files)

	dumpSQL, err := os.ReadFile(dumpPath)
	require.NoError(t, err)
	require.Contains(t, string(dumpSQL), "DEFAULT 'a,b'")
	require.Contains(t, string(dumpSQL), "INOUT")
	require.Contains(t, string(dumpSQL), "OUT")
	require.Contains(t, string(dumpSQL), "RETURNS TABLE")
	require.Contains(t, string(dumpSQL), "VARIADIC")
	require.Contains(t, string(dumpSQL), "STRICT")
	require.Contains(t, string(dumpSQL), "SECURITY DEFINER")
	require.Contains(t, string(dumpSQL), "PARALLEL SAFE")
	require.Contains(t, string(dumpSQL), `SET search_path TO "$user", "pg_catalog", "public", "Function Dump Types"`)

	parsedDump, err := parser.NewParser().ParseFile(dumpPath)
	require.NoError(t, err, "CLI dump must be accepted as declarative schema:\n%s", dumpSQL)
	parsedDiff, err := differ.NewDiffer().Diff(parsedDump, originalMap)
	require.NoError(t, err)
	require.True(t, parsedDiff.IsEmpty(), "parsed CLI dump must match its catalog source: creates=%v drops=%v alters=%v", parsedDiff.ToCreate, parsedDiff.ToDrop, parsedDiff.ToAlter)

	reset()
	_, err = pool.Exec(ctx, string(dumpSQL))
	require.NoError(t, err, "CLI dump must reapply to a clean database:\n%s", dumpSQL)

	reappliedObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, includeSchemas, nil)
	require.NoError(t, err)
	reappliedMap, err := objectmap.Build(reappliedObjects)
	require.NoError(t, err)
	roundTrip, err := differ.NewDiffer().Diff(originalMap, reappliedMap)
	require.NoError(t, err)
	require.True(t, roundTrip.IsEmpty(), "reapplied CLI dump must round-trip exactly: creates=%v drops=%v alters=%v", roundTrip.ToCreate, roundTrip.ToDrop, roundTrip.ToAlter)
}
