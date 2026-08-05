//go:build integration
// +build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestGrantOwnerMetadataGeneratedDDLRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		DROP ROLE IF EXISTS schemata_acl_owner;
		DROP ROLE IF EXISTS schemata_acl_reader;
		DROP ROLE IF EXISTS "PUBLIC";
		CREATE ROLE schemata_acl_owner;
		CREATE ROLE schemata_acl_reader;
		CREATE ROLE "PUBLIC";
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS public CASCADE;
			CREATE SCHEMA public;
			GRANT ALL ON SCHEMA public TO postgres;
			GRANT ALL ON SCHEMA public TO public;
			DROP ROLE IF EXISTS schemata_acl_owner;
			DROP ROLE IF EXISTS schemata_acl_reader;
			DROP ROLE IF EXISTS "PUBLIC";
		`)
	})

	source := `
		CREATE TYPE public.mood AS ENUM ('ok', 'great');
		ALTER TYPE public.mood OWNER TO schemata_acl_owner;
		COMMENT ON TYPE public.mood IS 'mood type';
		REVOKE USAGE ON TYPE public.mood FROM PUBLIC, schemata_acl_owner;
		GRANT USAGE ON TYPE public.mood TO schemata_acl_reader;

		CREATE DOMAIN public.positive_integer AS integer CHECK (VALUE > 0);
		ALTER DOMAIN public.positive_integer OWNER TO schemata_acl_owner;
		COMMENT ON DOMAIN public.positive_integer IS 'positive values';
		REVOKE USAGE ON TYPE public.positive_integer FROM PUBLIC, schemata_acl_owner;
		GRANT USAGE ON TYPE public.positive_integer TO schemata_acl_reader;

		CREATE TYPE public.item_pair AS (left_id integer, right_id integer);
		ALTER TYPE public.item_pair OWNER TO schemata_acl_owner;
		COMMENT ON TYPE public.item_pair IS 'item pair';
		REVOKE USAGE ON TYPE public.item_pair FROM PUBLIC, schemata_acl_owner;
		GRANT USAGE ON TYPE public.item_pair TO schemata_acl_reader;

		CREATE TABLE public.items (id integer, mood public.mood);
		ALTER TABLE public.items OWNER TO schemata_acl_owner;
		COMMENT ON TABLE public.items IS 'items table';
		COMMENT ON COLUMN public.items.id IS 'item identity';
		REVOKE ALL ON TABLE public.items FROM PUBLIC, schemata_acl_owner;
		GRANT SELECT ON TABLE public.items TO PUBLIC;
		GRANT INSERT ON TABLE public.items TO "PUBLIC";
		GRANT UPDATE ON TABLE public.items TO schemata_acl_reader WITH GRANT OPTION;

		CREATE SEQUENCE public.item_ids AS integer OWNED BY public.items.id;
		ALTER SEQUENCE public.item_ids OWNER TO schemata_acl_owner;
		COMMENT ON SEQUENCE public.item_ids IS 'item ids';
		REVOKE ALL ON SEQUENCE public.item_ids FROM PUBLIC, schemata_acl_owner;
		GRANT USAGE, SELECT ON SEQUENCE public.item_ids TO schemata_acl_reader;

		CREATE VIEW public.item_view AS SELECT id, mood FROM public.items;
		ALTER VIEW public.item_view OWNER TO schemata_acl_owner;
		COMMENT ON VIEW public.item_view IS 'item view';
		REVOKE ALL ON TABLE public.item_view FROM PUBLIC, schemata_acl_owner;
		GRANT SELECT ON TABLE public.item_view TO schemata_acl_reader;

		CREATE MATERIALIZED VIEW public.item_snapshot AS SELECT id FROM public.items;
		ALTER MATERIALIZED VIEW public.item_snapshot OWNER TO schemata_acl_owner;
		COMMENT ON MATERIALIZED VIEW public.item_snapshot IS 'item snapshot';
		REVOKE ALL ON TABLE public.item_snapshot FROM PUBLIC, schemata_acl_owner;
		GRANT SELECT ON TABLE public.item_snapshot TO schemata_acl_reader;

		CREATE FUNCTION public.describe_item(value integer) RETURNS text LANGUAGE sql AS 'SELECT value::text';
		CREATE FUNCTION public.describe_item(value text) RETURNS text LANGUAGE sql AS 'SELECT value';
		ALTER FUNCTION public.describe_item(integer) OWNER TO schemata_acl_owner;
		ALTER FUNCTION public.describe_item(text) OWNER TO schemata_acl_owner;
		COMMENT ON FUNCTION public.describe_item(integer) IS 'integer overload';
		COMMENT ON FUNCTION public.describe_item(text) IS 'text overload';
		REVOKE EXECUTE ON FUNCTION public.describe_item(integer), public.describe_item(text) FROM PUBLIC, schemata_acl_owner;
		GRANT EXECUTE ON FUNCTION public.describe_item(integer) TO schemata_acl_reader;
		GRANT EXECUTE ON FUNCTION public.describe_item(text) TO "PUBLIC";

		CREATE FUNCTION public.private_task() RETURNS integer LANGUAGE sql AS 'SELECT 1';
		REVOKE EXECUTE ON FUNCTION public.private_task() FROM PUBLIC;

		CREATE FUNCTION public.mood_label(value public.mood) RETURNS text LANGUAGE sql AS 'SELECT value::text';
		ALTER FUNCTION public.mood_label(public.mood) OWNER TO schemata_acl_owner;
		COMMENT ON FUNCTION public.mood_label(public.mood) IS 'custom type overload';
		REVOKE EXECUTE ON FUNCTION public.mood_label(public.mood) FROM PUBLIC, schemata_acl_owner;
		GRANT EXECUTE ON FUNCTION public.mood_label(public.mood) TO schemata_acl_reader;
	`

	desired, err := parser.NewParser().ParseSQL(source)
	require.NoError(t, err)
	diff, err := differ.NewDiffer().Diff(desired, schema.SchemaObjectMap{})
	require.NoError(t, err)
	generated, err := planner.NewDDLGenerator().GenerateDDL(diff, desired)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, generated)
	require.NoError(t, err, "generated metadata DDL must execute:\n%s", generated)

	actualObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err)
	actual, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	roundTrip, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	require.True(t, roundTrip.IsEmpty(), "metadata round-trip must be empty: creates=%v drops=%v alters=%v\nDDL:\n%s", roundTrip.ToCreate, roundTrip.ToDrop, roundTrip.ToAlter, generated)
}

func TestManagedACLRemovesStaleGrantAcrossOwnerChange(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		DROP ROLE IF EXISTS schemata_acl_old_owner;
		DROP ROLE IF EXISTS schemata_acl_new_owner;
		DROP ROLE IF EXISTS schemata_acl_stale_reader;
		DROP ROLE IF EXISTS schemata_acl_exact_reader;
		CREATE ROLE schemata_acl_old_owner;
		CREATE ROLE schemata_acl_new_owner;
		CREATE ROLE schemata_acl_stale_reader;
		CREATE ROLE schemata_acl_exact_reader;
		CREATE TABLE public.items (id integer);
		ALTER TABLE public.items OWNER TO schemata_acl_old_owner;
		GRANT SELECT ON TABLE public.items TO schemata_acl_stale_reader;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS public CASCADE;
			CREATE SCHEMA public;
			GRANT ALL ON SCHEMA public TO postgres;
			GRANT ALL ON SCHEMA public TO public;
			DROP ROLE IF EXISTS schemata_acl_old_owner;
			DROP ROLE IF EXISTS schemata_acl_new_owner;
			DROP ROLE IF EXISTS schemata_acl_stale_reader;
			DROP ROLE IF EXISTS schemata_acl_exact_reader;
		`)
	})

	desired, err := parser.NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		ALTER TABLE public.items OWNER TO schemata_acl_new_owner;
		REVOKE ALL ON TABLE public.items FROM PUBLIC, schemata_acl_new_owner;
		GRANT SELECT ON TABLE public.items TO schemata_acl_exact_reader;
	`)
	require.NoError(t, err)
	actualObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err)
	actual, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	diff, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	generated, err := planner.NewDDLGenerator().GenerateDDL(diff, desired, actual)
	require.NoError(t, err)
	require.Contains(t, generated, `REVOKE ALL ON TABLE "public"."items" FROM "schemata_acl_stale_reader";`)
	_, err = pool.Exec(ctx, generated)
	require.NoError(t, err, "owner-change ACL DDL must execute:\n%s", generated)

	afterObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err)
	after, err := buildObjectMapFromObjects(afterObjects)
	require.NoError(t, err)
	roundTrip, err := differ.NewDiffer().Diff(desired, after)
	require.NoError(t, err)
	require.True(t, roundTrip.IsEmpty(), "owner-change ACL must be exact: creates=%v drops=%v alters=%v\nDDL:\n%s", roundTrip.ToCreate, roundTrip.ToDrop, roundTrip.ToAlter, generated)
}

func TestCatalogRejectsColumnLevelACL(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		DROP ROLE IF EXISTS schemata_column_reader;
		CREATE ROLE schemata_column_reader;
		CREATE TABLE public.column_acl (id integer, value text);
		GRANT SELECT (id) ON TABLE public.column_acl TO schemata_column_reader;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS public CASCADE;
			CREATE SCHEMA public;
			GRANT ALL ON SCHEMA public TO postgres;
			GRANT ALL ON SCHEMA public TO public;
			DROP ROLE IF EXISTS schemata_column_reader;
		`)
	})

	_, err = db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "column-level ACL") && strings.Contains(err.Error(), "public.column_acl.id"), err.Error())
}

func TestCatalogRejectsNonOwnerACLGrantor(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		DROP ROLE IF EXISTS schemata_grant_owner;
		DROP ROLE IF EXISTS schemata_delegated_grantor;
		DROP ROLE IF EXISTS schemata_delegated_reader;
		CREATE ROLE schemata_grant_owner;
		CREATE ROLE schemata_delegated_grantor;
		CREATE ROLE schemata_delegated_reader;
		CREATE TABLE public.delegated_acl (id integer);
		ALTER TABLE public.delegated_acl OWNER TO schemata_grant_owner;
		GRANT SELECT ON TABLE public.delegated_acl TO schemata_delegated_grantor WITH GRANT OPTION;
		SET ROLE schemata_delegated_grantor;
		GRANT SELECT ON TABLE public.delegated_acl TO schemata_delegated_reader;
		RESET ROLE;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS public CASCADE;
			CREATE SCHEMA public;
			GRANT ALL ON SCHEMA public TO postgres;
			GRANT ALL ON SCHEMA public TO public;
			DROP ROLE IF EXISTS schemata_grant_owner;
			DROP ROLE IF EXISTS schemata_delegated_grantor;
			DROP ROLE IF EXISTS schemata_delegated_reader;
		`)
	})

	_, err = db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "grantor other than the object owner")
}

func TestACLPrivilegeStateTransitionsRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		DROP ROLE IF EXISTS schemata_acl_state_app;
		DROP ROLE IF EXISTS schemata_acl_state_removed;
		CREATE ROLE schemata_acl_state_app;
		CREATE ROLE schemata_acl_state_removed;
		CREATE TABLE public.acl_states (id integer);
		GRANT SELECT, REFERENCES ON TABLE public.acl_states TO schemata_acl_state_app;
		GRANT UPDATE, DELETE ON TABLE public.acl_states TO schemata_acl_state_app WITH GRANT OPTION;
		GRANT TRIGGER ON TABLE public.acl_states TO schemata_acl_state_removed WITH GRANT OPTION;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS public CASCADE;
			CREATE SCHEMA public;
			GRANT ALL ON SCHEMA public TO postgres;
			GRANT ALL ON SCHEMA public TO public;
			DROP ROLE IF EXISTS schemata_acl_state_app;
			DROP ROLE IF EXISTS schemata_acl_state_removed;
		`)
	})

	desired, err := parser.NewParser().ParseSQL(`
		CREATE TABLE public.acl_states (id integer);
		REVOKE ALL ON TABLE public.acl_states FROM PUBLIC, postgres;
		GRANT SELECT, INSERT, DELETE ON TABLE public.acl_states TO schemata_acl_state_app;
		GRANT UPDATE, REFERENCES ON TABLE public.acl_states TO schemata_acl_state_app WITH GRANT OPTION;
	`)
	require.NoError(t, err)
	actualObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err)
	actual, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	diff, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	generated, err := planner.NewDDLGenerator().GenerateDDL(diff, desired, actual)
	require.NoError(t, err)
	require.Contains(t, generated, `GRANT INSERT ON TABLE "public"."acl_states" TO "schemata_acl_state_app";`)
	require.Contains(t, generated, `GRANT REFERENCES ON TABLE "public"."acl_states" TO "schemata_acl_state_app" WITH GRANT OPTION;`)
	require.Contains(t, generated, `REVOKE GRANT OPTION FOR DELETE ON TABLE "public"."acl_states" FROM "schemata_acl_state_app";`)
	require.Contains(t, generated, `REVOKE TRIGGER ON TABLE "public"."acl_states" FROM "schemata_acl_state_removed";`)
	require.NotContains(t, generated, `REVOKE GRANT OPTION FOR TRIGGER`)

	_, err = pool.Exec(ctx, generated)
	require.NoError(t, err, "ACL transition DDL must execute:\n%s", generated)
	afterObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err)
	after, err := buildObjectMapFromObjects(afterObjects)
	require.NoError(t, err)
	roundTrip, err := differ.NewDiffer().Diff(desired, after)
	require.NoError(t, err)
	require.True(t, roundTrip.IsEmpty(), "ACL state transitions must converge exactly: creates=%v drops=%v alters=%v\nDDL:\n%s", roundTrip.ToCreate, roundTrip.ToDrop, roundTrip.ToAlter, generated)
}
