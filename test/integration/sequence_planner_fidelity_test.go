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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExplicitOwnedSequenceCreatePlanExecutesAndRoundTrips(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	t.Cleanup(func() { _ = resetPublicSchema(context.Background(), pool) })
	require.NoError(t, resetPublicSchema(ctx, pool))

	desired, err := parser.NewParser().ParseSQL(`
		CREATE SEQUENCE public.item_ids AS integer
			START WITH 11 INCREMENT BY 3 CACHE 7 CYCLE;
		CREATE TABLE public.items (
			id integer DEFAULT nextval('public.item_ids'::regclass),
			payload text
		);
		ALTER SEQUENCE public.item_ids OWNED BY public.items.id;
	`)
	require.NoError(t, err)
	diff, err := differ.NewDiffer().Diff(desired, schema.SchemaObjectMap{})
	require.NoError(t, err)
	generated, err := planner.NewDDLGenerator().GenerateDDL(diff, desired)
	require.NoError(t, err)

	sequenceCreate := `CREATE SEQUENCE "public"."item_ids"`
	tableCreate := `CREATE TABLE "public"."items"`
	ownedBy := `ALTER SEQUENCE "public"."item_ids" OWNED BY "public"."items"."id";`
	assert.Less(t, stringIndex(t, generated, sequenceCreate), stringIndex(t, generated, tableCreate))
	assert.Less(t, stringIndex(t, generated, tableCreate), stringIndex(t, generated, ownedBy))

	_, err = pool.Exec(ctx, generated)
	require.NoError(t, err, "phased sequence plan must execute:\n%s", generated)
	actualObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, excludedCatalogSchemas())
	require.NoError(t, err)
	actual, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)
	roundTrip, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	require.True(t, roundTrip.IsEmpty(), "explicit owned sequence must round-trip: creates=%v drops=%v alters=%v\nDDL:\n%s", roundTrip.ToCreate, roundTrip.ToDrop, roundTrip.ToAlter, generated)
}

func TestOwnedSequenceOwnerTransitionRunsSafely(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	const oldOwner = "schemata_seq_old_owner"
	const newOwner = "schemata_seq_new_owner"
	const staleReader = "schemata_seq_stale_reader"
	const exactReader = "schemata_seq_exact_reader"
	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		DROP ROLE IF EXISTS schemata_seq_old_owner;
		DROP ROLE IF EXISTS schemata_seq_new_owner;
		DROP ROLE IF EXISTS schemata_seq_stale_reader;
		DROP ROLE IF EXISTS schemata_seq_exact_reader;
		CREATE ROLE schemata_seq_old_owner;
		CREATE ROLE schemata_seq_new_owner;
		CREATE ROLE schemata_seq_stale_reader;
		CREATE ROLE schemata_seq_exact_reader;

		CREATE TABLE public.items (id integer);
		ALTER TABLE public.items OWNER TO schemata_seq_old_owner;
		CREATE SEQUENCE public.item_ids AS integer;
		ALTER SEQUENCE public.item_ids OWNER TO schemata_seq_old_owner;
		ALTER SEQUENCE public.item_ids OWNED BY public.items.id;
		GRANT USAGE ON SEQUENCE public.item_ids TO schemata_seq_stale_reader;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = resetPublicSchema(context.Background(), pool)
		_, _ = pool.Exec(context.Background(), `
			DROP ROLE IF EXISTS schemata_seq_old_owner;
			DROP ROLE IF EXISTS schemata_seq_new_owner;
			DROP ROLE IF EXISTS schemata_seq_stale_reader;
			DROP ROLE IF EXISTS schemata_seq_exact_reader;
		`)
	})

	desired, err := parser.NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		ALTER TABLE public.items OWNER TO schemata_seq_new_owner;
		CREATE SEQUENCE public.item_ids AS integer;
		ALTER SEQUENCE public.item_ids OWNER TO schemata_seq_new_owner;
		ALTER SEQUENCE public.item_ids OWNED BY public.items.id;
		REVOKE ALL ON SEQUENCE public.item_ids FROM PUBLIC, schemata_seq_new_owner;
		GRANT USAGE ON SEQUENCE public.item_ids TO schemata_seq_exact_reader;
	`)
	require.NoError(t, err)
	actualObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, excludedCatalogSchemas())
	require.NoError(t, err)
	actual, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)
	diff, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	generated, err := planner.NewDDLGenerator().GenerateDDL(diff, desired, actual)
	require.NoError(t, err)
	assert.Contains(t, generated, `ALTER TABLE "public"."items" OWNER TO "schemata_seq_new_owner";`)
	assert.NotContains(t, generated, `ALTER SEQUENCE "public"."item_ids" OWNER TO "schemata_seq_new_owner";`)
	assert.Less(t,
		stringIndex(t, generated, `ALTER TABLE "public"."items" OWNER TO "schemata_seq_new_owner";`),
		stringIndex(t, generated, `REVOKE ALL ON SEQUENCE "public"."item_ids" FROM PUBLIC;`),
	)

	_, err = pool.Exec(ctx, generated)
	require.NoError(t, err, "owned sequence owner transition must execute:\n%s", generated)

	var tableOwner, sequenceOwner string
	err = pool.QueryRow(ctx, `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.items'::regclass`).Scan(&tableOwner)
	require.NoError(t, err)
	err = pool.QueryRow(ctx, `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.item_ids'::regclass`).Scan(&sequenceOwner)
	require.NoError(t, err)
	assert.Equal(t, newOwner, tableOwner)
	assert.Equal(t, newOwner, sequenceOwner)

	var totalACLRows, exactACLRows int
	err = pool.QueryRow(ctx, `
		SELECT
			count(priv.grantee),
			count(priv.grantee) FILTER (
				WHERE grantee.rolname = $1
				  AND priv.privilege_type = 'USAGE'
				  AND NOT priv.is_grantable
				  AND priv.grantor = c.relowner
			)
		FROM pg_class c
		LEFT JOIN LATERAL aclexplode(COALESCE(c.relacl, ARRAY[]::aclitem[])) AS priv ON true
		LEFT JOIN pg_roles grantee ON grantee.oid = priv.grantee
		WHERE c.oid = 'public.item_ids'::regclass
		GROUP BY c.relowner
	`, exactReader).Scan(&totalACLRows, &exactACLRows)
	require.NoError(t, err)
	assert.Equal(t, 1, totalACLRows)
	assert.Equal(t, 1, exactACLRows)
}

func stringIndex(t *testing.T, value, fragment string) int {
	t.Helper()
	index := strings.Index(value, fragment)
	require.NotEqual(t, -1, index, "fragment %q missing from:\n%s", fragment, value)
	return index
}
