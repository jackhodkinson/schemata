//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
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

func TestExplicitEmptyACLIsPreservedThroughCatalogAndReplay(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	t.Cleanup(func() { _ = resetPublicSchema(context.Background(), pool) })

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, `
		CREATE VIEW public.empty_acl_view AS SELECT 1 AS id;
		CREATE MATERIALIZED VIEW public.empty_acl_snapshot AS SELECT 1 AS id;
		CREATE FUNCTION public.empty_acl_function() RETURNS integer
			LANGUAGE sql AS 'SELECT 1';

		REVOKE ALL ON TABLE public.empty_acl_view FROM PUBLIC, postgres;
		REVOKE ALL ON TABLE public.empty_acl_snapshot FROM PUBLIC, postgres;
		REVOKE ALL ON FUNCTION public.empty_acl_function() FROM PUBLIC, postgres;
	`)
	require.NoError(t, err)

	objects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, excludedCatalogSchemas())
	require.NoError(t, err)
	objectMap, err := buildObjectMapFromObjects(objects)
	require.NoError(t, err)

	view := objectMap[schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "empty_acl_view"}].Payload.(schema.View)
	snapshot := objectMap[schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "empty_acl_snapshot"}].Payload.(schema.View)
	function := objectMap[schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "empty_acl_function", Signature: "()"}].Payload.(schema.Function)
	for name, grants := range map[string][]schema.Grant{
		"regular view":      view.Grants,
		"materialized view": snapshot.Grants,
		"function":          function.Grants,
	} {
		require.NotNil(t, grants, "%s explicit empty ACL must not become unmanaged", name)
		require.Empty(t, grants, "%s ACL must remain empty", name)
	}

	generator := planner.NewDDLGenerator()
	viewDDL, err := generator.GenerateCreateStatement(view)
	require.NoError(t, err)
	snapshotDDL, err := generator.GenerateCreateStatement(snapshot)
	require.NoError(t, err)
	functionDDL, err := generator.GenerateCreateStatement(function)
	require.NoError(t, err)
	assert.Contains(t, viewDDL, `REVOKE ALL ON TABLE "public"."empty_acl_view" FROM PUBLIC;`)
	assert.Contains(t, viewDDL, `REVOKE ALL ON TABLE "public"."empty_acl_view" FROM "postgres";`)
	assert.Contains(t, snapshotDDL, `REVOKE ALL ON TABLE "public"."empty_acl_snapshot" FROM PUBLIC;`)
	assert.Contains(t, snapshotDDL, `REVOKE ALL ON TABLE "public"."empty_acl_snapshot" FROM "postgres";`)
	assert.Contains(t, functionDDL, `REVOKE ALL ON FUNCTION "public"."empty_acl_function"() FROM PUBLIC;`)
	assert.Contains(t, functionDDL, `REVOKE ALL ON FUNCTION "public"."empty_acl_function"() FROM "postgres";`)

	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, strings.Join([]string{viewDDL, snapshotDDL, functionDDL}, "\n\n"))
	require.NoError(t, err)

	for _, relation := range []string{"empty_acl_view", "empty_acl_snapshot"} {
		var isExplicit, isEmpty bool
		err := pool.QueryRow(ctx, `
			SELECT c.relacl IS NOT NULL, COALESCE(cardinality(c.relacl), -1) = 0
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'public' AND c.relname = $1
		`, relation).Scan(&isExplicit, &isEmpty)
		require.NoError(t, err)
		assert.True(t, isExplicit, "%s ACL must be explicit after replay", relation)
		assert.True(t, isEmpty, "%s ACL must have zero entries after replay", relation)
	}

	var functionACLIsExplicit, functionACLIsEmpty bool
	err = pool.QueryRow(ctx, `
		SELECT p.proacl IS NOT NULL, COALESCE(cardinality(p.proacl), -1) = 0
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public' AND p.proname = 'empty_acl_function' AND p.pronargs = 0
	`).Scan(&functionACLIsExplicit, &functionACLIsEmpty)
	require.NoError(t, err)
	assert.True(t, functionACLIsExplicit)
	assert.True(t, functionACLIsEmpty)
}

func TestCatalogRejectsUnmodeledViewColumnMetadata(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	t.Cleanup(func() { _ = resetPublicSchema(context.Background(), pool) })

	tests := []struct {
		name     string
		setupSQL string
		identity string
		want     string
	}{
		{
			name: "regular view column ACL",
			setupSQL: `
				CREATE TABLE public.base (id integer);
				CREATE VIEW public.column_acl_view AS SELECT id FROM public.base;
				GRANT SELECT (id) ON TABLE public.column_acl_view TO PUBLIC;
			`,
			identity: "public.column_acl_view.id",
			want:     "column-level ACL",
		},
		{
			name: "materialized view column ACL",
			setupSQL: `
				CREATE TABLE public.base (id integer);
				CREATE MATERIALIZED VIEW public.column_acl_snapshot AS SELECT id FROM public.base;
				GRANT SELECT (id) ON TABLE public.column_acl_snapshot TO PUBLIC;
			`,
			identity: "public.column_acl_snapshot.id",
			want:     "column-level ACL",
		},
		{
			name: "regular view column comment",
			setupSQL: `
				CREATE TABLE public.base (id integer);
				CREATE VIEW public.commented_view AS SELECT id FROM public.base;
				COMMENT ON COLUMN public.commented_view.id IS 'view output';
			`,
			identity: "public.commented_view.id",
			want:     "has a comment",
		},
		{
			name: "materialized view column comment",
			setupSQL: `
				CREATE TABLE public.base (id integer);
				CREATE MATERIALIZED VIEW public.commented_snapshot AS SELECT id FROM public.base;
				COMMENT ON COLUMN public.commented_snapshot.id IS 'snapshot output';
			`,
			identity: "public.commented_snapshot.id",
			want:     "has a comment",
		},
		{
			name: "regular view column default",
			setupSQL: `
				CREATE TABLE public.base (id integer);
				CREATE VIEW public.defaulted_view AS SELECT id FROM public.base;
				ALTER VIEW public.defaulted_view ALTER COLUMN id SET DEFAULT 42;
			`,
			identity: "public.defaulted_view.id",
			want:     "has a default",
		},
		{
			name: "materialized view statistics target",
			setupSQL: `
				CREATE MATERIALIZED VIEW public.statistics_snapshot AS SELECT 'value'::text AS value;
				ALTER MATERIALIZED VIEW public.statistics_snapshot ALTER COLUMN value SET STATISTICS 321;
			`,
			identity: "public.statistics_snapshot.value",
			want:     "has a statistics target",
		},
		{
			name: "materialized view storage",
			setupSQL: `
				CREATE MATERIALIZED VIEW public.storage_snapshot AS SELECT 'value'::text AS value;
				ALTER MATERIALIZED VIEW public.storage_snapshot ALTER COLUMN value SET STORAGE EXTERNAL;
			`,
			identity: "public.storage_snapshot.value",
			want:     "has non-default storage",
		},
		{
			name: "materialized view attribute options",
			setupSQL: `
				CREATE MATERIALIZED VIEW public.options_snapshot AS SELECT 'value'::text AS value;
				ALTER MATERIALIZED VIEW public.options_snapshot ALTER COLUMN value SET (n_distinct = 0.25);
			`,
			identity: "public.options_snapshot.value",
			want:     "has attribute options",
		},
		{
			name: "materialized view compression",
			setupSQL: `
				CREATE MATERIALIZED VIEW public.compression_snapshot AS SELECT 'value'::text AS value;
				ALTER MATERIALIZED VIEW public.compression_snapshot ALTER COLUMN value SET COMPRESSION pglz;
			`,
			identity: "public.compression_snapshot.value",
			want:     "has compression metadata",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, resetPublicSchema(ctx, pool))
			_, err := pool.Exec(ctx, test.setupSQL)
			require.NoError(t, err)

			_, err = db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, excludedCatalogSchemas())
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.identity)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestStructuralViewReplacementPreservesUnmanagedOwnerAndACL(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL
	pool, err := db.Connect(ctx, &config.DBConnection{URL: &dbURL})
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	const ownerRole = "schemata_view_preserved_owner"
	const readerRole = "schemata_view_preserved_reader"
	require.NoError(t, resetPublicSchema(ctx, pool))
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		DROP ROLE IF EXISTS %s;
		DROP ROLE IF EXISTS %s;
		CREATE ROLE %s;
		CREATE ROLE %s;
	`, ownerRole, readerRole, ownerRole, readerRole))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = resetPublicSchema(context.Background(), pool)
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`
			DROP ROLE IF EXISTS %s;
			DROP ROLE IF EXISTS %s;
		`, ownerRole, readerRole))
	})

	tests := []struct {
		name       string
		objectName string
		keyword    string
		drop       string
	}{
		{name: "regular view", objectName: "preserved_view", keyword: "VIEW", drop: "DROP VIEW"},
		{name: "materialized view", objectName: "preserved_snapshot", keyword: "MATERIALIZED VIEW", drop: "DROP MATERIALIZED VIEW"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, resetPublicSchema(ctx, pool))
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				CREATE TABLE public.base (id integer);
				GRANT SELECT ON TABLE public.base TO %s;
				CREATE %s public.%s AS SELECT id FROM public.base;
				ALTER %s public.%s OWNER TO %s;
				REVOKE ALL ON TABLE public.%s FROM PUBLIC, %s;
				GRANT SELECT ON TABLE public.%s TO %s WITH GRANT OPTION;
			`, ownerRole, test.keyword, test.objectName, test.keyword, test.objectName, ownerRole,
				test.objectName, ownerRole, test.objectName, readerRole))
			require.NoError(t, err)

			actualObjects, err := db.NewCatalog(pool).ExtractAllObjects(ctx, []string{"public"}, excludedCatalogSchemas())
			require.NoError(t, err)
			actual, err := buildObjectMapFromObjects(actualObjects)
			require.NoError(t, err)

			desired, err := parser.NewParser().ParseSQL(fmt.Sprintf(`
				CREATE TABLE public.base (id integer);
				CREATE %s public.%s AS SELECT id + 1 AS id FROM public.base;
			`, test.keyword, test.objectName))
			require.NoError(t, err)
			desiredView := desired[schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: test.objectName}].Payload.(schema.View)
			require.Nil(t, desiredView.Owner)
			require.Nil(t, desiredView.Grants)

			diff, err := differ.NewDiffer().Diff(desired, actual)
			require.NoError(t, err)
			require.Len(t, diff.ToAlter, 1)
			generated, err := planner.NewDDLGenerator().GenerateDDL(diff, desired, actual)
			require.NoError(t, err)
			assert.Contains(t, generated, test.drop+` IF EXISTS "public"."`+test.objectName+`";`)
			assert.Contains(t, generated, `ALTER `+test.keyword+` "public"."`+test.objectName+`" OWNER TO "`+ownerRole+`";`)
			assert.Contains(t, generated, `GRANT SELECT ON TABLE "public"."`+test.objectName+`" TO "`+readerRole+`" WITH GRANT OPTION;`)

			_, err = pool.Exec(ctx, generated)
			require.NoError(t, err, "replacement DDL must execute:\n%s", generated)

			var owner string
			err = pool.QueryRow(ctx, `
				SELECT pg_get_userbyid(c.relowner)
				FROM pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = 'public' AND c.relname = $1
			`, test.objectName).Scan(&owner)
			require.NoError(t, err)
			assert.Equal(t, ownerRole, owner)

			var totalACLRows, exactACLRows int
			err = pool.QueryRow(ctx, `
				SELECT
					count(priv.grantee),
					count(priv.grantee) FILTER (
						WHERE grantee.rolname = $2
						  AND priv.privilege_type = 'SELECT'
						  AND priv.is_grantable
						  AND priv.grantor = c.relowner
					)
				FROM pg_class c
				JOIN pg_namespace n ON n.oid = c.relnamespace
				LEFT JOIN LATERAL aclexplode(COALESCE(c.relacl, ARRAY[]::aclitem[])) AS priv ON true
				LEFT JOIN pg_roles grantee ON grantee.oid = priv.grantee
				WHERE n.nspname = 'public' AND c.relname = $1
				GROUP BY c.relowner
			`, test.objectName, readerRole).Scan(&totalACLRows, &exactACLRows)
			require.NoError(t, err)
			assert.Equal(t, 1, totalACLRows, "replacement must preserve the exact ACL without restoring owner defaults")
			assert.Equal(t, 1, exactACLRows, "reader grant and grant option must survive replacement")
		})
	}
}

func excludedCatalogSchemas() []string {
	return []string{"pg_catalog", "information_schema", "pg_toast", "schemata"}
}
