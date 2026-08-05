package parser

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGrantPreservesPublicAndQuotedPublicRole(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		GRANT SELECT ON TABLE public.items TO PUBLIC, "PUBLIC";
	`)
	require.NoError(t, err)

	payload := objects[schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "items"}].Payload
	table := payload.(schema.Table)
	require.Len(t, table.Grants, 2)
	assert.Contains(t, table.Grants, schema.Grant{Grantee: schema.PublicGrantee(), Privileges: []schema.Privilege{schema.PrivSelect}})
	assert.Contains(t, table.Grants, schema.Grant{Grantee: schema.RoleGrantee("PUBLIC"), Privileges: []schema.Privilege{schema.PrivSelect}})
}

func TestFunctionMetadataUsesExactOverloadIdentity(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE FUNCTION public.convert_value(value integer) RETURNS integer LANGUAGE sql AS 'SELECT value';
		CREATE FUNCTION public.convert_value(value text) RETURNS text LANGUAGE sql AS 'SELECT value';
		GRANT EXECUTE ON FUNCTION public.convert_value(integer) TO app_reader;
		ALTER FUNCTION public.convert_value(text) OWNER TO app_owner;
	`)
	require.NoError(t, err)

	integerFn := objects[schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "convert_value", Signature: "(integer)"}].Payload.(schema.Function)
	textFn := objects[schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "convert_value", Signature: "(text)"}].Payload.(schema.Function)
	require.Equal(t, []schema.Grant{{Grantee: schema.RoleGrantee("app_reader"), Privileges: []schema.Privilege{schema.PrivExecute}}}, integerFn.Grants)
	require.Nil(t, integerFn.Owner)
	require.NotNil(t, textFn.Owner)
	assert.Equal(t, "app_owner", *textFn.Owner)
	assert.Empty(t, textFn.Grants)
}

func TestGrantOnTableCanAttachViewButNotWrongObjectFamily(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE VIEW public.item_ids AS SELECT 1 AS id;
		GRANT SELECT ON TABLE public.item_ids TO report_reader;
	`)
	require.NoError(t, err)
	view := objects[schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "item_ids"}].Payload.(schema.View)
	require.Equal(t, []schema.Grant{{Grantee: schema.RoleGrantee("report_reader"), Privileges: []schema.Privilege{schema.PrivSelect}}}, view.Grants)
}

func TestTypeGrantAndOwnerMetadataAreAttached(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE TYPE public.mood AS ENUM ('ok', 'great');
		ALTER TYPE public.mood OWNER TO type_owner;
		GRANT USAGE ON TYPE public.mood TO app_reader;
	`)
	require.NoError(t, err)
	enum := objects[schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "mood"}].Payload.(schema.EnumDef)
	require.NotNil(t, enum.Owner)
	assert.Equal(t, "type_owner", *enum.Owner)
	assert.Equal(t, []schema.Grant{{Grantee: schema.RoleGrantee("app_reader"), Privileges: []schema.Privilege{schema.PrivUsage}}}, enum.Grants)
}

func TestCommentsAttachByExactObjectIdentity(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		CREATE SEQUENCE public.item_seq;
		CREATE VIEW public.item_view AS SELECT id FROM public.items;
		CREATE MATERIALIZED VIEW public.item_snapshot AS SELECT id FROM public.items;
		CREATE FUNCTION public.show_value(value integer) RETURNS integer LANGUAGE sql AS 'SELECT value';
		CREATE TYPE public.mood AS ENUM ('ok');
		COMMENT ON TABLE public.items IS '';
		COMMENT ON SEQUENCE public.item_seq IS 'sequence';
		COMMENT ON VIEW public.item_view IS 'view';
		COMMENT ON MATERIALIZED VIEW public.item_snapshot IS 'snapshot';
		COMMENT ON FUNCTION public.show_value(integer) IS 'function';
		COMMENT ON TYPE public.mood IS 'type';
	`)
	require.NoError(t, err)

	table := objects[schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "items"}].Payload.(schema.Table)
	sequence := objects[schema.ObjectKey{Kind: schema.SequenceKind, Schema: "public", Name: "item_seq"}].Payload.(schema.Sequence)
	view := objects[schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "item_view"}].Payload.(schema.View)
	snapshot := objects[schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "item_snapshot"}].Payload.(schema.View)
	function := objects[schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "show_value", Signature: "(integer)"}].Payload.(schema.Function)
	enum := objects[schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "mood"}].Payload.(schema.EnumDef)

	require.NotNil(t, table.Comment)
	assert.Empty(t, *table.Comment, "empty string comments must not collapse to COMMENT ... IS NULL")
	assert.Equal(t, "sequence", *sequence.Comment)
	assert.Equal(t, "view", *view.Comment)
	assert.Equal(t, "snapshot", *snapshot.Comment)
	assert.Equal(t, "function", *function.Comment)
	assert.Equal(t, "type", *enum.Comment)
}

func TestMetadataStatementsFailClosed(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"unmatched grant":         `GRANT SELECT ON TABLE public.missing TO app;`,
		"unmatched owner":         `ALTER TABLE public.missing OWNER TO app;`,
		"wrong overload":          `CREATE FUNCTION f(integer) RETURNS integer LANGUAGE sql AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION f(text) TO app;`,
		"column privilege":        `CREATE TABLE t (id integer); GRANT SELECT (id) ON TABLE t TO app;`,
		"explicit grantor":        `CREATE TABLE t (id integer); GRANT SELECT ON TABLE t TO app GRANTED BY grantor;`,
		"version dependent all":   `CREATE TABLE t (id integer); GRANT ALL ON TABLE t TO app;`,
		"context dependent owner": `CREATE TABLE t (id integer); ALTER TABLE t OWNER TO CURRENT_USER;`,
		"all tables in schema":    `GRANT SELECT ON ALL TABLES IN SCHEMA public TO app;`,
	}
	for name, sql := range tests {
		name, sql := name, sql
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewParser().ParseSQL(sql)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "grant/owner metadata")
		})
	}
}

func TestMaintainPrivilegeIsModeledExplicitly(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		GRANT MAINTAIN ON TABLE public.items TO maintenance_worker;
	`)
	require.NoError(t, err)
	table := objects[schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "items"}].Payload.(schema.Table)
	assert.Equal(t, []schema.Grant{{
		Grantee: schema.RoleGrantee("maintenance_worker"), Privileges: []schema.Privilege{schema.PrivMaintain},
	}}, table.Grants)
}

func TestRevokeAllRemovesExplicitMaintainPrivilege(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		GRANT MAINTAIN ON TABLE public.items TO maintenance_worker;
		REVOKE ALL ON TABLE public.items FROM maintenance_worker;
	`)
	require.NoError(t, err)
	table := objects[schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "items"}].Payload.(schema.Table)
	assert.NotNil(t, table.Grants)
	assert.Empty(t, table.Grants)
}

func TestRevokeDeclaresManagedEmptyACL(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE FUNCTION public.run_task() RETURNS integer LANGUAGE sql AS 'SELECT 1';
		REVOKE EXECUTE ON FUNCTION public.run_task() FROM PUBLIC;
	`)
	require.NoError(t, err)
	function := objects[schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "run_task", Signature: "()"}].Payload.(schema.Function)
	assert.NotNil(t, function.Grants)
	assert.Empty(t, function.Grants)
}

func TestRevokeGrantOptionDowngradesPrivilege(t *testing.T) {
	t.Parallel()

	objects, err := NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		GRANT SELECT, UPDATE ON TABLE public.items TO app WITH GRANT OPTION;
		REVOKE GRANT OPTION FOR SELECT ON TABLE public.items FROM app;
	`)
	require.NoError(t, err)
	table := objects[schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "items"}].Payload.(schema.Table)
	assert.Equal(t, []schema.Grant{
		{Grantee: schema.RoleGrantee("app"), Privileges: []schema.Privilege{schema.PrivSelect}},
		{Grantee: schema.RoleGrantee("app"), Privileges: []schema.Privilege{schema.PrivUpdate}, Grantable: true},
	}, schema.CanonicalizeGrants(table.Grants))
}

func TestRevokeCascadeFailsClosed(t *testing.T) {
	t.Parallel()

	_, err := NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		REVOKE SELECT ON TABLE public.items FROM app_reader CASCADE;
	`)
	require.ErrorContains(t, err, "REVOKE CASCADE is not modeled")

	_, err = NewParser().ParseSQL(`
		CREATE TABLE public.items (id integer);
		REVOKE SELECT ON TABLE public.items FROM app_reader RESTRICT;
	`)
	require.NoError(t, err, "explicit RESTRICT is equivalent to the generated default")
}

func TestNewlyModeledObjectFamiliesRejectUnrepresentableVariants(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"composite collation":       `CREATE TYPE public.pair AS (label text COLLATE "C");`,
		"unlogged sequence":         `CREATE UNLOGGED SEQUENCE public.ids;`,
		"materialized view options": `CREATE MATERIALIZED VIEW public.snapshot WITH (fillfactor=80) AS SELECT 1 AS id;`,
		"view check option":         `CREATE VIEW public.checked AS SELECT 1 AS id WITH LOCAL CHECK OPTION;`,
	}
	for name, sql := range tests {
		name, sql := name, sql
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewParser().ParseSQL(sql)
			require.Error(t, err)
		})
	}
}
