package parser

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestParseFunctionPreservesStructuredIdentityDefaultsAndConfig(t *testing.T) {
	objects, err := NewParser().ParseSQL(`
		CREATE FUNCTION public.identity_probe(
			timestamp with time zone,
			named "Odd Schema"."Odd Type"[],
			payload text DEFAULT 'a,b'::text
		)
		RETURNS SETOF text
		LANGUAGE sql
		SECURITY DEFINER
		PARALLEL SAFE
		SET search_path TO "$user", public, "Odd Schema"
		AS 'SELECT payload';
	`)
	require.NoError(t, err)

	key := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "identity_probe",
		Signature: `(timestamp with time zone,"Odd Schema"."Odd Type"[],text)`,
	}
	hashed, ok := objects[key]
	require.True(t, ok, "function key should retain exact overload identity: %#v", objects)
	function := hashed.Payload.(schema.Function)
	require.Len(t, function.Args, 3)
	require.Nil(t, function.Args[0].Name)
	require.Equal(t, schema.TypeName("timestamp with time zone"), function.Args[0].Type)
	require.Equal(t, "named", *function.Args[1].Name)
	require.Equal(t, schema.TypeName(`"Odd Schema"."Odd Type"[]`), function.Args[1].Type)
	require.Equal(t, schema.Expr("'a,b'"), *function.Args[2].Default)
	require.Equal(t, schema.ReturnsSetOf{Type: "text"}, function.Returns)
	require.True(t, function.SecurityDefiner)
	require.Equal(t, schema.ParallelSafe, function.Parallel)
	require.Equal(t, []schema.SchemaName{"$user", "public", "Odd Schema"}, function.SearchPath)
}

func TestParseFunctionPreservesTableAndOutputModes(t *testing.T) {
	objects, err := NewParser().ParseSQL(`
		CREATE FUNCTION public.table_probe(value integer)
		RETURNS TABLE (
			"Odd Result" "Odd Schema"."Odd Type"[],
			observed_at timestamp with time zone
		)
		LANGUAGE sql AS 'SELECT NULL, now()';

		CREATE FUNCTION public.output_probe(
			IN seed integer,
			INOUT accumulator integer DEFAULT 1,
			OUT rendered text
		)
		LANGUAGE plpgsql AS 'BEGIN rendered := accumulator::text; END';
	`)
	require.NoError(t, err)

	tableKey := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "table_probe", Signature: "(integer)"}
	tableFunction := objects[tableKey].Payload.(schema.Function)
	require.Equal(t, []schema.ArgMode{schema.InMode, schema.TableMode, schema.TableMode}, []schema.ArgMode{
		tableFunction.Args[0].Mode,
		tableFunction.Args[1].Mode,
		tableFunction.Args[2].Mode,
	})
	require.Equal(t, schema.ReturnsTable{Columns: []schema.TableColumn{
		{Name: "Odd Result", Type: `"Odd Schema"."Odd Type"[]`},
		{Name: "observed_at", Type: "timestamp with time zone"},
	}}, tableFunction.Returns)

	outputKey := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "output_probe", Signature: "(integer,integer)"}
	outputFunction := objects[outputKey].Payload.(schema.Function)
	require.Equal(t, []schema.ArgMode{schema.InMode, schema.InOutMode, schema.OutMode}, []schema.ArgMode{
		outputFunction.Args[0].Mode,
		outputFunction.Args[1].Mode,
		outputFunction.Args[2].Mode,
	})
	require.Equal(t, schema.ReturnsType{Type: "record"}, outputFunction.Returns)
	require.Equal(t, schema.Expr("1"), *outputFunction.Args[1].Default)
}

func TestParseFunctionRejectsUnmodeledConfiguration(t *testing.T) {
	_, err := NewParser().ParseSQL(`
		CREATE FUNCTION public.unsafe_config() RETURNS integer
		LANGUAGE sql
		SECURITY DEFINER
		SET statement_timeout TO '1s'
		AS 'SELECT 1';
	`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `configuration key "statement_timeout" is not modeled`)
}

func TestParseFunctionPreservesExplicitEmptySearchPath(t *testing.T) {
	objects, err := NewParser().ParseSQL(`
		CREATE FUNCTION public.empty_path() RETURNS integer
		LANGUAGE sql SET search_path TO '' AS 'SELECT 1';
	`)
	require.NoError(t, err)
	key := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "empty_path", Signature: "()"}
	function := objects[key].Payload.(schema.Function)
	require.NotNil(t, function.SearchPath)
	require.Empty(t, function.SearchPath)
}
