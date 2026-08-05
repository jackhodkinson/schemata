package db

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestExtractFunctionBody(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "typical pg_get_functiondef output",
			input: `CREATE OR REPLACE FUNCTION public.update_updated_at_column()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$

BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;

$function$
`,
			expected: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
		},
		{
			name: "function with $$ delimiter",
			input: `CREATE OR REPLACE FUNCTION public.test()
 RETURNS void
 LANGUAGE plpgsql
AS $$
BEGIN
    RETURN;
END;
$$`,
			expected: `BEGIN
    RETURN;
END;`,
		},
		{
			name: "function with custom tag",
			input: `CREATE OR REPLACE FUNCTION public.test()
 RETURNS void
 LANGUAGE plpgsql
AS $body$
SELECT 1;
$body$`,
			expected: `SELECT 1;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractFunctionBody(tt.input)
			if err != nil {
				t.Fatalf("extractFunctionBody() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("extractFunctionBody() =\n%q\n\nwant:\n%q", result, tt.expected)
			}
		})
	}
}

func TestExtractFunctionBodyFailsClosed(t *testing.T) {
	for _, input := range []string{
		"not valid SQL",
		"SELECT 1",
		"CREATE FUNCTION public.f() RETURNS void LANGUAGE sql",
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := extractFunctionBody(input); err == nil {
				t.Fatal("extractFunctionBody() unexpectedly succeeded")
			}
		})
	}
}

func TestExtractFunctionBodyRejectsMultipartAS(t *testing.T) {
	_, err := extractFunctionBody(`
		CREATE FUNCTION public.f(integer) RETURNS integer
		LANGUAGE c AS 'library', 'symbol'
	`)
	require.ErrorContains(t, err, "multi-part AS bodies")
}

func TestValidateCatalogFunctionAttributes(t *testing.T) {
	require.NoError(t, validateCatalogFunctionAttributes("sql", false, false, 100, 0, false, false))
	require.NoError(t, validateCatalogFunctionAttributes("c", true, false, 1, 1000, false, false))

	for _, test := range []struct {
		name          string
		language      string
		returnsSet    bool
		leakproof     bool
		cost          float32
		rows          float32
		hasSupport    bool
		hasTransforms bool
		want          string
	}{
		{name: "leakproof", language: "sql", cost: 100, leakproof: true, want: "LEAKPROOF"},
		{name: "cost", language: "sql", cost: 7, want: "COST"},
		{name: "rows", language: "sql", returnsSet: true, cost: 100, rows: 7, want: "ROWS"},
		{name: "support", language: "sql", cost: 100, hasSupport: true, want: "SUPPORT"},
		{name: "transform", language: "sql", cost: 100, hasTransforms: true, want: "TRANSFORM"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateCatalogFunctionAttributes(test.language, test.returnsSet, test.leakproof, test.cost, test.rows, test.hasSupport, test.hasTransforms)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestParseCatalogFunctionArgumentsUsesStructuredMetadata(t *testing.T) {
	encoded := `[
		{"mode":"i","name":null,"type":"timestamp with time zone"},
		{"mode":"i","name":"typed value","type":"\"Odd Schema\".\"Odd Type\"[]"},
		{"mode":"o","name":"result","type":"text"},
		{"mode":"b","name":"state","type":"integer"},
		{"mode":"v","name":"items","type":"text[]"},
		{"mode":"t","name":"table result","type":"timestamp without time zone"}
	]`

	args, err := parseCatalogFunctionArguments(encoded)
	require.NoError(t, err)
	require.Equal(t, []schema.FunctionArg{
		{Mode: schema.InMode, Type: "timestamp with time zone"},
		{Mode: schema.InMode, Name: stringPointer("typed value"), Type: `"Odd Schema"."Odd Type"[]`},
		{Mode: schema.OutMode, Name: stringPointer("result"), Type: "text"},
		{Mode: schema.InOutMode, Name: stringPointer("state"), Type: "integer"},
		{Mode: schema.VariadicMode, Name: stringPointer("items"), Type: "text[]"},
		{Mode: schema.TableMode, Name: stringPointer("table result"), Type: "timestamp without time zone"},
	}, args)
}

func TestParseCatalogFunctionArgumentsFailsClosed(t *testing.T) {
	for _, encoded := range []string{
		`not json`,
		`[{"mode":"x","name":null,"type":"integer"}]`,
		`[{"mode":"i","name":null,"type":""}]`,
	} {
		t.Run(encoded, func(t *testing.T) {
			_, err := parseCatalogFunctionArguments(encoded)
			require.Error(t, err)
		})
	}
}

func TestAttachFunctionArgumentDefaultsUsesExpressionAST(t *testing.T) {
	args := []schema.FunctionArg{
		{Mode: schema.InMode, Name: stringPointer("required"), Type: "integer"},
		{Mode: schema.OutMode, Name: stringPointer("output"), Type: "text"},
		{Mode: schema.InOutMode, Name: stringPointer("flags"), Type: "integer[]"},
		{Mode: schema.InMode, Name: stringPointer("payload"), Type: "text"},
	}

	err := attachFunctionArgumentDefaults(args, `ARRAY[1, 2], 'a,b'::text`, 2)
	require.NoError(t, err)
	require.Nil(t, args[0].Default)
	require.Nil(t, args[1].Default)
	require.Equal(t, schema.Expr("ARRAY[1, 2]"), *args[2].Default)
	require.Equal(t, schema.Expr("'a,b'::text"), *args[3].Default)
}

func TestAttachFunctionArgumentDefaultsFailsClosedOnCatalogMismatch(t *testing.T) {
	args := []schema.FunctionArg{{Mode: schema.InMode, Type: "integer"}}
	for _, test := range []struct {
		defaults string
		count    int
	}{
		{defaults: "1", count: 0},
		{defaults: "", count: 1},
		{defaults: "1, 2", count: 1},
		{defaults: "1, 2", count: 2},
	} {
		require.Error(t, attachFunctionArgumentDefaults(args, test.defaults, test.count))
	}
}

func TestFunctionReturnFromCatalogUsesArgumentModes(t *testing.T) {
	args := []schema.FunctionArg{
		{Mode: schema.InMode, Type: "integer"},
		{Mode: schema.TableMode, Name: stringPointer("value"), Type: `"Odd Schema"."Odd Type"[]`},
		{Mode: schema.TableMode, Name: stringPointer("observed_at"), Type: "timestamp with time zone"},
	}
	result, err := functionReturnFromCatalog(args, "record", true)
	require.NoError(t, err)
	require.Equal(t, schema.ReturnsTable{Columns: []schema.TableColumn{
		{Name: "value", Type: `"Odd Schema"."Odd Type"[]`},
		{Name: "observed_at", Type: "timestamp with time zone"},
	}}, result)

	result, err = functionReturnFromCatalog(nil, "integer", true)
	require.NoError(t, err)
	require.Equal(t, schema.ReturnsSetOf{Type: "integer"}, result)
}

func TestParseFunctionConfigPreservesSearchPath(t *testing.T) {
	path, err := parseFunctionConfig(`null`)
	require.NoError(t, err)
	require.Nil(t, path)

	path, err = parseFunctionConfig(`["search_path=\"$user\", public, \"Odd, Schema\""]`)
	require.NoError(t, err)
	require.Equal(t, []schema.SchemaName{"$user", "public", "Odd, Schema"}, path)

	path, err = parseFunctionConfig(`["search_path=\"\""]`)
	require.NoError(t, err)
	require.NotNil(t, path)
	require.Empty(t, path)
}

func TestParseFunctionConfigFailsClosedForUnmodeledOrMalformedSettings(t *testing.T) {
	for _, encoded := range []string{
		`[]`,
		`["statement_timeout=1s"]`,
		`["search_path=public","search_path=private"]`,
		`["search_path=\"unterminated"]`,
		`["missing_equals"]`,
	} {
		t.Run(encoded, func(t *testing.T) {
			_, err := parseFunctionConfig(encoded)
			require.Error(t, err)
		})
	}
}

func stringPointer(value string) *string { return &value }
