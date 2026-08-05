package planner

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/require"
)

func TestRenderFunctionPlacesTableArgumentsInReturnsClause(t *testing.T) {
	valueName := "value"
	resultName := "Odd Result"
	observedName := "observed_at"
	function := schema.Function{
		Schema: "public",
		Name:   "table_probe",
		Args: []schema.FunctionArg{
			{Mode: schema.InMode, Name: &valueName, Type: "integer"},
			{Mode: schema.TableMode, Name: &resultName, Type: `"Odd Schema"."Odd Type"[]`},
			{Mode: schema.TableMode, Name: &observedName, Type: "timestamp with time zone"},
		},
		Returns: schema.ReturnsTable{Columns: []schema.TableColumn{
			{Name: resultName, Type: `"Odd Schema"."Odd Type"[]`},
			{Name: observedName, Type: "timestamp with time zone"},
		}},
		Language:   schema.SQL,
		Volatility: schema.Volatile,
		Parallel:   schema.ParallelUnsafe,
		Body:       "SELECT NULL, now()",
	}

	statement, err := NewDDLGenerator().GenerateCreateStatement(function)
	require.NoError(t, err)
	require.Contains(t, statement, `CREATE FUNCTION "public"."table_probe"("value" integer)`)
	require.Contains(t, statement, `RETURNS TABLE ("Odd Result" "Odd Schema"."Odd Type"[], "observed_at" timestamp with time zone)`)
	_, err = pg_query.Parse(statement)
	require.NoError(t, err, "generated TABLE function must be valid PostgreSQL syntax:\n%s", statement)
}

func TestRenderFunctionPreservesExplicitEmptySearchPath(t *testing.T) {
	function := schema.Function{
		Schema:     "public",
		Name:       "empty_path",
		Returns:    schema.ReturnsType{Type: "integer"},
		Language:   schema.SQL,
		Volatility: schema.Volatile,
		Parallel:   schema.ParallelUnsafe,
		SearchPath: make([]schema.SchemaName, 0),
		Body:       "SELECT 1",
	}

	statement, err := NewDDLGenerator().GenerateCreateStatement(function)
	require.NoError(t, err)
	require.Contains(t, statement, "SET search_path TO ''")
	_, err = pg_query.Parse(statement)
	require.NoError(t, err)
}
