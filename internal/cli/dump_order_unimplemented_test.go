//go:build orderfail

package cli

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

// These tests intentionally FAIL until per-schema dump orders output files by dependency
// graph (parent/referenced schema before child/dependent), not lexicographic schema name.
//
// Run (expect RED):
//
//	go test -tags=orderfail ./internal/cli/... -run TestDumpOrdering -count=1
//
// Default CI / go test ./... does not include this build tag, so the suite stays green.

func TestDumpOrdering_FKReferencedSchemaMustSortBeforeReferencing(t *testing.T) {
	// Parent in zeta, child FK in alpha. Correct apply order: zeta.sql then alpha.sql.
	// sortedSchemaNames uses lexicographic order: alpha before zeta — wrong for apply.
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"alpha": nil,
		"zeta":  nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"zeta", "alpha"}
	require.Equal(t, want, got, "dependency order for cross-schema FK: parent schema file first")
}

func TestDumpOrdering_ViewBaseSchemaMustSortBeforeViewSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"aaa": nil,
		"zzz": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"zzz", "aaa"}
	require.Equal(t, want, got, "dependency order for view: base table schema before view schema")
}

func TestDumpOrdering_FunctionReferencedSchemaMustSortBeforeDefiningSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"fn_a": nil,
		"fn_z": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"fn_z", "fn_a"}
	require.Equal(t, want, got, "dependency order: body references fn_z before fn_a defines function")
}

func TestDumpOrdering_TypeDefiningSchemaMustSortBeforeUsingSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"ty_a": nil,
		"ty_z": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"ty_z", "ty_a"}
	require.Equal(t, want, got, "dependency order: type in ty_z before table in ty_a")
}

func TestDumpOrdering_TriggerFunctionSchemaMustSortBeforeTriggerSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"tr_a": nil,
		"tr_z": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"tr_z", "tr_a"}
	require.Equal(t, want, got, "dependency order: trigger function in tr_z before trigger on tr_a")
}

func TestDumpOrdering_ExtensionPublicMustSortBeforeSchemaUsingExtensionTypes(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"early":  nil,
		"public": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"public", "early"}
	require.Equal(t, want, got, "dependency order: CREATE EXTENSION (public) before tables using citext etc.")
}
