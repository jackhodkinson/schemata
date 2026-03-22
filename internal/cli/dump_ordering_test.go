package cli

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestDumpOrdering_FKReferencedSchemaMustSortBeforeReferencing(t *testing.T) {
	// Build realistic groups: alpha.child has a FK to zeta.parent.
	// A dependency-aware schema sorter should place zeta before alpha.
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"alpha": {
			schema.Table{
				Schema: "alpha",
				Name:   "child",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "parent_id", Type: "INTEGER", NotNull: true},
				},
				PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "child_parent_fk",
						Cols: []schema.ColumnName{"parent_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "zeta",
							Table:  "parent",
							Cols:   []schema.ColumnName{"id"},
						},
						OnDelete: schema.NoAction,
						OnUpdate: schema.NoAction,
					},
				},
			},
		},
		"zeta": {
			schema.Table{
				Schema: "zeta",
				Name:   "parent",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
				PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
			},
		},
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"zeta", "alpha"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_ViewBaseSchemaMustSortBeforeViewSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"aaa": nil,
		"zzz": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"zzz", "aaa"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_FunctionReferencedSchemaMustSortBeforeDefiningSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"fn_a": nil,
		"fn_z": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"fn_z", "fn_a"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_TypeDefiningSchemaMustSortBeforeUsingSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"ty_a": nil,
		"ty_z": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"ty_z", "ty_a"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_TriggerFunctionSchemaMustSortBeforeTriggerSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"tr_a": nil,
		"tr_z": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"tr_z", "tr_a"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_ExtensionPublicMustSortBeforeSchemaUsingExtensionTypes(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"early":  nil,
		"public": nil,
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"public", "early"}
	require.Equal(t, want, got)
}
