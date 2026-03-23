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
		"aaa": {
			schema.View{
				Schema: "aaa",
				Name:   "v_base",
				Type:   schema.RegularView,
				Definition: schema.ViewDefinition{
					Query: "SELECT id FROM zzz.base",
					Dependencies: []schema.ObjectReference{
						{Kind: schema.TableKind, Schema: "zzz", Name: "base"},
					},
				},
			},
		},
		"zzz": {
			schema.Table{
				Schema: "zzz",
				Name:   "base",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
				PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
			},
		},
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"zzz", "aaa"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_FunctionReferencedSchemaMustSortBeforeDefiningSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"fn_a": {
			schema.Function{
				Schema:   "fn_a",
				Name:     "use_external_type",
				Args:     []schema.FunctionArg{{Mode: schema.InMode, Type: "fn_z.idtype"}},
				Returns:  schema.ReturnsType{Type: "integer"},
				Language: schema.SQL,
				Body:     "SELECT 1",
			},
		},
		"fn_z": {
			schema.DomainDef{
				Schema:   "fn_z",
				Name:     "idtype",
				BaseType: "integer",
			},
		},
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"fn_z", "fn_a"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_TypeDefiningSchemaMustSortBeforeUsingSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"ty_a": {
			schema.Table{
				Schema: "ty_a",
				Name:   "uses_pair",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "payload", Type: "ty_z.pair"},
				},
				PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
			},
		},
		"ty_z": {
			schema.CompositeDef{
				Schema: "ty_z",
				Name:   "pair",
				Attributes: []schema.CompositeAttr{
					{Name: "x", Type: "integer"},
					{Name: "y", Type: "integer"},
				},
			},
		},
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"ty_z", "ty_a"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_TriggerFunctionSchemaMustSortBeforeTriggerSchema(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"tr_a": {
			schema.Table{
				Schema: "tr_a",
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
				PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
			},
			schema.Trigger{
				Schema:     "tr_a",
				Table:      "events",
				Name:       "events_trg",
				Timing:     schema.After,
				Events:     []schema.TriggerEvent{schema.Insert},
				ForEachRow: true,
				Function: schema.QualifiedName{
					Schema: "tr_z",
					Name:   "apply_event",
				},
				Enabled: schema.EnabledAlways,
			},
		},
		"tr_z": {
			schema.Function{
				Schema:   "tr_z",
				Name:     "apply_event",
				Args:     nil,
				Returns:  schema.ReturnsType{Type: "trigger"},
				Language: schema.PlpgSQL,
				Body:     "BEGIN RETURN NEW; END",
			},
		},
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"tr_z", "tr_a"}
	require.Equal(t, want, got)
}

func TestDumpOrdering_ExtensionPublicMustSortBeforeSchemaUsingExtensionTypes(t *testing.T) {
	groups := map[schema.SchemaName][]schema.DatabaseObject{
		"early": {
			schema.Table{
				Schema: "early",
				Name:   "uses_citext",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "email", Type: "citext"},
				},
				PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
			},
		},
		"public": {
			schema.Extension{
				Schema: "public",
				Name:   "citext",
			},
		},
	}
	got := sortedSchemaNames(groups)
	want := []schema.SchemaName{"public", "early"}
	require.Equal(t, want, got)
}
