package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test enum value additions (safe)
func TestDifferEnumSafeAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "status"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.EnumDef{
				Schema: "public",
				Name:   "status",
				Values: []string{"active", "inactive", "pending", "archived"}, // Added "archived"
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.EnumDef{
				Schema: "public",
				Name:   "status",
				Values: []string{"active", "inactive", "pending"},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "enum values added at end")
}

// Test enum value changes (unsafe)
func TestDifferEnumUnsafeChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "status"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.EnumDef{
				Schema: "public",
				Name:   "status",
				Values: []string{"active", "pending", "inactive"}, // Reordered
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.EnumDef{
				Schema: "public",
				Name:   "status",
				Values: []string{"active", "inactive", "pending"},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	// Check that some enum value changed (specific message may vary)
	assert.True(t, len(diff.ToAlter[0].Changes) > 0, "Should have detected enum changes")
	assert.Contains(t, diff.ToAlter[0].Changes[0], "enum value")
}

// Test enum value removal (unsafe)
func TestDifferEnumValueRemoval(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "status"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.EnumDef{
				Schema: "public",
				Name:   "status",
				Values: []string{"active", "inactive"},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.EnumDef{
				Schema: "public",
				Name:   "status",
				Values: []string{"active", "inactive", "pending"},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "enum values removed (unsafe)")
}

// Test composite type addition
func TestDifferCompositeTypeAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "address"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.CompositeDef{
				Schema: "public",
				Name:   "address",
				Attributes: []schema.CompositeAttr{
					{Name: "street", Type: "text"},
					{Name: "city", Type: "text"},
					{Name: "zipcode", Type: "varchar(10)"},
				},
			},
			Hash: "hash1",
		},
	}

	actual := schema.SchemaObjectMap{}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	assert.Len(t, diff.ToCreate, 1)
	assert.Equal(t, key, diff.ToCreate[0])
}

// Test composite type attribute change
func TestDifferCompositeTypeAttributeChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "address"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.CompositeDef{
				Schema: "public",
				Name:   "address",
				Attributes: []schema.CompositeAttr{
					{Name: "street", Type: "text"},
					{Name: "city", Type: "text"},
					{Name: "country", Type: "text"}, // Added country
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.CompositeDef{
				Schema: "public",
				Name:   "address",
				Attributes: []schema.CompositeAttr{
					{Name: "street", Type: "text"},
					{Name: "city", Type: "text"},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "add attribute country")
}

// Test column with generated value
func TestDifferColumnGeneratedSpecChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "products"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "products",
				Columns: []schema.Column{
					{Name: "price", Type: "NUMERIC"},
					{
						Name: "price_with_tax",
						Type: "NUMERIC",
						Generated: &schema.GeneratedSpec{
							Expr:   schema.Expr("price * 1.2"),
							Stored: true,
						},
					},
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "products",
				Columns: []schema.Column{
					{Name: "price", Type: "NUMERIC"},
					{
						Name: "price_with_tax",
						Type: "NUMERIC",
						Generated: &schema.GeneratedSpec{
							Expr:   schema.Expr("price * 1.1"), // Different calculation
							Stored: true,
						},
					},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "alter column price_with_tax: generated spec changed")
}

// Test column with identity
func TestDifferColumnIdentityChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{
						Name: "id",
						Type: "INTEGER",
						Identity: &schema.IdentitySpec{
							Always: true, // Changed from false
						},
					},
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{
						Name: "id",
						Type: "INTEGER",
						Identity: &schema.IdentitySpec{
							Always: false,
						},
					},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "alter column id: identity spec changed")
}

// Test sequence owned by changes
func TestDifferSequenceOwnedByChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "public", Name: "user_id_seq"}

	start := int64(1)
	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Sequence{
				Schema: "public",
				Name:   "user_id_seq",
				Start:  &start,
				OwnedBy: &schema.SequenceOwner{
					Schema: "public",
					Table:  "users",
					Column: "id",
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Sequence{
				Schema:  "public",
				Name:    "user_id_seq",
				Start:   &start,
				OwnedBy: nil, // Not owned
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "owned by changed")
}

// Test index with predicate (partial index)
func TestDifferIndexPredicateChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.IndexKind, Schema: "public", Name: "idx_active_users"}

	predicate1 := schema.Expr("active = true")
	predicate2 := schema.Expr("active = true AND deleted_at IS NULL")

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_active_users",
				Method: schema.BTree,
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
				Predicate: &predicate2, // More restrictive predicate
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_active_users",
				Method: schema.BTree,
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
				Predicate: &predicate1,
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "predicate changed")
}

// Test index with include columns
func TestDifferIndexIncludeColumnsChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.IndexKind, Schema: "public", Name: "idx_users_email"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: schema.BTree,
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
				Include: []schema.ColumnName{"name", "created_at"}, // Added include columns
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: schema.BTree,
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
				Include: []schema.ColumnName{},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "include columns changed")
}

// Test function return type change
func TestDifferFunctionReturnTypeChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "get_user"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Function{
				Schema:   "public",
				Name:     "get_user",
				Language: schema.PlpgSQL,
				Returns:  schema.ReturnsType{Type: "users"}, // Returns table type
				Body:     "BEGIN RETURN QUERY SELECT * FROM users; END;",
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Function{
				Schema:   "public",
				Name:     "get_user",
				Language: schema.PlpgSQL,
				Returns:  schema.ReturnsType{Type: "void"},
				Body:     "BEGIN RETURN; END;",
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "return type changed")
}

// Test view security barrier change
func TestDifferViewSecurityBarrierChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "secure_users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.View{
				Schema:          "public",
				Name:            "secure_users",
				Type:            schema.RegularView,
				SecurityBarrier: true, // Enabled security barrier
				Definition: schema.ViewDefinition{
					Query: "SELECT * FROM users WHERE visible = true",
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.View{
				Schema:          "public",
				Name:            "secure_users",
				Type:            schema.RegularView,
				SecurityBarrier: false,
				Definition: schema.ViewDefinition{
					Query: "SELECT * FROM users WHERE visible = true",
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "security barrier changed")
}

// Test domain constraint change
func TestDifferDomainConstraintChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "positive_integer"}

	check1 := schema.Expr("VALUE > 0")
	check2 := schema.Expr("VALUE >= 0") // Changed constraint

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.DomainDef{
				Schema:   "public",
				Name:     "positive_integer",
				BaseType: "integer",
				Check:    &check2,
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.DomainDef{
				Schema:   "public",
				Name:     "positive_integer",
				BaseType: "integer",
				Check:    &check1,
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "check constraint changed")
}

// Test reloptions change
func TestDifferTableReloptionsChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "large_table"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:     "public",
				Name:       "large_table",
				Columns:    []schema.Column{{Name: "id", Type: "INTEGER"}},
				RelOptions: []string{"fillfactor=70", "autovacuum_enabled=true"}, // Changed fillfactor
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:     "public",
				Name:       "large_table",
				Columns:    []schema.Column{{Name: "id", Type: "INTEGER"}},
				RelOptions: []string{"fillfactor=90"},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "reloptions changed")
}

// Test column order normalization (column order is normalized for equivalence)
func TestDifferColumnOrderNormalized(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	table1 := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
			{Name: "name", Type: "TEXT"},
		},
	}

	table2 := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "email", Type: "TEXT"},
		},
	}

	// Compute actual hashes - they should be the same after normalization
	hash1, err := NormalizeAndHash(table1)
	require.NoError(t, err)

	hash2, err := NormalizeAndHash(table2)
	require.NoError(t, err)

	// After normalization, tables with same columns in different order should have same hash
	assert.Equal(t, hash1, hash2, "Normalized hashes should be equal for same columns in different order")

	// Same columns, different order
	desired := schema.SchemaObjectMap{
		key: {
			Payload: table1,
			Hash:    hash1,
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: table2,
			Hash:    hash2,
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	// Should detect no changes since hashes are the same
	assert.True(t, diff.IsEmpty(), "Tables with same columns in different order should be equivalent")
}
