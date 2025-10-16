package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDifferTableAdditions(t *testing.T) {
	desired := schema.SchemaObjectMap{
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "email", Type: "TEXT", NotNull: true},
				},
			},
			Hash: "hash1",
		},
	}

	actual := schema.SchemaObjectMap{}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	assert.Len(t, diff.ToCreate, 1, "should have one table to create")
	assert.Equal(t, schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}, diff.ToCreate[0])
	assert.Empty(t, diff.ToDrop)
	assert.Empty(t, diff.ToAlter)
}

func TestDifferTableRemovals(t *testing.T) {
	desired := schema.SchemaObjectMap{}

	actual := schema.SchemaObjectMap{
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "old_table"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "old_table",
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	assert.Empty(t, diff.ToCreate)
	assert.Len(t, diff.ToDrop, 1, "should have one table to drop")
	assert.Equal(t, schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "old_table"}, diff.ToDrop[0])
	assert.Empty(t, diff.ToAlter)
}

func TestDifferColumnAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "email", Type: "TEXT", NotNull: true},
					{Name: "name", Type: "TEXT", NotNull: false}, // New column
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
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "email", Type: "TEXT", NotNull: true},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	assert.Empty(t, diff.ToCreate)
	assert.Empty(t, diff.ToDrop)
	require.Len(t, diff.ToAlter, 1, "should have one table to alter")

	alter := diff.ToAlter[0]
	assert.Equal(t, key, alter.Key)
	assert.Contains(t, alter.Changes, "add column name", "should detect added column")
}

func TestDifferColumnRemoval(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
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
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "deprecated", Type: "TEXT", NotNull: false},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	alter := diff.ToAlter[0]
	assert.Contains(t, alter.Changes, "drop column deprecated")
}

func TestDifferColumnTypeChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "BIGINT", NotNull: true}, // Changed from INTEGER
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
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	alter := diff.ToAlter[0]
	assert.Contains(t, alter.Changes, "alter column id: type changed from INTEGER to BIGINT")
}

func TestDifferColumnNotNullChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "email", Type: "TEXT", NotNull: true}, // Now NOT NULL
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
					{Name: "email", Type: "TEXT", NotNull: false},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	alter := diff.ToAlter[0]
	assert.Contains(t, alter.Changes, "alter column email: set not null")
}

func TestDifferColumnDefaultChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	defaultVal := schema.Expr("NOW()")
	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "created_at", Type: "TIMESTAMP", Default: &defaultVal},
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
					{Name: "created_at", Type: "TIMESTAMP", Default: nil},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	alter := diff.ToAlter[0]
	assert.Contains(t, alter.Changes, "alter column created_at: default changed")
}

func TestDifferIndexAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.IndexKind, Schema: "public", Name: "idx_users_email"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Unique: false,
				Method: schema.BTree,
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
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

func TestDifferViewModification(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "active_users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.View{
				Schema: "public",
				Name:   "active_users",
				Type:   schema.RegularView,
				Definition: schema.ViewDefinition{
					Query: "SELECT * FROM users WHERE active = true AND deleted_at IS NULL",
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.View{
				Schema: "public",
				Name:   "active_users",
				Type:   schema.RegularView,
				Definition: schema.ViewDefinition{
					Query: "SELECT * FROM users WHERE active = true",
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	alter := diff.ToAlter[0]
	assert.Contains(t, alter.Changes, "definition changed")
}

func TestDifferFunctionBodyChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "calculate_total"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Function{
				Schema:   "public",
				Name:     "calculate_total",
				Language: "plpgsql",
				Body:     "BEGIN RETURN a + b + c; END;",
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Function{
				Schema:   "public",
				Name:     "calculate_total",
				Language: "plpgsql",
				Body:     "BEGIN RETURN a + b; END;",
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	alter := diff.ToAlter[0]
	assert.Contains(t, alter.Changes, "body changed")
}

func TestDifferSequenceAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "public", Name: "user_id_seq"}

	start := int64(1000)
	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Sequence{
				Schema: "public",
				Name:   "user_id_seq",
				Start:  &start,
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

func TestDifferEnumAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "status"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.EnumDef{
				Schema: "public",
				Name:   "status",
				Values: []string{"active", "inactive", "pending"},
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

func TestDifferNoChanges(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
			},
			Hash: "hash1",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
			},
			Hash: "hash1", // Same hash
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	assert.Empty(t, diff.ToCreate)
	assert.Empty(t, diff.ToDrop)
	assert.Empty(t, diff.ToAlter)
	assert.True(t, diff.IsEmpty(), "diff should be empty")
}

func TestDifferMultipleChanges(t *testing.T) {
	usersKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}
	postsKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "posts"}
	oldKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "old_table"}

	desired := schema.SchemaObjectMap{
		usersKey: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "email", Type: "TEXT", NotNull: true},
					{Name: "name", Type: "TEXT"}, // Added
				},
			},
			Hash: "hash2",
		},
		postsKey: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
			},
			Hash: "hash3",
		},
	}

	actual := schema.SchemaObjectMap{
		usersKey: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "email", Type: "TEXT", NotNull: true},
				},
			},
			Hash: "hash1",
		},
		oldKey: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "old_table",
			},
			Hash: "hash4",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	// Should create posts table
	assert.Len(t, diff.ToCreate, 1)
	assert.Contains(t, diff.ToCreate, postsKey)

	// Should drop old_table
	assert.Len(t, diff.ToDrop, 1)
	assert.Contains(t, diff.ToDrop, oldKey)

	// Should alter users table (add name column)
	assert.Len(t, diff.ToAlter, 1)
	assert.Equal(t, usersKey, diff.ToAlter[0].Key)
}
