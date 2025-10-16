package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test Primary Key changes
func TestDifferPrimaryKeyAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	pkName := "users_pkey"
	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
				PrimaryKey: &schema.PrimaryKey{
					Name: &pkName,
					Cols: []schema.ColumnName{"id"},
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
				PrimaryKey: nil,
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "add primary key")
}

func TestDifferPrimaryKeyRemoval(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	pkName := "users_pkey"
	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:     "public",
				Name:       "users",
				Columns:    []schema.Column{{Name: "id", Type: "INTEGER"}},
				PrimaryKey: nil,
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "users",
				Columns: []schema.Column{{Name: "id", Type: "INTEGER"}},
				PrimaryKey: &schema.PrimaryKey{
					Name: &pkName,
					Cols: []schema.ColumnName{"id"},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "drop primary key")
}

func TestDifferPrimaryKeyColumnsChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	pkName := "users_pkey"
	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER"},
					{Name: "tenant_id", Type: "INTEGER"},
				},
				PrimaryKey: &schema.PrimaryKey{
					Name: &pkName,
					Cols: []schema.ColumnName{"tenant_id", "id"}, // Composite PK
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
					{Name: "id", Type: "INTEGER"},
					{Name: "tenant_id", Type: "INTEGER"},
				},
				PrimaryKey: &schema.PrimaryKey{
					Name: &pkName,
					Cols: []schema.ColumnName{"id"},
				},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "primary key columns changed")
}

// Test Unique Constraints
func TestDifferUniqueConstraintAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "users",
				Columns: []schema.Column{{Name: "email", Type: "TEXT"}},
				Uniques: []schema.UniqueConstraint{
					{
						Name: "users_email_key",
						Cols: []schema.ColumnName{"email"},
					},
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "users",
				Columns: []schema.Column{{Name: "email", Type: "TEXT"}},
				Uniques: []schema.UniqueConstraint{},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "add unique constraint users_email_key")
}

// Test Check Constraints
func TestDifferCheckConstraintAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "users",
				Columns: []schema.Column{{Name: "age", Type: "INTEGER"}},
				Checks: []schema.CheckConstraint{
					{
						Name: "users_age_check",
						Expr: schema.Expr("age >= 0"),
					},
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "users",
				Columns: []schema.Column{{Name: "age", Type: "INTEGER"}},
				Checks:  []schema.CheckConstraint{},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "add check constraint users_age_check")
}

func TestDifferCheckConstraintExpressionChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "users",
				Columns: []schema.Column{{Name: "age", Type: "INTEGER"}},
				Checks: []schema.CheckConstraint{
					{
						Name: "users_age_check",
						Expr: schema.Expr("age >= 18"), // Changed expression
					},
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "users",
				Columns: []schema.Column{{Name: "age", Type: "INTEGER"}},
				Checks: []schema.CheckConstraint{
					{
						Name: "users_age_check",
						Expr: schema.Expr("age >= 0"),
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
	assert.Contains(t, diff.ToAlter[0].Changes, "check constraint users_age_check expression changed")
}

// Test Foreign Keys
func TestDifferForeignKeyAddition(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "posts"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "posts",
				Columns: []schema.Column{{Name: "user_id", Type: "INTEGER"}},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "posts_user_id_fkey",
						Cols: []schema.ColumnName{"user_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "public",
							Table:  "users",
							Cols:   []schema.ColumnName{"id"},
						},
						OnDelete: schema.Cascade,
						OnUpdate: schema.NoAction,
						Match:    schema.MatchSimple,
					},
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:      "public",
				Name:        "posts",
				Columns:     []schema.Column{{Name: "user_id", Type: "INTEGER"}},
				ForeignKeys: []schema.ForeignKey{},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "add foreign key posts_user_id_fkey")
}

func TestDifferForeignKeyOnDeleteChange(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "posts"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "posts",
				Columns: []schema.Column{{Name: "user_id", Type: "INTEGER"}},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "posts_user_id_fkey",
						Cols: []schema.ColumnName{"user_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "public",
							Table:  "users",
							Cols:   []schema.ColumnName{"id"},
						},
						OnDelete: schema.SetNull, // Changed from Cascade
						OnUpdate: schema.NoAction,
						Match:    schema.MatchSimple,
					},
				},
			},
			Hash: "hash2",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "posts",
				Columns: []schema.Column{{Name: "user_id", Type: "INTEGER"}},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "posts_user_id_fkey",
						Cols: []schema.ColumnName{"user_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "public",
							Table:  "users",
							Cols:   []schema.ColumnName{"id"},
						},
						OnDelete: schema.Cascade,
						OnUpdate: schema.NoAction,
						Match:    schema.MatchSimple,
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
	assert.Contains(t, diff.ToAlter[0].Changes, "foreign key posts_user_id_fkey on delete changed")
}

// Test composite key constraints
func TestDifferCompositeUniqueConstraint(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "user_roles"}

	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "user_roles",
				Columns: []schema.Column{
					{Name: "user_id", Type: "INTEGER"},
					{Name: "role_id", Type: "INTEGER"},
				},
				Uniques: []schema.UniqueConstraint{
					{
						Name: "user_roles_user_id_role_id_key",
						Cols: []schema.ColumnName{"user_id", "role_id"},
					},
				},
			},
			Hash: "hash1",
		},
	}

	actual := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "user_roles",
				Columns: []schema.Column{
					{Name: "user_id", Type: "INTEGER"},
					{Name: "role_id", Type: "INTEGER"},
				},
				Uniques: []schema.UniqueConstraint{},
			},
			Hash: "hash2",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	assert.Contains(t, diff.ToAlter[0].Changes, "add unique constraint user_roles_user_id_role_id_key")
}

// Test multiple constraints changing at once
func TestDifferMultipleConstraintChanges(t *testing.T) {
	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "orders"}

	pkName := "orders_pkey"
	desired := schema.SchemaObjectMap{
		key: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER"},
					{Name: "user_id", Type: "INTEGER"},
					{Name: "total", Type: "NUMERIC"},
				},
				PrimaryKey: &schema.PrimaryKey{
					Name: &pkName,
					Cols: []schema.ColumnName{"id"},
				},
				Checks: []schema.CheckConstraint{
					{
						Name: "orders_total_check",
						Expr: schema.Expr("total >= 0"),
					},
				},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "orders_user_id_fkey",
						Cols: []schema.ColumnName{"user_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "public",
							Table:  "users",
							Cols:   []schema.ColumnName{"id"},
						},
						OnDelete: schema.Cascade,
						OnUpdate: schema.NoAction,
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
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER"},
					{Name: "user_id", Type: "INTEGER"},
					{Name: "total", Type: "NUMERIC"},
				},
				PrimaryKey:  nil, // No PK
				Checks:      []schema.CheckConstraint{},
				ForeignKeys: []schema.ForeignKey{},
			},
			Hash: "hash1",
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desired, actual)
	require.NoError(t, err)

	require.Len(t, diff.ToAlter, 1)
	changes := diff.ToAlter[0].Changes
	assert.Contains(t, changes, "add primary key")
	assert.Contains(t, changes, "add check constraint orders_total_check")
	assert.Contains(t, changes, "add foreign key orders_user_id_fkey")
}
