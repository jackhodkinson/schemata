package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

// TestIndexKeyExpressionNormalization tests that index key expressions are normalized correctly
// This replicates the issue where catalog extracts full CREATE INDEX statement while parser extracts column name
func TestIndexKeyExpressionNormalization(t *testing.T) {
	tests := []struct {
		name     string
		desired  schema.Index
		actual   schema.Index
		expected []string // empty means no differences expected
	}{
		{
			name: "identical simple column indexes",
			desired: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: "btree",
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
			},
			actual: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: "btree",
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
			},
			expected: []string{},
		},
		{
			name: "parser extracts column name, catalog extracts full statement",
			desired: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: "btree",
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
			},
			actual: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: "btree",
				KeyExprs: []schema.IndexKeyExpr{
					// This is what the current catalog extraction returns (full CREATE INDEX statement)
					{Expr: "CREATE INDEX idx_users_email ON public.users USING btree (email)"},
				},
			},
			expected: []string{"key expressions changed"},
		},
		{
			name: "case sensitivity in column names",
			desired: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: "btree",
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "email"},
				},
			},
			actual: schema.Index{
				Schema: "public",
				Table:  "users",
				Name:   "idx_users_email",
				Method: "btree",
				KeyExprs: []schema.IndexKeyExpr{
					{Expr: "EMAIL"}, // uppercase
				},
			},
			expected: []string{}, // After normalization, these should be the same
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Normalize both indexes before comparing (mimics what happens in real code)
			normalizedDesired := normalizeIndex(tt.desired)
			normalizedActual := normalizeIndex(tt.actual)

			changes := compareIndexes(normalizedDesired, normalizedActual)

			if len(tt.expected) == 0 {
				assert.Empty(t, changes, "Expected no changes")
			} else {
				assert.Equal(t, tt.expected, changes)
			}
		})
	}
}

// TestIndexNormalizationInDiffer tests that normalization happens in the differ
func TestIndexNormalizationInDiffer(t *testing.T) {
	desired := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Method: "btree",
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "EMAIL"}, // uppercase
		},
	}

	actual := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Method: "btree",
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "email"}, // lowercase
		},
	}

	// Normalize both
	normalizedDesired := normalizeIndex(desired)
	normalizedActual := normalizeIndex(actual)

	// After normalization, key expressions should match
	assert.Equal(t, normalizedDesired.KeyExprs[0].Expr, normalizedActual.KeyExprs[0].Expr,
		"Normalized index key expressions should match")
}
