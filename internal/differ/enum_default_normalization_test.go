package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// TestEnumDefaultValueNormalization tests that ENUM default values with and without
// explicit type casting are treated as equivalent.
//
// This is the root cause test for the ENUM default value detection bug where:
// - PostgreSQL catalog returns: 'user'::user_role
// - Parser from schema.sql produces: 'user'
//
// Both should be treated as equivalent after normalization.
func TestEnumDefaultValueNormalization(t *testing.T) {
	tests := []struct {
		name     string
		expr1    schema.Expr
		expr2    schema.Expr
		expected bool // true if they should be equal after normalization
	}{
		{
			name:     "enum default with and without type cast",
			expr1:    "'user'::user_role",
			expr2:    "'user'",
			expected: true,
		},
		{
			name:     "enum default with different values",
			expr1:    "'admin'::user_role",
			expr2:    "'user'",
			expected: false,
		},
		{
			name:     "enum default both with type cast",
			expr1:    "'user'::user_role",
			expr2:    "'user'::user_role",
			expected: true,
		},
		{
			name:     "enum default both without type cast",
			expr1:    "'user'",
			expr2:    "'user'",
			expected: true,
		},
		{
			name:     "different enum types but same value",
			expr1:    "'active'::status_type",
			expr2:    "'active'::user_status",
			expected: true, // We normalize away the type cast, so values should match
		},
		{
			name:     "numeric defaults should not be affected",
			expr1:    "42",
			expr2:    "42",
			expected: true,
		},
		{
			name:     "string defaults with quotes",
			expr1:    "'hello world'",
			expr2:    "'hello world'",
			expected: true,
		},
		{
			name:     "boolean defaults",
			expr1:    "true",
			expr2:    "TRUE",
			expected: true, // Case insensitive after normalization
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized1 := normalizeExpr(tt.expr1)
			normalized2 := normalizeExpr(tt.expr2)

			equal := normalized1 == normalized2

			if equal != tt.expected {
				t.Errorf("normalizeExpr() mismatch:\n"+
					"  expr1: %q -> %q\n"+
					"  expr2: %q -> %q\n"+
					"  expected equal: %v, got: %v",
					tt.expr1, normalized1,
					tt.expr2, normalized2,
					tt.expected, equal)
			}
		})
	}
}

// TestTableColumnDefaultNormalization tests that tables with ENUM columns
// produce identical hashes whether the default value has explicit type casting or not.
func TestTableColumnDefaultNormalization(t *testing.T) {
	// Table from database (catalog returns type cast)
	tableFromDB := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{
				Name:    "id",
				Type:    "integer",
				NotNull: true,
			},
			{
				Name:    "role",
				Type:    "user_role",
				NotNull: true,
				Default: ptr(schema.Expr("'user'::user_role")),
			},
		},
	}

	// Table from schema.sql (parser returns no type cast)
	tableFromSQL := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{
				Name:    "id",
				Type:    "integer",
				NotNull: true,
			},
			{
				Name:    "role",
				Type:    "user_role",
				NotNull: true,
				Default: ptr(schema.Expr("'user'")),
			},
		},
	}

	// Normalize both tables
	normalizedDB := normalize(tableFromDB).(schema.Table)
	normalizedSQL := normalize(tableFromSQL).(schema.Table)

	// After normalization, the role column defaults should match
	dbDefault := normalizedDB.Columns[1].Default
	sqlDefault := normalizedSQL.Columns[1].Default

	if dbDefault == nil || sqlDefault == nil {
		t.Fatal("Default values should not be nil after normalization")
	}

	if *dbDefault != *sqlDefault {
		t.Errorf("Normalized default values should match:\n"+
			"  from DB (catalog): %q\n"+
			"  from SQL (parser): %q",
			*dbDefault, *sqlDefault)
	}

	// Hashes should also match
	hashDB, err := NormalizeAndHash(tableFromDB)
	if err != nil {
		t.Fatalf("Failed to hash table from DB: %v", err)
	}

	hashSQL, err := NormalizeAndHash(tableFromSQL)
	if err != nil {
		t.Fatalf("Failed to hash table from SQL: %v", err)
	}

	if hashDB != hashSQL {
		t.Errorf("Hashes should match after normalization:\n"+
			"  from DB: %s\n"+
			"  from SQL: %s",
			hashDB, hashSQL)
	}
}

// Helper function to create pointer to Expr
func ptr(e schema.Expr) *schema.Expr {
	return &e
}
