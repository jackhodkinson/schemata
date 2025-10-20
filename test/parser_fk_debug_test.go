package test

import (
	"encoding/json"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForeignKeyConstraintParsing is a deep dive into FK constraint parsing
func TestForeignKeyConstraintParsing(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		expectedFKCols []string
		expectedRefTable string
		expectedRefCols []string
	}{
		{
			name: "table-level CONSTRAINT with explicit name",
			sql: `CREATE TABLE posts (
				id SERIAL PRIMARY KEY,
				user_id INTEGER NOT NULL,
				CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)
			)`,
			expectedFKCols:   []string{"user_id"},
			expectedRefTable: "users",
			expectedRefCols:  []string{"id"},
		},
		{
			name: "table-level CONSTRAINT without explicit name",
			sql: `CREATE TABLE posts (
				id SERIAL PRIMARY KEY,
				user_id INTEGER NOT NULL,
				FOREIGN KEY (user_id) REFERENCES users(id)
			)`,
			expectedFKCols:   []string{"user_id"},
			expectedRefTable: "users",
			expectedRefCols:  []string{"id"},
		},
		{
			name: "column-level REFERENCES",
			sql: `CREATE TABLE posts (
				id SERIAL PRIMARY KEY,
				user_id INTEGER NOT NULL REFERENCES users(id)
			)`,
			expectedFKCols:   []string{"user_id"},
			expectedRefTable: "users",
			expectedRefCols:  []string{"id"},
		},
		{
			name: "multi-column FK",
			sql: `CREATE TABLE order_items (
				order_id INTEGER NOT NULL,
				product_id INTEGER NOT NULL,
				CONSTRAINT order_items_fkey FOREIGN KEY (order_id, product_id) REFERENCES orders(id, product_id)
			)`,
			expectedFKCols:   []string{"order_id", "product_id"},
			expectedRefTable: "orders",
			expectedRefCols:  []string{"id", "product_id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Testing SQL: %s", tt.sql)

			// Step 1: Parse with pg_query to see raw output
			result, err := pg_query.Parse(tt.sql)
			require.NoError(t, err, "pg_query.Parse should succeed")

			// Pretty print the parse tree for debugging
			jsonBytes, _ := json.MarshalIndent(result, "", "  ")
			t.Logf("Parse tree:\n%s", string(jsonBytes))

			// Step 2: Find the constraint node
			require.NotNil(t, result.Stmts, "should have statements")
			require.Greater(t, len(result.Stmts), 0, "should have at least one statement")

			stmt := result.Stmts[0]
			require.NotNil(t, stmt.Stmt, "statement should have Stmt")

			createStmt, ok := stmt.Stmt.Node.(*pg_query.Node_CreateStmt)
			require.True(t, ok, "should be CREATE TABLE statement")
			require.NotNil(t, createStmt.CreateStmt, "CreateStmt should not be nil")

			// Find the foreign key constraint (table-level only)
			// Note: column-level REFERENCES are on the column, not in TableElts
			var fkConstraint *pg_query.Constraint
			isColumnLevel := (tt.name == "column-level REFERENCES")

			if !isColumnLevel {
				for _, elt := range createStmt.CreateStmt.TableElts {
					if elt == nil {
						continue
					}
					if constraintNode, ok := elt.Node.(*pg_query.Node_Constraint); ok {
						if constraintNode.Constraint.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
							fkConstraint = constraintNode.Constraint
							break
						}
					}
				}

				require.NotNil(t, fkConstraint, "should find FK constraint")

				// Step 3: Inspect the constraint structure
				t.Logf("FK Constraint details:")
				t.Logf("  Conname: %s", fkConstraint.Conname)
				t.Logf("  Contype: %v", fkConstraint.Contype)
				t.Logf("  Keys length: %d", len(fkConstraint.Keys))
				t.Logf("  PkAttrs length: %d", len(fkConstraint.PkAttrs))
				t.Logf("  Pktable: %v", fkConstraint.Pktable)

				// Log individual Keys
				for i, key := range fkConstraint.Keys {
					if strNode, ok := key.Node.(*pg_query.Node_String_); ok {
						t.Logf("  Keys[%d]: %s", i, strNode.String_.Sval)
					} else {
						t.Logf("  Keys[%d]: %T", i, key.Node)
					}
				}

				// Log individual PkAttrs
				for i, attr := range fkConstraint.PkAttrs {
					if strNode, ok := attr.Node.(*pg_query.Node_String_); ok {
						t.Logf("  PkAttrs[%d]: %s", i, strNode.String_.Sval)
					} else {
						t.Logf("  PkAttrs[%d]: %T", i, attr.Node)
					}
				}
			} else {
				t.Logf("Skipping raw pg_query FK inspection for column-level REFERENCES")
			}

			// Step 4: Use our parser to extract the table
			p := parser.NewParser()
			objectMap, err := p.ParseSQL(tt.sql)
			require.NoError(t, err, "parser should succeed")

			// Find the table
			var table schema.Table
			found := false
			for _, hashedObj := range objectMap {
				if tbl, ok := hashedObj.Payload.(schema.Table); ok {
					table = tbl
					found = true
					break
				}
			}
			require.True(t, found, "should find table in parsed objects")

			// Step 5: Verify FK extraction
			require.Len(t, table.ForeignKeys, 1, "should have exactly 1 foreign key")
			fk := table.ForeignKeys[0]

			t.Logf("Parsed FK:")
			t.Logf("  Name: %s", fk.Name)
			t.Logf("  Cols: %v", fk.Cols)
			t.Logf("  Ref.Table: %s", fk.Ref.Table)
			t.Logf("  Ref.Cols: %v", fk.Ref.Cols)

			// Assertions
			assert.Equal(t, tt.expectedRefTable, string(fk.Ref.Table), "referenced table should match")

			// Convert to strings for comparison
			actualFKCols := make([]string, len(fk.Cols))
			for i, col := range fk.Cols {
				actualFKCols[i] = string(col)
			}
			assert.Equal(t, tt.expectedFKCols, actualFKCols, "FK source columns should match")

			actualRefCols := make([]string, len(fk.Ref.Cols))
			for i, col := range fk.Ref.Cols {
				actualRefCols[i] = string(col)
			}
			assert.Equal(t, tt.expectedRefCols, actualRefCols, "FK referenced columns should match")
		})
	}
}

// TestRawPgQueryFKExtraction tests pg_query directly to understand its output
func TestRawPgQueryFKExtraction(t *testing.T) {
	sql := `CREATE TABLE posts (
		id SERIAL PRIMARY KEY,
		user_id INTEGER NOT NULL,
		CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	)`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	// Navigate to the constraint
	stmt := result.Stmts[0].Stmt.Node.(*pg_query.Node_CreateStmt).CreateStmt

	var fkConstraint *pg_query.Constraint
	for _, elt := range stmt.TableElts {
		if elt == nil {
			continue
		}
		if constraintNode, ok := elt.Node.(*pg_query.Node_Constraint); ok {
			if constraintNode.Constraint.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
				fkConstraint = constraintNode.Constraint
				break
			}
		}
	}

	require.NotNil(t, fkConstraint, "should find FK constraint")

	// Debug output
	t.Logf("Raw FK Constraint from pg_query:")
	t.Logf("  Conname: '%s'", fkConstraint.Conname)
	t.Logf("  Keys: %v (length: %d)", fkConstraint.Keys, len(fkConstraint.Keys))
	t.Logf("  PkAttrs: %v (length: %d)", fkConstraint.PkAttrs, len(fkConstraint.PkAttrs))
	t.Logf("  FkAttrs: %v (length: %d)", fkConstraint.FkAttrs, len(fkConstraint.FkAttrs))
	t.Logf("  Pktable.Relname: %s", fkConstraint.Pktable.Relname)
	t.Logf("  FkDelAction: %s", fkConstraint.FkDelAction)

	// Try to extract from all possible fields
	t.Logf("\nTrying different extraction approaches:")

	// Approach 1: Keys (what we currently use)
	t.Logf("1. constraint.Keys:")
	for i, key := range fkConstraint.Keys {
		if strNode, ok := key.Node.(*pg_query.Node_String_); ok {
			t.Logf("   [%d] = '%s'", i, strNode.String_.Sval)
		} else {
			t.Logf("   [%d] = %T", i, key.Node)
		}
	}

	// Approach 2: FkAttrs
	t.Logf("2. constraint.FkAttrs:")
	for i, attr := range fkConstraint.FkAttrs {
		if strNode, ok := attr.Node.(*pg_query.Node_String_); ok {
			t.Logf("   [%d] = '%s'", i, strNode.String_.Sval)
		} else {
			t.Logf("   [%d] = %T", i, attr.Node)
		}
	}

	// Approach 3: PkAttrs (what we use for referenced columns)
	t.Logf("3. constraint.PkAttrs:")
	for i, attr := range fkConstraint.PkAttrs {
		if strNode, ok := attr.Node.(*pg_query.Node_String_); ok {
			t.Logf("   [%d] = '%s'", i, strNode.String_.Sval)
		} else {
			t.Logf("   [%d] = %T", i, attr.Node)
		}
	}

	// Check if Keys is empty and FkAttrs has the data
	if len(fkConstraint.Keys) == 0 && len(fkConstraint.FkAttrs) > 0 {
		t.Logf("\n⚠️  FOUND IT: Keys is empty but FkAttrs has data!")
		t.Logf("    We should be using FkAttrs for FK source columns, not Keys")
	}
}

// TestColumnLevelFKParsing tests inline REFERENCES
func TestColumnLevelFKParsing(t *testing.T) {
	sql := `CREATE TABLE posts (
		id SERIAL PRIMARY KEY,
		user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE
	)`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)

	// Navigate to the column definition
	stmt := result.Stmts[0].Stmt.Node.(*pg_query.Node_CreateStmt).CreateStmt

	var userIdCol *pg_query.ColumnDef
	for _, elt := range stmt.TableElts {
		if elt == nil {
			continue
		}
		if colDef, ok := elt.Node.(*pg_query.Node_ColumnDef); ok {
			if colDef.ColumnDef.Colname == "user_id" {
				userIdCol = colDef.ColumnDef
				break
			}
		}
	}

	require.NotNil(t, userIdCol, "should find user_id column")

	t.Logf("Column: %s", userIdCol.Colname)
	t.Logf("Constraints: %d", len(userIdCol.Constraints))

	// Look for FK constraint in column constraints
	for i, constraint := range userIdCol.Constraints {
		if c, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
			t.Logf("Constraint[%d]:", i)
			t.Logf("  Contype: %v", c.Constraint.Contype)
			if c.Constraint.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
				t.Logf("  Found FK constraint on column!")
				t.Logf("  Keys: %v (length: %d)", c.Constraint.Keys, len(c.Constraint.Keys))
				t.Logf("  FkAttrs: %v (length: %d)", c.Constraint.FkAttrs, len(c.Constraint.FkAttrs))
				t.Logf("  PkAttrs: %v (length: %d)", c.Constraint.PkAttrs, len(c.Constraint.PkAttrs))
				t.Logf("  Pktable: %v", c.Constraint.Pktable)
			}
		}
	}

	// Test with parser
	p := parser.NewParser()
	objectMap, err := p.ParseSQL(sql)
	require.NoError(t, err)

	var table schema.Table
	for _, hashedObj := range objectMap {
		if tbl, ok := hashedObj.Payload.(schema.Table); ok {
			table = tbl
			break
		}
	}

	t.Logf("\nParsed table foreign keys: %d", len(table.ForeignKeys))
	for i, fk := range table.ForeignKeys {
		t.Logf("FK[%d]: cols=%v -> %s.%s(%v)", i, fk.Cols, fk.Ref.Schema, fk.Ref.Table, fk.Ref.Cols)
	}
}
