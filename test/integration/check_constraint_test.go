package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnnamedCheckConstraints tests that unnamed CHECK constraints are properly
// auto-named to match PostgreSQL's naming scheme, preventing spurious diffs
func TestUnnamedCheckConstraints(t *testing.T) {
	ctx := context.Background()

	// Create test schema file with various unnamed CHECK constraints
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err := os.WriteFile(schemaFile, []byte(`
		-- Table with column-level CHECK constraints
		CREATE TABLE products (
			id SERIAL PRIMARY KEY,
			price DECIMAL CHECK (price > 0),
			quantity INT CHECK (quantity >= 0),
			name TEXT NOT NULL
		);

		-- Table with table-level CHECK constraint
		CREATE TABLE orders (
			id SERIAL PRIMARY KEY,
			user_id INT NOT NULL,
			total DECIMAL NOT NULL,
			CHECK (total > 0)
		);

		-- Table with multiple table-level CHECK constraints
		CREATE TABLE ranges (
			id SERIAL PRIMARY KEY,
			min_val INT,
			max_val INT,
			CHECK (min_val < max_val),
			CHECK (min_val >= 0)
		);

		-- Table with mix of named and unnamed CHECK constraints
		CREATE TABLE mixed (
			id SERIAL PRIMARY KEY,
			a INT CHECK (a > 0),
			b INT,
			CONSTRAINT custom_check CHECK (b > 0),
			CHECK (a + b > 0)
		);
	`), 0644)
	require.NoError(t, err)

	// Parse the schema
	p := parser.NewParser()
	parsedSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err, "should parse schema.sql")

	// Verify auto-generated constraint names match PostgreSQL's pattern
	checkNames := make(map[string]string) // table -> constraint names
	for _, objWithHash := range parsedSchema {
		if tbl, ok := objWithHash.Payload.(schema.Table); ok {
			for _, check := range tbl.Checks {
				t.Logf("Table %s: Check constraint %s = %s", tbl.Name, check.Name, check.Expr)
				checkNames[string(tbl.Name)] += check.Name + ";"
			}
		}
	}

	// Verify products table (column-level checks)
	assert.Contains(t, checkNames["products"], "products_price_check")
	assert.Contains(t, checkNames["products"], "products_quantity_check")

	// Verify orders table (table-level check)
	assert.Contains(t, checkNames["orders"], "orders_check")

	// Verify ranges table (multiple table-level checks)
	assert.Contains(t, checkNames["ranges"], "ranges_check")
	assert.Contains(t, checkNames["ranges"], "ranges_check1")

	// Verify mixed table (mix of named and unnamed)
	assert.Contains(t, checkNames["mixed"], "mixed_a_check") // column-level unnamed
	assert.Contains(t, checkNames["mixed"], "custom_check")  // explicitly named
	assert.Contains(t, checkNames["mixed"], "mixed_check")   // table-level unnamed

	// Now create these tables in the database and verify they match
	ddlGen := planner.NewDDLGenerator()

	// Generate DDL for each table
	var ddlStatements []string
	for key, objWithHash := range parsedSchema {
		if key.Kind == schema.TableKind {
			stmt, err := ddlGen.GenerateCreateStatement(objWithHash.Payload)
			require.NoError(t, err)
			ddlStatements = append(ddlStatements, stmt)
		}
	}

	// Connect to dev database and apply DDL
	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	// Clean up
	_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS products CASCADE")
	_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS orders CASCADE")
	_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS ranges CASCADE")
	_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS mixed CASCADE")
	defer func() {
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS products CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS orders CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS ranges CASCADE")
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS mixed CASCADE")
	}()

	// Apply DDL
	for _, ddl := range ddlStatements {
		_, err = devPool.Exec(ctx, ddl)
		require.NoError(t, err, "should execute DDL: %s", ddl)
	}

	// Extract schema from database
	catalog := db.NewCatalog(devPool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "schemata"})
	require.NoError(t, err)

	// Build actual schema map
	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err)

	// Compute diff - should be empty since names match PostgreSQL's auto-generation
	d := differ.NewDiffer()
	diff, err := d.Diff(parsedSchema, actualSchema)
	require.NoError(t, err)

	// The key success criteria: NO check constraints should be dropped/created
	// (Expression normalization differences are a separate issue affecting all constraints)
	for _, key := range diff.ToDrop {
		if key.Kind == schema.TableKind {
			t.Errorf("Should not drop any tables, but found: %v", key)
		}
	}
	for _, key := range diff.ToCreate {
		if key.Kind == schema.TableKind {
			t.Errorf("Should not create any tables, but found: %v", key)
		}
	}

	// Check that no check constraints are being dropped/recreated
	// (which would happen if names didn't match)
	for _, alter := range diff.ToAlter {
		for _, change := range alter.Changes {
			assert.NotContains(t, change, "drop check constraint",
				"Should not drop check constraints - names should match PostgreSQL's auto-generation")
			assert.NotContains(t, change, "add check constraint",
				"Should not add check constraints - names should match PostgreSQL's auto-generation")
		}
	}

	t.Log("✅ Success: Constraint names match PostgreSQL's auto-generation pattern")
	t.Log("   No check constraints were dropped/recreated due to name mismatches")
}
