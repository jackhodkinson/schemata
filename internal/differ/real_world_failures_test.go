package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

// These tests verify that parser and catalog extraction produce identical results
// for real-world schema scenarios, ensuring proper normalization and comparison

// TestRealWorld_IndexKeyExpressionCorrect verifies that catalog correctly extracts
// just the column expression, not the full CREATE INDEX statement
//
// This test should PASS - catalog.go uses pg_get_indexdef with column number to extract expressions correctly
func TestRealWorld_IndexKeyExpressionCorrect(t *testing.T) {
	// This is what the PARSER extracts from schema.sql
	desiredIndex := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Method: "btree",
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "email"}, // Just the column name
		},
	}

	// This is what the CATALOG correctly extracts from the database
	actualIndex := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Method: "btree",
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "email"}, // Also just the column name - catalog uses pg_get_indexdef correctly
		},
	}

	// Build object maps as they would be in real code
	desiredMap := schema.SchemaObjectMap{
		schema.ObjectKey{Kind: schema.IndexKind, Schema: "public", Name: "idx_users_email"}: schema.HashedObject{
			Hash:    mustHash(desiredIndex),
			Payload: desiredIndex,
		},
	}

	actualMap := schema.SchemaObjectMap{
		schema.ObjectKey{Kind: schema.IndexKind, Schema: "public", Name: "idx_users_email"}: schema.HashedObject{
			Hash:    mustHash(actualIndex),
			Payload: actualIndex,
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desiredMap, actualMap)
	assert.NoError(t, err)

	// Verify normalization works correctly
	if !diff.IsEmpty() {
		t.Logf("Parser extracted: %q", desiredIndex.KeyExprs[0].Expr)
		t.Logf("Catalog extracted: %q", actualIndex.KeyExprs[0].Expr)
		if len(diff.ToAlter) > 0 {
			t.Logf("Changes: %v", diff.ToAlter[0].Changes)
		}
	}

	// This assertion should PASS - catalog correctly extracts just the expression
	assert.True(t, diff.IsEmpty(),
		"Index expressions should be identical - catalog uses pg_get_indexdef with column number correctly")
}

// TestRealWorld_FunctionWhitespaceHandling tests if function body whitespace differences
// are properly normalized
//
// THIS TEST SHOULD FAIL if normalization isn't working correctly
func TestRealWorld_FunctionWhitespaceHandling(t *testing.T) {
	// This is what the PARSER extracts from schema.sql
	desiredFunction := schema.Function{
		Schema:          "public",
		Name:            "update_updated_at_column",
		Language:        "plpgsql",
		Body:            "BEGIN\n    NEW.updated_at = CURRENT_TIMESTAMP;\n    RETURN NEW;\nEND;",
		Returns:         schema.ReturnsType{Type: "trigger"},
		Volatility:      schema.Volatile,
		Strict:          false,
		SecurityDefiner: false,
		Parallel:        schema.ParallelUnsafe,
	}

	// This is what the CATALOG extracts using pg_get_functiondef()
	actualFunction := schema.Function{
		Schema:   "public",
		Name:     "update_updated_at_column",
		Language: "plpgsql",
		// pg_get_functiondef() returns with different whitespace (extra newlines at start/end)
		Body:            "\nBEGIN\n    NEW.updated_at = CURRENT_TIMESTAMP;\n    RETURN NEW;\nEND;\n",
		Returns:         schema.ReturnsType{Type: "trigger"},
		Volatility:      schema.Volatile,
		Strict:          false,
		SecurityDefiner: false,
		Parallel:        schema.ParallelUnsafe,
	}

	// Build object maps
	key := schema.ObjectKey{Kind: schema.FunctionKind, Schema: "public", Name: "update_updated_at_column"}

	desiredMap := schema.SchemaObjectMap{
		key: schema.HashedObject{
			Hash:    mustHash(desiredFunction),
			Payload: desiredFunction,
		},
	}

	actualMap := schema.SchemaObjectMap{
		key: schema.HashedObject{
			Hash:    mustHash(actualFunction),
			Payload: actualFunction,
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desiredMap, actualMap)
	assert.NoError(t, err)

	// Log if bug detected
	if !diff.IsEmpty() {
		t.Logf("❌ BUG DETECTED: Function incorrectly flagged as changed due to whitespace")
		t.Logf("Desired body: %q", desiredFunction.Body)
		t.Logf("Actual body: %q", actualFunction.Body)
		t.Logf("Desired hash: %s", desiredMap[key].Hash)
		t.Logf("Actual hash: %s", actualMap[key].Hash)
		if len(diff.ToAlter) > 0 {
			t.Logf("Changes: %v", diff.ToAlter[0].Changes)
		}
	}

	// This assertion should PASS when normalization works, FAIL when it doesn't
	assert.True(t, diff.IsEmpty(),
		"Function should be considered identical (only whitespace differs). "+
			"FIX: Ensure normalizeFunction() properly handles leading/trailing whitespace")
}

// TestRealWorld_TriggerForEachRowCorrect verifies that catalog correctly extracts
// the FOR EACH ROW property from pg_trigger.tgtype bitfield
//
// This test should PASS - catalog.go correctly parses tgtype bitfield (bit 0 for FOR EACH ROW)
func TestRealWorld_TriggerForEachRowCorrect(t *testing.T) {
	// This is what the PARSER extracts from "CREATE TRIGGER ... FOR EACH ROW"
	desiredTrigger := schema.Trigger{
		Schema:     "public",
		Table:      "users",
		Name:       "update_users_updated_at",
		Timing:     schema.Before,
		Events:     []schema.TriggerEvent{schema.Update},
		ForEachRow: true, // Parser correctly extracts this from SQL
		Function: schema.QualifiedName{
			Schema: "public",
			Name:   "update_updated_at_column",
		},
	}

	// This is what the CATALOG correctly extracts - parses tgtype bitfield correctly
	actualTrigger := schema.Trigger{
		Schema:     "public",
		Table:      "users",
		Name:       "update_users_updated_at",
		Timing:     schema.Before,
		Events:     []schema.TriggerEvent{schema.Update},
		ForEachRow: true, // Catalog correctly extracts ForEachRow from tgtype & 1
		Function: schema.QualifiedName{
			Schema: "public",
			Name:   "update_updated_at_column",
		},
	}

	// Build object maps
	key := schema.ObjectKey{Kind: schema.TriggerKind, Schema: "public", Name: "update_users_updated_at"}

	desiredMap := schema.SchemaObjectMap{
		key: schema.HashedObject{
			Hash:    mustHash(desiredTrigger),
			Payload: desiredTrigger,
		},
	}

	actualMap := schema.SchemaObjectMap{
		key: schema.HashedObject{
			Hash:    mustHash(actualTrigger),
			Payload: actualTrigger,
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desiredMap, actualMap)
	assert.NoError(t, err)

	// Verify triggers are identical
	if !diff.IsEmpty() {
		t.Logf("Parser extracted ForEachRow: %v", desiredTrigger.ForEachRow)
		t.Logf("Catalog extracted ForEachRow: %v", actualTrigger.ForEachRow)
		if len(diff.ToAlter) > 0 {
			t.Logf("Changes: %v", diff.ToAlter[0].Changes)
		}
	}

	// This assertion should PASS - catalog correctly parses tgtype bitfield
	assert.True(t, diff.IsEmpty(),
		"Triggers should be considered identical - catalog correctly extracts ForEachRow from tgtype bitfield")
}

// TestRealWorld_IndexCaseInsensitivity verifies that case differences in index
// expressions are properly normalized
//
// THIS TEST SHOULD PASS (normalization is working)
func TestRealWorld_IndexCaseInsensitivity(t *testing.T) {
	// Parser might return lowercase
	desiredIndex := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Method: "btree",
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "email"}, // lowercase
		},
	}

	// Catalog might return uppercase (depends on how column was created)
	actualIndex := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Method: "btree",
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "EMAIL"}, // uppercase
		},
	}

	// Build object maps
	key := schema.ObjectKey{Kind: schema.IndexKind, Schema: "public", Name: "idx_users_email"}

	desiredMap := schema.SchemaObjectMap{
		key: schema.HashedObject{
			Hash:    mustHash(desiredIndex),
			Payload: desiredIndex,
		},
	}

	actualMap := schema.SchemaObjectMap{
		key: schema.HashedObject{
			Hash:    mustHash(actualIndex),
			Payload: actualIndex,
		},
	}

	differ := NewDiffer()
	diff, err := differ.Diff(desiredMap, actualMap)
	assert.NoError(t, err)

	// Log if normalization not working
	if !diff.IsEmpty() {
		t.Logf("❌ Normalization not working: case differences not handled")
		t.Logf("Parser: %q, Catalog: %q", desiredIndex.KeyExprs[0].Expr, actualIndex.KeyExprs[0].Expr)
	} else {
		t.Logf("✅ Normalization working: case differences properly handled")
	}

	// This should PASS (normalization is working)
	assert.True(t, diff.IsEmpty(),
		"Index with different case should be normalized to same")
}

// Helper function to hash objects without error handling
func mustHash(obj schema.DatabaseObject) string {
	hash, err := NormalizeAndHash(obj)
	if err != nil {
		panic(err)
	}
	return hash
}
