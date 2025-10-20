package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

// These tests replicate the ACTUAL failure modes we see in ../test-schemata
// They should FAIL to demonstrate the bugs exist, then PASS once we fix them

// TestRealWorld_IndexKeyExpressionMismatch replicates the actual bug where
// catalog extracts full CREATE INDEX statement but parser extracts just column name
//
// THIS TEST SHOULD FAIL until we fix catalog.go line 714
func TestRealWorld_IndexKeyExpressionMismatch(t *testing.T) {
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

	// This is what the CATALOG extracts from the database
	actualIndex := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Method: "btree",
		KeyExprs: []schema.IndexKeyExpr{
			// The TODO in catalog.go line 714 - it stores the whole CREATE INDEX statement!
			{Expr: "CREATE INDEX idx_users_email ON public.users USING btree (email)"},
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

	// Log if bug detected
	if !diff.IsEmpty() {
		t.Logf("❌ BUG DETECTED: Index incorrectly flagged as changed")
		t.Logf("Parser extracted: %q", desiredIndex.KeyExprs[0].Expr)
		t.Logf("Catalog extracted: %q", actualIndex.KeyExprs[0].Expr)
		if len(diff.ToAlter) > 0 {
			t.Logf("Changes: %v", diff.ToAlter[0].Changes)
		}
	}

	// This assertion should PASS when bug is fixed, FAIL when bug exists
	assert.True(t, diff.IsEmpty(),
		"Index should be considered identical but was flagged as different. "+
			"FIX: Update catalog.go extractIndexes() to query pg_index.indkey instead of using pg_get_indexdef()")
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

// TestRealWorld_TriggerForEachRowExtraction tests if catalog correctly extracts
// the FOR EACH ROW property from pg_trigger.tgtype bitfield
//
// THIS TEST SHOULD FAIL until we fix the catalog extraction
func TestRealWorld_TriggerForEachRowExtraction(t *testing.T) {
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

	// This simulates what the CATALOG would extract if the tgtype bitfield parsing is wrong
	// In reality, the trigger IS "FOR EACH ROW" but if we're not parsing tgtype correctly,
	// we might extract it as false
	actualTrigger := schema.Trigger{
		Schema:     "public",
		Table:      "users",
		Name:       "update_users_updated_at",
		Timing:     schema.Before,
		Events:     []schema.TriggerEvent{schema.Update},
		ForEachRow: false, // BUG: catalog extraction might be wrong
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

	// Log if bug detected
	if !diff.IsEmpty() {
		t.Logf("❌ BUG DETECTED: Trigger incorrectly flagged as changed")
		t.Logf("Parser extracted ForEachRow: %v", desiredTrigger.ForEachRow)
		t.Logf("Catalog extracted ForEachRow: %v", actualTrigger.ForEachRow)
		if len(diff.ToAlter) > 0 {
			t.Logf("Changes: %v", diff.ToAlter[0].Changes)
		}
	}

	// This assertion should PASS when catalog extraction is fixed, FAIL when it's broken
	// We're testing that when BOTH triggers are FOR EACH ROW, they should be considered identical
	// But if catalog extracts it wrong, this will fail
	assert.True(t, diff.IsEmpty(),
		"Triggers should be considered identical (both are FOR EACH ROW in reality). "+
			"FIX: Check catalog.go extractTriggers() - ensure tgtype bitfield is parsed correctly for FOR EACH ROW flag")
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
