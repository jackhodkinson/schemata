package integration

import (
	"context"
	"os"
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndToEnd_SchemataMigrate replicates the actual behavior of `schemata migrate`
// by parsing schema.sql, extracting from database, and comparing them.
//
// This test uses the actual schema from ../test-schemata to ensure we catch
// real-world issues, not just mock data scenarios.
func TestEndToEnd_SchemataMigrate(t *testing.T) {
	// Skip if no database available
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgresql://postgres:dev_password@localhost:54320/dev_db"
	}

	ctx := context.Background()

	// Connect to test database
	dbConn := &config.DBConnection{ConnectionString: dbURL}
	pool, err := db.Connect(ctx, dbConn)
	require.NoError(t, err, "Failed to connect to test database")
	defer pool.Close()

	// Clean database and apply schema
	t.Log("Cleaning database and applying schema...")
	err = cleanAndApplySchema(ctx, pool)
	require.NoError(t, err, "Failed to apply schema")

	// Parse schema.sql file (what the user wrote)
	schemaFile := "../../testdata/schema.sql"
	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err, "Failed to parse schema.sql")

	t.Logf("Parsed %d objects from schema.sql", len(desiredSchema))

	// Extract actual schema from database (what's actually in the DB)
	catalog := db.NewCatalog(pool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, nil, nil)
	require.NoError(t, err, "Failed to extract catalog from database")

	// Build object map from catalog
	actualSchema, err := buildObjectMap(actualObjects)
	require.NoError(t, err, "Failed to build object map from catalog")

	t.Logf("Extracted %d objects from database", len(actualSchema))

	// Run diff (this is what `schemata migrate` does internally)
	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err, "Failed to compute diff")

	// Log detailed diff information
	if !diff.IsEmpty() {
		t.Logf("❌ DIFF NOT EMPTY - Found differences:")

		if len(diff.ToCreate) > 0 {
			t.Logf("  Objects to CREATE (%d):", len(diff.ToCreate))
			for _, key := range diff.ToCreate {
				t.Logf("    + %s: %s.%s", key.Kind, key.Schema, key.Name)
			}
		}

		if len(diff.ToDrop) > 0 {
			t.Logf("  Objects to DROP (%d):", len(diff.ToDrop))
			for _, key := range diff.ToDrop {
				t.Logf("    - %s: %s.%s", key.Kind, key.Schema, key.Name)
			}
		}

		if len(diff.ToAlter) > 0 {
			t.Logf("  Objects to ALTER (%d):", len(diff.ToAlter))
			for _, alter := range diff.ToAlter {
				t.Logf("    ~ %s: %s.%s", alter.Key.Kind, alter.Key.Schema, alter.Key.Name)
				for _, change := range alter.Changes {
					t.Logf("        %s", change)
				}
			}
		}
	} else {
		t.Log("✅ Diff is empty - schemas match perfectly!")
	}

	// Assert that diff is empty
	// If this fails, it indicates remaining bugs in:
	// - Parser (not extracting correctly from schema.sql)
	// - Catalog (not extracting correctly from database)
	// - Differ/Normalization (not properly normalizing before comparison)
	assert.True(t, diff.IsEmpty(),
		"Expected no differences between schema.sql and database, but found %d creates, %d drops, %d alters. "+
		"This indicates a bug in parser, catalog extraction, or normalization.",
		len(diff.ToCreate), len(diff.ToDrop), len(diff.ToAlter))
}

// cleanAndApplySchema drops all objects and applies the schema from testdata/schema.sql
func cleanAndApplySchema(ctx context.Context, pool *db.Pool) error {
	// Drop all objects by dropping and recreating the public schema
	dropSQL := `
		DROP SCHEMA IF EXISTS public CASCADE;
		CREATE SCHEMA public;
		GRANT ALL ON SCHEMA public TO postgres;
		GRANT ALL ON SCHEMA public TO public;
	`
	_, err := pool.Exec(ctx, dropSQL)
	if err != nil {
		return err
	}

	// Read schema.sql
	schemaSQL, err := os.ReadFile("../../testdata/schema.sql")
	if err != nil {
		return err
	}

	// Apply schema
	_, err = pool.Exec(ctx, string(schemaSQL))
	return err
}

// buildObjectMap converts a slice of objects to a SchemaObjectMap with hashes
func buildObjectMap(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	objectMap := make(schema.SchemaObjectMap)

	for _, obj := range objects {
		key := schema.GetObjectKey(obj)
		hash, err := differ.NormalizeAndHash(obj)
		if err != nil {
			return nil, err
		}

		objectMap[key] = schema.HashedObject{
			Hash:    hash,
			Payload: obj,
		}
	}

	return objectMap, nil
}
