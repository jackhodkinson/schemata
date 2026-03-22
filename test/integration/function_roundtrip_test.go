//go:build integration
// +build integration

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
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFunctionRoundTrip_NoDiffOnFormatting checks the contract that parser output
// and catalog output for the same function must compare equal after normalization.
func TestFunctionRoundTrip_NoDiffOnFormatting(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL

	dbConn := &config.DBConnection{URL: &dbURL}
	pool, err := db.Connect(ctx, dbConn)
	require.NoError(t, err, "failed to connect to integration test database")
	defer pool.Close()

	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err = os.WriteFile(schemaFile, []byte(`
CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$function$;
`), 0644)
	require.NoError(t, err)

	err = cleanAndApplySchema(ctx, pool, schemaFile)
	require.NoError(t, err, "failed to apply schema with function definition")

	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err, "failed to parse desired schema file")

	catalog := db.NewCatalog(pool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err, "failed to extract function from catalog")

	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err, "failed to build actual schema map")

	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err, "failed to diff parser and catalog objects")

	fnKey := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "update_updated_at_column",
		Signature: "()",
	}

	assert.NotContains(t, diff.ToCreate, fnKey, "function should not be recreated after round-trip")
	assert.NotContains(t, diff.ToDrop, fnKey, "function should not be dropped after round-trip")
	for _, alter := range diff.ToAlter {
		assert.NotEqual(t, fnKey, alter.Key, "function should not be altered when only formatting differs")
	}
	assert.True(t, diff.IsEmpty(), "function round-trip should produce an empty diff")
}

// TestFunctionRoundTrip_OutArgIdentityStable ensures parser/catalog agree on
// function identity when OUT parameters are present.
func TestFunctionRoundTrip_OutArgIdentityStable(t *testing.T) {
	ctx := context.Background()
	dbURL := devDBURL

	dbConn := &config.DBConnection{URL: &dbURL}
	pool, err := db.Connect(ctx, dbConn)
	require.NoError(t, err, "failed to connect to integration test database")
	defer pool.Close()

	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err = os.WriteFile(schemaFile, []byte(`
CREATE OR REPLACE FUNCTION public.add_one_with_out(IN n integer, OUT result integer)
LANGUAGE sql
AS $function$
    SELECT n + 1;
$function$;
`), 0644)
	require.NoError(t, err)

	err = cleanAndApplySchema(ctx, pool, schemaFile)
	require.NoError(t, err, "failed to apply schema with OUT-arg function definition")

	p := parser.NewParser()
	desiredSchema, err := p.ParseFile(schemaFile)
	require.NoError(t, err, "failed to parse desired schema file")

	catalog := db.NewCatalog(pool)
	actualObjects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "pg_toast", "schemata"})
	require.NoError(t, err, "failed to extract function from catalog")

	actualSchema, err := buildObjectMapFromObjects(actualObjects)
	require.NoError(t, err, "failed to build actual schema map")

	d := differ.NewDiffer()
	diff, err := d.Diff(desiredSchema, actualSchema)
	require.NoError(t, err, "failed to diff parser and catalog objects")

	for _, key := range diff.ToCreate {
		assert.NotEqual(t, "add_one_with_out", key.Name, "OUT-arg function should not show as create")
	}
	for _, key := range diff.ToDrop {
		assert.NotEqual(t, "add_one_with_out", key.Name, "OUT-arg function should not show as drop")
	}
	for _, alter := range diff.ToAlter {
		assert.NotEqual(t, "add_one_with_out", alter.Key.Name, "OUT-arg function should not show as alter")
	}
	assert.True(t, diff.IsEmpty(), "OUT-arg function should round-trip with stable identity")
}
