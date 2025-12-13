//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//TestSerialNormalizationRealDB tests SERIAL normalization with a real database
func TestSerialNormalizationRealDB(t *testing.T) {
	ctx := context.Background()

	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	// Clean up before and after
	_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS test_serial CASCADE")
	defer func() {
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS test_serial CASCADE")
	}()

	// Create a table with SERIAL column
	_, err = devPool.Exec(ctx, `
		CREATE TABLE test_serial (
			id SERIAL PRIMARY KEY,
			name TEXT
		);
	`)
	require.NoError(t, err)

	// Extract objects from catalog
	catalog := db.NewCatalog(devPool)
	objects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "schemata"})
	require.NoError(t, err)

	// Find the table and sequence
	var table *schema.Table
	var sequences []schema.Sequence

	for _, obj := range objects {
		switch v := obj.(type) {
		case schema.Table:
			if v.Name == "test_serial" {
				tableCopy := v
				table = &tableCopy
				t.Logf("Found table: %s", v.Name)
				for i, col := range v.Columns {
					t.Logf("  Column[%d]: %s type=%s notNull=%v default=%v",
						i, col.Name, col.Type, col.NotNull, col.Default)
				}
			}
		case schema.Sequence:
			sequences = append(sequences, v)
			t.Logf("Found sequence: %s.%s owned_by=%v type=%s",
				v.Schema, v.Name, v.OwnedBy, v.Type)
		}
	}

	require.NotNil(t, table, "test_serial table should be extracted")
	require.Len(t, table.Columns, 2, "should have 2 columns")

	// Check id column
	idCol := table.Columns[0]
	assert.Equal(t, schema.ColumnName("id"), idCol.Name)

	// THIS IS THE KEY ASSERTION: After normalization, SERIAL should be detected
	assert.Equal(t, schema.TypeName("serial"), idCol.Type,
		"id column should be normalized to 'serial' type")
	assert.Nil(t, idCol.Default,
		"SERIAL column should have nil default after normalization")

	// Verify sequence was filtered out (it should NOT be in objects list)
	foundSequence := false
	for _, seq := range sequences {
		if seq.Name == "test_serial_id_seq" {
			foundSequence = true
			t.Logf("ERROR: Sequence %s should have been filtered out but was found", seq.Name)
		}
	}
	assert.False(t, foundSequence,
		"Sequence owned by SERIAL column should be filtered out from objects list")
}

// TestMultipleSerialTypes tests different SERIAL variants
func TestMultipleSerialTypes(t *testing.T) {
	ctx := context.Background()

	devConn := &config.DBConnection{URL: strPtr(devDBURL)}
	devPool, err := db.Connect(ctx, devConn)
	require.NoError(t, err)
	defer devPool.Close()

	// Clean up before and after
	_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS test_serials CASCADE")
	defer func() {
		_, _ = devPool.Exec(ctx, "DROP TABLE IF EXISTS test_serials CASCADE")
	}()

	// Create table with different SERIAL types
	_, err = devPool.Exec(ctx, `
		CREATE TABLE test_serials (
			small_id SMALLSERIAL,
			normal_id SERIAL,
			big_id BIGSERIAL
		);
	`)
	require.NoError(t, err)

	// Extract and verify
	catalog := db.NewCatalog(devPool)
	objects, err := catalog.ExtractAllObjects(ctx, []string{"public"}, []string{"pg_catalog", "information_schema", "schemata"})
	require.NoError(t, err)

	var table *schema.Table
	for _, obj := range objects {
		if tbl, ok := obj.(schema.Table); ok && tbl.Name == "test_serials" {
			tableCopy := tbl
			table = &tableCopy
			break
		}
	}

	require.NotNil(t, table)
	require.Len(t, table.Columns, 3)

	// Debug: show what we got
	for i, col := range table.Columns {
		t.Logf("Column[%d]: %s type=%s default=%v", i, col.Name, col.Type, col.Default)
	}

	// Find sequences
	var seqs []schema.Sequence
	for _, obj := range objects {
		if seq, ok := obj.(schema.Sequence); ok {
			t.Logf("Sequence: %s type=%s owned_by=%v", seq.Name, seq.Type, seq.OwnedBy)
			seqs = append(seqs, seq)
		}
	}

	// Check each column type
	assert.Equal(t, schema.TypeName("smallserial"), table.Columns[0].Type)
	assert.Equal(t, schema.TypeName("serial"), table.Columns[1].Type)
	assert.Equal(t, schema.TypeName("bigserial"), table.Columns[2].Type)

	// All should have nil defaults
	assert.Nil(t, table.Columns[0].Default)
	assert.Nil(t, table.Columns[1].Default)
	assert.Nil(t, table.Columns[2].Default)
}
