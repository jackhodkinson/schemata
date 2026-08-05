package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDumpPhasesOwnedSequenceAfterTargetTable(t *testing.T) {
	t.Parallel()

	defaultExpr := schema.Expr(`nextval('public.item_ids'::regclass)`)
	objects := []schema.DatabaseObject{
		schema.Table{
			Schema: "public", Name: "items",
			Columns: []schema.Column{{Name: "id", Type: "integer", Default: &defaultExpr}},
		},
		schema.Sequence{
			Schema: "public", Name: "item_ids", Type: "integer",
			OwnedBy: &schema.SequenceOwner{Schema: "public", Table: "items", Column: "id"},
		},
	}

	t.Run("single file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "schema.sql")
		_, err := writeDumpSingleFile(path, objects, planner.NewDDLGenerator())
		require.NoError(t, err)
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		assertOwnedSequenceDumpOrder(t, string(contents))
	})

	t.Run("per schema", func(t *testing.T) {
		dir := t.TempDir()
		_, err := writeDumpPerSchemaDir(dir, objects, planner.NewDDLGenerator())
		require.NoError(t, err)
		contents, err := os.ReadFile(filepath.Join(dir, "public.sql"))
		require.NoError(t, err)
		assertOwnedSequenceDumpOrder(t, string(contents))
	})
}

func TestPerSchemaDumpRejectsCrossSchemaOwnedSequence(t *testing.T) {
	t.Parallel()

	objects := []schema.DatabaseObject{
		schema.Table{Schema: "tables", Name: "items", Columns: []schema.Column{{Name: "id", Type: "integer"}}},
		schema.Sequence{
			Schema: "sequences", Name: "item_ids", Type: "integer",
			OwnedBy: &schema.SequenceOwner{Schema: "tables", Table: "items", Column: "id"},
		},
	}
	dir := filepath.Join(t.TempDir(), "dump")
	_, err := writeDumpPerSchemaDir(dir, objects, planner.NewDDLGenerator())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PostgreSQL requires")
	_, statErr := os.Stat(dir)
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestPerSchemaOrderingIncludesCrossSchemaSequenceDefault(t *testing.T) {
	t.Parallel()

	defaultExpr := schema.Expr(`nextval('z_sequences.item_ids'::regclass)`)
	objects := []schema.DatabaseObject{
		schema.Table{
			Schema: "a_tables", Name: "items",
			Columns: []schema.Column{{Name: "id", Type: "integer", Default: &defaultExpr}},
		},
		schema.Sequence{Schema: "z_sequences", Name: "item_ids", Type: "integer"},
	}
	names := sortedSchemaNames(groupObjectsBySchema(objects))
	require.Equal(t, []schema.SchemaName{"z_sequences", "a_tables"}, names)
}

func assertOwnedSequenceDumpOrder(t *testing.T, ddl string) {
	t.Helper()
	fragments := []string{
		`CREATE SEQUENCE "public"."item_ids" AS integer;`,
		`CREATE TABLE "public"."items"`,
		`ALTER SEQUENCE "public"."item_ids" OWNED BY "public"."items"."id";`,
	}
	position := -1
	for _, fragment := range fragments {
		next := strings.Index(ddl, fragment)
		require.Greater(t, next, position, "fragment %q missing or out of order in:\n%s", fragment, ddl)
		position = next
	}
}
