package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDumpSchemaFilePath(t *testing.T) {
	assert.True(t, isDumpSchemaFilePath("schema.sql"))
	assert.True(t, isDumpSchemaFilePath("foo/bar/MySchema.SQL"))
	assert.False(t, isDumpSchemaFilePath("schema"))
	assert.False(t, isDumpSchemaFilePath("schema.d"))
	assert.False(t, isDumpSchemaFilePath("dir/schema"))
}

func TestSafeSchemaSQLFileName(t *testing.T) {
	assert.Equal(t, "_empty", safeSchemaSQLFileName(""))
	assert.Equal(t, "public", safeSchemaSQLFileName("public"))
	assert.Equal(t, "a_b", safeSchemaSQLFileName("a/b"))
}

func TestValidateDumpSchemaPath(t *testing.T) {
	tmp := t.TempDir()
	sqlFile := filepath.Join(tmp, "only.sql")
	require.NoError(t, os.WriteFile(sqlFile, []byte("x"), 0644))
	dirPath := filepath.Join(tmp, "dir")
	require.NoError(t, os.MkdirAll(dirPath, 0755))
	// Directory whose name ends with .sql (single-file mode still treats path as file)
	sqlNamedDir := filepath.Join(tmp, "weird.sql")
	require.NoError(t, os.MkdirAll(sqlNamedDir, 0755))
	noExtFile := filepath.Join(tmp, "blob")
	require.NoError(t, os.WriteFile(noExtFile, []byte("x"), 0644))

	assert.NoError(t, validateDumpSchemaPath(filepath.Join(tmp, "missing.sql"), true))
	assert.NoError(t, validateDumpSchemaPath(filepath.Join(tmp, "missing-dir"), false))

	err := validateDumpSchemaPath(sqlNamedDir, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "directory")

	err = validateDumpSchemaPath(noExtFile, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a directory")
}

func TestGroupObjectsBySchemaAndSort(t *testing.T) {
	objs := []schema.DatabaseObject{
		schema.Table{Schema: "app", Name: "z"},
		schema.Table{Schema: "public", Name: "a"},
		schema.Table{Schema: "app", Name: "a"},
	}
	groups := groupObjectsBySchema(objs)
	require.Len(t, groups, 2)
	assert.Len(t, groups["public"], 1)
	assert.Len(t, groups["app"], 2)
	// Within app, sorted by ObjectKey: table "a" before "z"
	app := groups["app"]
	t0, ok0 := app[0].(schema.Table)
	t1, ok1 := app[1].(schema.Table)
	require.True(t, ok0 && ok1)
	assert.Equal(t, schema.TableName("a"), t0.Name)
	assert.Equal(t, schema.TableName("z"), t1.Name)
}

func TestWriteDumpPerSchemaDirCreatesAndWrites(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "out")
	objs := []schema.DatabaseObject{
		schema.Table{Schema: "public", Name: "u", Columns: []schema.Column{{Name: "id", Type: "int4", NotNull: true}}},
		schema.Table{Schema: "sales", Name: "orders", Columns: []schema.Column{{Name: "id", Type: "int4", NotNull: true}}},
	}
	gen := planner.NewDDLGenerator()
	n, err := writeDumpPerSchemaDir(dir, objs, gen)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	_, err = os.Stat(filepath.Join(dir, "public.sql"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "sales.sql"))
	require.NoError(t, err)
}

func TestWriteDumpSingleFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "all.sql")
	objs := []schema.DatabaseObject{
		schema.Table{Schema: "public", Name: "t", Columns: []schema.Column{{Name: "id", Type: "int4", NotNull: true}}},
	}
	gen := planner.NewDDLGenerator()
	n, err := writeDumpSingleFile(path, objs, gen)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	st, err := os.Stat(path)
	require.NoError(t, err)
	assert.False(t, st.IsDir())
}

// TestWriteDumpPerSchemaDirCrossSchemaForeignKey documents per-schema dump behavior when a
// table references another schema: the FK constraint is emitted with the child table (in the
// child schema file) and uses a schema-qualified REFERENCES target. The referenced table DDL
// stays in the referenced schema's file. Load order of files is not topologically sorted—apply
// referenced schemas before dependents (or use a single-file dump) if dependencies matter.
func TestWriteDumpPerSchemaDirCrossSchemaForeignKey(t *testing.T) {
	dir := t.TempDir()
	objs := []schema.DatabaseObject{
		schema.Table{
			Schema: "public",
			Name:   "users",
			Columns: []schema.Column{
				{Name: "id", Type: "INTEGER", NotNull: true},
			},
			PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
		},
		schema.Table{
			Schema: "sales",
			Name:   "orders",
			Columns: []schema.Column{
				{Name: "id", Type: "INTEGER", NotNull: true},
				{Name: "user_id", Type: "INTEGER", NotNull: true},
			},
			PrimaryKey: &schema.PrimaryKey{Cols: []schema.ColumnName{"id"}},
			ForeignKeys: []schema.ForeignKey{
				{
					Name: "orders_user_fk",
					Cols: []schema.ColumnName{"user_id"},
					Ref: schema.ForeignKeyRef{
						Schema: "public",
						Table:  "users",
						Cols:   []schema.ColumnName{"id"},
					},
					OnDelete: schema.NoAction,
					OnUpdate: schema.NoAction,
				},
			},
		},
	}
	gen := planner.NewDDLGenerator()
	n, err := writeDumpPerSchemaDir(dir, objs, gen)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	publicBytes, err := os.ReadFile(filepath.Join(dir, "public.sql"))
	require.NoError(t, err)
	salesBytes, err := os.ReadFile(filepath.Join(dir, "sales.sql"))
	require.NoError(t, err)
	publicSQL := string(publicBytes)
	salesSQL := string(salesBytes)

	assert.Contains(t, publicSQL, "CREATE TABLE public.users")
	assert.NotContains(t, publicSQL, "sales.orders")
	assert.NotContains(t, publicSQL, "orders_user_fk")

	assert.Contains(t, salesSQL, "CREATE TABLE sales.orders")
	assert.Contains(t, salesSQL, "REFERENCES public.users (id)")
	assert.Contains(t, salesSQL, "CONSTRAINT orders_user_fk FOREIGN KEY (user_id)")

	// File emission order is lexicographic by schema name (public.sql then sales.sql), not FK dependency order.
	names := sortedSchemaNames(groupObjectsBySchema(objs))
	require.Len(t, names, 2)
	assert.Equal(t, schema.SchemaName("public"), names[0])
	assert.Equal(t, schema.SchemaName("sales"), names[1])
}
