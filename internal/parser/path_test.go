package parser

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePathFile(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "schema.sql")
	err := os.WriteFile(schemaFile, []byte("CREATE TABLE users (id integer primary key);"), 0644)
	require.NoError(t, err)

	p := NewParser()
	objects, err := p.ParsePath(schemaFile)
	require.NoError(t, err)
	assert.Len(t, objects, 1)

	key := schema.ObjectKey{
		Kind:   schema.TableKind,
		Schema: schema.SchemaName("public"),
		Name:   "users",
	}
	_, ok := objects[key]
	assert.True(t, ok)
}

func TestParsePathDirectoryRecursive(t *testing.T) {
	tmpDir := t.TempDir()
	nestedDir := filepath.Join(tmpDir, "nested", "feature")
	require.NoError(t, os.MkdirAll(nestedDir, 0755))

	err := os.WriteFile(filepath.Join(tmpDir, "a.sql"), []byte("CREATE TABLE public.alpha (id integer primary key);"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(nestedDir, "b.sql"), []byte("CREATE TABLE public.beta (id integer primary key);"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(tmpDir, "ignore.txt"), []byte("CREATE TABLE public.ignored (id integer primary key);"), 0644)
	require.NoError(t, err)

	p := NewParser()
	objects, err := p.ParsePath(tmpDir)
	require.NoError(t, err)
	assert.Len(t, objects, 2)
}

func TestParsePathDirectoryIgnoresHiddenAndTempFiles(t *testing.T) {
	tmpDir := t.TempDir()
	hiddenDir := filepath.Join(tmpDir, ".scratch")
	require.NoError(t, os.MkdirAll(hiddenDir, 0755))

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "valid.sql"), []byte("CREATE TABLE public.visible_table (id integer primary key);"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, ".hidden.sql"), []byte("CREATE TABLE public.hidden_file_table (id integer primary key);"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "#draft#.sql"), []byte("CREATE TABLE public.temp_file_table (id integer primary key);"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "editor.tmp"), []byte("CREATE TABLE public.tmp_file_table (id integer primary key);"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(hiddenDir, "nested.sql"), []byte("CREATE TABLE public.hidden_dir_table (id integer primary key);"), 0644))

	p := NewParser()
	objects, err := p.ParsePath(tmpDir)
	require.NoError(t, err)
	assert.Len(t, objects, 1)

	key := schema.ObjectKey{
		Kind:   schema.TableKind,
		Schema: schema.SchemaName("public"),
		Name:   "visible_table",
	}
	_, ok := objects[key]
	assert.True(t, ok)
}

func TestParsePathDirectoryDuplicateObjectError(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "nested"), 0755))

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "01-a.sql"), []byte("CREATE TABLE public.users (id integer primary key);"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "nested", "02-b.sql"), []byte("CREATE TABLE public.users (id integer primary key);"), 0644))

	p := NewParser()
	_, err := p.ParsePath(tmpDir)
	require.Error(t, err)
	assert.True(t, IsDuplicateObjectError(err))

	var dupErr *DuplicateObjectError
	require.ErrorAs(t, err, &dupErr)
	assert.Contains(t, dupErr.FirstPath, "01-a.sql")
	assert.Contains(t, dupErr.SecondPath, "02-b.sql")
}

func TestParsePathEmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	p := NewParser()

	objects, err := p.ParsePath(tmpDir)
	require.NoError(t, err)
	assert.Empty(t, objects)
}
