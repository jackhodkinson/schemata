package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateWithVersion(t *testing.T) {
	dir := t.TempDir()
	gen := NewGenerator(dir)

	mig, err := gen.GenerateWithVersion("20260225235959", "add-extensions", "CREATE EXTENSION IF NOT EXISTS citext;\n")
	require.NoError(t, err)

	assert.Equal(t, "20260225235959", mig.Version)
	assert.Equal(t, "add-extensions", mig.Name)
	assert.Equal(t, filepath.Join(dir, "20260225235959-add-extensions.sql"), mig.FilePath)

	content, err := os.ReadFile(mig.FilePath)
	require.NoError(t, err)
	assert.Equal(t, "CREATE EXTENSION IF NOT EXISTS citext;\n", string(content))
}

func TestGenerateWithVersion_duplicateFile(t *testing.T) {
	dir := t.TempDir()
	gen := NewGenerator(dir)

	_, err := gen.GenerateWithVersion("20260225235959", "add-extensions", "sql1")
	require.NoError(t, err)

	_, err = gen.GenerateWithVersion("20260225235959", "add-extensions", "sql2")
	assert.ErrorContains(t, err, "already exists")
}
