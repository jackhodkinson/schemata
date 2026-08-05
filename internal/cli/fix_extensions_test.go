package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildExtensionMigrationSQLQuotesCatalogNames(t *testing.T) {
	t.Parallel()

	sql, err := buildExtensionMigrationSQL([]string{`uuid-ossp`, `odd"; DROP SCHEMA public; --`})
	require.NoError(t, err)
	assert.Contains(t, sql, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`)
	assert.Contains(t, sql, `CREATE EXTENSION IF NOT EXISTS "odd""; DROP SCHEMA public; --";`)
	assert.NotContains(t, sql, `CREATE EXTENSION IF NOT EXISTS odd";`)
}

func TestBuildExtensionMigrationSQLRejectsUnrepresentableName(t *testing.T) {
	t.Parallel()

	_, err := buildExtensionMigrationSQL([]string{"bad\x00name"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "contains a NUL byte")
}
