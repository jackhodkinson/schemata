package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSchemaFilterRendersValuesAsLiterals(t *testing.T) {
	t.Parallel()

	catalog := &Catalog{}
	filter, err := catalog.buildSchemaFilter([]string{
		"select",
		"MixedCase",
		"tenant.data",
		"顧客",
		"x'); DROP SCHEMA public; --",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t,
		"nspname IN ('select', 'MixedCase', 'tenant.data', '顧客', 'x''); DROP SCHEMA public; --')",
		filter,
	)
}

func TestBuildSchemaFilterRejectsNUL(t *testing.T) {
	t.Parallel()

	_, err := (&Catalog{}).buildSchemaFilter([]string{"public\x00ignored"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NUL")
}
