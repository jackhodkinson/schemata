package test

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/require"
)

// TestPgQueryPatchContract enforces a narrow behavioral contract for our
// patched pg_query_go dependency. If this fails, dependency behavior changed
// and the patch/upgrade must be reviewed before merge.
func TestPgQueryPatchContract(t *testing.T) {
	t.Parallel()

	parseFixtures := []struct {
		name      string
		sql       string
		wantStmts int
	}{
		{
			name:      "simple select",
			sql:       "SELECT 1",
			wantStmts: 1,
		},
		{
			name:      "multi statement",
			sql:       "SELECT 1; SELECT a FROM b",
			wantStmts: 2,
		},
		{
			name:      "ddl statement",
			sql:       "CREATE TEMPORARY TABLE my_temp_table AS SELECT 1",
			wantStmts: 1,
		},
	}

	for _, fixture := range parseFixtures {
		fixture := fixture
		t.Run("parse/"+fixture.name, func(t *testing.T) {
			result, err := pg_query.Parse(fixture.sql)
			require.NoError(t, err, "pg_query parse changed unexpectedly for fixture %q", fixture.name)
			require.Len(t, result.Stmts, fixture.wantStmts, "pg_query parse statement count changed for fixture %q", fixture.name)
		})
	}

	normalizeFixtures := []struct {
		name           string
		sql            string
		wantNormalized string
	}{
		{
			name:           "select one",
			sql:            "SELECT 1",
			wantNormalized: "SELECT $1",
		},
		{
			name:           "select literal preserves shape",
			sql:            "SELECT 2",
			wantNormalized: "SELECT $1",
		},
		{
			name:           "select bind param remains bind param",
			sql:            "SELECT $1",
			wantNormalized: "SELECT $1",
		},
	}

	for _, fixture := range normalizeFixtures {
		fixture := fixture
		t.Run("normalize/"+fixture.name, func(t *testing.T) {
			normalized, err := pg_query.Normalize(fixture.sql)
			require.NoError(t, err, "pg_query normalize changed unexpectedly for fixture %q", fixture.name)
			require.Equal(t, fixture.wantNormalized, normalized, "pg_query normalize output changed for fixture %q", fixture.name)
		})
	}

	fingerprintFixtures := []struct {
		name            string
		sql             string
		wantFingerprint string
	}{
		{
			name:            "select one",
			sql:             "SELECT 1",
			wantFingerprint: "50fde20626009aba",
		},
		{
			name:            "multi statement",
			sql:             "SELECT 1; SELECT a FROM b",
			wantFingerprint: "3efa3b10d558d06d",
		},
		{
			name:            "ddl statement",
			sql:             "CREATE TEMPORARY TABLE my_temp_table AS SELECT 1",
			wantFingerprint: "695ebe73a3abc45c",
		},
	}

	for _, fixture := range fingerprintFixtures {
		fixture := fixture
		t.Run("fingerprint/"+fixture.name, func(t *testing.T) {
			fingerprint, err := pg_query.Fingerprint(fixture.sql)
			require.NoError(t, err, "pg_query fingerprint changed unexpectedly for fixture %q", fixture.name)
			require.Equal(t, fixture.wantFingerprint, string(fingerprint), "pg_query fingerprint output changed for fixture %q", fixture.name)
		})
	}
}
