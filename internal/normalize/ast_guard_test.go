package normalize

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/require"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

func TestExprRejectsASTThatIsUnsafeToDeparse(t *testing.T) {
	t.Parallel()

	// This shape is the deterministic form of fuzz seed b0cf370b710abb14.
	// Parsing succeeds, but sending its deeply nested AST to libpg_query's C
	// deparser used to exhaust the C stack and terminate the process with SIGBUS.
	input := schema.Expr("ԉ" + strings.Repeat("+", 4_000) + "0")

	_, err := canonicalizeExprOnce(string(input))
	require.ErrorContains(t, err, "depth exceeds safe limit")
	require.Equal(t, input, Expr(input))
}

func TestViewPreservesQueryWhenASTIsUnsafeToDeparse(t *testing.T) {
	t.Parallel()

	query := "SELECT ԉ" + strings.Repeat("+", 4_000) + "0"
	view := schema.View{Definition: schema.ViewDefinition{Query: query}}

	normalized := Object(view).(schema.View)
	require.Equal(t, query, normalized.Definition.Query)
}

func TestDeparseASTResourceGuard(t *testing.T) {
	t.Parallel()

	parsed, err := pg_query.Parse(`SELECT some_function('payload', ARRAY[1, 2, 3])`)
	require.NoError(t, err)

	generous := astResourceLimits{
		maxDepth:              1_000,
		maxMessages:           1_000,
		maxCollectionElements: 1_000,
		maxScalarBytes:        1_000,
	}
	require.NoError(t, validateASTResources(parsed.ProtoReflect(), generous))

	tests := []struct {
		name       string
		limits     astResourceLimits
		errorMatch string
	}{
		{
			name:       "depth",
			limits:     astResourceLimits{1, 1_000, 1_000, 1_000},
			errorMatch: "depth exceeds safe limit",
		},
		{
			name:       "message count",
			limits:     astResourceLimits{1_000, 1, 1_000, 1_000},
			errorMatch: "message count exceeds safe limit",
		},
		{
			name:       "collection elements",
			limits:     astResourceLimits{1_000, 1_000, 1, 1_000},
			errorMatch: "collection element count exceeds safe limit",
		},
		{
			name:       "scalar bytes",
			limits:     astResourceLimits{1_000, 1_000, 1_000, 1},
			errorMatch: "scalar byte count exceeds safe limit",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateASTResources(parsed.ProtoReflect(), test.limits)
			require.ErrorContains(t, err, test.errorMatch)
		})
	}
}
