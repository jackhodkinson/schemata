package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeTypeNameTreatsPublicAsCanonicalDefaultSchema(t *testing.T) {
	t.Parallel()

	require.Equal(t, TypeName("mood"), NormalizeTypeName("public.mood"))
	require.Equal(t, TypeName("mood"), NormalizeTypeName("PUBLIC.mood"))
	require.Equal(t, TypeName(`"Odd Type"`), NormalizeTypeName(`"public"."Odd Type"`))
	require.Equal(t, TypeName("mood[]"), NormalizeTypeName("public.mood[]"))
	require.Equal(t, TypeName("tenant.mood"), NormalizeTypeName("tenant.mood"))
	require.Equal(t, TypeName(`"PUBLIC".mood`), NormalizeTypeName(`"PUBLIC".mood`))
	require.Equal(t, TypeName(`"PG_CATALOG".int4`), NormalizeTypeName(`"PG_CATALOG".int4`))
}

func TestNormalizeTypeNameCanonicalizesArrayElementAliases(t *testing.T) {
	require.Equal(t, TypeName("integer[]"), NormalizeTypeName("int[]"))
	require.Equal(t, TypeName("integer[]"), NormalizeTypeName("pg_catalog.int4[][]"))
	require.Equal(t, TypeName(`"Odd Schema"."Odd Type"[]`), NormalizeTypeName(`"Odd Schema"."Odd Type"[]`))
}
