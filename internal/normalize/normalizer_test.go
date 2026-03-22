package normalize

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestExprContract(t *testing.T) {
	t.Run("strip_enum_cast", func(t *testing.T) {
		got := Expr(schema.Expr("'user'::user_role"))
		require.Equal(t, schema.Expr("'user'"), got)
	})

	t.Run("normalize_now", func(t *testing.T) {
		got := Expr(schema.Expr("now()"))
		require.Equal(t, schema.Expr("current_timestamp"), got)
	})
}

func TestFunctionBodyContract(t *testing.T) {
	in := `
	BEGIN
		NEW.updated_at = CURRENT_TIMESTAMP;
		RETURN NEW;
	END;
	`
	got := FunctionBody(in)
	require.Equal(t, "begin new.updated_at = current_timestamp; return new; end;", got)
}

func TestObjectContract_PolicyExpressionsNormalized(t *testing.T) {
	using := schema.Expr("owner_name = CURRENT_USER")
	withCheck := schema.Expr("(owner_name = CURRENT_USER)")
	pol := schema.Policy{
		Schema:    "public",
		Table:     "docs",
		Name:      "owner_only",
		Using:     &using,
		WithCheck: &withCheck,
		To:        []string{"postgres", "public"},
	}

	got := Object(pol).(schema.Policy)
	require.NotNil(t, got.Using)
	require.NotNil(t, got.WithCheck)
	require.Equal(t, schema.Expr("owner_name = current_user"), *got.Using)
	require.Equal(t, schema.Expr("owner_name = current_user"), *got.WithCheck)
	require.Equal(t, []string{"postgres", "public"}, got.To)
}
