package normalize

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestExprContract(t *testing.T) {
	t.Run("preserve_cast_without_type_context", func(t *testing.T) {
		got := Expr(schema.Expr("'user'::user_role"))
		require.Equal(t, schema.Expr("'user'::user_role"), got)
	})

	t.Run("normalize_now", func(t *testing.T) {
		got := Expr(schema.Expr("now()"))
		require.Equal(t, schema.Expr("current_timestamp"), got)
	})

	t.Run("preserve_string_literal_case", func(t *testing.T) {
		require.NotEqual(t, Expr("'Admin'"), Expr("'admin'"))
	})

	t.Run("preserve_quoted_identifier_case", func(t *testing.T) {
		require.NotEqual(t, Expr(`"UserName"`), Expr(`"username"`))
	})

	t.Run("preserve_different_casts", func(t *testing.T) {
		require.NotEqual(t, Expr("'1'::integer"), Expr("'1'::text"))
	})
}

func TestExprForTypeRemovesOnlyMatchingCast(t *testing.T) {
	require.Equal(t, schema.Expr("'user'"), exprForType("'user'::user_role", "user_role"))
	require.Equal(t, schema.Expr("'user'::other_role"), exprForType("'user'::other_role", "user_role"))
	require.Equal(t, schema.Expr("'Admin'"), exprForType("'Admin'::text", "text"))
	require.NotEqual(t, exprForType("'Admin'::text", "text"), exprForType("'admin'::text", "text"))
}

func TestObjectPreservesMeaningBearingOrder(t *testing.T) {
	tbl := schema.Table{
		Schema:  "public",
		Name:    "ordered",
		Columns: []schema.Column{{Name: "z", Type: "text"}, {Name: "a", Type: "text"}},
	}
	require.Equal(t, []schema.ColumnName{"z", "a"}, columnNames(Object(tbl).(schema.Table).Columns))

	fn := schema.Function{SearchPath: []schema.SchemaName{"tenant", "public"}}
	require.Equal(t, []schema.SchemaName{"tenant", "public"}, Object(fn).(schema.Function).SearchPath)

	idx := schema.Index{Include: []schema.ColumnName{"z", "a"}}
	require.Equal(t, []schema.ColumnName{"z", "a"}, Object(idx).(schema.Index).Include)

	comp := schema.CompositeDef{Attributes: []schema.CompositeAttr{{Name: "z"}, {Name: "a"}}}
	require.Equal(t, []schema.CompositeAttr{{Name: "z"}, {Name: "a"}}, Object(comp).(schema.CompositeDef).Attributes)
}

func columnNames(cols []schema.Column) []schema.ColumnName {
	names := make([]schema.ColumnName, len(cols))
	for i := range cols {
		names[i] = cols[i].Name
	}
	return names
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
