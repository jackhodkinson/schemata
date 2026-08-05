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

func TestExprCanonicalizesPublicRegclassQualification(t *testing.T) {
	t.Parallel()

	assertions := []struct {
		qualified   schema.Expr
		unqualified schema.Expr
	}{
		{`nextval('public.item_ids'::regclass)`, `nextval('item_ids'::regclass)`},
		{`nextval('PUBLIC.item_ids'::pg_catalog.regclass)`, `nextval('item_ids'::regclass)`},
		{`nextval('"public"."Item IDs"'::regclass)`, `nextval('"Item IDs"'::regclass)`},
	}
	for _, assertion := range assertions {
		require.Equal(t, Expr(assertion.unqualified), Expr(assertion.qualified))
	}

	require.NotEqual(t,
		Expr(`nextval('tenant.item_ids'::regclass)`),
		Expr(`nextval('item_ids'::regclass)`),
	)
	require.NotEqual(t,
		Expr(`nextval('"PUBLIC".item_ids'::regclass)`),
		Expr(`nextval('item_ids'::regclass)`),
	)
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

func TestViewNormalizationTreatsPublicAsCanonicalDefaultSchema(t *testing.T) {
	t.Parallel()

	unqualified := Object(schema.View{Definition: schema.ViewDefinition{Query: `SELECT id FROM items`}}).(schema.View)
	qualified := Object(schema.View{Definition: schema.ViewDefinition{Query: `SELECT id FROM public.items`}}).(schema.View)
	require.Equal(t, unqualified.Definition.Query, qualified.Definition.Query)
	quotedPublic := Object(schema.View{Definition: schema.ViewDefinition{Query: `SELECT id FROM "public".items`}}).(schema.View)
	require.Equal(t, unqualified.Definition.Query, quotedPublic.Definition.Query)

	otherSchema := Object(schema.View{Definition: schema.ViewDefinition{Query: `SELECT id FROM tenant.items`}}).(schema.View)
	require.NotEqual(t, unqualified.Definition.Query, otherSchema.Definition.Query)
	quotedUppercasePublic := Object(schema.View{Definition: schema.ViewDefinition{Query: `SELECT id FROM "PUBLIC".items`}}).(schema.View)
	require.NotEqual(t, unqualified.Definition.Query, quotedUppercasePublic.Definition.Query)
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

func TestFunctionBodyPreservesLineCommentTerminator(t *testing.T) {
	t.Parallel()

	withTerminator := FunctionBody("SELECT 1 -- Keep Case\r\n + 2")
	withoutTerminator := FunctionBody("SELECT 1 -- Keep Case + 2")

	require.Equal(t, "select 1 -- Keep Case\n + 2", withTerminator)
	require.NotEqual(t, withTerminator, withoutTerminator)
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
