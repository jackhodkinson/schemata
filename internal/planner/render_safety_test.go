package planner

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDDLRenderingQuotesEveryIdentifierComponent(t *testing.T) {
	t.Parallel()

	comment := "customer's record'); DROP TABLE audit; --"
	table := schema.Table{
		Schema:  "Mixed.Schema",
		Name:    "select",
		Comment: &comment,
		Columns: []schema.Column{
			{Name: `na"me`, Type: "text"},
			{Name: "顧客", Type: "integer"},
		},
		PrimaryKey: &schema.PrimaryKey{
			Name: ptrString(`pk"; DROP SCHEMA public; --`),
			Cols: []schema.ColumnName{`na"me`},
		},
	}

	ddl, err := NewDDLGenerator().GenerateCreateStatement(table)
	require.NoError(t, err)
	assert.Contains(t, ddl, `CREATE TABLE "Mixed.Schema"."select"`)
	assert.Contains(t, ddl, `"na""me" text`)
	assert.Contains(t, ddl, `"顧客" integer`)
	assert.Contains(t, ddl, `CONSTRAINT "pk""; DROP SCHEMA public; --"`)
	assert.Contains(t, ddl, `COMMENT ON TABLE "Mixed.Schema"."select" IS 'customer''s record''); DROP TABLE audit; --';`)
	_, err = pg_query.Parse(ddl)
	require.NoError(t, err, ddl)
}

func TestGrantRenderingQuotesRolesAndKeepsPublicExplicit(t *testing.T) {
	t.Parallel()

	table := schema.Table{Schema: "tenant.data", Name: "select"}
	assert.Equal(t,
		`GRANT SELECT ON TABLE "tenant.data"."select" TO "ops""; DROP ROLE admin; --";`,
		formatTableGrant(table, `ops"; DROP ROLE admin; --`, []schema.Privilege{schema.PrivSelect}, false),
	)
	assert.Equal(t,
		`GRANT SELECT ON TABLE "tenant.data"."select" TO PUBLIC;`,
		formatTableGrant(table, "PUBLIC", []schema.Privilege{schema.PrivSelect}, false),
	)
	assert.Equal(t,
		`GRANT SELECT ON TABLE "tenant.data"."select" TO "public";`,
		formatTableGrant(table, "public", []schema.Privilege{schema.PrivSelect}, false),
	)
}

func TestDDLRenderingRejectsUnrepresentableOrAmbiguousNames(t *testing.T) {
	t.Parallel()

	_, err := NewDDLGenerator().GenerateCreateStatement(schema.Table{
		Schema: "public",
		Name:   "users\x00ignored",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NUL")

	ambiguous := "catalog.schema.collation"
	_, err = NewDDLGenerator().GenerateCreateStatement(schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{{
			Name:      "name",
			Type:      "text",
			Collation: &ambiguous,
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than 2 components")
}

func TestDDLRenderingAcceptsExplicitlyQuotedDottedQualifiedName(t *testing.T) {
	t.Parallel()

	collation := `"odd.schema"."select"`
	ddl, err := NewDDLGenerator().GenerateCreateStatement(schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{{
			Name:      "name",
			Type:      "text",
			Collation: &collation,
		}},
	})
	require.NoError(t, err)
	assert.Contains(t, ddl, `COLLATE "odd.schema"."select"`)
}

func ptrString(value string) *string { return &value }
