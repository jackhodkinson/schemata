package planner

import (
	"strings"
	"testing"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTableEmitsMetadataInDeterministicOrder(t *testing.T) {
	t.Parallel()
	owner := "app_owner"
	comment := "items"
	columnComment := "identity"
	table := schema.Table{
		Schema:  "public",
		Name:    "items",
		Owner:   &owner,
		Comment: &comment,
		Columns: []schema.Column{{Name: "id", Type: "integer", Comment: &columnComment}},
		Grants: []schema.Grant{
			{Grantee: schema.RoleGrantee("PUBLIC"), Privileges: []schema.Privilege{schema.PrivSelect}},
			{Grantee: schema.PublicGrantee(), Privileges: []schema.Privilege{schema.PrivSelect}},
		},
	}

	ddl, err := NewDDLGenerator().GenerateCreateStatement(table)
	require.NoError(t, err)
	assertOrdered(t, ddl,
		`CREATE TABLE "public"."items"`,
		`ALTER TABLE "public"."items" OWNER TO "app_owner";`,
		`COMMENT ON TABLE "public"."items" IS 'items';`,
		`COMMENT ON COLUMN "public"."items"."id" IS 'identity';`,
		`REVOKE ALL ON TABLE "public"."items" FROM PUBLIC;`,
		`REVOKE ALL ON TABLE "public"."items" FROM "app_owner";`,
		`GRANT SELECT ON TABLE "public"."items" TO PUBLIC;`,
		`GRANT SELECT ON TABLE "public"."items" TO "PUBLIC";`,
	)
}

func TestCreateSequenceEmitsCompleteMetadata(t *testing.T) {
	t.Parallel()
	owner := "app_owner"
	comment := "ids"
	sequence := schema.Sequence{
		Schema:  "public",
		Name:    "item_ids",
		Type:    "integer",
		Owner:   &owner,
		Comment: &comment,
		OwnedBy: &schema.SequenceOwner{Schema: "public", Table: "items", Column: "id"},
		Grants:  []schema.Grant{{Grantee: schema.RoleGrantee("app"), Privileges: []schema.Privilege{schema.PrivUsage}}},
	}
	ddl, err := NewDDLGenerator().GenerateCreateStatement(sequence)
	require.NoError(t, err)
	assertOrdered(t, ddl,
		`CREATE SEQUENCE "public"."item_ids" AS integer;`,
		`ALTER SEQUENCE "public"."item_ids" OWNER TO "app_owner";`,
		`ALTER SEQUENCE "public"."item_ids" OWNED BY "public"."items"."id";`,
		`COMMENT ON SEQUENCE "public"."item_ids" IS 'ids';`,
		`GRANT USAGE ON SEQUENCE "public"."item_ids" TO "app";`,
	)
}

func TestCreateViewFunctionAndTypeEmitMetadata(t *testing.T) {
	t.Parallel()
	owner := "app_owner"
	comment := "metadata"
	grant := []schema.Grant{{Grantee: schema.RoleGrantee("app"), Privileges: []schema.Privilege{schema.PrivSelect}}}

	viewDDL, err := NewDDLGenerator().GenerateCreateStatement(schema.View{
		Schema: "public", Name: "items_v", Type: schema.MaterializedView, Owner: &owner, Comment: &comment,
		Definition: schema.ViewDefinition{Query: "SELECT 1 AS id"}, Grants: grant,
	})
	require.NoError(t, err)
	assert.Contains(t, viewDDL, `ALTER MATERIALIZED VIEW "public"."items_v" OWNER TO "app_owner";`)
	assert.Contains(t, viewDDL, `COMMENT ON MATERIALIZED VIEW "public"."items_v" IS 'metadata';`)
	assert.Contains(t, viewDDL, `GRANT SELECT ON TABLE "public"."items_v" TO "app";`)

	functionDDL, err := NewDDLGenerator().GenerateCreateStatement(schema.Function{
		Schema: "public", Name: "lookup", Owner: &owner,
		Args:    []schema.FunctionArg{{Mode: schema.InMode, Type: "integer"}, {Mode: schema.OutMode, Name: stringPointer("result"), Type: "text"}},
		Returns: schema.ReturnsType{Type: "text"}, Language: schema.SQL, Body: "SELECT 'x'", Comment: &comment,
		Grants: []schema.Grant{{Grantee: schema.RoleGrantee("app"), Privileges: []schema.Privilege{schema.PrivExecute}}},
	})
	require.NoError(t, err)
	assert.Contains(t, functionDDL, `ALTER FUNCTION "public"."lookup"(integer) OWNER TO "app_owner";`)
	assert.NotContains(t, functionDDL, `lookup(integer,text)`)
	assert.Contains(t, functionDDL, `COMMENT ON FUNCTION "public"."lookup"(integer) IS 'metadata';`)
	assert.Contains(t, functionDDL, `GRANT EXECUTE ON FUNCTION "public"."lookup"(integer) TO "app";`)

	typeDDL, err := NewDDLGenerator().GenerateCreateStatement(schema.EnumDef{
		Schema: "public", Name: "mood", Owner: &owner, Values: []string{"ok"}, Comment: &comment,
		Grants: []schema.Grant{{Grantee: schema.RoleGrantee("app"), Privileges: []schema.Privilege{schema.PrivUsage}}},
	})
	require.NoError(t, err)
	assert.Contains(t, typeDDL, `ALTER TYPE "public"."mood" OWNER TO "app_owner";`)
	assert.Contains(t, typeDDL, `COMMENT ON TYPE "public"."mood" IS 'metadata';`)
	assert.Contains(t, typeDDL, `GRANT USAGE ON TYPE "public"."mood" TO "app";`)
}

func TestCreateRejectsInvalidGranteeKinds(t *testing.T) {
	t.Parallel()
	_, err := NewDDLGenerator().GenerateCreateStatement(schema.Table{
		Schema: "public", Name: "items",
		Grants: []schema.Grant{{Grantee: schema.Grantee{}, Privileges: []schema.Privilege{schema.PrivSelect}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown grantee kind")
}

func TestTableGrantRendersMaintainPrivilege(t *testing.T) {
	t.Parallel()

	ddl, err := NewDDLGenerator().GenerateCreateStatement(schema.Table{
		Schema: "public", Name: "items",
		Grants: []schema.Grant{{Grantee: schema.RoleGrantee("maintenance_worker"), Privileges: []schema.Privilege{schema.PrivMaintain}}},
	})
	require.NoError(t, err)
	assert.Contains(t, ddl, `GRANT MAINTAIN ON TABLE "public"."items" TO "maintenance_worker";`)
}

func TestCreateRejectsVersionDependentAllPrivilege(t *testing.T) {
	t.Parallel()

	_, err := NewDDLGenerator().GenerateCreateStatement(schema.Table{
		Schema: "public", Name: "items",
		Grants: []schema.Grant{{Grantee: schema.RoleGrantee("app"), Privileges: []schema.Privilege{schema.PrivAll}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown privilege")
}

func TestSequenceStructuralAlterFailsClosed(t *testing.T) {
	t.Parallel()

	oldStart := int64(1)
	newStart := int64(20)
	actual, err := objectmap.Build([]schema.DatabaseObject{schema.Sequence{
		Schema: "public", Name: "ids", Type: "bigint", Start: &oldStart,
	}})
	require.NoError(t, err)
	desired, err := objectmap.Build([]schema.DatabaseObject{schema.Sequence{
		Schema: "public", Name: "ids", Type: "bigint", Start: &newStart,
	}})
	require.NoError(t, err)
	diff, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	_, err = NewDDLGenerator().GenerateDDL(diff, desired, actual)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "explicit ALTER SEQUENCE migration")
}

func TestOwnedSequenceCreateIsPhasedAroundTargetTable(t *testing.T) {
	t.Parallel()

	defaultExpr := schema.Expr(`nextval('public.item_ids'::regclass)`)
	objects := []schema.DatabaseObject{
		schema.Table{
			Schema: "public", Name: "items",
			Columns: []schema.Column{{Name: "id", Type: "integer", Default: &defaultExpr}},
		},
		schema.Sequence{
			Schema: "public", Name: "item_ids", Type: "integer",
			OwnedBy: &schema.SequenceOwner{Schema: "public", Table: "items", Column: "id"},
		},
	}

	ddl, err := NewDDLGenerator().GenerateCreateObjects(objects)
	require.NoError(t, err)
	assertOrdered(t, ddl,
		`CREATE SEQUENCE "public"."item_ids" AS integer;`,
		`CREATE TABLE "public"."items"`,
		`DEFAULT nextval('public.item_ids'::regclass)`,
		`ALTER SEQUENCE "public"."item_ids" OWNED BY "public"."items"."id";`,
	)
}

func TestGenerateDDLRejectsCreateMissingFromDesiredMap(t *testing.T) {
	t.Parallel()

	key := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "missing"}
	_, err := NewDDLGenerator().GenerateDDL(&differ.Diff{ToCreate: []schema.ObjectKey{key}}, schema.SchemaObjectMap{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing from the desired object map")
}

func TestOwnedSequenceRejectsMismatchedOwners(t *testing.T) {
	t.Parallel()

	tableOwner := "table_owner"
	sequenceOwner := "sequence_owner"
	objects := []schema.DatabaseObject{
		schema.Table{Schema: "public", Name: "items", Owner: &tableOwner, Columns: []schema.Column{{Name: "id", Type: "integer"}}},
		schema.Sequence{
			Schema: "public", Name: "item_ids", Owner: &sequenceOwner,
			OwnedBy: &schema.SequenceOwner{Schema: "public", Table: "items", Column: "id"},
		},
	}

	_, err := NewDDLGenerator().GenerateCreateObjects(objects)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "owner \"sequence_owner\" must match")
}

func TestOwnedSequenceRejectsCrossSchemaBinding(t *testing.T) {
	t.Parallel()

	objects := []schema.DatabaseObject{
		schema.Table{Schema: "tables", Name: "items", Columns: []schema.Column{{Name: "id", Type: "integer"}}},
		schema.Sequence{
			Schema: "sequences", Name: "item_ids",
			OwnedBy: &schema.SequenceOwner{Schema: "tables", Table: "items", Column: "id"},
		},
	}
	_, err := NewDDLGenerator().GenerateCreateObjects(objects)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same schema")
}

func TestGenerateCreateStatementRejectsCrossSchemaOwnedSequence(t *testing.T) {
	t.Parallel()

	_, err := NewDDLGenerator().GenerateCreateStatement(schema.Sequence{
		Schema: "sequences",
		Name:   "item_ids",
		OwnedBy: &schema.SequenceOwner{
			Schema: "tables",
			Table:  "items",
			Column: "id",
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same schema")
}

func TestUnchangedOwnedSequenceDoesNotBlockUnrelatedPlanWithoutActualMap(t *testing.T) {
	t.Parallel()

	sequenceOwner := "existing_owner"
	desired, err := objectmap.Build([]schema.DatabaseObject{
		schema.Table{Schema: "public", Name: "items", Columns: []schema.Column{{Name: "id", Type: "integer"}}},
		schema.Sequence{
			Schema: "public", Name: "item_ids", Owner: &sequenceOwner,
			OwnedBy: &schema.SequenceOwner{Schema: "public", Table: "items", Column: "id"},
		},
		schema.Table{Schema: "public", Name: "unrelated", Columns: []schema.Column{{Name: "id", Type: "integer"}}},
	})
	require.NoError(t, err)
	unrelatedKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "unrelated"}

	ddl, err := NewDDLGenerator().GenerateDDL(&differ.Diff{ToCreate: []schema.ObjectKey{unrelatedKey}}, desired)
	require.NoError(t, err)
	assert.Contains(t, ddl, `CREATE TABLE "public"."unrelated"`)
}

func TestOwnedSequenceOwnerTransitionRunsTableFirstAndRebuildsACL(t *testing.T) {
	t.Parallel()

	oldOwner := "old_owner"
	newOwner := "new_owner"
	ownedBy := &schema.SequenceOwner{Schema: "public", Table: "items", Column: "id"}
	actual, err := objectmap.Build([]schema.DatabaseObject{
		schema.Table{Schema: "public", Name: "items", Owner: &oldOwner, Columns: []schema.Column{{Name: "id", Type: "integer"}}},
		schema.Sequence{
			Schema: "public", Name: "item_ids", Owner: &oldOwner, OwnedBy: ownedBy,
			Grants: []schema.Grant{{Grantee: schema.RoleGrantee("stale_reader"), Privileges: []schema.Privilege{schema.PrivUsage}}},
		},
	})
	require.NoError(t, err)
	desired, err := objectmap.Build([]schema.DatabaseObject{
		schema.Table{Schema: "public", Name: "items", Owner: &newOwner, Columns: []schema.Column{{Name: "id", Type: "integer"}}},
		schema.Sequence{
			Schema: "public", Name: "item_ids", Owner: &newOwner, OwnedBy: ownedBy,
			Grants: []schema.Grant{{Grantee: schema.RoleGrantee("reader"), Privileges: []schema.Privilege{schema.PrivUsage}}},
		},
	})
	require.NoError(t, err)
	diff, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)

	ddl, err := NewDDLGenerator().GenerateDDL(diff, desired, actual)
	require.NoError(t, err)
	assertOrdered(t, ddl,
		`ALTER TABLE "public"."items" OWNER TO "new_owner";`,
		`REVOKE ALL ON SEQUENCE "public"."item_ids" FROM PUBLIC;`,
		`REVOKE ALL ON SEQUENCE "public"."item_ids" FROM "new_owner";`,
		`REVOKE ALL ON SEQUENCE "public"."item_ids" FROM "stale_reader";`,
		`GRANT USAGE ON SEQUENCE "public"."item_ids" TO "reader";`,
	)
	assert.NotContains(t, ddl, `ALTER SEQUENCE "public"."item_ids" OWNER TO "new_owner";`)
}

func TestOwnerChangeRebuildsManagedACLAfterOwnershipTransfer(t *testing.T) {
	t.Parallel()
	oldOwner := "old_owner"
	newOwner := "new_owner"
	actual, err := objectmap.Build([]schema.DatabaseObject{schema.Table{
		Schema: "public", Name: "items", Owner: &oldOwner,
		Grants: []schema.Grant{
			{Grantee: schema.RoleGrantee("old_owner"), Privileges: []schema.Privilege{schema.PrivSelect}},
			{Grantee: schema.RoleGrantee("stale_reader"), Privileges: []schema.Privilege{schema.PrivSelect}},
		},
	}})
	require.NoError(t, err)
	desired, err := objectmap.Build([]schema.DatabaseObject{schema.Table{
		Schema: "public", Name: "items", Owner: &newOwner,
		Grants: []schema.Grant{{Grantee: schema.RoleGrantee("reader"), Privileges: []schema.Privilege{schema.PrivSelect}}},
	}})
	require.NoError(t, err)
	diff, err := differ.NewDiffer().Diff(desired, actual)
	require.NoError(t, err)
	ddl, err := NewDDLGenerator().GenerateDDL(diff, desired, actual)
	require.NoError(t, err)
	assertOrdered(t, ddl,
		`ALTER TABLE "public"."items" OWNER TO "new_owner";`,
		`REVOKE ALL ON TABLE "public"."items" FROM PUBLIC;`,
		`REVOKE ALL ON TABLE "public"."items" FROM "new_owner";`,
		`REVOKE ALL ON TABLE "public"."items" FROM "old_owner";`,
		`REVOKE ALL ON TABLE "public"."items" FROM "stale_reader";`,
		`GRANT SELECT ON TABLE "public"."items" TO "reader";`,
	)
	assert.NotContains(t, ddl, `REVOKE SELECT ON TABLE "public"."items" FROM "old_owner";`)
}

func TestStructuralViewReplacementPreservesUnmanagedOwnerAndACL(t *testing.T) {
	t.Parallel()

	for _, viewType := range []schema.ViewType{schema.RegularView, schema.MaterializedView} {
		viewType := viewType
		t.Run(string(viewType), func(t *testing.T) {
			owner := "existing_owner"
			actual, err := objectmap.Build([]schema.DatabaseObject{schema.View{
				Schema: "public", Name: "items_v", Type: viewType, Owner: &owner,
				Definition: schema.ViewDefinition{Query: "SELECT 1 AS id"},
				Grants: []schema.Grant{{
					Grantee:    schema.RoleGrantee("existing_reader"),
					Privileges: []schema.Privilege{schema.PrivSelect},
				}},
			}})
			require.NoError(t, err)
			desired, err := objectmap.Build([]schema.DatabaseObject{schema.View{
				Schema: "public", Name: "items_v", Type: viewType,
				Definition: schema.ViewDefinition{Query: "SELECT 2 AS id"},
				// Owner and Grants are deliberately nil: both are unmanaged.
			}})
			require.NoError(t, err)

			diff, err := differ.NewDiffer().Diff(desired, actual)
			require.NoError(t, err)
			ddl, err := NewDDLGenerator().GenerateDDL(diff, desired, actual)
			require.NoError(t, err)

			keyword := "VIEW"
			if viewType == schema.MaterializedView {
				keyword = "MATERIALIZED VIEW"
			}
			assertOrdered(t, ddl,
				`DROP `+keyword+` IF EXISTS "public"."items_v";`,
				`CREATE `+keyword+` "public"."items_v"`,
				`ALTER `+keyword+` "public"."items_v" OWNER TO "existing_owner";`,
				`REVOKE ALL ON TABLE "public"."items_v" FROM PUBLIC;`,
				`GRANT SELECT ON TABLE "public"."items_v" TO "existing_reader";`,
			)
		})
	}
}

func assertOrdered(t *testing.T, text string, fragments ...string) {
	t.Helper()
	position := -1
	for _, fragment := range fragments {
		next := strings.Index(text, fragment)
		require.Greater(t, next, position, "fragment %q was missing or out of order in:\n%s", fragment, text)
		position = next
	}
}

func stringPointer(value string) *string { return &value }
