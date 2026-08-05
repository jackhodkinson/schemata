package migration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareStatementsHandlesDollarQuotedBodies(t *testing.T) {
	migration := Migration{SQL: `
		CREATE FUNCTION public.answer() RETURNS integer
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RETURN 42;
		END;
		$$;

		CREATE TABLE public.result (value integer);
	`}

	require.NoError(t, migration.LoadSQL())
	require.Len(t, migration.Statements, 2)
	assert.Contains(t, migration.Statements[0], "RETURN 42;")
	assert.Contains(t, migration.Statements[1], "CREATE TABLE")
}

func TestPrepareStatementsRejectsExplicitTransactionControl(t *testing.T) {
	for _, sql := range []string{
		"BEGIN; CREATE TABLE example (id integer); COMMIT;",
		"SAVEPOINT manual_boundary;",
		"ROLLBACK;",
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
		"SET LOCAL TRANSACTION READ ONLY;",
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
		"PREPARE TRANSACTION 'manual-boundary';",
	} {
		t.Run(sql, func(t *testing.T) {
			migration := Migration{SQL: sql}
			err := migration.LoadSQL()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "explicit transaction control")
		})
	}
}

func TestPrepareStatementsRejectsInternalHistoryAccess(t *testing.T) {
	for _, sql := range []string{
		"DELETE FROM schemata.version;",
		`UPDATE "schemata"."version" SET status = 'applied';`,
		"DROP SCHEMA IF EXISTS schemata CASCADE;",
		"SET search_path = schemata, public; DELETE FROM version;",
	} {
		t.Run(sql, func(t *testing.T) {
			migration := Migration{SQL: sql}
			err := migration.LoadSQL()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "reserved internal schema")
		})
	}
}

func TestPrepareStatementsAllowsSQLPrepareWithinOneMigration(t *testing.T) {
	migration := Migration{SQL: "PREPARE find_value(integer) AS SELECT $1; EXECUTE find_value(42); DEALLOCATE find_value;"}
	require.NoError(t, migration.LoadSQL())
	assert.Len(t, migration.Statements, 3)
}

func TestPrepareStatementsPreservesBeginAtomicBody(t *testing.T) {
	migration := Migration{SQL: `
CREATE FUNCTION add_one(value integer)
RETURNS integer
LANGUAGE SQL
BEGIN ATOMIC
  SELECT value + 1;
  SELECT value + 2;
END;
SELECT 1;
`}

	require.NoError(t, migration.LoadSQL())
	require.Len(t, migration.Statements, 2)
	assert.Contains(t, migration.Statements[0], "BEGIN ATOMIC")
	assert.Contains(t, migration.Statements[0], "SELECT value + 2;")
	assert.Equal(t, "SELECT 1", migration.Statements[1])
}

func TestPrepareStatementsRejectsIncompleteOrOmittedInput(t *testing.T) {
	tests := []string{
		"SELECT 1);",
		"CREATE TABLE incomplete (id integer;",
		"SELECT 1; BOGUS;",
		"SELECT 1;\x00SELECT 2;",
	}

	for _, sql := range tests {
		t.Run(fmt.Sprintf("%q", sql), func(t *testing.T) {
			err := (&Migration{SQL: sql}).LoadSQL()
			require.Error(t, err)
		})
	}
}
