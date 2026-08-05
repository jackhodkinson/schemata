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
		"-- leading comment\nBEGIN; COMMIT;",
		"-- schemata:transaction off\nBEGIN; COMMIT;",
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

func TestPrepareStatementsRequiresExplicitNonTransactionalMode(t *testing.T) {
	for _, sql := range []string{
		"CREATE INDEX CONCURRENTLY example_idx ON example (id);",
		"DROP INDEX CONCURRENTLY example_idx;",
		"VACUUM example;",
	} {
		t.Run(sql, func(t *testing.T) {
			err := (&Migration{SQL: sql}).LoadSQL()
			require.ErrorContains(t, err, "schemata:transaction off")

			nonTransactional := Migration{SQL: "-- schemata:transaction off\n" + sql}
			require.NoError(t, nonTransactional.LoadSQL())
			assert.Equal(t, ExecutionModeNonTransactional, nonTransactional.ExecutionMode)
		})
	}
}

func TestPrepareStatementsRejectsClusterScopedCommandsInEveryMode(t *testing.T) {
	for _, sql := range []string{
		"CREATE DATABASE other_database;",
		"DROP DATABASE other_database;",
		"CREATE TABLESPACE other_space LOCATION '/tmp/other';",
		"ALTER TABLESPACE other_space RENAME TO renamed_space;",
		"ALTER SYSTEM SET work_mem = '64MB';",
		"CREATE ROLE cluster_role;",
		"ALTER ROLE cluster_role LOGIN;",
		"DROP USER cluster_user;",
		"COMMENT ON DATABASE other_database IS 'cluster scoped';",
		"GRANT cluster_role TO cluster_user;",
		"REVOKE ADMIN OPTION FOR cluster_role FROM cluster_user;",
		"GRANT CONNECT ON DATABASE other_database TO cluster_user;",
		"REASSIGN OWNED BY old_owner TO new_owner;",
		"DROP OWNED BY cluster_role;",
		"DROP SUBSCRIPTION external_subscription;",
	} {
		t.Run(sql, func(t *testing.T) {
			for _, prefix := range []string{"", "-- schemata:transaction off\n"} {
				err := (&Migration{SQL: prefix + sql}).LoadSQL()
				require.ErrorContains(t, err, "cluster-scoped")
			}
		})
	}
}

func TestPrepareStatementsRejectsNonDurableSessionStateInNonTransactionalMode(t *testing.T) {
	for _, sql := range []string{
		"SET ROLE migration_owner;",
		"PREPARE query AS SELECT 1;",
		"CREATE TEMP TABLE staging (id integer);",
		"SELECT pg_advisory_lock(42);",
	} {
		t.Run(sql, func(t *testing.T) {
			err := (&Migration{SQL: "-- schemata:transaction off\n" + sql}).LoadSQL()
			require.ErrorContains(t, err, "session-local state")
		})
	}
}

func TestPrepareStatementsRejectsProceduralExecutionInNonTransactionalMode(t *testing.T) {
	for _, sql := range []string{
		"CALL procedure_that_commits();",
		"DO $$ BEGIN PERFORM set_config('application_name', 'leaked', false); END $$;",
	} {
		t.Run(sql, func(t *testing.T) {
			err := (&Migration{SQL: "-- schemata:transaction off\n" + sql}).LoadSQL()
			require.ErrorContains(t, err, "procedural block")
		})
	}
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
