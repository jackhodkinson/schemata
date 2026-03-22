//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ddlPrimitiveCase struct {
	name string
	sql  string
}

func TestNoOpSecondRun_PerDDLPrimitive(t *testing.T) {
	devDB := newTestDB(t, devDBURL)

	extensionName := pickInstallableExtension(t, devDB, []string{
		"hstore",
		"citext",
		"pg_trgm",
		"uuid-ossp",
	})
	if extensionName == "" {
		t.Fatalf("no installable extension found for extension primitive idempotency case")
	}

	cases := []ddlPrimitiveCase{
		{
			name: "table",
			sql: `
				CREATE TABLE no_op_table (
					id INTEGER PRIMARY KEY,
					name TEXT NOT NULL
				);
			`,
		},
		{
			name: "index",
			sql: `
				CREATE TABLE no_op_index_base (
					id INTEGER PRIMARY KEY,
					email TEXT
				);
				CREATE INDEX idx_no_op_email ON no_op_index_base(email);
			`,
		},
		{
			name: "view",
			sql: `
				CREATE TABLE no_op_view_base (
					id INTEGER PRIMARY KEY,
					email TEXT
				);
				CREATE VIEW no_op_view AS
				SELECT id, email FROM no_op_view_base;
			`,
		},
		{
			name: "function",
			sql: `
				CREATE OR REPLACE FUNCTION no_op_add(a INTEGER, b INTEGER)
				RETURNS INTEGER
				LANGUAGE sql
				AS $$
					SELECT a + b;
				$$;
			`,
		},
		{
			name: "sequence",
			sql: `
				CREATE SEQUENCE no_op_seq START 100 INCREMENT 5;
			`,
		},
		{
			name: "enum type",
			sql: `
				CREATE TYPE no_op_status AS ENUM ('pending', 'active', 'archived');
			`,
		},
		{
			name: "domain type",
			sql: `
				CREATE DOMAIN no_op_positive_int AS INTEGER CHECK (VALUE > 0);
			`,
		},
		{
			name: "extension",
			sql: fmt.Sprintf(`
				CREATE EXTENSION IF NOT EXISTS %s;
			`, extensionName),
		},
		{
			name: "trigger",
			sql: `
				CREATE TABLE no_op_trigger_table (
					id INTEGER PRIMARY KEY,
					updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				);

				CREATE OR REPLACE FUNCTION no_op_set_updated_at()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.updated_at = CURRENT_TIMESTAMP;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				CREATE TRIGGER trg_no_op_set_updated_at
					BEFORE UPDATE ON no_op_trigger_table
					FOR EACH ROW
					EXECUTE FUNCTION no_op_set_updated_at();
			`,
		},
		{
			name: "policy",
			sql: `
				CREATE TABLE no_op_policy_table (
					id INTEGER PRIMARY KEY,
					owner_name TEXT NOT NULL
				);

				CREATE POLICY no_op_owner_policy ON no_op_policy_table
					FOR SELECT
					TO postgres
					USING (owner_name = current_user);
			`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			devDB.reset(t)

			desired := parseSQL(t, tc.sql)
			actual := devDB.extractSchema(t)
			firstDiff := diffSchemas(t, desired, actual)
			require.False(t, firstDiff.IsEmpty(), "first run should require changes for primitive %q", tc.name)

			ddl := generateDDL(t, firstDiff, desired, actual)
			require.NotEmpty(t, ddl, "generated DDL should not be empty for primitive %q", tc.name)
			devDB.execSQL(t, ddl)

			actualAfterApply := devDB.extractSchema(t)
			secondDiff := diffSchemas(t, desired, actualAfterApply)
			if !secondDiff.IsEmpty() {
				t.Logf("primitive %q failed second-run no-op check; residual diff:", tc.name)
				logDiff(t, secondDiff)
			}
			assert.True(t, secondDiff.IsEmpty(),
				"second run should be no-op for primitive %q", tc.name)
		})
	}
}

func pickInstallableExtension(t *testing.T, d *testDB, candidates []string) string {
	t.Helper()

	for _, name := range candidates {
		var available bool
		err := d.pool.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = $1)", name).
			Scan(&available)
		require.NoError(t, err, "failed checking availability for extension %q", name)
		if !available {
			continue
		}

		var installed bool
		err = d.pool.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = $1)", name).
			Scan(&installed)
		require.NoError(t, err, "failed checking install status for extension %q", name)
		if installed {
			continue
		}

		return name
	}

	return ""
}
