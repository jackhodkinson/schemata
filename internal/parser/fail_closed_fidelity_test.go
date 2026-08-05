package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFunctionRejectsIdentityTypeModifiers(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "argument typmod",
			sql:  `CREATE FUNCTION public.f(v varchar(10)) RETURNS integer LANGUAGE sql AS 'SELECT 1'`,
		},
		{
			name: "return typmod",
			sql:  `CREATE FUNCTION public.f() RETURNS numeric(10, 2) LANGUAGE sql AS 'SELECT 1'`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewParser().ParseSQL(test.sql)
			require.ErrorContains(t, err, "type modifier that PostgreSQL does not preserve")
		})
	}
}

func TestParseFunctionRejectsUnmodeledFunctionShapes(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "multipart C body",
			sql:  `CREATE FUNCTION public.f(integer) RETURNS integer LANGUAGE c AS 'library', 'symbol'`,
			want: "multi-part AS bodies",
		},
		{
			name: "cost",
			sql:  `CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql COST 7 AS 'SELECT 1'`,
			want: `function option "cost" is not modeled`,
		},
		{
			name: "rows",
			sql:  `CREATE FUNCTION public.f() RETURNS SETOF integer LANGUAGE sql ROWS 7 AS 'SELECT 1'`,
			want: `function option "rows" is not modeled`,
		},
		{
			name: "leakproof",
			sql:  `CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql LEAKPROOF AS 'SELECT 1'`,
			want: `function option "leakproof" is not modeled`,
		},
		{
			name: "support",
			sql:  `CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql SUPPORT public.support AS 'SELECT 1'`,
			want: `function option "support" is not modeled`,
		},
		{
			name: "transform",
			sql:  `CREATE FUNCTION public.f(jsonb) RETURNS jsonb LANGUAGE sql TRANSFORM FOR TYPE jsonb AS 'SELECT $1'`,
			want: `function option "transform" is not modeled`,
		},
		{
			name: "window",
			sql:  `CREATE FUNCTION public.f(internal) RETURNS bigint LANGUAGE internal WINDOW AS 'window_row_number'`,
			want: `function option "window" is not modeled`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewParser().ParseSQL(test.sql)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.want)
		})
	}
}

func TestParseRelationsRejectsUnsupportedPersistence(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want string
	}{
		{name: "temporary table", sql: `CREATE TEMPORARY TABLE public.t (id integer)`, want: "temporary or unlogged tables are not modeled"},
		{name: "unlogged table", sql: `CREATE UNLOGGED TABLE public.t (id integer)`, want: "temporary or unlogged tables are not modeled"},
		{name: "temporary view", sql: `CREATE TEMPORARY VIEW public.v AS SELECT 1`, want: "temporary views are not modeled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewParser().ParseSQL(test.sql)
			require.ErrorContains(t, err, test.want)
		})
	}
}
