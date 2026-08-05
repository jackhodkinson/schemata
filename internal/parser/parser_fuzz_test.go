package parser

import (
	"reflect"
	"testing"
)

func FuzzParseSQLDeterministicAndPanicFree(f *testing.F) {
	for _, seed := range []string{
		"",
		"CREATE TABLE public.users (id bigint PRIMARY KEY, name text);",
		`CREATE TABLE "odd.schema"."select" ("a.b" text DEFAULT 'Case');`,
		"CREATE TYPE public.mood AS ENUM ('happy', 'Sad');",
		"CREATE FUNCTION public.answer() RETURNS integer LANGUAGE SQL AS $$ SELECT 42; $$;",
		"GRANT SELECT ON TABLE public.users TO app_reader;",
		"COMMENT ON TABLE public.users IS 'users';",
		"SET search_path = tenant, public;",
		"SELECT pg_catalog.set_config('search_path', '', false);",
		"CREATE TABLE public.prefix (id integer);\x00DROP TABLE public.prefix;",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 64*1024 {
			t.Skip()
		}

		first, firstErr := NewParser().ParseSQL(input)
		second, secondErr := NewParser().ParseSQL(input)
		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("parser result changed between identical inputs: first=%v second=%v", firstErr, secondErr)
		}
		if firstErr != nil {
			if firstErr.Error() != secondErr.Error() {
				t.Fatalf("parser error is nondeterministic:\nfirst:  %q\nsecond: %q", firstErr, secondErr)
			}
			return
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("parser output is nondeterministic for input %q", input)
		}
	})
}

func TestParseSQLRejectsEmbeddedNUL(t *testing.T) {
	_, err := NewParser().ParseSQL("CREATE TABLE public.prefix (id integer);\x00DROP TABLE public.prefix;")
	if err == nil {
		t.Fatal("expected embedded NUL to be rejected")
	}
}
