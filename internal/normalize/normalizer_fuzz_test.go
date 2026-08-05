package normalize

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

func FuzzExprIdempotent(f *testing.F) {
	for _, seed := range []string{
		"now()",
		"CURRENT_TIMESTAMP",
		"'Admin'::text",
		`"UserName"`,
		"(price > 0)",
		"ARRAY['a', 'B']::text[]",
		"jsonb_build_object('Case', value)",
		"tenant.calculate_total(amount, 'GBP'::currency)",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 64*1024 {
			t.Skip()
		}
		once := Expr(schema.Expr(input))
		twice := Expr(once)
		if once != twice {
			t.Fatalf("expression normalization is not idempotent:\ninput:  %q\nonce:   %q\ntwice:  %q", input, once, twice)
		}
	})
}

func FuzzFunctionBodyIdempotent(f *testing.F) {
	for _, seed := range []string{
		"BEGIN RETURN NEW; END;",
		"SELECT 'Admin', \"UserName\";",
		"$body$BEGIN\nRETURN 'Case';\nEND;$body$",
		"-- comment\nSELECT 1;",
		"/* block */ SELECT E'line\\n';",
		"IF value = 'A' THEN RETURN \"MixedCase\"; END IF;",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 64*1024 {
			t.Skip()
		}
		once := FunctionBody(input)
		twice := FunctionBody(once)
		if once != twice {
			t.Fatalf("function-body normalization is not idempotent:\ninput:  %q\nonce:   %q\ntwice:  %q", input, once, twice)
		}
	})
}
