package db

import (
	"testing"
)

func TestExtractFunctionBody(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "typical pg_get_functiondef output",
			input: `CREATE OR REPLACE FUNCTION public.update_updated_at_column()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$

BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;

$function$
`,
			expected: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
		},
		{
			name: "function with $$ delimiter",
			input: `CREATE OR REPLACE FUNCTION public.test()
 RETURNS void
 LANGUAGE plpgsql
AS $$
BEGIN
    RETURN;
END;
$$`,
			expected: `BEGIN
    RETURN;
END;`,
		},
		{
			name: "function with custom tag",
			input: `CREATE OR REPLACE FUNCTION public.test()
 RETURNS void
 LANGUAGE plpgsql
AS $body$
SELECT 1;
$body$`,
			expected: `SELECT 1;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractFunctionBody(tt.input)
			if result != tt.expected {
				t.Errorf("extractFunctionBody() =\n%q\n\nwant:\n%q", result, tt.expected)
			}
		})
	}
}
