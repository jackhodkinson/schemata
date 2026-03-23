package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDirectives(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "no directives",
			sql:  "CREATE TABLE users (id INT);",
			want: nil,
		},
		{
			name: "single depends-on",
			sql:  "-- schemata:depends-on 20231015120530\nCREATE TABLE users (id INT);",
			want: []string{"20231015120530"},
		},
		{
			name: "multiple depends-on",
			sql:  "-- schemata:depends-on 20231015120530\n-- schemata:depends-on 20231015130000\nCREATE TABLE users (id INT);",
			want: []string{"20231015120530", "20231015130000"},
		},
		{
			name: "directive after SQL is ignored",
			sql:  "CREATE TABLE users (id INT);\n-- schemata:depends-on 20231015120530\n",
			want: nil,
		},
		{
			name: "mixed with regular comments",
			sql:  "-- This is a regular comment\n-- schemata:depends-on 20231015120530\n-- Another comment\nCREATE TABLE users (id INT);",
			want: []string{"20231015120530"},
		},
		{
			name: "blank lines before directive",
			sql:  "\n\n-- schemata:depends-on 20231015120530\nCREATE TABLE users (id INT);",
			want: []string{"20231015120530"},
		},
		{
			name: "empty SQL",
			sql:  "",
			want: nil,
		},
		{
			name: "only comments no directives",
			sql:  "-- just a comment\n-- another comment\nCREATE TABLE users (id INT);",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseDirectives(tt.sql)
			assert.Equal(t, tt.want, got)
		})
	}
}
