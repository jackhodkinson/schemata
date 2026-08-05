package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDirectives(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDeps []string
		wantMode string
		wantErr  string
	}{
		{
			name:     "no directives",
			sql:      "CREATE TABLE users (id INT);",
			wantMode: ExecutionModeTransactional,
		},
		{
			name:     "single depends-on",
			sql:      "-- schemata:depends-on 20231015120530\nCREATE TABLE users (id INT);",
			wantDeps: []string{"20231015120530"},
			wantMode: ExecutionModeTransactional,
		},
		{
			name:     "multiple depends-on and transaction off",
			sql:      "-- schemata:depends-on 20231015120530\n-- schemata:transaction off\n-- schemata:depends-on 20231015130000\nCREATE INDEX CONCURRENTLY users_email_idx ON users (email);",
			wantDeps: []string{"20231015120530", "20231015130000"},
			wantMode: ExecutionModeNonTransactional,
		},
		{
			name:     "directive after SQL is ignored",
			sql:      "CREATE TABLE users (id INT);\n-- schemata:transaction off\n",
			wantMode: ExecutionModeTransactional,
		},
		{
			name:     "mixed with regular comments",
			sql:      "-- This is a regular comment\n-- schemata:depends-on 20231015120530\n-- Another comment\nCREATE TABLE users (id INT);",
			wantDeps: []string{"20231015120530"},
			wantMode: ExecutionModeTransactional,
		},
		{
			name:     "explicit transaction on",
			sql:      "\n\n-- schemata:transaction on\nCREATE TABLE users (id INT);",
			wantMode: ExecutionModeTransactional,
		},
		{
			name:     "empty SQL",
			sql:      "",
			wantMode: ExecutionModeTransactional,
		},
		{
			name:    "unknown directive",
			sql:     "-- schemata:transactions off\nSELECT 1;",
			wantErr: "unknown schemata directive",
		},
		{
			name:    "malformed transaction directive",
			sql:     "-- schemata:transaction maybe\nSELECT 1;",
			wantErr: "expected -- schemata:transaction on|off",
		},
		{
			name:    "duplicate transaction directive",
			sql:     "-- schemata:transaction off\n-- schemata:transaction off\nSELECT 1;",
			wantErr: "duplicate schemata:transaction",
		},
		{
			name:    "malformed dependency directive",
			sql:     "-- schemata:depends-on\nSELECT 1;",
			wantErr: "expected -- schemata:depends-on VERSION",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMigrationDirectives(tt.sql)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantDeps, got.DependsOn)
			assert.Equal(t, tt.wantMode, got.ExecutionMode)
		})
	}
}
