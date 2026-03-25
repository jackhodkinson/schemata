package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindExtensionsInMigrations(t *testing.T) {
	tests := []struct {
		name       string
		migrations []Migration
		want       []string
	}{
		{
			name:       "no migrations",
			migrations: nil,
			want:       []string{},
		},
		{
			name: "no extensions in SQL",
			migrations: []Migration{
				{Version: "20260226120000", SQL: "CREATE TABLE users (id INT);"},
			},
			want: []string{},
		},
		{
			name: "simple CREATE EXTENSION",
			migrations: []Migration{
				{Version: "20260226120000", SQL: "CREATE EXTENSION citext;"},
			},
			want: []string{"citext"},
		},
		{
			name: "CREATE EXTENSION IF NOT EXISTS",
			migrations: []Migration{
				{Version: "20260226120000", SQL: "CREATE EXTENSION IF NOT EXISTS pg_trgm;"},
			},
			want: []string{"pg_trgm"},
		},
		{
			name: "quoted extension name",
			migrations: []Migration{
				{Version: "20260226120000", SQL: `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`},
			},
			want: []string{"uuid-ossp"},
		},
		{
			name: "case insensitive",
			migrations: []Migration{
				{Version: "20260226120000", SQL: "create extension citext;"},
			},
			want: []string{"citext"},
		},
		{
			name: "multiple extensions across migrations",
			migrations: []Migration{
				{Version: "20260226120000", SQL: "CREATE EXTENSION citext;\nCREATE EXTENSION pg_trgm;"},
				{Version: "20260227120000", SQL: "CREATE EXTENSION IF NOT EXISTS pgcrypto;"},
			},
			want: []string{"citext", "pg_trgm", "pgcrypto"},
		},
		{
			name: "deduplicated",
			migrations: []Migration{
				{Version: "20260226120000", SQL: "CREATE EXTENSION citext;"},
				{Version: "20260227120000", SQL: "CREATE EXTENSION IF NOT EXISTS citext;"},
			},
			want: []string{"citext"},
		},
		{
			name: "extension among other SQL",
			migrations: []Migration{
				{Version: "20260226120000", SQL: "CREATE EXTENSION citext;\nCREATE TABLE users (email citext NOT NULL);"},
			},
			want: []string{"citext"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FindExtensionsInMigrations(tt.migrations)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestVersionBefore(t *testing.T) {
	tests := []struct {
		name     string
		earliest string
		want     string
	}{
		{
			name:     "SQL format",
			earliest: "20260226120000",
			want:     "20260226115959",
		},
		{
			name:     "SQL format midnight",
			earliest: "20260226000000",
			want:     "20260225235959",
		},
		{
			name:     "moo format",
			earliest: "2026-02-26-epoch",
			want:     "20260225235959",
		},
		{
			name:     "moo format january first",
			earliest: "2026-01-01-init",
			want:     "20251231235959",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VersionBefore(tt.earliest)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestVersionBefore_empty(t *testing.T) {
	// Empty string should return a current timestamp (14 digits)
	got := VersionBefore("")
	assert.Len(t, got, 14)
}
