package test

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPgQueryDDLStatements tests parsing various DDL statements we'll need
func TestPgQueryDDLStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantStmt int
	}{
		{
			name:     "CREATE TABLE with constraints",
			sql:      "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT NOT NULL UNIQUE, created_at TIMESTAMP DEFAULT NOW());",
			wantStmt: 1,
		},
		{
			name:     "ALTER TABLE add column",
			sql:      "ALTER TABLE users ADD COLUMN age INTEGER;",
			wantStmt: 1,
		},
		{
			name:     "ALTER TABLE add constraint",
			sql:      "ALTER TABLE posts ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);",
			wantStmt: 1,
		},
		{
			name:     "CREATE INDEX",
			sql:      "CREATE INDEX idx_users_email ON users(email);",
			wantStmt: 1,
		},
		{
			name:     "CREATE UNIQUE INDEX",
			sql:      "CREATE UNIQUE INDEX idx_users_email_unique ON users(lower(email));",
			wantStmt: 1,
		},
		{
			name:     "CREATE VIEW",
			sql:      "CREATE VIEW active_users AS SELECT * FROM users WHERE deleted_at IS NULL;",
			wantStmt: 1,
		},
		{
			name:     "CREATE FUNCTION",
			sql:      "CREATE FUNCTION add(a INT, b INT) RETURNS INT AS $$ SELECT a + b; $$ LANGUAGE SQL;",
			wantStmt: 1,
		},
		{
			name:     "CREATE TYPE enum",
			sql:      "CREATE TYPE status AS ENUM ('active', 'inactive', 'pending');",
			wantStmt: 1,
		},
		{
			name:     "CREATE SEQUENCE",
			sql:      "CREATE SEQUENCE user_id_seq START 1000;",
			wantStmt: 1,
		},
		{
			name:     "DROP TABLE",
			sql:      "DROP TABLE users;",
			wantStmt: 1,
		},
		{
			name:     "CREATE TABLE with CHECK constraint",
			sql:      "CREATE TABLE products (id INT, price NUMERIC CHECK (price > 0));",
			wantStmt: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pg_query.Parse(tt.sql)
			require.NoError(t, err, "should parse %s", tt.name)
			require.NotNil(t, result)
			assert.Len(t, result.Stmts, tt.wantStmt)
		})
	}
}

// TestPgQueryComplexSchema tests parsing a complete schema file
func TestPgQueryComplexSchema(t *testing.T) {
	schemaSQL := `
		-- Users table
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			username TEXT NOT NULL,
			password_hash TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW(),
			deleted_at TIMESTAMP,
			CONSTRAINT valid_email CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
		);

		-- Posts table
		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			body TEXT,
			published BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT NOW(),
			CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		);

		-- Create indexes
		CREATE INDEX idx_users_email ON users(lower(email));
		CREATE INDEX idx_posts_user_id ON posts(user_id);
		CREATE INDEX idx_posts_published ON posts(published) WHERE published = TRUE;

		-- Create view
		CREATE VIEW published_posts AS
			SELECT p.*, u.username
			FROM posts p
			JOIN users u ON p.user_id = u.id
			WHERE p.published = TRUE AND u.deleted_at IS NULL;
	`

	result, err := pg_query.Parse(schemaSQL)
	require.NoError(t, err, "should parse complex schema")
	require.NotNil(t, result)

	// Should have 6 statements: 2 tables, 3 indexes, 1 view
	assert.Len(t, result.Stmts, 6, "should have 6 statements")
}

// TestPgQueryNormalization tests that pg_query can normalize SQL
func TestPgQueryNormalization(t *testing.T) {
	tests := []struct {
		name string
		sql1 string
		sql2 string
		same bool
	}{
		{
			name: "same statement different formatting",
			sql1: "SELECT id, name FROM users WHERE id = 1",
			sql2: "SELECT id,name FROM users WHERE id=1",
			same: true,
		},
		{
			name: "different statements",
			sql1: "SELECT id FROM users",
			sql2: "SELECT name FROM users",
			same: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result1, err1 := pg_query.Parse(tt.sql1)
			require.NoError(t, err1)

			result2, err2 := pg_query.Parse(tt.sql2)
			require.NoError(t, err2)

			// Both should parse successfully
			require.NotNil(t, result1)
			require.NotNil(t, result2)

			// For normalization testing, we can use Fingerprint
			fp1, err := pg_query.Fingerprint(tt.sql1)
			require.NoError(t, err)

			fp2, err := pg_query.Fingerprint(tt.sql2)
			require.NoError(t, err)

			if tt.same {
				assert.Equal(t, fp1, fp2, "fingerprints should match for equivalent SQL")
			} else {
				assert.NotEqual(t, fp1, fp2, "fingerprints should differ for different SQL")
			}
		})
	}
}

// TestPgQueryErrorHandling tests parser error handling
func TestPgQueryErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		shouldErr bool
	}{
		{"empty string", "", false},                              // Empty is valid - returns empty result
		{"invalid SQL", "INVALID SQL STATEMENT", true},           // Invalid keyword
		{"incomplete statement", "CREATE TABLE users (", true},   // Incomplete
		{"syntax error", "SELECT * FORM users", true},            // Typo in FROM
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pg_query.Parse(tt.sql)
			// Should not panic
			if tt.shouldErr {
				assert.Error(t, err, "should error on invalid SQL")
			} else {
				assert.NoError(t, err, "should not error")
				assert.NotNil(t, result)
			}
		})
	}
}
