package test

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPgQueryBasic tests that pg_query_go can parse basic SQL
// This is THE MOST CRITICAL test - if this fails, nothing else matters
func TestPgQueryBasic(t *testing.T) {
	sql := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);"

	result, err := pg_query.Parse(sql)
	require.NoError(t, err, "pg_query_go MUST be able to parse SQL")
	require.NotNil(t, result, "parse result should not be nil")

	assert.Greater(t, len(result.Stmts), 0, "should have at least one statement")
}

// TestPgQuerySelect tests parsing a SELECT statement
func TestPgQuerySelect(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE id = 1;"

	result, err := pg_query.Parse(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, 1, len(result.Stmts))
}

// TestPgQueryMultipleStatements tests parsing multiple statements
func TestPgQueryMultipleStatements(t *testing.T) {
	sql := `
		CREATE TABLE users (id INT PRIMARY KEY);
		CREATE TABLE posts (id INT PRIMARY KEY, user_id INT);
		ALTER TABLE posts ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);
	`

	result, err := pg_query.Parse(sql)
	require.NoError(t, err, "should parse multiple statements")
	require.NotNil(t, result)

	assert.Equal(t, 3, len(result.Stmts), "should have 3 statements")
}
