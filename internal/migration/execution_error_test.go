package migration

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestStatementExecutionErrorIsBoundedAndStructured(t *testing.T) {
	migration := Migration{
		Version:    "001",
		Name:       "broken",
		FilePath:   "/migrations/001-broken.sql",
		Statements: []string{"SELECT 1", "INSERT INTO missing VALUES (" + strings.Repeat("1", 500) + ")"},
	}
	cause := errors.New("relation does not exist")
	err := newStatementExecutionError(migration, 1, cause)

	assert.ErrorIs(t, err, cause)
	var statementErr *StatementExecutionError
	assert.ErrorAs(t, err, &statementErr)
	assert.Equal(t, 2, statementErr.StatementIndex)
	assert.Equal(t, 2, statementErr.StatementCount)
	assert.LessOrEqual(t, len(statementErr.Snippet), statementSnippetLimit+len("…"))
	assert.Contains(t, err.Error(), "statement 2 of 2")
}

func TestBoundedStatementSnippetPreservesUTF8(t *testing.T) {
	statement := "SELECT '" + strings.Repeat("é", statementSnippetLimit) + "'"
	snippet := boundedStatementSnippet(statement)

	assert.True(t, utf8.ValidString(snippet))
	assert.LessOrEqual(t, len(strings.TrimSuffix(snippet, "…")), statementSnippetLimit)
}
