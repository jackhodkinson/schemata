package migration

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const statementSnippetLimit = 240

// StatementExecutionError identifies the exact statement that failed without
// dumping an unbounded migration or function body into logs.
type StatementExecutionError struct {
	Version        string
	Name           string
	FilePath       string
	StatementIndex int
	StatementCount int
	Snippet        string
	Err            error
}

func (e *StatementExecutionError) Error() string {
	location := fmt.Sprintf("migration %s (%s)", e.Version, e.Name)
	if e.FilePath != "" {
		location += fmt.Sprintf(" in %s", e.FilePath)
	}
	return fmt.Sprintf(
		"%s failed at statement %d of %d [%s]: %v",
		location,
		e.StatementIndex,
		e.StatementCount,
		e.Snippet,
		e.Err,
	)
}

func (e *StatementExecutionError) Unwrap() error {
	return e.Err
}

func newStatementExecutionError(migration Migration, zeroBasedIndex int, err error) error {
	return &StatementExecutionError{
		Version:        migration.Version,
		Name:           migration.Name,
		FilePath:       migration.FilePath,
		StatementIndex: zeroBasedIndex + 1,
		StatementCount: len(migration.Statements),
		Snippet:        boundedStatementSnippet(migration.Statements[zeroBasedIndex]),
		Err:            err,
	}
}

func boundedStatementSnippet(statement string) string {
	snippet := strings.Join(strings.Fields(statement), " ")
	if len(snippet) <= statementSnippetLimit {
		return snippet
	}
	end := statementSnippetLimit
	for end > 0 && !utf8.ValidString(snippet[:end]) {
		end--
	}
	return snippet[:end] + "…"
}
