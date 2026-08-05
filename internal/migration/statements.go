package migration

import (
	"bytes"
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func (m *Migration) prepareStatements() error {
	if bytes.IndexByte([]byte(m.SQL), 0) >= 0 {
		return fmt.Errorf("migration SQL contains a NUL byte")
	}

	// Parser-backed splitting is deliberately fail-closed. The scanner-only
	// splitter can omit malformed fragments and cannot recognize semicolons in
	// SQL-standard BEGIN ATOMIC routine bodies.
	statements, err := pg_query.SplitWithParser(m.SQL, true)
	if err != nil {
		return fmt.Errorf("failed to parse migration SQL into complete statements: %w", err)
	}

	for i, statement := range statements {
		tokens, err := migrationTokenTexts(statement)
		if err != nil {
			return fmt.Errorf("failed to scan migration statement %d: %w", i+1, err)
		}
		if isTransactionControl(tokens) {
			return fmt.Errorf(
				"migration statement %d contains explicit transaction control; remove transaction-boundary or SET TRANSACTION commands because schemata owns the transaction",
				i+1,
			)
		}
		if referencesInternalSchema(tokens) {
			return fmt.Errorf(
				"migration statement %d references the reserved internal schema schemata; migration SQL may not read or modify migration history",
				i+1,
			)
		}
	}

	m.Statements = statements
	return nil
}

func migrationTokenTexts(statement string) ([]string, error) {
	result, err := pg_query.Scan(statement)
	if err != nil {
		return nil, err
	}

	tokens := make([]string, 0, len(result.Tokens))
	for _, token := range result.Tokens {
		start, end := int(token.Start), int(token.End)
		if start < 0 || end < start || end > len(statement) {
			return nil, fmt.Errorf("scanner returned invalid token bounds %d:%d", start, end)
		}
		tokens = append(tokens, strings.ToLower(statement[start:end]))
	}
	return tokens, nil
}

func isTransactionControl(tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}

	switch tokens[0] {
	case "begin", "commit", "end", "rollback", "abort", "savepoint", "release":
		return true
	case "start":
		return tokenAt(tokens, 1) == "transaction"
	case "prepare":
		return tokenAt(tokens, 1) == "transaction"
	case "set":
		if tokenAt(tokens, 1) == "transaction" {
			return true
		}
		if (tokenAt(tokens, 1) == "local" || tokenAt(tokens, 1) == "session") && tokenAt(tokens, 2) == "transaction" {
			return true
		}
		return containsTokenSequence(tokens, []string{"session", "characteristics", "as", "transaction"})
	}

	return false
}

func referencesInternalSchema(tokens []string) bool {
	for i, token := range tokens {
		identifier := normalizeTokenIdentifier(token)
		if identifier == "search_path" && tokenAt(tokens, 0) == "set" {
			return true
		}
		if identifier != "schemata" {
			continue
		}
		if tokenAt(tokens, i+1) == "." {
			return true
		}
		for j := 0; j < i; j++ {
			if normalizeTokenIdentifier(tokens[j]) == "schema" {
				return true
			}
		}
	}
	return false
}

func normalizeTokenIdentifier(token string) string {
	if len(token) >= 2 && token[0] == '"' && token[len(token)-1] == '"' {
		return strings.ReplaceAll(token[1:len(token)-1], `""`, `"`)
	}
	return token
}

func tokenAt(tokens []string, index int) string {
	if index < 0 || index >= len(tokens) {
		return ""
	}
	return tokens[index]
}

func containsTokenSequence(tokens, sequence []string) bool {
	if len(sequence) == 0 || len(sequence) > len(tokens) {
		return false
	}
	for start := 0; start <= len(tokens)-len(sequence); start++ {
		matches := true
		for offset := range sequence {
			if tokens[start+offset] != sequence[offset] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}
