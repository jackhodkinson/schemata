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
		if isClusterScopedCommand(tokens) {
			return fmt.Errorf(
				"migration statement %d is cluster-scoped and cannot be coordinated by Schemata's database-local history and locks",
				i+1,
			)
		}
		if m.ExecutionMode == ExecutionModeTransactional && requiresNonTransactionalExecution(tokens) {
			return fmt.Errorf(
				"migration statement %d must run outside a transaction; add -- schemata:transaction off to the leading migration comment block",
				i+1,
			)
		}
		if m.ExecutionMode == ExecutionModeNonTransactional && invokesProceduralBlock(tokens) {
			return fmt.Errorf(
				"non-transactional migration statement %d invokes a procedural block whose transaction and cluster effects cannot be inspected safely; use explicit independently resumable SQL statements instead",
				i+1,
			)
		}
		if m.ExecutionMode == ExecutionModeNonTransactional && changesNonDurableSessionState(tokens) {
			return fmt.Errorf(
				"non-transactional migration statement %d changes session-local state that cannot be reconstructed safely after a crash; move it to a transactional migration or make each non-transactional statement independently resumable",
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

func changesNonDurableSessionState(tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}
	switch tokens[0] {
	case "set", "reset", "discard", "prepare", "execute", "deallocate",
		"listen", "unlisten", "load", "lock", "declare", "fetch", "close":
		return true
	case "create":
		for _, token := range tokens {
			if token == "temp" || token == "temporary" {
				return true
			}
		}
	case "select":
		for _, token := range tokens {
			switch normalizeTokenIdentifier(token) {
			case "set_config", "pg_advisory_lock", "pg_try_advisory_lock", "pg_advisory_unlock", "pg_advisory_unlock_all":
				return true
			}
		}
	}
	return false
}

func invokesProceduralBlock(tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}
	return tokens[0] == "call" || tokens[0] == "do"
}

func requiresNonTransactionalExecution(tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}
	if tokens[0] == "vacuum" {
		return true
	}
	if containsTokenSequence(tokens, []string{"index", "concurrently"}) {
		return tokens[0] == "create" || tokens[0] == "drop" || tokens[0] == "reindex"
	}
	return tokens[0] == "reindex" && containsTokenSequence(tokens, []string{"concurrently"})
}

func isClusterScopedCommand(tokens []string) bool {
	if len(tokens) < 2 {
		return false
	}
	clusterObject := func(token string) bool {
		switch normalizeTokenIdentifier(token) {
		case "database", "tablespace", "role", "user", "group", "subscription":
			return true
		default:
			return false
		}
	}

	switch tokens[0] {
	case "create", "alter", "drop":
		return tokenAt(tokens, 1) == "system" ||
			(tokens[0] == "drop" && tokenAt(tokens, 1) == "owned") ||
			clusterObject(tokenAt(tokens, 1))
	case "comment":
		return tokenAt(tokens, 1) == "on" && clusterObject(tokenAt(tokens, 2))
	case "security":
		return tokenAt(tokens, 1) == "label" && tokenAt(tokens, 2) == "on" && clusterObject(tokenAt(tokens, 3))
	case "reassign":
		return tokenAt(tokens, 1) == "owned"
	case "grant", "revoke":
		return isClusterScopedGrant(tokens)
	default:
		return false
	}
}

func isClusterScopedGrant(tokens []string) bool {
	onIndex := -1
	for i, token := range tokens {
		if token == "on" {
			onIndex = i
			break
		}
	}
	if onIndex == -1 {
		// GRANT role TO role and REVOKE role FROM role modify cluster-wide
		// membership. Object privilege statements always contain ON.
		return true
	}
	switch normalizeTokenIdentifier(tokenAt(tokens, onIndex+1)) {
	case "database", "tablespace", "parameter":
		return true
	default:
		return false
	}
}

func migrationTokenTexts(statement string) ([]string, error) {
	result, err := pg_query.Scan(statement)
	if err != nil {
		return nil, err
	}

	tokens := make([]string, 0, len(result.Tokens))
	for _, token := range result.Tokens {
		if token.Token == pg_query.Token_SQL_COMMENT || token.Token == pg_query.Token_C_COMMENT {
			continue
		}
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
