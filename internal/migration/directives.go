package migration

import (
	"fmt"
	"strings"
)

type migrationDirectives struct {
	DependsOn     []string
	ExecutionMode string
}

// parseMigrationDirectives scans the leading comment block for directives.
// Directives are deliberately restricted to this header so a comment inside a
// routine body or later SQL statement cannot silently change execution policy.
// Unknown and malformed schemata directives fail closed.
func parseMigrationDirectives(sql string) (migrationDirectives, error) {
	directives := migrationDirectives{ExecutionMode: ExecutionModeTransactional}
	transactionSeen := false

	for lineNumber, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "--") {
			break
		}

		comment := strings.TrimSpace(strings.TrimPrefix(trimmed, "--"))
		if !strings.HasPrefix(strings.ToLower(comment), "schemata:") {
			continue
		}

		fields := strings.Fields(comment)
		if len(fields) == 0 {
			continue
		}
		name := strings.ToLower(strings.TrimPrefix(fields[0], "schemata:"))
		switch name {
		case "depends-on":
			if len(fields) != 2 {
				return migrationDirectives{}, fmt.Errorf(
					"invalid schemata:depends-on directive on line %d: expected -- schemata:depends-on VERSION",
					lineNumber+1,
				)
			}
			directives.DependsOn = append(directives.DependsOn, fields[1])
		case "transaction":
			if len(fields) != 2 || (strings.ToLower(fields[1]) != "on" && strings.ToLower(fields[1]) != "off") {
				return migrationDirectives{}, fmt.Errorf(
					"invalid schemata:transaction directive on line %d: expected -- schemata:transaction on|off",
					lineNumber+1,
				)
			}
			if transactionSeen {
				return migrationDirectives{}, fmt.Errorf(
					"duplicate schemata:transaction directive on line %d",
					lineNumber+1,
				)
			}
			transactionSeen = true
			if strings.EqualFold(fields[1], "off") {
				directives.ExecutionMode = ExecutionModeNonTransactional
			}
		case "":
			return migrationDirectives{}, fmt.Errorf("empty schemata directive on line %d", lineNumber+1)
		default:
			return migrationDirectives{}, fmt.Errorf(
				"unknown schemata directive %q on line %d",
				name,
				lineNumber+1,
			)
		}
	}

	return directives, nil
}
