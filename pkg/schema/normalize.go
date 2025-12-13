package schema

import (
	"regexp"
	"strings"
)

// NormalizeTypeName normalizes type names to a canonical form suitable for
// stable hashing and function signature identity.
func NormalizeTypeName(typeName TypeName) TypeName {
	typeStr := strings.TrimSpace(string(typeName))
	if typeStr == "" {
		return typeName
	}

	// Strip schema qualification for stable comparisons (parser typically does not preserve it).
	// This intentionally keeps only the final identifier component for dotted names like
	// "pg_catalog.int4" or public.my_domain.
	if strings.Contains(typeStr, ".") && !strings.Contains(typeStr, " ") {
		arraySuffix := ""
		for strings.HasSuffix(typeStr, "[]") {
			arraySuffix += "[]"
			typeStr = strings.TrimSuffix(typeStr, "[]")
		}
		if idx := strings.LastIndex(typeStr, "."); idx != -1 {
			typeStr = typeStr[idx+1:]
		}
		typeStr += arraySuffix
	}

	// Handle common aliases - normalize TO the SQL standard names.
	switch strings.ToLower(typeStr) {
	case "int", "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "int2":
		return "smallint"
	case "bool":
		return "boolean"
	case "timestamptz":
		return "timestamp with time zone"
	case "timestamp":
		return "timestamp without time zone"
	case "timetz":
		return "time with time zone"
	case "time":
		return "time without time zone"
	case "character varying":
		re := regexp.MustCompile(`character varying\((\d+)\)`)
		if matches := re.FindStringSubmatch(typeStr); len(matches) > 0 {
			return TypeName("varchar(" + matches[1] + ")")
		}
		return "varchar"
	case "character":
		re := regexp.MustCompile(`character\((\d+)\)`)
		if matches := re.FindStringSubmatch(typeStr); len(matches) > 0 {
			return TypeName("char(" + matches[1] + ")")
		}
		return "char"
	}

	// Handle parameterized types by converting base type to lowercase,
	// while preserving the parameter structure.
	if strings.Contains(typeStr, "(") {
		parts := strings.SplitN(typeStr, "(", 2)
		if len(parts) == 2 {
			baseType := strings.ToLower(strings.TrimSpace(parts[0]))
			params := parts[1]

			switch baseType {
			case "character varying":
				return TypeName("varchar(" + params)
			case "character":
				return TypeName("char(" + params)
			}
		}
	}

	return TypeName(typeStr)
}

