package schema

import (
	"regexp"
	"strings"
)

// normalizeTypeParams normalizes whitespace around commas in type parameters.
// e.g., "10,2)" → "10, 2)" and "10 , 2)" → "10, 2)"
func normalizeTypeParams(params string) string {
	// Split on commas, trim each part, rejoin with ", "
	closeParen := ""
	if strings.HasSuffix(params, ")") {
		closeParen = ")"
		params = strings.TrimSuffix(params, ")")
	}
	parts := strings.Split(params, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return strings.Join(parts, ", ") + closeParen
}

// NormalizeTypeName normalizes type names to a canonical form suitable for
// stable hashing and function signature identity.
func NormalizeTypeName(typeName TypeName) TypeName {
	typeStr := strings.TrimSpace(string(typeName))
	if typeStr == "" {
		return typeName
	}

	// Normalize the element type independently so aliases remain canonical for
	// arrays as well (for example int[] and int4[] must identify integer[]).
	isArray := false
	for strings.HasSuffix(typeStr, "[]") {
		isArray = true
		typeStr = strings.TrimSpace(strings.TrimSuffix(typeStr, "[]"))
	}
	if isArray {
		// PostgreSQL has one array type OID per element type. Declared ranks such
		// as integer[][] are not preserved by format_type(), which is the catalog
		// representation Schemata compares against.
		return TypeName(string(NormalizeTypeName(TypeName(typeStr))) + "[]")
	}

	// pg_catalog qualification is redundant for built-in aliases. The parser's
	// default schema is public and catalog rendering uses public in its canonical
	// search path, so public.value_type and value_type are one canonical identity.
	// Other user-defined qualifications remain semantic: a.value_type and
	// b.value_type are different types and can identify different overloads.
	if qualifier, name, ok := splitQualifiedTypeName(typeStr); ok {
		if isCanonicalTypeQualifier(qualifier) {
			typeStr = name
		} else {
			return TypeName(typeStr)
		}
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
			baseType := strings.TrimSpace(parts[0])
			params := parts[1]

			// Normalize whitespace inside parameters: "10,2" and "10, 2" → "10, 2"
			params = normalizeTypeParams(params)
			if strings.HasPrefix(baseType, `"`) {
				return TypeName(baseType + "(" + params)
			}
			baseType = strings.ToLower(baseType)

			switch baseType {
			case "character varying":
				return TypeName("varchar(" + params)
			case "character":
				return TypeName("char(" + params)
			default:
				return TypeName(baseType + "(" + params)
			}
		}
	}

	return TypeName(typeStr)
}

// splitQualifiedTypeName finds one schema/type separator while respecting
// quoted identifiers. More heavily qualified or malformed names are preserved
// unchanged rather than guessed at.
func splitQualifiedTypeName(typeName string) (qualifier, name string, ok bool) {
	inQuotes := false
	separator := -1
	for i := 0; i < len(typeName); i++ {
		switch typeName[i] {
		case '"':
			if inQuotes && i+1 < len(typeName) && typeName[i+1] == '"' {
				i++
				continue
			}
			inQuotes = !inQuotes
		case '.':
			if inQuotes {
				continue
			}
			if separator != -1 {
				return "", "", false
			}
			separator = i
		}
	}
	if inQuotes || separator <= 0 || separator == len(typeName)-1 {
		return "", "", false
	}
	qualifier = strings.TrimSpace(typeName[:separator])
	name = strings.TrimSpace(typeName[separator+1:])
	return qualifier, name, qualifier != "" && name != ""
}

func isCanonicalTypeQualifier(qualifier string) bool {
	qualifier = strings.TrimSpace(qualifier)
	if qualifier == `"pg_catalog"` || qualifier == `"public"` {
		return true
	}
	if strings.ContainsAny(qualifier, `".`) {
		return false
	}
	return strings.EqualFold(qualifier, "pg_catalog") || strings.EqualFold(qualifier, "public")
}
