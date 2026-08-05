// Package sqlrender contains the only rendering primitives that may turn
// PostgreSQL names and string values into generated SQL.
//
// Callers must distinguish an identifier component from a textual qualified
// name. Identifier and Qualified never interpret dots inside a component.
// ParseQualified is deliberately strict for the small number of model fields
// which currently store a qualified name as text.
package sqlrender

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// InvalidValueError reports a value that cannot be represented faithfully in
// PostgreSQL SQL text.
type InvalidValueError struct {
	Kind   string
	Value  string
	Reason string
}

func (e *InvalidValueError) Error() string {
	return fmt.Sprintf("invalid PostgreSQL %s %q: %s", e.Kind, e.Value, e.Reason)
}

// ValidateIdentifier validates one identifier component. A dot is valid in an
// identifier component and is quoted as data; it is never treated as a
// qualification separator by Identifier or Qualified.
func ValidateIdentifier(value string) error {
	return validateText("identifier", value, false)
}

// Identifier quotes one already-validated PostgreSQL identifier component.
// It is intentionally total so renderers can validate a complete object once
// at their public boundary and then compose SQL without partial output.
func Identifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// Qualified quotes identifier components without parsing any component.
func Qualified(parts ...string) string {
	quoted := make([]string, len(parts))
	for i := range parts {
		quoted[i] = Identifier(parts[i])
	}
	return strings.Join(quoted, ".")
}

// IdentifierList quotes each identifier component in order.
func IdentifierList(values []string) string {
	quoted := make([]string, len(values))
	for i := range values {
		quoted[i] = Identifier(values[i])
	}
	return strings.Join(quoted, ", ")
}

// Role renders a grantee or owner. PUBLIC is PostgreSQL's pseudo-role only
// when represented by the canonical, upper-case sentinel. Other spellings,
// including a real quoted role named "public", remain ordinary identifiers.
func Role(value string) (string, error) {
	if value == "PUBLIC" {
		return "PUBLIC", nil
	}
	if err := ValidateIdentifier(value); err != nil {
		return "", &InvalidValueError{Kind: "role", Value: value, Reason: err.(*InvalidValueError).Reason}
	}
	return Identifier(value), nil
}

// Literal renders a PostgreSQL string literal. NUL cannot occur in PostgreSQL
// text values, so accepting it would only defer failure (or C-string
// truncation) to a lower layer.
func Literal(value string) (string, error) {
	if err := validateText("literal", value, true); err != nil {
		return "", err
	}
	escaped := strings.ReplaceAll(value, `'`, `''`)
	if strings.Contains(escaped, `\`) {
		// E strings make backslash semantics independent of the session's
		// standard_conforming_strings setting. Escape backslashes first so
		// they cannot consume either quote from a doubled apostrophe.
		escaped = strings.ReplaceAll(escaped, `\`, `\\`)
		return `E'` + escaped + `'`, nil
	}
	return `'` + escaped + `'`, nil
}

// ParseQualified parses a textual qualified name containing between minParts
// and maxParts components and renders it canonically. It accepts PostgreSQL
// double-quoted components (including doubled quote escapes), but rejects
// whitespace, empty components, trailing dots, and over-qualification.
//
// Use Qualified instead whenever the model already stores components
// separately. This parser exists only for legacy string fields such as
// collation and operator-class names.
func ParseQualified(value string, minParts, maxParts int) (string, error) {
	if minParts < 1 || maxParts < minParts {
		return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "invalid component bounds"}
	}
	if value == "" {
		return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "must not be empty"}
	}
	if strings.TrimSpace(value) != value {
		return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "surrounding whitespace is ambiguous"}
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains a NUL byte"}
	}
	if !utf8.ValidString(value) {
		return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains invalid UTF-8"}
	}

	parts := make([]string, 0, maxParts)
	for pos := 0; pos < len(value); {
		if len(parts) == maxParts {
			return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: fmt.Sprintf("has more than %d components", maxParts)}
		}

		var part string
		if value[pos] == '"' {
			pos++
			var b strings.Builder
			closed := false
			for pos < len(value) {
				if value[pos] != '"' {
					b.WriteByte(value[pos])
					pos++
					continue
				}
				if pos+1 < len(value) && value[pos+1] == '"' {
					b.WriteByte('"')
					pos += 2
					continue
				}
				pos++
				closed = true
				break
			}
			if !closed {
				return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "has an unterminated quoted component"}
			}
			part = b.String()
			if pos < len(value) && value[pos] != '.' {
				return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "has characters after a quoted component"}
			}
		} else {
			start := pos
			for pos < len(value) && value[pos] != '.' {
				if value[pos] == '"' {
					return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains a quote inside an unquoted component"}
				}
				if value[pos] == ' ' || value[pos] == '\t' || value[pos] == '\r' || value[pos] == '\n' {
					return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains whitespace in an unquoted component"}
				}
				pos++
			}
			part = value[start:pos]
		}

		if err := ValidateIdentifier(part); err != nil {
			return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: err.(*InvalidValueError).Reason}
		}
		parts = append(parts, part)

		if pos == len(value) {
			break
		}
		pos++ // dot
		if pos == len(value) {
			return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "has a trailing dot"}
		}
	}

	if len(parts) < minParts {
		return "", &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: fmt.Sprintf("has fewer than %d components", minParts)}
	}
	return Qualified(parts...), nil
}

func validateText(kind, value string, allowEmpty bool) error {
	if !allowEmpty && value == "" {
		return &InvalidValueError{Kind: kind, Value: value, Reason: "must not be empty"}
	}
	if strings.IndexByte(value, 0) >= 0 {
		return &InvalidValueError{Kind: kind, Value: value, Reason: "contains a NUL byte"}
	}
	if !utf8.ValidString(value) {
		return &InvalidValueError{Kind: kind, Value: value, Reason: "contains invalid UTF-8"}
	}
	return nil
}
