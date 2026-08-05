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
	"sort"
	"strings"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"google.golang.org/protobuf/reflect/protoreflect"
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

// Role renders an ordinary PostgreSQL role identifier. PostgreSQL's PUBLIC
// pseudo-role is deliberately not represented by this string API; callers
// must model and render it explicitly so a real quoted role named "PUBLIC"
// cannot be confused with the pseudo-role.
func Role(value string) (string, error) {
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
	parts, err := parseQualifiedIdentifier(value, minParts, maxParts)
	if err != nil {
		return "", err
	}
	values := make([]string, len(parts))
	for i := range parts {
		values[i] = parts[i].value
	}
	return Qualified(values...), nil
}

type qualifiedIdentifierPart struct {
	value  string
	quoted bool
}

func parseQualifiedIdentifier(value string, minParts, maxParts int) ([]qualifiedIdentifierPart, error) {
	if minParts < 1 || maxParts < minParts {
		return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "invalid component bounds"}
	}
	if value == "" {
		return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "must not be empty"}
	}
	if strings.TrimSpace(value) != value {
		return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "surrounding whitespace is ambiguous"}
	}
	if strings.IndexByte(value, 0) >= 0 {
		return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains a NUL byte"}
	}
	if !utf8.ValidString(value) {
		return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains invalid UTF-8"}
	}

	parts := make([]qualifiedIdentifierPart, 0, maxParts)
	for pos := 0; pos < len(value); {
		if len(parts) == maxParts {
			return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: fmt.Sprintf("has more than %d components", maxParts)}
		}

		var part string
		quoted := false
		if value[pos] == '"' {
			quoted = true
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
				return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "has an unterminated quoted component"}
			}
			part = b.String()
			if pos < len(value) && value[pos] != '.' {
				return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "has characters after a quoted component"}
			}
		} else {
			start := pos
			for pos < len(value) && value[pos] != '.' {
				if value[pos] == '"' {
					return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains a quote inside an unquoted component"}
				}
				if value[pos] == ' ' || value[pos] == '\t' || value[pos] == '\r' || value[pos] == '\n' {
					return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "contains whitespace in an unquoted component"}
				}
				pos++
			}
			part = value[start:pos]
		}

		if err := ValidateIdentifier(part); err != nil {
			return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: err.(*InvalidValueError).Reason}
		}
		parts = append(parts, qualifiedIdentifierPart{value: part, quoted: quoted})

		if pos == len(value) {
			break
		}
		pos++ // dot
		if pos == len(value) {
			return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: "has a trailing dot"}
		}
	}

	if len(parts) < minParts {
		return nil, &InvalidValueError{Kind: "qualified identifier", Value: value, Reason: fmt.Sprintf("has fewer than %d components", minParts)}
	}
	return parts, nil
}

// NextvalReference is one exact nextval('name'::regclass) call found in an SQL
// expression. Reference is a canonically quoted one- or two-part name.
type NextvalReference struct {
	Reference string
	Qualified bool
}

// NextvalRegclassReference recognizes only an expression whose root is exactly
// nextval('name'::regclass), optionally pg_catalog-qualified. SERIAL collapse
// uses this strict form: a wrapper or compound expression is observably not the
// PostgreSQL SERIAL expansion even when it contains a nextval call.
func NextvalRegclassReference(expression string) (reference string, qualified bool, ok bool) {
	root := parseExpressionRoot(expression)
	match, ok := nextvalRegclassReferenceFromNode(root)
	if !ok {
		return "", false, false
	}
	return match.Reference, match.Qualified, true
}

// NextvalRegclassReferences finds exact nextval('name'::regclass) call nodes
// anywhere inside an expression. Dependency discovery uses this AST walk so a
// sequence is created before a table whose compound default references it.
// Invalid SQL and near-miss calls are ignored, and duplicate references are
// returned once in deterministic order.
func NextvalRegclassReferences(expression string) []NextvalReference {
	root := parseExpressionRoot(expression)
	if root == nil {
		return nil
	}

	found := make(map[NextvalReference]struct{})
	walkSQLMessages(root.ProtoReflect(), func(node *pg_query.Node) {
		if match, ok := nextvalRegclassReferenceFromNode(node); ok {
			found[match] = struct{}{}
		}
	})

	references := make([]NextvalReference, 0, len(found))
	for reference := range found {
		references = append(references, reference)
	}
	sort.Slice(references, func(i, j int) bool {
		if references[i].Reference != references[j].Reference {
			return references[i].Reference < references[j].Reference
		}
		return !references[i].Qualified && references[j].Qualified
	})
	return references
}

func parseExpressionRoot(expression string) *pg_query.Node {
	parsed, err := pg_query.Parse("SELECT " + strings.TrimSpace(expression))
	if err != nil || len(parsed.Stmts) != 1 || parsed.Stmts[0].Stmt == nil {
		return nil
	}
	selectStmt := parsed.Stmts[0].Stmt.GetSelectStmt()
	if selectStmt == nil || len(selectStmt.TargetList) != 1 {
		return nil
	}
	target := selectStmt.TargetList[0].GetResTarget()
	if target == nil {
		return nil
	}
	return target.Val
}

func nextvalRegclassReferenceFromNode(node *pg_query.Node) (NextvalReference, bool) {
	if node == nil {
		return NextvalReference{}, false
	}
	call := node.GetFuncCall()
	if call == nil || len(call.Funcname) == 0 || len(call.Funcname) > 2 || len(call.Args) != 1 {
		return NextvalReference{}, false
	}
	functionParts := make([]string, len(call.Funcname))
	for i, node := range call.Funcname {
		value := node.GetString_()
		if value == nil {
			return NextvalReference{}, false
		}
		functionParts[i] = value.Sval
	}
	// PostgreSQL folds unquoted identifiers before they reach this AST. Exact
	// comparison therefore accepts ordinary NEXTVAL while preserving the
	// distinct semantics of a quoted function such as "NEXTVAL".
	if functionParts[len(functionParts)-1] != "nextval" {
		return NextvalReference{}, false
	}
	if len(functionParts) == 2 && functionParts[0] != "pg_catalog" {
		return NextvalReference{}, false
	}

	cast := call.Args[0].GetTypeCast()
	if cast == nil || cast.TypeName == nil || cast.Arg == nil {
		return NextvalReference{}, false
	}
	typeNames := cast.TypeName.Names
	if len(typeNames) == 0 || len(typeNames) > 2 {
		return NextvalReference{}, false
	}
	regclass := typeNames[len(typeNames)-1].GetString_()
	if regclass == nil || regclass.Sval != "regclass" {
		return NextvalReference{}, false
	}
	if len(typeNames) == 2 {
		qualifier := typeNames[0].GetString_()
		if qualifier == nil || qualifier.Sval != "pg_catalog" {
			return NextvalReference{}, false
		}
	}
	constant := cast.Arg.GetAConst()
	if constant == nil || constant.GetSval() == nil {
		return NextvalReference{}, false
	}
	rawReference := constant.GetSval().Sval
	parts, err := parseQualifiedIdentifier(rawReference, 1, 2)
	if err != nil {
		return NextvalReference{}, false
	}
	values := make([]string, len(parts))
	for i, part := range parts {
		values[i] = part.value
		if !part.quoted {
			values[i] = strings.ToLower(values[i])
		}
	}
	return NextvalReference{Reference: Qualified(values...), Qualified: len(values) == 2}, true
}

func walkSQLMessages(message protoreflect.Message, visit func(*pg_query.Node)) {
	if node, ok := message.Interface().(*pg_query.Node); ok {
		visit(node)
	}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Kind() == protoreflect.MessageKind:
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				walkSQLMessages(list.Get(i).Message(), visit)
			}
		case field.IsMap() && field.MapValue().Kind() == protoreflect.MessageKind:
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				walkSQLMessages(item.Message(), visit)
				return true
			})
		case !field.IsList() && !field.IsMap() && field.Kind() == protoreflect.MessageKind:
			walkSQLMessages(value.Message(), visit)
		}
		return true
	})
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
