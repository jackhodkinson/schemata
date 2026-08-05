package normalize

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Object normalizes a database object into a canonical representation.
func Object(obj schema.DatabaseObject) schema.DatabaseObject {
	switch v := obj.(type) {
	case schema.Table:
		return table(v)
	case schema.Index:
		return index(v)
	case schema.View:
		return view(v)
	case schema.Function:
		return function(v)
	case schema.Sequence:
		return sequence(v)
	case schema.EnumDef:
		return enum(v)
	case schema.DomainDef:
		return domain(v)
	case schema.CompositeDef:
		return composite(v)
	case schema.Trigger:
		return trigger(v)
	case schema.Policy:
		return policy(v)
	case schema.Extension:
		return extension(v)
	default:
		return obj
	}
}

// Expr normalizes SQL expressions to a canonical form.
func Expr(expr schema.Expr) schema.Expr {
	exprStr := strings.TrimSpace(string(expr))
	// pg_query_go crosses a C string boundary. Never pass an embedded NUL to
	// it: doing so truncates the expression and can make normalization
	// non-idempotent. The declarative parser rejects NUL-containing SQL.
	if strings.IndexByte(exprStr, 0) >= 0 {
		return schema.Expr(exprStr)
	}

	canonical, err := canonicalizeExpr(exprStr)
	if err != nil || canonical == "" {
		// Invalid or context-dependent fragments must remain opaque. Applying
		// partial textual rewrites after a parse failure can turn one malformed
		// value into a different, accidentally valid expression.
		return schema.Expr(exprStr)
	}
	exprStr = canonical
	exprStr = stripOuterParentheses(exprStr)

	if strings.EqualFold(exprStr, "current_timestamp") || strings.EqualFold(exprStr, "current_timestamp()") {
		return "current_timestamp"
	}
	if strings.EqualFold(exprStr, "now()") {
		return "current_timestamp"
	}
	return schema.Expr(exprStr)
}

// FunctionBody normalizes function bodies while preserving quoted literals/identifiers.
func FunctionBody(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}

	const (
		stateNormal = iota
		stateSingleQuote
		stateDoubleQuote
		stateDollarQuote
		stateLineComment
	)

	state := stateNormal
	dollarTag := ""
	pendingSpace := false

	var out strings.Builder
	out.Grow(len(body))

	for i := 0; i < len(body); i++ {
		ch := body[i]

		switch state {
		case stateNormal:
			if isSQLWhitespace(ch) {
				pendingSpace = true
				continue
			}

			if pendingSpace && out.Len() > 0 {
				out.WriteByte(' ')
			}
			pendingSpace = false

			if ch == '\'' {
				state = stateSingleQuote
				out.WriteByte(ch)
				continue
			}
			if ch == '"' {
				state = stateDoubleQuote
				out.WriteByte(ch)
				continue
			}
			if tag, ok := detectDollarTag(body, i); ok {
				state = stateDollarQuote
				dollarTag = tag
				out.WriteString(tag)
				i += len(tag) - 1
				continue
			}
			if ch == '-' && i+1 < len(body) && body[i+1] == '-' {
				state = stateLineComment
				out.WriteString("--")
				i++
				continue
			}

			out.WriteByte(toLowerASCII(ch))

		case stateSingleQuote:
			out.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(body) && body[i+1] == '\'' {
					out.WriteByte(body[i+1])
					i++
				} else {
					state = stateNormal
				}
			}

		case stateDoubleQuote:
			out.WriteByte(ch)
			if ch == '"' {
				if i+1 < len(body) && body[i+1] == '"' {
					out.WriteByte(body[i+1])
					i++
				} else {
					state = stateNormal
				}
			}

		case stateDollarQuote:
			if strings.HasPrefix(body[i:], dollarTag) {
				out.WriteString(dollarTag)
				i += len(dollarTag) - 1
				state = stateNormal
				continue
			}
			out.WriteByte(ch)

		case stateLineComment:
			if ch == '\r' || ch == '\n' {
				if ch == '\r' && i+1 < len(body) && body[i+1] == '\n' {
					i++
				}
				// Unlike ordinary whitespace, this newline terminates the SQL
				// comment and is therefore meaning-bearing.
				out.WriteByte('\n')
				state = stateNormal
				pendingSpace = false
				continue
			}
			out.WriteByte(ch)
		}
	}

	return strings.TrimSpace(out.String())
}

func table(tbl schema.Table) schema.Table {
	normalizedCols := make([]schema.Column, len(tbl.Columns))
	copy(normalizedCols, tbl.Columns)
	for i := range normalizedCols {
		normalizedCols[i].Type = schema.NormalizeTypeName(normalizedCols[i].Type)
		if normalizedCols[i].Default != nil {
			normalized := exprForType(*normalizedCols[i].Default, normalizedCols[i].Type)
			normalizedCols[i].Default = &normalized
		}
		if normalizedCols[i].Generated != nil {
			normalizedCols[i].Generated.Expr = Expr(normalizedCols[i].Generated.Expr)
		}
	}

	tbl.Columns = normalizedCols

	for i := range tbl.Uniques {
		if tbl.Uniques[i].Name == "" && len(tbl.Uniques[i].Cols) > 0 {
			colParts := make([]string, len(tbl.Uniques[i].Cols))
			for j, col := range tbl.Uniques[i].Cols {
				colParts[j] = string(col)
			}
			tbl.Uniques[i].Name = string(tbl.Name) + "_" + strings.Join(colParts, "_") + "_key"
		}
	}

	sort.Slice(tbl.Uniques, func(i, j int) bool { return tbl.Uniques[i].Name < tbl.Uniques[j].Name })
	sort.Slice(tbl.Checks, func(i, j int) bool { return tbl.Checks[i].Name < tbl.Checks[j].Name })
	sort.Slice(tbl.ForeignKeys, func(i, j int) bool { return tbl.ForeignKeys[i].Name < tbl.ForeignKeys[j].Name })

	if tbl.RelOptions != nil {
		sorted := make([]string, len(tbl.RelOptions))
		copy(sorted, tbl.RelOptions)
		sort.Strings(sorted)
		tbl.RelOptions = sorted
	}

	tbl.Grants = schema.CanonicalizeGrants(tbl.Grants)
	return tbl
}

func index(idx schema.Index) schema.Index {
	normalizedExprs := make([]schema.IndexKeyExpr, len(idx.KeyExprs))
	for i, keyExpr := range idx.KeyExprs {
		normalizedExprs[i] = keyExpr
		normalizedExprs[i].Expr = Expr(keyExpr.Expr)
	}
	idx.KeyExprs = normalizedExprs

	if idx.Predicate != nil {
		normalized := Expr(*idx.Predicate)
		idx.Predicate = &normalized
	}

	return idx
}

func view(v schema.View) schema.View {
	query := strings.TrimSpace(v.Definition.Query)
	if query == "" {
		v.Grants = schema.CanonicalizeGrants(v.Grants)
		return v
	}

	parsed, err := pg_query.Parse(query)
	if err != nil {
		v.Grants = schema.CanonicalizeGrants(v.Grants)
		return v
	}
	if err := validateASTForDeparse(parsed.ProtoReflect()); err != nil {
		v.Grants = schema.CanonicalizeGrants(v.Grants)
		return v
	}
	stripPublicRangeVarQualifications(parsed.ProtoReflect())
	deparsed, err := pg_query.Deparse(parsed)
	if err != nil {
		v.Grants = schema.CanonicalizeGrants(v.Grants)
		return v
	}
	deparsed = strings.TrimSpace(deparsed)
	deparsed = strings.TrimSuffix(deparsed, ";")
	v.Definition.Query = deparsed
	v.Grants = schema.CanonicalizeGrants(v.Grants)
	return v
}

// stripPublicRangeVarQualifications makes explicit public relation references
// canonical with the parser's unqualified default. Qualifications for every
// other schema are preserved. Walking protobuf reflection keeps this valid for
// nested SELECTs, CTE bodies, and subqueries without a fragile SQL text rewrite.
func stripPublicRangeVarQualifications(message protoreflect.Message) {
	if message.Descriptor().FullName() == "pg_query.RangeVar" {
		field := message.Descriptor().Fields().ByName("schemaname")
		// PostgreSQL's parser has already folded unquoted identifiers to lower
		// case. Exact comparison therefore includes public, PUBLIC, and
		// "public", while preserving the distinct quoted schema "PUBLIC".
		if field != nil && message.Get(field).String() == "public" {
			message.Clear(field)
		}
	}
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Kind() == protoreflect.MessageKind:
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				stripPublicRangeVarQualifications(list.Get(i).Message())
			}
		case field.IsMap() && field.MapValue().Kind() == protoreflect.MessageKind:
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				stripPublicRangeVarQualifications(item.Message())
				return true
			})
		case !field.IsList() && !field.IsMap() && field.Kind() == protoreflect.MessageKind:
			stripPublicRangeVarQualifications(value.Message())
		}
		return true
	})
}

func function(fn schema.Function) schema.Function {
	for i := range fn.Args {
		fn.Args[i].Type = schema.NormalizeTypeName(fn.Args[i].Type)
		if fn.Args[i].Default != nil {
			normalized := exprForType(*fn.Args[i].Default, fn.Args[i].Type)
			fn.Args[i].Default = &normalized
		}
	}

	switch ret := fn.Returns.(type) {
	case schema.ReturnsType:
		ret.Type = schema.NormalizeTypeName(ret.Type)
		fn.Returns = ret
	case schema.ReturnsSetOf:
		ret.Type = schema.NormalizeTypeName(ret.Type)
		fn.Returns = ret
	case schema.ReturnsTable:
		for i := range ret.Columns {
			ret.Columns[i].Type = schema.NormalizeTypeName(ret.Columns[i].Type)
		}
		fn.Returns = ret
	}

	fn.Body = FunctionBody(fn.Body)

	fn.Grants = schema.CanonicalizeGrants(fn.Grants)
	return fn
}

func sequence(seq schema.Sequence) schema.Sequence {
	seq.Type = string(schema.NormalizeTypeName(schema.TypeName(seq.Type)))
	seq.Grants = schema.CanonicalizeGrants(seq.Grants)
	return seq
}

func enum(enum schema.EnumDef) schema.EnumDef {
	enum.Grants = schema.CanonicalizeGrants(enum.Grants)
	return enum
}

func domain(domain schema.DomainDef) schema.DomainDef {
	domain.BaseType = schema.NormalizeTypeName(domain.BaseType)
	if domain.Default != nil {
		normalized := exprForType(*domain.Default, domain.BaseType)
		domain.Default = &normalized
	}
	if domain.Check != nil {
		normalized := Expr(*domain.Check)
		domain.Check = &normalized
	}
	domain.Grants = schema.CanonicalizeGrants(domain.Grants)
	return domain
}

func composite(comp schema.CompositeDef) schema.CompositeDef {
	for i := range comp.Attributes {
		comp.Attributes[i].Type = schema.NormalizeTypeName(comp.Attributes[i].Type)
	}
	comp.Grants = schema.CanonicalizeGrants(comp.Grants)
	return comp
}

// exprForType removes only a trailing cast that is provably redundant in the
// typed field being normalized. Expr itself deliberately preserves casts
// because casts can change values, operators, collations, and function
// resolution.
func exprForType(expr schema.Expr, valueType schema.TypeName) schema.Expr {
	normalized := Expr(expr)
	base, castType, ok := splitTrailingCast(string(normalized))
	if !ok || schema.NormalizeTypeName(schema.TypeName(castType)) != schema.NormalizeTypeName(valueType) {
		return normalized
	}
	return schema.Expr(stripOuterParentheses(strings.TrimSpace(base)))
}

func splitTrailingCast(expr string) (base string, castType string, ok bool) {
	const (
		castStateNormal = iota
		castStateSingleQuote
		castStateDoubleQuote
		castStateDollarQuote
	)

	state := castStateNormal
	depth := 0
	dollarTag := ""
	lastCast := -1

	for i := 0; i < len(expr); i++ {
		switch state {
		case castStateNormal:
			switch expr[i] {
			case '\'':
				state = castStateSingleQuote
			case '"':
				state = castStateDoubleQuote
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			case '$':
				if tag, found := detectDollarTag(expr, i); found {
					state = castStateDollarQuote
					dollarTag = tag
					i += len(tag) - 1
				}
			case ':':
				if depth == 0 && i+1 < len(expr) && expr[i+1] == ':' {
					lastCast = i
					i++
				}
			}
		case castStateSingleQuote:
			if expr[i] == '\'' {
				if i+1 < len(expr) && expr[i+1] == '\'' {
					i++
				} else {
					state = castStateNormal
				}
			}
		case castStateDoubleQuote:
			if expr[i] == '"' {
				if i+1 < len(expr) && expr[i+1] == '"' {
					i++
				} else {
					state = castStateNormal
				}
			}
		case castStateDollarQuote:
			if strings.HasPrefix(expr[i:], dollarTag) {
				i += len(dollarTag) - 1
				state = castStateNormal
			}
		}
	}

	if lastCast < 0 {
		return "", "", false
	}
	base = strings.TrimSpace(expr[:lastCast])
	castType = strings.TrimSpace(expr[lastCast+2:])
	if base == "" || castType == "" {
		return "", "", false
	}
	return base, castType, true
}

func trigger(trig schema.Trigger) schema.Trigger {
	if len(trig.Events) > 1 {
		sorted := make([]schema.TriggerEvent, len(trig.Events))
		copy(sorted, trig.Events)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		trig.Events = sorted
	}
	return trig
}

func policy(pol schema.Policy) schema.Policy {
	if len(pol.To) > 1 {
		sorted := make([]string, len(pol.To))
		copy(sorted, pol.To)
		sort.Strings(sorted)
		pol.To = sorted
	}
	if pol.Using != nil {
		normalized := Expr(*pol.Using)
		pol.Using = &normalized
	}
	if pol.WithCheck != nil {
		normalized := Expr(*pol.WithCheck)
		pol.WithCheck = &normalized
	}
	return pol
}

func extension(ext schema.Extension) schema.Extension {
	ext.Version = nil
	return ext
}

func canonicalizeExpr(expr string) (string, error) {
	if expr == "" {
		return "", nil
	}

	// PostgreSQL's parser/deparser is not guaranteed to reach its canonical
	// spelling in one pass. For example, malformed operator expressions can
	// acquire grouping on a second pass. Iterate to a fixed point so callers
	// never observe a value that changes when it is normalized again. Treat a
	// cycle or a failure to converge as a parse failure; Expr will then preserve
	// the original fragment opaquely.
	const maxCanonicalizationPasses = 16
	current := expr
	seen := make(map[string]struct{}, maxCanonicalizationPasses)
	for range maxCanonicalizationPasses {
		if _, exists := seen[current]; exists {
			return "", fmt.Errorf("expression canonicalization entered a cycle")
		}
		seen[current] = struct{}{}

		next, err := canonicalizeExprOnce(current)
		if err != nil {
			return "", err
		}
		if next == current {
			return next, nil
		}
		current = next
	}

	return "", fmt.Errorf("expression canonicalization did not converge after %d passes", maxCanonicalizationPasses)
}

func canonicalizeExprOnce(expr string) (string, error) {
	return canonicalizeExprOnceWithTransforms(expr, stripPublicRegclassQualifications)
}

func canonicalizeExprOnceWithTransforms(expr string, transforms ...func(protoreflect.Message)) (string, error) {
	query := fmt.Sprintf("SELECT %s", expr)
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return "", err
	}
	if err := validateASTForDeparse(parsed.ProtoReflect()); err != nil {
		return "", err
	}
	for _, transform := range transforms {
		transform(parsed.ProtoReflect())
	}
	deparsed, err := pg_query.Deparse(parsed)
	if err != nil {
		return "", err
	}
	deparsed = strings.TrimSpace(deparsed)
	deparsed = strings.TrimPrefix(deparsed, "SELECT ")
	deparsed = strings.TrimSuffix(deparsed, ";")
	return strings.TrimSpace(deparsed), nil
}

// stripPublicRegclassQualifications makes explicit public references canonical
// with the unqualified form emitted under catalog extraction's fixed
// `pg_catalog, public` search path. It only rewrites a parsed regclass cast and
// an exact public qualifier, never an arbitrary string or substring.
func stripPublicRegclassQualifications(message protoreflect.Message) {
	if cast, ok := message.Interface().(*pg_query.TypeCast); ok && isRegclassTypeName(cast.TypeName) {
		if len(cast.TypeName.Names) == 2 {
			cast.TypeName.Names = cast.TypeName.Names[1:]
		}
		if constant := cast.Arg.GetAConst(); constant != nil {
			if value := constant.GetSval(); value != nil {
				if unqualified, ok := stripPublicRegclassQualifier(value.Sval); ok {
					value.Sval = unqualified
				}
			}
		}
	}
	if call, ok := message.Interface().(*pg_query.FuncCall); ok && isPgCatalogNextval(call.Funcname) {
		call.Funcname = call.Funcname[1:]
	}

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Kind() == protoreflect.MessageKind:
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				stripPublicRegclassQualifications(list.Get(i).Message())
			}
		case field.IsMap() && field.MapValue().Kind() == protoreflect.MessageKind:
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				stripPublicRegclassQualifications(item.Message())
				return true
			})
		case !field.IsList() && !field.IsMap() && field.Kind() == protoreflect.MessageKind:
			stripPublicRegclassQualifications(value.Message())
		}
		return true
	})
}

func isPgCatalogNextval(parts []*pg_query.Node) bool {
	if len(parts) != 2 {
		return false
	}
	prefix := parts[0].GetString_()
	name := parts[1].GetString_()
	return prefix != nil && name != nil &&
		strings.EqualFold(prefix.Sval, "pg_catalog") &&
		strings.EqualFold(name.Sval, "nextval")
}

func isRegclassTypeName(typeName *pg_query.TypeName) bool {
	if typeName == nil || len(typeName.Names) == 0 || len(typeName.Names) > 2 {
		return false
	}
	last := typeName.Names[len(typeName.Names)-1].GetString_()
	if last == nil || !strings.EqualFold(last.Sval, "regclass") {
		return false
	}
	if len(typeName.Names) == 2 {
		prefix := typeName.Names[0].GetString_()
		return prefix != nil && strings.EqualFold(prefix.Sval, "pg_catalog")
	}
	return true
}

func stripPublicRegclassQualifier(value string) (string, bool) {
	if value == "" {
		return "", false
	}

	var separator int
	if value[0] == '"' {
		separator = quotedIdentifierEnd(value)
		if separator < 0 || value[1:separator-1] != "public" {
			return "", false
		}
	} else {
		separator = strings.IndexByte(value, '.')
		if separator <= 0 || !strings.EqualFold(value[:separator], "public") {
			return "", false
		}
		for _, char := range value[:separator] {
			if char == '"' || char == ' ' || char == '\t' || char == '\r' || char == '\n' {
				return "", false
			}
		}
	}

	if separator >= len(value) || value[separator] != '.' {
		return "", false
	}
	unqualified := value[separator+1:]
	if !isSingleRegclassIdentifier(unqualified) {
		return "", false
	}
	return unqualified, true
}

// quotedIdentifierEnd returns the index immediately after a complete quoted
// identifier at the start of value, accounting for doubled quote escapes.
func quotedIdentifierEnd(value string) int {
	for i := 1; i < len(value); i++ {
		if value[i] != '"' {
			continue
		}
		if i+1 < len(value) && value[i+1] == '"' {
			i++
			continue
		}
		return i + 1
	}
	return -1
}

func isSingleRegclassIdentifier(value string) bool {
	if value == "" {
		return false
	}
	if value[0] == '"' {
		return quotedIdentifierEnd(value) == len(value)
	}
	for _, char := range value {
		if char == '.' || char == '"' || char == ' ' || char == '\t' || char == '\r' || char == '\n' {
			return false
		}
	}
	return true
}

func stripOuterParentheses(expr string) string {
	for {
		expr = strings.TrimSpace(expr)
		if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
			return expr
		}

		depth := 0
		inLiteral := false
		valid := true

		for i := 0; i < len(expr); i++ {
			ch := expr[i]
			if inLiteral {
				if ch == '\'' {
					if i+1 < len(expr) && expr[i+1] == '\'' {
						i++
					} else {
						inLiteral = false
					}
				}
				continue
			}

			switch ch {
			case '\'':
				inLiteral = true
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 && i != len(expr)-1 {
					valid = false
					return expr
				}
				if depth < 0 {
					valid = false
					return expr
				}
			}
		}

		if !valid || depth != 0 {
			return expr
		}

		expr = expr[1 : len(expr)-1]
	}
}

func isSQLWhitespace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

func toLowerASCII(ch byte) byte {
	if ch >= 'A' && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	return ch
}

func detectDollarTag(s string, start int) (string, bool) {
	if start >= len(s) || s[start] != '$' {
		return "", false
	}

	j := start + 1
	for j < len(s) {
		if s[j] == '$' {
			return s[start : j+1], true
		}
		if !isDollarTagChar(s[j]) {
			return "", false
		}
		j++
	}
	return "", false
}

func isDollarTagChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}
