package normalize

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"

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

	if canonical, err := canonicalizeExpr(exprStr); err == nil && canonical != "" {
		exprStr = canonical
	}

	typeCastRegex := regexp.MustCompile(`::[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*(?:\s+[a-zA-Z_][a-zA-Z0-9_]*)*(?:\[\])*`)
	exprStr = typeCastRegex.ReplaceAllString(exprStr, "")
	exprStr = stripOuterParentheses(exprStr)

	exprLower := strings.ToLower(exprStr)
	if exprLower == "current_timestamp" || exprLower == "current_timestamp()" {
		return "current_timestamp"
	}
	if exprLower == "now()" {
		return "current_timestamp"
	}
	return schema.Expr(exprLower)
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
			normalized := Expr(*normalizedCols[i].Default)
			normalizedCols[i].Default = &normalized
		}
		if normalizedCols[i].Generated != nil {
			normalizedCols[i].Generated.Expr = Expr(normalizedCols[i].Generated.Expr)
		}
	}

	sort.Slice(normalizedCols, func(i, j int) bool {
		return normalizedCols[i].Name < normalizedCols[j].Name
	})
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

	sortedInclude := make([]schema.ColumnName, len(idx.Include))
	copy(sortedInclude, idx.Include)
	sort.Slice(sortedInclude, func(i, j int) bool { return sortedInclude[i] < sortedInclude[j] })
	idx.Include = sortedInclude

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

func function(fn schema.Function) schema.Function {
	for i := range fn.Args {
		fn.Args[i].Type = schema.NormalizeTypeName(fn.Args[i].Type)
		if fn.Args[i].Default != nil {
			normalized := Expr(*fn.Args[i].Default)
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

	sortedPath := make([]schema.SchemaName, len(fn.SearchPath))
	copy(sortedPath, fn.SearchPath)
	sort.Slice(sortedPath, func(i, j int) bool { return sortedPath[i] < sortedPath[j] })
	fn.SearchPath = sortedPath

	fn.Grants = schema.CanonicalizeGrants(fn.Grants)
	return fn
}

func sequence(seq schema.Sequence) schema.Sequence {
	seq.Grants = schema.CanonicalizeGrants(seq.Grants)
	return seq
}

func enum(enum schema.EnumDef) schema.EnumDef {
	return enum
}

func domain(domain schema.DomainDef) schema.DomainDef {
	domain.BaseType = schema.NormalizeTypeName(domain.BaseType)
	if domain.Default != nil {
		normalized := Expr(*domain.Default)
		domain.Default = &normalized
	}
	if domain.Check != nil {
		normalized := Expr(*domain.Check)
		domain.Check = &normalized
	}
	return domain
}

func composite(comp schema.CompositeDef) schema.CompositeDef {
	sortedAttrs := make([]schema.CompositeAttr, len(comp.Attributes))
	copy(sortedAttrs, comp.Attributes)
	sort.Slice(sortedAttrs, func(i, j int) bool { return sortedAttrs[i].Name < sortedAttrs[j].Name })
	comp.Attributes = sortedAttrs
	return comp
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
	query := fmt.Sprintf("SELECT %s", expr)
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return "", err
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
