package differ

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Hash computes a stable SHA-256 hash of a database object
func Hash(obj schema.DatabaseObject) (string, error) {
	// Serialize to JSON with sorted keys
	data, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object: %w", err)
	}

	// Compute SHA-256 hash
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}

// HashString computes a SHA-256 hash of a string
func HashString(s string) string {
	hash := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", hash)
}

// NormalizeAndHash normalizes an object and computes its hash
// Normalization ensures that equivalent objects produce the same hash
func NormalizeAndHash(obj schema.DatabaseObject) (string, error) {
	// Normalize the object first
	normalized := normalize(obj)

	// Compute hash
	return Hash(normalized)
}

// normalize applies normalization rules to make objects comparable
func normalize(obj schema.DatabaseObject) schema.DatabaseObject {
	switch v := obj.(type) {
	case schema.Table:
		return normalizeTable(v)
	case schema.Index:
		return normalizeIndex(v)
	case schema.View:
		return normalizeView(v)
	case schema.Function:
		return normalizeFunction(v)
	case schema.Sequence:
		return normalizeSequence(v)
	case schema.EnumDef:
		return normalizeEnum(v)
	case schema.DomainDef:
		return normalizeDomain(v)
	case schema.CompositeDef:
		return normalizeComposite(v)
	case schema.Trigger:
		return normalizeTrigger(v)
	case schema.Policy:
		return normalizePolicy(v)
	default:
		// For Schema, Extension, and other simple types, no normalization needed
		return obj
	}
}

func normalizeTable(tbl schema.Table) schema.Table {
	// Normalize column types
	normalizedCols := make([]schema.Column, len(tbl.Columns))
	copy(normalizedCols, tbl.Columns)
	for i := range normalizedCols {
		normalizedCols[i].Type = schema.NormalizeTypeName(normalizedCols[i].Type)
		// Normalize default expressions
		if normalizedCols[i].Default != nil {
			normalized := normalizeExpr(*normalizedCols[i].Default)
			normalizedCols[i].Default = &normalized
		}
		if normalizedCols[i].Generated != nil {
			normalizedCols[i].Generated.Expr = normalizeExpr(normalizedCols[i].Generated.Expr)
		}
	}

	// Sort columns by name for consistent hashing
	// Note: While column order affects physical layout in Postgres, for schema diffing
	// purposes we treat tables with the same columns in different order as equivalent.
	// Physical column reordering requires table rebuild, which is beyond basic schema management.
	sort.Slice(normalizedCols, func(i, j int) bool {
		return normalizedCols[i].Name < normalizedCols[j].Name
	})
	tbl.Columns = normalizedCols

	// Assign auto-generated names to unnamed unique constraints.
	// PostgreSQL names unnamed UNIQUE constraints as {table}_{cols}_key.
	// Without this, the parser's empty name won't match the catalog's auto-name.
	for i := range tbl.Uniques {
		if tbl.Uniques[i].Name == "" && len(tbl.Uniques[i].Cols) > 0 {
			colParts := make([]string, len(tbl.Uniques[i].Cols))
			for j, col := range tbl.Uniques[i].Cols {
				colParts[j] = string(col)
			}
			tbl.Uniques[i].Name = string(tbl.Name) + "_" + strings.Join(colParts, "_") + "_key"
		}
	}

	// Sort constraints by name for consistent hashing
	sort.Slice(tbl.Uniques, func(i, j int) bool {
		return tbl.Uniques[i].Name < tbl.Uniques[j].Name
	})
	sort.Slice(tbl.Checks, func(i, j int) bool {
		return tbl.Checks[i].Name < tbl.Checks[j].Name
	})
	sort.Slice(tbl.ForeignKeys, func(i, j int) bool {
		return tbl.ForeignKeys[i].Name < tbl.ForeignKeys[j].Name
	})

	// Sort reloptions
	if tbl.RelOptions != nil {
		sorted := make([]string, len(tbl.RelOptions))
		copy(sorted, tbl.RelOptions)
		sort.Strings(sorted)
		tbl.RelOptions = sorted
	}

	return tbl
}

func normalizeIndex(idx schema.Index) schema.Index {
	// Normalize key expressions
	normalizedExprs := make([]schema.IndexKeyExpr, len(idx.KeyExprs))
	for i, keyExpr := range idx.KeyExprs {
		normalizedExprs[i] = keyExpr
		normalizedExprs[i].Expr = normalizeExpr(keyExpr.Expr)
	}
	idx.KeyExprs = normalizedExprs

	// Normalize predicate if present
	if idx.Predicate != nil {
		normalized := normalizeExpr(*idx.Predicate)
		idx.Predicate = &normalized
	}

	// Sort include columns
	sortedInclude := make([]schema.ColumnName, len(idx.Include))
	copy(sortedInclude, idx.Include)
	sort.Slice(sortedInclude, func(i, j int) bool {
		return sortedInclude[i] < sortedInclude[j]
	})
	idx.Include = sortedInclude

	return idx
}

func normalizeView(view schema.View) schema.View {
	query := strings.TrimSpace(view.Definition.Query)
	if query == "" {
		return view
	}

	parsed, err := pg_query.Parse(query)
	if err != nil {
		return view
	}
	deparsed, err := pg_query.Deparse(parsed)
	if err != nil {
		return view
	}
	deparsed = strings.TrimSpace(deparsed)
	deparsed = strings.TrimSuffix(deparsed, ";")
	view.Definition.Query = deparsed
	return view
}

func normalizeFunction(fn schema.Function) schema.Function {
	for i := range fn.Args {
		fn.Args[i].Type = schema.NormalizeTypeName(fn.Args[i].Type)
		if fn.Args[i].Default != nil {
			normalized := normalizeExpr(*fn.Args[i].Default)
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

	// Normalize function body
	// 1. Trim leading/trailing whitespace
	// 2. Normalize internal whitespace (multiple spaces/newlines to single space)
	// 3. Convert to lowercase for case-insensitive comparison
	body := strings.TrimSpace(fn.Body)

	// Normalize whitespace: replace multiple whitespace chars with single space
	body = regexp.MustCompile(`\s+`).ReplaceAllString(body, " ")

	// Convert to lowercase for case-insensitive keyword comparison
	fn.Body = strings.ToLower(body)

	// Sort search path
	sortedPath := make([]schema.SchemaName, len(fn.SearchPath))
	copy(sortedPath, fn.SearchPath)
	sort.Slice(sortedPath, func(i, j int) bool {
		return sortedPath[i] < sortedPath[j]
	})
	fn.SearchPath = sortedPath

	return fn
}

func normalizeSequence(seq schema.Sequence) schema.Sequence {
	// Sequences don't need special normalization
	return seq
}

func normalizeEnum(enum schema.EnumDef) schema.EnumDef {
	// Enum values order matters, so don't sort
	return enum
}

func normalizeDomain(domain schema.DomainDef) schema.DomainDef {
	// Domains don't need special normalization
	return domain
}

func normalizeComposite(comp schema.CompositeDef) schema.CompositeDef {
	// Sort attributes by name for consistent hashing
	// Note: Unlike table columns, composite type attribute order typically doesn't matter
	// in the same way for most use cases
	sortedAttrs := make([]schema.CompositeAttr, len(comp.Attributes))
	copy(sortedAttrs, comp.Attributes)
	sort.Slice(sortedAttrs, func(i, j int) bool {
		return sortedAttrs[i].Name < sortedAttrs[j].Name
	})
	comp.Attributes = sortedAttrs
	return comp
}

func normalizeTrigger(trig schema.Trigger) schema.Trigger {
	// Sort events for consistent comparison
	if len(trig.Events) > 1 {
		sorted := make([]schema.TriggerEvent, len(trig.Events))
		copy(sorted, trig.Events)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i] < sorted[j]
		})
		trig.Events = sorted
	}
	return trig
}

func normalizePolicy(pol schema.Policy) schema.Policy {
	// Sort role names for consistent comparison
	if len(pol.To) > 1 {
		sorted := make([]string, len(pol.To))
		copy(sorted, pol.To)
		sort.Strings(sorted)
		pol.To = sorted
	}
	return pol
}

// normalizeExpr normalizes SQL expressions to a canonical form
func normalizeExpr(expr schema.Expr) schema.Expr {
	exprStr := strings.TrimSpace(string(expr))

	if canonical, err := canonicalizeExpr(exprStr); err == nil && canonical != "" {
		exprStr = canonical
	}

	// Strip PostgreSQL type casts (::typename) for normalization
	// This handles cases where catalog returns 'value'::typename but parser returns 'value'
	// Common for ENUM defaults: 'user'::user_role vs 'user'
	// We use a regex to match :: followed by a type name
	typeCastRegex := regexp.MustCompile(`::[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*(?:\s+[a-zA-Z_][a-zA-Z0-9_]*)*(?:\[\])*`)
	exprStr = typeCastRegex.ReplaceAllString(exprStr, "")

	// Remove redundant wrapping parentheses
	exprStr = stripOuterParentheses(exprStr)

	// Normalize to lowercase for function names and keywords
	exprLower := strings.ToLower(exprStr)

	// Common expression normalizations
	// CURRENT_TIMESTAMP, current_timestamp, CURRENT_TIMESTAMP(), etc.
	if exprLower == "current_timestamp" || exprLower == "current_timestamp()" {
		return "current_timestamp"
	}

	// now() is equivalent to CURRENT_TIMESTAMP
	if exprLower == "now()" {
		return "current_timestamp"
	}

	// Default: return lowercase version
	return schema.Expr(exprLower)
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
					break
				}
				if depth < 0 {
					valid = false
					break
				}
			}
		}

		if !valid || depth != 0 {
			return expr
		}

		expr = expr[1 : len(expr)-1]
	}
}
