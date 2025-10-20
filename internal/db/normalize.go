package db

import (
	"regexp"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// NormalizeTable normalizes a table extracted from the catalog to match
// the parser's representation. This is necessary because PostgreSQL expands
// certain syntactic sugar (like SERIAL) into their underlying implementation.
func NormalizeTable(tbl schema.Table, sequences []schema.Sequence) schema.Table {
	// Build a map of sequences by their OwnedBy reference for quick lookup
	seqMap := make(map[string]schema.Sequence)
	for _, seq := range sequences {
		if seq.OwnedBy != nil {
			key := string(seq.OwnedBy.Schema) + "." + string(seq.OwnedBy.Table) + "." + string(seq.OwnedBy.Column)
			seqMap[key] = seq
		}
	}

	// Normalize columns
	for i := range tbl.Columns {
		tbl.Columns[i] = normalizeColumn(tbl.Schema, tbl.Name, tbl.Columns[i], seqMap)
	}

	return tbl
}

// normalizeColumn normalizes a single column
func normalizeColumn(tableSchema schema.SchemaName, tableName schema.TableName, col schema.Column, seqMap map[string]schema.Sequence) schema.Column {
	// Check if this column looks like an expanded SERIAL type
	// Do this BEFORE normalizing type names because we want to detect based on the raw catalog output
	if col.Default != nil {
		serialType := detectSerialType(col.Type, *col.Default, tableSchema, tableName, col.Name, seqMap)
		if serialType != "" {
			// Convert to SERIAL type and remove the default expression
			col.Type = serialType
			col.Default = nil
			// Return early - serial types don't need further normalization
			return col
		}
	}

	// Only normalize type names if NOT a serial type
	col.Type = normalizeTypeName(col.Type)

	return col
}

// detectSerialType detects if a column is an expanded SERIAL type and returns
// the appropriate SERIAL type name (serial, bigserial, smallserial) or empty string
func detectSerialType(typeName schema.TypeName, defaultExpr schema.Expr, tableSchema schema.SchemaName, tableName schema.TableName, colName schema.ColumnName, seqMap map[string]schema.Sequence) schema.TypeName {
	// Check if default expression is a nextval() call
	defaultStr := strings.TrimSpace(string(defaultExpr))
	if !strings.HasPrefix(defaultStr, "nextval(") {
		return ""
	}

	// Check if there's a sequence owned by this column
	seqKey := string(tableSchema) + "." + string(tableName) + "." + string(colName)
	seq, hasOwnedSeq := seqMap[seqKey]

	// If there's no owned sequence, it's not a SERIAL
	if !hasOwnedSeq {
		return ""
	}

	// Verify the sequence type matches the column type
	// SERIAL = integer + int4 sequence
	// BIGSERIAL = bigint + int8 sequence
	// SMALLSERIAL = smallint + int2 sequence

	normalizedType := strings.ToLower(strings.TrimSpace(string(typeName)))
	seqType := strings.ToLower(strings.TrimSpace(seq.Type))

	switch normalizedType {
	case "integer", "int", "int4":
		// SERIAL: column is integer, sequence type can be integer or bigint
		if seqType == "bigint" || seqType == "int8" || seqType == "integer" || seqType == "int" || seqType == "int4" {
			// Check if default references this sequence
			if referencesSequence(defaultExpr, seq.Schema, seq.Name) {
				return "serial"
			}
		}
	case "bigint", "int8":
		// BIGSERIAL: column is bigint, sequence type is bigint
		if seqType == "bigint" || seqType == "int8" {
			if referencesSequence(defaultExpr, seq.Schema, seq.Name) {
				return "bigserial"
			}
		}
	case "smallint", "int2":
		// SMALLSERIAL: column is smallint, sequence type can be smallint or bigint
		if seqType == "smallint" || seqType == "int2" || seqType == "bigint" || seqType == "int8" {
			if referencesSequence(defaultExpr, seq.Schema, seq.Name) {
				return "smallserial"
			}
		}
	}

	return ""
}

// referencesSequence checks if a default expression references a specific sequence
func referencesSequence(expr schema.Expr, seqSchema schema.SchemaName, seqName string) bool {
	exprStr := string(expr)

	// Pattern: nextval('schema.sequence_name'::regclass)
	// or: nextval('sequence_name'::regclass)
	// or: nextval('"schema"."sequence_name"'::regclass)

	// Build possible sequence references
	possibleRefs := []string{
		// Unqualified name
		"'" + seqName + "'",
		// Qualified name without quotes
		"'" + string(seqSchema) + "." + seqName + "'",
		// Qualified name with quotes
		"'\"" + string(seqSchema) + "\".\"" + seqName + "\"'",
		// Just check if the sequence name appears at all
		seqName,
	}

	for _, ref := range possibleRefs {
		if strings.Contains(exprStr, ref) {
			return true
		}
	}

	return false
}

// normalizeTypeName normalizes type names to their canonical form
func normalizeTypeName(typeName schema.TypeName) schema.TypeName {
	typeStr := strings.TrimSpace(string(typeName))

	// Handle common aliases
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
		// Extract length if present
		re := regexp.MustCompile(`character varying\((\d+)\)`)
		if matches := re.FindStringSubmatch(typeStr); len(matches) > 0 {
			return schema.TypeName("varchar(" + matches[1] + ")")
		}
		return "varchar"
	case "character":
		// Extract length if present
		re := regexp.MustCompile(`character\((\d+)\)`)
		if matches := re.FindStringSubmatch(typeStr); len(matches) > 0 {
			return schema.TypeName("char(" + matches[1] + ")")
		}
		return "char"
	}

	// Handle parameterized types by converting them to lowercase
	// but preserving the structure
	if strings.Contains(typeStr, "(") {
		// Extract base type and parameters
		parts := strings.SplitN(typeStr, "(", 2)
		if len(parts) == 2 {
			baseType := strings.ToLower(strings.TrimSpace(parts[0]))
			params := parts[1]

			// Apply normalization to base type
			switch baseType {
			case "character varying":
				return schema.TypeName("varchar(" + params)
			case "character":
				return schema.TypeName("char(" + params)
			}
		}
	}

	return typeName
}
