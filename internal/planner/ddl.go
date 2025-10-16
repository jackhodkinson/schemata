package planner

import (
	"fmt"
	"strings"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// DDLGenerator generates DDL statements from diff operations
type DDLGenerator struct{}

// NewDDLGenerator creates a new DDL generator
func NewDDLGenerator() *DDLGenerator {
	return &DDLGenerator{}
}

// GenerateDDL generates DDL statements for a diff
func (g *DDLGenerator) GenerateDDL(diff *differ.Diff, objectMap schema.SchemaObjectMap) (string, error) {
	var statements []string

	// Generate CREATE statements
	for _, key := range diff.ToCreate {
		if obj, exists := objectMap[key]; exists {
			stmt, err := g.generateCreate(obj.Payload)
			if err != nil {
				return "", fmt.Errorf("failed to generate CREATE for %v: %w", key, err)
			}
			statements = append(statements, stmt)
		}
	}

	// Generate ALTER statements
	for _, alter := range diff.ToAlter {
		stmts, err := g.generateAlter(alter)
		if err != nil {
			return "", fmt.Errorf("failed to generate ALTER for %v: %w", alter.Key, err)
		}
		statements = append(statements, stmts...)
	}

	// Generate DROP statements
	for _, key := range diff.ToDrop {
		stmt, err := g.generateDrop(key)
		if err != nil {
			return "", fmt.Errorf("failed to generate DROP for %v: %w", key, err)
		}
		statements = append(statements, stmt)
	}

	return strings.Join(statements, "\n\n"), nil
}

// GenerateCreateStatement generates a CREATE statement for an object
func (g *DDLGenerator) GenerateCreateStatement(obj schema.DatabaseObject) (string, error) {
	return g.generateCreate(obj)
}

func (g *DDLGenerator) generateCreate(obj schema.DatabaseObject) (string, error) {
	switch v := obj.(type) {
	case schema.Table:
		return g.generateCreateTable(v), nil
	case schema.Index:
		return g.generateCreateIndex(v), nil
	case schema.View:
		return g.generateCreateView(v), nil
	case schema.Function:
		return g.generateCreateFunction(v), nil
	case schema.Sequence:
		return g.generateCreateSequence(v), nil
	case schema.EnumDef:
		return g.generateCreateEnum(v), nil
	case schema.DomainDef:
		return g.generateCreateDomain(v), nil
	case schema.Extension:
		return g.generateCreateExtension(v), nil
	case schema.Trigger:
		return g.generateCreateTrigger(v), nil
	case schema.Policy:
		return g.generateCreatePolicy(v), nil
	default:
		return "", fmt.Errorf("unsupported object type for CREATE: %T", obj)
	}
}

func (g *DDLGenerator) generateCreateTable(tbl schema.Table) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("CREATE TABLE %s.%s (", tbl.Schema, tbl.Name))

	// Columns
	var colDefs []string
	for _, col := range tbl.Columns {
		colDef := fmt.Sprintf("  %s %s", col.Name, col.Type)
		if col.NotNull {
			colDef += " NOT NULL"
		}
		if col.Default != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.Default)
		}
		colDefs = append(colDefs, colDef)
	}

	// Primary key
	if tbl.PrimaryKey != nil {
		pkCols := make([]string, len(tbl.PrimaryKey.Cols))
		for i, col := range tbl.PrimaryKey.Cols {
			pkCols[i] = string(col)
		}
		colDefs = append(colDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	// Unique constraints
	for _, uq := range tbl.Uniques {
		uqCols := make([]string, len(uq.Cols))
		for i, col := range uq.Cols {
			uqCols[i] = string(col)
		}
		colDefs = append(colDefs, fmt.Sprintf("  CONSTRAINT %s UNIQUE (%s)", uq.Name, strings.Join(uqCols, ", ")))
	}

	// Check constraints
	for _, check := range tbl.Checks {
		colDefs = append(colDefs, fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", check.Name, check.Expr))
	}

	// Foreign keys
	for _, fk := range tbl.ForeignKeys {
		fkCols := make([]string, len(fk.Cols))
		for i, col := range fk.Cols {
			fkCols[i] = string(col)
		}
		refCols := make([]string, len(fk.Ref.Cols))
		for i, col := range fk.Ref.Cols {
			refCols[i] = string(col)
		}
		fkDef := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
			fk.Name, strings.Join(fkCols, ", "), fk.Ref.Schema, fk.Ref.Table, strings.Join(refCols, ", "))
		if fk.OnDelete != schema.NoAction {
			fkDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
		}
		if fk.OnUpdate != schema.NoAction {
			fkDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
		}
		colDefs = append(colDefs, fkDef)
	}

	parts = append(parts, strings.Join(colDefs, ",\n"))
	parts = append(parts, ");")

	return strings.Join(parts, "\n")
}

func (g *DDLGenerator) generateCreateIndex(idx schema.Index) string {
	uniqueStr := ""
	if idx.Unique {
		uniqueStr = "UNIQUE "
	}

	// Build key expressions
	keyExprs := make([]string, len(idx.KeyExprs))
	for i, key := range idx.KeyExprs {
		keyExprs[i] = string(key.Expr)
	}

	stmt := fmt.Sprintf("CREATE %sINDEX %s ON %s.%s USING %s (%s)",
		uniqueStr, idx.Name, idx.Schema, idx.Table, idx.Method, strings.Join(keyExprs, ", "))

	// Add WHERE clause for partial index
	if idx.Predicate != nil {
		stmt += fmt.Sprintf(" WHERE %s", *idx.Predicate)
	}

	stmt += ";"

	return stmt
}

func (g *DDLGenerator) generateCreateView(view schema.View) string {
	viewType := ""
	if view.Type == schema.MaterializedView {
		viewType = "MATERIALIZED "
	}

	return fmt.Sprintf("CREATE %sVIEW %s.%s AS\n%s;",
		viewType, view.Schema, view.Name, view.Definition.Query)
}

func (g *DDLGenerator) generateCreateFunction(fn schema.Function) string {
	// Build arguments
	args := make([]string, len(fn.Args))
	for i, arg := range fn.Args {
		argStr := string(arg.Type)
		if arg.Name != nil {
			argStr = fmt.Sprintf("%s %s", *arg.Name, arg.Type)
		}
		args[i] = argStr
	}

	// Build returns clause
	returnsClause := ""
	switch ret := fn.Returns.(type) {
	case schema.ReturnsType:
		returnsClause = fmt.Sprintf("RETURNS %s", ret.Type)
	case schema.ReturnsTable:
		// TODO: Format table return type
		returnsClause = "RETURNS TABLE"
	case schema.ReturnsSetOf:
		returnsClause = fmt.Sprintf("RETURNS SETOF %s", ret.Type)
	}

	return fmt.Sprintf(`CREATE FUNCTION %s.%s(%s)
%s
LANGUAGE %s
VOLATILITY %s
AS $$
%s
$$;`, fn.Schema, fn.Name, strings.Join(args, ", "), returnsClause, fn.Language, fn.Volatility, fn.Body)
}

func (g *DDLGenerator) generateCreateSequence(seq schema.Sequence) string {
	stmt := fmt.Sprintf("CREATE SEQUENCE %s.%s", seq.Schema, seq.Name)

	if seq.Start != nil {
		stmt += fmt.Sprintf(" START %d", *seq.Start)
	}
	if seq.Increment != nil {
		stmt += fmt.Sprintf(" INCREMENT %d", *seq.Increment)
	}
	if seq.MinValue != nil {
		stmt += fmt.Sprintf(" MINVALUE %d", *seq.MinValue)
	}
	if seq.MaxValue != nil {
		stmt += fmt.Sprintf(" MAXVALUE %d", *seq.MaxValue)
	}
	if seq.Cache != nil {
		stmt += fmt.Sprintf(" CACHE %d", *seq.Cache)
	}
	if seq.Cycle {
		stmt += " CYCLE"
	}

	stmt += ";"
	return stmt
}

func (g *DDLGenerator) generateCreateEnum(enum schema.EnumDef) string {
	values := make([]string, len(enum.Values))
	for i, v := range enum.Values {
		values[i] = fmt.Sprintf("'%s'", v)
	}

	return fmt.Sprintf("CREATE TYPE %s.%s AS ENUM (%s);",
		enum.Schema, enum.Name, strings.Join(values, ", "))
}

func (g *DDLGenerator) generateCreateDomain(domain schema.DomainDef) string {
	stmt := fmt.Sprintf("CREATE DOMAIN %s.%s AS %s", domain.Schema, domain.Name, domain.BaseType)

	if domain.Default != nil {
		stmt += fmt.Sprintf(" DEFAULT %s", *domain.Default)
	}
	if domain.NotNull {
		stmt += " NOT NULL"
	}
	if domain.Check != nil {
		stmt += fmt.Sprintf(" CHECK (%s)", *domain.Check)
	}

	stmt += ";"
	return stmt
}

func (g *DDLGenerator) generateCreateExtension(ext schema.Extension) string {
	return fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", ext.Name)
}

func (g *DDLGenerator) generateCreateTrigger(trig schema.Trigger) string {
	events := make([]string, len(trig.Events))
	for i, event := range trig.Events {
		events[i] = string(event)
	}

	rowClause := ""
	if trig.ForEachRow {
		rowClause = "FOR EACH ROW "
	}

	return fmt.Sprintf(`CREATE TRIGGER %s
%s %s ON %s.%s
%sEXECUTE FUNCTION %s.%s();`,
		trig.Name, trig.Timing, strings.Join(events, " OR "), trig.Schema, trig.Table,
		rowClause, trig.Function.Schema, trig.Function.Name)
}

func (g *DDLGenerator) generateCreatePolicy(pol schema.Policy) string {
	permissive := "PERMISSIVE"
	if !pol.Permissive {
		permissive = "RESTRICTIVE"
	}

	stmt := fmt.Sprintf("CREATE POLICY %s ON %s.%s AS %s FOR %s",
		pol.Name, pol.Schema, pol.Table, permissive, pol.For)

	if len(pol.To) > 0 {
		stmt += fmt.Sprintf(" TO %s", strings.Join(pol.To, ", "))
	}

	if pol.Using != nil {
		stmt += fmt.Sprintf(" USING (%s)", *pol.Using)
	}

	if pol.WithCheck != nil {
		stmt += fmt.Sprintf(" WITH CHECK (%s)", *pol.WithCheck)
	}

	stmt += ";"
	return stmt
}

func (g *DDLGenerator) generateAlter(alter differ.AlterOperation) ([]string, error) {
	// For now, implement basic ALTER TABLE operations
	// More sophisticated ALTERs will be added later

	switch obj := alter.NewObject.(type) {
	case schema.Table:
		return g.generateAlterTable(obj, alter), nil
	case schema.View:
		// Views: DROP and CREATE
		dropStmt, _ := g.generateDrop(alter.Key)
		createStmt, _ := g.generateCreate(alter.NewObject)
		return []string{dropStmt, createStmt}, nil
	case schema.Function:
		// Functions: CREATE OR REPLACE
		return []string{g.generateCreateFunction(obj)}, nil
	default:
		// For other objects, drop and recreate
		dropStmt, _ := g.generateDrop(alter.Key)
		createStmt, _ := g.generateCreate(alter.NewObject)
		return []string{dropStmt, createStmt}, nil
	}
}

func (g *DDLGenerator) generateAlterTable(tbl schema.Table, alter differ.AlterOperation) []string {
	var statements []string

	// For now, generate simple ALTER statements based on change descriptions
	for _, change := range alter.Changes {
		// This is simplified - would need more sophisticated parsing of changes
		if strings.Contains(change, "add column") {
			// Extract column name (simplified)
			statements = append(statements, fmt.Sprintf("-- TODO: %s", change))
		} else if strings.Contains(change, "drop column") {
			statements = append(statements, fmt.Sprintf("-- TODO: %s", change))
		} else {
			statements = append(statements, fmt.Sprintf("-- TODO: %s", change))
		}
	}

	return statements
}

func (g *DDLGenerator) generateDrop(key schema.ObjectKey) (string, error) {
	switch key.Kind {
	case schema.TableKind:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE;", key.Schema, key.Name), nil
	case schema.IndexKind:
		return fmt.Sprintf("DROP INDEX IF EXISTS %s.%s;", key.Schema, key.Name), nil
	case schema.ViewKind:
		return fmt.Sprintf("DROP VIEW IF EXISTS %s.%s CASCADE;", key.Schema, key.Name), nil
	case schema.FunctionKind:
		return fmt.Sprintf("DROP FUNCTION IF EXISTS %s.%s CASCADE;", key.Schema, key.Name), nil
	case schema.SequenceKind:
		return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s.%s CASCADE;", key.Schema, key.Name), nil
	case schema.TypeKind:
		return fmt.Sprintf("DROP TYPE IF EXISTS %s.%s CASCADE;", key.Schema, key.Name), nil
	case schema.TriggerKind:
		return fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s.%s;", key.Name, key.Schema, key.TableName), nil
	case schema.PolicyKind:
		return fmt.Sprintf("DROP POLICY IF EXISTS %s ON %s.%s;", key.Name, key.Schema, key.TableName), nil
	default:
		return "", fmt.Errorf("unsupported object kind for DROP: %s", key.Kind)
	}
}
