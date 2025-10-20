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

	// Generate DROP statements first (in reverse dependency order)
	if len(diff.ToDrop) > 0 {
		dropStatements, err := g.generateDropStatements(diff.ToDrop, objectMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate DROP statements: %w", err)
		}
		statements = append(statements, dropStatements...)
	}

	// Generate CREATE statements (in dependency order)
	if len(diff.ToCreate) > 0 {
		createStatements, err := g.generateCreateStatements(diff.ToCreate, objectMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate CREATE statements: %w", err)
		}
		statements = append(statements, createStatements...)
	}

	// Generate ALTER statements
	for _, alter := range diff.ToAlter {
		stmts, err := g.generateAlter(alter)
		if err != nil {
			return "", fmt.Errorf("failed to generate ALTER for %v: %w", alter.Key, err)
		}
		statements = append(statements, stmts...)
	}

	return strings.Join(statements, "\n\n"), nil
}

// generateCreateStatements generates CREATE statements in dependency order
func (g *DDLGenerator) generateCreateStatements(keys []schema.ObjectKey, objectMap schema.SchemaObjectMap) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Build a graph for only the objects being created
	graph := BuildGraph(objectMap)

	// Filter graph to only include objects being created
	filteredGraph := FilterGraphForKeys(graph, keys)

	// Topologically sort to respect dependencies
	sortedKeys, err := filteredGraph.TopologicalSort()
	if err != nil {
		// Circular dependency detected - provide helpful error
		cycle, _ := filteredGraph.DetectCycle()
		if cycle != nil {
			return nil, fmt.Errorf("circular dependency detected: %s\nCannot determine creation order. Consider creating tables first without foreign keys, then adding foreign keys with ALTER TABLE", formatCycle(cycle))
		}
		return nil, err
	}

	// Generate CREATE statements in sorted order
	var statements []string
	for _, key := range sortedKeys {
		if obj, exists := objectMap[key]; exists {
			stmt, err := g.generateCreate(obj.Payload)
			if err != nil {
				return nil, fmt.Errorf("failed to generate CREATE for %v: %w", key, err)
			}
			statements = append(statements, stmt)
		}
	}

	return statements, nil
}

// generateDropStatements generates DROP statements in reverse dependency order
func (g *DDLGenerator) generateDropStatements(keys []schema.ObjectKey, objectMap schema.SchemaObjectMap) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Build graph
	graph := BuildGraph(objectMap)

	// Filter to only objects being dropped
	filteredGraph := FilterGraphForKeys(graph, keys)

	// Reverse topological sort (drop dependents before dependencies)
	sortedKeys, err := filteredGraph.ReverseTopologicalSort()
	if err != nil {
		return nil, err
	}

	// Generate DROP statements in reverse dependency order
	var statements []string
	for _, key := range sortedKeys {
		stmt, err := g.generateDrop(key)
		if err != nil {
			return nil, fmt.Errorf("failed to generate DROP for %v: %w", key, err)
		}
		statements = append(statements, stmt)
	}

	return statements, nil
}

// formatCycle formats a dependency cycle for error messages
func formatCycle(cycle []schema.ObjectKey) string {
	if len(cycle) == 0 {
		return ""
	}

	parts := make([]string, len(cycle))
	for i, key := range cycle {
		parts[i] = fmt.Sprintf("%s.%s", key.Schema, key.Name)
	}

	return strings.Join(parts, " → ")
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

	// Build the function definition
	// Note: VOLATILITY and other options must come AFTER the function body
	stmt := fmt.Sprintf(`CREATE FUNCTION %s.%s(%s)
%s
LANGUAGE %s
AS $$
%s
$$`, fn.Schema, fn.Name, strings.Join(args, ", "), returnsClause, fn.Language, fn.Body)

	// Add function options after the body (if not defaults)
	// Default for plpgsql is VOLATILE, so we can omit it in most cases
	// But for completeness, we'll include it if specified
	if fn.Volatility != schema.Volatile {
		stmt += fmt.Sprintf("\nVOLATILITY %s", fn.Volatility)
	}

	stmt += ";"
	return stmt
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
		// Pass both old and new table objects for proper constraint handling
		var oldTable *schema.Table
		if oldObj, ok := alter.OldObject.(schema.Table); ok {
			oldTable = &oldObj
		}
		return g.generateAlterTable(obj, oldTable, alter), nil
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

func (g *DDLGenerator) generateAlterTable(tbl schema.Table, oldTable *schema.Table, alter differ.AlterOperation) []string {
	var statements []string

	// Build column and constraint maps for quick lookup (new table)
	colMap := make(map[schema.ColumnName]schema.Column)
	for _, col := range tbl.Columns {
		colMap[col.Name] = col
	}

	// Build old constraint maps for dropping
	var oldPKName string
	oldUniqueMap := make(map[string]schema.UniqueConstraint)
	oldCheckMap := make(map[string]schema.CheckConstraint)
	oldFKMap := make(map[string]schema.ForeignKey)

	if oldTable != nil {
		if oldTable.PrimaryKey != nil && oldTable.PrimaryKey.Name != nil {
			oldPKName = *oldTable.PrimaryKey.Name
		}
		for _, uq := range oldTable.Uniques {
			oldUniqueMap[uq.Name] = uq
		}
		for _, ck := range oldTable.Checks {
			oldCheckMap[ck.Name] = ck
		}
		for _, fk := range oldTable.ForeignKeys {
			oldFKMap[fk.Name] = fk
		}
	}

	// Process each change and generate appropriate DDL
	for _, change := range alter.Changes {
		if strings.HasPrefix(change, "add column ") {
			colName := schema.ColumnName(strings.TrimPrefix(change, "add column "))
			if col, exists := colMap[colName]; exists {
				colDef := fmt.Sprintf("%s %s", col.Name, col.Type)
				if col.NotNull {
					colDef += " NOT NULL"
				}
				if col.Default != nil {
					colDef += fmt.Sprintf(" DEFAULT %s", *col.Default)
				}
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s;", tbl.Schema, tbl.Name, colDef))
			}
		} else if strings.HasPrefix(change, "drop column ") {
			colName := strings.TrimPrefix(change, "drop column ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s;", tbl.Schema, tbl.Name, colName))
		} else if strings.HasPrefix(change, "alter column ") {
			// Parse "alter column <name>: <details>"
			parts := strings.SplitN(strings.TrimPrefix(change, "alter column "), ": ", 2)
			if len(parts) == 2 {
				colName := parts[0]
				changeDetail := parts[1]
				statements = append(statements, g.generateColumnAlter(tbl, colName, changeDetail)...)
			}
		} else if strings.HasPrefix(change, "add primary key") {
			if tbl.PrimaryKey != nil {
				pkCols := make([]string, len(tbl.PrimaryKey.Cols))
				for i, col := range tbl.PrimaryKey.Cols {
					pkCols[i] = string(col)
				}
				pkName := "pkey"
				if tbl.PrimaryKey.Name != nil {
					pkName = *tbl.PrimaryKey.Name
				}
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY (%s);",
					tbl.Schema, tbl.Name, pkName, strings.Join(pkCols, ", ")))
			}
		} else if strings.HasPrefix(change, "drop primary key") {
			if oldPKName != "" {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
					tbl.Schema, tbl.Name, oldPKName))
			} else {
				statements = append(statements, fmt.Sprintf("-- TODO: %s (old constraint name not available)", change))
			}
		} else if strings.HasPrefix(change, "primary key columns changed") || strings.Contains(change, "primary key") && strings.Contains(change, "changed") {
			// Drop old primary key and add new one
			if oldPKName != "" {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
					tbl.Schema, tbl.Name, oldPKName))
			}
			if tbl.PrimaryKey != nil {
				pkCols := make([]string, len(tbl.PrimaryKey.Cols))
				for i, col := range tbl.PrimaryKey.Cols {
					pkCols[i] = string(col)
				}
				pkName := "pkey"
				if tbl.PrimaryKey.Name != nil {
					pkName = *tbl.PrimaryKey.Name
				}
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY (%s);",
					tbl.Schema, tbl.Name, pkName, strings.Join(pkCols, ", ")))
			}
		} else if strings.HasPrefix(change, "add unique constraint ") {
			constraintName := strings.TrimPrefix(change, "add unique constraint ")
			for _, uq := range tbl.Uniques {
				if uq.Name == constraintName {
					uqCols := make([]string, len(uq.Cols))
					for i, col := range uq.Cols {
						uqCols[i] = string(col)
					}
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE (%s);",
						tbl.Schema, tbl.Name, uq.Name, strings.Join(uqCols, ", ")))
					break
				}
			}
		} else if strings.HasPrefix(change, "drop unique constraint ") {
			constraintName := strings.TrimPrefix(change, "drop unique constraint ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
				tbl.Schema, tbl.Name, constraintName))
		} else if strings.HasPrefix(change, "add check constraint ") {
			constraintName := strings.TrimPrefix(change, "add check constraint ")
			for _, ck := range tbl.Checks {
				if ck.Name == constraintName {
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK (%s);",
						tbl.Schema, tbl.Name, ck.Name, ck.Expr))
					break
				}
			}
		} else if strings.HasPrefix(change, "drop check constraint ") {
			constraintName := strings.TrimPrefix(change, "drop check constraint ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
				tbl.Schema, tbl.Name, constraintName))
		} else if strings.HasPrefix(change, "add foreign key ") {
			constraintName := strings.TrimPrefix(change, "add foreign key ")
			for _, fk := range tbl.ForeignKeys {
				if fk.Name == constraintName {
					fkCols := make([]string, len(fk.Cols))
					for i, col := range fk.Cols {
						fkCols[i] = string(col)
					}
					refCols := make([]string, len(fk.Ref.Cols))
					for i, col := range fk.Ref.Cols {
						refCols[i] = string(col)
					}
					fkDef := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
						tbl.Schema, tbl.Name, fk.Name, strings.Join(fkCols, ", "), fk.Ref.Schema, fk.Ref.Table, strings.Join(refCols, ", "))
					if fk.OnDelete != schema.NoAction {
						fkDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
					}
					if fk.OnUpdate != schema.NoAction {
						fkDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
					}
					statements = append(statements, fkDef+";")
					break
				}
			}
		} else if strings.HasPrefix(change, "drop foreign key ") {
			constraintName := strings.TrimPrefix(change, "drop foreign key ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
				tbl.Schema, tbl.Name, constraintName))
		} else if strings.Contains(change, "unique constraint") && strings.Contains(change, "changed") {
			// Modified unique constraint: drop and recreate
			// Parse "unique constraint <name> <property> changed"
			parts := strings.Fields(change)
			if len(parts) >= 3 {
				constraintName := parts[2]
				// Drop old
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
					tbl.Schema, tbl.Name, constraintName))
				// Add new
				for _, uq := range tbl.Uniques {
					if uq.Name == constraintName {
						uqCols := make([]string, len(uq.Cols))
						for i, col := range uq.Cols {
							uqCols[i] = string(col)
						}
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE (%s);",
							tbl.Schema, tbl.Name, uq.Name, strings.Join(uqCols, ", ")))
						break
					}
				}
			}
		} else if strings.Contains(change, "check constraint") && strings.Contains(change, "changed") {
			// Modified check constraint: drop and recreate
			parts := strings.Fields(change)
			if len(parts) >= 3 {
				constraintName := parts[2]
				// Drop old
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
					tbl.Schema, tbl.Name, constraintName))
				// Add new
				for _, ck := range tbl.Checks {
					if ck.Name == constraintName {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK (%s);",
							tbl.Schema, tbl.Name, ck.Name, ck.Expr))
						break
					}
				}
			}
		} else if strings.Contains(change, "foreign key") && strings.Contains(change, "changed") {
			// Modified foreign key: drop and recreate
			parts := strings.Fields(change)
			if len(parts) >= 3 {
				constraintName := parts[2]
				// Drop old
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
					tbl.Schema, tbl.Name, constraintName))
				// Add new
				for _, fk := range tbl.ForeignKeys {
					if fk.Name == constraintName {
						fkCols := make([]string, len(fk.Cols))
						for i, col := range fk.Cols {
							fkCols[i] = string(col)
						}
						refCols := make([]string, len(fk.Ref.Cols))
						for i, col := range fk.Ref.Cols {
							refCols[i] = string(col)
						}
						fkDef := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
							tbl.Schema, tbl.Name, fk.Name, strings.Join(fkCols, ", "), fk.Ref.Schema, fk.Ref.Table, strings.Join(refCols, ", "))
						if fk.OnDelete != schema.NoAction {
							fkDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
						}
						if fk.OnUpdate != schema.NoAction {
							fkDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
						}
						statements = append(statements, fkDef+";")
						break
					}
				}
			}
		} else {
			// Fallback for other changes
			statements = append(statements, fmt.Sprintf("-- TODO: %s", change))
		}
	}

	return statements
}

func (g *DDLGenerator) generateColumnAlter(tbl schema.Table, colName, changeDetail string) []string {
	var statements []string
	tableName := fmt.Sprintf("%s.%s", tbl.Schema, tbl.Name)

	if strings.Contains(changeDetail, "type changed") {
		// Parse "type changed from <old> to <new>"
		if strings.Contains(changeDetail, " to ") {
			parts := strings.Split(changeDetail, " to ")
			if len(parts) == 2 {
				newType := strings.TrimSpace(parts[1])
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
					tableName, colName, newType))
			}
		}
	} else if changeDetail == "set not null" {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;",
			tableName, colName))
	} else if changeDetail == "drop not null" {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
			tableName, colName))
	} else if changeDetail == "default changed" {
		// Find the column to get the new default value
		for _, col := range tbl.Columns {
			if col.Name == schema.ColumnName(colName) {
				if col.Default != nil {
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
						tableName, colName, *col.Default))
				} else {
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
						tableName, colName))
				}
				break
			}
		}
	} else {
		// For other column changes, generate a TODO
		statements = append(statements, fmt.Sprintf("-- TODO: ALTER TABLE %s ALTER COLUMN %s: %s",
			tableName, colName, changeDetail))
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
