package planner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// DDLGenerator generates DDL statements from diff operations
type DDLGenerator struct {
	allowCascade bool
}

type UnsupportedChangeError struct {
	Key         schema.ObjectKey
	Change      string
	Remediation string
}

func (e *UnsupportedChangeError) Error() string {
	key := fmt.Sprintf("%s/%s/%s", e.Key.Kind, e.Key.Schema, e.Key.Name)
	if e.Key.TableName != "" {
		key += fmt.Sprintf("/%s", e.Key.TableName)
	}
	if e.Key.Signature != "" {
		key += e.Key.Signature
	}
	if e.Remediation != "" {
		return fmt.Sprintf("unsupported change (%s): %s; remediation: %s", key, e.Change, e.Remediation)
	}
	return fmt.Sprintf("unsupported change (%s): %s", key, e.Change)
}

type DDLGeneratorOption func(*DDLGenerator)

func WithAllowCascade(allow bool) DDLGeneratorOption {
	return func(g *DDLGenerator) {
		g.allowCascade = allow
	}
}

// NewDDLGenerator creates a new DDL generator
func NewDDLGenerator(opts ...DDLGeneratorOption) *DDLGenerator {
	g := &DDLGenerator{}
	for _, opt := range opts {
		if opt != nil {
			opt(g)
		}
	}
	return g
}

// GenerateDDL generates DDL statements for a diff.
//
// Order: CREATE structural → ALTER → CREATE dependent → DROP
//
// Structural objects (types, tables, functions, sequences, extensions, views)
// are created first so that ALTERs can reference them.
// Dependent objects (indexes, triggers, policies) are created after ALTERs
// because they may reference columns added by ALTER TABLE.
// GenerateDDL generates DDL statements for a diff.
//
// objectMap is the desired schema (used for CREATE ordering).
// actualObjectMap, if provided, is the current database schema (used for DROP
// ordering). When omitted, objectMap is used for both.
func (g *DDLGenerator) GenerateDDL(diff *differ.Diff, objectMap schema.SchemaObjectMap, actualObjectMap ...schema.SchemaObjectMap) (string, error) {
	var statements []string

	// Use actual schema for drop ordering if provided, otherwise fall back to desired.
	dropMap := objectMap
	if len(actualObjectMap) > 0 && actualObjectMap[0] != nil {
		dropMap = actualObjectMap[0]
	}

	// Split creates into structural (before ALTER) and dependent (after ALTER).
	var structuralKeys, dependentKeys []schema.ObjectKey
	for _, key := range diff.ToCreate {
		switch key.Kind {
		case schema.IndexKind, schema.TriggerKind, schema.PolicyKind:
			dependentKeys = append(dependentKeys, key)
		default:
			structuralKeys = append(structuralKeys, key)
		}
	}

	// 1. Create structural objects (types, tables, functions, etc.)
	if len(structuralKeys) > 0 {
		stmts, err := g.generateCreateStatements(structuralKeys, objectMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate CREATE statements: %w", err)
		}
		statements = append(statements, stmts...)
	}

	// 2. ALTER existing objects (may add columns that new indexes reference)
	for _, alter := range diff.ToAlter {
		stmts, err := g.generateAlter(alter)
		if err != nil {
			return "", fmt.Errorf("failed to generate ALTER for %v: %w", alter.Key, err)
		}
		statements = append(statements, stmts...)
	}

	// 3. Create dependent objects (indexes, triggers, policies)
	if len(dependentKeys) > 0 {
		stmts, err := g.generateCreateStatements(dependentKeys, objectMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate dependent CREATE statements: %w", err)
		}
		statements = append(statements, stmts...)
	}

	// 4. DROP statements (in reverse dependency order, using actual schema)
	if len(diff.ToDrop) > 0 {
		dropStatements, err := g.generateDropStatements(diff.ToDrop, dropMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate DROP statements: %w", err)
		}
		statements = append(statements, dropStatements...)
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

	// Build set of keys that actually need CREATE (not just dependency-ordering nodes)
	createSet := make(map[schema.ObjectKey]bool, len(keys))
	for _, k := range keys {
		createSet[k] = true
	}

	// Generate CREATE statements in sorted order, skipping dependency-only nodes
	var statements []string
	for _, key := range sortedKeys {
		if !createSet[key] {
			continue // included only for ordering, already exists
		}
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
	var columnComments []string
	for _, col := range tbl.Columns {
		base := fmt.Sprintf("  %s %s", col.Name, col.Type)
		colParts := []string{base}

		if col.Collation != nil {
			colParts = append(colParts, fmt.Sprintf("COLLATE %s", formatQualifiedIdentifier(*col.Collation)))
		}

		if col.Identity != nil {
			colParts = append(colParts, formatIdentityClause(*col.Identity))
		} else if col.Generated != nil {
			colParts = append(colParts, formatGeneratedClause(*col.Generated))
		} else if col.Default != nil {
			colParts = append(colParts, fmt.Sprintf("DEFAULT %s", *col.Default))
		}

		if col.NotNull {
			colParts = append(colParts, "NOT NULL")
		}

		colDefs = append(colDefs, strings.Join(colParts, " "))

		if col.Comment != nil {
			columnComments = append(columnComments, formatColumnCommentStatement(tbl.Schema, tbl.Name, col.Name, col.Comment))
		}
	}

	// Primary key
	if tbl.PrimaryKey != nil {
		pkCols := make([]string, len(tbl.PrimaryKey.Cols))
		for i, col := range tbl.PrimaryKey.Cols {
			pkCols[i] = string(col)
		}
		pkClause := fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pkCols, ", "))
		if tbl.PrimaryKey.Name != nil && *tbl.PrimaryKey.Name != "" {
			pkClause = fmt.Sprintf("  CONSTRAINT %s PRIMARY KEY (%s)", *tbl.PrimaryKey.Name, strings.Join(pkCols, ", "))
		}
		pkClause += formatConstraintTiming(tbl.PrimaryKey.Deferrable, tbl.PrimaryKey.InitiallyDeferred)
		colDefs = append(colDefs, pkClause)
	}

	// Unique constraints
	for _, uq := range tbl.Uniques {
		uqCols := make([]string, len(uq.Cols))
		for i, col := range uq.Cols {
			uqCols[i] = string(col)
		}
		var uqClause string
		if uq.Name != "" {
			uqClause = fmt.Sprintf("  CONSTRAINT %s UNIQUE (%s)", uq.Name, strings.Join(uqCols, ", "))
		} else {
			uqClause = fmt.Sprintf("  UNIQUE (%s)", strings.Join(uqCols, ", "))
		}
		if !uq.NullsDistinct {
			uqClause += " NULLS NOT DISTINCT"
		}
		uqClause += formatConstraintTiming(uq.Deferrable, uq.InitiallyDeferred)
		uqClause += formatConstraintValidation(uq.NotValid)
		colDefs = append(colDefs, uqClause)
	}

	// Check constraints
	for _, check := range tbl.Checks {
		if check.Name != "" {
			checkClause := fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", check.Name, check.Expr)
			if check.NoInherit {
				checkClause += " NO INHERIT"
			}
			checkClause += formatConstraintTiming(check.Deferrable, check.InitiallyDeferred)
			checkClause += formatConstraintValidation(check.NotValid)
			colDefs = append(colDefs, checkClause)
		} else {
			checkClause := fmt.Sprintf("  CHECK (%s)", check.Expr)
			if check.NoInherit {
				checkClause += " NO INHERIT"
			}
			checkClause += formatConstraintTiming(check.Deferrable, check.InitiallyDeferred)
			checkClause += formatConstraintValidation(check.NotValid)
			colDefs = append(colDefs, checkClause)
		}
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
		switch fk.Match {
		case schema.MatchFull:
			fkDef += " MATCH FULL"
		case schema.MatchPartial:
			fkDef += " MATCH PARTIAL"
		}
		if fk.OnDelete != schema.NoAction {
			fkDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
		}
		if fk.OnUpdate != schema.NoAction {
			fkDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
		}
		fkDef += formatConstraintTiming(fk.Deferrable, fk.InitiallyDeferred)
		fkDef += formatConstraintValidation(fk.NotValid)
		colDefs = append(colDefs, fkDef)
	}

	parts = append(parts, strings.Join(colDefs, ",\n"))
	closeLine := ")"
	if len(tbl.RelOptions) > 0 {
		options := append([]string(nil), tbl.RelOptions...)
		sort.Strings(options)
		closeLine += fmt.Sprintf(" WITH (%s)", strings.Join(options, ", "))
	}
	closeLine += ";"
	parts = append(parts, closeLine)

	stmt := strings.Join(parts, "\n")

	var commentStatements []string
	if tbl.Comment != nil {
		commentStatements = append(commentStatements, formatTableCommentStatement(tbl.Schema, tbl.Name, tbl.Comment))
	}
	if len(columnComments) > 0 {
		commentStatements = append(commentStatements, columnComments...)
	}
	if len(commentStatements) > 0 {
		stmt += "\n\n" + strings.Join(commentStatements, "\n")
	}

	return stmt
}

func (g *DDLGenerator) generateCreateIndex(idx schema.Index) string {
	uniqueStr := ""
	if idx.Unique {
		uniqueStr = "UNIQUE "
	}

	// Build key expressions with ordering
	keyExprs := make([]string, len(idx.KeyExprs))
	for i, key := range idx.KeyExprs {
		var parts []string
		parts = append(parts, string(key.Expr))

		if key.Collation != nil {
			parts = append(parts, fmt.Sprintf("COLLATE %s", formatQualifiedIdentifier(*key.Collation)))
		}
		if key.OpClass != nil {
			parts = append(parts, *key.OpClass)
		}
		if key.Ordering != nil && *key.Ordering == schema.Desc {
			parts = append(parts, "DESC")
		}
		if key.NullsOrdering != nil {
			parts = append(parts, string(*key.NullsOrdering))
		}

		keyExprs[i] = strings.Join(parts, " ")
	}

	stmt := fmt.Sprintf("CREATE %sINDEX %s ON %s.%s USING %s (%s)",
		uniqueStr, idx.Name, idx.Schema, idx.Table, idx.Method, strings.Join(keyExprs, ", "))

	// Add WHERE clause for partial index
	if idx.Predicate != nil {
		predicate := strings.TrimSpace(string(*idx.Predicate))
		if shouldWrapPredicate(predicate) && !(strings.HasPrefix(predicate, "(") && strings.HasSuffix(predicate, ")")) {
			predicate = fmt.Sprintf("(%s)", predicate)
		}
		stmt += fmt.Sprintf(" WHERE %s", predicate)
	}

	if len(idx.Include) > 0 {
		includeCols := make([]string, len(idx.Include))
		for i, col := range idx.Include {
			includeCols[i] = string(col)
		}
		stmt += fmt.Sprintf(" INCLUDE (%s)", strings.Join(includeCols, ", "))
	}

	stmt += ";"

	if idx.Comment != nil {
		stmt += "\n\n" + formatIndexCommentStatement(idx.Schema, idx.Name, idx.Comment)
	}

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
		if len(ret.Columns) == 0 {
			returnsClause = "RETURNS TABLE ()"
		} else {
			cols := make([]string, len(ret.Columns))
			for i, col := range ret.Columns {
				cols[i] = fmt.Sprintf("%s %s", quoteIdentifier(col.Name), col.Type)
			}
			returnsClause = fmt.Sprintf("RETURNS TABLE (%s)", strings.Join(cols, ", "))
		}
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
		return g.generateAlterTable(obj, oldTable, alter)
	case schema.View:
		// Views: DROP and CREATE
		dropStmt, err := g.generateDrop(alter.Key)
		if err != nil {
			return nil, err
		}
		createStmt, err := g.generateCreate(alter.NewObject)
		if err != nil {
			return nil, err
		}
		return []string{dropStmt, createStmt}, nil
	case schema.Function:
		// Functions: CREATE OR REPLACE
		return []string{g.generateCreateFunction(obj)}, nil
	default:
		// For other objects, drop and recreate
		dropStmt, err := g.generateDrop(alter.Key)
		if err != nil {
			return nil, err
		}
		createStmt, err := g.generateCreate(alter.NewObject)
		if err != nil {
			return nil, err
		}
		return []string{dropStmt, createStmt}, nil
	}
}

func (g *DDLGenerator) generateAlterTable(tbl schema.Table, oldTable *schema.Table, alter differ.AlterOperation) ([]string, error) {
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
				base := fmt.Sprintf("%s %s", col.Name, col.Type)
				colParts := []string{base}

				if col.Collation != nil {
					colParts = append(colParts, fmt.Sprintf("COLLATE %s", formatQualifiedIdentifier(*col.Collation)))
				}

				if col.Identity != nil {
					colParts = append(colParts, formatIdentityClause(*col.Identity))
				} else if col.Generated != nil {
					colParts = append(colParts, formatGeneratedClause(*col.Generated))
				} else if col.Default != nil {
					colParts = append(colParts, fmt.Sprintf("DEFAULT %s", *col.Default))
				}

				if col.NotNull {
					colParts = append(colParts, "NOT NULL")
				}

				statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s;", tbl.Schema, tbl.Name, strings.Join(colParts, " ")))

				if col.Comment != nil {
					statements = append(statements, formatColumnCommentStatement(tbl.Schema, tbl.Name, col.Name, col.Comment))
				}
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
					colStatements, err := g.generateColumnAlter(tbl, oldTable, colName, changeDetail)
					if err != nil {
						return nil, err
					}
					statements = append(statements, colStatements...)
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
				pkStmt := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY (%s)",
					tbl.Schema, tbl.Name, pkName, strings.Join(pkCols, ", "))
				pkStmt += formatConstraintTiming(tbl.PrimaryKey.Deferrable, tbl.PrimaryKey.InitiallyDeferred)
				statements = append(statements, pkStmt+";")
			}
			} else if strings.HasPrefix(change, "drop primary key") {
				if oldPKName != "" {
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
						tbl.Schema, tbl.Name, oldPKName))
				} else {
					return nil, &UnsupportedChangeError{
						Key: schema.ObjectKey{
							Kind:      schema.TableKind,
							Schema:    tbl.Schema,
							Name:      string(tbl.Name),
							TableName: "",
						},
						Change:      change,
						Remediation: "The old primary key constraint name is not available. Manually drop the primary key using: ALTER TABLE " + string(tbl.Schema) + "." + string(tbl.Name) + " DROP CONSTRAINT <constraint_name>;",
					}
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
				pkStmt := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY (%s)",
					tbl.Schema, tbl.Name, pkName, strings.Join(pkCols, ", "))
				pkStmt += formatConstraintTiming(tbl.PrimaryKey.Deferrable, tbl.PrimaryKey.InitiallyDeferred)
				statements = append(statements, pkStmt+";")
			}
		} else if strings.HasPrefix(change, "add unique constraint ") {
			constraintName := strings.TrimPrefix(change, "add unique constraint ")
			for _, uq := range tbl.Uniques {
				if uq.Name == constraintName {
					statements = append(statements, buildAddUniqueConstraintStatement(tbl, uq)+";")
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
					statements = append(statements, buildAddCheckConstraintStatement(tbl, ck)+";")
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
					statements = append(statements, buildAddForeignKeyStatement(tbl, fk)+";")
					break
				}
			}
		} else if strings.HasPrefix(change, "drop foreign key ") {
			constraintName := strings.TrimPrefix(change, "drop foreign key ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;",
				tbl.Schema, tbl.Name, constraintName))
		} else if strings.HasPrefix(change, "unique constraint ") && strings.HasSuffix(change, " validation changed") {
			constraintName := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(change, "unique constraint "), " validation changed"))

			if constraintName != "" {
				if uq, ok := oldUniqueMap[constraintName]; ok && !uq.NotValid {
					// Old constraint existed; drop only if moving to NOT VALID
					if newUq := findUniqueConstraint(tbl.Uniques, constraintName); newUq != nil && newUq.NotValid {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
						statements = append(statements, buildAddUniqueConstraintStatement(tbl, *newUq)+";")
					} else {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
					}
				} else {
					if newUq := findUniqueConstraint(tbl.Uniques, constraintName); newUq != nil {
						if newUq.NotValid {
							statements = append(statements, buildAddUniqueConstraintStatement(tbl, *newUq)+";")
						} else {
							statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
						}
					} else {
						return nil, &UnsupportedChangeError{
							Key: schema.ObjectKey{
								Kind:   schema.TableKind,
								Schema: tbl.Schema,
								Name:   string(tbl.Name),
							},
							Change:      fmt.Sprintf("unique constraint %s validation changed (constraint details not available)", constraintName),
							Remediation: "manually validate or recreate the constraint in a migration",
						}
					}
				}
			}
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
						statements = append(statements, buildAddUniqueConstraintStatement(tbl, uq)+";")
						break
					}
				}
			}
		} else if strings.HasPrefix(change, "check constraint ") && strings.HasSuffix(change, " validation changed") {
			constraintName := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(change, "check constraint "), " validation changed"))

			if constraintName != "" {
				if ck, ok := oldCheckMap[constraintName]; ok && !ck.NotValid {
					if newCk := findCheckConstraint(tbl.Checks, constraintName); newCk != nil && newCk.NotValid {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
						statements = append(statements, buildAddCheckConstraintStatement(tbl, *newCk)+";")
					} else {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
					}
				} else {
					if newCk := findCheckConstraint(tbl.Checks, constraintName); newCk != nil {
						if newCk.NotValid {
							statements = append(statements, buildAddCheckConstraintStatement(tbl, *newCk)+";")
						} else {
							statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
						}
						} else {
							return nil, &UnsupportedChangeError{
								Key: schema.ObjectKey{
									Kind:      schema.TableKind,
									Schema:    tbl.Schema,
									Name:      string(tbl.Name),
									TableName: "",
								},
								Change:      fmt.Sprintf("check constraint %s validation changed", constraintName),
								Remediation: "The check constraint details are not available in the old schema. Manually validate the constraint using: ALTER TABLE " + string(tbl.Schema) + "." + string(tbl.Name) + " VALIDATE CONSTRAINT " + constraintName + ";",
							}
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
						statements = append(statements, buildAddCheckConstraintStatement(tbl, ck)+";")
						break
					}
				}
			}
		} else if strings.HasPrefix(change, "foreign key ") && strings.HasSuffix(change, " validation changed") {
			constraintName := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(change, "foreign key "), " validation changed"))

			if constraintName != "" {
				if fk, ok := oldFKMap[constraintName]; ok && !fk.NotValid {
					if newFk := findForeignKey(tbl.ForeignKeys, constraintName); newFk != nil && newFk.NotValid {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
						statements = append(statements, buildAddForeignKeyStatement(tbl, *newFk)+";")
					} else {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
					}
				} else {
					if newFk := findForeignKey(tbl.ForeignKeys, constraintName); newFk != nil {
						if newFk.NotValid {
							statements = append(statements, buildAddForeignKeyStatement(tbl, *newFk)+";")
						} else {
							statements = append(statements, fmt.Sprintf("ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;", tbl.Schema, tbl.Name, constraintName))
						}
						} else {
							return nil, &UnsupportedChangeError{
								Key: schema.ObjectKey{
									Kind:      schema.TableKind,
									Schema:    tbl.Schema,
									Name:      string(tbl.Name),
									TableName: "",
								},
								Change:      fmt.Sprintf("foreign key %s validation changed", constraintName),
								Remediation: "The foreign key constraint details are not available in the old schema. Manually validate the constraint using: ALTER TABLE " + string(tbl.Schema) + "." + string(tbl.Name) + " VALIDATE CONSTRAINT " + constraintName + ";",
							}
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
						statements = append(statements, buildAddForeignKeyStatement(tbl, fk)+";")
						break
					}
				}
			}
			} else {
				// Fallback for other changes
				return nil, &UnsupportedChangeError{
					Key: schema.ObjectKey{
						Kind:      schema.TableKind,
						Schema:    tbl.Schema,
						Name:      string(tbl.Name),
						TableName: "",
					},
					Change:      change,
					Remediation: "This table change type is not yet supported by schemata. Review the diff output and apply the change manually.",
				}
			}
		}

		return statements, nil
	}

func (g *DDLGenerator) generateColumnAlter(tbl schema.Table, oldTable *schema.Table, colName, changeDetail string) ([]string, error) {
	var statements []string
	tableName := fmt.Sprintf("%s.%s", tbl.Schema, tbl.Name)

	var newCol *schema.Column
	for i := range tbl.Columns {
		if tbl.Columns[i].Name == schema.ColumnName(colName) {
			newCol = &tbl.Columns[i]
			break
		}
	}

	var oldCol *schema.Column
	if oldTable != nil {
		for i := range oldTable.Columns {
			if oldTable.Columns[i].Name == schema.ColumnName(colName) {
				oldCol = &oldTable.Columns[i]
				break
			}
		}
	}

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
		if newCol != nil && newCol.Default != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
				tableName, colName, *newCol.Default))
		} else {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
				tableName, colName))
		}
	} else if changeDetail == "generated spec changed" {
		if oldCol != nil && oldCol.Generated != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP EXPRESSION;",
				tableName, colName))
		}
		if newCol != nil && newCol.Generated != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD %s;",
				tableName, colName, formatGeneratedClause(*newCol.Generated)))
		}
	} else if changeDetail == "identity spec changed" {
		if oldCol != nil && oldCol.Identity != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP IDENTITY IF EXISTS;",
				tableName, colName))
		}
		if newCol != nil && newCol.Identity != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD %s;",
				tableName, colName, formatIdentityClause(*newCol.Identity)))
		}
	} else if changeDetail == "collation changed" {
		if newCol != nil {
			if newCol.Collation != nil {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s COLLATE %s;",
					tableName, colName, newCol.Type, formatQualifiedIdentifier(*newCol.Collation)))
			} else {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
					tableName, colName, newCol.Type))
			}
		}
	} else if changeDetail == "comment changed" {
		var comment *string
		if newCol != nil {
			comment = newCol.Comment
		}
		statements = append(statements, formatColumnCommentStatement(tbl.Schema, tbl.Name, schema.ColumnName(colName), comment))
	} else {
		return nil, &UnsupportedChangeError{
			Key: schema.ObjectKey{
				Kind:      schema.ColumnKind,
				Schema:    tbl.Schema,
				Name:      string(colName),
				TableName: tbl.Name,
			},
			Change:      fmt.Sprintf("alter column %s: %s", colName, changeDetail),
			Remediation: "This column change type is not yet supported by schemata. Review the diff output and apply the change manually using ALTER TABLE " + tableName + " ALTER COLUMN " + colName + ";",
		}
	}

	return statements, nil
}

func findUniqueConstraint(constraints []schema.UniqueConstraint, name string) *schema.UniqueConstraint {
	for i := range constraints {
		if constraints[i].Name == name {
			return &constraints[i]
		}
	}
	return nil
}

func findCheckConstraint(constraints []schema.CheckConstraint, name string) *schema.CheckConstraint {
	for i := range constraints {
		if constraints[i].Name == name {
			return &constraints[i]
		}
	}
	return nil
}

func findForeignKey(constraints []schema.ForeignKey, name string) *schema.ForeignKey {
	for i := range constraints {
		if constraints[i].Name == name {
			return &constraints[i]
		}
	}
	return nil
}

func buildAddUniqueConstraintStatement(tbl schema.Table, uq schema.UniqueConstraint) string {
	uqCols := make([]string, len(uq.Cols))
	for i, col := range uq.Cols {
		uqCols[i] = string(col)
	}

	stmt := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE (%s)",
		tbl.Schema, tbl.Name, uq.Name, strings.Join(uqCols, ", "))
	if !uq.NullsDistinct {
		stmt += " NULLS NOT DISTINCT"
	}
	stmt += formatConstraintTiming(uq.Deferrable, uq.InitiallyDeferred)
	stmt += formatConstraintValidation(uq.NotValid)
	return stmt
}

func buildAddCheckConstraintStatement(tbl schema.Table, ck schema.CheckConstraint) string {
	var stmt string
	if ck.Name != "" {
		stmt = fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK (%s)",
			tbl.Schema, tbl.Name, ck.Name, ck.Expr)
	} else {
		stmt = fmt.Sprintf("ALTER TABLE %s.%s ADD CHECK (%s)",
			tbl.Schema, tbl.Name, ck.Expr)
	}
	if ck.NoInherit {
		stmt += " NO INHERIT"
	}
	stmt += formatConstraintTiming(ck.Deferrable, ck.InitiallyDeferred)
	stmt += formatConstraintValidation(ck.NotValid)
	return stmt
}

func buildAddForeignKeyStatement(tbl schema.Table, fk schema.ForeignKey) string {
	fkCols := make([]string, len(fk.Cols))
	for i, col := range fk.Cols {
		fkCols[i] = string(col)
	}
	refCols := make([]string, len(fk.Ref.Cols))
	for i, col := range fk.Ref.Cols {
		refCols[i] = string(col)
	}

	stmt := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
		tbl.Schema, tbl.Name, fk.Name, strings.Join(fkCols, ", "), fk.Ref.Schema, fk.Ref.Table, strings.Join(refCols, ", "))

	switch fk.Match {
	case schema.MatchFull:
		stmt += " MATCH FULL"
	case schema.MatchPartial:
		stmt += " MATCH PARTIAL"
	}
	if fk.OnDelete != schema.NoAction {
		stmt += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}
	if fk.OnUpdate != schema.NoAction {
		stmt += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}
	stmt += formatConstraintTiming(fk.Deferrable, fk.InitiallyDeferred)
	stmt += formatConstraintValidation(fk.NotValid)
	return stmt
}

func formatGeneratedClause(spec schema.GeneratedSpec) string {
	mode := "STORED"
	if !spec.Stored {
		mode = "VIRTUAL"
	}
	return fmt.Sprintf("GENERATED ALWAYS AS (%s) %s", spec.Expr, mode)
}

func formatIdentityClause(spec schema.IdentitySpec) string {
	mode := "BY DEFAULT"
	if spec.Always {
		mode = "ALWAYS"
	}

	clause := fmt.Sprintf("GENERATED %s AS IDENTITY", mode)

	if options := formatSequenceOptions(spec.SequenceOptions); options != "" {
		clause += fmt.Sprintf(" (%s)", options)
	}

	return clause
}

func formatSequenceOptions(options []schema.SequenceOption) string {
	if len(options) == 0 {
		return ""
	}

	ordered := append([]schema.SequenceOption(nil), options...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Type == ordered[j].Type {
			if ordered[i].HasValue == ordered[j].HasValue {
				return ordered[i].Value < ordered[j].Value
			}
			return !ordered[i].HasValue && ordered[j].HasValue
		}
		return sequenceOptionOrder(ordered[i].Type) < sequenceOptionOrder(ordered[j].Type)
	})

	parts := make([]string, 0, len(ordered))
	for _, opt := range ordered {
		if opt.HasValue {
			parts = append(parts, fmt.Sprintf("%s %d", opt.Type, opt.Value))
		} else {
			parts = append(parts, opt.Type)
		}
	}

	return strings.Join(parts, " ")
}

func sequenceOptionOrder(optionType string) int {
	switch optionType {
	case "START WITH":
		return 0
	case "INCREMENT BY":
		return 1
	case "MINVALUE", "NO MINVALUE":
		return 2
	case "MAXVALUE", "NO MAXVALUE":
		return 3
	case "CACHE":
		return 4
	case "CYCLE", "NO CYCLE":
		return 5
	default:
		return 100
	}
}

func shouldWrapPredicate(predicate string) bool {
	upper := strings.ToUpper(predicate)
	return strings.Contains(upper, " IS ") ||
		strings.Contains(upper, " AND ") ||
		strings.Contains(upper, " OR ") ||
		strings.Contains(upper, " BETWEEN ") ||
		strings.Contains(upper, " LIKE ") ||
		strings.Contains(upper, " IN (")
}

func formatConstraintTiming(deferrable, initiallyDeferred bool) string {
	if !deferrable {
		return ""
	}
	if initiallyDeferred {
		return " DEFERRABLE INITIALLY DEFERRED"
	}
	return " DEFERRABLE INITIALLY IMMEDIATE"
}

func formatConstraintValidation(notValid bool) string {
	if !notValid {
		return ""
	}
	return " NOT VALID"
}

func formatTableCommentStatement(schemaName schema.SchemaName, tableName schema.TableName, comment *string) string {
	qualified := fmt.Sprintf("%s.%s", schemaName, tableName)
	if comment == nil {
		return fmt.Sprintf("COMMENT ON TABLE %s IS NULL;", qualified)
	}
	return fmt.Sprintf("COMMENT ON TABLE %s IS %s;", qualified, quoteLiteral(*comment))
}

func formatColumnCommentStatement(schemaName schema.SchemaName, tableName schema.TableName, columnName schema.ColumnName, comment *string) string {
	qualified := fmt.Sprintf("%s.%s.%s", schemaName, tableName, columnName)
	if comment == nil {
		return fmt.Sprintf("COMMENT ON COLUMN %s IS NULL;", qualified)
	}
	return fmt.Sprintf("COMMENT ON COLUMN %s IS %s;", qualified, quoteLiteral(*comment))
}

func formatIndexCommentStatement(schemaName schema.SchemaName, indexName string, comment *string) string {
	qualified := fmt.Sprintf("%s.%s", schemaName, indexName)
	if comment == nil {
		return fmt.Sprintf("COMMENT ON INDEX %s IS NULL;", qualified)
	}
	return fmt.Sprintf("COMMENT ON INDEX %s IS %s;", qualified, quoteLiteral(*comment))
}

func quoteLiteral(value string) string {
	escaped := strings.ReplaceAll(value, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}

func formatQualifiedIdentifier(value string) string {
	if value == "" {
		return `""`
	}
	parts := strings.Split(value, ".")
	for i, part := range parts {
		if part == "" {
			parts[i] = `""`
			continue
		}
		parts[i] = quoteIdentifier(part)
	}
	return strings.Join(parts, ".")
}

func quoteIdentifier(ident string) string {
	escaped := strings.ReplaceAll(ident, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

func (g *DDLGenerator) generateDrop(key schema.ObjectKey) (string, error) {
	switch key.Kind {
	case schema.TableKind:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s.%s%s;", key.Schema, key.Name, g.cascadeClause()), nil
	case schema.IndexKind:
		return fmt.Sprintf("DROP INDEX IF EXISTS %s.%s;", key.Schema, key.Name), nil
	case schema.ViewKind:
		return fmt.Sprintf("DROP VIEW IF EXISTS %s.%s%s;", key.Schema, key.Name, g.cascadeClause()), nil
	case schema.FunctionKind:
		return fmt.Sprintf("DROP FUNCTION IF EXISTS %s.%s%s;", key.Schema, key.Name, g.cascadeClause()), nil
	case schema.SequenceKind:
		return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s.%s%s;", key.Schema, key.Name, g.cascadeClause()), nil
	case schema.TypeKind:
		return fmt.Sprintf("DROP TYPE IF EXISTS %s.%s%s;", key.Schema, key.Name, g.cascadeClause()), nil
	case schema.TriggerKind:
		return fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s.%s;", key.Name, key.Schema, key.TableName), nil
	case schema.PolicyKind:
		return fmt.Sprintf("DROP POLICY IF EXISTS %s ON %s.%s;", key.Name, key.Schema, key.TableName), nil
	default:
		return "", fmt.Errorf("unsupported object kind for DROP: %s", key.Kind)
	}
}

func (g *DDLGenerator) cascadeClause() string {
	if g.allowCascade {
		return " CASCADE"
	}
	return ""
}
