package parser

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// Parser parses SQL files using pg_query_go and extracts schema objects
type Parser struct{}

type UnsupportedStatementError struct {
	StatementType string
	Remediation   string
}

type DuplicateObjectError struct {
	Key        schema.ObjectKey
	FirstPath  string
	SecondPath string
}

func (e *DuplicateObjectError) Error() string {
	return fmt.Sprintf("duplicate schema object %s found in both '%s' and '%s'", formatObjectKey(e.Key), e.FirstPath, e.SecondPath)
}

func (e *UnsupportedStatementError) Error() string {
	if e.Remediation != "" {
		return fmt.Sprintf("unsupported statement type %s; remediation: %s", e.StatementType, e.Remediation)
	}
	return fmt.Sprintf("unsupported statement type %s", e.StatementType)
}

// NewParser creates a new SQL parser
func NewParser() *Parser {
	return &Parser{}
}

// ParseFile parses a SQL file and returns a SchemaObjectMap
func (p *Parser) ParseFile(filePath string) (schema.SchemaObjectMap, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return p.parseSQLWithSource(string(content), filePath)
}

// ParsePath parses a schema path that may be either a SQL file or directory.
// Directory mode recursively scans for .sql files in deterministic path order.
func (p *Parser) ParsePath(path string) (schema.SchemaObjectMap, error) {
	if path == "" {
		return nil, fmt.Errorf("no schema path configured")
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect schema path '%s': %w", path, err)
	}

	if !info.IsDir() {
		return p.ParseFile(path)
	}

	files, err := collectSchemaFiles(path)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return p.buildObjectMap([]schema.DatabaseObject{})
	}

	var objects []schema.DatabaseObject
	objectSources := make(map[schema.ObjectKey]string)

	for _, filePath := range files {
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read schema file '%s': %w", filePath, err)
		}
		fileObjects, err := p.parseSQLObjects(string(content))
		if err != nil {
			return nil, fmt.Errorf("failed to parse schema file '%s': %w", filePath, err)
		}

		for _, obj := range fileObjects {
			key := objectmap.Key(obj)
			if prior, exists := objectSources[key]; exists {
				return nil, &DuplicateObjectError{
					Key:        key,
					FirstPath:  prior,
					SecondPath: filePath,
				}
			}
			objectSources[key] = filePath
			objects = append(objects, obj)
		}
	}

	return p.buildObjectMap(objects)
}

// ParseSQL parses SQL text and returns a SchemaObjectMap
func (p *Parser) ParseSQL(sql string) (schema.SchemaObjectMap, error) {
	return p.parseSQLWithSource(sql, "")
}

func (p *Parser) parseSQLWithSource(sql string, sourcePath string) (schema.SchemaObjectMap, error) {
	objects, err := p.parseSQLObjects(sql)
	if err != nil {
		if sourcePath != "" {
			return nil, fmt.Errorf("failed to parse schema file '%s': %w", sourcePath, err)
		}
		return nil, err
	}

	return p.buildObjectMap(objects)
}

func (p *Parser) parseSQLObjects(sql string) ([]schema.DatabaseObject, error) {
	// Parse using pg_query_go
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("pg_query_go parse error: %w", err)
	}

	// Extract objects from parsed statements
	objects := []schema.DatabaseObject{}
	var comments []commentInstruction

	for _, rawStmt := range result.Stmts {
		if rawStmt.Stmt == nil {
			continue
		}

		if commentStmt := rawStmt.Stmt.GetCommentStmt(); commentStmt != nil {
			if instr := p.parseCommentInstruction(commentStmt); instr != nil {
				comments = append(comments, *instr)
			}
			continue
		}

		// Extract object based on statement type
		obj, err := p.extractObject(rawStmt.Stmt)
		if err != nil {
			snippet := extractStatementSnippet(sql, int(rawStmt.StmtLocation), int(rawStmt.StmtLen))
			if snippet != "" {
				return nil, fmt.Errorf("failed to parse statement: %w\n\nStatement snippet:\n%s", err, snippet)
			}
			return nil, fmt.Errorf("failed to parse statement: %w", err)
		}
		if obj != nil {
			objects = append(objects, obj)
		}
	}

	if len(comments) > 0 {
		objects = p.applyCommentInstructions(objects, comments)
	}

	objects = p.mergeGrantsAndOwners(objects, result)

	return objects, nil
}

func extractStatementSnippet(sql string, location, length int) string {
	if location < 0 || location >= len(sql) {
		return ""
	}

	end := len(sql)
	if length > 0 && location+length <= len(sql) {
		end = location + length
	}

	snippet := strings.TrimSpace(sql[location:end])
	if snippet == "" {
		return ""
	}

	const maxLen = 800
	if len(snippet) > maxLen {
		return snippet[:maxLen] + "…"
	}
	return snippet
}

// extractObject extracts a schema object from a pg_query statement node
func (p *Parser) extractObject(stmt *pg_query.Node) (schema.DatabaseObject, error) {
	// Handle different statement types
	switch node := stmt.Node.(type) {
	case *pg_query.Node_CreateStmt:
		return p.parseCreateTable(node.CreateStmt)
	case *pg_query.Node_IndexStmt:
		return p.parseCreateIndex(node.IndexStmt)
	case *pg_query.Node_ViewStmt:
		return p.parseCreateView(node.ViewStmt)
	case *pg_query.Node_CreateSeqStmt:
		return p.parseCreateSequence(node.CreateSeqStmt)
	case *pg_query.Node_CreateEnumStmt:
		return p.parseCreateEnum(node.CreateEnumStmt)
	case *pg_query.Node_CreateDomainStmt:
		return p.parseCreateDomain(node.CreateDomainStmt)
	case *pg_query.Node_CompositeTypeStmt:
		return p.parseCreateComposite(node.CompositeTypeStmt)
	case *pg_query.Node_CreateFunctionStmt:
		return p.parseCreateFunction(node.CreateFunctionStmt)
	case *pg_query.Node_CreateTrigStmt:
		return p.parseCreateTrigger(node.CreateTrigStmt)
	case *pg_query.Node_CreatePolicyStmt:
		return p.parseCreatePolicy(node.CreatePolicyStmt)
	case *pg_query.Node_CreateExtensionStmt:
		return p.parseCreateExtension(node.CreateExtensionStmt)
	case *pg_query.Node_CreateSchemaStmt:
		return p.parseCreateSchema(node.CreateSchemaStmt)
	case *pg_query.Node_GrantStmt,
		*pg_query.Node_AlterOwnerStmt:
		// Handled in mergeGrantsAndOwners after object extraction.
		return nil, nil
	case *pg_query.Node_VariableSetStmt,
		*pg_query.Node_VariableShowStmt,
		*pg_query.Node_TransactionStmt,
		*pg_query.Node_SelectStmt:
		// These statements are commonly present in dumps/fixtures and do not define schema objects directly.
		return nil, nil
	default:
		// Fail closed: unknown statements may affect schema correctness (e.g., ALTER, DO, DROP).
		return nil, &UnsupportedStatementError{
			StatementType: fmt.Sprintf("%T", node),
			Remediation:   "schemata only supports a subset of DDL; remove or rewrite this statement, or apply it separately",
		}
	}
}

// buildObjectMap converts a list of objects into a SchemaObjectMap with hashing
func (p *Parser) buildObjectMap(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	return objectmap.Build(objects)
}

// Helper to extract qualified name from RangeVar
func (p *Parser) extractQualifiedName(rangeVar *pg_query.RangeVar) (schema.SchemaName, string) {
	schemaName := schema.SchemaName("public") // Default schema
	if rangeVar.Schemaname != "" {
		schemaName = schema.SchemaName(rangeVar.Schemaname)
	}
	return schemaName, rangeVar.Relname
}

// Helper to deparse an expression node back to SQL
// This is critical for constraints, defaults, and index expressions
func (p *Parser) deparseExpr(node *pg_query.Node) string {
	if node == nil {
		return ""
	}

	// For simple cases, use pg_query_go's deparsing
	// Wrap in a SELECT to make it a valid statement for deparsing
	result, err := pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: &pg_query.Node{
			Node: &pg_query.Node_SelectStmt{
				SelectStmt: &pg_query.SelectStmt{
					TargetList: []*pg_query.Node{{
						Node: &pg_query.Node_ResTarget{
							ResTarget: &pg_query.ResTarget{
								Val: node,
							},
						},
					}},
				},
			},
		}}},
	})
	if err != nil {
		// If deparsing fails, return a placeholder
		// This is better than crashing
		return "(expression)"
	}

	// Extract just the expression from "SELECT <expr>"
	result = strings.TrimSpace(result)
	result = strings.TrimPrefix(result, "SELECT ")
	result = strings.TrimSuffix(result, ";")

	return result
}

type commentInstruction struct {
	schema  schema.SchemaName
	table   schema.TableName
	column  *schema.ColumnName
	comment *string
}

func (p *Parser) parseCommentInstruction(stmt *pg_query.CommentStmt) *commentInstruction {
	if stmt == nil {
		return nil
	}

	var commentPtr *string
	if stmt.Comment != "" {
		comment := stmt.Comment
		commentPtr = &comment
	}

	switch stmt.Objtype {
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_MATVIEW:
		names := extractStringList(stmt.Object)
		if len(names) == 0 {
			return nil
		}
		schemaName := schema.SchemaName("public")
		tableName := names[len(names)-1]
		if len(names) > 1 {
			schemaName = schema.SchemaName(names[len(names)-2])
		}
		return &commentInstruction{
			schema:  schemaName,
			table:   schema.TableName(tableName),
			comment: commentPtr,
		}
	case pg_query.ObjectType_OBJECT_COLUMN:
		names := extractStringList(stmt.Object)
		if len(names) < 2 {
			return nil
		}
		schemaName := schema.SchemaName("public")
		tableName := names[len(names)-2]
		columnName := schema.ColumnName(names[len(names)-1])
		if len(names) > 2 {
			schemaName = schema.SchemaName(names[len(names)-3])
		}
		return &commentInstruction{
			schema:  schemaName,
			table:   schema.TableName(tableName),
			column:  &columnName,
			comment: commentPtr,
		}
	default:
		return nil
	}
}

func (p *Parser) applyCommentInstructions(objects []schema.DatabaseObject, comments []commentInstruction) []schema.DatabaseObject {
	if len(comments) == 0 {
		return objects
	}

	type tableKey struct {
		schema schema.SchemaName
		name   schema.TableName
	}

	index := make(map[tableKey]int)
	for i, obj := range objects {
		if tbl, ok := obj.(schema.Table); ok {
			index[tableKey{schema: tbl.Schema, name: tbl.Name}] = i
		}
	}

	for _, instr := range comments {
		key := tableKey{schema: instr.schema, name: instr.table}
		idx, ok := index[key]
		if !ok {
			continue
		}
		tbl, ok := objects[idx].(schema.Table)
		if !ok {
			continue
		}

		if instr.column == nil {
			tbl.Comment = instr.comment
		} else {
			for i := range tbl.Columns {
				if tbl.Columns[i].Name == *instr.column {
					tbl.Columns[i].Comment = instr.comment
					break
				}
			}
		}

		objects[idx] = tbl
	}

	return objects
}

func extractStringList(node *pg_query.Node) []string {
	if node == nil {
		return nil
	}
	listNode, ok := node.Node.(*pg_query.Node_List)
	if !ok || listNode.List == nil {
		return nil
	}

	var values []string
	for _, item := range listNode.List.Items {
		if strNode, ok := item.Node.(*pg_query.Node_String_); ok {
			values = append(values, strNode.String_.Sval)
		}
	}
	return values
}

func collectSchemaFiles(root string) ([]string, error) {
	var relPaths []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		name := d.Name()
		if d.IsDir() {
			if path != root && strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if shouldIgnoreSchemaFile(name) {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(name), ".sql") {
			return nil
		}

		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("failed to compute schema file path: %w", err)
		}
		relPaths = append(relPaths, filepath.ToSlash(relPath))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan schema directory '%s': %w", root, err)
	}

	sort.Strings(relPaths)
	files := make([]string, 0, len(relPaths))
	for _, rel := range relPaths {
		files = append(files, filepath.Join(root, filepath.FromSlash(rel)))
	}
	return files, nil
}

func shouldIgnoreSchemaFile(name string) bool {
	if strings.HasPrefix(name, ".") {
		return true
	}
	if strings.HasSuffix(name, "~") || strings.HasSuffix(name, ".swp") || strings.HasSuffix(name, ".tmp") {
		return true
	}
	if strings.HasPrefix(name, "#") {
		return true
	}
	return strings.HasPrefix(name, ".#")
}

func formatObjectKey(key schema.ObjectKey) string {
	var b strings.Builder
	if key.Kind != "" {
		b.WriteString(string(key.Kind))
	} else {
		b.WriteString("unknown")
	}
	b.WriteString(":")
	if key.Schema != "" {
		b.WriteString(string(key.Schema))
		b.WriteString(".")
	}
	if key.Name != "" {
		b.WriteString(key.Name)
	}
	if key.TableName != "" {
		b.WriteString(" table=")
		b.WriteString(string(key.TableName))
	}
	if key.ColumnName != "" {
		b.WriteString(" column=")
		b.WriteString(string(key.ColumnName))
	}
	if key.Signature != "" {
		b.WriteString(" signature=")
		b.WriteString(string(key.Signature))
	}
	return b.String()
}

func IsDuplicateObjectError(err error) bool {
	var dupErr *DuplicateObjectError
	return errors.As(err, &dupErr)
}
