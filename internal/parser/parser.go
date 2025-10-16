package parser

import (
	"fmt"
	"os"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Parser parses SQL files using pg_query_go and extracts schema objects
type Parser struct{}

// NewParser creates a new SQL parser
func NewParser() *Parser {
	return &Parser{}
}

// ParseFile parses a SQL file and returns a SchemaObjectMap
func (p *Parser) ParseFile(filePath string) (schema.SchemaObjectMap, error) {
	// Read file
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return p.ParseSQL(string(content))
}

// ParseSQL parses SQL text and returns a SchemaObjectMap
func (p *Parser) ParseSQL(sql string) (schema.SchemaObjectMap, error) {
	// Parse using pg_query_go
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("pg_query_go parse error: %w", err)
	}

	// Extract objects from parsed statements
	objects := []schema.DatabaseObject{}

	for _, rawStmt := range result.Stmts {
		if rawStmt.Stmt == nil {
			continue
		}

		// Extract object based on statement type
		obj, err := p.extractObject(rawStmt.Stmt)
		if err != nil {
			// Log but continue - some statements might not be schema objects
			continue
		}
		if obj != nil {
			objects = append(objects, obj)
		}
	}

	// Build object map with hashing
	return p.buildObjectMap(objects)
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
	case *pg_query.Node_GrantStmt:
		// GRANT statements - we'll handle these by attaching grants to objects
		// For now, skip as standalone objects (grants are properties of tables/views/functions)
		return nil, nil
	case *pg_query.Node_AlterOwnerStmt:
		// ALTER ... OWNER TO statements
		// For now, skip as standalone objects (owner is a property of objects)
		return nil, nil
	default:
		// Not a schema object we track (e.g., INSERT, UPDATE, etc.)
		return nil, nil
	}
}

// buildObjectMap converts a list of objects into a SchemaObjectMap with hashing
func (p *Parser) buildObjectMap(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	objectMap := make(schema.SchemaObjectMap)

	for _, obj := range objects {
		// Generate object key
		key := p.getObjectKey(obj)

		// Compute hash
		hash, err := differ.NormalizeAndHash(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to hash object %v: %w", key, err)
		}

		// Store in map
		objectMap[key] = schema.HashedObject{
			Hash:    hash,
			Payload: obj,
		}
	}

	return objectMap, nil
}

// getObjectKey generates an ObjectKey for a database object
func (p *Parser) getObjectKey(obj schema.DatabaseObject) schema.ObjectKey {
	switch v := obj.(type) {
	case schema.Table:
		return schema.ObjectKey{
			Kind:   schema.TableKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.Index:
		return schema.ObjectKey{
			Kind:      schema.IndexKind,
			Schema:    v.Schema,
			Name:      v.Name,
			TableName: v.Table,
		}
	case schema.View:
		return schema.ObjectKey{
			Kind:   schema.ViewKind,
			Schema: v.Schema,
			Name:   v.Name,
		}
	case schema.Function:
		return schema.ObjectKey{
			Kind:      schema.FunctionKind,
			Schema:    v.Schema,
			Name:      v.Name,
			Signature: p.getFunctionSignature(v),
		}
	case schema.Sequence:
		return schema.ObjectKey{
			Kind:   schema.SequenceKind,
			Schema: v.Schema,
			Name:   v.Name,
		}
	case schema.EnumDef:
		return schema.ObjectKey{
			Kind:   schema.TypeKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.DomainDef:
		return schema.ObjectKey{
			Kind:   schema.TypeKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.CompositeDef:
		return schema.ObjectKey{
			Kind:   schema.TypeKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.Trigger:
		return schema.ObjectKey{
			Kind:      schema.TriggerKind,
			Schema:    v.Schema,
			Name:      v.Name,
			TableName: v.Table,
		}
	case schema.Policy:
		return schema.ObjectKey{
			Kind:      schema.PolicyKind,
			Schema:    v.Schema,
			Name:      v.Name,
			TableName: v.Table,
		}
	case schema.Extension:
		return schema.ObjectKey{
			Kind:   schema.ExtensionKind,
			Schema: v.Schema,
			Name:   v.Name,
		}
	case schema.Schema:
		return schema.ObjectKey{
			Kind:   schema.SchemaKind,
			Schema: v.Name,
			Name:   string(v.Name),
		}
	default:
		return schema.ObjectKey{}
	}
}

// getFunctionSignature generates a signature string for function overloading
func (p *Parser) getFunctionSignature(fn schema.Function) string {
	argTypes := make([]string, len(fn.Args))
	for i, arg := range fn.Args {
		argTypes[i] = string(arg.Type)
	}
	return strings.Join(argTypes, ",")
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
