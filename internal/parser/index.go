package parser

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// parseCreateIndex parses a CREATE INDEX statement
func (p *Parser) parseCreateIndex(stmt *pg_query.IndexStmt) (schema.DatabaseObject, error) {
	if stmt.Relation == nil {
		return nil, fmt.Errorf("CREATE INDEX missing relation")
	}

	schemaName, tableName := p.extractQualifiedName(stmt.Relation)

	index := schema.Index{
		Schema:   schemaName,
		Table:    schema.TableName(tableName),
		Name:     stmt.Idxname,
		Unique:   stmt.Unique,
		Method:   schema.IndexMethod(stmt.AccessMethod),
		KeyExprs: []schema.IndexKeyExpr{},
		Include:  []schema.ColumnName{},
	}

	// Parse index elements (columns/expressions)
	for _, param := range stmt.IndexParams {
		if param == nil {
			continue
		}

		if indexElem, ok := param.Node.(*pg_query.Node_IndexElem); ok {
			keyExpr := p.parseIndexElement(indexElem.IndexElem)
			index.KeyExprs = append(index.KeyExprs, keyExpr)
		}
	}

	// Parse INCLUDE columns
	for _, incl := range stmt.IndexIncludingParams {
		if incl == nil {
			continue
		}
		if indexElem, ok := incl.Node.(*pg_query.Node_IndexElem); ok {
			if indexElem.IndexElem.Name != "" {
				index.Include = append(index.Include, schema.ColumnName(indexElem.IndexElem.Name))
			}
		}
	}

	// Parse WHERE clause (partial index)
	if stmt.WhereClause != nil {
		predicateStr := p.deparseExpr(stmt.WhereClause)
		predicate := schema.Expr(predicateStr)
		index.Predicate = &predicate
	}

	return index, nil
}

// parseIndexElement parses an index element (column or expression)
func (p *Parser) parseIndexElement(elem *pg_query.IndexElem) schema.IndexKeyExpr {
	keyExpr := schema.IndexKeyExpr{}

	// Column name or expression
	if elem.Name != "" {
		keyExpr.Expr = schema.Expr(elem.Name)
	} else if elem.Expr != nil {
		exprStr := p.deparseExpr(elem.Expr)
		keyExpr.Expr = schema.Expr(exprStr)
	}

	// Collation
	if elem.Collation != nil && len(elem.Collation) > 0 {
		collation := p.extractCollationName(elem.Collation)
		keyExpr.Collation = &collation
	}

	// Operator class
	if elem.Opclass != nil && len(elem.Opclass) > 0 {
		opclass := p.extractOpClassName(elem.Opclass)
		keyExpr.OpClass = &opclass
	}

	// Ordering (ASC/DESC)
	if elem.Ordering != pg_query.SortByDir_SORTBY_DEFAULT {
		if elem.Ordering == pg_query.SortByDir_SORTBY_ASC {
			ordering := schema.Asc
			keyExpr.Ordering = &ordering
		} else if elem.Ordering == pg_query.SortByDir_SORTBY_DESC {
			ordering := schema.Desc
			keyExpr.Ordering = &ordering
		}
	}

	// NULLS ordering
	if elem.NullsOrdering != pg_query.SortByNulls_SORTBY_NULLS_DEFAULT {
		if elem.NullsOrdering == pg_query.SortByNulls_SORTBY_NULLS_FIRST {
			nulls := schema.NullsFirst
			keyExpr.NullsOrdering = &nulls
		} else if elem.NullsOrdering == pg_query.SortByNulls_SORTBY_NULLS_LAST {
			nulls := schema.NullsLast
			keyExpr.NullsOrdering = &nulls
		}
	}

	return keyExpr
}

// extractCollationName extracts collation name from node list
func (p *Parser) extractCollationName(nodes []*pg_query.Node) string {
	if len(nodes) == 0 {
		return ""
	}
	// Get last part of qualified name
	if strNode, ok := nodes[len(nodes)-1].Node.(*pg_query.Node_String_); ok {
		return strNode.String_.Sval
	}
	return ""
}

// extractOpClassName extracts operator class name from node list
func (p *Parser) extractOpClassName(nodes []*pg_query.Node) string {
	if len(nodes) == 0 {
		return ""
	}
	// Get last part of qualified name
	if strNode, ok := nodes[len(nodes)-1].Node.(*pg_query.Node_String_); ok {
		return strNode.String_.Sval
	}
	return ""
}
