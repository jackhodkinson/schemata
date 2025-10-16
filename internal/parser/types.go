package parser

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// parseCreateEnum parses a CREATE TYPE ... AS ENUM statement
func (p *Parser) parseCreateEnum(stmt *pg_query.CreateEnumStmt) (schema.DatabaseObject, error) {
	if len(stmt.TypeName) == 0 {
		return nil, fmt.Errorf("CREATE ENUM missing type name")
	}

	// Extract schema and type name
	schemaName := schema.SchemaName("public")
	typeName := ""

	for i, node := range stmt.TypeName {
		if strNode, ok := node.Node.(*pg_query.Node_String_); ok {
			if i == len(stmt.TypeName)-1 {
				typeName = strNode.String_.Sval
			} else {
				schemaName = schema.SchemaName(strNode.String_.Sval)
			}
		}
	}

	// Extract enum values
	var values []string
	for _, val := range stmt.Vals {
		if strNode, ok := val.Node.(*pg_query.Node_String_); ok {
			values = append(values, strNode.String_.Sval)
		}
	}

	return schema.EnumDef{
		Schema: schemaName,
		Name:   schema.TypeName(typeName),
		Values: values,
	}, nil
}

// parseCreateDomain parses a CREATE DOMAIN statement
func (p *Parser) parseCreateDomain(stmt *pg_query.CreateDomainStmt) (schema.DatabaseObject, error) {
	if len(stmt.Domainname) == 0 {
		return nil, fmt.Errorf("CREATE DOMAIN missing domain name")
	}

	// Extract schema and domain name
	schemaName := schema.SchemaName("public")
	domainName := ""

	for i, node := range stmt.Domainname {
		if strNode, ok := node.Node.(*pg_query.Node_String_); ok {
			if i == len(stmt.Domainname)-1 {
				domainName = strNode.String_.Sval
			} else {
				schemaName = schema.SchemaName(strNode.String_.Sval)
			}
		}
	}

	domain := schema.DomainDef{
		Schema:  schemaName,
		Name:    schema.TypeName(domainName),
		NotNull: false,
	}

	// Parse base type
	if stmt.TypeName != nil {
		domain.BaseType = p.parseTypeName(stmt.TypeName)
	}

	// Parse constraints
	for _, constraint := range stmt.Constraints {
		if constraint == nil {
			continue
		}
		if c, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
			p.parseDomainConstraint(c.Constraint, &domain)
		}
	}

	return domain, nil
}

// parseDomainConstraint parses domain constraints
func (p *Parser) parseDomainConstraint(constraint *pg_query.Constraint, domain *schema.DomainDef) {
	if constraint == nil {
		return
	}

	switch constraint.Contype {
	case pg_query.ConstrType_CONSTR_NOTNULL:
		domain.NotNull = true

	case pg_query.ConstrType_CONSTR_DEFAULT:
		if constraint.RawExpr != nil {
			exprStr := p.deparseExpr(constraint.RawExpr)
			expr := schema.Expr(exprStr)
			domain.Default = &expr
		}

	case pg_query.ConstrType_CONSTR_CHECK:
		if constraint.RawExpr != nil {
			exprStr := p.deparseExpr(constraint.RawExpr)
			expr := schema.Expr(exprStr)
			domain.Check = &expr
		}
	}
}

// parseCreateComposite parses a CREATE TYPE ... AS composite statement
func (p *Parser) parseCreateComposite(stmt *pg_query.CompositeTypeStmt) (schema.DatabaseObject, error) {
	if stmt.Typevar == nil {
		return nil, fmt.Errorf("CREATE TYPE composite missing type name")
	}

	schemaName, typeName := p.extractQualifiedName(stmt.Typevar)

	composite := schema.CompositeDef{
		Schema:     schemaName,
		Name:       schema.TypeName(typeName),
		Attributes: []schema.CompositeAttr{},
	}

	// Parse attributes
	for _, col := range stmt.Coldeflist {
		if col == nil {
			continue
		}
		if colDef, ok := col.Node.(*pg_query.Node_ColumnDef); ok {
			attr := schema.CompositeAttr{
				Name: colDef.ColumnDef.Colname,
				Type: p.parseTypeName(colDef.ColumnDef.TypeName),
			}
			composite.Attributes = append(composite.Attributes, attr)
		}
	}

	return composite, nil
}
