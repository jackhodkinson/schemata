package parser

import (
	"fmt"

	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
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
		var err error
		domain.BaseType, err = p.parseTypeName(stmt.TypeName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse domain %s base type: %w", domainName, err)
		}
	}

	// Parse constraints
	for _, constraint := range stmt.Constraints {
		if constraint == nil {
			continue
		}
		if c, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
			if err := p.parseDomainConstraint(c.Constraint, &domain); err != nil {
				return nil, fmt.Errorf("failed to parse domain %s constraint: %w", domainName, err)
			}
		}
	}

	return domain, nil
}

// parseDomainConstraint parses domain constraints
func (p *Parser) parseDomainConstraint(constraint *pg_query.Constraint, domain *schema.DomainDef) error {
	if constraint == nil {
		return nil
	}

	switch constraint.Contype {
	case pg_query.ConstrType_CONSTR_NOTNULL:
		domain.NotNull = true

	case pg_query.ConstrType_CONSTR_DEFAULT:
		if constraint.RawExpr != nil {
			exprStr, err := p.deparseExpr(constraint.RawExpr)
			if err != nil {
				return fmt.Errorf("failed to deparse DEFAULT expression: %w", err)
			}
			expr := schema.Expr(exprStr)
			domain.Default = &expr
		}

	case pg_query.ConstrType_CONSTR_CHECK:
		if constraint.RawExpr != nil {
			exprStr, err := p.deparseExpr(constraint.RawExpr)
			if err != nil {
				return fmt.Errorf("failed to deparse CHECK expression: %w", err)
			}
			expr := schema.Expr(exprStr)
			domain.Check = &expr
		}
	}
	return nil
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

	seen := make(map[string]struct{}, len(stmt.Coldeflist))
	for _, col := range stmt.Coldeflist {
		if col == nil {
			return nil, fmt.Errorf("composite %s contains an empty attribute", typeName)
		}
		colDef := col.GetColumnDef()
		if colDef == nil || colDef.Colname == "" || colDef.TypeName == nil {
			return nil, fmt.Errorf("composite %s contains an unsupported attribute definition", typeName)
		}
		if colDef.CollClause != nil {
			return nil, fmt.Errorf("composite %s attribute %s uses COLLATE, which the declarative composite model cannot preserve", typeName, colDef.Colname)
		}
		if colDef.Compression != "" || colDef.Storage != "" || colDef.StorageName != "" || colDef.RawDefault != nil || colDef.CookedDefault != nil || colDef.Identity != "" || colDef.IdentitySequence != nil || colDef.Generated != "" || colDef.IsNotNull || colDef.IsFromType || len(colDef.Constraints) > 0 || len(colDef.Fdwoptions) > 0 {
			return nil, fmt.Errorf("composite %s attribute %s contains unsupported column metadata", typeName, colDef.Colname)
		}
		if _, duplicate := seen[colDef.Colname]; duplicate {
			return nil, fmt.Errorf("composite %s declares duplicate attribute %s", typeName, colDef.Colname)
		}
		seen[colDef.Colname] = struct{}{}
		attrType, err := p.parseTypeName(colDef.TypeName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse composite %s attribute %s type: %w", typeName, colDef.Colname, err)
		}
		composite.Attributes = append(composite.Attributes, schema.CompositeAttr{Name: colDef.Colname, Type: attrType})
	}

	return composite, nil
}
