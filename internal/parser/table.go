package parser

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// parseCreateTable parses a CREATE TABLE statement
func (p *Parser) parseCreateTable(stmt *pg_query.CreateStmt) (schema.DatabaseObject, error) {
	if stmt.Relation == nil {
		return nil, fmt.Errorf("CREATE TABLE missing relation")
	}

	schemaName, tableName := p.extractQualifiedName(stmt.Relation)

	table := schema.Table{
		Schema:      schemaName,
		Name:        schema.TableName(tableName),
		Columns:     []schema.Column{},
		Uniques:     []schema.UniqueConstraint{},
		Checks:      []schema.CheckConstraint{},
		ForeignKeys: []schema.ForeignKey{},
	}

	// Parse table elements (columns and constraints)
	for _, elt := range stmt.TableElts {
		if elt == nil {
			continue
		}

		switch node := elt.Node.(type) {
		case *pg_query.Node_ColumnDef:
			col, isPK, err := p.parseColumnDef(node.ColumnDef)
			if err != nil {
				return nil, fmt.Errorf("failed to parse column: %w", err)
			}
			table.Columns = append(table.Columns, col)

			// Handle inline PRIMARY KEY constraint
			if isPK {
				pkName := "" // Inline PK constraints don't have names
				table.PrimaryKey = &schema.PrimaryKey{
					Name: &pkName,
					Cols: []schema.ColumnName{col.Name},
				}
			}

		case *pg_query.Node_Constraint:
			err := p.parseTableConstraint(node.Constraint, &table)
			if err != nil {
				return nil, fmt.Errorf("failed to parse constraint: %w", err)
			}
		}
	}

	return table, nil
}

// parseColumnDef parses a column definition
// Returns the column and a bool indicating if this column has an inline PRIMARY KEY
func (p *Parser) parseColumnDef(col *pg_query.ColumnDef) (schema.Column, bool, error) {
	if col == nil {
		return schema.Column{}, false, fmt.Errorf("nil column definition")
	}

	column := schema.Column{
		Name:    schema.ColumnName(col.Colname),
		NotNull: false,
	}

	// Parse column type
	if col.TypeName != nil {
		column.Type = p.parseTypeName(col.TypeName)
	}

	// Parse column constraints
	isPrimaryKey := false
	for _, constraint := range col.Constraints {
		if constraint == nil {
			continue
		}

		if c, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
			if p.parseColumnConstraint(c.Constraint, &column) {
				isPrimaryKey = true
			}
		}
	}

	return column, isPrimaryKey, nil
}

// parseTypeName converts a TypeName node to our TypeName
func (p *Parser) parseTypeName(typeName *pg_query.TypeName) schema.TypeName {
	if typeName == nil {
		return ""
	}

	// Build type name from names list
	var parts []string
	for _, name := range typeName.Names {
		if n, ok := name.Node.(*pg_query.Node_String_); ok {
			parts = append(parts, n.String_.Sval)
		}
	}

	typeStr := ""
	if len(parts) > 0 {
		// Last part is the type name, earlier parts are schema
		typeStr = parts[len(parts)-1]
	}

	// Handle type modifiers (e.g., varchar(255), numeric(10,2))
	if len(typeName.Typmods) > 0 {
		typeStr += "(" + p.formatTypeModifiers(typeName.Typmods) + ")"
	}

	// Handle array types
	if len(typeName.ArrayBounds) > 0 {
		typeStr += "[]"
	}

	return schema.TypeName(typeStr)
}

// formatTypeModifiers formats type modifiers for display
func (p *Parser) formatTypeModifiers(mods []*pg_query.Node) string {
	if len(mods) == 0 {
		return ""
	}

	// Use deparse to get the proper representation
	result := ""
	for i, mod := range mods {
		if i > 0 {
			result += ", "
		}
		result += p.deparseExpr(mod)
	}
	return result
}

// parseColumnConstraint parses column-level constraints
// Returns true if this is a PRIMARY KEY constraint (needs table-level handling)
func (p *Parser) parseColumnConstraint(constraint *pg_query.Constraint, column *schema.Column) bool {
	if constraint == nil {
		return false
	}

	switch constraint.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		// PRIMARY KEY on column - caller needs to handle this at table level
		column.NotNull = true // Primary keys are implicitly NOT NULL
		return true

	case pg_query.ConstrType_CONSTR_NOTNULL:
		column.NotNull = true

	case pg_query.ConstrType_CONSTR_DEFAULT:
		if constraint.RawExpr != nil {
			exprStr := p.deparseExpr(constraint.RawExpr)
			expr := schema.Expr(exprStr)
			column.Default = &expr
		}

	case pg_query.ConstrType_CONSTR_IDENTITY:
		// IDENTITY column
		// Note: pg_query_go doesn't expose the ALWAYS/BY DEFAULT distinction easily
		// Defaulting to ALWAYS for now
		column.Identity = &schema.IdentitySpec{
			Always: true,
		}

	case pg_query.ConstrType_CONSTR_GENERATED:
		// GENERATED column
		if constraint.RawExpr != nil {
			exprStr := p.deparseExpr(constraint.RawExpr)
			column.Generated = &schema.GeneratedSpec{
				Expr:   schema.Expr(exprStr),
				Stored: true, // Default to STORED
			}
		}
	}

	return false
}

// parseTableConstraint parses table-level constraints
func (p *Parser) parseTableConstraint(constraint *pg_query.Constraint, table *schema.Table) error {
	if constraint == nil {
		return nil
	}

	constraintName := constraint.Conname

	switch constraint.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		// Primary key
		cols := p.extractColumnNames(constraint.Keys)
		table.PrimaryKey = &schema.PrimaryKey{
			Name:              &constraintName,
			Cols:              cols,
			Deferrable:        constraint.Deferrable,
			InitiallyDeferred: constraint.Initdeferred,
		}

	case pg_query.ConstrType_CONSTR_UNIQUE:
		// Unique constraint
		cols := p.extractColumnNames(constraint.Keys)
		table.Uniques = append(table.Uniques, schema.UniqueConstraint{
			Name:              constraintName,
			Cols:              cols,
			NullsDistinct:     !constraint.NullsNotDistinct, // Note: inverted logic
			Deferrable:        constraint.Deferrable,
			InitiallyDeferred: constraint.Initdeferred,
		})

	case pg_query.ConstrType_CONSTR_CHECK:
		// Check constraint
		if constraint.RawExpr != nil {
			exprStr := p.deparseExpr(constraint.RawExpr)
			table.Checks = append(table.Checks, schema.CheckConstraint{
				Name:              constraintName,
				Expr:              schema.Expr(exprStr),
				NoInherit:         constraint.IsNoInherit,
				Deferrable:        constraint.Deferrable,
				InitiallyDeferred: constraint.Initdeferred,
			})
		}

	case pg_query.ConstrType_CONSTR_FOREIGN:
		// Foreign key
		cols := p.extractColumnNames(constraint.Keys)
		refCols := p.extractColumnNames(constraint.PkAttrs)

		// Extract referenced table
		refSchema := schema.SchemaName("public")
		refTable := ""
		if constraint.Pktable != nil {
			refSchema, refTable = p.extractQualifiedName(constraint.Pktable)
		}

		table.ForeignKeys = append(table.ForeignKeys, schema.ForeignKey{
			Name: constraintName,
			Cols: cols,
			Ref: schema.ForeignKeyRef{
				Schema: refSchema,
				Table:  schema.TableName(refTable),
				Cols:   refCols,
			},
			OnUpdate:          p.parseFkActionString(constraint.FkUpdAction),
			OnDelete:          p.parseFkActionString(constraint.FkDelAction),
			Match:             p.parseFkMatchTypeString(constraint.FkMatchtype),
			Deferrable:        constraint.Deferrable,
			InitiallyDeferred: constraint.Initdeferred,
		})
	}

	return nil
}

// extractColumnNames extracts column names from a list of nodes
func (p *Parser) extractColumnNames(keys []*pg_query.Node) []schema.ColumnName {
	var cols []schema.ColumnName
	for _, key := range keys {
		if key == nil {
			continue
		}
		if strNode, ok := key.Node.(*pg_query.Node_String_); ok {
			cols = append(cols, schema.ColumnName(strNode.String_.Sval))
		}
	}
	return cols
}

// parseFkActionString converts pg_query foreign key action string to our type
func (p *Parser) parseFkActionString(action string) schema.ReferentialAction {
	switch action {
	case "a", "NO ACTION":
		return schema.NoAction
	case "r", "RESTRICT":
		return schema.Restrict
	case "c", "CASCADE":
		return schema.Cascade
	case "n", "SET NULL":
		return schema.SetNull
	case "d", "SET DEFAULT":
		return schema.SetDefault
	default:
		return schema.NoAction
	}
}

// parseFkMatchTypeString converts pg_query match type string to our type
func (p *Parser) parseFkMatchTypeString(matchType string) schema.MatchType {
	switch matchType {
	case "f", "FULL":
		return schema.MatchFull
	case "p", "PARTIAL":
		return schema.MatchPartial
	case "s", "SIMPLE", "":
		return schema.MatchSimple
	default:
		return schema.MatchSimple
	}
}
