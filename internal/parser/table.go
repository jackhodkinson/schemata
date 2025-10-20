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
			col, isPK, isUnique, colFK, err := p.parseColumnDef(node.ColumnDef)
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

			// Handle inline UNIQUE constraint
			if isUnique {
				// Generate constraint name: table_column_key
				constraintName := fmt.Sprintf("%s_%s_key", table.Name, col.Name)
				table.Uniques = append(table.Uniques, schema.UniqueConstraint{
					Name:          constraintName,
					Cols:          []schema.ColumnName{col.Name},
					NullsDistinct: true, // Default
				})
			}

			// Handle inline REFERENCES constraint (column-level FK)
			if colFK != nil {
				// Generate constraint name if not provided
				if colFK.Name == "" {
					colFK.Name = fmt.Sprintf("%s_%s_fkey", table.Name, col.Name)
				}
				table.ForeignKeys = append(table.ForeignKeys, *colFK)
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
// Returns the column, a bool indicating if this column has an inline PRIMARY KEY,
// a bool indicating if this column has UNIQUE constraint,
// and an optional ForeignKey if the column has an inline REFERENCES clause
func (p *Parser) parseColumnDef(col *pg_query.ColumnDef) (schema.Column, bool, bool, *schema.ForeignKey, error) {
	if col == nil {
		return schema.Column{}, false, false, nil, fmt.Errorf("nil column definition")
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
	isUnique := false
	var columnFK *schema.ForeignKey

	for _, constraint := range col.Constraints {
		if constraint == nil {
			continue
		}

		if c, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
			isPK, isUQ, fk := p.parseColumnConstraint(c.Constraint, &column)
			if isPK {
				isPrimaryKey = true
			}
			if isUQ {
				isUnique = true
			}
			if fk != nil {
				columnFK = fk
			}
		}
	}

	return column, isPrimaryKey, isUnique, columnFK, nil
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
// Returns (isPrimaryKey, isUnique, foreignKey)
// - isPrimaryKey: true if this is a PRIMARY KEY constraint (needs table-level handling)
// - isUnique: true if this is a UNIQUE constraint (needs table-level handling)
// - foreignKey: non-nil if this is a REFERENCES constraint (needs to be added to table.ForeignKeys)
func (p *Parser) parseColumnConstraint(constraint *pg_query.Constraint, column *schema.Column) (bool, bool, *schema.ForeignKey) {
	if constraint == nil {
		return false, false, nil
	}

	switch constraint.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		// PRIMARY KEY on column - caller needs to handle this at table level
		column.NotNull = true // Primary keys are implicitly NOT NULL
		return true, false, nil

	case pg_query.ConstrType_CONSTR_UNIQUE:
		// UNIQUE constraint on column - caller needs to handle this at table level
		return false, true, nil

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

	case pg_query.ConstrType_CONSTR_FOREIGN:
		// Column-level REFERENCES constraint
		// The column name is implicit (it's the column being defined)
		refCols := p.extractColumnNames(constraint.PkAttrs)

		// Extract referenced table
		refSchema := schema.SchemaName("public")
		refTable := ""
		if constraint.Pktable != nil {
			refSchema, refTable = p.extractQualifiedName(constraint.Pktable)
		}

		// Generate a constraint name if not provided
		constraintName := constraint.Conname
		if constraintName == "" {
			// Auto-generate name: <table>_<column>_fkey
			// Note: We don't have table name here, so we'll let the caller set it
			constraintName = ""
		}

		fk := &schema.ForeignKey{
			Name: constraintName,
			Cols: []schema.ColumnName{column.Name}, // Column is implicit!
			Ref: schema.ForeignKeyRef{
				Schema: refSchema,
				Table:  schema.TableName(refTable),
				Cols:   refCols,
			}			,
			OnUpdate:          p.parseFkActionString(constraint.FkUpdAction),
			OnDelete:          p.parseFkActionString(constraint.FkDelAction),
			Match:             p.parseFkMatchTypeString(constraint.FkMatchtype),
			Deferrable:        constraint.Deferrable,
			InitiallyDeferred: constraint.Initdeferred,
		}

		return false, false, fk
	}

	return false, false, nil
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
		// Note: For FK constraints, pg_query uses FkAttrs for source columns, not Keys
		cols := p.extractColumnNames(constraint.FkAttrs)
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
