package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Catalog provides methods to query PostgreSQL catalog tables
type Catalog struct {
	pool *Pool
}

// NewCatalog creates a new catalog querier
func NewCatalog(pool *Pool) *Catalog {
	return &Catalog{pool: pool}
}

// ExtractAllObjects queries the database and extracts all schema objects
func (c *Catalog) ExtractAllObjects(ctx context.Context, includeSchemas, excludeSchemas []string) ([]schema.DatabaseObject, error) {
	var objects []schema.DatabaseObject

	// Build schema filter clause
	schemaFilter := c.buildSchemaFilter(includeSchemas, excludeSchemas)

	// Extract extensions
	extensions, err := c.extractExtensions(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract extensions: %w", err)
	}
	objects = append(objects, extensions...)

	// Extract enums
	enums, err := c.extractEnums(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract enums: %w", err)
	}
	objects = append(objects, enums...)

	// Extract domains
	domains, err := c.extractDomains(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract domains: %w", err)
	}
	objects = append(objects, domains...)

	// Extract sequences
	sequences, err := c.extractSequences(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract sequences: %w", err)
	}
	objects = append(objects, sequences...)

	// Extract tables (with columns and constraints)
	tables, err := c.extractTables(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}
	objects = append(objects, tables...)

	// Extract indexes (excluding implicit indexes for PK/UNIQUE)
	indexes, err := c.extractIndexes(ctx, schemaFilter, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to extract indexes: %w", err)
	}
	objects = append(objects, indexes...)

	// Extract views
	views, err := c.extractViews(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract views: %w", err)
	}
	objects = append(objects, views...)

	// Extract functions
	functions, err := c.extractFunctions(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract functions: %w", err)
	}
	objects = append(objects, functions...)

	// Extract triggers
	triggers, err := c.extractTriggers(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract triggers: %w", err)
	}
	objects = append(objects, triggers...)

	// Extract policies
	policies, err := c.extractPolicies(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract policies: %w", err)
	}
	objects = append(objects, policies...)

	return objects, nil
}

func (c *Catalog) buildSchemaFilter(include, exclude []string) string {
	if len(include) > 0 {
		quoted := make([]string, len(include))
		for i, s := range include {
			quoted[i] = fmt.Sprintf("'%s'", s)
		}
		return fmt.Sprintf("nspname IN (%s)", strings.Join(quoted, ", "))
	}

	if len(exclude) > 0 {
		quoted := make([]string, len(exclude))
		for i, s := range exclude {
			quoted[i] = fmt.Sprintf("'%s'", s)
		}
		return fmt.Sprintf("nspname NOT IN (%s)", strings.Join(quoted, ", "))
	}

	// Default: exclude system schemas
	return "nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')"
}

func (c *Catalog) extractExtensions(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			e.extname as name,
			e.extversion as version
		FROM pg_extension e
		JOIN pg_namespace n ON e.extnamespace = n.oid
		WHERE %s
		ORDER BY n.nspname, e.extname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var ext schema.Extension
		var version *string

		if err := rows.Scan(&ext.Schema, &ext.Name, &version); err != nil {
			return nil, err
		}
		ext.Version = version

		objects = append(objects, ext)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractEnums(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			t.typname as name,
			array_agg(e.enumlabel ORDER BY e.enumsortorder) as values,
			obj_description(t.oid, 'pg_type') as comment
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		JOIN pg_enum e ON t.oid = e.enumtypid
		WHERE t.typtype = 'e' AND %s
		GROUP BY n.nspname, t.typname, t.oid
		ORDER BY n.nspname, t.typname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var enum schema.EnumDef
		var comment *string

		if err := rows.Scan(&enum.Schema, &enum.Name, &enum.Values, &comment); err != nil {
			return nil, err
		}
		enum.Comment = comment

		objects = append(objects, enum)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractDomains(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			t.typname as name,
			format_type(t.typbasetype, t.typtypmod) as base_type,
			t.typnotnull as not_null,
			pg_get_expr(t.typdefaultbin, t.typrelid) as default_expr,
			pg_get_expr(c.conbin, c.conrelid) as check_expr,
			obj_description(t.oid, 'pg_type') as comment
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		LEFT JOIN pg_constraint c ON t.oid = c.contypid
		WHERE t.typtype = 'd' AND %s
		ORDER BY n.nspname, t.typname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var domain schema.DomainDef
		var defaultExpr, checkExpr, comment *string

		if err := rows.Scan(&domain.Schema, &domain.Name, &domain.BaseType, &domain.NotNull, &defaultExpr, &checkExpr, &comment); err != nil {
			return nil, err
		}

		if defaultExpr != nil {
			expr := schema.Expr(*defaultExpr)
			domain.Default = &expr
		}
		if checkExpr != nil {
			expr := schema.Expr(*checkExpr)
			domain.Check = &expr
		}
		domain.Comment = comment

		objects = append(objects, domain)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractSequences(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as name,
			s.seqtypid::regtype::text as type,
			s.seqstart as start_value,
			s.seqincrement as increment,
			s.seqmin as min_value,
			s.seqmax as max_value,
			s.seqcache as cache_size,
			s.seqcycle as cycle,
			d.refobjid::regclass::text as owned_by_table,
			a.attname as owned_by_column
		FROM pg_sequence s
		JOIN pg_class c ON s.seqrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
		LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE %s
		ORDER BY n.nspname, c.relname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var seq schema.Sequence
		var ownedByTable, ownedByColumn *string

		if err := rows.Scan(&seq.Schema, &seq.Name, &seq.Type, &seq.Start, &seq.Increment,
			&seq.MinValue, &seq.MaxValue, &seq.Cache, &seq.Cycle, &ownedByTable, &ownedByColumn); err != nil {
			return nil, err
		}

		if ownedByTable != nil && ownedByColumn != nil {
			// Parse schema.table from owned_by_table
			parts := strings.Split(*ownedByTable, ".")
			if len(parts) == 2 {
				seq.OwnedBy = &schema.SequenceOwner{
					Schema: schema.SchemaName(parts[0]),
					Table:  schema.TableName(parts[1]),
					Column: schema.ColumnName(*ownedByColumn),
				}
			}
		}

		objects = append(objects, seq)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractTables(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	// First get all tables
	tablesQuery := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as table_name,
			pg_get_userbyid(c.relowner) as owner,
			c.reloptions,
			obj_description(c.oid, 'pg_class') as comment
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relkind = 'r' AND %s
		ORDER BY n.nspname, c.relname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, tablesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []schema.Table
	for rows.Next() {
		var tbl schema.Table
		var relOptions []string
		var owner, comment *string

		if err := rows.Scan(&tbl.Schema, &tbl.Name, &owner, &relOptions, &comment); err != nil {
			return nil, err
		}

		tbl.Owner = owner
		tbl.RelOptions = relOptions
		tbl.Comment = comment

		// Extract columns for this table
		columns, err := c.extractColumns(ctx, tbl.Schema, tbl.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to extract columns for %s.%s: %w", tbl.Schema, tbl.Name, err)
		}
		tbl.Columns = columns

		// Extract constraints for this table
		pk, uniques, checks, fks, err := c.extractConstraints(ctx, tbl.Schema, tbl.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to extract constraints for %s.%s: %w", tbl.Schema, tbl.Name, err)
		}
		tbl.PrimaryKey = pk
		tbl.Uniques = uniques
		tbl.Checks = checks
		tbl.ForeignKeys = fks

		tables = append(tables, tbl)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Convert to DatabaseObject slice
	var objects []schema.DatabaseObject
	for _, tbl := range tables {
		tblCopy := tbl
		objects = append(objects, tblCopy)
	}

	return objects, nil
}

func (c *Catalog) extractColumns(ctx context.Context, schemaName schema.SchemaName, tableName schema.TableName) ([]schema.Column, error) {
	query := `
		SELECT
			a.attname as column_name,
			format_type(a.atttypid, a.atttypmod) as column_type,
			a.attnotnull as not_null,
			pg_get_expr(ad.adbin, ad.adrelid) as default_expr,
			a.attgenerated::text as generated,
			a.attidentity::text as identity,
			col.collname as collation,
			col_description(a.attrelid, a.attnum) as comment
		FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		LEFT JOIN pg_collation col ON a.attcollation = col.oid AND a.attcollation != 0
		WHERE n.nspname = $1
			AND c.relname = $2
			AND a.attnum > 0
			AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	rows, err := c.pool.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []schema.Column
	for rows.Next() {
		var col schema.Column
		var defaultExpr, generated, identity, collation, comment *string

		if err := rows.Scan(&col.Name, &col.Type, &col.NotNull, &defaultExpr, &generated, &identity, &collation, &comment); err != nil {
			return nil, err
		}

		if defaultExpr != nil {
			expr := schema.Expr(*defaultExpr)
			col.Default = &expr
		}

		if generated != nil && len(*generated) > 0 {
			// 's' = STORED, 'v' = VIRTUAL
			col.Generated = &schema.GeneratedSpec{
				Expr:   schema.Expr(*defaultExpr),
				Stored: (*generated)[0] == 's',
			}
		}

		if identity != nil && len(*identity) > 0 {
			// 'a' = ALWAYS, 'd' = BY DEFAULT
			col.Identity = &schema.IdentitySpec{
				Always: (*identity)[0] == 'a',
			}
		}

		// Normalize default collation to nil
		if collation != nil && (*collation == "default" || *collation == "pg_catalog.default") {
			collation = nil
		}
		col.Collation = collation
		col.Comment = comment

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

func (c *Catalog) extractConstraints(ctx context.Context, schemaName schema.SchemaName, tableName schema.TableName) (
	*schema.PrimaryKey, []schema.UniqueConstraint, []schema.CheckConstraint, []schema.ForeignKey, error) {

	query := `
		SELECT
			con.conname as constraint_name,
			con.contype::text as constraint_type,
			array_agg(a.attname ORDER BY u.pos) as columns,
			con.condeferrable as deferrable,
			con.condeferred as deferred,
			pg_get_constraintdef(con.oid, true) as definition,
			fn.nspname as foreign_schema,
			fc.relname as foreign_table,
			con.confupdtype::text as update_action,
			con.confdeltype::text as delete_action,
			con.confmatchtype::text as match_type
		FROM pg_constraint con
		JOIN pg_class c ON con.conrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, pos) ON true
		LEFT JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = u.attnum
		LEFT JOIN pg_class fc ON con.confrelid = fc.oid
		LEFT JOIN pg_namespace fn ON fc.relnamespace = fn.oid
		WHERE n.nspname = $1 AND c.relname = $2
		GROUP BY con.oid, con.conname, con.contype, con.condeferrable, con.condeferred, fn.nspname, fc.relname, con.confupdtype, con.confdeltype, con.confmatchtype
		ORDER BY con.contype, con.conname
	`

	rows, err := c.pool.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer rows.Close()

	var pk *schema.PrimaryKey
	var uniques []schema.UniqueConstraint
	var checks []schema.CheckConstraint
	var fks []schema.ForeignKey

	for rows.Next() {
		var name, contype, definition string
		var columns []string
		var deferrable, deferred bool
		var foreignSchema, foreignTable *string
		var updateAction, deleteAction, matchType *string

		if err := rows.Scan(&name, &contype, &columns, &deferrable, &deferred, &definition,
			&foreignSchema, &foreignTable, &updateAction, &deleteAction, &matchType); err != nil {
			return nil, nil, nil, nil, err
		}

		switch contype {
		case "p": // Primary key
			cols := make([]schema.ColumnName, len(columns))
			for i, c := range columns {
				cols[i] = schema.ColumnName(c)
			}
			pk = &schema.PrimaryKey{
				Name:              &name,
				Cols:              cols,
				Deferrable:        deferrable,
				InitiallyDeferred: deferred,
			}

		case "u": // Unique
			cols := make([]schema.ColumnName, len(columns))
			for i, c := range columns {
				cols[i] = schema.ColumnName(c)
			}
			uniques = append(uniques, schema.UniqueConstraint{
				Name:              name,
				Cols:              cols,
				NullsDistinct:     true, // Default
				Deferrable:        deferrable,
				InitiallyDeferred: deferred,
			})

		case "c": // Check
			// Extract expression from definition (strip "CHECK ()")
			expr := strings.TrimPrefix(definition, "CHECK (")
			expr = strings.TrimSuffix(expr, ")")

			checks = append(checks, schema.CheckConstraint{
				Name:              name,
				Expr:              schema.Expr(expr),
				Deferrable:        deferrable,
				InitiallyDeferred: deferred,
			})

		case "f": // Foreign key
			cols := make([]schema.ColumnName, len(columns))
			for i, c := range columns {
				cols[i] = schema.ColumnName(c)
			}

			// Parse referenced columns from definition
			// TODO: This is a simplified version, might need more robust parsing
			refCols := []schema.ColumnName{} // Placeholder

			fks = append(fks, schema.ForeignKey{
				Name: name,
				Cols: cols,
				Ref: schema.ForeignKeyRef{
					Schema: schema.SchemaName(*foreignSchema),
					Table:  schema.TableName(*foreignTable),
					Cols:   refCols,
				},
				OnUpdate:          parseReferentialAction(updateAction),
				OnDelete:          parseReferentialAction(deleteAction),
				Match:             parseMatchType(matchType),
				Deferrable:        deferrable,
				InitiallyDeferred: deferred,
			})
		}
	}

	return pk, uniques, checks, fks, rows.Err()
}

func parseReferentialAction(action *string) schema.ReferentialAction {
	if action == nil {
		return schema.NoAction
	}
	switch *action {
	case "a":
		return schema.NoAction
	case "r":
		return schema.Restrict
	case "c":
		return schema.Cascade
	case "n":
		return schema.SetNull
	case "d":
		return schema.SetDefault
	default:
		return schema.NoAction
	}
}

func parseMatchType(match *string) schema.MatchType {
	if match == nil {
		return schema.MatchSimple
	}
	switch *match {
	case "f":
		return schema.MatchFull
	case "p":
		return schema.MatchPartial
	default:
		return schema.MatchSimple
	}
}

func (c *Catalog) extractIndexes(ctx context.Context, schemaFilter string, tables []schema.DatabaseObject) ([]schema.DatabaseObject, error) {
	// Build set of implicit index names (indexes backing PK/UNIQUE)
	implicitIndexes := make(map[string]bool)
	for _, obj := range tables {
		if tbl, ok := obj.(schema.Table); ok {
			if tbl.PrimaryKey != nil && tbl.PrimaryKey.Name != nil {
				implicitIndexes[*tbl.PrimaryKey.Name] = true
			}
			for _, uq := range tbl.Uniques {
				implicitIndexes[uq.Name] = true
			}
		}
	}

	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as table_name,
			i.relname as index_name,
			ix.indisunique as is_unique,
			am.amname as method,
			pg_get_indexdef(ix.indexrelid) as definition,
			obj_description(i.oid, 'pg_class') as comment
		FROM pg_index ix
		JOIN pg_class c ON ix.indrelid = c.oid
		JOIN pg_class i ON ix.indexrelid = i.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_am am ON i.relam = am.oid
		WHERE %s
			AND NOT ix.indisprimary
			AND NOT ix.indisexclusion
		ORDER BY n.nspname, c.relname, i.relname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var idx schema.Index
		var definition string
		var comment *string
		var method string

		if err := rows.Scan(&idx.Schema, &idx.Table, &idx.Name, &idx.Unique, &method, &definition, &comment); err != nil {
			return nil, err
		}

		// Skip implicit indexes
		if implicitIndexes[idx.Name] {
			continue
		}

		// Parse method
		idx.Method = schema.IndexMethod(method)
		idx.Comment = comment

		// TODO: Parse definition to extract key expressions, predicate, include columns
		// For now, just create a simple key expr
		idx.KeyExprs = []schema.IndexKeyExpr{
			{Expr: schema.Expr(definition)},
		}

		objects = append(objects, idx)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractViews(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as name,
			pg_get_userbyid(c.relowner) as owner,
			c.relkind = 'm' as is_materialized,
			pg_get_viewdef(c.oid, true) as definition,
			obj_description(c.oid, 'pg_class') as comment
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relkind IN ('v', 'm') AND %s
		ORDER BY n.nspname, c.relname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var view schema.View
		var owner, comment *string
		var isMaterialized bool
		var definition string

		if err := rows.Scan(&view.Schema, &view.Name, &owner, &isMaterialized, &definition, &comment); err != nil {
			return nil, err
		}

		view.Owner = owner
		view.Comment = comment
		if isMaterialized {
			view.Type = schema.MaterializedView
		} else {
			view.Type = schema.RegularView
		}

		view.Definition = schema.ViewDefinition{
			Query: definition,
		}

		objects = append(objects, view)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractFunctions(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			p.proname as name,
			pg_get_function_identity_arguments(p.oid) as args,
			pg_get_function_result(p.oid) as returns,
			l.lanname as language,
			p.provolatile as volatility,
			p.proisstrict as is_strict,
			p.prosecdef as security_definer,
			p.proparallel as parallel,
			pg_get_functiondef(p.oid) as source,
			obj_description(p.oid, 'pg_proc') as comment
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		JOIN pg_language l ON p.prolang = l.oid
		WHERE %s AND p.prokind = 'f'
		ORDER BY n.nspname, p.proname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var fn schema.Function
		var args, returns, language, volatility, parallel, source string
		var isStrict, securityDefiner bool
		var comment *string

		if err := rows.Scan(&fn.Schema, &fn.Name, &args, &returns, &language, &volatility, &isStrict, &securityDefiner, &parallel, &source, &comment); err != nil {
			return nil, err
		}

		fn.Language = schema.Language(language)
		fn.Strict = isStrict
		fn.SecurityDefiner = securityDefiner
		fn.Body = source
		fn.Comment = comment

		// Parse volatility
		switch volatility {
		case "i":
			fn.Volatility = schema.Immutable
		case "s":
			fn.Volatility = schema.Stable
		default:
			fn.Volatility = schema.Volatile
		}

		// Parse parallel safety
		switch parallel {
		case "s":
			fn.Parallel = schema.ParallelSafe
		case "r":
			fn.Parallel = schema.ParallelRestricted
		default:
			fn.Parallel = schema.ParallelUnsafe
		}

		// TODO: Parse args and returns properly
		fn.Returns = schema.ReturnsType{Type: schema.TypeName(returns)}

		objects = append(objects, fn)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractTriggers(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as table_name,
			t.tgname as trigger_name,
			t.tgtype as timing_events,
			t.tgfoid::regproc as function_name,
			obj_description(t.oid, 'pg_trigger') as comment
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE %s AND NOT t.tgisinternal
		ORDER BY n.nspname, c.relname, t.tgname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var trig schema.Trigger
		var timingEvents int16
		var functionName string

		if err := rows.Scan(&trig.Schema, &trig.Table, &trig.Name, &timingEvents, &functionName); err != nil {
			return nil, err
		}

		// Parse timing and events from tgtype bitfield
		// Timing: 2=BEFORE, 4=AFTER, 64=INSTEAD OF
		// Events: 4=INSERT, 8=DELETE, 16=UPDATE, 32=TRUNCATE
		if timingEvents&2 != 0 {
			trig.Timing = schema.Before
		} else if timingEvents&4 != 0 {
			trig.Timing = schema.After
		} else if timingEvents&64 != 0 {
			trig.Timing = schema.InsteadOf
		}

		// Parse events
		events := []schema.TriggerEvent{}
		if timingEvents&4 != 0 {
			events = append(events, schema.Insert)
		}
		if timingEvents&8 != 0 {
			events = append(events, schema.Delete)
		}
		if timingEvents&16 != 0 {
			events = append(events, schema.Update)
		}
		if timingEvents&32 != 0 {
			events = append(events, schema.Truncate)
		}
		trig.Events = events

		// Parse function name (schema.function format)
		parts := strings.Split(functionName, ".")
		if len(parts) == 2 {
			trig.Function = schema.QualifiedName{
				Schema: schema.SchemaName(parts[0]),
				Name:   parts[1],
			}
		} else {
			trig.Function = schema.QualifiedName{
				Schema: schema.SchemaName("public"),
				Name:   functionName,
			}
		}

		objects = append(objects, trig)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractPolicies(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as table_name,
			pol.polname as policy_name,
			pol.polpermissive as is_permissive,
			pol.polcmd as command,
			pol.polroles,
			pg_get_expr(pol.polqual, pol.polrelid) as using_expr,
			pg_get_expr(pol.polwithcheck, pol.polrelid) as with_check_expr
		FROM pg_policy pol
		JOIN pg_class c ON pol.polrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE %s
		ORDER BY n.nspname, c.relname, pol.polname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var pol schema.Policy
		var command string
		var roles []int32
		var usingExpr, withCheckExpr *string

		if err := rows.Scan(&pol.Schema, &pol.Table, &pol.Name, &pol.Permissive, &command, &roles, &usingExpr, &withCheckExpr); err != nil {
			return nil, err
		}

		// Parse command
		switch command {
		case "r":
			pol.For = schema.ForSelect
		case "a":
			pol.For = schema.ForInsert
		case "w":
			pol.For = schema.ForUpdate
		case "d":
			pol.For = schema.ForDelete
		default:
			pol.For = schema.ForAll
		}

		if usingExpr != nil {
			expr := schema.Expr(*usingExpr)
			pol.Using = &expr
		}
		if withCheckExpr != nil {
			expr := schema.Expr(*withCheckExpr)
			pol.WithCheck = &expr
		}

		// TODO: Convert role OIDs to role names
		pol.To = []string{"public"} // Placeholder

		objects = append(objects, pol)
	}

	return objects, rows.Err()
}
