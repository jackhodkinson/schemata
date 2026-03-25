package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
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
	sequenceObjs, err := c.extractSequences(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract sequences: %w", err)
	}

	// Extract tables (with columns and constraints)
	tableObjs, err := c.extractTables(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}

	// Normalize tables to convert expanded SERIAL types back to SERIAL
	// Build sequence slice for normalization
	var sequences []schema.Sequence
	for _, obj := range sequenceObjs {
		if seq, ok := obj.(schema.Sequence); ok {
			sequences = append(sequences, seq)
		}
	}

	// Normalize each table and filter out sequences owned by SERIAL columns
	var serialSequences = make(map[schema.ObjectKey]bool)
	for i, obj := range tableObjs {
		if tbl, ok := obj.(schema.Table); ok {
			normalizedTable := NormalizeTable(tbl, sequences)
			tableObjs[i] = normalizedTable

			// Mark sequences that are owned by SERIAL columns so we can filter them out
			for _, col := range normalizedTable.Columns {
				// If column type is serial/bigserial/smallserial, it has an owned sequence
				typeLower := strings.ToLower(string(col.Type))
				if typeLower == "serial" || typeLower == "bigserial" || typeLower == "smallserial" {
					// Find the sequence owned by this column
					for _, seq := range sequences {
						if seq.OwnedBy != nil &&
							seq.OwnedBy.Schema == normalizedTable.Schema &&
							seq.OwnedBy.Table == normalizedTable.Name &&
							seq.OwnedBy.Column == col.Name {
							key := objectmap.Key(seq)
							serialSequences[key] = true
						}
					}
				}
			}
		}
	}

	// Add tables to objects
	objects = append(objects, tableObjs...)

	// Add only non-SERIAL sequences to objects (filter out auto-generated sequences)
	for _, obj := range sequenceObjs {
		if seq, ok := obj.(schema.Sequence); ok {
			key := objectmap.Key(seq)
			if !serialSequences[key] {
				objects = append(objects, seq)
			}
		}
	}

	// Extract indexes (excluding implicit indexes for PK/UNIQUE)
	indexes, err := c.extractIndexes(ctx, schemaFilter, tableObjs)
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

// ExtractExtensions queries installed extensions from the database, excluding
// system schemas (pg_catalog, information_schema, pg_toast) by default.
func (c *Catalog) ExtractExtensions(ctx context.Context) ([]schema.DatabaseObject, error) {
	return c.extractExtensions(ctx, c.buildSchemaFilter(nil, nil))
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
			c.oid,
			pg_get_userbyid(c.relowner) as owner,
			s.seqtypid::regtype::text as type,
			s.seqstart as start_value,
			s.seqincrement as increment,
			s.seqmin as min_value,
			s.seqmax as max_value,
			s.seqcache as cache_size,
			s.seqcycle as cycle,
			tn.nspname as owned_by_schema,
			tc.relname as owned_by_table,
			a.attname as owned_by_column
		FROM pg_sequence s
		JOIN pg_class c ON s.seqrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
		LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		LEFT JOIN pg_class tc ON tc.oid = d.refobjid
		LEFT JOIN pg_namespace tn ON tc.relnamespace = tn.oid
		WHERE n.%s
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
		var ownedBySchema, ownedByTable, ownedByColumn *string
		var oid uint32
		var owner *string

		if err := rows.Scan(&seq.Schema, &seq.Name, &oid, &owner, &seq.Type, &seq.Start, &seq.Increment,
			&seq.MinValue, &seq.MaxValue, &seq.Cache, &seq.Cycle,
			&ownedBySchema, &ownedByTable, &ownedByColumn); err != nil {
			return nil, err
		}

		seq.Owner = owner

		if ownedBySchema != nil && ownedByTable != nil && ownedByColumn != nil {
			seq.OwnedBy = &schema.SequenceOwner{
				Schema: schema.SchemaName(*ownedBySchema),
				Table:  schema.TableName(*ownedByTable),
				Column: schema.ColumnName(*ownedByColumn),
			}
		}

		grants, err := c.extractRelationACL(ctx, oid)
		if err != nil {
			return nil, fmt.Errorf("acl for sequence %s.%s: %w", seq.Schema, seq.Name, err)
		}
		seq.Grants = grants

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
			c.oid,
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
		var oid uint32

		if err := rows.Scan(&tbl.Schema, &tbl.Name, &oid, &owner, &relOptions, &comment); err != nil {
			return nil, err
		}

		tbl.Owner = owner
		tbl.RelOptions = relOptions
		tbl.Comment = comment

		grants, err := c.extractRelationACL(ctx, oid)
		if err != nil {
			return nil, fmt.Errorf("acl for table %s.%s: %w", tbl.Schema, tbl.Name, err)
		}
		tbl.Grants = grants

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
        seq.seqstart,
        seq.seqincrement,
        seq.seqmin,
        seq.seqmax,
        seq.seqcache,
        seq.seqcycle,
        col.collname as collation,
        col_description(a.attrelid, a.attnum) as comment
		FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
    LEFT JOIN LATERAL (
        SELECT
            s.seqstart,
            s.seqincrement,
            s.seqmin,
            s.seqmax,
            s.seqcache,
            s.seqcycle
        FROM pg_depend d
        JOIN pg_class seq_class ON d.objid = seq_class.oid AND d.deptype = 'a'
        JOIN pg_sequence s ON s.seqrelid = seq_class.oid
        WHERE d.classid = 'pg_class'::regclass
          AND d.refclassid = 'pg_class'::regclass
          AND d.refobjid = c.oid
          AND d.refobjsubid = a.attnum
        LIMIT 1
    ) seq ON true
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
		var seqStart, seqIncrement, seqMin, seqMax, seqCache sql.NullInt64
		var seqCycle sql.NullBool

		if err := rows.Scan(&col.Name, &col.Type, &col.NotNull, &defaultExpr, &generated, &identity,
			&seqStart, &seqIncrement, &seqMin, &seqMax, &seqCache, &seqCycle,
			&collation, &comment); err != nil {
			return nil, err
		}

		if generated != nil && len(*generated) > 0 {
			// 's' = STORED, 'v' = VIRTUAL
			var expr schema.Expr
			if defaultExpr != nil {
				expr = schema.Expr(*defaultExpr)
			}
			col.Generated = &schema.GeneratedSpec{
				Expr:   expr,
				Stored: (*generated)[0] == 's' || (*generated)[0] == 'S',
			}
			// Generated columns do not have traditional defaults
			col.Default = nil
		} else if defaultExpr != nil {
			expr := schema.Expr(*defaultExpr)
			col.Default = &expr
		}

		if identity != nil && len(*identity) > 0 {
			// 'a' = ALWAYS, 'd' = BY DEFAULT
			spec := &schema.IdentitySpec{
				Always: (*identity)[0] == 'a',
			}

			var startPtr, incrementPtr, minPtr, maxPtr, cachePtr *int64
			var cyclePtr *bool

			if seqStart.Valid {
				val := seqStart.Int64
				startPtr = &val
			}
			if seqIncrement.Valid {
				val := seqIncrement.Int64
				incrementPtr = &val
			}
			if seqMin.Valid {
				val := seqMin.Int64
				minPtr = &val
			}
			if seqMax.Valid {
				val := seqMax.Int64
				maxPtr = &val
			}
			if seqCache.Valid {
				val := seqCache.Int64
				cachePtr = &val
			}
			if seqCycle.Valid {
				val := seqCycle.Bool
				cyclePtr = &val
			}

			spec.SequenceOptions = schema.IdentityOptionsFromParameters(col.Type, startPtr, incrementPtr, minPtr, maxPtr, cachePtr, cyclePtr)
			col.Identity = spec
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
			array_agg(a.attname ORDER BY u.pos) FILTER (WHERE a.attname IS NOT NULL) as columns,
			array_agg(af.attname ORDER BY uf.pos) FILTER (WHERE af.attname IS NOT NULL) as ref_columns,
        con.condeferrable as deferrable,
        con.condeferred as deferred,
        con.convalidated as validated,
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
		LEFT JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS uf(attnum, pos) ON true
		LEFT JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = uf.attnum
		LEFT JOIN pg_class fc ON con.confrelid = fc.oid
		LEFT JOIN pg_namespace fn ON fc.relnamespace = fn.oid
		WHERE n.nspname = $1 AND c.relname = $2
    GROUP BY con.oid, con.conname, con.contype, con.condeferrable, con.condeferred, con.convalidated, fn.nspname, fc.relname, con.confupdtype, con.confdeltype, con.confmatchtype
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
		var refColumns []string
		var deferrable, deferred bool
		var validated bool
		var foreignSchema, foreignTable *string
		var updateAction, deleteAction, matchType *string

		if err := rows.Scan(&name, &contype, &columns, &refColumns, &deferrable, &deferred, &validated, &definition,
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
				NotValid:          !validated,
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
				NotValid:          !validated,
			})

		case "f": // Foreign key
			cols := make([]schema.ColumnName, len(columns))
			for i, c := range columns {
				cols[i] = schema.ColumnName(c)
			}

			// Convert referenced columns from array
			refCols := make([]schema.ColumnName, len(refColumns))
			for i, c := range refColumns {
				refCols[i] = schema.ColumnName(c)
			}

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
				NotValid:          !validated,
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

	// First, get index metadata
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as table_name,
			i.relname as index_name,
			ix.indisunique as is_unique,
			am.amname as method,
        ix.indexrelid::int8 as index_oid,
        pg_get_expr(ix.indpred, ix.indrelid, true) as predicate,
			pg_get_indexdef(ix.indexrelid) as source,
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
		var indexOID int64
		var comment, predicate *string
		var source string
		var method string

		if err := rows.Scan(&idx.Schema, &idx.Table, &idx.Name, &idx.Unique, &method, &indexOID, &predicate, &source, &comment); err != nil {
			return nil, err
		}

		// Skip implicit indexes
		if implicitIndexes[idx.Name] {
			continue
		}

		// Parse method
		idx.Method = schema.IndexMethod(method)
		idx.Comment = comment

		// Parse predicate
		if predicate != nil {
			pred := schema.Expr(*predicate)
			idx.Predicate = &pred
		}

		// Extract key expressions by parsing the canonical `pg_get_indexdef()` output. This matches
		// what pg stores as the index definition, and avoids false positives from implicit/default
		// collations/opclasses and ordering/nulls semantics.
		keyExprs, includeCols, whereExpr, err := parseIndexDefinition(source)
		if err != nil {
			return nil, fmt.Errorf("failed to parse index definition for %s: %w", idx.Name, err)
		}
		idx.KeyExprs = keyExprs
		if len(includeCols) > 0 {
			idx.Include = includeCols
		}
		if whereExpr != nil {
			idx.Predicate = whereExpr
		}

		objects = append(objects, idx)
	}

	return objects, rows.Err()
}

func parseIndexDefinition(source string) ([]schema.IndexKeyExpr, []schema.ColumnName, *schema.Expr, error) {
	result, err := pg_query.Parse(source)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(result.Stmts) == 0 || result.Stmts[0].Stmt == nil {
		return nil, nil, nil, fmt.Errorf("empty parse result")
	}

	stmt := result.Stmts[0].Stmt
	indexNode, ok := stmt.Node.(*pg_query.Node_IndexStmt)
	if !ok || indexNode.IndexStmt == nil {
		return nil, nil, nil, fmt.Errorf("expected IndexStmt, got %T", stmt.Node)
	}

	indexStmt := indexNode.IndexStmt

	var keyExprs []schema.IndexKeyExpr
	for _, param := range indexStmt.IndexParams {
		if param == nil {
			continue
		}
		indexElemNode, ok := param.Node.(*pg_query.Node_IndexElem)
		if !ok || indexElemNode.IndexElem == nil {
			continue
		}
		keyExprs = append(keyExprs, parseIndexElem(indexElemNode.IndexElem))
	}

	var includeCols []schema.ColumnName
	for _, incl := range indexStmt.IndexIncludingParams {
		if incl == nil {
			continue
		}
		indexElemNode, ok := incl.Node.(*pg_query.Node_IndexElem)
		if !ok || indexElemNode.IndexElem == nil {
			continue
		}
		if indexElemNode.IndexElem.Name != "" {
			includeCols = append(includeCols, schema.ColumnName(indexElemNode.IndexElem.Name))
		}
	}

	var predicate *schema.Expr
	if indexStmt.WhereClause != nil {
		exprStr := deparseExpr(indexStmt.WhereClause)
		if exprStr != "" {
			tmp := schema.Expr(exprStr)
			predicate = &tmp
		}
	}

	return keyExprs, includeCols, predicate, nil
}

func parseIndexElem(elem *pg_query.IndexElem) schema.IndexKeyExpr {
	keyExpr := schema.IndexKeyExpr{}

	if elem.Name != "" {
		keyExpr.Expr = schema.Expr(elem.Name)
	} else if elem.Expr != nil {
		keyExpr.Expr = schema.Expr(deparseExpr(elem.Expr))
	}

	if len(elem.Collation) > 0 {
		collation := extractLastName(elem.Collation)
		if collation != "" {
			keyExpr.Collation = &collation
		}
	}

	if len(elem.Opclass) > 0 {
		opclass := extractLastName(elem.Opclass)
		if opclass != "" {
			keyExpr.OpClass = &opclass
		}
	}

	if elem.Ordering != pg_query.SortByDir_SORTBY_DEFAULT {
		if elem.Ordering == pg_query.SortByDir_SORTBY_ASC {
			ordering := schema.Asc
			keyExpr.Ordering = &ordering
		} else if elem.Ordering == pg_query.SortByDir_SORTBY_DESC {
			ordering := schema.Desc
			keyExpr.Ordering = &ordering
		}
	}

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

func extractLastName(nodes []*pg_query.Node) string {
	if len(nodes) == 0 {
		return ""
	}
	if strNode, ok := nodes[len(nodes)-1].Node.(*pg_query.Node_String_); ok {
		return strNode.String_.Sval
	}
	return ""
}

func deparseExpr(node *pg_query.Node) string {
	if node == nil {
		return ""
	}

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
		return ""
	}

	result = strings.TrimSpace(result)
	result = strings.TrimPrefix(result, "SELECT ")
	result = strings.TrimSuffix(result, ";")
	return strings.TrimSpace(result)
}

func (c *Catalog) extractViews(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as name,
			c.oid,
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
		var oid uint32

		if err := rows.Scan(&view.Schema, &view.Name, &oid, &owner, &isMaterialized, &definition, &comment); err != nil {
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

		grants, err := c.extractRelationACL(ctx, oid)
		if err != nil {
			return nil, fmt.Errorf("acl for view %s.%s: %w", view.Schema, view.Name, err)
		}
		view.Grants = grants

		objects = append(objects, view)
	}

	return objects, rows.Err()
}

// extractFunctionBody extracts the function body from pg_get_functiondef() output
// Uses pg_query to parse the CREATE FUNCTION statement and extract the body from the AST
func extractFunctionBody(fullDef string) string {
	// Parse the CREATE FUNCTION statement using pg_query
	result, err := pg_query.Parse(fullDef)
	if err != nil {
		// If parsing fails, return empty string
		// This shouldn't happen with valid pg_get_functiondef() output
		return ""
	}

	// Extract the CreateFunctionStmt from the parsed result
	if len(result.Stmts) == 0 {
		return ""
	}

	stmt := result.Stmts[0].Stmt
	createFuncNode, ok := stmt.Node.(*pg_query.Node_CreateFunctionStmt)
	if !ok {
		return ""
	}

	createFunc := createFuncNode.CreateFunctionStmt

	// Look through the function options for the "as" option (function body)
	for _, option := range createFunc.Options {
		if option == nil {
			continue
		}
		defElem, ok := option.Node.(*pg_query.Node_DefElem)
		if !ok {
			continue
		}

		if strings.ToLower(defElem.DefElem.Defname) == "as" {
			// Function body can be a single string or a list of strings
			// This matches the parser logic in internal/parser/objects.go:238-246
			if body := extractStringValue(defElem.DefElem.Arg); body != "" {
				return strings.TrimSpace(body)
			} else if bodyParts := extractListValues(defElem.DefElem.Arg); len(bodyParts) > 0 {
				// PL/pgSQL functions often have body as a list of strings
				return strings.TrimSpace(strings.Join(bodyParts, "\n"))
			}
		}
	}

	return ""
}

// extractStringValue extracts a string from a pg_query Node (helper for AST traversal)
func extractStringValue(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if strNode, ok := node.Node.(*pg_query.Node_String_); ok {
		return strNode.String_.Sval
	}
	return ""
}

// extractListValues extracts a list of strings from a pg_query Node (helper for AST traversal)
func extractListValues(node *pg_query.Node) []string {
	if node == nil {
		return nil
	}
	if listNode, ok := node.Node.(*pg_query.Node_List); ok {
		var values []string
		for _, item := range listNode.List.Items {
			if str := extractStringValue(item); str != "" {
				values = append(values, str)
			}
		}
		return values
	}
	return nil
}

func (c *Catalog) extractFunctions(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			p.proname as name,
			p.oid,
			pg_get_userbyid(p.proowner) as owner,
			pg_get_function_identity_arguments(p.oid) as args,
			pg_get_function_result(p.oid) as returns,
			l.lanname as language,
			p.provolatile::text as volatility,
			p.proisstrict as is_strict,
			p.prosecdef as security_definer,
			p.proparallel::text as parallel,
			pg_get_functiondef(p.oid) as source,
			obj_description(p.oid, 'pg_proc') as comment
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		JOIN pg_language l ON p.prolang = l.oid
		LEFT JOIN pg_depend d
			ON d.classid = 'pg_proc'::regclass
			AND d.objid = p.oid
			AND d.refclassid = 'pg_extension'::regclass
			AND d.deptype = 'e'
		WHERE %s AND p.prokind = 'f' AND d.objid IS NULL
		ORDER BY n.nspname, p.proname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		fn := schema.Function{
			Args:       []schema.FunctionArg{},
			SearchPath: []schema.SchemaName{},
		}
		var args, returns, language, volatility, parallel, source string
		var isStrict, securityDefiner bool
		var comment *string
		var oid uint32
		var owner *string

		if err := rows.Scan(&fn.Schema, &fn.Name, &oid, &owner, &args, &returns, &language, &volatility, &isStrict, &securityDefiner, &parallel, &source, &comment); err != nil {
			return nil, err
		}

		fn.Owner = owner

		parsedArgs, err := parseFunctionIdentityArguments(args)
		if err != nil {
			return nil, fmt.Errorf("failed to parse function %s.%s identity args %q: %w", fn.Schema, fn.Name, args, err)
		}
		fn.Args = parsedArgs

		fn.Language = schema.Language(language)
		fn.Strict = isStrict
		fn.SecurityDefiner = securityDefiner

		// Extract function body from pg_get_functiondef output
		// pg_get_functiondef returns: CREATE OR REPLACE FUNCTION ... AS $tag$ body $tag$
		// We need to extract just the body part
		fn.Body = extractFunctionBody(source)
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

		parsedReturn, err := parseFunctionReturn(returns)
		if err != nil {
			return nil, fmt.Errorf("failed to parse function %s.%s return %q: %w", fn.Schema, fn.Name, returns, err)
		}
		fn.Returns = parsedReturn

		grants, err := c.extractFunctionACL(ctx, oid)
		if err != nil {
			return nil, fmt.Errorf("acl for function %s.%s: %w", fn.Schema, fn.Name, err)
		}
		fn.Grants = grants

		objects = append(objects, fn)
	}

	return objects, rows.Err()
}

func parseFunctionIdentityArguments(args string) ([]schema.FunctionArg, error) {
	args = strings.TrimSpace(args)
	if args == "" {
		return nil, nil
	}

	parts := splitTopLevelComma(args)
	out := make([]schema.FunctionArg, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		arg := schema.FunctionArg{Mode: schema.InMode}

		upper := strings.ToUpper(part)
		switch {
		case strings.HasPrefix(upper, "VARIADIC "):
			arg.Mode = schema.VariadicMode
			part = strings.TrimSpace(part[len("VARIADIC "):])
		case strings.HasPrefix(upper, "INOUT "):
			arg.Mode = schema.InOutMode
			part = strings.TrimSpace(part[len("INOUT "):])
		case strings.HasPrefix(upper, "OUT "):
			arg.Mode = schema.OutMode
			part = strings.TrimSpace(part[len("OUT "):])
		case strings.HasPrefix(upper, "IN "):
			arg.Mode = schema.InMode
			part = strings.TrimSpace(part[len("IN "):])
		}

		if name, typ, ok := splitIdentifierAndType(part); ok {
			argName := name
			arg.Name = &argName
			arg.Type = schema.NormalizeTypeName(schema.TypeName(typ))
		} else {
			// Fallback for identity arguments provided as type-only.
			arg.Type = schema.NormalizeTypeName(schema.TypeName(part))
		}
		out = append(out, arg)
	}

	return out, nil
}

func parseFunctionReturn(returns string) (schema.FunctionReturn, error) {
	returns = strings.TrimSpace(returns)
	if returns == "" {
		return schema.ReturnsType{Type: "void"}, nil
	}

	upper := strings.ToUpper(returns)
	if strings.HasPrefix(upper, "SETOF ") {
		typ := strings.TrimSpace(returns[len("SETOF "):])
		return schema.ReturnsSetOf{Type: schema.NormalizeTypeName(schema.TypeName(typ))}, nil
	}

	if strings.HasPrefix(upper, "TABLE") {
		rest := strings.TrimSpace(returns[len("TABLE"):])
		if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
			return nil, fmt.Errorf("invalid TABLE return syntax")
		}
		inside := strings.TrimSpace(rest[1 : len(rest)-1])
		if inside == "" {
			return schema.ReturnsTable{Columns: nil}, nil
		}
		colParts := splitTopLevelComma(inside)
		cols := make([]schema.TableColumn, 0, len(colParts))
		for _, colPart := range colParts {
			colPart = strings.TrimSpace(colPart)
			if colPart == "" {
				continue
			}
			name, typ, ok := splitIdentifierAndType(colPart)
			if !ok {
				return nil, fmt.Errorf("invalid TABLE column %q", colPart)
			}
			cols = append(cols, schema.TableColumn{
				Name: name,
				Type: schema.NormalizeTypeName(schema.TypeName(typ)),
			})
		}
		return schema.ReturnsTable{Columns: cols}, nil
	}

	return schema.ReturnsType{Type: schema.NormalizeTypeName(schema.TypeName(returns))}, nil
}

func splitTopLevelComma(s string) []string {
	var parts []string
	start := 0
	depth := 0
	inQuotes := false

	for i, r := range s {
		switch r {
		case '"':
			inQuotes = !inQuotes
		case '(':
			if !inQuotes {
				depth++
			}
		case ')':
			if !inQuotes && depth > 0 {
				depth--
			}
		case ',':
			if !inQuotes && depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}

	parts = append(parts, s[start:])
	return parts
}

func splitIdentifierAndType(s string) (name string, typ string, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", false
	}

	// Column name may be quoted; type may contain spaces and parentheses.
	if s[0] == '"' {
		end := strings.Index(s[1:], `"`)
		if end == -1 {
			return "", "", false
		}
		name = s[1 : 1+end]
		rest := strings.TrimSpace(s[2+end:])
		if rest == "" {
			return "", "", false
		}
		return name, rest, true
	}

	fields := strings.Fields(s)
	if len(fields) < 2 {
		return "", "", false
	}

	name = fields[0]
	typ = strings.TrimSpace(s[len(name):])
	return name, typ, true
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
		var comment *string

		if err := rows.Scan(&trig.Schema, &trig.Table, &trig.Name, &timingEvents, &functionName, &comment); err != nil {
			return nil, err
		}

		// Parse timing and events from tgtype bitfield
		// Bit 0 (1): Row-level trigger (FOR EACH ROW)
		// Bit 1 (2): BEFORE
		// Bit 2 (4): INSERT (conflicts with AFTER - need to check timing first)
		// Bit 3 (8): DELETE
		// Bit 4 (16): UPDATE
		// Bit 5 (32): TRUNCATE
		// Bit 6 (64): INSTEAD OF

		// Parse FOR EACH ROW (bit 0)
		trig.ForEachRow = (timingEvents & 1) != 0

		// Parse timing
		if timingEvents&2 != 0 {
			trig.Timing = schema.Before
		} else if timingEvents&64 != 0 {
			trig.Timing = schema.InsteadOf
		} else {
			// If neither BEFORE nor INSTEAD OF, it's AFTER
			trig.Timing = schema.After
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
			pol.polcmd::text as command,
			COALESCE((
				SELECT array_agg(
					CASE
						WHEN role_oid.oid = 0 THEN 'public'
						ELSE r.rolname
					END
					ORDER BY
					CASE
						WHEN role_oid.oid = 0 THEN 'public'
						ELSE r.rolname
					END
				)
				FROM unnest(pol.polroles) AS role_oid(oid)
				LEFT JOIN pg_roles r ON r.oid = role_oid.oid
			), ARRAY[]::text[]) as roles,
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
		var roles []string
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

		pol.To = roles

		objects = append(objects, pol)
	}

	return objects, rows.Err()
}
