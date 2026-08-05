package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/internal/sqlrender"
	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// Catalog provides methods to query PostgreSQL catalog tables
type Catalog struct {
	pool *Pool
}

// sequenceDependencyKind records PostgreSQL's typed pg_depend relationship for
// a column-backed sequence. `a` is the auto dependency used by SERIAL; `i` is
// the internal dependency used by IDENTITY. Treating them as interchangeable
// causes identity sequences to be dumped as standalone objects and loses their
// column options.
type sequenceDependencyKind string

const (
	sequenceStandalone sequenceDependencyKind = ""
	sequenceSerial     sequenceDependencyKind = "serial"
	sequenceIdentity   sequenceDependencyKind = "identity"
)

type catalogSequence struct {
	Sequence       schema.Sequence
	DependencyKind sequenceDependencyKind
}

// NewCatalog creates a new catalog querier
func NewCatalog(pool *Pool) *Catalog {
	return &Catalog{pool: pool}
}

// ExtractAllObjects queries the database and extracts all schema objects
func (c *Catalog) ExtractAllObjects(ctx context.Context, includeSchemas, excludeSchemas []string) ([]schema.DatabaseObject, error) {
	var objects []schema.DatabaseObject

	// Build schema filter clause
	schemaFilter, err := c.buildSchemaFilter(includeSchemas, excludeSchemas)
	if err != nil {
		return nil, fmt.Errorf("invalid schema filter: %w", err)
	}

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

	composites, err := c.extractComposites(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract composite types: %w", err)
	}
	objects = append(objects, composites...)

	// Extract sequences
	catalogSequences, err := c.extractSequences(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract sequences: %w", err)
	}

	// Extract tables (with columns and constraints)
	tableObjs, err := c.extractTables(ctx, schemaFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}

	// Validate and consume IDENTITY backing sequences. PostgreSQL creates these
	// implicitly with an internal dependency; they must be represented by their
	// column, never as a second standalone CREATE SEQUENCE object.
	identitySequences, err := validateIdentityBackingSequences(tableObjs, catalogSequences)
	if err != nil {
		return nil, err
	}

	// Normalize only exact, metadata-free PostgreSQL SERIAL expansions. Any
	// noncanonical sequence remains explicit so its name/options/ACL are retained.
	serialSequences := make(map[schema.ObjectKey]bool)
	for i, obj := range tableObjs {
		if tbl, ok := obj.(schema.Table); ok {
			normalizedTable, collapsed := normalizeCatalogTable(tbl, catalogSequences)
			tableObjs[i] = normalizedTable
			for key := range collapsed {
				serialSequences[key] = true
			}
		}
	}

	// Add tables to objects
	objects = append(objects, tableObjs...)

	// Add standalone and noncanonical SERIAL sequences. IDENTITY sequences are
	// already represented by their owning column and exact sequence name/options.
	for _, catalogSeq := range catalogSequences {
		seq := catalogSeq.Sequence
		key := objectmap.Key(seq)
		if identitySequences[key] || serialSequences[key] {
			continue
		}
		objects = append(objects, seq)
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

func (c *Catalog) buildSchemaFilter(include, exclude []string) (string, error) {
	if len(include) > 0 {
		quoted := make([]string, len(include))
		for i, s := range include {
			literal, err := sqlrender.Literal(s)
			if err != nil {
				return "", err
			}
			quoted[i] = literal
		}
		return fmt.Sprintf("nspname IN (%s)", strings.Join(quoted, ", ")), nil
	}

	if len(exclude) > 0 {
		quoted := make([]string, len(exclude))
		for i, s := range exclude {
			literal, err := sqlrender.Literal(s)
			if err != nil {
				return "", err
			}
			quoted[i] = literal
		}
		return fmt.Sprintf("nspname NOT IN (%s)", strings.Join(quoted, ", ")), nil
	}

	// Default: exclude system schemas
	return "nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')", nil
}

// ExtractExtensions queries installed extensions from the database, excluding
// system schemas (pg_catalog, information_schema, pg_toast) by default.
func (c *Catalog) ExtractExtensions(ctx context.Context) ([]schema.DatabaseObject, error) {
	schemaFilter, err := c.buildSchemaFilter(nil, nil)
	if err != nil {
		return nil, err
	}
	return c.extractExtensions(ctx, schemaFilter)
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
			t.oid,
			pg_get_userbyid(t.typowner) as owner,
			array_agg(e.enumlabel ORDER BY e.enumsortorder) as values,
			obj_description(t.oid, 'pg_type') as comment
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		JOIN pg_enum e ON t.oid = e.enumtypid
		WHERE t.typtype = 'e' AND %s
		GROUP BY n.nspname, t.typname, t.oid, t.typowner
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
		var oid uint32
		var owner *string

		if err := rows.Scan(&enum.Schema, &enum.Name, &oid, &owner, &enum.Values, &comment); err != nil {
			return nil, err
		}
		enum.Owner = owner
		enum.Comment = comment
		grants, err := c.extractTypeACL(ctx, oid)
		if err != nil {
			return nil, fmt.Errorf("acl for enum %s.%s: %w", enum.Schema, enum.Name, err)
		}
		enum.Grants = grants

		objects = append(objects, enum)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractDomains(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		WITH canonical_search_path AS MATERIALIZED (
			SELECT set_config('search_path', 'pg_catalog, public', true)
		)
		SELECT
			n.nspname as schema,
			t.typname as name,
			t.oid,
			pg_get_userbyid(t.typowner) as owner,
			format_type(t.typbasetype, t.typtypmod) as base_type,
			t.typnotnull as not_null,
			pg_get_expr(t.typdefaultbin, t.typrelid) as default_expr,
			pg_get_expr(c.conbin, c.conrelid) as check_expr,
			obj_description(t.oid, 'pg_type') as comment
		FROM pg_type t
		CROSS JOIN canonical_search_path
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
		var oid uint32
		var owner *string

		if err := rows.Scan(&domain.Schema, &domain.Name, &oid, &owner, &domain.BaseType, &domain.NotNull, &defaultExpr, &checkExpr, &comment); err != nil {
			return nil, err
		}
		domain.Owner = owner

		if defaultExpr != nil {
			expr := schema.Expr(*defaultExpr)
			domain.Default = &expr
		}
		if checkExpr != nil {
			expr := schema.Expr(*checkExpr)
			domain.Check = &expr
		}
		domain.Comment = comment
		grants, err := c.extractTypeACL(ctx, oid)
		if err != nil {
			return nil, fmt.Errorf("acl for domain %s.%s: %w", domain.Schema, domain.Name, err)
		}
		domain.Grants = grants

		objects = append(objects, domain)
	}

	return objects, rows.Err()
}

func (c *Catalog) extractComposites(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	query := fmt.Sprintf(`
		WITH canonical_search_path AS MATERIALIZED (
			SELECT set_config('search_path', 'pg_catalog, public', true)
		)
		SELECT
			n.nspname,
			t.typname,
			t.oid,
			pg_get_userbyid(t.typowner),
			obj_description(t.oid, 'pg_type'),
			COALESCE(array_agg(a.attname::text ORDER BY a.attnum) FILTER (WHERE a.attnum IS NOT NULL), ARRAY[]::text[]),
			COALESCE(array_agg(format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) FILTER (WHERE a.attnum IS NOT NULL), ARRAY[]::text[]),
			COALESCE(bool_or(a.attcollation <> attribute_type.typcollation) FILTER (WHERE a.attnum IS NOT NULL), false)
		FROM pg_type t
		CROSS JOIN canonical_search_path
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_class relation ON relation.oid = t.typrelid AND relation.relkind = 'c'
		LEFT JOIN pg_attribute a ON a.attrelid = relation.oid AND a.attnum > 0 AND NOT a.attisdropped
		LEFT JOIN pg_type attribute_type ON attribute_type.oid = a.atttypid
		WHERE t.typtype = 'c' AND %s
		GROUP BY n.nspname, t.typname, t.oid, t.typowner
		ORDER BY n.nspname, t.typname
	`, schemaFilter)

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []schema.DatabaseObject
	for rows.Next() {
		var composite schema.CompositeDef
		var oid uint32
		var owner, comment *string
		var names, types []string
		var hasNondefaultCollation bool
		if err := rows.Scan(&composite.Schema, &composite.Name, &oid, &owner, &comment, &names, &types, &hasNondefaultCollation); err != nil {
			return nil, err
		}
		if hasNondefaultCollation {
			return nil, fmt.Errorf("composite %s.%s has an attribute with non-default collation, which is not modeled", composite.Schema, composite.Name)
		}
		if len(names) != len(types) {
			return nil, fmt.Errorf("composite %s.%s returned %d names and %d types", composite.Schema, composite.Name, len(names), len(types))
		}
		composite.Owner = owner
		composite.Comment = comment
		composite.Attributes = make([]schema.CompositeAttr, len(names))
		for i := range names {
			composite.Attributes[i] = schema.CompositeAttr{Name: names[i], Type: schema.TypeName(types[i])}
		}
		grants, err := c.extractTypeACL(ctx, oid)
		if err != nil {
			return nil, fmt.Errorf("acl for composite %s.%s: %w", composite.Schema, composite.Name, err)
		}
		composite.Grants = grants
		objects = append(objects, composite)
	}
	return objects, rows.Err()
}

func (c *Catalog) extractSequences(ctx context.Context, schemaFilter string) ([]catalogSequence, error) {
	query := fmt.Sprintf(`
		WITH canonical_search_path AS MATERIALIZED (
			SELECT set_config('search_path', 'pg_catalog, public', true)
		)
		SELECT
			n.nspname as schema,
			c.relname as name,
			c.oid,
			pg_get_userbyid(c.relowner) as owner,
			obj_description(c.oid, 'pg_class') as comment,
			c.relpersistence::text,
			s.seqtypid::regtype::text as type,
			s.seqstart as start_value,
			s.seqincrement as increment,
			s.seqmin as min_value,
			s.seqmax as max_value,
				s.seqcache as cache_size,
				s.seqcycle as cycle,
				d.deptype::text as dependency_type,
				tn.nspname as owned_by_schema,
			tc.relname as owned_by_table,
			a.attname as owned_by_column
		FROM pg_sequence s
		CROSS JOIN canonical_search_path
		JOIN pg_class c ON s.seqrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
			LEFT JOIN pg_depend d
				ON d.classid = 'pg_class'::regclass
				AND d.objid = c.oid
				AND d.objsubid = 0
				AND d.refclassid = 'pg_class'::regclass
				AND d.refobjsubid > 0
				AND d.deptype IN ('a', 'i')
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

	var sequences []catalogSequence
	for rows.Next() {
		var seq schema.Sequence
		var dependencyType, ownedBySchema, ownedByTable, ownedByColumn *string
		var oid uint32
		var owner *string
		var comment *string
		var persistence string

		if err := rows.Scan(&seq.Schema, &seq.Name, &oid, &owner, &comment, &persistence, &seq.Type, &seq.Start, &seq.Increment,
			&seq.MinValue, &seq.MaxValue, &seq.Cache, &seq.Cycle,
			&dependencyType, &ownedBySchema, &ownedByTable, &ownedByColumn); err != nil {
			return nil, err
		}
		if persistence != "p" {
			return nil, fmt.Errorf("sequence %s.%s has unsupported persistence %q", seq.Schema, seq.Name, persistence)
		}

		seq.Owner = owner
		seq.Comment = comment

		dependencyKind := sequenceStandalone
		if dependencyType != nil {
			switch *dependencyType {
			case "a":
				dependencyKind = sequenceSerial
			case "i":
				dependencyKind = sequenceIdentity
			default:
				return nil, fmt.Errorf("sequence %s.%s has unsupported column dependency type %q", seq.Schema, seq.Name, *dependencyType)
			}
		}

		hasAnyOwnerPart := ownedBySchema != nil || ownedByTable != nil || ownedByColumn != nil
		hasAllOwnerParts := ownedBySchema != nil && ownedByTable != nil && ownedByColumn != nil
		if dependencyKind == sequenceStandalone && hasAnyOwnerPart {
			return nil, fmt.Errorf("standalone sequence %s.%s unexpectedly has partial column ownership metadata", seq.Schema, seq.Name)
		}
		if dependencyKind != sequenceStandalone && !hasAllOwnerParts {
			return nil, fmt.Errorf("column-backed sequence %s.%s has incomplete ownership metadata", seq.Schema, seq.Name)
		}
		if hasAllOwnerParts {
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

		sequences = append(sequences, catalogSequence{Sequence: seq, DependencyKind: dependencyKind})
	}

	return sequences, rows.Err()
}

func validateIdentityBackingSequences(tableObjects []schema.DatabaseObject, sequences []catalogSequence) (map[schema.ObjectKey]bool, error) {
	byKey := make(map[schema.ObjectKey]catalogSequence, len(sequences))
	for _, catalogSeq := range sequences {
		seq := catalogSeq.Sequence
		key := schema.ObjectKey{Kind: schema.SequenceKind, Schema: seq.Schema, Name: seq.Name}
		if _, duplicate := byKey[key]; duplicate {
			return nil, fmt.Errorf("catalog returned duplicate sequence identity %v", key)
		}
		byKey[key] = catalogSeq
	}

	consumed := make(map[schema.ObjectKey]bool)
	for _, object := range tableObjects {
		table, ok := object.(schema.Table)
		if !ok {
			continue
		}
		for _, column := range table.Columns {
			if column.Identity == nil {
				continue
			}
			if column.Identity.SequenceName == nil {
				return nil, fmt.Errorf("identity column %s.%s.%s has no modeled backing-sequence name", table.Schema, table.Name, column.Name)
			}
			sequenceName := column.Identity.SequenceName
			key := schema.ObjectKey{Kind: schema.SequenceKind, Schema: sequenceName.Schema, Name: sequenceName.Name}
			catalogSeq, exists := byKey[key]
			if !exists {
				return nil, fmt.Errorf("identity column %s.%s.%s references backing sequence %s.%s outside the extracted object set", table.Schema, table.Name, column.Name, sequenceName.Schema, sequenceName.Name)
			}
			if catalogSeq.DependencyKind != sequenceIdentity || catalogSeq.Sequence.OwnedBy == nil {
				return nil, fmt.Errorf("identity column %s.%s.%s backing sequence %s.%s is not classified as an internal identity dependency", table.Schema, table.Name, column.Name, sequenceName.Schema, sequenceName.Name)
			}
			owner := catalogSeq.Sequence.OwnedBy
			if owner.Schema != table.Schema || owner.Table != table.Name || owner.Column != column.Name {
				return nil, fmt.Errorf("identity sequence %s.%s is linked to %s.%s.%s, expected %s.%s.%s", catalogSeq.Sequence.Schema, catalogSeq.Sequence.Name, owner.Schema, owner.Table, owner.Column, table.Schema, table.Name, column.Name)
			}
			if table.Owner == nil || catalogSeq.Sequence.Owner == nil || *table.Owner != *catalogSeq.Sequence.Owner {
				return nil, fmt.Errorf("identity sequence %s.%s owner does not match owning table %s.%s", catalogSeq.Sequence.Schema, catalogSeq.Sequence.Name, table.Schema, table.Name)
			}
			if schema.NormalizeTypeName(schema.TypeName(catalogSeq.Sequence.Type)) != schema.NormalizeTypeName(column.Type) {
				return nil, fmt.Errorf("identity sequence %s.%s type %q does not match column %s.%s.%s type %q", catalogSeq.Sequence.Schema, catalogSeq.Sequence.Name, catalogSeq.Sequence.Type, table.Schema, table.Name, column.Name, column.Type)
			}
			if catalogSeq.Sequence.Comment != nil || !hasDefaultSequenceACL(catalogSeq.Sequence) {
				return nil, fmt.Errorf("identity sequence %s.%s has a comment or non-default ACL that the identity-column model cannot preserve; remove that metadata or use an explicit migration", catalogSeq.Sequence.Schema, catalogSeq.Sequence.Name)
			}
			consumed[key] = true
		}
	}

	for _, catalogSeq := range sequences {
		if catalogSeq.DependencyKind != sequenceIdentity {
			continue
		}
		seq := catalogSeq.Sequence
		key := schema.ObjectKey{Kind: schema.SequenceKind, Schema: seq.Schema, Name: seq.Name}
		if !consumed[key] {
			return nil, fmt.Errorf("identity sequence %s.%s has no owning identity column in the extracted object set", seq.Schema, seq.Name)
		}
	}

	return consumed, nil
}

func (c *Catalog) extractTables(ctx context.Context, schemaFilter string) ([]schema.DatabaseObject, error) {
	// First get all tables
	tablesQuery := fmt.Sprintf(`
		SELECT
			n.nspname as schema,
			c.relname as table_name,
			c.oid,
			c.relpersistence::text,
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
		var persistence string
		var owner, comment *string
		var oid uint32

		if err := rows.Scan(&tbl.Schema, &tbl.Name, &oid, &persistence, &owner, &relOptions, &comment); err != nil {
			return nil, err
		}
		if persistence != "p" {
			return nil, fmt.Errorf("table %s.%s has unsupported persistence %q; temporary and unlogged tables are not modeled", tbl.Schema, tbl.Name, persistence)
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
	WITH canonical_search_path AS MATERIALIZED (
		SELECT set_config('search_path', 'pg_catalog, public', true)
	)
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
			seq.sequence_schema,
			seq.sequence_name,
			seq.dependency_type,
			col.collname as collation,
		col_description(a.attrelid, a.attnum) as comment,
		a.attacl IS NOT NULL AS has_column_acl
		FROM pg_attribute a
		CROSS JOIN canonical_search_path
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
	            s.seqcycle,
	            seq_namespace.nspname AS sequence_schema,
	            seq_class.relname AS sequence_name,
	            d.deptype::text AS dependency_type
	        FROM pg_depend d
	        JOIN pg_class seq_class ON d.objid = seq_class.oid AND d.deptype IN ('a', 'i')
	        JOIN pg_namespace seq_namespace ON seq_namespace.oid = seq_class.relnamespace
	        JOIN pg_sequence s ON s.seqrelid = seq_class.oid
	        WHERE d.classid = 'pg_class'::regclass
	          AND d.objsubid = 0
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
		var defaultExpr, generated, identity, sequenceSchema, sequenceName, dependencyType, collation, comment *string
		var seqStart, seqIncrement, seqMin, seqMax, seqCache sql.NullInt64
		var seqCycle sql.NullBool
		var hasColumnACL bool

		if err := rows.Scan(&col.Name, &col.Type, &col.NotNull, &defaultExpr, &generated, &identity,
			&seqStart, &seqIncrement, &seqMin, &seqMax, &seqCache, &seqCycle,
			&sequenceSchema, &sequenceName, &dependencyType,
			&collation, &comment, &hasColumnACL); err != nil {
			return nil, err
		}
		if hasColumnACL {
			return nil, fmt.Errorf("column-level ACL on %s.%s.%s is not modeled; use object-level grants or an explicit migration", schemaName, tableName, col.Name)
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
			if dependencyType == nil || *dependencyType != "i" || sequenceSchema == nil || sequenceName == nil {
				return nil, fmt.Errorf("identity column %s.%s.%s has no complete internal backing-sequence dependency", schemaName, tableName, col.Name)
			}
			// 'a' = ALWAYS, 'd' = BY DEFAULT
			spec := &schema.IdentitySpec{
				Always:       (*identity)[0] == 'a',
				SequenceName: &schema.QualifiedName{Schema: schema.SchemaName(*sequenceSchema), Name: *sequenceName},
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
		} else if dependencyType != nil && *dependencyType == "i" {
			return nil, fmt.Errorf("column %s.%s.%s has an identity backing sequence but is not marked as an identity column", schemaName, tableName, col.Name)
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
		WITH canonical_search_path AS MATERIALIZED (
			SELECT set_config('search_path', 'pg_catalog, public', true)
		)
		SELECT
			n.nspname as schema,
			c.relname as name,
			c.oid,
			pg_get_userbyid(c.relowner) as owner,
			c.relkind = 'm' as is_materialized,
			pg_get_viewdef(c.oid, true) as definition,
			obj_description(c.oid, 'pg_class') as comment,
			c.reloptions,
			c.reltablespace,
			COALESCE(am.amname, ''),
			c.relispopulated,
			c.relpersistence::text
		FROM pg_class c
		CROSS JOIN canonical_search_path
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_am am ON am.oid = c.relam
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
		var relOptions []string
		var tablespaceOID uint32
		var accessMethod, persistence string
		var populated bool

		if err := rows.Scan(&view.Schema, &view.Name, &oid, &owner, &isMaterialized, &definition, &comment, &relOptions, &tablespaceOID, &accessMethod, &populated, &persistence); err != nil {
			return nil, err
		}
		if persistence != "p" {
			return nil, fmt.Errorf("view %s.%s has unsupported persistence %q", view.Schema, view.Name, persistence)
		}
		if len(relOptions) > 0 {
			return nil, fmt.Errorf("view %s.%s has storage or view options that are not modeled", view.Schema, view.Name)
		}
		if isMaterialized && (tablespaceOID != 0 || accessMethod != "heap" || !populated) {
			return nil, fmt.Errorf("materialized view %s.%s uses a tablespace, non-heap access method, or unpopulated state that is not modeled", view.Schema, view.Name)
		}
		if err := c.validateViewColumnMetadata(ctx, oid, view.Schema, view.Name, isMaterialized); err != nil {
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

// validateViewColumnMetadata rejects pg_attribute state that schema.View
// cannot represent. PostgreSQL permits column ACLs and comments on both view
// kinds, defaults on regular views, and physical column tuning on materialized
// views. Silently omitting any of it would make a dump lossy.
func (c *Catalog) validateViewColumnMetadata(ctx context.Context, oid uint32, schemaName schema.SchemaName, viewName string, materialized bool) error {
	query := `
		SELECT
			a.attname,
			a.attacl IS NOT NULL AS has_acl,
			col_description(a.attrelid, a.attnum) IS NOT NULL AS has_comment,
			ad.oid IS NOT NULL AS has_default,
			a.attstattarget <> -1 AS has_statistics_target,
			a.attstorage <> t.typstorage AS has_nondefault_storage,
			a.attcompression::text <> '' AS has_compression,
			a.attoptions IS NOT NULL AS has_options,
			a.attfdwoptions IS NOT NULL AS has_fdw_options
		FROM pg_attribute a
		JOIN pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_attrdef ad
			ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		WHERE a.attrelid = $1
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum
	`
	rows, err := c.pool.Query(ctx, query, oid)
	if err != nil {
		return fmt.Errorf("inspect view column metadata for %s.%s: %w", schemaName, viewName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var column string
		var hasACL, hasComment, hasDefault bool
		var hasStatistics, hasStorage, hasCompression, hasOptions, hasFDWOptions bool
		if err := rows.Scan(
			&column,
			&hasACL,
			&hasComment,
			&hasDefault,
			&hasStatistics,
			&hasStorage,
			&hasCompression,
			&hasOptions,
			&hasFDWOptions,
		); err != nil {
			return fmt.Errorf("inspect view column metadata for %s.%s: %w", schemaName, viewName, err)
		}

		identity := fmt.Sprintf("%s.%s.%s", schemaName, viewName, column)
		switch {
		case hasACL:
			return fmt.Errorf("view column %s has a column-level ACL, which is not modeled", identity)
		case hasComment:
			return fmt.Errorf("view column %s has a comment, which is not modeled", identity)
		case hasDefault:
			return fmt.Errorf("view column %s has a default, which is not modeled", identity)
		case materialized && hasStatistics:
			return fmt.Errorf("materialized view column %s has a statistics target, which is not modeled", identity)
		case materialized && hasStorage:
			return fmt.Errorf("materialized view column %s has non-default storage, which is not modeled", identity)
		case materialized && hasCompression:
			return fmt.Errorf("materialized view column %s has compression metadata, which is not modeled", identity)
		case materialized && hasOptions:
			return fmt.Errorf("materialized view column %s has attribute options, which are not modeled", identity)
		case materialized && hasFDWOptions:
			return fmt.Errorf("materialized view column %s has FDW options, which are not modeled", identity)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect view column metadata for %s.%s: %w", schemaName, viewName, err)
	}
	return nil
}

// extractFunctionBody extracts the function body from pg_get_functiondef() output
// Uses pg_query to parse the CREATE FUNCTION statement and extract the body from the AST
func extractFunctionBody(fullDef string) (string, error) {
	// Parse the CREATE FUNCTION statement using pg_query
	result, err := pg_query.Parse(fullDef)
	if err != nil {
		return "", fmt.Errorf("failed to parse pg_get_functiondef output: %w", err)
	}

	// Extract the CreateFunctionStmt from the parsed result
	if len(result.Stmts) == 0 {
		return "", fmt.Errorf("pg_get_functiondef output contained no statements")
	}

	stmt := result.Stmts[0].Stmt
	if stmt == nil {
		return "", fmt.Errorf("pg_get_functiondef output contained an empty statement")
	}
	createFuncNode, ok := stmt.Node.(*pg_query.Node_CreateFunctionStmt)
	if !ok {
		return "", fmt.Errorf("pg_get_functiondef output was %T, not CREATE FUNCTION", stmt.Node)
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
			body, err := extractSingleFunctionBody(defElem.DefElem.Arg)
			if err != nil {
				return "", err
			}
			return strings.TrimSpace(body), nil
		}
	}

	return "", fmt.Errorf("CREATE FUNCTION definition did not contain an AS body")
}

func extractSingleFunctionBody(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", fmt.Errorf("CREATE FUNCTION AS option did not contain a body")
	}
	switch value := node.Node.(type) {
	case *pg_query.Node_String_:
		if value.String_ == nil {
			return "", fmt.Errorf("CREATE FUNCTION AS option contained an empty string node")
		}
		return value.String_.Sval, nil
	case *pg_query.Node_List:
		if value.List == nil || len(value.List.Items) != 1 {
			return "", fmt.Errorf("multi-part AS bodies used by C/internal functions are not modeled")
		}
		item, ok := value.List.Items[0].Node.(*pg_query.Node_String_)
		if !ok || item.String_ == nil {
			return "", fmt.Errorf("CREATE FUNCTION AS body has an unsupported AST node")
		}
		return item.String_.Sval, nil
	default:
		return "", fmt.Errorf("CREATE FUNCTION AS body has an unsupported AST node %T", node.Node)
	}
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
		WITH canonical_search_path AS MATERIALIZED (
			SELECT set_config('search_path', 'pg_catalog, public', true)
		)
		SELECT
			n.nspname as schema,
			p.proname as name,
			p.oid,
			p.prokind::text,
			pg_get_userbyid(p.proowner) as owner,
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'mode', COALESCE(p.proargmodes[arg.ordinality]::text, 'i'),
						'name', NULLIF(p.proargnames[arg.ordinality], ''),
						'type', pg_catalog.format_type(arg.type_oid, NULL)
					)
					ORDER BY arg.ordinality
				)::text
				FROM unnest(COALESCE(p.proallargtypes, p.proargtypes::oid[]))
					WITH ORDINALITY AS arg(type_oid, ordinality)
			), '[]') as args,
			COALESCE(pg_get_expr(p.proargdefaults, 0), '') as argument_defaults,
			p.pronargdefaults,
			pg_catalog.format_type(p.prorettype, NULL) as returns,
			p.proretset,
			l.lanname as language,
			p.provolatile::text as volatility,
			p.proisstrict as is_strict,
			p.prosecdef as security_definer,
			p.proparallel::text as parallel,
			p.proleakproof,
			p.procost,
			p.prorows,
			p.prosupport::oid <> 0::oid as has_support,
			COALESCE(cardinality(p.protrftypes::oid[]), 0) > 0 as has_transforms,
			COALESCE(to_json(p.proconfig)::text, 'null') as config,
			pg_get_functiondef(p.oid) as source,
			obj_description(p.oid, 'pg_proc') as comment
		FROM pg_proc p
		CROSS JOIN canonical_search_path
		JOIN pg_namespace n ON p.pronamespace = n.oid
		JOIN pg_language l ON p.prolang = l.oid
		LEFT JOIN pg_depend d
			ON d.classid = 'pg_proc'::regclass
			AND d.objid = p.oid
			AND d.refclassid = 'pg_extension'::regclass
			AND d.deptype = 'e'
		WHERE %s AND p.prokind IN ('f', 'w') AND d.objid IS NULL
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
			Args: []schema.FunctionArg{},
		}
		var argsJSON, argumentDefaults string
		var defaultCount int
		var routineKind, returns, language, volatility, parallel, configJSON, source string
		var cost, rowsEstimate float32
		var isStrict, securityDefiner, returnsSet, leakproof, hasSupport, hasTransforms bool
		var comment *string
		var oid uint32
		var owner *string

		if err := rows.Scan(
			&fn.Schema,
			&fn.Name,
			&oid,
			&routineKind,
			&owner,
			&argsJSON,
			&argumentDefaults,
			&defaultCount,
			&returns,
			&returnsSet,
			&language,
			&volatility,
			&isStrict,
			&securityDefiner,
			&parallel,
			&leakproof,
			&cost,
			&rowsEstimate,
			&hasSupport,
			&hasTransforms,
			&configJSON,
			&source,
			&comment,
		); err != nil {
			return nil, err
		}
		if routineKind != "f" {
			return nil, fmt.Errorf("window function %s.%s is not modeled", fn.Schema, fn.Name)
		}
		if err := validateCatalogFunctionAttributes(language, returnsSet, leakproof, cost, rowsEstimate, hasSupport, hasTransforms); err != nil {
			return nil, fmt.Errorf("unsupported attributes on function %s.%s: %w", fn.Schema, fn.Name, err)
		}

		fn.Owner = owner

		parsedArgs, err := parseCatalogFunctionArguments(argsJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to decode function %s.%s arguments: %w", fn.Schema, fn.Name, err)
		}
		if err := attachFunctionArgumentDefaults(parsedArgs, argumentDefaults, defaultCount); err != nil {
			return nil, fmt.Errorf("failed to decode function %s.%s argument defaults: %w", fn.Schema, fn.Name, err)
		}
		fn.Args = parsedArgs

		fn.Language = schema.Language(language)
		fn.Strict = isStrict
		fn.SecurityDefiner = securityDefiner

		// Extract function body from pg_get_functiondef output
		// pg_get_functiondef returns: CREATE OR REPLACE FUNCTION ... AS $tag$ body $tag$
		// We need to extract just the body part
		fn.Body, err = extractFunctionBody(source)
		if err != nil {
			return nil, fmt.Errorf("failed to extract function %s.%s body: %w", fn.Schema, fn.Name, err)
		}
		fn.Comment = comment
		fn.SearchPath, err = parseFunctionConfig(configJSON)
		if err != nil {
			return nil, fmt.Errorf("unsupported configuration on function %s.%s: %w", fn.Schema, fn.Name, err)
		}

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

		parsedReturn, err := functionReturnFromCatalog(fn.Args, returns, returnsSet)
		if err != nil {
			return nil, fmt.Errorf("failed to decode function %s.%s return type %q: %w", fn.Schema, fn.Name, returns, err)
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

func validateCatalogFunctionAttributes(language string, returnsSet, leakproof bool, cost, rowsEstimate float32, hasSupport, hasTransforms bool) error {
	if leakproof {
		return fmt.Errorf("LEAKPROOF is not modeled")
	}
	defaultCost := float32(100)
	if strings.EqualFold(language, "c") || strings.EqualFold(language, "internal") {
		defaultCost = 1
	}
	if cost != defaultCost {
		return fmt.Errorf("non-default COST %v is not modeled", cost)
	}
	defaultRows := float32(0)
	if returnsSet {
		defaultRows = 1000
	}
	if rowsEstimate != defaultRows {
		return fmt.Errorf("non-default ROWS %v is not modeled", rowsEstimate)
	}
	if hasSupport {
		return fmt.Errorf("SUPPORT functions are not modeled")
	}
	if hasTransforms {
		return fmt.Errorf("TRANSFORM clauses are not modeled")
	}
	return nil
}

type catalogFunctionArgument struct {
	Mode string  `json:"mode"`
	Name *string `json:"name"`
	Type string  `json:"type"`
}

func parseCatalogFunctionArguments(encoded string) ([]schema.FunctionArg, error) {
	var catalogArgs []catalogFunctionArgument
	if err := json.Unmarshal([]byte(encoded), &catalogArgs); err != nil {
		return nil, fmt.Errorf("invalid argument metadata JSON: %w", err)
	}

	args := make([]schema.FunctionArg, len(catalogArgs))
	for i, catalogArg := range catalogArgs {
		if strings.TrimSpace(catalogArg.Type) == "" {
			return nil, fmt.Errorf("argument %d has no type", i+1)
		}

		arg := schema.FunctionArg{
			Name: catalogArg.Name,
			Type: schema.NormalizeTypeName(schema.TypeName(catalogArg.Type)),
		}
		switch catalogArg.Mode {
		case "i":
			arg.Mode = schema.InMode
		case "o":
			arg.Mode = schema.OutMode
		case "b":
			arg.Mode = schema.InOutMode
		case "v":
			arg.Mode = schema.VariadicMode
		case "t":
			arg.Mode = schema.TableMode
		default:
			return nil, fmt.Errorf("argument %d has unknown mode %q", i+1, catalogArg.Mode)
		}
		args[i] = arg
	}
	return args, nil
}

func attachFunctionArgumentDefaults(args []schema.FunctionArg, defaultsSQL string, defaultCount int) error {
	if defaultCount < 0 {
		return fmt.Errorf("negative default count %d", defaultCount)
	}
	if defaultCount == 0 {
		if strings.TrimSpace(defaultsSQL) != "" {
			return fmt.Errorf("catalog returned defaults %q with a zero default count", defaultsSQL)
		}
		return nil
	}
	if strings.TrimSpace(defaultsSQL) == "" {
		return fmt.Errorf("catalog returned %d defaults without expressions", defaultCount)
	}

	parsed, err := pg_query.Parse("SELECT " + defaultsSQL)
	if err != nil {
		return fmt.Errorf("invalid default expression list: %w", err)
	}
	if len(parsed.Stmts) != 1 || parsed.Stmts[0].Stmt == nil {
		return fmt.Errorf("default expression list did not parse to one statement")
	}
	selectNode, ok := parsed.Stmts[0].Stmt.Node.(*pg_query.Node_SelectStmt)
	if !ok {
		return fmt.Errorf("default expression list parsed as %T, not SELECT", parsed.Stmts[0].Stmt.Node)
	}
	if len(selectNode.SelectStmt.TargetList) != defaultCount {
		return fmt.Errorf("catalog returned %d default expressions for count %d", len(selectNode.SelectStmt.TargetList), defaultCount)
	}

	inputPositions := make([]int, 0, len(args))
	for i, arg := range args {
		if arg.Mode == schema.InMode || arg.Mode == schema.InOutMode || arg.Mode == schema.VariadicMode {
			inputPositions = append(inputPositions, i)
		}
	}
	if defaultCount > len(inputPositions) {
		return fmt.Errorf("catalog returned %d defaults for only %d input arguments", defaultCount, len(inputPositions))
	}
	defaultPositions := inputPositions[len(inputPositions)-defaultCount:]
	for i, targetNode := range selectNode.SelectStmt.TargetList {
		target, ok := targetNode.Node.(*pg_query.Node_ResTarget)
		if !ok || target.ResTarget == nil || target.ResTarget.Val == nil {
			return fmt.Errorf("default expression %d has unsupported AST node %T", i+1, targetNode.Node)
		}
		expression := deparseExpr(target.ResTarget.Val)
		if expression == "" {
			return fmt.Errorf("default expression %d could not be deparsed", i+1)
		}
		value := schema.Expr(expression)
		args[defaultPositions[i]].Default = &value
	}
	return nil
}

func functionReturnFromCatalog(args []schema.FunctionArg, returnType string, returnsSet bool) (schema.FunctionReturn, error) {
	var tableColumns []schema.TableColumn
	for i, arg := range args {
		if arg.Mode != schema.TableMode {
			continue
		}
		if arg.Name == nil || *arg.Name == "" {
			return nil, fmt.Errorf("TABLE argument %d has no name", i+1)
		}
		tableColumns = append(tableColumns, schema.TableColumn{Name: *arg.Name, Type: arg.Type})
	}
	if len(tableColumns) > 0 {
		return schema.ReturnsTable{Columns: tableColumns}, nil
	}

	returnType = strings.TrimSpace(returnType)
	if returnType == "" {
		return nil, fmt.Errorf("empty return type")
	}
	normalized := schema.NormalizeTypeName(schema.TypeName(returnType))
	if returnsSet {
		return schema.ReturnsSetOf{Type: normalized}, nil
	}
	return schema.ReturnsType{Type: normalized}, nil
}

func parseFunctionConfig(encoded string) ([]schema.SchemaName, error) {
	if encoded == "null" {
		return nil, nil
	}
	var entries []string
	if err := json.Unmarshal([]byte(encoded), &entries); err != nil {
		return nil, fmt.Errorf("invalid proconfig JSON: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("empty non-NULL proconfig")
	}

	var searchPath []schema.SchemaName
	seenSearchPath := false
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("malformed configuration entry %q", entry)
		}
		if key != "search_path" {
			return nil, fmt.Errorf("configuration key %q is not modeled", key)
		}
		if seenSearchPath {
			return nil, fmt.Errorf("duplicate search_path configuration")
		}
		seenSearchPath = true
		var err error
		searchPath, err = parseCatalogSearchPath(value)
		if err != nil {
			return nil, fmt.Errorf("invalid search_path value %q: %w", value, err)
		}
	}
	return searchPath, nil
}

func parseCatalogSearchPath(value string) ([]schema.SchemaName, error) {
	// PostgreSQL represents an explicitly empty list as two double quotes in
	// the GUC text value. Preserve it as a non-nil empty slice so the renderer
	// can distinguish it from a function without any SET search_path clause.
	if value == `""` {
		return make([]schema.SchemaName, 0), nil
	}

	parsed, err := pg_query.Parse("SET search_path TO " + value)
	if err != nil {
		return nil, err
	}
	if len(parsed.Stmts) != 1 || parsed.Stmts[0].Stmt == nil {
		return nil, fmt.Errorf("search_path did not parse to one statement")
	}
	setNode, ok := parsed.Stmts[0].Stmt.Node.(*pg_query.Node_VariableSetStmt)
	if !ok || setNode.VariableSetStmt == nil {
		return nil, fmt.Errorf("search_path parsed as %T, not SET", parsed.Stmts[0].Stmt.Node)
	}
	set := setNode.VariableSetStmt
	if set.Kind != pg_query.VariableSetKind_VAR_SET_VALUE || set.Name != "search_path" || set.IsLocal {
		return nil, fmt.Errorf("unsupported SET search_path form")
	}
	if len(set.Args) == 0 {
		return nil, fmt.Errorf("search_path has no values")
	}

	path := make([]schema.SchemaName, len(set.Args))
	for i, arg := range set.Args {
		value, ok := variableSetStringValue(arg)
		if !ok || value == "" {
			return nil, fmt.Errorf("search_path item %d is not a non-empty schema name", i+1)
		}
		path[i] = schema.SchemaName(value)
	}
	return path, nil
}

func variableSetStringValue(node *pg_query.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	switch value := node.Node.(type) {
	case *pg_query.Node_String_:
		return value.String_.Sval, true
	case *pg_query.Node_AConst:
		if stringValue := value.AConst.GetSval(); stringValue != nil {
			return stringValue.Sval, true
		}
	}
	return "", false
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
						WHEN role_oid.oid = 0 THEN 'PUBLIC'
						ELSE r.rolname
					END
					ORDER BY
						CASE
							WHEN role_oid.oid = 0 THEN 'PUBLIC'
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
