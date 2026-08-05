package planner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackhodkinson/schemata/internal/capability"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// DDLGenerator generates DDL statements from diff operations
type DDLGenerator struct {
	allowCascade bool
}

type UnsupportedChangeError struct {
	Key         schema.ObjectKey
	Change      string
	Remediation string
}

func (e *UnsupportedChangeError) Error() string {
	key := fmt.Sprintf("%s/%s/%s", e.Key.Kind, e.Key.Schema, e.Key.Name)
	if e.Key.TableName != "" {
		key += fmt.Sprintf("/%s", e.Key.TableName)
	}
	if e.Key.Signature != "" {
		key += e.Key.Signature
	}
	if e.Remediation != "" {
		return fmt.Sprintf("unsupported change (%s): %s; remediation: %s", key, e.Change, e.Remediation)
	}
	return fmt.Sprintf("unsupported change (%s): %s", key, e.Change)
}

// Unwrap exposes the common structured capability error while preserving the
// more specific legacy error type for callers already matching it.
func (e *UnsupportedChangeError) Unwrap() error {
	return &capability.UnsupportedError{
		Stage:       capability.AlterStage,
		Family:      capability.Family(e.Key.Kind),
		Feature:     e.Change,
		Object:      e.Key,
		Reason:      "the requested change is not safely renderable",
		Remediation: e.Remediation,
	}
}

type DDLGeneratorOption func(*DDLGenerator)

func WithAllowCascade(allow bool) DDLGeneratorOption {
	return func(g *DDLGenerator) {
		g.allowCascade = allow
	}
}

// NewDDLGenerator creates a new DDL generator
func NewDDLGenerator(opts ...DDLGeneratorOption) *DDLGenerator {
	g := &DDLGenerator{}
	for _, opt := range opts {
		if opt != nil {
			opt(g)
		}
	}
	return g
}

// GenerateDDL generates DDL statements for a diff.
//
// Order: CREATE structural → ALTER → CREATE dependent → DROP
//
// Structural objects (types, tables, functions, sequences, extensions, views)
// are created first so that ALTERs can reference them.
// Dependent objects (indexes, triggers, policies) are created after ALTERs
// because they may reference columns added by ALTER TABLE.
// GenerateDDL generates DDL statements for a diff.
//
// objectMap is the desired schema (used for CREATE ordering).
// actualObjectMap, if provided, is the current database schema (used for DROP
// ordering). When omitted, objectMap is used for both.
func (g *DDLGenerator) GenerateDDL(diff *differ.Diff, objectMap schema.SchemaObjectMap, actualObjectMap ...schema.SchemaObjectMap) (string, error) {
	var statements []string

	// Use actual schema for drop ordering if provided, otherwise fall back to desired.
	dropMap := objectMap
	strictDropMap := false
	var actualMap schema.SchemaObjectMap
	if len(actualObjectMap) > 0 && actualObjectMap[0] != nil {
		dropMap = actualObjectMap[0]
		actualMap = actualObjectMap[0]
		strictDropMap = true
	}

	// Split creates into structural (before ALTER) and dependent (after ALTER).
	var structuralKeys, dependentKeys []schema.ObjectKey
	for _, key := range diff.ToCreate {
		switch key.Kind {
		case schema.IndexKind, schema.TriggerKind, schema.PolicyKind:
			dependentKeys = append(dependentKeys, key)
		default:
			structuralKeys = append(structuralKeys, key)
		}
	}
	schema.SortObjectKeys(structuralKeys)
	schema.SortObjectKeys(dependentKeys)

	if err := validateOwnedSequenceState(diff, objectMap, actualMap, false); err != nil {
		return "", err
	}

	// 1. Create structural objects (types, tables, functions, etc.). Sequence
	// OWNED BY clauses are deferred because a table can itself reference the
	// sequence from an inline DEFAULT, while OWNED BY requires the target
	// table/column to already exist.
	var deferredCreateStatements []string
	if len(structuralKeys) > 0 {
		stmts, deferred, err := g.generateCreateStatementsPhased(structuralKeys, objectMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate CREATE statements: %w", err)
		}
		statements = append(statements, stmts...)
		deferredCreateStatements = append(deferredCreateStatements, deferred...)
	}

	// 2. ALTER existing objects (may add columns that new indexes reference)
	orderedAlters, cascadedSequenceOwners := orderAlterOperations(diff.ToAlter, objectMap)
	for _, alter := range orderedAlters {
		stmts, err := g.generateAlter(alter, cascadedSequenceOwners[alter.Key])
		if err != nil {
			return "", fmt.Errorf("failed to generate ALTER for %v: %w", alter.Key, err)
		}
		statements = append(statements, stmts...)
	}

	// 3. Create dependent objects (indexes, triggers, policies)
	if len(dependentKeys) > 0 {
		stmts, deferred, err := g.generateCreateStatementsPhased(dependentKeys, objectMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate dependent CREATE statements: %w", err)
		}
		statements = append(statements, stmts...)
		deferredCreateStatements = append(deferredCreateStatements, deferred...)
	}

	// 4. Bind newly-created sequences only after all target columns exist.
	statements = append(statements, deferredCreateStatements...)

	// 5. DROP statements (in reverse dependency order, using actual schema)
	if len(diff.ToDrop) > 0 {
		dropStatements, err := g.generateDropStatements(diff.ToDrop, dropMap, strictDropMap)
		if err != nil {
			return "", fmt.Errorf("failed to generate DROP statements: %w", err)
		}
		statements = append(statements, dropStatements...)
	}

	return strings.Join(statements, "\n\n"), nil
}

func validateOwnedSequenceState(diff *differ.Diff, desired, actual schema.SchemaObjectMap, validateAll bool) error {
	created := make(map[schema.ObjectKey]struct{}, len(diff.ToCreate))
	changed := make(map[schema.ObjectKey]struct{}, len(diff.ToCreate)+len(diff.ToAlter)+len(diff.ToDrop))
	dropped := make(map[schema.ObjectKey]struct{}, len(diff.ToDrop))
	ownerAltered := make(map[schema.ObjectKey]struct{})
	for _, key := range diff.ToCreate {
		created[key] = struct{}{}
		changed[key] = struct{}{}
	}
	for _, alter := range diff.ToAlter {
		changed[alter.Key] = struct{}{}
		if hasAlterChange(alter.Changes, "owner changed") {
			ownerAltered[alter.Key] = struct{}{}
		}
	}
	for _, key := range diff.ToDrop {
		changed[key] = struct{}{}
		dropped[key] = struct{}{}
	}

	for key, hashed := range desired {
		sequence, ok := hashed.Payload.(schema.Sequence)
		if !ok || sequence.OwnedBy == nil {
			continue
		}
		tableKey := schema.ObjectKey{
			Kind: schema.TableKind, Schema: sequence.OwnedBy.Schema, Name: string(sequence.OwnedBy.Table),
		}
		_, sequenceChanged := changed[key]
		_, tableChanged := changed[tableKey]
		if !validateAll && !sequenceChanged && !tableChanged {
			continue
		}
		if sequence.Schema != sequence.OwnedBy.Schema {
			return fmt.Errorf("sequence %s.%s cannot be OWNED BY table %s.%s: PostgreSQL requires a linked sequence and table to be in the same schema", sequence.Schema, sequence.Name, sequence.OwnedBy.Schema, sequence.OwnedBy.Table)
		}
		if _, targetDropped := dropped[tableKey]; targetDropped {
			return fmt.Errorf("sequence %s.%s cannot retain OWNED BY %s.%s because the target table is being dropped", sequence.Schema, sequence.Name, sequence.OwnedBy.Schema, sequence.OwnedBy.Table)
		}
		table, tableExists := tableFromMap(desired, tableKey)
		if !tableExists {
			table, tableExists = tableFromMap(actual, tableKey)
		}
		if !tableExists {
			return fmt.Errorf("sequence %s.%s OWNED BY target table %s.%s is not present in the desired or actual object map", sequence.Schema, sequence.Name, sequence.OwnedBy.Schema, sequence.OwnedBy.Table)
		}
		columnExists := false
		for _, column := range table.Columns {
			if column.Name == sequence.OwnedBy.Column {
				columnExists = true
				break
			}
		}
		if !columnExists {
			return fmt.Errorf("sequence %s.%s OWNED BY target column %s.%s.%s does not exist", sequence.Schema, sequence.Name, sequence.OwnedBy.Schema, sequence.OwnedBy.Table, sequence.OwnedBy.Column)
		}

		_, sequenceCreated := created[key]
		_, tableCreated := created[tableKey]
		_, sequenceOwnerAltered := ownerAltered[key]
		_, tableOwnerAltered := ownerAltered[tableKey]
		if !validateAll && !sequenceCreated && !tableCreated && !sequenceOwnerAltered && !tableOwnerAltered {
			continue
		}
		sequenceOwner := sequence.Owner
		tableOwner := table.Owner

		// Existing owned sequences follow an ALTER TABLE OWNER automatically.
		// A newly-created sequence is not bound yet, so it cannot benefit from
		// that cascade and must be assigned the same explicit owner itself.
		if sequenceOwner == nil && !sequenceCreated && table.Owner != nil {
			sequenceOwner = table.Owner
		}
		if sequenceOwner == nil && !sequenceCreated {
			if current, exists := sequenceFromMap(actual, key); exists {
				sequenceOwner = current.Owner
			}
		}
		if tableOwner == nil && !tableCreated {
			if current, exists := tableFromMap(actual, tableKey); exists {
				tableOwner = current.Owner
			}
		}

		switch {
		case sequenceOwner != nil && tableOwner != nil && *sequenceOwner != *tableOwner:
			return fmt.Errorf("sequence %s.%s owner %q must match OWNED BY table %s.%s owner %q", sequence.Schema, sequence.Name, *sequenceOwner, table.Schema, table.Name, *tableOwner)
		case sequenceCreated && tableCreated && (sequenceOwner == nil) != (tableOwner == nil):
			return fmt.Errorf("new sequence %s.%s and its OWNED BY table %s.%s must either both use the migration role or declare the same explicit owner", sequence.Schema, sequence.Name, table.Schema, table.Name)
		case sequenceCreated && !tableCreated && sequenceOwner == nil:
			return fmt.Errorf("new sequence %s.%s owned by existing table %s.%s requires an explicit owner matching the table owner", sequence.Schema, sequence.Name, table.Schema, table.Name)
		case sequenceOwner != nil && tableOwner == nil:
			return fmt.Errorf("cannot verify that sequence %s.%s owner %q matches unmanaged OWNED BY table %s.%s; provide the actual object map or manage both owners", sequence.Schema, sequence.Name, *sequenceOwner, table.Schema, table.Name)
		}
	}
	return nil
}

func tableFromMap(objects schema.SchemaObjectMap, key schema.ObjectKey) (schema.Table, bool) {
	if objects == nil {
		return schema.Table{}, false
	}
	hashed, ok := objects[key]
	if !ok {
		return schema.Table{}, false
	}
	table, ok := hashed.Payload.(schema.Table)
	return table, ok
}

func sequenceFromMap(objects schema.SchemaObjectMap, key schema.ObjectKey) (schema.Sequence, bool) {
	if objects == nil {
		return schema.Sequence{}, false
	}
	hashed, ok := objects[key]
	if !ok {
		return schema.Sequence{}, false
	}
	sequence, ok := hashed.Payload.(schema.Sequence)
	return sequence, ok
}

func orderAlterOperations(alters []differ.AlterOperation, desired schema.SchemaObjectMap) ([]differ.AlterOperation, map[schema.ObjectKey]bool) {
	prerequisiteTables := make(map[schema.ObjectKey]struct{})
	cascadedSequenceOwners := make(map[schema.ObjectKey]bool)
	tableAlters := make(map[schema.ObjectKey]differ.AlterOperation)
	for _, alter := range alters {
		if alter.Key.Kind == schema.TableKind && hasAlterChange(alter.Changes, "owner changed") {
			tableAlters[alter.Key] = alter
		}
	}
	for _, alter := range alters {
		if alter.Key.Kind != schema.SequenceKind || !hasAlterChange(alter.Changes, "owner changed") {
			continue
		}
		hashed, exists := desired[alter.Key]
		if !exists {
			continue
		}
		sequence, ok := hashed.Payload.(schema.Sequence)
		if !ok || sequence.OwnedBy == nil {
			continue
		}
		tableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: sequence.OwnedBy.Schema, Name: string(sequence.OwnedBy.Table)}
		if _, exists := tableAlters[tableKey]; exists {
			prerequisiteTables[tableKey] = struct{}{}
			cascadedSequenceOwners[alter.Key] = true
		}
	}

	ordered := make([]differ.AlterOperation, 0, len(alters))
	for _, alter := range alters {
		if _, prerequisite := prerequisiteTables[alter.Key]; prerequisite {
			ordered = append(ordered, alter)
		}
	}
	for _, alter := range alters {
		if _, prerequisite := prerequisiteTables[alter.Key]; !prerequisite {
			ordered = append(ordered, alter)
		}
	}
	return ordered, cascadedSequenceOwners
}

// generateCreateStatements generates CREATE statements in dependency order
func (g *DDLGenerator) generateCreateStatements(keys []schema.ObjectKey, objectMap schema.SchemaObjectMap) ([]string, error) {
	statements, deferred, err := g.generateCreateStatementsPhased(keys, objectMap)
	if err != nil {
		return nil, err
	}
	return append(statements, deferred...), nil
}

// generateCreateStatementsPhased returns immediately executable CREATE DDL
// separately from post-create bindings such as ALTER SEQUENCE ... OWNED BY.
func (g *DDLGenerator) generateCreateStatementsPhased(keys []schema.ObjectKey, objectMap schema.SchemaObjectMap) ([]string, []string, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}

	// Build a graph for only the objects being created
	graph := BuildGraph(objectMap)

	// Filter graph to only include objects being created
	filteredGraph := FilterGraphForKeys(graph, keys)

	// Topologically sort to respect dependencies
	sortedKeys, err := filteredGraph.TopologicalSort()
	if err != nil {
		// Circular dependency detected - provide helpful error
		cycle, _ := filteredGraph.DetectCycle()
		if cycle != nil {
			return nil, nil, fmt.Errorf("circular dependency detected: %s\nCannot determine creation order. Consider creating tables first without foreign keys, then adding foreign keys with ALTER TABLE", formatCycle(cycle))
		}
		return nil, nil, err
	}

	// Build set of keys that actually need CREATE (not just dependency-ordering nodes)
	createSet := make(map[schema.ObjectKey]bool, len(keys))
	for _, k := range keys {
		createSet[k] = true
	}

	// Generate CREATE statements in sorted order, skipping dependency-only nodes
	var statements, deferred []string
	for _, key := range sortedKeys {
		if !createSet[key] {
			continue // included only for ordering, already exists
		}
		obj, exists := objectMap[key]
		if !exists {
			return nil, nil, fmt.Errorf("CREATE object %v is missing from the desired object map", key)
		}
		payload := obj.Payload
		if sequence, ok := payload.(schema.Sequence); ok && sequence.OwnedBy != nil {
			ownedBy := sequence.OwnedBy
			sequence.OwnedBy = nil
			payload = sequence
			deferred = append(deferred, fmt.Sprintf(
				"ALTER SEQUENCE %s OWNED BY %s;",
				qualifiedName(string(sequence.Schema), sequence.Name),
				qualifiedColumnName(string(ownedBy.Schema), string(ownedBy.Table), string(ownedBy.Column)),
			))
		}
		stmt, err := g.generateCreate(payload)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate CREATE for %v: %w", key, err)
		}
		statements = append(statements, stmt)
	}

	return statements, deferred, nil
}

// GenerateCreateObjects renders a replayable, dependency-ordered schema dump.
// Unlike calling GenerateCreateStatement object-by-object, it can defer
// cross-object metadata until every prerequisite has been created.
func (g *DDLGenerator) GenerateCreateObjects(objects []schema.DatabaseObject) (string, error) {
	objectMap := make(schema.SchemaObjectMap, len(objects))
	firstIndex := make(map[schema.ObjectKey]int, len(objects))
	for index, object := range objects {
		key := schema.ObjectKeyFor(object)
		if key.Kind == "" {
			return "", fmt.Errorf("cannot render unrecognized database object %T at position %d", object, index+1)
		}
		if first, duplicate := firstIndex[key]; duplicate {
			return "", fmt.Errorf("duplicate schema object %v at positions %d and %d", key, first+1, index+1)
		}
		firstIndex[key] = index
		objectMap[key] = schema.HashedObject{Payload: object}
	}
	keys := make([]schema.ObjectKey, 0, len(objectMap))
	for key := range objectMap {
		keys = append(keys, key)
	}
	schema.SortObjectKeys(keys)
	if err := validateOwnedSequenceState(&differ.Diff{ToCreate: keys}, objectMap, nil, true); err != nil {
		return "", err
	}
	statements, err := g.generateCreateStatements(keys, objectMap)
	if err != nil {
		return "", err
	}
	if len(statements) == 0 {
		return "", nil
	}
	return strings.Join(statements, "\n\n") + "\n", nil
}

// generateDropStatements generates DROP statements in reverse dependency order
func (g *DDLGenerator) generateDropStatements(keys []schema.ObjectKey, objectMap schema.SchemaObjectMap, strict ...bool) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Build graph
	graph := BuildGraph(objectMap)

	// Filter to only objects being dropped
	filteredGraph := FilterGraphForKeys(graph, keys)

	// Reverse topological sort (drop dependents before dependencies)
	sortedKeys, err := filteredGraph.ReverseTopologicalSort()
	if err != nil {
		return nil, err
	}

	// Generate DROP statements in reverse dependency order
	var statements []string
	dropSet := make(map[schema.ObjectKey]bool, len(keys))
	for _, key := range keys {
		dropSet[key] = true
	}
	for _, key := range sortedKeys {
		if !dropSet[key] {
			continue // retained dependency included only to calculate ordering
		}
		hashed, exists := objectMap[key]
		if !exists {
			if len(strict) > 0 && strict[0] {
				return nil, fmt.Errorf("DROP object %v is missing from the actual object map", key)
			}
			stmt, err := g.generateDrop(key)
			if err != nil {
				return nil, fmt.Errorf("failed to generate DROP for %v: %w", key, err)
			}
			statements = append(statements, stmt)
			continue
		}
		stmt, err := g.generateDropObject(key, hashed.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to generate DROP for %v: %w", key, err)
		}
		statements = append(statements, stmt)
	}

	return statements, nil
}

// formatCycle formats a dependency cycle for error messages
func formatCycle(cycle []schema.ObjectKey) string {
	if len(cycle) == 0 {
		return ""
	}

	parts := make([]string, len(cycle))
	for i, key := range cycle {
		parts[i] = fmt.Sprintf("%s.%s", key.Schema, key.Name)
	}

	return strings.Join(parts, " → ")
}

// GenerateCreateStatement generates a CREATE statement for an object
func (g *DDLGenerator) GenerateCreateStatement(obj schema.DatabaseObject) (string, error) {
	return g.generateCreate(obj)
}

func (g *DDLGenerator) generateCreate(obj schema.DatabaseObject) (string, error) {
	if err := validateDatabaseObjectRenderInputs(obj); err != nil {
		return "", fmt.Errorf("invalid CREATE render input: %w", err)
	}
	switch v := obj.(type) {
	case schema.Table:
		return g.generateCreateTable(v), nil
	case schema.Index:
		return g.generateCreateIndex(v), nil
	case schema.View:
		return g.generateCreateView(v), nil
	case schema.Function:
		return g.generateCreateFunction(v), nil
	case schema.Sequence:
		return g.generateCreateSequence(v), nil
	case schema.EnumDef:
		return g.generateCreateEnum(v), nil
	case schema.DomainDef:
		return g.generateCreateDomain(v), nil
	case schema.CompositeDef:
		return g.generateCreateComposite(v), nil
	case schema.Extension:
		return g.generateCreateExtension(v), nil
	case schema.Trigger:
		return g.generateCreateTrigger(v), nil
	case schema.Policy:
		return g.generateCreatePolicy(v), nil
	case schema.Schema:
		return "", &capability.UnsupportedError{
			Stage:       capability.CreateStage,
			Family:      capability.SchemaFamily,
			Feature:     "schema definition",
			Object:      schema.ObjectKey{Kind: schema.SchemaKind, Schema: v.Name, Name: string(v.Name)},
			Reason:      "CREATE SCHEMA rendering is not implemented",
			Remediation: "use an explicit migration until schema rendering is supported",
		}
	default:
		return "", &capability.UnsupportedError{
			Stage:   capability.CreateStage,
			Family:  capability.Family(obj.GetObjectKind()),
			Feature: fmt.Sprintf("object type %T", obj),
			Reason:  "object type is not registered with the DDL renderer",
		}
	}
}

func (g *DDLGenerator) generateCreateTable(tbl schema.Table) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("CREATE TABLE %s (", qualifiedName(string(tbl.Schema), string(tbl.Name))))

	// Column order is semantic in PostgreSQL (including SELECT * and physical
	// tuple layout), so preserve the declaration order.
	var colDefs []string
	for _, col := range tbl.Columns {
		base := fmt.Sprintf("  %s %s", quotedIdentifier(string(col.Name)), col.Type)
		colParts := []string{base}

		if col.Collation != nil {
			colParts = append(colParts, fmt.Sprintf("COLLATE %s", qualifiedText(*col.Collation)))
		}

		if col.Identity != nil {
			colParts = append(colParts, formatIdentityClause(*col.Identity))
		} else if col.Generated != nil {
			colParts = append(colParts, formatGeneratedClause(*col.Generated))
		} else if col.Default != nil {
			colParts = append(colParts, fmt.Sprintf("DEFAULT %s", *col.Default))
		}

		if col.NotNull {
			colParts = append(colParts, "NOT NULL")
		}

		colDefs = append(colDefs, strings.Join(colParts, " "))

	}

	// Primary key
	if tbl.PrimaryKey != nil {
		pkCols := quotedColumnNames(tbl.PrimaryKey.Cols)
		pkClause := fmt.Sprintf("  PRIMARY KEY (%s)", pkCols)
		if tbl.PrimaryKey.Name != nil && *tbl.PrimaryKey.Name != "" {
			pkClause = fmt.Sprintf("  CONSTRAINT %s PRIMARY KEY (%s)", quotedIdentifier(*tbl.PrimaryKey.Name), pkCols)
		}
		pkClause += formatConstraintTiming(tbl.PrimaryKey.Deferrable, tbl.PrimaryKey.InitiallyDeferred)
		colDefs = append(colDefs, pkClause)
	}

	// Unique constraints
	for _, uq := range sortedUniques(tbl.Uniques) {
		uqCols := quotedColumnNames(uq.Cols)
		var uqClause string
		if uq.Name != "" {
			uqClause = fmt.Sprintf("  CONSTRAINT %s UNIQUE (%s)", quotedIdentifier(uq.Name), uqCols)
		} else {
			uqClause = fmt.Sprintf("  UNIQUE (%s)", uqCols)
		}
		if !uq.NullsDistinct {
			uqClause += " NULLS NOT DISTINCT"
		}
		uqClause += formatConstraintTiming(uq.Deferrable, uq.InitiallyDeferred)
		uqClause += formatConstraintValidation(uq.NotValid)
		colDefs = append(colDefs, uqClause)
	}

	// Check constraints
	for _, check := range sortedChecks(tbl.Checks) {
		if check.Name != "" {
			checkClause := fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", quotedIdentifier(check.Name), check.Expr)
			if check.NoInherit {
				checkClause += " NO INHERIT"
			}
			checkClause += formatConstraintTiming(check.Deferrable, check.InitiallyDeferred)
			checkClause += formatConstraintValidation(check.NotValid)
			colDefs = append(colDefs, checkClause)
		} else {
			checkClause := fmt.Sprintf("  CHECK (%s)", check.Expr)
			if check.NoInherit {
				checkClause += " NO INHERIT"
			}
			checkClause += formatConstraintTiming(check.Deferrable, check.InitiallyDeferred)
			checkClause += formatConstraintValidation(check.NotValid)
			colDefs = append(colDefs, checkClause)
		}
	}

	// Foreign keys
	for _, fk := range sortedForeignKeys(tbl.ForeignKeys) {
		fkDef := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
			quotedIdentifier(fk.Name), quotedColumnNames(fk.Cols),
			qualifiedName(string(fk.Ref.Schema), string(fk.Ref.Table)), quotedColumnNames(fk.Ref.Cols))
		switch fk.Match {
		case schema.MatchFull:
			fkDef += " MATCH FULL"
		case schema.MatchPartial:
			fkDef += " MATCH PARTIAL"
		}
		if fk.OnDelete != schema.NoAction {
			fkDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
		}
		if fk.OnUpdate != schema.NoAction {
			fkDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
		}
		fkDef += formatConstraintTiming(fk.Deferrable, fk.InitiallyDeferred)
		fkDef += formatConstraintValidation(fk.NotValid)
		colDefs = append(colDefs, fkDef)
	}

	parts = append(parts, strings.Join(colDefs, ",\n"))
	closeLine := ")"
	if len(tbl.RelOptions) > 0 {
		options := append([]string(nil), tbl.RelOptions...)
		sort.Strings(options)
		closeLine += fmt.Sprintf(" WITH (%s)", strings.Join(options, ", "))
	}
	closeLine += ";"
	parts = append(parts, closeLine)

	stmt := strings.Join(parts, "\n")

	return appendMetadata(stmt, tableCreateMetadata(tbl))
}

func (g *DDLGenerator) generateCreateIndex(idx schema.Index) string {
	uniqueStr := ""
	if idx.Unique {
		uniqueStr = "UNIQUE "
	}

	// Build key expressions with ordering
	keyExprs := make([]string, len(idx.KeyExprs))
	for i, key := range idx.KeyExprs {
		var parts []string
		parts = append(parts, string(key.Expr))

		if key.Collation != nil {
			parts = append(parts, fmt.Sprintf("COLLATE %s", qualifiedText(*key.Collation)))
		}
		if key.OpClass != nil {
			parts = append(parts, qualifiedText(*key.OpClass))
		}
		if key.Ordering != nil && *key.Ordering == schema.Desc {
			parts = append(parts, "DESC")
		}
		if key.NullsOrdering != nil {
			parts = append(parts, string(*key.NullsOrdering))
		}

		keyExprs[i] = strings.Join(parts, " ")
	}

	stmt := fmt.Sprintf("CREATE %sINDEX %s ON %s USING %s (%s)",
		uniqueStr, quotedIdentifier(idx.Name), qualifiedName(string(idx.Schema), string(idx.Table)),
		quotedIdentifier(string(idx.Method)), strings.Join(keyExprs, ", "))

	if len(idx.Include) > 0 {
		stmt += fmt.Sprintf(" INCLUDE (%s)", quotedColumnNames(idx.Include))
	}

	// PostgreSQL grammar requires INCLUDE before WHERE.
	if idx.Predicate != nil {
		predicate := strings.TrimSpace(string(*idx.Predicate))
		if shouldWrapPredicate(predicate) && !(strings.HasPrefix(predicate, "(") && strings.HasSuffix(predicate, ")")) {
			predicate = fmt.Sprintf("(%s)", predicate)
		}
		stmt += fmt.Sprintf(" WHERE %s", predicate)
	}

	stmt += ";"

	if idx.Comment != nil {
		stmt += "\n\n" + formatIndexCommentStatement(idx.Schema, idx.Name, idx.Comment)
	}

	return stmt
}

func (g *DDLGenerator) generateCreateView(view schema.View) string {
	viewType := ""
	if view.Type == schema.MaterializedView {
		viewType = "MATERIALIZED "
	}

	stmt := fmt.Sprintf("CREATE %sVIEW %s AS\n%s;",
		viewType, qualifiedName(string(view.Schema), view.Name), view.Definition.Query)
	return appendMetadata(stmt, viewCreateMetadata(view))
}

func (g *DDLGenerator) generateCreateFunction(fn schema.Function) string {
	return appendMetadata(g.renderFunction(fn, false), functionCreateMetadata(fn))
}

func (g *DDLGenerator) renderFunction(fn schema.Function, replace bool) string {
	// Build arguments
	args := make([]string, 0, len(fn.Args))
	for _, arg := range fn.Args {
		// PostgreSQL represents RETURNS TABLE columns as TABLE-mode function
		// parameters in its AST/catalog, but their SQL spelling belongs in the
		// RETURNS TABLE clause rather than the input argument list.
		if arg.Mode == schema.TableMode {
			continue
		}
		var argParts []string
		switch arg.Mode {
		case schema.OutMode:
			argParts = append(argParts, "OUT")
		case schema.InOutMode:
			argParts = append(argParts, "INOUT")
		case schema.VariadicMode:
			argParts = append(argParts, "VARIADIC")
		}
		if arg.Name != nil {
			argParts = append(argParts, quotedIdentifier(*arg.Name))
		}
		argParts = append(argParts, string(arg.Type))
		if arg.Default != nil {
			argParts = append(argParts, "DEFAULT", string(*arg.Default))
		}
		args = append(args, strings.Join(argParts, " "))
	}

	// Build returns clause
	returnsClause := ""
	switch ret := fn.Returns.(type) {
	case schema.ReturnsType:
		returnsClause = fmt.Sprintf("RETURNS %s", ret.Type)
	case schema.ReturnsTable:
		if len(ret.Columns) == 0 {
			returnsClause = "RETURNS TABLE ()"
		} else {
			cols := make([]string, len(ret.Columns))
			for i, col := range ret.Columns {
				cols[i] = fmt.Sprintf("%s %s", quotedIdentifier(col.Name), col.Type)
			}
			returnsClause = fmt.Sprintf("RETURNS TABLE (%s)", strings.Join(cols, ", "))
		}
	case schema.ReturnsSetOf:
		returnsClause = fmt.Sprintf("RETURNS SETOF %s", ret.Type)
	}

	createClause := "CREATE FUNCTION"
	if replace {
		createClause = "CREATE OR REPLACE FUNCTION"
	}
	dollarTag := functionDollarTag(fn.Body)
	stmt := fmt.Sprintf(`%s %s(%s)
%s
LANGUAGE %s
AS %s
%s
%s`, createClause, qualifiedName(string(fn.Schema), fn.Name), strings.Join(args, ", "), returnsClause, quotedIdentifier(string(fn.Language)), dollarTag, fn.Body, dollarTag)

	// Add function options after the body (if not defaults)
	// Default for plpgsql is VOLATILE, so we can omit it in most cases
	// But for completeness, we'll include it if specified
	if fn.Volatility != "" && fn.Volatility != schema.Volatile {
		stmt += fmt.Sprintf("\n%s", fn.Volatility)
	}
	if fn.Strict {
		stmt += "\nSTRICT"
	}
	if fn.SecurityDefiner {
		stmt += "\nSECURITY DEFINER"
	}
	if fn.Parallel != "" && fn.Parallel != schema.ParallelUnsafe {
		stmt += fmt.Sprintf("\nPARALLEL %s", fn.Parallel)
	}
	if fn.SearchPath != nil {
		if len(fn.SearchPath) == 0 {
			stmt += "\nSET search_path TO ''"
		} else {
			path := make([]string, len(fn.SearchPath))
			for i := range fn.SearchPath {
				path[i] = quotedIdentifier(string(fn.SearchPath[i]))
			}
			stmt += fmt.Sprintf("\nSET search_path TO %s", strings.Join(path, ", "))
		}
	}

	stmt += ";"
	return stmt
}

func functionDollarTag(body string) string {
	for i := 0; ; i++ {
		tag := "$schemata$"
		if i > 0 {
			tag = fmt.Sprintf("$schemata_%d$", i)
		}
		if !strings.Contains(body, tag) {
			return tag
		}
	}
}

func (g *DDLGenerator) generateCreateSequence(seq schema.Sequence) string {
	stmt := fmt.Sprintf("CREATE SEQUENCE %s", qualifiedName(string(seq.Schema), seq.Name))
	if seq.Type != "" {
		stmt += fmt.Sprintf(" AS %s", seq.Type)
	}

	if seq.Start != nil {
		stmt += fmt.Sprintf(" START %d", *seq.Start)
	}
	if seq.Increment != nil {
		stmt += fmt.Sprintf(" INCREMENT %d", *seq.Increment)
	}
	if seq.MinValue != nil {
		stmt += fmt.Sprintf(" MINVALUE %d", *seq.MinValue)
	}
	if seq.MaxValue != nil {
		stmt += fmt.Sprintf(" MAXVALUE %d", *seq.MaxValue)
	}
	if seq.Cache != nil {
		stmt += fmt.Sprintf(" CACHE %d", *seq.Cache)
	}
	if seq.Cycle {
		stmt += " CYCLE"
	}

	stmt += ";"
	return appendMetadata(stmt, sequenceCreateMetadata(seq))
}

func (g *DDLGenerator) generateCreateEnum(enum schema.EnumDef) string {
	values := make([]string, len(enum.Values))
	for i, v := range enum.Values {
		values[i] = quotedLiteral(v)
	}

	stmt := fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
		qualifiedName(string(enum.Schema), string(enum.Name)), strings.Join(values, ", "))
	return appendMetadata(stmt, typeCreateMetadata("TYPE", enum.Schema, enum.Name, enum.Owner, enum.Comment, enum.Grants))
}

func (g *DDLGenerator) generateCreateDomain(domain schema.DomainDef) string {
	stmt := fmt.Sprintf("CREATE DOMAIN %s AS %s", qualifiedName(string(domain.Schema), string(domain.Name)), domain.BaseType)

	if domain.Default != nil {
		stmt += fmt.Sprintf(" DEFAULT %s", *domain.Default)
	}
	if domain.NotNull {
		stmt += " NOT NULL"
	}
	if domain.Check != nil {
		stmt += fmt.Sprintf(" CHECK (%s)", *domain.Check)
	}

	stmt += ";"
	return appendMetadata(stmt, typeCreateMetadata("DOMAIN", domain.Schema, domain.Name, domain.Owner, domain.Comment, domain.Grants))
}

func (g *DDLGenerator) generateCreateComposite(composite schema.CompositeDef) string {
	attributes := make([]string, len(composite.Attributes))
	for i, attribute := range composite.Attributes {
		attributes[i] = fmt.Sprintf("%s %s", quotedIdentifier(attribute.Name), attribute.Type)
	}
	stmt := fmt.Sprintf("CREATE TYPE %s AS (%s);", qualifiedName(string(composite.Schema), string(composite.Name)), strings.Join(attributes, ", "))
	return appendMetadata(stmt, typeCreateMetadata("TYPE", composite.Schema, composite.Name, composite.Owner, composite.Comment, composite.Grants))
}

func (g *DDLGenerator) generateCreateExtension(ext schema.Extension) string {
	return fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", quotedIdentifier(ext.Name))
}

func (g *DDLGenerator) generateCreateTrigger(trig schema.Trigger) string {
	sev := sortedTriggerEvents(trig.Events)
	events := make([]string, len(sev))
	for i, event := range sev {
		events[i] = string(event)
	}

	rowClause := ""
	if trig.ForEachRow {
		rowClause = "FOR EACH ROW "
	}

	return fmt.Sprintf(`CREATE TRIGGER %s
%s %s ON %s
%sEXECUTE FUNCTION %s();`,
		quotedIdentifier(trig.Name), trig.Timing, strings.Join(events, " OR "),
		qualifiedName(string(trig.Schema), string(trig.Table)), rowClause,
		qualifiedName(string(trig.Function.Schema), trig.Function.Name))
}

func (g *DDLGenerator) generateCreatePolicy(pol schema.Policy) string {
	permissive := "PERMISSIVE"
	if !pol.Permissive {
		permissive = "RESTRICTIVE"
	}

	stmt := fmt.Sprintf("CREATE POLICY %s ON %s AS %s FOR %s",
		quotedIdentifier(pol.Name), qualifiedName(string(pol.Schema), string(pol.Table)), permissive, pol.For)

	if len(pol.To) > 0 {
		roles := sortedPolicyRoles(pol.To)
		for i := range roles {
			roles[i] = quotedRole(roles[i])
		}
		stmt += fmt.Sprintf(" TO %s", strings.Join(roles, ", "))
	}

	if pol.Using != nil {
		stmt += fmt.Sprintf(" USING (%s)", *pol.Using)
	}

	if pol.WithCheck != nil {
		stmt += fmt.Sprintf(" WITH CHECK (%s)", *pol.WithCheck)
	}

	stmt += ";"
	return stmt
}

func (g *DDLGenerator) generateAlter(alter differ.AlterOperation, sequenceOwnerCascaded bool) ([]string, error) {
	if err := validateObjectKeyRenderInputs(alter.Key); err != nil {
		return nil, fmt.Errorf("invalid ALTER object identity: %w", err)
	}
	if err := validateDatabaseObjectRenderInputs(alter.NewObject); err != nil {
		return nil, fmt.Errorf("invalid ALTER render input: %w", err)
	}
	// For now, implement basic ALTER TABLE operations
	// More sophisticated ALTERs will be added later

	switch obj := alter.NewObject.(type) {
	case schema.Table:
		// Pass both old and new table objects for proper constraint handling
		var oldTable *schema.Table
		if oldObj, ok := alter.OldObject.(schema.Table); ok {
			oldTable = &oldObj
		}
		return g.generateAlterTable(obj, oldTable, alter)
	case schema.View:
		if onlyOwnerOrGrantChanges(alter.Changes) {
			return g.generateAlterViewOwnerAndGrants(obj, alter)
		}
		oldView, ok := alter.OldObject.(schema.View)
		if !ok {
			return nil, fmt.Errorf("cannot preserve unmanaged metadata while replacing view %v without the prior view definition", alter.Key)
		}
		// A nil desired owner or ACL means unmanaged, not "replace with the
		// migration user's defaults". Structural view changes require DROP +
		// CREATE, so carry the observed metadata through the replacement.
		replacement := obj
		if replacement.Owner == nil {
			replacement.Owner = oldView.Owner
		}
		if replacement.Grants == nil {
			replacement.Grants = oldView.Grants
		}
		dropStmt, err := g.generateDropObject(alter.Key, alter.OldObject)
		if err != nil {
			return nil, err
		}
		createStmt, err := g.generateCreate(replacement)
		if err != nil {
			return nil, err
		}
		return []string{dropStmt, createStmt}, nil
	case schema.Function:
		return g.generateAlterFunction(obj, alter)
	case schema.Sequence:
		if onlyOwnerOrGrantChanges(alter.Changes) {
			return g.generateAlterSequenceOwnerAndGrants(obj, alter, sequenceOwnerCascaded)
		}
		return nil, &UnsupportedChangeError{
			Key: alter.Key, Change: strings.Join(alter.Changes, ", "),
			Remediation: "sequence structural changes require an explicit ALTER SEQUENCE migration so current state and dependencies are preserved",
		}
	case schema.EnumDef:
		return g.generateAlterEnum(obj, alter)
	case schema.DomainDef:
		if onlyOwnerOrGrantChanges(alter.Changes) {
			return g.generateAlterTypeMetadata("DOMAIN", obj.Schema, obj.Name, obj.Owner, obj.Comment, obj.Grants, alter)
		}
		return nil, &UnsupportedChangeError{
			Key: alter.Key, Change: strings.Join(alter.Changes, ", "),
			Remediation: "domain structural changes require an explicit migration that accounts for dependent columns and data",
		}
	case schema.CompositeDef:
		if onlyOwnerOrGrantChanges(alter.Changes) {
			return g.generateAlterTypeMetadata("TYPE", obj.Schema, obj.Name, obj.Owner, obj.Comment, obj.Grants, alter)
		}
		return nil, &UnsupportedChangeError{
			Key: alter.Key, Change: strings.Join(alter.Changes, ", "),
			Remediation: "composite structural changes require an explicit migration that accounts for dependent columns and functions",
		}
	default:
		// For other objects, drop and recreate
		dropStmt, err := g.generateDropObject(alter.Key, alter.OldObject)
		if err != nil {
			return nil, err
		}
		createStmt, err := g.generateCreate(alter.NewObject)
		if err != nil {
			return nil, err
		}
		return []string{dropStmt, createStmt}, nil
	}
}

func (g *DDLGenerator) generateAlterEnum(enum schema.EnumDef, alter differ.AlterOperation) ([]string, error) {
	oldEnum, ok := alter.OldObject.(schema.EnumDef)
	if !ok {
		return nil, &UnsupportedChangeError{
			Key:         alter.Key,
			Change:      strings.Join(alter.Changes, ", "),
			Remediation: "the previous enum definition is unavailable; write an explicit migration",
		}
	}
	if len(enum.Values) < len(oldEnum.Values) {
		return nil, &UnsupportedChangeError{
			Key:         alter.Key,
			Change:      "enum values removed",
			Remediation: "PostgreSQL cannot safely remove enum values in place; use an explicit data migration and replacement type",
		}
	}
	for i := range oldEnum.Values {
		if enum.Values[i] != oldEnum.Values[i] {
			return nil, &UnsupportedChangeError{
				Key:         alter.Key,
				Change:      "enum values reordered or renamed",
				Remediation: "use an explicit migration that accounts for existing rows and dependent objects",
			}
		}
	}

	statements := make([]string, 0, len(enum.Values)-len(oldEnum.Values)+4)
	replaceACL := enum.Grants != nil && hasAlterChange(alter.Changes, "owner changed")
	if hasAlterChange(alter.Changes, "owner changed") && enum.Owner != nil {
		statements = append(statements, fmt.Sprintf("ALTER TYPE %s OWNER TO %s;", qualifiedName(string(enum.Schema), string(enum.Name)), quotedRole(*enum.Owner)))
	}
	for _, value := range enum.Values[len(oldEnum.Values):] {
		statements = append(statements, fmt.Sprintf("ALTER TYPE %s ADD VALUE %s;",
			qualifiedName(string(enum.Schema), string(enum.Name)), quotedLiteral(value)))
	}
	if !plannerStringPtrEqual(enum.Comment, oldEnum.Comment) {
		if enum.Comment == nil {
			statements = append(statements, fmt.Sprintf("COMMENT ON TYPE %s IS NULL;", qualifiedName(string(enum.Schema), string(enum.Name))))
		} else {
			statements = append(statements, fmt.Sprintf("COMMENT ON TYPE %s IS %s;",
				qualifiedName(string(enum.Schema), string(enum.Name)), quotedLiteral(*enum.Comment)))
		}
	}
	for _, change := range alter.Changes {
		if strings.HasPrefix(change, "add grant\t") || strings.HasPrefix(change, "revoke grant\t") {
			if replaceACL {
				continue
			}
			revoke, grantee, privileges, grantable, ok := differ.ParseGrantChange(change)
			if !ok {
				return nil, fmt.Errorf("invalid encoded type grant change %q", change)
			}
			if revoke {
				statements = append(statements, formatTypeRevoke(enum.Schema, enum.Name, grantee, privileges, grantable))
			} else {
				statements = append(statements, formatTypeGrant(enum.Schema, enum.Name, grantee, privileges, grantable))
			}
		}
	}
	if replaceACL {
		statements = append(statements, typeManagedACLStatements(enum.Schema, enum.Name, enum.Owner, enum.Grants, oldEnum.Grants)...)
	}
	return statements, nil
}

func plannerStringPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func (g *DDLGenerator) generateAlterTable(tbl schema.Table, oldTable *schema.Table, alter differ.AlterOperation) ([]string, error) {
	if err := validateDatabaseObjectRenderInputs(tbl); err != nil {
		return nil, fmt.Errorf("invalid ALTER TABLE render input: %w", err)
	}
	if oldTable != nil {
		if err := validateDatabaseObjectRenderInputs(*oldTable); err != nil {
			return nil, fmt.Errorf("invalid prior TABLE render input: %w", err)
		}
	}
	var statements []string
	tableSQL := qualifiedName(string(tbl.Schema), string(tbl.Name))
	replaceACL := tbl.Grants != nil && hasAlterChange(alter.Changes, "owner changed")

	// Build column and constraint maps for quick lookup (new table)
	colMap := make(map[schema.ColumnName]schema.Column)
	for _, col := range tbl.Columns {
		colMap[col.Name] = col
	}

	// Build old constraint maps for dropping
	var oldPKName string
	oldUniqueMap := make(map[string]schema.UniqueConstraint)
	oldCheckMap := make(map[string]schema.CheckConstraint)
	oldFKMap := make(map[string]schema.ForeignKey)

	if oldTable != nil {
		if oldTable.PrimaryKey != nil && oldTable.PrimaryKey.Name != nil {
			oldPKName = *oldTable.PrimaryKey.Name
		}
		for _, uq := range oldTable.Uniques {
			oldUniqueMap[uq.Name] = uq
		}
		for _, ck := range oldTable.Checks {
			oldCheckMap[ck.Name] = ck
		}
		for _, fk := range oldTable.ForeignKeys {
			oldFKMap[fk.Name] = fk
		}
	}

	// Process each change and generate appropriate DDL
	for _, change := range alter.Changes {
		if change == "owner changed" {
			if tbl.Owner != nil {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s OWNER TO %s;", tableSQL, quotedRole(*tbl.Owner)))
			}
		} else if strings.HasPrefix(change, "add grant\t") || strings.HasPrefix(change, "revoke grant\t") {
			if replaceACL {
				continue
			}
			revoke, grantee, privs, grantable, ok := differ.ParseGrantChange(change)
			if !ok {
				return nil, fmt.Errorf("invalid encoded table grant change %q", change)
			}
			if revoke {
				statements = append(statements, formatTableRevoke(tbl, grantee, privs, grantable))
			} else {
				statements = append(statements, formatTableGrant(tbl, grantee, privs, grantable))
			}
		} else if change == "comment changed" {
			statements = append(statements, formatTableCommentStatement(tbl.Schema, tbl.Name, tbl.Comment))
		} else if strings.HasPrefix(change, "add column ") {
			colName := schema.ColumnName(strings.TrimPrefix(change, "add column "))
			if col, exists := colMap[colName]; exists {
				base := fmt.Sprintf("%s %s", quotedIdentifier(string(col.Name)), col.Type)
				colParts := []string{base}

				if col.Collation != nil {
					colParts = append(colParts, fmt.Sprintf("COLLATE %s", qualifiedText(*col.Collation)))
				}

				if col.Identity != nil {
					colParts = append(colParts, formatIdentityClause(*col.Identity))
				} else if col.Generated != nil {
					colParts = append(colParts, formatGeneratedClause(*col.Generated))
				} else if col.Default != nil {
					colParts = append(colParts, fmt.Sprintf("DEFAULT %s", *col.Default))
				}

				if col.NotNull {
					colParts = append(colParts, "NOT NULL")
				}

				statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", tableSQL, strings.Join(colParts, " ")))

				if col.Comment != nil {
					statements = append(statements, formatColumnCommentStatement(tbl.Schema, tbl.Name, col.Name, col.Comment))
				}
			}
		} else if strings.HasPrefix(change, "drop column ") {
			colName := strings.TrimPrefix(change, "drop column ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tableSQL, quotedIdentifier(colName)))
		} else if strings.HasPrefix(change, "alter column ") {
			// Parse "alter column <name>: <details>"
			parts := strings.SplitN(strings.TrimPrefix(change, "alter column "), ": ", 2)
			if len(parts) == 2 {
				colName := parts[0]
				changeDetail := parts[1]
				colStatements, err := g.generateColumnAlter(tbl, oldTable, colName, changeDetail)
				if err != nil {
					return nil, err
				}
				statements = append(statements, colStatements...)
			}
		} else if strings.HasPrefix(change, "add primary key") {
			if tbl.PrimaryKey != nil {
				pkName := "pkey"
				if tbl.PrimaryKey.Name != nil {
					pkName = *tbl.PrimaryKey.Name
				}
				pkStmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
					tableSQL, quotedIdentifier(pkName), quotedColumnNames(tbl.PrimaryKey.Cols))
				pkStmt += formatConstraintTiming(tbl.PrimaryKey.Deferrable, tbl.PrimaryKey.InitiallyDeferred)
				statements = append(statements, pkStmt+";")
			}
		} else if strings.HasPrefix(change, "drop primary key") {
			if oldPKName != "" {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
					tableSQL, quotedIdentifier(oldPKName)))
			} else {
				return nil, &UnsupportedChangeError{
					Key: schema.ObjectKey{
						Kind:      schema.TableKind,
						Schema:    tbl.Schema,
						Name:      string(tbl.Name),
						TableName: "",
					},
					Change:      change,
					Remediation: "The old primary key constraint name is not available. Manually drop the primary key using: ALTER TABLE " + string(tbl.Schema) + "." + string(tbl.Name) + " DROP CONSTRAINT <constraint_name>;",
				}
			}
		} else if strings.HasPrefix(change, "primary key columns changed") || strings.Contains(change, "primary key") && strings.Contains(change, "changed") {
			// Drop old primary key and add new one
			if oldPKName != "" {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
					tableSQL, quotedIdentifier(oldPKName)))
			}
			if tbl.PrimaryKey != nil {
				pkName := "pkey"
				if tbl.PrimaryKey.Name != nil {
					pkName = *tbl.PrimaryKey.Name
				}
				pkStmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
					tableSQL, quotedIdentifier(pkName), quotedColumnNames(tbl.PrimaryKey.Cols))
				pkStmt += formatConstraintTiming(tbl.PrimaryKey.Deferrable, tbl.PrimaryKey.InitiallyDeferred)
				statements = append(statements, pkStmt+";")
			}
		} else if strings.HasPrefix(change, "add unique constraint ") {
			constraintName := strings.TrimPrefix(change, "add unique constraint ")
			for _, uq := range tbl.Uniques {
				if uq.Name == constraintName {
					statements = append(statements, buildAddUniqueConstraintStatement(tbl, uq)+";")
					break
				}
			}
		} else if strings.HasPrefix(change, "drop unique constraint ") {
			constraintName := strings.TrimPrefix(change, "drop unique constraint ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
				tableSQL, quotedIdentifier(constraintName)))
		} else if strings.HasPrefix(change, "add check constraint ") {
			constraintName := strings.TrimPrefix(change, "add check constraint ")
			for _, ck := range tbl.Checks {
				if ck.Name == constraintName {
					statements = append(statements, buildAddCheckConstraintStatement(tbl, ck)+";")
					break
				}
			}
		} else if strings.HasPrefix(change, "drop check constraint ") {
			constraintName := strings.TrimPrefix(change, "drop check constraint ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
				tableSQL, quotedIdentifier(constraintName)))
		} else if strings.HasPrefix(change, "add foreign key ") {
			constraintName := strings.TrimPrefix(change, "add foreign key ")
			for _, fk := range tbl.ForeignKeys {
				if fk.Name == constraintName {
					statements = append(statements, buildAddForeignKeyStatement(tbl, fk)+";")
					break
				}
			}
		} else if strings.HasPrefix(change, "drop foreign key ") {
			constraintName := strings.TrimPrefix(change, "drop foreign key ")
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
				tableSQL, quotedIdentifier(constraintName)))
		} else if strings.HasPrefix(change, "unique constraint ") && strings.HasSuffix(change, " validation changed") {
			constraintName := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(change, "unique constraint "), " validation changed"))

			if constraintName != "" {
				if uq, ok := oldUniqueMap[constraintName]; ok && !uq.NotValid {
					// Old constraint existed; drop only if moving to NOT VALID
					if newUq := findUniqueConstraint(tbl.Uniques, constraintName); newUq != nil && newUq.NotValid {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
						statements = append(statements, buildAddUniqueConstraintStatement(tbl, *newUq)+";")
					} else {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
					}
				} else {
					if newUq := findUniqueConstraint(tbl.Uniques, constraintName); newUq != nil {
						if newUq.NotValid {
							statements = append(statements, buildAddUniqueConstraintStatement(tbl, *newUq)+";")
						} else {
							statements = append(statements, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
						}
					} else {
						return nil, &UnsupportedChangeError{
							Key: schema.ObjectKey{
								Kind:   schema.TableKind,
								Schema: tbl.Schema,
								Name:   string(tbl.Name),
							},
							Change:      fmt.Sprintf("unique constraint %s validation changed (constraint details not available)", constraintName),
							Remediation: "manually validate or recreate the constraint in a migration",
						}
					}
				}
			}
		} else if strings.Contains(change, "unique constraint") && strings.Contains(change, "changed") {
			// Modified unique constraint: drop and recreate
			// Parse "unique constraint <name> <property> changed"
			parts := strings.Fields(change)
			if len(parts) >= 3 {
				constraintName := parts[2]
				// Drop old
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
					tableSQL, quotedIdentifier(constraintName)))
				// Add new
				for _, uq := range tbl.Uniques {
					if uq.Name == constraintName {
						statements = append(statements, buildAddUniqueConstraintStatement(tbl, uq)+";")
						break
					}
				}
			}
		} else if strings.HasPrefix(change, "check constraint ") && strings.HasSuffix(change, " validation changed") {
			constraintName := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(change, "check constraint "), " validation changed"))

			if constraintName != "" {
				if ck, ok := oldCheckMap[constraintName]; ok && !ck.NotValid {
					if newCk := findCheckConstraint(tbl.Checks, constraintName); newCk != nil && newCk.NotValid {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
						statements = append(statements, buildAddCheckConstraintStatement(tbl, *newCk)+";")
					} else {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
					}
				} else {
					if newCk := findCheckConstraint(tbl.Checks, constraintName); newCk != nil {
						if newCk.NotValid {
							statements = append(statements, buildAddCheckConstraintStatement(tbl, *newCk)+";")
						} else {
							statements = append(statements, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
						}
					} else {
						return nil, &UnsupportedChangeError{
							Key: schema.ObjectKey{
								Kind:      schema.TableKind,
								Schema:    tbl.Schema,
								Name:      string(tbl.Name),
								TableName: "",
							},
							Change:      fmt.Sprintf("check constraint %s validation changed", constraintName),
							Remediation: "The check constraint details are not available in the old schema. Manually validate the constraint using: ALTER TABLE " + string(tbl.Schema) + "." + string(tbl.Name) + " VALIDATE CONSTRAINT " + constraintName + ";",
						}
					}
				}
			}
		} else if strings.Contains(change, "check constraint") && strings.Contains(change, "changed") {
			// Modified check constraint: drop and recreate
			parts := strings.Fields(change)
			if len(parts) >= 3 {
				constraintName := parts[2]
				// Drop old
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
					tableSQL, quotedIdentifier(constraintName)))
				// Add new
				for _, ck := range tbl.Checks {
					if ck.Name == constraintName {
						statements = append(statements, buildAddCheckConstraintStatement(tbl, ck)+";")
						break
					}
				}
			}
		} else if strings.HasPrefix(change, "foreign key ") && strings.HasSuffix(change, " validation changed") {
			constraintName := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(change, "foreign key "), " validation changed"))

			if constraintName != "" {
				if fk, ok := oldFKMap[constraintName]; ok && !fk.NotValid {
					if newFk := findForeignKey(tbl.ForeignKeys, constraintName); newFk != nil && newFk.NotValid {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
						statements = append(statements, buildAddForeignKeyStatement(tbl, *newFk)+";")
					} else {
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
					}
				} else {
					if newFk := findForeignKey(tbl.ForeignKeys, constraintName); newFk != nil {
						if newFk.NotValid {
							statements = append(statements, buildAddForeignKeyStatement(tbl, *newFk)+";")
						} else {
							statements = append(statements, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", tableSQL, quotedIdentifier(constraintName)))
						}
					} else {
						return nil, &UnsupportedChangeError{
							Key: schema.ObjectKey{
								Kind:      schema.TableKind,
								Schema:    tbl.Schema,
								Name:      string(tbl.Name),
								TableName: "",
							},
							Change:      fmt.Sprintf("foreign key %s validation changed", constraintName),
							Remediation: "The foreign key constraint details are not available in the old schema. Manually validate the constraint using: ALTER TABLE " + string(tbl.Schema) + "." + string(tbl.Name) + " VALIDATE CONSTRAINT " + constraintName + ";",
						}
					}
				}
			}
		} else if strings.Contains(change, "foreign key") && strings.Contains(change, "changed") {
			// Modified foreign key: drop and recreate
			parts := strings.Fields(change)
			if len(parts) >= 3 {
				constraintName := parts[2]
				// Drop old
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
					tableSQL, quotedIdentifier(constraintName)))
				// Add new
				for _, fk := range tbl.ForeignKeys {
					if fk.Name == constraintName {
						statements = append(statements, buildAddForeignKeyStatement(tbl, fk)+";")
						break
					}
				}
			}
		} else {
			// Fallback for other changes
			return nil, &UnsupportedChangeError{
				Key: schema.ObjectKey{
					Kind:      schema.TableKind,
					Schema:    tbl.Schema,
					Name:      string(tbl.Name),
					TableName: "",
				},
				Change:      change,
				Remediation: "This table change type is not yet supported by schemata. Review the diff output and apply the change manually.",
			}
		}
	}
	if replaceACL {
		if oldTable == nil {
			return nil, fmt.Errorf("cannot rebuild managed table ACL without the prior table definition")
		}
		statements = append(statements, tableManagedACLStatements(tbl, oldTable.Grants)...)
	}

	return statements, nil
}

func (g *DDLGenerator) generateColumnAlter(tbl schema.Table, oldTable *schema.Table, colName, changeDetail string) ([]string, error) {
	var statements []string
	tableName := qualifiedName(string(tbl.Schema), string(tbl.Name))
	columnName := quotedIdentifier(colName)

	var newCol *schema.Column
	for i := range tbl.Columns {
		if tbl.Columns[i].Name == schema.ColumnName(colName) {
			newCol = &tbl.Columns[i]
			break
		}
	}

	var oldCol *schema.Column
	if oldTable != nil {
		for i := range oldTable.Columns {
			if oldTable.Columns[i].Name == schema.ColumnName(colName) {
				oldCol = &oldTable.Columns[i]
				break
			}
		}
	}

	if strings.Contains(changeDetail, "type changed") {
		// The human-readable change string is not an authoritative SQL source.
		// Render the target type from the desired column object instead.
		if newCol == nil {
			return nil, &UnsupportedChangeError{
				Key:         schema.ObjectKey{Kind: schema.ColumnKind, Schema: tbl.Schema, Name: colName, TableName: tbl.Name},
				Change:      changeDetail,
				Remediation: "the desired column definition is unavailable; write an explicit migration",
			}
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
			tableName, columnName, newCol.Type))
	} else if changeDetail == "set not null" {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;",
			tableName, columnName))
	} else if changeDetail == "drop not null" {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
			tableName, columnName))
	} else if changeDetail == "default changed" {
		// Find the column to get the new default value
		if newCol != nil && newCol.Default != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
				tableName, columnName, *newCol.Default))
		} else {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
				tableName, columnName))
		}
	} else if changeDetail == "generated spec changed" {
		if oldCol != nil && oldCol.Generated != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP EXPRESSION;",
				tableName, columnName))
		}
		if newCol != nil && newCol.Generated != nil {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD %s;",
				tableName, columnName, formatGeneratedClause(*newCol.Generated)))
		}
	} else if changeDetail == "identity spec changed" {
		if oldCol != nil && newCol != nil && oldCol.Identity != nil && newCol.Identity != nil &&
			identityDefinitionEqualExceptMode(newCol.Type, *oldCol.Identity, *newCol.Identity) {
			mode := "BY DEFAULT"
			if newCol.Identity.Always {
				mode = "ALWAYS"
			}
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET GENERATED %s;",
				tableName, columnName, mode))
		} else {
			return nil, &UnsupportedChangeError{
				Key: schema.ObjectKey{
					Kind: schema.ColumnKind, Schema: tbl.Schema, Name: colName, TableName: tbl.Name, ColumnName: schema.ColumnName(colName),
				},
				Change:      changeDetail,
				Remediation: "Only switching an existing identity column between GENERATED ALWAYS and BY DEFAULT is changed automatically. Adding, removing, renaming, restarting, or changing identity sequence options requires an explicit migration that preserves the backing sequence and reviews existing values.",
			}
		}
	} else if changeDetail == "collation changed" {
		if newCol != nil {
			if newCol.Collation != nil {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s COLLATE %s;",
					tableName, columnName, newCol.Type, qualifiedText(*newCol.Collation)))
			} else {
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
					tableName, columnName, newCol.Type))
			}
		}
	} else if changeDetail == "comment changed" {
		var comment *string
		if newCol != nil {
			comment = newCol.Comment
		}
		statements = append(statements, formatColumnCommentStatement(tbl.Schema, tbl.Name, schema.ColumnName(colName), comment))
	} else {
		return nil, &UnsupportedChangeError{
			Key: schema.ObjectKey{
				Kind:      schema.ColumnKind,
				Schema:    tbl.Schema,
				Name:      string(colName),
				TableName: tbl.Name,
			},
			Change:      fmt.Sprintf("alter column %s: %s", colName, changeDetail),
			Remediation: "This column change type is not yet supported by schemata. Review the diff output and apply the change manually using ALTER TABLE " + tableName + " ALTER COLUMN " + colName + ";",
		}
	}

	return statements, nil
}

func findUniqueConstraint(constraints []schema.UniqueConstraint, name string) *schema.UniqueConstraint {
	for i := range constraints {
		if constraints[i].Name == name {
			return &constraints[i]
		}
	}
	return nil
}

func findCheckConstraint(constraints []schema.CheckConstraint, name string) *schema.CheckConstraint {
	for i := range constraints {
		if constraints[i].Name == name {
			return &constraints[i]
		}
	}
	return nil
}

func findForeignKey(constraints []schema.ForeignKey, name string) *schema.ForeignKey {
	for i := range constraints {
		if constraints[i].Name == name {
			return &constraints[i]
		}
	}
	return nil
}

func buildAddUniqueConstraintStatement(tbl schema.Table, uq schema.UniqueConstraint) string {
	stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
		qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedIdentifier(uq.Name), quotedColumnNames(uq.Cols))
	if !uq.NullsDistinct {
		stmt += " NULLS NOT DISTINCT"
	}
	stmt += formatConstraintTiming(uq.Deferrable, uq.InitiallyDeferred)
	stmt += formatConstraintValidation(uq.NotValid)
	return stmt
}

func buildAddCheckConstraintStatement(tbl schema.Table, ck schema.CheckConstraint) string {
	var stmt string
	if ck.Name != "" {
		stmt = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
			qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedIdentifier(ck.Name), ck.Expr)
	} else {
		stmt = fmt.Sprintf("ALTER TABLE %s ADD CHECK (%s)",
			qualifiedName(string(tbl.Schema), string(tbl.Name)), ck.Expr)
	}
	if ck.NoInherit {
		stmt += " NO INHERIT"
	}
	stmt += formatConstraintTiming(ck.Deferrable, ck.InitiallyDeferred)
	stmt += formatConstraintValidation(ck.NotValid)
	return stmt
}

func buildAddForeignKeyStatement(tbl schema.Table, fk schema.ForeignKey) string {
	stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedIdentifier(fk.Name), quotedColumnNames(fk.Cols),
		qualifiedName(string(fk.Ref.Schema), string(fk.Ref.Table)), quotedColumnNames(fk.Ref.Cols))

	switch fk.Match {
	case schema.MatchFull:
		stmt += " MATCH FULL"
	case schema.MatchPartial:
		stmt += " MATCH PARTIAL"
	}
	if fk.OnDelete != schema.NoAction {
		stmt += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}
	if fk.OnUpdate != schema.NoAction {
		stmt += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}
	stmt += formatConstraintTiming(fk.Deferrable, fk.InitiallyDeferred)
	stmt += formatConstraintValidation(fk.NotValid)
	return stmt
}

func formatGeneratedClause(spec schema.GeneratedSpec) string {
	mode := "STORED"
	if !spec.Stored {
		mode = "VIRTUAL"
	}
	return fmt.Sprintf("GENERATED ALWAYS AS (%s) %s", spec.Expr, mode)
}

func formatIdentityClause(spec schema.IdentitySpec) string {
	mode := "BY DEFAULT"
	if spec.Always {
		mode = "ALWAYS"
	}

	clause := fmt.Sprintf("GENERATED %s AS IDENTITY", mode)

	var options []string
	if spec.SequenceName != nil {
		options = append(options, "SEQUENCE NAME "+qualifiedName(string(spec.SequenceName.Schema), spec.SequenceName.Name))
	}
	if sequenceOptions := formatSequenceOptions(spec.SequenceOptions); sequenceOptions != "" {
		options = append(options, sequenceOptions)
	}
	if len(options) > 0 {
		clause += fmt.Sprintf(" (%s)", strings.Join(options, " "))
	}

	return clause
}

func identityDefinitionEqualExceptMode(columnType schema.TypeName, oldIdentity, newIdentity schema.IdentitySpec) bool {
	// A nil desired sequence name deliberately leaves PostgreSQL's generated
	// backing-sequence name unmanaged. Catalog extraction always has that name,
	// so it must not block an otherwise in-place ALWAYS/BY DEFAULT change.
	if newIdentity.SequenceName != nil && (oldIdentity.SequenceName == nil || *oldIdentity.SequenceName != *newIdentity.SequenceName) {
		return false
	}
	oldOptions := schema.NormalizeIdentityOptions(columnType, oldIdentity.SequenceOptions)
	newOptions := schema.NormalizeIdentityOptions(columnType, newIdentity.SequenceOptions)
	if len(oldOptions) != len(newOptions) {
		return false
	}
	for i := range oldOptions {
		if oldOptions[i] != newOptions[i] {
			return false
		}
	}
	return oldIdentity.Always != newIdentity.Always
}

func formatSequenceOptions(options []schema.SequenceOption) string {
	if len(options) == 0 {
		return ""
	}

	ordered := append([]schema.SequenceOption(nil), options...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Type == ordered[j].Type {
			if ordered[i].HasValue == ordered[j].HasValue {
				return ordered[i].Value < ordered[j].Value
			}
			return !ordered[i].HasValue && ordered[j].HasValue
		}
		return sequenceOptionOrder(ordered[i].Type) < sequenceOptionOrder(ordered[j].Type)
	})

	parts := make([]string, 0, len(ordered))
	for _, opt := range ordered {
		if opt.HasValue {
			parts = append(parts, fmt.Sprintf("%s %d", opt.Type, opt.Value))
		} else {
			parts = append(parts, opt.Type)
		}
	}

	return strings.Join(parts, " ")
}

func sequenceOptionOrder(optionType string) int {
	switch optionType {
	case "START WITH":
		return 0
	case "INCREMENT BY":
		return 1
	case "MINVALUE", "NO MINVALUE":
		return 2
	case "MAXVALUE", "NO MAXVALUE":
		return 3
	case "CACHE":
		return 4
	case "CYCLE", "NO CYCLE":
		return 5
	default:
		return 100
	}
}

func shouldWrapPredicate(predicate string) bool {
	upper := strings.ToUpper(predicate)
	return strings.Contains(upper, " IS ") ||
		strings.Contains(upper, " AND ") ||
		strings.Contains(upper, " OR ") ||
		strings.Contains(upper, " BETWEEN ") ||
		strings.Contains(upper, " LIKE ") ||
		strings.Contains(upper, " IN (")
}

func formatConstraintTiming(deferrable, initiallyDeferred bool) string {
	if !deferrable {
		return ""
	}
	if initiallyDeferred {
		return " DEFERRABLE INITIALLY DEFERRED"
	}
	return " DEFERRABLE INITIALLY IMMEDIATE"
}

func formatConstraintValidation(notValid bool) string {
	if !notValid {
		return ""
	}
	return " NOT VALID"
}

func formatTableCommentStatement(schemaName schema.SchemaName, tableName schema.TableName, comment *string) string {
	qualified := qualifiedName(string(schemaName), string(tableName))
	if comment == nil {
		return fmt.Sprintf("COMMENT ON TABLE %s IS NULL;", qualified)
	}
	return fmt.Sprintf("COMMENT ON TABLE %s IS %s;", qualified, quotedLiteral(*comment))
}

func formatColumnCommentStatement(schemaName schema.SchemaName, tableName schema.TableName, columnName schema.ColumnName, comment *string) string {
	qualified := qualifiedColumnName(string(schemaName), string(tableName), string(columnName))
	if comment == nil {
		return fmt.Sprintf("COMMENT ON COLUMN %s IS NULL;", qualified)
	}
	return fmt.Sprintf("COMMENT ON COLUMN %s IS %s;", qualified, quotedLiteral(*comment))
}

func formatIndexCommentStatement(schemaName schema.SchemaName, indexName string, comment *string) string {
	qualified := qualifiedName(string(schemaName), indexName)
	if comment == nil {
		return fmt.Sprintf("COMMENT ON INDEX %s IS NULL;", qualified)
	}
	return fmt.Sprintf("COMMENT ON INDEX %s IS %s;", qualified, quotedLiteral(*comment))
}

func (g *DDLGenerator) generateDrop(key schema.ObjectKey) (string, error) {
	return g.generateDropObject(key, nil)
}

func (g *DDLGenerator) generateDropObject(key schema.ObjectKey, object schema.DatabaseObject) (string, error) {
	if err := validateObjectKeyRenderInputs(key); err != nil {
		return "", fmt.Errorf("invalid DROP render input: %w", err)
	}
	qualified := qualifiedName(string(key.Schema), key.Name)
	switch key.Kind {
	case schema.TableKind:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s%s;", qualified, g.cascadeClause()), nil
	case schema.IndexKind:
		return fmt.Sprintf("DROP INDEX IF EXISTS %s;", qualified), nil
	case schema.ViewKind:
		if view, ok := object.(schema.View); ok && view.Type == schema.MaterializedView {
			return fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s%s;", qualified, g.cascadeClause()), nil
		}
		return fmt.Sprintf("DROP VIEW IF EXISTS %s%s;", qualified, g.cascadeClause()), nil
	case schema.FunctionKind:
		if key.Signature == "" {
			return "", &UnsupportedChangeError{
				Key:         key,
				Change:      "drop function without an identity signature",
				Remediation: "specify the function argument types explicitly",
			}
		}
		return fmt.Sprintf("DROP FUNCTION IF EXISTS %s%s%s;", qualified, key.Signature, g.cascadeClause()), nil
	case schema.SequenceKind:
		return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s%s;", qualified, g.cascadeClause()), nil
	case schema.TypeKind:
		if _, ok := object.(schema.DomainDef); ok {
			return fmt.Sprintf("DROP DOMAIN IF EXISTS %s%s;", qualified, g.cascadeClause()), nil
		}
		return fmt.Sprintf("DROP TYPE IF EXISTS %s%s;", qualified, g.cascadeClause()), nil
	case schema.TriggerKind:
		return fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;", quotedIdentifier(key.Name), qualifiedName(string(key.Schema), string(key.TableName))), nil
	case schema.PolicyKind:
		return fmt.Sprintf("DROP POLICY IF EXISTS %s ON %s;", quotedIdentifier(key.Name), qualifiedName(string(key.Schema), string(key.TableName))), nil
	default:
		return "", &capability.UnsupportedError{
			Stage:   capability.DropStage,
			Family:  capability.Family(key.Kind),
			Feature: "drop object",
			Object:  key,
			Reason:  "object kind is not registered with the DROP renderer",
		}
	}
}

func (g *DDLGenerator) cascadeClause() string {
	if g.allowCascade {
		return " CASCADE"
	}
	return ""
}
