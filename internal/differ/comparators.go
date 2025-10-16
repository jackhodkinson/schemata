package differ

import (
	"fmt"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// compareTables compares two table objects and returns a list of changes
func compareTables(desired, actual schema.Table) []string {
	var changes []string

	// Compare owner
	if !stringPtrEqual(desired.Owner, actual.Owner) {
		changes = append(changes, "owner changed")
	}

	// Compare columns
	desiredCols := make(map[schema.ColumnName]schema.Column)
	for _, col := range desired.Columns {
		desiredCols[col.Name] = col
	}

	actualCols := make(map[schema.ColumnName]schema.Column)
	for _, col := range actual.Columns {
		actualCols[col.Name] = col
	}

	// Find added columns
	for name := range desiredCols {
		if _, exists := actualCols[name]; !exists {
			changes = append(changes, fmt.Sprintf("add column %s", name))
		}
	}

	// Find dropped columns
	for name := range actualCols {
		if _, exists := desiredCols[name]; !exists {
			changes = append(changes, fmt.Sprintf("drop column %s", name))
		}
	}

	// Find altered columns
	for name, desiredCol := range desiredCols {
		if actualCol, exists := actualCols[name]; exists {
			colChanges := compareColumns(desiredCol, actualCol)
			for _, change := range colChanges {
				changes = append(changes, fmt.Sprintf("alter column %s: %s", name, change))
			}
		}
	}

	// Compare primary key
	pkChanges := comparePrimaryKeys(desired.PrimaryKey, actual.PrimaryKey)
	changes = append(changes, pkChanges...)

	// Compare unique constraints
	uniqueChanges := compareUniqueConstraints(desired.Uniques, actual.Uniques)
	changes = append(changes, uniqueChanges...)

	// Compare check constraints
	checkChanges := compareCheckConstraints(desired.Checks, actual.Checks)
	changes = append(changes, checkChanges...)

	// Compare foreign keys
	fkChanges := compareForeignKeys(desired.ForeignKeys, actual.ForeignKeys)
	changes = append(changes, fkChanges...)

	// Compare options
	if !stringSliceEqual(desired.RelOptions, actual.RelOptions) {
		changes = append(changes, "reloptions changed")
	}

	// Compare comment
	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// compareColumns compares two column objects
func compareColumns(desired, actual schema.Column) []string {
	var changes []string

	if desired.Type != actual.Type {
		changes = append(changes, fmt.Sprintf("type changed from %s to %s", actual.Type, desired.Type))
	}

	if desired.NotNull != actual.NotNull {
		if desired.NotNull {
			changes = append(changes, "set not null")
		} else {
			changes = append(changes, "drop not null")
		}
	}

	// Compare defaults
	if !exprPtrEqual(desired.Default, actual.Default) {
		changes = append(changes, "default changed")
	}

	// Compare generated specs
	if !generatedSpecEqual(desired.Generated, actual.Generated) {
		changes = append(changes, "generated spec changed")
	}

	// Compare identity specs
	if !identitySpecEqual(desired.Identity, actual.Identity) {
		changes = append(changes, "identity spec changed")
	}

	// Compare collation
	if !stringPtrEqual(desired.Collation, actual.Collation) {
		changes = append(changes, "collation changed")
	}

	// Compare comment
	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// comparePrimaryKeys compares primary key constraints
func comparePrimaryKeys(desired, actual *schema.PrimaryKey) []string {
	var changes []string

	if desired == nil && actual == nil {
		return changes
	}

	if desired == nil && actual != nil {
		changes = append(changes, "drop primary key")
		return changes
	}

	if desired != nil && actual == nil {
		changes = append(changes, "add primary key")
		return changes
	}

	// Both exist, compare details
	if !columnSliceEqual(desired.Cols, actual.Cols) {
		changes = append(changes, "primary key columns changed")
	}

	if desired.Deferrable != actual.Deferrable {
		changes = append(changes, "primary key deferrable changed")
	}

	if desired.InitiallyDeferred != actual.InitiallyDeferred {
		changes = append(changes, "primary key initially deferred changed")
	}

	return changes
}

// compareUniqueConstraints compares unique constraints
func compareUniqueConstraints(desired, actual []schema.UniqueConstraint) []string {
	var changes []string

	desiredMap := make(map[string]schema.UniqueConstraint)
	for _, uq := range desired {
		desiredMap[uq.Name] = uq
	}

	actualMap := make(map[string]schema.UniqueConstraint)
	for _, uq := range actual {
		actualMap[uq.Name] = uq
	}

	// Find added constraints
	for name := range desiredMap {
		if _, exists := actualMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("add unique constraint %s", name))
		}
	}

	// Find dropped constraints
	for name := range actualMap {
		if _, exists := desiredMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("drop unique constraint %s", name))
		}
	}

	// Find altered constraints
	for name, desiredUq := range desiredMap {
		if actualUq, exists := actualMap[name]; exists {
			if !columnSliceEqual(desiredUq.Cols, actualUq.Cols) {
				changes = append(changes, fmt.Sprintf("unique constraint %s columns changed", name))
			}
			if desiredUq.NullsDistinct != actualUq.NullsDistinct {
				changes = append(changes, fmt.Sprintf("unique constraint %s nulls distinct changed", name))
			}
			if desiredUq.Deferrable != actualUq.Deferrable {
				changes = append(changes, fmt.Sprintf("unique constraint %s deferrable changed", name))
			}
			if desiredUq.InitiallyDeferred != actualUq.InitiallyDeferred {
				changes = append(changes, fmt.Sprintf("unique constraint %s initially deferred changed", name))
			}
		}
	}

	return changes
}

// compareCheckConstraints compares check constraints
func compareCheckConstraints(desired, actual []schema.CheckConstraint) []string {
	var changes []string

	desiredMap := make(map[string]schema.CheckConstraint)
	for _, ck := range desired {
		desiredMap[ck.Name] = ck
	}

	actualMap := make(map[string]schema.CheckConstraint)
	for _, ck := range actual {
		actualMap[ck.Name] = ck
	}

	// Find added constraints
	for name := range desiredMap {
		if _, exists := actualMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("add check constraint %s", name))
		}
	}

	// Find dropped constraints
	for name := range actualMap {
		if _, exists := desiredMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("drop check constraint %s", name))
		}
	}

	// Find altered constraints
	for name, desiredCk := range desiredMap {
		if actualCk, exists := actualMap[name]; exists {
			if desiredCk.Expr != actualCk.Expr {
				changes = append(changes, fmt.Sprintf("check constraint %s expression changed", name))
			}
			if desiredCk.NoInherit != actualCk.NoInherit {
				changes = append(changes, fmt.Sprintf("check constraint %s no inherit changed", name))
			}
			if desiredCk.Deferrable != actualCk.Deferrable {
				changes = append(changes, fmt.Sprintf("check constraint %s deferrable changed", name))
			}
			if desiredCk.InitiallyDeferred != actualCk.InitiallyDeferred {
				changes = append(changes, fmt.Sprintf("check constraint %s initially deferred changed", name))
			}
		}
	}

	return changes
}

// compareForeignKeys compares foreign key constraints
func compareForeignKeys(desired, actual []schema.ForeignKey) []string {
	var changes []string

	desiredMap := make(map[string]schema.ForeignKey)
	for _, fk := range desired {
		desiredMap[fk.Name] = fk
	}

	actualMap := make(map[string]schema.ForeignKey)
	for _, fk := range actual {
		actualMap[fk.Name] = fk
	}

	// Find added constraints
	for name := range desiredMap {
		if _, exists := actualMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("add foreign key %s", name))
		}
	}

	// Find dropped constraints
	for name := range actualMap {
		if _, exists := desiredMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("drop foreign key %s", name))
		}
	}

	// Find altered constraints
	for name, desiredFk := range desiredMap {
		if actualFk, exists := actualMap[name]; exists {
			if !columnSliceEqual(desiredFk.Cols, actualFk.Cols) {
				changes = append(changes, fmt.Sprintf("foreign key %s columns changed", name))
			}
			if !foreignKeyRefEqual(desiredFk.Ref, actualFk.Ref) {
				changes = append(changes, fmt.Sprintf("foreign key %s reference changed", name))
			}
			if desiredFk.OnUpdate != actualFk.OnUpdate {
				changes = append(changes, fmt.Sprintf("foreign key %s on update changed", name))
			}
			if desiredFk.OnDelete != actualFk.OnDelete {
				changes = append(changes, fmt.Sprintf("foreign key %s on delete changed", name))
			}
			if desiredFk.Match != actualFk.Match {
				changes = append(changes, fmt.Sprintf("foreign key %s match type changed", name))
			}
			if desiredFk.Deferrable != actualFk.Deferrable {
				changes = append(changes, fmt.Sprintf("foreign key %s deferrable changed", name))
			}
			if desiredFk.InitiallyDeferred != actualFk.InitiallyDeferred {
				changes = append(changes, fmt.Sprintf("foreign key %s initially deferred changed", name))
			}
		}
	}

	return changes
}

// compareIndexes compares two index objects
func compareIndexes(desired, actual schema.Index) []string {
	var changes []string

	if desired.Unique != actual.Unique {
		changes = append(changes, "uniqueness changed")
	}

	if desired.Method != actual.Method {
		changes = append(changes, fmt.Sprintf("method changed from %s to %s", actual.Method, desired.Method))
	}

	// Compare key expressions
	if len(desired.KeyExprs) != len(actual.KeyExprs) {
		changes = append(changes, "key expressions changed")
	} else {
		for i := range desired.KeyExprs {
			if !indexKeyExprEqual(desired.KeyExprs[i], actual.KeyExprs[i]) {
				changes = append(changes, "key expressions changed")
				break
			}
		}
	}

	// Compare predicate
	if !exprPtrEqual(desired.Predicate, actual.Predicate) {
		changes = append(changes, "predicate changed")
	}

	// Compare include columns
	if !columnSliceEqual(desired.Include, actual.Include) {
		changes = append(changes, "include columns changed")
	}

	// Compare comment
	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// compareViews compares two view objects
func compareViews(desired, actual schema.View) []string {
	var changes []string

	if desired.Definition.Query != actual.Definition.Query {
		changes = append(changes, "definition changed")
	}

	if desired.Type != actual.Type {
		changes = append(changes, "view type changed")
	}

	if desired.SecurityBarrier != actual.SecurityBarrier {
		changes = append(changes, "security barrier changed")
	}

	if !checkOptionPtrEqual(desired.CheckOption, actual.CheckOption) {
		changes = append(changes, "check option changed")
	}

	if !stringPtrEqual(desired.Owner, actual.Owner) {
		changes = append(changes, "owner changed")
	}

	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// compareFunctions compares two function objects
func compareFunctions(desired, actual schema.Function) []string {
	var changes []string

	if desired.Language != actual.Language {
		changes = append(changes, "language changed")
	}

	if desired.Volatility != actual.Volatility {
		changes = append(changes, "volatility changed")
	}

	if desired.Body != actual.Body {
		changes = append(changes, "body changed")
	}

	// Compare arguments
	if len(desired.Args) != len(actual.Args) {
		changes = append(changes, "arguments changed")
	} else {
		for i := range desired.Args {
			if !functionArgEqual(desired.Args[i], actual.Args[i]) {
				changes = append(changes, "arguments changed")
				break
			}
		}
	}

	// Compare returns
	if !functionReturnEqual(desired.Returns, actual.Returns) {
		changes = append(changes, "return type changed")
	}

	if desired.Strict != actual.Strict {
		changes = append(changes, "strict changed")
	}

	if desired.SecurityDefiner != actual.SecurityDefiner {
		changes = append(changes, "security definer changed")
	}

	if desired.Parallel != actual.Parallel {
		changes = append(changes, "parallel safety changed")
	}

	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// compareSequences compares two sequence objects
func compareSequences(desired, actual schema.Sequence) []string {
	var changes []string

	if desired.Type != actual.Type {
		changes = append(changes, "type changed")
	}

	if !int64PtrEqual(desired.Start, actual.Start) {
		changes = append(changes, "start value changed")
	}

	if !int64PtrEqual(desired.Increment, actual.Increment) {
		changes = append(changes, "increment changed")
	}

	if !int64PtrEqual(desired.MinValue, actual.MinValue) {
		changes = append(changes, "min value changed")
	}

	if !int64PtrEqual(desired.MaxValue, actual.MaxValue) {
		changes = append(changes, "max value changed")
	}

	if !int64PtrEqual(desired.Cache, actual.Cache) {
		changes = append(changes, "cache changed")
	}

	if desired.Cycle != actual.Cycle {
		changes = append(changes, "cycle changed")
	}

	if !sequenceOwnerEqual(desired.OwnedBy, actual.OwnedBy) {
		changes = append(changes, "owned by changed")
	}

	return changes
}

// compareEnums compares two enum type objects
func compareEnums(desired, actual schema.EnumDef) []string {
	var changes []string

	// Enums: order matters, can only safely add values at the end
	if len(desired.Values) != len(actual.Values) {
		// Check if values were only added at the end
		if len(desired.Values) > len(actual.Values) {
			allMatch := true
			for i, v := range actual.Values {
				if v != desired.Values[i] {
					allMatch = false
					break
				}
			}
			if allMatch {
				changes = append(changes, "enum values added at end")
			} else {
				changes = append(changes, "enum values changed (unsafe)")
			}
		} else {
			changes = append(changes, "enum values removed (unsafe)")
		}
		return changes
	}

	for i, v := range desired.Values {
		if v != actual.Values[i] {
			changes = append(changes, fmt.Sprintf("enum value %d changed", i))
		}
	}

	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// compareDomains compares two domain type objects
func compareDomains(desired, actual schema.DomainDef) []string {
	var changes []string

	if desired.BaseType != actual.BaseType {
		changes = append(changes, "base type changed")
	}

	if desired.NotNull != actual.NotNull {
		changes = append(changes, "not null constraint changed")
	}

	if !exprPtrEqual(desired.Default, actual.Default) {
		changes = append(changes, "default changed")
	}

	if !exprPtrEqual(desired.Check, actual.Check) {
		changes = append(changes, "check constraint changed")
	}

	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// compareComposites compares two composite type objects
func compareComposites(desired, actual schema.CompositeDef) []string {
	var changes []string

	desiredMap := make(map[string]schema.CompositeAttr)
	for _, attr := range desired.Attributes {
		desiredMap[attr.Name] = attr
	}

	actualMap := make(map[string]schema.CompositeAttr)
	for _, attr := range actual.Attributes {
		actualMap[attr.Name] = attr
	}

	// Check for added/removed attributes
	for name := range desiredMap {
		if _, exists := actualMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("add attribute %s", name))
		}
	}

	for name := range actualMap {
		if _, exists := desiredMap[name]; !exists {
			changes = append(changes, fmt.Sprintf("drop attribute %s", name))
		}
	}

	// Check for type changes in common attributes
	for name, desiredAttr := range desiredMap {
		if actualAttr, exists := actualMap[name]; exists {
			if desiredAttr.Type != actualAttr.Type {
				changes = append(changes, fmt.Sprintf("attribute %s type changed", name))
			}
		}
	}

	if !stringPtrEqual(desired.Comment, actual.Comment) {
		changes = append(changes, "comment changed")
	}

	return changes
}

// compareTriggers compares two trigger objects
func compareTriggers(desired, actual schema.Trigger) []string {
	var changes []string

	if desired.Timing != actual.Timing {
		changes = append(changes, "timing changed")
	}

	if !triggerEventSliceEqual(desired.Events, actual.Events) {
		changes = append(changes, "events changed")
	}

	if desired.ForEachRow != actual.ForEachRow {
		changes = append(changes, "for each row changed")
	}

	if !exprPtrEqual(desired.When, actual.When) {
		changes = append(changes, "when condition changed")
	}

	if !qualifiedNameEqual(desired.Function, actual.Function) {
		changes = append(changes, "function changed")
	}

	if desired.Enabled != actual.Enabled {
		changes = append(changes, "enabled status changed")
	}

	return changes
}

// comparePolicies compares two policy objects
func comparePolicies(desired, actual schema.Policy) []string {
	var changes []string

	if desired.Permissive != actual.Permissive {
		changes = append(changes, "permissive/restrictive changed")
	}

	if desired.For != actual.For {
		changes = append(changes, "policy command changed")
	}

	if !stringSliceEqual(desired.To, actual.To) {
		changes = append(changes, "roles changed")
	}

	if !exprPtrEqual(desired.Using, actual.Using) {
		changes = append(changes, "using expression changed")
	}

	if !exprPtrEqual(desired.WithCheck, actual.WithCheck) {
		changes = append(changes, "with check expression changed")
	}

	return changes
}

// Helper comparison functions

func stringPtrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func exprPtrEqual(a, b *schema.Expr) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func int64PtrEqual(a, b *int64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func columnSliceEqual(a, b []schema.ColumnName) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func generatedSpecEqual(a, b *schema.GeneratedSpec) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Expr == b.Expr && a.Stored == b.Stored
}

func identitySpecEqual(a, b *schema.IdentitySpec) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Always == b.Always
}

func foreignKeyRefEqual(a, b schema.ForeignKeyRef) bool {
	return a.Schema == b.Schema &&
		a.Table == b.Table &&
		columnSliceEqual(a.Cols, b.Cols)
}

func indexKeyExprEqual(a, b schema.IndexKeyExpr) bool {
	return a.Expr == b.Expr &&
		stringPtrEqual(a.Collation, b.Collation) &&
		stringPtrEqual(a.OpClass, b.OpClass)
}

func checkOptionPtrEqual(a, b *schema.CheckOption) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func functionArgEqual(a, b schema.FunctionArg) bool {
	return a.Mode == b.Mode &&
		stringPtrEqual(a.Name, b.Name) &&
		a.Type == b.Type &&
		exprPtrEqual(a.Default, b.Default)
}

func functionReturnEqual(a, b schema.FunctionReturn) bool {
	switch aRet := a.(type) {
	case schema.ReturnsType:
		if bRet, ok := b.(schema.ReturnsType); ok {
			return aRet.Type == bRet.Type
		}
	case schema.ReturnsTable:
		if bRet, ok := b.(schema.ReturnsTable); ok {
			if len(aRet.Columns) != len(bRet.Columns) {
				return false
			}
			for i := range aRet.Columns {
				if aRet.Columns[i].Name != bRet.Columns[i].Name ||
					aRet.Columns[i].Type != bRet.Columns[i].Type {
					return false
				}
			}
			return true
		}
	case schema.ReturnsSetOf:
		if bRet, ok := b.(schema.ReturnsSetOf); ok {
			return aRet.Type == bRet.Type
		}
	}
	return false
}

func sequenceOwnerEqual(a, b *schema.SequenceOwner) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Schema == b.Schema && a.Table == b.Table && a.Column == b.Column
}

func triggerEventSliceEqual(a, b []schema.TriggerEvent) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func qualifiedNameEqual(a, b schema.QualifiedName) bool {
	return a.Schema == b.Schema && a.Name == b.Name
}
