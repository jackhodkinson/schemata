package schema

import "sort"

var objectKindRank = map[ObjectKind]int{
	SchemaKind:     0,
	ExtensionKind:  1,
	TypeKind:       2,
	SequenceKind:   3,
	TableKind:      4,
	ColumnKind:     5,
	ConstraintKind: 6,
	FunctionKind:   7,
	ViewKind:       8,
	IndexKind:      9,
	TriggerKind:    10,
	PolicyKind:     11,
	GrantKind:      12,
	OwnerKind:      13,
}

// ObjectKeyLess provides deterministic ordering for object keys across the
// parser, differ, planner, and CLI output.
func ObjectKeyLess(a, b ObjectKey) bool {
	ar, aok := objectKindRank[a.Kind]
	br, bok := objectKindRank[b.Kind]
	switch {
	case aok && bok && ar != br:
		return ar < br
	case aok != bok:
		// Known kinds come before unknown kinds.
		return aok
	}

	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.Schema != b.Schema {
		return a.Schema < b.Schema
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	if a.TableName != b.TableName {
		return a.TableName < b.TableName
	}
	if a.ColumnName != b.ColumnName {
		return a.ColumnName < b.ColumnName
	}
	return a.Signature < b.Signature
}

// SortObjectKeys sorts keys in place using ObjectKeyLess.
func SortObjectKeys(keys []ObjectKey) {
	sort.Slice(keys, func(i, j int) bool {
		return ObjectKeyLess(keys[i], keys[j])
	})
}
