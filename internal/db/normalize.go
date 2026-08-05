package db

import (
	"reflect"

	"github.com/jackhodkinson/schemata/internal/sqlrender"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

type ownedColumnKey struct {
	Schema schema.SchemaName
	Table  schema.TableName
	Column schema.ColumnName
}

// NormalizeTable is retained for package callers and tests. Catalog extraction
// uses normalizeCatalogTable so the pg_depend kind remains available and an
// IDENTITY sequence can never be mistaken for SERIAL.
func NormalizeTable(tbl schema.Table, sequences []schema.Sequence) schema.Table {
	catalogSequences := make([]catalogSequence, len(sequences))
	for i, seq := range sequences {
		kind := sequenceStandalone
		if seq.OwnedBy != nil {
			kind = sequenceSerial
		}
		catalogSequences[i] = catalogSequence{Sequence: seq, DependencyKind: kind}
	}
	normalized, _ := normalizeCatalogTable(tbl, catalogSequences)
	return normalized
}

// normalizeCatalogTable collapses only sequences that are exactly equivalent
// to PostgreSQL's SERIAL expansion. It returns the identities of sequences that
// are safe to omit from the standalone object list.
func normalizeCatalogTable(tbl schema.Table, sequences []catalogSequence) (schema.Table, map[schema.ObjectKey]struct{}) {
	owned := make(map[ownedColumnKey]catalogSequence)
	for _, catalogSeq := range sequences {
		if catalogSeq.DependencyKind != sequenceSerial || catalogSeq.Sequence.OwnedBy == nil {
			continue
		}
		owner := catalogSeq.Sequence.OwnedBy
		owned[ownedColumnKey{Schema: owner.Schema, Table: owner.Table, Column: owner.Column}] = catalogSeq
	}

	collapsed := make(map[schema.ObjectKey]struct{})
	for i := range tbl.Columns {
		column := tbl.Columns[i]
		key := ownedColumnKey{Schema: tbl.Schema, Table: tbl.Name, Column: column.Name}
		if catalogSeq, ok := owned[key]; ok && column.Default != nil {
			if serialType := canonicalSerialType(tbl, column, catalogSeq.Sequence); serialType != "" {
				column.Type = serialType
				column.Default = nil
				collapsed[schema.ObjectKey{Kind: schema.SequenceKind, Schema: catalogSeq.Sequence.Schema, Name: catalogSeq.Sequence.Name}] = struct{}{}
				tbl.Columns[i] = column
				continue
			}
		}

		column.Type = schema.NormalizeTypeName(column.Type)
		tbl.Columns[i] = column
	}

	return tbl, collapsed
}

func canonicalSerialType(tbl schema.Table, col schema.Column, seq schema.Sequence) schema.TypeName {
	serialType, sequenceType, sequenceMax, ok := serialDefaults(col.Type)
	if !ok || col.Default == nil || seq.OwnedBy == nil {
		return ""
	}
	if seq.OwnedBy.Schema != tbl.Schema || seq.OwnedBy.Table != tbl.Name || seq.OwnedBy.Column != col.Name {
		return ""
	}
	if seq.Name != string(tbl.Name)+"_"+string(col.Name)+"_seq" {
		// PostgreSQL truncation and collision suffixes are semantically observable.
		// Preserve those sequences explicitly rather than guessing a generated name.
		return ""
	}
	if schema.NormalizeTypeName(schema.TypeName(seq.Type)) != sequenceType {
		return ""
	}
	if !int64PtrHasValue(seq.Start, 1) ||
		!int64PtrHasValue(seq.Increment, 1) ||
		!int64PtrHasValue(seq.MinValue, 1) ||
		!int64PtrHasValue(seq.MaxValue, sequenceMax) ||
		!int64PtrHasValue(seq.Cache, 1) ||
		seq.Cycle {
		return ""
	}
	if tbl.Owner == nil || seq.Owner == nil || *tbl.Owner != *seq.Owner {
		return ""
	}
	if seq.Comment != nil || !hasDefaultSequenceACL(seq) {
		return ""
	}
	if !referencesSequence(*col.Default, seq.Schema, seq.Name) {
		return ""
	}
	return serialType
}

func serialDefaults(columnType schema.TypeName) (schema.TypeName, schema.TypeName, int64, bool) {
	switch schema.NormalizeTypeName(columnType) {
	case "smallint":
		return "smallserial", "smallint", 32767, true
	case "integer":
		return "serial", "integer", 2147483647, true
	case "bigint":
		return "bigserial", "bigint", 9223372036854775807, true
	default:
		return "", "", 0, false
	}
}

func int64PtrHasValue(value *int64, expected int64) bool {
	return value != nil && *value == expected
}

func hasDefaultSequenceACL(seq schema.Sequence) bool {
	if seq.Owner == nil {
		return false
	}
	// This mirrors PostgreSQL's acldefault('s', owner), which is what catalog
	// extraction expands when relacl is NULL.
	want := schema.CanonicalizeGrants([]schema.Grant{{
		Grantee:    schema.RoleGrantee(*seq.Owner),
		Privileges: []schema.Privilege{schema.PrivSelect, schema.PrivUpdate, schema.PrivUsage},
	}})
	got := schema.CanonicalizeGrants(seq.Grants)
	return reflect.DeepEqual(got, want)
}

// referencesSequence recognizes only an exact nextval(regclass) reference. A
// substring check can turn nextval('not_id_seq') into a reference to id_seq and
// silently change which sequence supplies values after a round trip.
func referencesSequence(expr schema.Expr, seqSchema schema.SchemaName, seqName string) bool {
	reference, qualified, ok := sqlrender.NextvalRegclassReference(string(expr))
	if !ok {
		return false
	}
	if qualified {
		return reference == sqlrender.Qualified(string(seqSchema), seqName)
	}
	return reference == sqlrender.Qualified(seqName)
}
