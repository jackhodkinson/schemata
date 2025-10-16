package differ

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Hash computes a stable SHA-256 hash of a database object
func Hash(obj schema.DatabaseObject) (string, error) {
	// Serialize to JSON with sorted keys
	data, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object: %w", err)
	}

	// Compute SHA-256 hash
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}

// HashString computes a SHA-256 hash of a string
func HashString(s string) string {
	hash := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", hash)
}

// NormalizeAndHash normalizes an object and computes its hash
// Normalization ensures that equivalent objects produce the same hash
func NormalizeAndHash(obj schema.DatabaseObject) (string, error) {
	// Normalize the object first
	normalized := normalize(obj)

	// Compute hash
	return Hash(normalized)
}

// normalize applies normalization rules to make objects comparable
func normalize(obj schema.DatabaseObject) schema.DatabaseObject {
	switch v := obj.(type) {
	case schema.Table:
		return normalizeTable(v)
	case schema.Index:
		return normalizeIndex(v)
	case schema.View:
		return normalizeView(v)
	case schema.Function:
		return normalizeFunction(v)
	case schema.Sequence:
		return normalizeSequence(v)
	case schema.EnumDef:
		return normalizeEnum(v)
	case schema.DomainDef:
		return normalizeDomain(v)
	case schema.CompositeDef:
		return normalizeComposite(v)
	case schema.Trigger:
		return normalizeTrigger(v)
	case schema.Policy:
		return normalizePolicy(v)
	default:
		// For Schema, Extension, and other simple types, no normalization needed
		return obj
	}
}

func normalizeTable(tbl schema.Table) schema.Table {
	// Sort columns by name for consistent hashing
	// Note: While column order affects physical layout in Postgres, for schema diffing
	// purposes we treat tables with the same columns in different order as equivalent.
	// Physical column reordering requires table rebuild, which is beyond basic schema management.
	sortedCols := make([]schema.Column, len(tbl.Columns))
	copy(sortedCols, tbl.Columns)
	sort.Slice(sortedCols, func(i, j int) bool {
		return sortedCols[i].Name < sortedCols[j].Name
	})
	tbl.Columns = sortedCols

	// Sort constraints by name for consistent hashing
	sort.Slice(tbl.Uniques, func(i, j int) bool {
		return tbl.Uniques[i].Name < tbl.Uniques[j].Name
	})
	sort.Slice(tbl.Checks, func(i, j int) bool {
		return tbl.Checks[i].Name < tbl.Checks[j].Name
	})
	sort.Slice(tbl.ForeignKeys, func(i, j int) bool {
		return tbl.ForeignKeys[i].Name < tbl.ForeignKeys[j].Name
	})

	// Sort reloptions
	if tbl.RelOptions != nil {
		sorted := make([]string, len(tbl.RelOptions))
		copy(sorted, tbl.RelOptions)
		sort.Strings(sorted)
		tbl.RelOptions = sorted
	}

	return tbl
}

func normalizeIndex(idx schema.Index) schema.Index {
	// Sort include columns
	sortedInclude := make([]schema.ColumnName, len(idx.Include))
	copy(sortedInclude, idx.Include)
	sort.Slice(sortedInclude, func(i, j int) bool {
		return sortedInclude[i] < sortedInclude[j]
	})
	idx.Include = sortedInclude

	return idx
}

func normalizeView(view schema.View) schema.View {
	// Normalize query text (strip extra whitespace, etc.)
	// For now, just trim
	// TODO: More sophisticated normalization
	return view
}

func normalizeFunction(fn schema.Function) schema.Function {
	// Sort search path
	sortedPath := make([]schema.SchemaName, len(fn.SearchPath))
	copy(sortedPath, fn.SearchPath)
	sort.Slice(sortedPath, func(i, j int) bool {
		return sortedPath[i] < sortedPath[j]
	})
	fn.SearchPath = sortedPath

	return fn
}

func normalizeSequence(seq schema.Sequence) schema.Sequence {
	// Sequences don't need special normalization
	return seq
}

func normalizeEnum(enum schema.EnumDef) schema.EnumDef {
	// Enum values order matters, so don't sort
	return enum
}

func normalizeDomain(domain schema.DomainDef) schema.DomainDef {
	// Domains don't need special normalization
	return domain
}

func normalizeComposite(comp schema.CompositeDef) schema.CompositeDef {
	// Sort attributes by name for consistent hashing
	// Note: Unlike table columns, composite type attribute order typically doesn't matter
	// in the same way for most use cases
	sortedAttrs := make([]schema.CompositeAttr, len(comp.Attributes))
	copy(sortedAttrs, comp.Attributes)
	sort.Slice(sortedAttrs, func(i, j int) bool {
		return sortedAttrs[i].Name < sortedAttrs[j].Name
	})
	comp.Attributes = sortedAttrs
	return comp
}

func normalizeTrigger(trig schema.Trigger) schema.Trigger {
	// Sort events for consistent comparison
	if len(trig.Events) > 1 {
		sorted := make([]schema.TriggerEvent, len(trig.Events))
		copy(sorted, trig.Events)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i] < sorted[j]
		})
		trig.Events = sorted
	}
	return trig
}

func normalizePolicy(pol schema.Policy) schema.Policy {
	// Sort role names for consistent comparison
	if len(pol.To) > 1 {
		sorted := make([]string, len(pol.To))
		copy(sorted, pol.To)
		sort.Strings(sorted)
		pol.To = sorted
	}
	return pol
}
