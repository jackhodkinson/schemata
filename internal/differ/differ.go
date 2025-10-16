package differ

import (
	"fmt"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Diff represents the differences between two schemas
type Diff struct {
	ToCreate []schema.ObjectKey
	ToDrop   []schema.ObjectKey
	ToAlter  []AlterOperation
}

// AlterOperation represents a change to an existing object
type AlterOperation struct {
	Key        schema.ObjectKey
	Changes    []string
	OldObject  schema.DatabaseObject
	NewObject  schema.DatabaseObject
}

// Differ compares two schema object maps and produces a diff
type Differ struct{}

// NewDiffer creates a new differ
func NewDiffer() *Differ {
	return &Differ{}
}

// Diff compares desired schema against actual schema
func (d *Differ) Diff(desired, actual schema.SchemaObjectMap) (*Diff, error) {
	diff := &Diff{}

	// Build key sets
	desiredKeys := make(map[schema.ObjectKey]bool)
	for key := range desired {
		desiredKeys[key] = true
	}

	actualKeys := make(map[schema.ObjectKey]bool)
	for key := range actual {
		actualKeys[key] = true
	}

	// Find objects to create (in desired but not in actual)
	for key := range desired {
		if !actualKeys[key] {
			diff.ToCreate = append(diff.ToCreate, key)
		}
	}

	// Find objects to drop (in actual but not in desired)
	for key := range actual {
		if !desiredKeys[key] {
			diff.ToDrop = append(diff.ToDrop, key)
		}
	}

	// Find objects that might need altering (in both, but different hashes)
	for key := range desired {
		if actualKeys[key] {
			desiredHash := desired[key].Hash
			actualHash := actual[key].Hash

			if desiredHash != actualHash {
				// Deep compare to find specific changes
				changes, err := d.compareObjects(desired[key].Payload, actual[key].Payload)
				if err != nil {
					return nil, fmt.Errorf("failed to compare objects for key %v: %w", key, err)
				}

				if len(changes) > 0 {
					diff.ToAlter = append(diff.ToAlter, AlterOperation{
						Key:       key,
						Changes:   changes,
						OldObject: actual[key].Payload,
						NewObject: desired[key].Payload,
					})
				}
			}
		}
	}

	return diff, nil
}

// compareObjects performs deep comparison of two objects to identify specific changes
func (d *Differ) compareObjects(desired, actual schema.DatabaseObject) ([]string, error) {
	// Type-specific comparison using comparators
	switch desiredObj := desired.(type) {
	case schema.Table:
		if actualObj, ok := actual.(schema.Table); ok {
			return compareTables(desiredObj, actualObj), nil
		}
	case schema.Index:
		if actualObj, ok := actual.(schema.Index); ok {
			return compareIndexes(desiredObj, actualObj), nil
		}
	case schema.View:
		if actualObj, ok := actual.(schema.View); ok {
			return compareViews(desiredObj, actualObj), nil
		}
	case schema.Function:
		if actualObj, ok := actual.(schema.Function); ok {
			return compareFunctions(desiredObj, actualObj), nil
		}
	case schema.Sequence:
		if actualObj, ok := actual.(schema.Sequence); ok {
			return compareSequences(desiredObj, actualObj), nil
		}
	case schema.EnumDef:
		if actualObj, ok := actual.(schema.EnumDef); ok {
			return compareEnums(desiredObj, actualObj), nil
		}
	case schema.DomainDef:
		if actualObj, ok := actual.(schema.DomainDef); ok {
			return compareDomains(desiredObj, actualObj), nil
		}
	case schema.CompositeDef:
		if actualObj, ok := actual.(schema.CompositeDef); ok {
			return compareComposites(desiredObj, actualObj), nil
		}
	case schema.Trigger:
		if actualObj, ok := actual.(schema.Trigger); ok {
			return compareTriggers(desiredObj, actualObj), nil
		}
	case schema.Policy:
		if actualObj, ok := actual.(schema.Policy); ok {
			return comparePolicies(desiredObj, actualObj), nil
		}
	case schema.Schema:
		// Schemas are simple, no deep comparison needed
		return nil, nil
	case schema.Extension:
		// Extensions changes are always replace
		return []string{"extension changed"}, nil
	}

	return []string{"object type mismatch"}, nil
}

// IsEmpty returns true if the diff contains no changes
func (d *Diff) IsEmpty() bool {
	return len(d.ToCreate) == 0 && len(d.ToDrop) == 0 && len(d.ToAlter) == 0
}
