package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

// TestTriggerForEachRowComparison tests that trigger ForEachRow field is compared correctly
// This replicates the issue where triggers show as changed due to ForEachRow differences
func TestTriggerForEachRowComparison(t *testing.T) {
	tests := []struct {
		name     string
		desired  schema.Trigger
		actual   schema.Trigger
		expected []string // empty means no differences expected
	}{
		{
			name: "identical triggers with ForEachRow true",
			desired: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "update_users_updated_at",
				Timing:     schema.Before,
				Events:     []schema.TriggerEvent{schema.Update},
				ForEachRow: true,
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "update_updated_at_column",
				},
			},
			actual: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "update_users_updated_at",
				Timing:     schema.Before,
				Events:     []schema.TriggerEvent{schema.Update},
				ForEachRow: true,
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "update_updated_at_column",
				},
			},
			expected: []string{},
		},
		{
			name: "ForEachRow defaults - parser may not set it, catalog does",
			desired: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "update_users_updated_at",
				Timing:     schema.Before,
				Events:     []schema.TriggerEvent{schema.Update},
				ForEachRow: false, // Parser might default to false
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "update_updated_at_column",
				},
			},
			actual: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "update_users_updated_at",
				Timing:     schema.Before,
				Events:     []schema.TriggerEvent{schema.Update},
				ForEachRow: true, // Catalog extracts actual value
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "update_updated_at_column",
				},
			},
			expected: []string{"for each row changed"},
		},
		{
			name: "identical triggers with statement-level (ForEachRow false)",
			desired: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "audit_trigger",
				Timing:     schema.After,
				Events:     []schema.TriggerEvent{schema.Insert, schema.Update, schema.Delete},
				ForEachRow: false, // Statement-level trigger
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "audit_changes",
				},
			},
			actual: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "audit_trigger",
				Timing:     schema.After,
				Events:     []schema.TriggerEvent{schema.Insert, schema.Update, schema.Delete},
				ForEachRow: false,
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "audit_changes",
				},
			},
			expected: []string{},
		},
		{
			name: "events in different order should be normalized",
			desired: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "multi_event_trigger",
				Timing:     schema.Before,
				Events:     []schema.TriggerEvent{schema.Insert, schema.Update, schema.Delete},
				ForEachRow: true,
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "check_changes",
				},
			},
			actual: schema.Trigger{
				Schema:     "public",
				Table:      "users",
				Name:       "multi_event_trigger",
				Timing:     schema.Before,
				Events:     []schema.TriggerEvent{schema.Delete, schema.Insert, schema.Update}, // Different order
				ForEachRow: true,
				Function: schema.QualifiedName{
					Schema: "public",
					Name:   "check_changes",
				},
			},
			expected: []string{}, // Should be normalized to same order
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Normalize both triggers before comparing (mimics what happens in real code)
			normalizedDesired := normalizeTrigger(tt.desired)
			normalizedActual := normalizeTrigger(tt.actual)

			changes := compareTriggers(normalizedDesired, normalizedActual)

			if len(tt.expected) == 0 {
				assert.Empty(t, changes, "Expected no changes but got: %v", changes)
			} else {
				assert.Equal(t, tt.expected, changes)
			}
		})
	}
}

// TestTriggerNormalizationInDiffer tests that trigger events are sorted during normalization
func TestTriggerNormalizationInDiffer(t *testing.T) {
	desired := schema.Trigger{
		Schema:     "public",
		Table:      "users",
		Name:       "test_trigger",
		Timing:     schema.Before,
		Events:     []schema.TriggerEvent{schema.Delete, schema.Insert, schema.Update},
		ForEachRow: true,
		Function: schema.QualifiedName{
			Schema: "public",
			Name:   "test_func",
		},
	}

	actual := schema.Trigger{
		Schema:     "public",
		Table:      "users",
		Name:       "test_trigger",
		Timing:     schema.Before,
		Events:     []schema.TriggerEvent{schema.Insert, schema.Update, schema.Delete}, // Different order
		ForEachRow: true,
		Function: schema.QualifiedName{
			Schema: "public",
			Name:   "test_func",
		},
	}

	// Normalize both
	normalizedDesired := normalizeTrigger(desired)
	normalizedActual := normalizeTrigger(actual)

	// After normalization, events should be in same order
	assert.Equal(t, normalizedDesired.Events, normalizedActual.Events,
		"Normalized trigger events should be in same order")
}

// TestTriggerHashConsistency tests that triggers with same events in different order produce same hash
func TestTriggerHashConsistency(t *testing.T) {
	trigger1 := schema.Trigger{
		Schema:     "public",
		Table:      "users",
		Name:       "test_trigger",
		Timing:     schema.Before,
		Events:     []schema.TriggerEvent{schema.Insert, schema.Update},
		ForEachRow: true,
		Function: schema.QualifiedName{
			Schema: "public",
			Name:   "test_func",
		},
	}

	trigger2 := schema.Trigger{
		Schema:     "public",
		Table:      "users",
		Name:       "test_trigger",
		Timing:     schema.Before,
		Events:     []schema.TriggerEvent{schema.Update, schema.Insert}, // Different order
		ForEachRow: true,
		Function: schema.QualifiedName{
			Schema: "public",
			Name:   "test_func",
		},
	}

	hash1, err := NormalizeAndHash(trigger1)
	assert.NoError(t, err)

	hash2, err := NormalizeAndHash(trigger2)
	assert.NoError(t, err)

	assert.Equal(t, hash1, hash2, "Triggers with same events in different order should produce same hash after normalization")
}
