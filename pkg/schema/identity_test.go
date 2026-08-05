package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeIdentityOptionsUsesDescendingDefaults(t *testing.T) {
	t.Parallel()

	options := NormalizeIdentityOptions("integer", []SequenceOption{
		{Type: "INCREMENT BY", Value: -1, HasValue: true},
		{Type: "NO MINVALUE"},
		{Type: "NO MAXVALUE"},
	})

	assert.Equal(t, []SequenceOption{{Type: "INCREMENT BY", Value: -1, HasValue: true}}, options)
}

func TestIdentityOptionsFromParametersUsesDescendingDefaults(t *testing.T) {
	t.Parallel()

	start := int64(-1)
	increment := int64(-1)
	min := int64(-2147483648)
	max := int64(-1)
	cache := int64(1)
	cycle := false

	options := IdentityOptionsFromParameters("integer", &start, &increment, &min, &max, &cache, &cycle)

	assert.Equal(t, []SequenceOption{{Type: "INCREMENT BY", Value: -1, HasValue: true}}, options)
}

func TestNoMinAndMaxValueNormalizeToDirectionDefaults(t *testing.T) {
	t.Parallel()

	ascending := NormalizeIdentityOptions("bigint", []SequenceOption{{Type: "NO MINVALUE"}, {Type: "NO MAXVALUE"}})
	descending := NormalizeIdentityOptions("bigint", []SequenceOption{
		{Type: "INCREMENT BY", Value: -5, HasValue: true},
		{Type: "NO MINVALUE"},
		{Type: "NO MAXVALUE"},
	})

	assert.Empty(t, ascending)
	assert.Equal(t, []SequenceOption{{Type: "INCREMENT BY", Value: -5, HasValue: true}}, descending)
}
