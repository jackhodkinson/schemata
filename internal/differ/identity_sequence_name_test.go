package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentitySequenceNameComparisonSemantics(t *testing.T) {
	t.Parallel()

	generated := schema.QualifiedName{Schema: "public", Name: "items_id_seq"}
	explicit := schema.QualifiedName{Schema: "public", Name: "custom_ids"}
	wrongName := schema.QualifiedName{Schema: "public", Name: "other_ids"}
	wrongSchema := schema.QualifiedName{Schema: "tenant", Name: "custom_ids"}

	tests := map[string]struct {
		desired *schema.QualifiedName
		actual  *schema.QualifiedName
		equal   bool
	}{
		"generated actual name is unmanaged when desired is nil": {
			desired: nil,
			actual:  &generated,
			equal:   true,
		},
		"same explicit qualified name": {
			desired: &explicit,
			actual:  &explicit,
			equal:   true,
		},
		"explicit name mismatch": {
			desired: &explicit,
			actual:  &wrongName,
			equal:   false,
		},
		"explicit schema mismatch": {
			desired: &explicit,
			actual:  &wrongSchema,
			equal:   false,
		},
		"explicit desired name requires an actual name": {
			desired: &explicit,
			actual:  nil,
			equal:   false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			desired := &schema.IdentitySpec{SequenceName: test.desired}
			actual := &schema.IdentitySpec{SequenceName: test.actual}
			assert.Equal(t, test.equal, identitySpecEqual(desired, actual))
		})
	}
}

func TestNormalizeAndHashPreservesIdentitySequenceName(t *testing.T) {
	t.Parallel()

	firstName := schema.QualifiedName{Schema: "public", Name: "first_ids"}
	secondName := schema.QualifiedName{Schema: "public", Name: "second_ids"}
	base := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{{
			Name:     "id",
			Type:     "int4",
			Identity: &schema.IdentitySpec{SequenceName: &firstName},
		}},
	}
	other := base
	other.Columns = append([]schema.Column(nil), base.Columns...)
	other.Columns[0].Identity = &schema.IdentitySpec{SequenceName: &secondName}

	firstHash, err := NormalizeAndHash(base)
	require.NoError(t, err)
	secondHash, err := NormalizeAndHash(other)
	require.NoError(t, err)
	assert.NotEqual(t, firstHash, secondHash, "an explicit identity sequence name must remain meaning-bearing through normalization and hashing")
}
