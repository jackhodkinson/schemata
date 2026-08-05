package db

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateIdentityBackingSequencesConsumesInternalSequence(t *testing.T) {
	t.Parallel()

	table, sequence := identityBackingFixture()
	consumed, err := validateIdentityBackingSequences(
		[]schema.DatabaseObject{table},
		[]catalogSequence{{Sequence: sequence, DependencyKind: sequenceIdentity}},
	)

	require.NoError(t, err)
	assert.True(t, consumed[schema.ObjectKey{Kind: schema.SequenceKind, Schema: "public", Name: "custom_ids"}])
}

func TestValidateIdentityBackingSequencesFailsClosedOnUnmodeledMetadata(t *testing.T) {
	t.Parallel()

	table, sequence := identityBackingFixture()
	comment := "identity values"
	sequence.Comment = &comment

	_, err := validateIdentityBackingSequences(
		[]schema.DatabaseObject{table},
		[]catalogSequence{{Sequence: sequence, DependencyKind: sequenceIdentity}},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "comment or non-default ACL")
}

func TestValidateIdentityBackingSequencesRejectsSerialClassification(t *testing.T) {
	t.Parallel()

	table, sequence := identityBackingFixture()
	_, err := validateIdentityBackingSequences(
		[]schema.DatabaseObject{table},
		[]catalogSequence{{Sequence: sequence, DependencyKind: sequenceSerial}},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not classified as an internal identity dependency")
}

func identityBackingFixture() (schema.Table, schema.Sequence) {
	owner := "postgres"
	name := schema.QualifiedName{Schema: "public", Name: "custom_ids"}
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Owner:  &owner,
		Columns: []schema.Column{{
			Name:     "id",
			Type:     "integer",
			Identity: &schema.IdentitySpec{Always: true, SequenceName: &name},
		}},
	}
	sequence := canonicalSerialSequence("public", "items", "id", "integer", 2147483647)
	sequence.Name = "custom_ids"
	return table, sequence
}
