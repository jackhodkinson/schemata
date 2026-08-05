package capability

import (
	"errors"
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ error = (*UnsupportedError)(nil)

func TestAdvertisedObjectFamiliesHaveEveryPipelineStage(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateMatrix())
	assert.Len(t, Matrix, len(V1Families)+1, "v1 families plus the optional policy family")
}

func TestRequireFailsClosedForPartialAndUnsupportedCapabilities(t *testing.T) {
	t.Parallel()

	key := schema.ObjectKey{Kind: schema.TypeKind, Schema: "public", Name: "address"}
	assert.NoError(t, Require(CompositeFamily, CreateStage, "composite type", key))
	err := Require(CompositeFamily, CaptureStage, "composite type", key)
	var unsupported *UnsupportedError
	require.True(t, errors.As(err, &unsupported))
	assert.Equal(t, CompositeFamily, unsupported.Family)
	assert.Equal(t, CaptureStage, unsupported.Stage)
	assert.Contains(t, err.Error(), "explicit composites are captured")

	assert.NoError(t, Require(EnumFamily, CreateStage, "enum", key))
}
