package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunctionDiff_NoCreateOrDropOnFormattingOnlyChanges(t *testing.T) {
	desired := schema.Function{
		Schema:     "public",
		Name:       "update_updated_at_column",
		Language:   schema.PlpgSQL,
		Volatility: schema.Volatile,
		Returns:    schema.ReturnsType{Type: "trigger"},
		Body: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
	}

	actual := desired
	actual.Body = `
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
`

	key := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "update_updated_at_column",
		Signature: schema.FunctionSignature(desired.Args),
	}

	desiredHash, err := NormalizeAndHash(desired)
	require.NoError(t, err)
	actualHash, err := NormalizeAndHash(actual)
	require.NoError(t, err)

	desiredMap := schema.SchemaObjectMap{
		key: {Hash: desiredHash, Payload: desired},
	}
	actualMap := schema.SchemaObjectMap{
		key: {Hash: actualHash, Payload: actual},
	}

	d := NewDiffer()
	diff, err := d.Diff(desiredMap, actualMap)
	require.NoError(t, err)

	assert.Empty(t, diff.ToCreate, "formatting-only body changes must not create functions")
	assert.Empty(t, diff.ToDrop, "formatting-only body changes must not drop functions")
	assert.Empty(t, diff.ToAlter, "formatting-only body changes must not alter functions")
	assert.True(t, diff.IsEmpty(), "function should be round-trip stable when only formatting differs")
}

func TestFunctionDiff_ReportsAlterForRealBodyLogicChange(t *testing.T) {
	desired := schema.Function{
		Schema:     "public",
		Name:       "update_updated_at_column",
		Language:   schema.PlpgSQL,
		Volatility: schema.Volatile,
		Returns:    schema.ReturnsType{Type: "trigger"},
		Body: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
	}

	actual := desired
	actual.Body = `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NULL;
END;`

	key := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "update_updated_at_column",
		Signature: schema.FunctionSignature(desired.Args),
	}

	desiredHash, err := NormalizeAndHash(desired)
	require.NoError(t, err)
	actualHash, err := NormalizeAndHash(actual)
	require.NoError(t, err)

	desiredMap := schema.SchemaObjectMap{
		key: {Hash: desiredHash, Payload: desired},
	}
	actualMap := schema.SchemaObjectMap{
		key: {Hash: actualHash, Payload: actual},
	}

	d := NewDiffer()
	diff, err := d.Diff(desiredMap, actualMap)
	require.NoError(t, err)

	assert.Empty(t, diff.ToCreate, "logic changes should not look like create operations")
	assert.Empty(t, diff.ToDrop, "logic changes should not look like drop operations")
	require.Len(t, diff.ToAlter, 1, "logic changes must produce one alter operation")
	assert.Contains(t, diff.ToAlter[0].Changes, "body changed")
}

func TestFunctionDiff_DetectsStringLiteralCaseChanges(t *testing.T) {
	desired := schema.Function{
		Schema:     "public",
		Name:       "greet_user",
		Language:   schema.PlpgSQL,
		Volatility: schema.Volatile,
		Returns:    schema.ReturnsType{Type: "text"},
		Body: `BEGIN
    RETURN 'Admin';
END;`,
	}

	actual := desired
	actual.Body = `BEGIN
    RETURN 'admin';
END;`

	key := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "greet_user",
		Signature: schema.FunctionSignature(desired.Args),
	}

	desiredHash, err := NormalizeAndHash(desired)
	require.NoError(t, err)
	actualHash, err := NormalizeAndHash(actual)
	require.NoError(t, err)

	desiredMap := schema.SchemaObjectMap{
		key: {Hash: desiredHash, Payload: desired},
	}
	actualMap := schema.SchemaObjectMap{
		key: {Hash: actualHash, Payload: actual},
	}

	d := NewDiffer()
	diff, err := d.Diff(desiredMap, actualMap)
	require.NoError(t, err)

	// Contract: literal value changes are semantic changes and must be detected.
	assert.Empty(t, diff.ToCreate, "literal changes must not look like creates")
	assert.Empty(t, diff.ToDrop, "literal changes must not look like drops")
	require.Len(t, diff.ToAlter, 1, "literal case changes should produce one alter")
	assert.Contains(t, diff.ToAlter[0].Changes, "body changed")
}

func TestFunctionDiff_DetectsQuotedIdentifierCaseChanges(t *testing.T) {
	desired := schema.Function{
		Schema:     "public",
		Name:       "quoted_identifier_case",
		Language:   schema.PlpgSQL,
		Volatility: schema.Volatile,
		Returns:    schema.ReturnsType{Type: "text"},
		Body: `BEGIN
    RETURN NEW."UserName";
END;`,
	}

	actual := desired
	actual.Body = `BEGIN
    RETURN NEW."username";
END;`

	key := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "quoted_identifier_case",
		Signature: schema.FunctionSignature(desired.Args),
	}

	desiredHash, err := NormalizeAndHash(desired)
	require.NoError(t, err)
	actualHash, err := NormalizeAndHash(actual)
	require.NoError(t, err)

	desiredMap := schema.SchemaObjectMap{
		key: {Hash: desiredHash, Payload: desired},
	}
	actualMap := schema.SchemaObjectMap{
		key: {Hash: actualHash, Payload: actual},
	}

	d := NewDiffer()
	diff, err := d.Diff(desiredMap, actualMap)
	require.NoError(t, err)

	assert.Empty(t, diff.ToCreate)
	assert.Empty(t, diff.ToDrop)
	require.Len(t, diff.ToAlter, 1, "quoted identifier case changes should produce one alter")
	assert.Contains(t, diff.ToAlter[0].Changes, "body changed")
}

func TestFunctionDiff_DetectsStringLiteralWhitespaceChanges(t *testing.T) {
	desired := schema.Function{
		Schema:     "public",
		Name:       "literal_whitespace",
		Language:   schema.PlpgSQL,
		Volatility: schema.Volatile,
		Returns:    schema.ReturnsType{Type: "text"},
		Body: `BEGIN
    RETURN 'a  b';
END;`,
	}

	actual := desired
	actual.Body = `BEGIN
    RETURN 'a b';
END;`

	key := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "literal_whitespace",
		Signature: schema.FunctionSignature(desired.Args),
	}

	desiredHash, err := NormalizeAndHash(desired)
	require.NoError(t, err)
	actualHash, err := NormalizeAndHash(actual)
	require.NoError(t, err)

	desiredMap := schema.SchemaObjectMap{
		key: {Hash: desiredHash, Payload: desired},
	}
	actualMap := schema.SchemaObjectMap{
		key: {Hash: actualHash, Payload: actual},
	}

	d := NewDiffer()
	diff, err := d.Diff(desiredMap, actualMap)
	require.NoError(t, err)

	assert.Empty(t, diff.ToCreate)
	assert.Empty(t, diff.ToDrop)
	require.Len(t, diff.ToAlter, 1, "string literal whitespace changes should produce one alter")
	assert.Contains(t, diff.ToAlter[0].Changes, "body changed")
}
