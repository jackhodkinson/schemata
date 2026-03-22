package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObjectKeyLess_UsesStableKindPrecedence(t *testing.T) {
	keys := []ObjectKey{
		{Kind: IndexKind, Schema: "public", Name: "idx_users_email", TableName: "users"},
		{Kind: TableKind, Schema: "public", Name: "users"},
		{Kind: FunctionKind, Schema: "public", Name: "f", Signature: "(integer)"},
		{Kind: TypeKind, Schema: "public", Name: "user_role"},
	}

	SortObjectKeys(keys)

	assert.Equal(t, []ObjectKind{TypeKind, TableKind, FunctionKind, IndexKind}, []ObjectKind{
		keys[0].Kind, keys[1].Kind, keys[2].Kind, keys[3].Kind,
	})
}

func TestObjectKeyLess_UsesAllTieBreakers(t *testing.T) {
	keys := []ObjectKey{
		{Kind: FunctionKind, Schema: "public", Name: "f", Signature: "(text)"},
		{Kind: FunctionKind, Schema: "public", Name: "f", Signature: "(integer)"},
		{Kind: FunctionKind, Schema: "audit", Name: "f", Signature: "(integer)"},
		{Kind: FunctionKind, Schema: "public", Name: "a", Signature: "(integer)"},
	}

	SortObjectKeys(keys)

	assert.Equal(t, ObjectKey{Kind: FunctionKind, Schema: "audit", Name: "f", Signature: "(integer)"}, keys[0])
	assert.Equal(t, ObjectKey{Kind: FunctionKind, Schema: "public", Name: "a", Signature: "(integer)"}, keys[1])
	assert.Equal(t, ObjectKey{Kind: FunctionKind, Schema: "public", Name: "f", Signature: "(integer)"}, keys[2])
	assert.Equal(t, ObjectKey{Kind: FunctionKind, Schema: "public", Name: "f", Signature: "(text)"}, keys[3])
}
