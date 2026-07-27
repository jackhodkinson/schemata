package objectmap

import (
	"errors"
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestKey_FunctionIncludesSignature(t *testing.T) {
	fn := schema.Function{
		Schema: "public",
		Name:   "do_thing",
		Args: []schema.FunctionArg{
			{Type: "integer"},
			{Type: "text"},
		},
	}

	key := Key(fn)

	require.Equal(t, schema.FunctionKind, key.Kind)
	require.Equal(t, schema.SchemaName("public"), key.Schema)
	require.Equal(t, "do_thing", key.Name)
	require.Equal(t, "(integer,text)", key.Signature)
}

func TestBuildRejectsDuplicateCanonicalIdentity(t *testing.T) {
	objects := []schema.DatabaseObject{
		schema.Table{Schema: "public", Name: "users"},
		schema.Table{Schema: "public", Name: "users"},
	}

	_, err := Build(objects)
	require.Error(t, err)

	var duplicate *DuplicateObjectError
	require.True(t, errors.As(err, &duplicate))
	require.Equal(t, schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}, duplicate.Key)
	require.Equal(t, 0, duplicate.FirstIndex)
	require.Equal(t, 1, duplicate.SecondIndex)
}

func TestBuild_UsesCanonicalIdentityAndHashing(t *testing.T) {
	objects := []schema.DatabaseObject{
		schema.Table{
			Schema: "public",
			Name:   "users",
			Columns: []schema.Column{
				{Name: "id", Type: "integer"},
				{Name: "email", Type: "text"},
			},
		},
		schema.Function{
			Schema: "public",
			Name:   "do_thing",
			Args: []schema.FunctionArg{
				{Type: "integer"},
			},
			Returns:  schema.ReturnsType{Type: "integer"},
			Language: "sql",
			Body:     "SELECT 1",
		},
	}

	objectMap, err := Build(objects)
	require.NoError(t, err)
	require.Len(t, objectMap, 2)

	tableKey := schema.ObjectKey{
		Kind:   schema.TableKind,
		Schema: "public",
		Name:   "users",
	}
	functionKey := schema.ObjectKey{
		Kind:      schema.FunctionKind,
		Schema:    "public",
		Name:      "do_thing",
		Signature: "(integer)",
	}

	require.Contains(t, objectMap, tableKey)
	require.Contains(t, objectMap, functionKey)
	require.NotEmpty(t, objectMap[tableKey].Hash)
	require.NotEmpty(t, objectMap[functionKey].Hash)
}
