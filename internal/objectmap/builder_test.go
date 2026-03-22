package objectmap

import (
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
