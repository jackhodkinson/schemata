package objectmap

import (
	"fmt"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Build creates a schema object map using the canonical identity and
// normalization-hash contract used across parser, app service, and tests.
func Build(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	objectMap := make(schema.SchemaObjectMap, len(objects))

	for _, obj := range objects {
		key := Key(obj)
		hash, err := differ.NormalizeAndHash(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to hash object %v: %w", key, err)
		}

		objectMap[key] = schema.HashedObject{
			Hash:    hash,
			Payload: obj,
		}
	}

	return objectMap, nil
}

// Key returns the canonical identity key for a database object.
func Key(obj schema.DatabaseObject) schema.ObjectKey {
	switch v := obj.(type) {
	case schema.Table:
		return schema.ObjectKey{
			Kind:   schema.TableKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.Index:
		return schema.ObjectKey{
			Kind:      schema.IndexKind,
			Schema:    v.Schema,
			Name:      v.Name,
			TableName: v.Table,
		}
	case schema.View:
		return schema.ObjectKey{
			Kind:   schema.ViewKind,
			Schema: v.Schema,
			Name:   v.Name,
		}
	case schema.Function:
		return schema.ObjectKey{
			Kind:      schema.FunctionKind,
			Schema:    v.Schema,
			Name:      v.Name,
			Signature: schema.FunctionSignature(v.Args),
		}
	case schema.Sequence:
		return schema.ObjectKey{
			Kind:   schema.SequenceKind,
			Schema: v.Schema,
			Name:   v.Name,
		}
	case schema.EnumDef:
		return schema.ObjectKey{
			Kind:   schema.TypeKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.DomainDef:
		return schema.ObjectKey{
			Kind:   schema.TypeKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.CompositeDef:
		return schema.ObjectKey{
			Kind:   schema.TypeKind,
			Schema: v.Schema,
			Name:   string(v.Name),
		}
	case schema.Trigger:
		return schema.ObjectKey{
			Kind:      schema.TriggerKind,
			Schema:    v.Schema,
			Name:      v.Name,
			TableName: v.Table,
		}
	case schema.Policy:
		return schema.ObjectKey{
			Kind:      schema.PolicyKind,
			Schema:    v.Schema,
			Name:      v.Name,
			TableName: v.Table,
		}
	case schema.Extension:
		return schema.ObjectKey{
			Kind:   schema.ExtensionKind,
			Schema: v.Schema,
			Name:   v.Name,
		}
	case schema.Schema:
		return schema.ObjectKey{
			Kind:   schema.SchemaKind,
			Schema: v.Name,
			Name:   string(v.Name),
		}
	default:
		return schema.ObjectKey{}
	}
}
