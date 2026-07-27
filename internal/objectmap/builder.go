package objectmap

import (
	"fmt"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// DuplicateObjectError reports two objects that resolve to the same canonical
// database identity. Silently choosing the last definition would make the
// resulting plan depend on input order.
type DuplicateObjectError struct {
	Key         schema.ObjectKey
	FirstIndex  int
	SecondIndex int
}

func (e *DuplicateObjectError) Error() string {
	return fmt.Sprintf("duplicate schema object %v at positions %d and %d", e.Key, e.FirstIndex+1, e.SecondIndex+1)
}

// Build creates a schema object map using the canonical identity and
// normalization-hash contract used across parser, app service, and tests.
func Build(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	objectMap := make(schema.SchemaObjectMap, len(objects))
	firstIndex := make(map[schema.ObjectKey]int, len(objects))

	for i, obj := range objects {
		key := Key(obj)
		if first, exists := firstIndex[key]; exists {
			return nil, &DuplicateObjectError{
				Key:         key,
				FirstIndex:  first,
				SecondIndex: i,
			}
		}
		hash, err := differ.NormalizeAndHash(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to hash object %v: %w", key, err)
		}

		firstIndex[key] = i
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
