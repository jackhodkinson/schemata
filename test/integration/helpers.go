package integration

import (
	"fmt"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// buildObjectMapFromObjects converts a slice of DatabaseObjects into a SchemaObjectMap
func buildObjectMapFromObjects(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	objectMap := make(schema.SchemaObjectMap)

	for _, obj := range objects {
		// Generate object key
		key := getObjectKey(obj)

		// Compute hash
		hash, err := differ.NormalizeAndHash(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to hash object %v: %w", key, err)
		}

		// Store in map
		objectMap[key] = schema.HashedObject{
			Hash:    hash,
			Payload: obj,
		}
	}

	return objectMap, nil
}

// getObjectKey generates an ObjectKey for a database object
func getObjectKey(obj schema.DatabaseObject) schema.ObjectKey {
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
			Signature: getFunctionSignature(v),
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
			Schema:   v.Schema,
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

// getFunctionSignature generates a signature string for function overloading
func getFunctionSignature(fn schema.Function) string {
	argTypes := make([]string, len(fn.Args))
	for i, arg := range fn.Args {
		argTypes[i] = string(arg.Type)
	}
	return fmt.Sprintf("(%s)", fmt.Sprint(argTypes))
}
