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
	return schema.ObjectKeyFor(obj)
}
