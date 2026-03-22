package differ

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	normalizer "github.com/jackhodkinson/schemata/internal/normalize"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Hash computes a stable SHA-256 hash of a database object
func Hash(obj schema.DatabaseObject) (string, error) {
	// Serialize to JSON with sorted keys
	data, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object: %w", err)
	}

	// Compute SHA-256 hash
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}

// NormalizeAndHash normalizes an object and computes its hash
// Normalization ensures that equivalent objects produce the same hash
func NormalizeAndHash(obj schema.DatabaseObject) (string, error) {
	// Normalize the object first
	normalized := normalize(obj)

	// Compute hash
	return Hash(normalized)
}

// normalize applies normalization rules to make objects comparable
func normalize(obj schema.DatabaseObject) schema.DatabaseObject {
	return normalizer.Object(obj)
}

func normalizeIndex(idx schema.Index) schema.Index {
	return normalizer.Object(idx).(schema.Index)
}

func normalizeFunction(fn schema.Function) schema.Function {
	return normalizer.Object(fn).(schema.Function)
}

func normalizeTrigger(trig schema.Trigger) schema.Trigger {
	return normalizer.Object(trig).(schema.Trigger)
}

// normalizeExpr normalizes SQL expressions to a canonical form
func normalizeExpr(expr schema.Expr) schema.Expr {
	return normalizer.Expr(expr)
}
