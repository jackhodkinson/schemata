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

func normalizeTable(tbl schema.Table) schema.Table {
	return normalizer.Object(tbl).(schema.Table)
}

func normalizeIndex(idx schema.Index) schema.Index {
	return normalizer.Object(idx).(schema.Index)
}

func normalizeView(view schema.View) schema.View {
	return normalizer.Object(view).(schema.View)
}

func normalizeFunction(fn schema.Function) schema.Function {
	return normalizer.Object(fn).(schema.Function)
}

func normalizeFunctionBody(body string) string {
	return normalizer.FunctionBody(body)
}

func normalizeSequence(seq schema.Sequence) schema.Sequence {
	return normalizer.Object(seq).(schema.Sequence)
}

func normalizeEnum(enum schema.EnumDef) schema.EnumDef {
	return normalizer.Object(enum).(schema.EnumDef)
}

func normalizeDomain(domain schema.DomainDef) schema.DomainDef {
	return normalizer.Object(domain).(schema.DomainDef)
}

func normalizeComposite(comp schema.CompositeDef) schema.CompositeDef {
	return normalizer.Object(comp).(schema.CompositeDef)
}

func normalizeTrigger(trig schema.Trigger) schema.Trigger {
	return normalizer.Object(trig).(schema.Trigger)
}

func normalizePolicy(pol schema.Policy) schema.Policy {
	return normalizer.Object(pol).(schema.Policy)
}

func normalizeExtension(ext schema.Extension) schema.Extension {
	return normalizer.Object(ext).(schema.Extension)
}

// normalizeExpr normalizes SQL expressions to a canonical form
func normalizeExpr(expr schema.Expr) schema.Expr {
	return normalizer.Expr(expr)
}
