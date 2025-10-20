package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

// TestFunctionBodyNormalization tests that function bodies are normalized correctly
// This replicates the issue where whitespace and formatting differences cause false positives
func TestFunctionBodyNormalization(t *testing.T) {
	tests := []struct {
		name     string
		desired  schema.Function
		actual   schema.Function
		expected []string // empty means no differences expected
	}{
		{
			name: "identical function bodies",
			desired: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				Body: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			actual: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				Body: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			expected: []string{},
		},
		{
			name: "whitespace differences should be normalized",
			desired: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				Body: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			actual: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				// pg_get_functiondef() often returns with extra newlines and different indentation
				Body: `
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			expected: []string{}, // Should be normalized to same
		},
		{
			name: "case differences in keywords",
			desired: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				Body: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			actual: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				// Keywords in different case
				Body: `begin
    new.updated_at = current_timestamp;
    return new;
end;`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			expected: []string{}, // Should be normalized to same (case-insensitive for keywords)
		},
		{
			name: "actual logic difference - different function calls",
			desired: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				Body: `BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			actual: schema.Function{
				Schema:   "public",
				Name:     "update_updated_at_column",
				Language: "plpgsql",
				Body: `BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;`,
				Returns:    schema.ReturnsType{Type: "trigger"},
				Volatility: schema.Volatile,
			},
			// NOW() and CURRENT_TIMESTAMP are semantically equivalent but textually different
			// Without deep semantic analysis, we detect this as a change
			expected: []string{"body changed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Normalize both functions before comparing (mimics what happens in real code)
			normalizedDesired := normalizeFunction(tt.desired)
			normalizedActual := normalizeFunction(tt.actual)

			changes := compareFunctions(normalizedDesired, normalizedActual)

			if len(tt.expected) == 0 {
				assert.Empty(t, changes, "Expected no changes but got: %v", changes)
			} else {
				assert.Equal(t, tt.expected, changes)
			}
		})
	}
}

// TestFunctionNormalizationInDiffer tests that normalization happens in the differ
func TestFunctionNormalizationInDiffer(t *testing.T) {
	desired := schema.Function{
		Schema:   "public",
		Name:     "test_func",
		Language: "plpgsql",
		Body:     "  BEGIN\n    RETURN 1;\n  END;  ", // extra whitespace
		Returns:  schema.ReturnsType{Type: "integer"},
	}

	actual := schema.Function{
		Schema:   "public",
		Name:     "test_func",
		Language: "plpgsql",
		Body:     "BEGIN\n    RETURN 1;\nEND;", // trimmed
		Returns:  schema.ReturnsType{Type: "integer"},
	}

	// Normalize both
	normalizedDesired := normalizeFunction(desired)
	normalizedActual := normalizeFunction(actual)

	// After normalization, bodies should have consistent whitespace
	assert.Equal(t, normalizedDesired.Body, normalizedActual.Body,
		"Normalized function bodies should have consistent whitespace")
}

// TestFunctionHashConsistency tests that functions with equivalent bodies produce same hash
func TestFunctionHashConsistency(t *testing.T) {
	func1 := schema.Function{
		Schema:   "public",
		Name:     "test_func",
		Language: "plpgsql",
		Body:     "BEGIN\n    RETURN 1;\nEND;",
		Returns:  schema.ReturnsType{Type: "integer"},
	}

	func2 := schema.Function{
		Schema:   "public",
		Name:     "test_func",
		Language: "plpgsql",
		Body:     "  BEGIN\n    RETURN 1;\nEND;  ", // extra whitespace
		Returns:  schema.ReturnsType{Type: "integer"},
	}

	hash1, err := NormalizeAndHash(func1)
	assert.NoError(t, err)

	hash2, err := NormalizeAndHash(func2)
	assert.NoError(t, err)

	assert.Equal(t, hash1, hash2, "Functions with equivalent bodies should produce same hash after normalization")
}
