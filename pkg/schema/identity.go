package schema

import (
	"strings"
)

type identityDefaults struct {
	start     int64
	increment int64
	min       int64
	max       int64
	typeMin   int64
	typeMax   int64
	cache     int64
	cycle     bool
}

func identityDefaultsForType(columnType TypeName) (identityDefaults, bool) {
	base := normalizeIntegerType(string(columnType))
	switch base {
	case "int2":
		return identityDefaults{
			start:     1,
			increment: 1,
			min:       1,
			max:       32767,
			typeMin:   -32768,
			typeMax:   32767,
			cache:     1,
			cycle:     false,
		}, true
	case "int4":
		return identityDefaults{
			start:     1,
			increment: 1,
			min:       1,
			max:       2147483647,
			typeMin:   -2147483648,
			typeMax:   2147483647,
			cache:     1,
			cycle:     false,
		}, true
	case "int8":
		return identityDefaults{
			start:     1,
			increment: 1,
			min:       1,
			max:       9223372036854775807,
			typeMin:   -9223372036854775808,
			typeMax:   9223372036854775807,
			cache:     1,
			cycle:     false,
		}, true
	default:
		return identityDefaults{}, false
	}
}

func normalizeIntegerType(typeName string) string {
	lower := strings.ToLower(typeName)
	lower = strings.ReplaceAll(lower, "\"", "")
	parts := strings.Split(lower, ".")
	base := parts[len(parts)-1]
	switch base {
	case "smallint", "int2", "smallserial":
		return "int2"
	case "integer", "int", "int4", "serial":
		return "int4"
	case "bigint", "int8", "bigserial":
		return "int8"
	default:
		return base
	}
}

// NormalizeIdentityOptions canonicalizes identity sequence options provided in DDL by
// removing entries that match the data type defaults and collapsing equivalent forms.
func NormalizeIdentityOptions(columnType TypeName, options []SequenceOption) []SequenceOption {
	defaults, ok := identityDefaultsForType(columnType)
	if !ok {
		// Unknown type: return options as-is
		return options
	}

	state := defaults
	startSet := false
	incrementSet := false
	minSet := false
	minNo := false
	maxSet := false
	maxNo := false
	cacheSet := false
	cycleSet := false

	for _, opt := range options {
		switch strings.ToUpper(opt.Type) {
		case "START WITH":
			if opt.HasValue {
				state.start = opt.Value
				startSet = true
			}
		case "INCREMENT BY":
			if opt.HasValue {
				state.increment = opt.Value
				incrementSet = true
			}
		case "MINVALUE":
			if opt.HasValue {
				state.min = opt.Value
				minSet = true
				minNo = false
			}
		case "NO MINVALUE":
			state.min = defaults.typeMin
			minSet = true
			minNo = true
		case "MAXVALUE":
			if opt.HasValue {
				state.max = opt.Value
				maxSet = true
				maxNo = false
			}
		case "NO MAXVALUE":
			state.max = defaults.typeMax
			maxSet = true
			maxNo = true
		case "CACHE":
			if opt.HasValue {
				state.cache = opt.Value
				cacheSet = true
			}
		case "CYCLE":
			state.cycle = true
			cycleSet = true
		case "NO CYCLE":
			state.cycle = false
			cycleSet = true
		}
	}

	var normalized []SequenceOption
	if startSet && state.start != defaults.start {
		normalized = append(normalized, SequenceOption{Type: "START WITH", Value: state.start, HasValue: true})
	}
	if incrementSet && state.increment != defaults.increment {
		normalized = append(normalized, SequenceOption{Type: "INCREMENT BY", Value: state.increment, HasValue: true})
	}

	if minSet {
		switch {
		case minNo:
			normalized = append(normalized, SequenceOption{Type: "NO MINVALUE"})
		case state.min != defaults.min:
			normalized = append(normalized, SequenceOption{Type: "MINVALUE", Value: state.min, HasValue: true})
		}
	}

	if maxSet {
		switch {
		case maxNo && state.max != defaults.max:
			// If the user explicitly set NO MAXVALUE but the default matches, omit it.
			// Only emit when it differs from defaults.
			normalized = append(normalized, SequenceOption{Type: "NO MAXVALUE"})
		case !maxNo && state.max != defaults.max:
			normalized = append(normalized, SequenceOption{Type: "MAXVALUE", Value: state.max, HasValue: true})
		}
	}

	if cacheSet && state.cache != defaults.cache {
		normalized = append(normalized, SequenceOption{Type: "CACHE", Value: state.cache, HasValue: true})
	}
	if cycleSet {
		if state.cycle {
			normalized = append(normalized, SequenceOption{Type: "CYCLE"})
		} else if defaults.cycle {
			normalized = append(normalized, SequenceOption{Type: "NO CYCLE"})
		}
	}

	return normalized
}

// IdentityOptionsFromParameters converts catalog sequence parameters into a canonical
// option list that omits default values for the column's data type.
func IdentityOptionsFromParameters(columnType TypeName, start, increment, min, max, cache *int64, cycle *bool) []SequenceOption {
	defaults, ok := identityDefaultsForType(columnType)
	if !ok {
		return nil
	}

	var options []SequenceOption

	if start != nil && *start != defaults.start {
		options = append(options, SequenceOption{Type: "START WITH", Value: *start, HasValue: true})
	}
	if increment != nil && *increment != defaults.increment {
		options = append(options, SequenceOption{Type: "INCREMENT BY", Value: *increment, HasValue: true})
	}

	if min != nil {
		switch {
		case *min == defaults.typeMin && defaults.min != defaults.typeMin:
			options = append(options, SequenceOption{Type: "NO MINVALUE"})
		case *min != defaults.min:
			options = append(options, SequenceOption{Type: "MINVALUE", Value: *min, HasValue: true})
		}
	}

	if max != nil && *max != defaults.max {
		options = append(options, SequenceOption{Type: "MAXVALUE", Value: *max, HasValue: true})
	}

	if cache != nil && *cache != defaults.cache {
		options = append(options, SequenceOption{Type: "CACHE", Value: *cache, HasValue: true})
	}

	if cycle != nil && *cycle {
		options = append(options, SequenceOption{Type: "CYCLE"})
	}

	return options
}
