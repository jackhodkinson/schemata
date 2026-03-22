package differ

import (
	"sort"
	"strings"
)

// SortAlterChanges sorts alter change strings to a canonical order.
// See docs/engineering/ORDERING_SPEC.md (Phase 3 — AlterOperation.Changes Ordering).
func SortAlterChanges(changes []string) {
	if len(changes) <= 1 {
		return
	}
	sort.SliceStable(changes, func(i, j int) bool {
		ai, aj := changeRank(changes[i]), changeRank(changes[j])
		if ai != aj {
			return ai < aj
		}
		return changes[i] < changes[j]
	})
}

func changeRank(s string) int {
	switch {
	case s == "owner changed":
		return 10
	// Structural / content (non-table)
	case s == "definition changed",
		s == "view type changed",
		s == "security barrier changed",
		s == "check option changed",
		s == "body changed",
		s == "arguments changed",
		s == "return type changed",
		s == "language changed",
		s == "volatility changed",
		s == "strict changed",
		s == "security definer changed",
		s == "parallel safety changed",
		s == "uniqueness changed",
		strings.HasPrefix(s, "method changed"),
		s == "key expressions changed",
		s == "predicate changed",
		s == "include columns changed",
		s == "timing changed",
		s == "events changed",
		s == "for each row changed",
		s == "when condition changed",
		s == "function changed",
		s == "enabled status changed",
		s == "permissive/restrictive changed",
		s == "policy command changed",
		s == "roles changed",
		s == "using expression changed",
		s == "with check expression changed",
		s == "type changed",
		strings.HasPrefix(s, "start value changed"),
		strings.HasPrefix(s, "increment changed"),
		strings.HasPrefix(s, "min value changed"),
		strings.HasPrefix(s, "max value changed"),
		strings.HasPrefix(s, "cache changed"),
		s == "cycle changed",
		s == "owned by changed",
		strings.HasPrefix(s, "enum "),
		s == "base type changed",
		s == "not null constraint changed",
		s == "check constraint changed",
		s == "extension changed":
		return 20
	case strings.HasPrefix(s, "add grant\t"),
		strings.HasPrefix(s, "revoke grant\t"):
		return 110
	}

	// Table-specific ranks
	return tableAlterRank(s)
}

func tableAlterRank(s string) int {
	switch {
	case strings.HasPrefix(s, "add column "):
		return 30
	case strings.HasPrefix(s, "drop column "):
		return 35
	case strings.HasPrefix(s, "alter column "):
		return 40
	case strings.HasPrefix(s, "add primary key"),
		strings.HasPrefix(s, "drop primary key"),
		strings.HasPrefix(s, "primary key columns changed"):
		return 50
	case strings.Contains(s, "primary key") && strings.Contains(s, "changed"):
		return 50
	case strings.HasPrefix(s, "add unique constraint "),
		strings.HasPrefix(s, "drop unique constraint "):
		return 60
	case strings.HasPrefix(s, "unique constraint ") && strings.HasSuffix(s, " validation changed"):
		return 60
	case strings.Contains(s, "unique constraint") && strings.Contains(s, "changed"):
		return 60
	case strings.HasPrefix(s, "add check constraint "),
		strings.HasPrefix(s, "drop check constraint "):
		return 70
	case strings.HasPrefix(s, "check constraint ") && strings.HasSuffix(s, " validation changed"):
		return 70
	case strings.Contains(s, "check constraint") && strings.Contains(s, "changed"):
		return 70
	case strings.HasPrefix(s, "add foreign key "),
		strings.HasPrefix(s, "drop foreign key "):
		return 80
	case strings.HasPrefix(s, "foreign key ") && strings.HasSuffix(s, " validation changed"):
		return 80
	case strings.Contains(s, "foreign key") && strings.Contains(s, "changed"):
		return 80
	case s == "reloptions changed":
		return 90
	case s == "comment changed":
		return 100
	case strings.HasPrefix(s, "add attribute "),
		strings.HasPrefix(s, "drop attribute "),
		strings.Contains(s, "attribute ") && strings.Contains(s, "type changed"):
		return 45
	default:
		return 1000
	}
}
