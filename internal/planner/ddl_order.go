package planner

import (
	"sort"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// sortedColumns returns a copy of columns sorted by name for deterministic DDL.
func sortedColumns(cols []schema.Column) []schema.Column {
	if len(cols) <= 1 {
		out := make([]schema.Column, len(cols))
		copy(out, cols)
		return out
	}
	out := append([]schema.Column(nil), cols...)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func sortedUniques(u []schema.UniqueConstraint) []schema.UniqueConstraint {
	if len(u) <= 1 {
		out := make([]schema.UniqueConstraint, len(u))
		copy(out, u)
		return out
	}
	out := append([]schema.UniqueConstraint(nil), u...)
	sort.SliceStable(out, func(i, j int) bool {
		ai, aj := out[i].Name, out[j].Name
		if ai != aj {
			return ai < aj
		}
		return uniqueColsKey(out[i].Cols) < uniqueColsKey(out[j].Cols)
	})
	return out
}

func uniqueColsKey(cols []schema.ColumnName) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = string(c)
	}
	return strings.Join(parts, ",")
}

func sortedChecks(c []schema.CheckConstraint) []schema.CheckConstraint {
	if len(c) <= 1 {
		out := make([]schema.CheckConstraint, len(c))
		copy(out, c)
		return out
	}
	out := append([]schema.CheckConstraint(nil), c...)
	sort.SliceStable(out, func(i, j int) bool {
		ai, aj := out[i].Name, out[j].Name
		if ai != aj {
			return ai < aj
		}
		return string(out[i].Expr) < string(out[j].Expr)
	})
	return out
}

func sortedForeignKeys(f []schema.ForeignKey) []schema.ForeignKey {
	if len(f) <= 1 {
		out := make([]schema.ForeignKey, len(f))
		copy(out, f)
		return out
	}
	out := append([]schema.ForeignKey(nil), f...)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func sortedPolicyRoles(roles []string) []string {
	if len(roles) <= 1 {
		out := make([]string, len(roles))
		copy(out, roles)
		return out
	}
	out := append([]string(nil), roles...)
	sort.Strings(out)
	return out
}

func sortedTriggerEvents(ev []schema.TriggerEvent) []schema.TriggerEvent {
	if len(ev) <= 1 {
		out := make([]schema.TriggerEvent, len(ev))
		copy(out, ev)
		return out
	}
	out := append([]schema.TriggerEvent(nil), ev...)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func sortedIncludeColumns(cols []schema.ColumnName) []schema.ColumnName {
	if len(cols) <= 1 {
		out := make([]schema.ColumnName, len(cols))
		copy(out, cols)
		return out
	}
	out := append([]schema.ColumnName(nil), cols...)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}
