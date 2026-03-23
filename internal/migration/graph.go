package migration

import (
	"fmt"
	"sort"
	"strings"
)

// topoSortMigrations returns migrations ordered by their dependency chains
// using Kahn's algorithm. When no dependencies exist, the output is identical
// to sorting by Version ascending (backward compatible).
//
// Dependencies referencing versions not in the input slice are silently
// ignored (assumed to be already applied).
func topoSortMigrations(migrations []Migration) ([]Migration, error) {
	byVersion := make(map[string]*Migration, len(migrations))
	inDegree := make(map[string]int, len(migrations))
	dependedBy := make(map[string][]string, len(migrations))

	for i := range migrations {
		v := migrations[i].Version
		byVersion[v] = &migrations[i]
		inDegree[v] = 0
	}

	// Build edges, ignoring deps not in the input set (already applied).
	for i := range migrations {
		m := &migrations[i]
		for _, dep := range m.DependsOn {
			if _, ok := byVersion[dep]; !ok {
				continue // dependency already applied or external
			}
			inDegree[m.Version]++
			dependedBy[dep] = append(dependedBy[dep], m.Version)
		}
	}

	// Seed queue with zero-dependency migrations, sorted by version.
	var queue []string
	for v, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, v)
		}
	}
	sort.Strings(queue)

	result := make([]Migration, 0, len(migrations))

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, *byVersion[current])

		for _, dependent := range dependedBy[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
				sort.Strings(queue)
			}
		}
	}

	if len(result) != len(migrations) {
		var cycled []string
		for v, deg := range inDegree {
			if deg > 0 {
				cycled = append(cycled, v)
			}
		}
		sort.Strings(cycled)
		return nil, fmt.Errorf("dependency cycle detected involving migrations: %s", strings.Join(cycled, ", "))
	}

	return result, nil
}
