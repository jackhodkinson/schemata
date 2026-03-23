package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopoSortMigrations_NoDependencies(t *testing.T) {
	// With no dependencies, output should match version-ascending order
	// (identical to current sort.Strings behavior).
	migrations := []Migration{
		{Version: "20231016090000", Name: "third"},
		{Version: "20231015120530", Name: "first"},
		{Version: "20231015130000", Name: "second"},
	}

	sorted, err := topoSortMigrations(migrations)
	require.NoError(t, err)
	require.Len(t, sorted, 3)
	assert.Equal(t, "20231015120530", sorted[0].Version)
	assert.Equal(t, "20231015130000", sorted[1].Version)
	assert.Equal(t, "20231016090000", sorted[2].Version)
}

func TestTopoSortMigrations_LinearChain(t *testing.T) {
	// C depends on B, B depends on A.
	// Version strings are ordered so lexicographic sort would be wrong:
	// A has the latest timestamp but must come first.
	migrations := []Migration{
		{Version: "20231017000000", Name: "c", DependsOn: []string{"20231016000000"}},
		{Version: "20231018000000", Name: "a"},
		{Version: "20231016000000", Name: "b", DependsOn: []string{"20231018000000"}},
	}

	sorted, err := topoSortMigrations(migrations)
	require.NoError(t, err)
	require.Len(t, sorted, 3)
	assert.Equal(t, "20231018000000", sorted[0].Version, "a should be first (no deps)")
	assert.Equal(t, "20231016000000", sorted[1].Version, "b depends on a")
	assert.Equal(t, "20231017000000", sorted[2].Version, "c depends on b")
}

func TestTopoSortMigrations_Diamond(t *testing.T) {
	// A is root. B and C both depend on A. D depends on both B and C.
	// B and C tie-break by version string.
	migrations := []Migration{
		{Version: "20231015000000", Name: "a"},
		{Version: "20231016000000", Name: "b", DependsOn: []string{"20231015000000"}},
		{Version: "20231017000000", Name: "c", DependsOn: []string{"20231015000000"}},
		{Version: "20231018000000", Name: "d", DependsOn: []string{"20231016000000", "20231017000000"}},
	}

	sorted, err := topoSortMigrations(migrations)
	require.NoError(t, err)
	require.Len(t, sorted, 4)
	assert.Equal(t, "20231015000000", sorted[0].Version, "a is root")
	assert.Equal(t, "20231016000000", sorted[1].Version, "b before c (version tie-break)")
	assert.Equal(t, "20231017000000", sorted[2].Version, "c after b")
	assert.Equal(t, "20231018000000", sorted[3].Version, "d depends on both b and c")
}

func TestTopoSortMigrations_CycleDetection(t *testing.T) {
	// A -> B -> C -> A
	migrations := []Migration{
		{Version: "20231015000000", Name: "a", DependsOn: []string{"20231017000000"}},
		{Version: "20231016000000", Name: "b", DependsOn: []string{"20231015000000"}},
		{Version: "20231017000000", Name: "c", DependsOn: []string{"20231016000000"}},
	}

	_, err := topoSortMigrations(migrations)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
	assert.Contains(t, err.Error(), "20231015000000")
	assert.Contains(t, err.Error(), "20231016000000")
	assert.Contains(t, err.Error(), "20231017000000")
}

func TestTopoSortMigrations_Mixed(t *testing.T) {
	// Three independent migrations (no deps) + two with a dependency chain.
	// Independent ones should appear in version order relative to each other.
	// Dependent ones should appear after their dependencies.
	migrations := []Migration{
		{Version: "20231015000000", Name: "independent-a"},
		{Version: "20231016000000", Name: "independent-b"},
		{Version: "20231017000000", Name: "independent-c"},
		{Version: "20231014000000", Name: "chain-base"},
		{Version: "20231019000000", Name: "chain-child", DependsOn: []string{"20231014000000"}},
	}

	sorted, err := topoSortMigrations(migrations)
	require.NoError(t, err)
	require.Len(t, sorted, 5)

	// chain-base (20231014) has no deps, earliest version → first
	assert.Equal(t, "20231014000000", sorted[0].Version)
	// Then the independents in version order
	assert.Equal(t, "20231015000000", sorted[1].Version)
	assert.Equal(t, "20231016000000", sorted[2].Version)
	assert.Equal(t, "20231017000000", sorted[3].Version)
	// chain-child last (depends on chain-base, and has latest version)
	assert.Equal(t, "20231019000000", sorted[4].Version)
}

func TestTopoSortMigrations_AlreadyAppliedDependency(t *testing.T) {
	// Migration B depends on A, but A is not in the input (already applied).
	// Should not error — the unknown dep is silently ignored.
	migrations := []Migration{
		{Version: "20231016000000", Name: "b", DependsOn: []string{"20231015000000"}},
		{Version: "20231017000000", Name: "c"},
	}

	sorted, err := topoSortMigrations(migrations)
	require.NoError(t, err)
	require.Len(t, sorted, 2)
	assert.Equal(t, "20231016000000", sorted[0].Version)
	assert.Equal(t, "20231017000000", sorted[1].Version)
}

func TestTopoSortMigrations_Empty(t *testing.T) {
	sorted, err := topoSortMigrations(nil)
	require.NoError(t, err)
	assert.Empty(t, sorted)
}

func TestTopoSortMigrations_SingleMigration(t *testing.T) {
	migrations := []Migration{
		{Version: "20231015000000", Name: "only"},
	}

	sorted, err := topoSortMigrations(migrations)
	require.NoError(t, err)
	require.Len(t, sorted, 1)
	assert.Equal(t, "20231015000000", sorted[0].Version)
}
