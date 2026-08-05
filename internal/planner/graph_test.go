package planner

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicGraph(t *testing.T) {
	graph := NewDependencyGraph()

	keyA := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "a"}
	keyB := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "b"}

	graph.AddNode(keyA)
	graph.AddNode(keyB)
	graph.AddDependency(keyB, keyA) // B depends on A

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 2)

	// A should come before B
	posA := -1
	posB := -1
	for i, key := range sorted {
		if key == keyA {
			posA = i
		}
		if key == keyB {
			posB = i
		}
	}

	assert.True(t, posA < posB, "A should come before B in sorted order")
}

func TestTopologicalSortNoDependencies(t *testing.T) {
	graph := NewDependencyGraph()

	keys := []schema.ObjectKey{
		{Kind: schema.TableKind, Schema: "public", Name: "table1"},
		{Kind: schema.TableKind, Schema: "public", Name: "table2"},
		{Kind: schema.TableKind, Schema: "public", Name: "table3"},
	}

	for _, key := range keys {
		graph.AddNode(key)
	}

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	assert.Len(t, sorted, 3)
}

func TestTopologicalSortWithChain(t *testing.T) {
	// A <- B <- C (C depends on B, B depends on A)
	graph := NewDependencyGraph()

	keyA := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "a"}
	keyB := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "b"}
	keyC := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "c"}

	graph.AddDependency(keyB, keyA) // B depends on A
	graph.AddDependency(keyC, keyB) // C depends on B

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 3)

	// Find positions
	positions := make(map[schema.ObjectKey]int)
	for i, key := range sorted {
		positions[key] = i
	}

	// A must come before B, B must come before C
	assert.True(t, positions[keyA] < positions[keyB], "A should come before B")
	assert.True(t, positions[keyB] < positions[keyC], "B should come before C")
}

func TestCycleDetection(t *testing.T) {
	// Create a cycle: A -> B -> C -> A
	graph := NewDependencyGraph()

	keyA := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "a"}
	keyB := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "b"}
	keyC := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "c"}

	graph.AddDependency(keyA, keyB)
	graph.AddDependency(keyB, keyC)
	graph.AddDependency(keyC, keyA) // Create cycle

	sorted, err := graph.TopologicalSort()
	assert.Error(t, err, "should detect circular dependency")
	assert.Nil(t, sorted)
	assert.Contains(t, err.Error(), "circular dependency")
}

func TestReverseTopologicalSort(t *testing.T) {
	// A <- B <- C
	graph := NewDependencyGraph()

	keyA := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "a"}
	keyB := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "b"}
	keyC := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "c"}

	graph.AddDependency(keyB, keyA) // B depends on A
	graph.AddDependency(keyC, keyB) // C depends on B

	sorted, err := graph.ReverseTopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 3)

	positions := make(map[schema.ObjectKey]int)
	for i, key := range sorted {
		positions[key] = i
	}

	// For reverse sort (DROP order): C before B before A
	assert.True(t, positions[keyC] < positions[keyB], "C should come before B in reverse order")
	assert.True(t, positions[keyB] < positions[keyA], "B should come before A in reverse order")
}

func TestBuildGraphWithForeignKeys(t *testing.T) {
	// Create users and posts tables where posts references users
	objectMap := schema.SchemaObjectMap{
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "integer", NotNull: true},
				},
				PrimaryKey: &schema.PrimaryKey{
					Name: strPtr("users_pkey"),
					Cols: []schema.ColumnName{"id"},
				},
			},
		},
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "posts"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", Type: "integer", NotNull: true},
					{Name: "user_id", Type: "integer", NotNull: true},
				},
				PrimaryKey: &schema.PrimaryKey{
					Name: strPtr("posts_pkey"),
					Cols: []schema.ColumnName{"id"},
				},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "posts_user_id_fkey",
						Cols: []schema.ColumnName{"user_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "public",
							Table:  "users",
							Cols:   []schema.ColumnName{"id"},
						},
						OnDelete: schema.Cascade,
					},
				},
			},
		},
	}

	graph := BuildGraph(objectMap)

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 2)

	// users should come before posts
	positions := make(map[string]int)
	for i, key := range sorted {
		positions[key.Name] = i
	}

	assert.True(t, positions["users"] < positions["posts"],
		"users table should be created before posts table")
}

func TestBuildGraphWithMultipleForeignKeys(t *testing.T) {
	// Create: orgs <- users <- posts
	// posts depends on users, users depends on orgs
	objectMap := schema.SchemaObjectMap{
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "orgs"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "orgs",
				Columns: []schema.Column{
					{Name: "id", Type: "integer", NotNull: true},
				},
			},
		},
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "integer", NotNull: true},
					{Name: "org_id", Type: "integer", NotNull: true},
				},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "users_org_id_fkey",
						Cols: []schema.ColumnName{"org_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "public",
							Table:  "orgs",
							Cols:   []schema.ColumnName{"id"},
						},
					},
				},
			},
		},
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "posts"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", Type: "integer", NotNull: true},
					{Name: "user_id", Type: "integer", NotNull: true},
				},
				ForeignKeys: []schema.ForeignKey{
					{
						Name: "posts_user_id_fkey",
						Cols: []schema.ColumnName{"user_id"},
						Ref: schema.ForeignKeyRef{
							Schema: "public",
							Table:  "users",
							Cols:   []schema.ColumnName{"id"},
						},
					},
				},
			},
		},
	}

	graph := BuildGraph(objectMap)

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 3)

	positions := make(map[string]int)
	for i, key := range sorted {
		positions[key.Name] = i
	}

	// orgs before users, users before posts
	assert.True(t, positions["orgs"] < positions["users"],
		"orgs should be created before users")
	assert.True(t, positions["users"] < positions["posts"],
		"users should be created before posts")
}

func TestBuildGraphWithViews(t *testing.T) {
	// Create table and view that depends on it
	objectMap := schema.SchemaObjectMap{
		schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "integer"},
					{Name: "email", Type: "text"},
				},
			},
		},
		schema.ObjectKey{Kind: schema.ViewKind, Schema: "public", Name: "user_emails"}: {
			Payload: schema.View{
				Schema: "public",
				Name:   "user_emails",
				Type:   schema.RegularView,
				Definition: schema.ViewDefinition{
					Query: "SELECT email FROM users",
					Dependencies: []schema.ObjectReference{
						{Kind: schema.TableKind, Schema: "public", Name: "users"},
					},
				},
			},
		},
	}

	graph := BuildGraph(objectMap)

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 2)

	positions := make(map[string]int)
	for i, key := range sorted {
		positions[key.Name] = i
	}

	assert.True(t, positions["users"] < positions["user_emails"],
		"table should be created before view that depends on it")
}

func TestFilterGraphForKeys(t *testing.T) {
	// Create: A <- B <- C <- D
	graph := NewDependencyGraph()

	keyA := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "a"}
	keyB := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "b"}
	keyC := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "c"}
	keyD := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "d"}

	graph.AddDependency(keyB, keyA)
	graph.AddDependency(keyC, keyB)
	graph.AddDependency(keyD, keyC)

	// Filter for just D - should include D, C, B, A (all dependencies)
	filtered := FilterGraphForKeys(graph, []schema.ObjectKey{keyD})

	sorted, err := filtered.TopologicalSort()
	require.NoError(t, err)
	assert.Len(t, sorted, 4, "filtered graph should contain D and all its dependencies")

	// Filter for just B - should include B and A
	filtered2 := FilterGraphForKeys(graph, []schema.ObjectKey{keyB})

	sorted2, err := filtered2.TopologicalSort()
	require.NoError(t, err)
	assert.Len(t, sorted2, 2, "filtered graph should contain B and A")
}

func TestDetectCycle(t *testing.T) {
	graph := NewDependencyGraph()

	keyA := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "a"}
	keyB := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "b"}
	keyC := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "c"}

	graph.AddDependency(keyA, keyB)
	graph.AddDependency(keyB, keyC)
	graph.AddDependency(keyC, keyA) // Create cycle

	cycle, err := graph.DetectCycle()
	require.Error(t, err)
	require.NotNil(t, cycle)
	assert.Greater(t, len(cycle), 1, "cycle should contain multiple nodes")
}

func TestBuildGraphOwnedSequenceBasePrecedesTable(t *testing.T) {
	tableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "items"}
	sequenceKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "public", Name: "item_ids"}
	objectMap := schema.SchemaObjectMap{
		tableKey: {Payload: schema.Table{Schema: "public", Name: "items", Columns: []schema.Column{{Name: "id", Type: "integer"}}}},
		sequenceKey: {Payload: schema.Sequence{
			Schema: "public", Name: "item_ids",
			OwnedBy: &schema.SequenceOwner{Schema: "public", Table: "items", Column: "id"},
		}},
	}

	sorted, err := BuildGraph(objectMap).TopologicalSort()
	require.NoError(t, err)
	require.Len(t, sorted, 2)
	assert.Equal(t, sequenceKey, sorted[0])
	assert.Equal(t, tableKey, sorted[1])
}

func TestBuildGraphTableDefaultDependsOnCrossSchemaSequence(t *testing.T) {
	t.Parallel()

	defaultExpr := schema.Expr(`nextval('z_sequences.item_ids'::regclass)`)
	tableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "a_tables", Name: "items"}
	sequenceKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "z_sequences", Name: "item_ids"}
	nearMissKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "z_sequences", Name: "not_item_ids"}
	objectMap := schema.SchemaObjectMap{
		tableKey: {Payload: schema.Table{
			Schema: "a_tables", Name: "items",
			Columns: []schema.Column{{Name: "id", Type: "integer", Default: &defaultExpr}},
		}},
		sequenceKey: {Payload: schema.Sequence{Schema: "z_sequences", Name: "item_ids"}},
		nearMissKey: {Payload: schema.Sequence{Schema: "z_sequences", Name: "not_item_ids"}},
	}

	dependencies := BuildGraph(objectMap).Dependencies(tableKey)
	assert.Contains(t, dependencies, sequenceKey)
	assert.NotContains(t, dependencies, nearMissKey)
	sorted, err := BuildGraph(objectMap).TopologicalSort()
	require.NoError(t, err)
	positions := make(map[schema.ObjectKey]int, len(sorted))
	for index, key := range sorted {
		positions[key] = index
	}
	assert.Less(t, positions[sequenceKey], positions[tableKey])
}

func TestBuildGraphTableDefaultFindsCompoundAndNestedSequenceCalls(t *testing.T) {
	t.Parallel()

	defaultExpr := schema.Expr(`coalesce(
		nextval('z_sequences.first_ids'::regclass),
		abs(nextval('z_sequences.second_ids'::pg_catalog.regclass))
	) + nextval('z_sequences.first_ids'::regclass)`)
	tableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "a_tables", Name: "items"}
	firstKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "z_sequences", Name: "first_ids"}
	secondKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "z_sequences", Name: "second_ids"}
	nearMissKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "z_sequences", Name: "not_first_ids"}
	objectMap := schema.SchemaObjectMap{
		tableKey: {Payload: schema.Table{
			Schema: "a_tables", Name: "items",
			Columns: []schema.Column{{Name: "id", Type: "bigint", Default: &defaultExpr}},
		}},
		firstKey:    {Payload: schema.Sequence{Schema: "z_sequences", Name: "first_ids"}},
		secondKey:   {Payload: schema.Sequence{Schema: "z_sequences", Name: "second_ids"}},
		nearMissKey: {Payload: schema.Sequence{Schema: "z_sequences", Name: "not_first_ids"}},
	}

	dependencies := BuildGraph(objectMap).Dependencies(tableKey)
	assert.ElementsMatch(t, []schema.ObjectKey{firstKey, secondKey}, dependencies)
}

func TestBuildGraphTableDefaultRejectsNestedNextvalNearMisses(t *testing.T) {
	t.Parallel()

	defaultExpr := schema.Expr(`coalesce(
		other.nextval('z_sequences.item_ids'::regclass),
		nextval('z_sequences.item_ids'::text),
		nextval('z_sequences.item_ids'::evil.regclass),
		nextval('z_sequences.not_item_ids'::regclass)
	)`)
	tableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "a_tables", Name: "items"}
	itemKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "z_sequences", Name: "item_ids"}
	nearNameKey := schema.ObjectKey{Kind: schema.SequenceKind, Schema: "z_sequences", Name: "not_item_ids"}
	objectMap := schema.SchemaObjectMap{
		tableKey: {Payload: schema.Table{
			Schema: "a_tables", Name: "items",
			Columns: []schema.Column{{Name: "id", Type: "bigint", Default: &defaultExpr}},
		}},
		itemKey:     {Payload: schema.Sequence{Schema: "z_sequences", Name: "item_ids"}},
		nearNameKey: {Payload: schema.Sequence{Schema: "z_sequences", Name: "not_item_ids"}},
	}

	dependencies := BuildGraph(objectMap).Dependencies(tableKey)
	assert.Equal(t, []schema.ObjectKey{nearNameKey}, dependencies)
	assert.NotContains(t, dependencies, itemKey)
}

// Helper function
func strPtr(s string) *string {
	return &s
}
