package planner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// DependencyGraph represents object dependencies
type DependencyGraph struct {
	// nodes maps ObjectKey to its dependencies (edges pointing TO this node)
	nodes map[schema.ObjectKey][]schema.ObjectKey
	// reverse maps ObjectKey to objects that depend on it (edges FROM this node)
	reverse map[schema.ObjectKey][]schema.ObjectKey
}

// Dependencies returns a copy of the direct dependencies for a key.
func (g *DependencyGraph) Dependencies(key schema.ObjectKey) []schema.ObjectKey {
	deps, ok := g.nodes[key]
	if !ok || len(deps) == 0 {
		return nil
	}
	out := make([]schema.ObjectKey, len(deps))
	copy(out, deps)
	return out
}

// NewDependencyGraph creates a new empty dependency graph
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		nodes:   make(map[schema.ObjectKey][]schema.ObjectKey),
		reverse: make(map[schema.ObjectKey][]schema.ObjectKey),
	}
}

// AddNode adds a node to the graph with no dependencies
func (g *DependencyGraph) AddNode(key schema.ObjectKey) {
	if _, exists := g.nodes[key]; !exists {
		g.nodes[key] = []schema.ObjectKey{}
	}
	if _, exists := g.reverse[key]; !exists {
		g.reverse[key] = []schema.ObjectKey{}
	}
}

// AddDependency adds an edge: 'from' depends on 'to'
func (g *DependencyGraph) AddDependency(from, to schema.ObjectKey) {
	g.AddNode(from)
	g.AddNode(to)
	g.nodes[from] = append(g.nodes[from], to)
	g.reverse[to] = append(g.reverse[to], from)
}

// BuildGraph builds a dependency graph from a set of objects
func BuildGraph(objectMap schema.SchemaObjectMap) *DependencyGraph {
	graph := NewDependencyGraph()

	// Add all nodes first
	for key := range objectMap {
		graph.AddNode(key)
	}

	// Add dependencies based on object types
	for key, objWithHash := range objectMap {
		obj := objWithHash.Payload
		addDependenciesForObject(graph, key, obj, objectMap)
	}

	return graph
}

// addDependenciesForObject adds dependencies for a specific object.
// Table/view/function sequence privileges (Grants) are modeled on the object itself and do not add separate graph nodes.
func addDependenciesForObject(graph *DependencyGraph, key schema.ObjectKey, obj schema.DatabaseObject, objectMap schema.SchemaObjectMap) {
	switch v := obj.(type) {
	case schema.Table:
		// Foreign keys create dependencies on referenced tables
		for _, fk := range v.ForeignKeys {
			refTableKey := schema.ObjectKey{
				Kind:   schema.TableKind,
				Schema: schema.SchemaName(fk.Ref.Schema),
				Name:   string(fk.Ref.Table),
			}
			// Skip self-referential foreign keys (they don't create ordering dependencies)
			if refTableKey.Kind == key.Kind && refTableKey.Schema == key.Schema && refTableKey.Name == key.Name {
				continue
			}
			// Only add dependency if the referenced table is in our object map
			if _, exists := objectMap[refTableKey]; exists {
				graph.AddDependency(key, refTableKey)
			}
		}

		// If table uses custom types or extension-provided types, it depends on those.
		for _, col := range v.Columns {
			addTypeDependencies(graph, key, string(col.Type), objectMap)
		}

	case schema.View:
		// Views depend on tables and other views they reference
		for _, dep := range v.Definition.Dependencies {
			depKey := schema.ObjectKey{
				Kind:   schema.ObjectKind(dep.Kind),
				Schema: schema.SchemaName(dep.Schema),
				Name:   dep.Name,
			}
			if _, exists := objectMap[depKey]; exists {
				graph.AddDependency(key, depKey)
			}
		}

	case schema.Index:
		// Indexes depend on their table
		tableKey := schema.ObjectKey{
			Kind:   schema.TableKind,
			Schema: key.Schema,
			Name:   string(v.Table),
		}
		if _, exists := objectMap[tableKey]; exists {
			graph.AddDependency(key, tableKey)
		}

	case schema.Trigger:
		// Triggers depend on their table and function
		tableKey := schema.ObjectKey{
			Kind:   schema.TableKind,
			Schema: key.Schema,
			Name:   string(v.Table),
		}
		if _, exists := objectMap[tableKey]; exists {
			graph.AddDependency(key, tableKey)
		}

		// Function keys include signature, while trigger references only schema+name.
		// Link trigger to all matching function signatures in the object map.
		for candidate := range objectMap {
			if candidate.Kind != schema.FunctionKind {
				continue
			}
			if candidate.Schema == schema.SchemaName(v.Function.Schema) && candidate.Name == v.Function.Name {
				graph.AddDependency(key, candidate)
			}
		}

	case schema.Policy:
		// Policies depend on their table
		tableKey := schema.ObjectKey{
			Kind:   schema.TableKind,
			Schema: key.Schema,
			Name:   string(v.Table),
		}
		if _, exists := objectMap[tableKey]; exists {
			graph.AddDependency(key, tableKey)
		}

	case schema.Function:
		// Functions depend on types used in arguments and return signatures.
		for _, arg := range v.Args {
			addTypeDependencies(graph, key, string(arg.Type), objectMap)
		}
		switch ret := v.Returns.(type) {
		case schema.ReturnsType:
			addTypeDependencies(graph, key, string(ret.Type), objectMap)
		case schema.ReturnsSetOf:
			addTypeDependencies(graph, key, string(ret.Type), objectMap)
		case schema.ReturnsTable:
			for _, col := range ret.Columns {
				addTypeDependencies(graph, key, string(col.Type), objectMap)
			}
		}

	case schema.Sequence:
		// Sequences typically don't have dependencies on other objects
		// (they're owned by columns, but that's the reverse dependency)

	default:
		// Other object types (extensions, enums, domains, composites, schemas)
		// typically don't have dependencies on other schema objects
	}
}

func addTypeDependencies(graph *DependencyGraph, ownerKey schema.ObjectKey, typeName string, objectMap schema.SchemaObjectMap) {
	if typeName == "" {
		return
	}

	// Check for schema-qualified type (schema.typename)
	if strings.Contains(typeName, ".") {
		parts := strings.Split(typeName, ".")
		if len(parts) == 2 {
			typeKey := schema.ObjectKey{
				Kind:   schema.TypeKind,
				Schema: schema.SchemaName(parts[0]),
				Name:   parts[1],
			}
			if _, exists := objectMap[typeKey]; exists {
				graph.AddDependency(ownerKey, typeKey)
			}
		}
		return
	}

	// Unqualified type: first check for custom type in the owning schema.
	typeKey := schema.ObjectKey{
		Kind:   schema.TypeKind,
		Schema: ownerKey.Schema,
		Name:   typeName,
	}
	if _, exists := objectMap[typeKey]; exists {
		graph.AddDependency(ownerKey, typeKey)
		return
	}

	// Extension-provided type heuristic: if extension name matches the type name,
	// treat objects using that type as depending on that extension.
	for key := range objectMap {
		if key.Kind != schema.ExtensionKind {
			continue
		}
		if strings.EqualFold(key.Name, typeName) {
			graph.AddDependency(ownerKey, key)
		}
	}
}

// TopologicalSort performs a topological sort on the graph
// Returns sorted keys or error if cycle detected
func (g *DependencyGraph) TopologicalSort() ([]schema.ObjectKey, error) {
	// Kahn's algorithm for topological sort

	// Calculate in-degrees (number of dependencies)
	inDegree := make(map[schema.ObjectKey]int)
	for node := range g.nodes {
		inDegree[node] = len(g.nodes[node])
	}

	// Queue of nodes with no dependencies
	var queue []schema.ObjectKey
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	// Sort the queue for deterministic output
	sort.Slice(queue, func(i, j int) bool {
		return compareKeys(queue[i], queue[j])
	})

	var result []schema.ObjectKey

	for len(queue) > 0 {
		// Pop from queue
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		// For each node that depends on current
		for _, dependent := range g.reverse[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
				// Keep queue sorted for deterministic output
				sort.Slice(queue, func(i, j int) bool {
					return compareKeys(queue[i], queue[j])
				})
			}
		}
	}

	// If we haven't processed all nodes, there's a cycle
	if len(result) != len(g.nodes) {
		return nil, fmt.Errorf("circular dependency detected in schema objects")
	}

	return result, nil
}

// ReverseTopologicalSort returns objects in reverse dependency order
// (for DROP operations - drop dependents before their dependencies)
func (g *DependencyGraph) ReverseTopologicalSort() ([]schema.ObjectKey, error) {
	sorted, err := g.TopologicalSort()
	if err != nil {
		return nil, err
	}

	// Reverse the slice
	for i := 0; i < len(sorted)/2; i++ {
		j := len(sorted) - 1 - i
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}

	return sorted, nil
}

// DetectCycle attempts to detect and describe a cycle in the graph
func (g *DependencyGraph) DetectCycle() ([]schema.ObjectKey, error) {
	visited := make(map[schema.ObjectKey]bool)
	recStack := make(map[schema.ObjectKey]bool)
	var path []schema.ObjectKey

	var dfs func(schema.ObjectKey) bool
	dfs = func(node schema.ObjectKey) bool {
		visited[node] = true
		recStack[node] = true
		path = append(path, node)

		for _, dep := range g.nodes[node] {
			if !visited[dep] {
				if dfs(dep) {
					return true
				}
			} else if recStack[dep] {
				// Found cycle - find where it starts in path
				cycleStart := -1
				for i, n := range path {
					if n == dep {
						cycleStart = i
						break
					}
				}
				if cycleStart >= 0 {
					path = append(path[cycleStart:], dep)
					return true
				}
			}
		}

		recStack[node] = false
		path = path[:len(path)-1]
		return false
	}

	for node := range g.nodes {
		if !visited[node] {
			if dfs(node) {
				return path, fmt.Errorf("circular dependency detected")
			}
		}
	}

	return nil, nil
}

// compareKeys provides a deterministic ordering for ObjectKeys.
func compareKeys(a, b schema.ObjectKey) bool {
	return schema.ObjectKeyLess(a, b)
}

// FilterGraphForKeys creates a subgraph containing only the specified keys
// Dependencies are preserved for ordering, but only keys in the input list are included
func FilterGraphForKeys(graph *DependencyGraph, keys []schema.ObjectKey) *DependencyGraph {
	// Track all keys that must be present (requested keys and their dependencies)
	keysSet := make(map[schema.ObjectKey]bool)

	// Explore dependencies for each requested key
	for _, key := range keys {
		collectDependencies(graph, key, keysSet)
	}

	// Build new graph containing the closure
	newGraph := NewDependencyGraph()
	for key := range keysSet {
		newGraph.AddNode(key)
	}

	// Replay dependencies between retained nodes
	for key := range keysSet {
		for _, dep := range graph.nodes[key] {
			if keysSet[dep] {
				newGraph.AddDependency(key, dep)
			}
		}
	}

	return newGraph
}

// collectDependencies performs a DFS collecting a key and all of its dependencies.
func collectDependencies(graph *DependencyGraph, key schema.ObjectKey, visited map[schema.ObjectKey]bool) {
	if visited[key] {
		return
	}
	visited[key] = true

	for _, dep := range graph.nodes[key] {
		collectDependencies(graph, dep, visited)
	}
}
