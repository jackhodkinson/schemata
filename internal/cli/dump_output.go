package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// isDumpSchemaFilePath returns true when schemaPath should be treated as a single
// SQL file (path ends with ".sql", case-insensitive). Otherwise it is treated as
// a directory for per-schema dump files.
func isDumpSchemaFilePath(schemaPath string) bool {
	return strings.EqualFold(filepath.Ext(schemaPath), ".sql")
}

// validateDumpSchemaPath checks that the path is compatible with the chosen dump mode.
func validateDumpSchemaPath(schemaPath string, fileMode bool) error {
	info, err := os.Stat(schemaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to inspect schema path: %w", err)
	}
	if fileMode {
		if info.IsDir() {
			return fmt.Errorf("schema path %q is a directory but ends with .sql; remove the directory or choose a different file path", schemaPath)
		}
		return nil
	}
	if !info.IsDir() {
		return fmt.Errorf("schema path %q exists and is not a directory; use a directory path for per-schema dump or a path ending in .sql for a single file", schemaPath)
	}
	return nil
}

// safeSchemaSQLFileName maps a Postgres schema name to a safe base filename (no path separators).
func safeSchemaSQLFileName(name schema.SchemaName) string {
	s := string(name)
	if s == "" {
		return "_empty"
	}
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '/', '\\', 0:
			b.WriteRune('_')
		default:
			if unicode.IsControl(r) {
				b.WriteRune('_')
			} else {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}

func groupObjectsBySchema(objects []schema.DatabaseObject) map[schema.SchemaName][]schema.DatabaseObject {
	m := make(map[schema.SchemaName][]schema.DatabaseObject)
	for _, obj := range objects {
		key := objectmap.Key(obj)
		sn := key.Schema
		m[sn] = append(m[sn], obj)
	}
	for sn := range m {
		objs := m[sn]
		sort.Slice(objs, func(i, j int) bool {
			return schema.ObjectKeyLess(objectmap.Key(objs[i]), objectmap.Key(objs[j]))
		})
		m[sn] = objs
	}
	return m
}

func sortedSchemaNames(groups map[schema.SchemaName][]schema.DatabaseObject) []schema.SchemaName {
	// Try dependency-aware schema ordering first; fall back to lexical if we
	// cannot derive a DAG (e.g. cycles in cross-schema references).
	if ordered, ok := sortSchemasByDependencies(groups); ok {
		return ordered
	}

	names := make([]string, 0, len(groups))
	for n := range groups {
		names = append(names, string(n))
	}
	sort.Strings(names)
	out := make([]schema.SchemaName, len(names))
	for i, n := range names {
		out[i] = schema.SchemaName(n)
	}
	return out
}

func sortSchemasByDependencies(groups map[schema.SchemaName][]schema.DatabaseObject) ([]schema.SchemaName, bool) {
	if len(groups) == 0 {
		return nil, true
	}

	objectMap := make(schema.SchemaObjectMap)
	for _, objs := range groups {
		for _, obj := range objs {
			key := objectmap.Key(obj)
			objectMap[key] = schema.HashedObject{Payload: obj}
		}
	}

	// No objects means no dependency signal; keep lexical behavior.
	if len(objectMap) == 0 {
		return nil, false
	}

	graph := planner.BuildGraph(objectMap)

	// schemaDeps[A][B] means schema A depends on schema B and must be ordered after B.
	schemaDeps := make(map[schema.SchemaName]map[schema.SchemaName]struct{}, len(groups))
	for sn := range groups {
		schemaDeps[sn] = make(map[schema.SchemaName]struct{})
	}

	for key := range objectMap {
		fromSchema := key.Schema
		if _, ok := schemaDeps[fromSchema]; !ok {
			schemaDeps[fromSchema] = make(map[schema.SchemaName]struct{})
		}
		for _, dep := range graph.Dependencies(key) {
			depSchema := dep.Schema
			if depSchema == fromSchema {
				continue
			}
			if _, ok := schemaDeps[depSchema]; !ok {
				schemaDeps[depSchema] = make(map[schema.SchemaName]struct{})
			}
			schemaDeps[fromSchema][depSchema] = struct{}{}
		}
	}

	ordered, ok := topoSortSchemas(schemaDeps)
	if !ok {
		return nil, false
	}
	return ordered, true
}

func topoSortSchemas(schemaDeps map[schema.SchemaName]map[schema.SchemaName]struct{}) ([]schema.SchemaName, bool) {
	// inDegree[sn] is number of schemas this schema depends on.
	inDegree := make(map[schema.SchemaName]int, len(schemaDeps))
	reverse := make(map[schema.SchemaName][]schema.SchemaName, len(schemaDeps))

	for sn := range schemaDeps {
		inDegree[sn] = len(schemaDeps[sn])
	}
	for sn := range schemaDeps {
		for dep := range schemaDeps[sn] {
			reverse[dep] = append(reverse[dep], sn)
		}
	}
	for dep := range reverse {
		sort.Slice(reverse[dep], func(i, j int) bool {
			return reverse[dep][i] < reverse[dep][j]
		})
	}

	var queue []schema.SchemaName
	for sn, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, sn)
		}
	}
	sort.Slice(queue, func(i, j int) bool { return queue[i] < queue[j] })

	var out []schema.SchemaName
	for len(queue) > 0 {
		sn := queue[0]
		queue = queue[1:]
		out = append(out, sn)

		for _, dependent := range reverse[sn] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
				sort.Slice(queue, func(i, j int) bool { return queue[i] < queue[j] })
			}
		}
	}

	if len(out) != len(schemaDeps) {
		return nil, false
	}
	return out, true
}

// writeDumpSingleFile writes all DDL to one file (existing behavior).
func writeDumpSingleFile(schemaPath string, objects []schema.DatabaseObject, ddlGen *planner.DDLGenerator) (int, error) {
	ddl, err := renderDumpObjects(objects, ddlGen)
	if err != nil {
		return 0, err
	}
	if err := writeFileAtomically(schemaPath, []byte(ddl), 0644); err != nil {
		return 0, fmt.Errorf("failed to write schema file: %w", err)
	}
	return 1, nil
}

// writeDumpPerSchemaDir writes one <schema>.sql file per schema bucket under dirPath.
// Creates dirPath if missing. Returns the number of files written.
func writeDumpPerSchemaDir(dirPath string, objects []schema.DatabaseObject, ddlGen *planner.DDLGenerator) (filesWritten int, err error) {
	groups := groupObjectsBySchema(objects)
	names := sortedSchemaNames(groups)
	seenOut := make(map[string]schema.SchemaName)
	rendered := make(map[string][]byte, len(names))

	for _, sn := range names {
		objs := groups[sn]
		if len(objs) == 0 {
			continue
		}
		base := safeSchemaSQLFileName(sn) + ".sql"
		outPath := filepath.Join(dirPath, base)
		if prior, dup := seenOut[outPath]; dup && prior != sn {
			return 0, fmt.Errorf("duplicate output file %q for schemas %q and %q; use distinct schema names or a single-file dump", base, prior, sn)
		}
		seenOut[outPath] = sn
		ddl, err := renderDumpObjects(objs, ddlGen)
		if err != nil {
			return 0, err
		}
		rendered[outPath] = []byte(ddl)
	}

	// Do not create or modify output paths until every object has rendered
	// successfully. This prevents a failed object from producing a partial dump.
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return 0, fmt.Errorf("failed to create schema directory: %w", err)
	}
	for _, sn := range names {
		outPath := filepath.Join(dirPath, safeSchemaSQLFileName(sn)+".sql")
		ddl, ok := rendered[outPath]
		if !ok {
			continue
		}
		if err := writeFileAtomically(outPath, ddl, 0644); err != nil {
			return filesWritten, fmt.Errorf("failed to write schema file %q: %w", outPath, err)
		}
		filesWritten++
	}
	return filesWritten, nil
}

func renderDumpObjects(objects []schema.DatabaseObject, ddlGen *planner.DDLGenerator) (string, error) {
	var ddl strings.Builder
	for _, obj := range objects {
		stmt, err := ddlGen.GenerateCreateStatement(obj)
		if err != nil {
			return "", fmt.Errorf("failed to render %v for dump: %w", objectmap.Key(obj), err)
		}
		ddl.WriteString(stmt)
		ddl.WriteString("\n\n")
	}
	return ddl.String(), nil
}

func writeFileAtomically(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op after a successful rename

	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
