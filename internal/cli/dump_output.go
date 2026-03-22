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

// writeDumpSingleFile writes all DDL to one file (existing behavior).
func writeDumpSingleFile(schemaPath string, objects []schema.DatabaseObject, ddlGen *planner.DDLGenerator) (int, error) {
	var ddlStatements []string
	for _, obj := range objects {
		stmt, err := ddlGen.GenerateCreateStatement(obj)
		if err != nil {
			fmt.Printf("Warning: failed to generate DDL for object: %v\n", err)
			continue
		}
		ddlStatements = append(ddlStatements, stmt)
	}
	ddl := ""
	for _, stmt := range ddlStatements {
		ddl += stmt + "\n\n"
	}
	if err := os.WriteFile(schemaPath, []byte(ddl), 0644); err != nil {
		return 0, fmt.Errorf("failed to write schema file: %w", err)
	}
	return 1, nil
}

// writeDumpPerSchemaDir writes one <schema>.sql file per schema bucket under dirPath.
// Creates dirPath if missing. Returns the number of files written.
func writeDumpPerSchemaDir(dirPath string, objects []schema.DatabaseObject, ddlGen *planner.DDLGenerator) (filesWritten int, err error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return 0, fmt.Errorf("failed to create schema directory: %w", err)
	}

	groups := groupObjectsBySchema(objects)
	names := sortedSchemaNames(groups)

	seenOut := make(map[string]schema.SchemaName)

	for _, sn := range names {
		objs := groups[sn]
		if len(objs) == 0 {
			continue
		}
		var ddlStatements []string
		for _, obj := range objs {
			stmt, err := ddlGen.GenerateCreateStatement(obj)
			if err != nil {
				fmt.Printf("Warning: failed to generate DDL for object: %v\n", err)
				continue
			}
			ddlStatements = append(ddlStatements, stmt)
		}
		if len(ddlStatements) == 0 {
			continue
		}
		ddl := ""
		for _, stmt := range ddlStatements {
			ddl += stmt + "\n\n"
		}
		base := safeSchemaSQLFileName(sn) + ".sql"
		outPath := filepath.Join(dirPath, base)
		if prior, dup := seenOut[outPath]; dup && prior != sn {
			return filesWritten, fmt.Errorf("duplicate output file %q for schemas %q and %q; use distinct schema names or a single-file dump", base, prior, sn)
		}
		seenOut[outPath] = sn
		if err := os.WriteFile(outPath, []byte(ddl), 0644); err != nil {
			return filesWritten, fmt.Errorf("failed to write schema file %q: %w", outPath, err)
		}
		filesWritten++
	}
	return filesWritten, nil
}
