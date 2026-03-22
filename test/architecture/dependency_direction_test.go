package architecture

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

const modulePath = "github.com/jackhodkinson/schemata"

type listedPackage struct {
	ImportPath string   `json:"ImportPath"`
	Imports    []string `json:"Imports"`
	Error      *struct {
		Err string `json:"Err"`
	} `json:"Error"`
}

func TestInternalDependencyDirection(t *testing.T) {
	t.Parallel()

	packages, err := listRepositoryPackages()
	if err != nil {
		t.Fatalf("failed to list repository packages: %v", err)
	}

	var violations []string

	for _, pkg := range packages {
		fromLayer := classifyLayer(pkg.ImportPath)
		if fromLayer == layerExternal {
			continue
		}
		if fromLayer == layerUnknownInternal {
			violations = append(violations, fmt.Sprintf("%s belongs to an unknown internal layer", pkg.ImportPath))
			continue
		}

		for _, imp := range pkg.Imports {
			if !strings.HasPrefix(imp, modulePath+"/") {
				continue
			}

			toLayer := classifyLayer(imp)
			if toLayer == layerExternal {
				continue
			}
			if toLayer == layerUnknownInternal {
				violations = append(violations, fmt.Sprintf("%s imports %s (unknown internal layer)", pkg.ImportPath, imp))
				continue
			}

			if !isAllowedLayerDependency(fromLayer, toLayer) {
				violations = append(violations, fmt.Sprintf("%s (%s) must not import %s (%s)", pkg.ImportPath, fromLayer, imp, toLayer))
			}
		}
	}

	if len(violations) > 0 {
		sort.Strings(violations)
		t.Fatalf("package dependency direction violations:\n- %s", strings.Join(violations, "\n- "))
	}
}

func listRepositoryPackages() ([]listedPackage, error) {
	cmd := exec.Command(
		"go",
		"list",
		"-json",
		modulePath+"/cmd/...",
		modulePath+"/internal/...",
		modulePath+"/pkg/...",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("go list failed: %w\n%s", err, strings.TrimSpace(string(output)))
	}

	decoder := json.NewDecoder(strings.NewReader(string(output)))
	var packages []listedPackage
	for decoder.More() {
		var p listedPackage
		if err := decoder.Decode(&p); err != nil {
			return nil, fmt.Errorf("failed to decode go list output: %w", err)
		}
		if p.Error != nil {
			return nil, fmt.Errorf("go list reported package error for %s: %s", p.ImportPath, p.Error.Err)
		}
		packages = append(packages, p)
	}

	return packages, nil
}

const (
	layerExternal        = "external"
	layerUnknownInternal = "unknown-internal"

	layerCmd       = "cmd"
	layerCLI       = "cli"
	layerApp       = "app"
	layerConfig    = "config"
	layerDB        = "db"
	layerDiffer    = "differ"
	layerMigration = "migration"
	layerNormalize = "normalize"
	layerObjectMap = "objectmap"
	layerParser    = "parser"
	layerPlanner   = "planner"
	layerVersion   = "version"
	layerPkg       = "pkg"
)

func classifyLayer(importPath string) string {
	switch {
	case importPath == modulePath+"/cmd/schemata":
		return layerCmd
	case strings.HasPrefix(importPath, modulePath+"/internal/cli"):
		return layerCLI
	case strings.HasPrefix(importPath, modulePath+"/internal/app"):
		return layerApp
	case strings.HasPrefix(importPath, modulePath+"/internal/config"):
		return layerConfig
	case strings.HasPrefix(importPath, modulePath+"/internal/db"):
		return layerDB
	case strings.HasPrefix(importPath, modulePath+"/internal/differ"):
		return layerDiffer
	case strings.HasPrefix(importPath, modulePath+"/internal/migration"):
		return layerMigration
	case strings.HasPrefix(importPath, modulePath+"/internal/normalize"):
		return layerNormalize
	case strings.HasPrefix(importPath, modulePath+"/internal/objectmap"):
		return layerObjectMap
	case strings.HasPrefix(importPath, modulePath+"/internal/parser"):
		return layerParser
	case strings.HasPrefix(importPath, modulePath+"/internal/planner"):
		return layerPlanner
	case strings.HasPrefix(importPath, modulePath+"/internal/version"):
		return layerVersion
	case strings.HasPrefix(importPath, modulePath+"/pkg/"):
		return layerPkg
	case strings.HasPrefix(importPath, modulePath+"/internal/"):
		return layerUnknownInternal
	default:
		return layerExternal
	}
}

func isAllowedLayerDependency(from, to string) bool {
	allowed := map[string]map[string]bool{
		layerCmd: {
			layerCLI: true,
		},
		layerCLI: {
			layerApp:       true,
			layerConfig:    true,
			layerDB:        true,
			layerDiffer:    true,
			layerMigration: true,
			layerObjectMap: true,
			layerParser:    true,
			layerPlanner:   true,
			layerVersion:   true,
			layerPkg:       true,
		},
		layerApp: {
			layerConfig:    true,
			layerDB:        true,
			layerDiffer:    true,
			layerMigration: true,
			layerObjectMap: true,
			layerParser:    true,
			layerPlanner:   true,
			layerPkg:       true,
		},
		layerConfig: {},
		layerDB: {
			layerConfig:    true,
			layerObjectMap: true,
			layerPkg:       true,
		},
		layerDiffer: {
			layerNormalize: true,
			layerPkg:       true,
		},
		layerMigration: {
			layerDB: true,
		},
		layerNormalize: {
			layerPkg: true,
		},
		layerObjectMap: {
			layerDiffer: true,
			layerPkg:    true,
		},
		layerParser: {
			layerDiffer:    true,
			layerObjectMap: true,
			layerPkg:       true,
		},
		layerPlanner: {
			layerDiffer: true,
			layerPkg:    true,
		},
		layerVersion: {},
		layerPkg: {},
	}

	return allowed[from][to]
}
