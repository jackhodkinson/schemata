package app

import (
	"context"
	"fmt"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/internal/parser"
	"github.com/jackhodkinson/schemata/internal/planner"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

// Service holds application-level orchestration logic used by CLI commands.
type Service struct {
	allowCascade bool
}

func NewService(allowCascade bool) *Service {
	return &Service{allowCascade: allowCascade}
}

func (s *Service) ScanMigrations(migrationsDir string, format string) ([]migration.Migration, error) {
	var scanner interface {
		Scan() ([]migration.Migration, error)
	}
	switch format {
	case "moo":
		scanner = migration.NewMooScanner(migrationsDir)
	default:
		scanner = migration.NewScanner(migrationsDir)
	}
	migrations, err := scanner.Scan()
	if err != nil {
		return nil, fmt.Errorf("failed to scan migrations: %w", err)
	}
	return migrations, nil
}

func (s *Service) ApplyMigrations(ctx context.Context, pool *db.Pool, migrations []migration.Migration, opts migration.ApplyOptions) error {
	applier := migration.NewApplier(pool, opts.DryRun)
	if err := applier.Apply(ctx, migrations, opts); err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	return nil
}

func (s *Service) ParseSchemaPath(schemaPath string) (schema.SchemaObjectMap, error) {
	if schemaPath == "" {
		return nil, fmt.Errorf("no schema path configured")
	}
	p := parser.NewParser()
	desiredSchema, err := p.ParsePath(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema path '%s': %w", schemaPath, err)
	}
	return desiredSchema, nil
}

func (s *Service) ExtractSchemaFromDB(ctx context.Context, pool *db.Pool, cfg *config.Config) (schema.SchemaObjectMap, error) {
	catalog := db.NewCatalog(pool)
	includeSchemas, excludeSchemas := cfg.Schema.GetSchemaFilters()
	actualObjects, err := catalog.ExtractAllObjects(ctx, includeSchemas, excludeSchemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query database schema: %w", err)
	}

	actualSchema, err := s.BuildObjectMapFromObjects(actualObjects)
	if err != nil {
		return nil, fmt.Errorf("failed to build object map: %w", err)
	}

	return actualSchema, nil
}

func (s *Service) ComputeDiff(desired, actual schema.SchemaObjectMap) (*differ.Diff, error) {
	d := differ.NewDiffer()
	diff, err := d.Diff(desired, actual)
	if err != nil {
		return nil, fmt.Errorf("failed to compute diff: %w", err)
	}
	return diff, nil
}

func (s *Service) GenerateDDL(diff *differ.Diff, desired schema.SchemaObjectMap) (string, error) {
	ddlGen := planner.NewDDLGenerator(planner.WithAllowCascade(s.allowCascade))
	ddl, err := ddlGen.GenerateDDL(diff, desired)
	if err != nil {
		return "", fmt.Errorf("failed to generate DDL: %w", err)
	}
	return ddl, nil
}

func (s *Service) CheckMigrationsInSync(ctx context.Context, cfg *config.Config) error {
	if cfg.Dev == nil {
		return fmt.Errorf("no dev database configured")
	}

	devPool, err := db.Connect(ctx, cfg.Dev)
	if err != nil {
		return fmt.Errorf("failed to connect to dev database: %w", err)
	}
	defer devPool.Close()

	if cfg.Migrations.GetDir() != "" {
		migrations, err := s.ScanMigrations(cfg.Migrations.GetDir(), cfg.Migrations.GetFormat())
		if err != nil {
			return err
		}
		if len(migrations) > 0 {
			if err := s.ApplyMigrations(ctx, devPool, migrations, migration.ApplyOptions{}); err != nil {
				return fmt.Errorf("failed to apply migrations to dev: %w", err)
			}
		}
	}

	schemaPath := cfg.Schema.GetSchemaPath()
	desiredSchema, err := s.ParseSchemaPath(schemaPath)
	if err != nil {
		return err
	}

	actualSchema, err := s.ExtractSchemaFromDB(ctx, devPool, cfg)
	if err != nil {
		return fmt.Errorf("failed to query dev database schema: %w", err)
	}

	diff, err := s.ComputeDiff(desiredSchema, actualSchema)
	if err != nil {
		return err
	}

	if !diff.IsEmpty() {
		return fmt.Errorf("migrations are out of sync with configured schema path:\n  %d to create, %d to drop, %d to alter",
			len(diff.ToCreate), len(diff.ToDrop), len(diff.ToAlter))
	}

	return nil
}

func (s *Service) DropAllObjects(ctx context.Context, pool *db.Pool) error {
	query := `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'schemata')
		  AND nspname NOT LIKE 'pg_temp_%'
		  AND nspname NOT LIKE 'pg_toast_temp_%'
		ORDER BY nspname
	`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return fmt.Errorf("failed to scan schema name: %w", err)
		}
		schemas = append(schemas, schemaName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading schema rows: %w", err)
	}

	dropMode := "RESTRICT"
	if s.allowCascade {
		dropMode = "CASCADE"
	}

	for _, schemaName := range schemas {
		dropSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %s %s", schemaName, dropMode)
		if _, err := pool.Exec(ctx, dropSQL); err != nil {
			if !s.allowCascade {
				return fmt.Errorf("failed to drop schema %s: %w\n\nHint: Schema has dependent objects. Use --allow-cascade to drop with CASCADE (this will drop all dependent objects)", schemaName, err)
			}
			return fmt.Errorf("failed to drop schema %s: %w", schemaName, err)
		}

		createSQL := fmt.Sprintf("CREATE SCHEMA %s", schemaName)
		if _, err := pool.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create schema %s: %w", schemaName, err)
		}
	}

	dropTrackingSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS schemata %s", dropMode)
	if _, err := pool.Exec(ctx, dropTrackingSQL); err != nil {
		if !s.allowCascade {
			return fmt.Errorf("failed to drop schemata tracking schema: %w\n\nHint: Schema has dependent objects. Use --allow-cascade to drop with CASCADE (this will drop all dependent objects)", err)
		}
		return fmt.Errorf("failed to drop schemata tracking schema: %w", err)
	}

	return nil
}

func (s *Service) BuildObjectMapFromObjects(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	return objectmap.Build(objects)
}
