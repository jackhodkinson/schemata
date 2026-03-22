//go:build integration
// +build integration

package integration

import (
	"context"

	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

func resetPublicSchema(ctx context.Context, pool *db.Pool) error {
	_, err := pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS public CASCADE;
		CREATE SCHEMA public;
		GRANT ALL ON SCHEMA public TO postgres;
		GRANT ALL ON SCHEMA public TO public;
	`)
	return err
}

// buildObjectMapFromObjects converts a slice of DatabaseObjects into a SchemaObjectMap
func buildObjectMapFromObjects(objects []schema.DatabaseObject) (schema.SchemaObjectMap, error) {
	return objectmap.Build(objects)
}
