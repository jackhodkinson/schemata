package migration

import (
	"context"
	"fmt"

	"github.com/jackhodkinson/schemata/internal/db"
)

// Applier applies migrations to a database
type Applier struct {
	pool    *db.Pool
	tracker *db.MigrationTracker
	dryRun  bool
}

// NewApplier creates a new migration applier
func NewApplier(pool *db.Pool, dryRun bool) *Applier {
	return &Applier{
		pool:    pool,
		tracker: db.NewMigrationTracker(pool),
		dryRun:  dryRun,
	}
}

// ApplyOptions configures migration application
type ApplyOptions struct {
	DryRun          bool
	ContinueOnError bool
}

// Apply applies all pending migrations
func (a *Applier) Apply(ctx context.Context, migrations []Migration, opts ApplyOptions) error {
	// Ensure migration tracking schema exists
	if !opts.DryRun {
		if err := a.tracker.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("failed to ensure migration tracking schema: %w", err)
		}
	}

	// Get pending migrations
	versions := make([]string, len(migrations))
	for i, m := range migrations {
		versions[i] = m.Version
	}

	pending, err := a.tracker.GetPendingVersions(ctx, versions)
	if err != nil {
		return fmt.Errorf("failed to get pending migrations: %w", err)
	}

	if len(pending) == 0 {
		fmt.Println("No pending migrations")
		return nil
	}

	// Apply pending migrations in order
	for _, version := range pending {
		// Find migration
		var migration *Migration
		for i := range migrations {
			if migrations[i].Version == version {
				migration = &migrations[i]
				break
			}
		}

		if migration == nil {
			return fmt.Errorf("migration %s not found in list", version)
		}

		// Load SQL if not already loaded
		if err := migration.LoadSQL(); err != nil {
			return fmt.Errorf("failed to load migration %s: %w", version, err)
		}

		// Apply migration
		if err := a.applyMigration(ctx, *migration, opts); err != nil {
			if opts.ContinueOnError {
				fmt.Printf("Error applying migration %s: %v\n", version, err)
				continue
			}
			return fmt.Errorf("failed to apply migration %s: %w", version, err)
		}

		fmt.Printf("Applied migration %s: %s\n", migration.Version, migration.Name)
	}

	return nil
}

// applyMigration applies a single migration in a transaction
func (a *Applier) applyMigration(ctx context.Context, migration Migration, opts ApplyOptions) error {
	if opts.DryRun {
		fmt.Printf("[DRY RUN] Would apply migration %s:\n%s\n", migration.Version, migration.SQL)
		return nil
	}

	// Start transaction
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Execute migration SQL
	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		return fmt.Errorf("failed to execute migration SQL: %w", err)
	}

	// Record migration version within the same transaction
	if err := a.tracker.MarkApplied(ctx, tx, migration.Version); err != nil {
		return fmt.Errorf("failed to mark migration as applied: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Rollback rolls back the last applied migration
// Note: This requires migrations to have down/undo SQL, which we don't support yet
func (a *Applier) Rollback(ctx context.Context, version string) error {
	// TODO: Implement rollback functionality
	return fmt.Errorf("rollback not yet implemented")
}
