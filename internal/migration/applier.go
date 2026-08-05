package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/db"
)

const (
	migrationLockName    = "schemata:migrations"
	migrationLockTimeout = 30 * time.Second
)

type migrationExecutor interface {
	db.Executor
	Begin(context.Context) (pgx.Tx, error)
}

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
	Step            int    // Apply at most N pending migrations. 0 means unlimited.
	ToVersion       string // Apply up to and including this version. Empty means unlimited.
}

// FilterPendingMigrations applies Step and ToVersion filters to a pending
// version list. allVersions is the complete set of known migration versions
// (used to distinguish "already applied" from "not found" in error messages).
func FilterPendingMigrations(pending, allVersions []string, opts ApplyOptions) ([]string, error) {
	if opts.ToVersion != "" {
		idx := -1
		for i, v := range pending {
			if v == opts.ToVersion {
				idx = i
				break
			}
		}
		if idx == -1 {
			for _, v := range allVersions {
				if v == opts.ToVersion {
					return nil, fmt.Errorf("version %s has already been applied", opts.ToVersion)
				}
			}
			return nil, fmt.Errorf("version %s not found in migrations", opts.ToVersion)
		}
		pending = pending[:idx+1]
	}

	if opts.Step > 0 && opts.Step < len(pending) {
		pending = pending[:opts.Step]
	}

	return pending, nil
}

// Apply applies all pending migrations
func (a *Applier) Apply(ctx context.Context, migrations []Migration, opts ApplyOptions) error {
	if err := ValidateInventory(migrations); err != nil {
		return fmt.Errorf("invalid migration inventory: %w", err)
	}

	if opts.DryRun {
		return a.applyPending(ctx, migrations, opts, a.pool)
	}

	// Ensure migration tracking schema exists before acquiring the migration
	// runner lock. EnsureSchema has its own connection-owned lock for the
	// concurrent first-run case.
	if err := a.tracker.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("failed to ensure migration tracking schema: %w", err)
	}

	// One dedicated PostgreSQL session owns this lock for the complete run,
	// including history reads and all per-migration transactions.
	return db.WithSessionAdvisoryLock(
		ctx,
		a.pool,
		migrationLockName,
		migrationLockTimeout,
		func(conn *pgxpool.Conn) error {
			return a.applyPending(ctx, migrations, opts, conn)
		},
	)
}

func (a *Applier) applyPending(
	ctx context.Context,
	migrations []Migration,
	opts ApplyOptions,
	executor migrationExecutor,
) error {
	// Get pending migrations
	versions := make([]string, len(migrations))
	for i, m := range migrations {
		versions[i] = m.Version
	}

	pending, err := a.tracker.GetPendingVersionsWithExecutor(ctx, executor, versions)
	if err != nil {
		return fmt.Errorf("failed to get pending migrations: %w", err)
	}

	if len(pending) == 0 {
		fmt.Println("No pending migrations")
		return nil
	}

	// Build filtered list of pending migrations and load their SQL
	// (needed upfront to parse dependency directives before sorting).
	pendingSet := make(map[string]bool, len(pending))
	for _, v := range pending {
		pendingSet[v] = true
	}

	var pendingMigrations []Migration
	for i := range migrations {
		if !pendingSet[migrations[i].Version] {
			continue
		}
		if err := migrations[i].LoadSQL(); err != nil {
			return fmt.Errorf("failed to load migration %s: %w", migrations[i].Version, err)
		}
		pendingMigrations = append(pendingMigrations, migrations[i])
	}

	// Sort respecting dependency chains. Falls back to version-string
	// ordering when no dependencies are declared.
	sorted, err := topoSortMigrations(pendingMigrations)
	if err != nil {
		return fmt.Errorf("failed to resolve migration ordering: %w", err)
	}

	// Apply step/to-version filters to the sorted list.
	sortedVersions := make([]string, len(sorted))
	for i, m := range sorted {
		sortedVersions[i] = m.Version
	}
	filteredVersions, err := FilterPendingMigrations(sortedVersions, versions, opts)
	if err != nil {
		return err
	}
	if len(filteredVersions) < len(sorted) {
		filteredSet := make(map[string]bool, len(filteredVersions))
		for _, v := range filteredVersions {
			filteredSet[v] = true
		}
		filtered := make([]Migration, 0, len(filteredVersions))
		for i := range sorted {
			if filteredSet[sorted[i].Version] {
				filtered = append(filtered, sorted[i])
			}
		}
		sorted = filtered
	}

	// Apply migrations in resolved order
	for i := range sorted {
		applied, err := a.applyMigration(ctx, executor, sorted[i], opts)
		if err != nil {
			if opts.ContinueOnError {
				fmt.Printf("Error applying migration %s: %v\n", sorted[i].Version, err)
				continue
			}
			return fmt.Errorf("failed to apply migration %s: %w", sorted[i].Version, err)
		}

		if applied {
			fmt.Printf("Applied migration %s: %s\n", sorted[i].Version, sorted[i].Name)
		}
	}

	return nil
}

// applyMigration applies a single migration in a transaction
func (a *Applier) applyMigration(
	ctx context.Context,
	executor migrationExecutor,
	migration Migration,
	opts ApplyOptions,
) (bool, error) {
	if opts.DryRun {
		fmt.Printf("[DRY RUN] Would apply migration %s:\n%s\n", migration.Version, migration.SQL)
		return false, nil
	}

	// Start transaction
	tx, err := executor.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Re-check within the locked transaction to avoid double-applying in races.
	var alreadyApplied bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schemata.version WHERE version_num = $1)", migration.Version).Scan(&alreadyApplied); err != nil {
		return false, fmt.Errorf("failed to check migration tracking table: %w", err)
	}
	if alreadyApplied {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit no-op transaction: %w", err)
		}
		return false, nil
	}

	// Execute migration SQL
	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		return false, fmt.Errorf("failed to execute migration SQL: %w", err)
	}

	// Record migration version within the same transaction
	if err := a.tracker.MarkApplied(ctx, tx, migration.Version); err != nil {
		return false, fmt.Errorf("failed to mark migration as applied: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}
