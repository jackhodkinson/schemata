package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/db"
)

const (
	migrationLockName       = "schemata:migrations"
	migrationExecutionLock  = "schemata:migration-transaction"
	migrationStatementLock  = "schemata:migration-active-statement"
	migrationLockTimeout    = 30 * time.Second
	migrationCleanupTimeout = 5 * time.Second
	migrationHeartbeatEvery = 250 * time.Millisecond
	migrationHeartbeatWait  = 2 * time.Second
)

// ErrHistoryInitializationRequired is returned when a target has no durable
// migration ledger and the caller has not explicitly authorized creating one.
// A missing ledger is ambiguous: it can mean either a first-time database or a
// previously managed database whose history was lost. Normal application must
// never guess which case it is.
var ErrHistoryInitializationRequired = errors.New(
	"migration history table schemata.version does not exist; refusing to initialize history or execute migrations without explicit authorization: use --initialize-history only for a first-time database, or restore the lost migration history before retrying",
)

// ErrHistoryResetAuthorizationRequired is returned when a caller asks to run
// a destructive reset workflow without explicitly authorizing recreation of
// the ledger that reset necessarily removes.
var ErrHistoryResetAuthorizationRequired = errors.New(
	"migration history reset requires explicit authorization: use --initialize-history to acknowledge that the ledger will be removed and recreated",
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
	DryRun            bool
	InitializeHistory bool   // Authorize first-time creation of schemata.version.
	Step              int    // Apply at most N pending migrations. 0 means unlimited.
	ToVersion         string // Apply up to and including this version. Empty means unlimited.
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
	opts.DryRun = opts.DryRun || a.dryRun

	if err := ValidateInventory(migrations); err != nil {
		return fmt.Errorf("invalid migration inventory: %w", err)
	}

	// One dedicated PostgreSQL session owns this lock for the complete run,
	// including the missing-history decision, optional first-run schema
	// creation, history reads, and all migrations. Keeping authorization and
	// schema creation inside this boundary prevents concurrent runners from
	// disagreeing about whether the target is fresh. Dry-run takes the same lock
	// and follows the same authorization path without creating anything.
	return db.WithSessionAdvisoryLock(
		ctx,
		a.pool,
		migrationLockName,
		migrationLockTimeout,
		func(conn *pgxpool.Conn) error {
			return a.applyLocked(ctx, migrations, opts, conn)
		},
	)
}

// ResetAndApply validates the current ledger, runs reset, recreates the ledger,
// and applies migrations while holding the ordinary runner lock throughout.
// It is intended only for explicitly destructive development workflows whose
// reset removes schemata.version. The reset callback may use the pool, but must
// not try to acquire the migration runner lock itself.
func (a *Applier) ResetAndApply(
	ctx context.Context,
	migrations []Migration,
	opts ApplyOptions,
	reset func(context.Context) error,
) error {
	opts.DryRun = opts.DryRun || a.dryRun
	if opts.DryRun {
		return fmt.Errorf("migration reset cannot run in dry-run mode")
	}
	if !opts.InitializeHistory {
		return ErrHistoryResetAuthorizationRequired
	}
	if reset == nil {
		return fmt.Errorf("migration reset callback must not be nil")
	}
	if err := ValidateInventory(migrations); err != nil {
		return fmt.Errorf("invalid migration inventory: %w", err)
	}

	return db.WithSessionAdvisoryLock(
		ctx,
		a.pool,
		migrationLockName,
		migrationLockTimeout,
		func(conn *pgxpool.Conn) error {
			// Validate any existing ledger's shape and internal state before
			// allowing the destructive callback to run.
			historyExists, err := a.tracker.HistoryExistsWithExecutor(ctx, conn)
			if err != nil {
				return err
			}
			if historyExists {
				if err := a.tracker.ValidateSchemaWithExecutor(ctx, conn); err != nil {
					return err
				}
				history, err := a.tracker.GetHistoryWithExecutor(ctx, conn)
				if err != nil {
					return fmt.Errorf("failed to get migration history before reset: %w", err)
				}
				if err := validateHistoryForReset(history); err != nil {
					return fmt.Errorf("migration history validation failed before reset: %w", err)
				}
			}
			if err := verifyMigrationLock(ctx, conn); err != nil {
				return err
			}

			resetCtx, cancelReset := context.WithCancelCause(ctx)
			stopHeartbeat := make(chan struct{})
			heartbeatDone := make(chan error, 1)
			go monitorMigrationLock(resetCtx, conn, stopHeartbeat, cancelReset, heartbeatDone)
			resetErr := reset(resetCtx)
			close(stopHeartbeat)
			heartbeatErr := <-heartbeatDone
			cancelReset(context.Canceled)
			if heartbeatErr != nil {
				return errors.Join(resetErr, heartbeatErr)
			}
			if resetErr != nil {
				return fmt.Errorf("migration reset failed: %w", resetErr)
			}
			if err := verifyMigrationLock(ctx, conn); err != nil {
				return fmt.Errorf("migration reset completed but runner lock state is ambiguous: %w", err)
			}
			return a.applyLocked(ctx, migrations, opts, conn)
		},
	)
}

func (a *Applier) applyLocked(
	ctx context.Context,
	migrations []Migration,
	opts ApplyOptions,
	conn *pgxpool.Conn,
) error {
	historyExists, err := a.tracker.HistoryExistsWithExecutor(ctx, conn)
	if err != nil {
		return err
	}
	if !historyExists {
		if !opts.InitializeHistory {
			return ErrHistoryInitializationRequired
		}
		if opts.DryRun {
			fmt.Println("[DRY RUN] Would initialize migration history table schemata.version")
			return a.applyPending(ctx, migrations, opts, conn, false)
		}
		if err := a.tracker.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("failed to initialize migration tracking schema: %w", err)
		}
		if err := verifyMigrationLock(ctx, conn); err != nil {
			return err
		}
		historyExists, err = a.tracker.HistoryExistsWithExecutor(ctx, conn)
		if err != nil {
			return err
		}
		if !historyExists {
			return fmt.Errorf("migration history initialization completed without creating schemata.version")
		}
	}
	if err := a.tracker.ValidateSchemaWithExecutor(ctx, conn); err != nil {
		return err
	}
	if err := verifyMigrationLock(ctx, conn); err != nil {
		return err
	}
	return a.applyPending(ctx, migrations, opts, conn, true)
}

func (a *Applier) applyPending(
	ctx context.Context,
	migrations []Migration,
	opts ApplyOptions,
	lockConn *pgxpool.Conn,
	historyExists bool,
) error {
	var history []db.MigrationRecord
	if historyExists {
		var err error
		history, err = a.tracker.GetHistoryWithExecutor(ctx, lockConn)
		if err != nil {
			return fmt.Errorf("failed to get migration history: %w", err)
		}
	}

	pending, err := validateMigrationHistory(migrations, history)
	if err != nil {
		return fmt.Errorf("migration history validation failed: %w", err)
	}

	if len(pending) == 0 {
		fmt.Println("No pending migrations")
		return nil
	}

	versions := make([]string, len(migrations))
	for i, migration := range migrations {
		versions[i] = migration.Version
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
		applied, updatedHistory, err := a.applyMigration(ctx, lockConn, sorted[i], opts, history)
		if err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", sorted[i].Version, err)
		}

		if applied {
			history = updatedHistory
			fmt.Printf("Applied migration %s: %s\n", sorted[i].Version, sorted[i].Name)
		}
	}

	return nil
}

// applyMigration applies a single migration in a transaction
func (a *Applier) applyMigration(
	ctx context.Context,
	lockConn *pgxpool.Conn,
	migration Migration,
	opts ApplyOptions,
	expectedHistory []db.MigrationRecord,
) (bool, []db.MigrationRecord, error) {
	if opts.DryRun {
		fmt.Printf("[DRY RUN] Would apply migration %s:\n%s\n", migration.Version, migration.SQL)
		return false, expectedHistory, nil
	}

	if err := verifyMigrationLock(ctx, lockConn); err != nil {
		return false, nil, err
	}

	migrationCtx, cancelMigration := context.WithCancelCause(ctx)
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan error, 1)
	go monitorMigrationLock(migrationCtx, lockConn, stopHeartbeat, cancelMigration, heartbeatDone)

	var updatedHistory []db.MigrationRecord
	var applyErr error
	switch migration.ExecutionMode {
	case ExecutionModeTransactional:
		applyErr = db.WithDedicatedConnection(migrationCtx, a.pool, func(conn *pgxpool.Conn) error {
			var err error
			updatedHistory, err = a.applyTransactionalMigration(
				migrationCtx,
				conn,
				migration,
				expectedHistory,
			)
			return err
		})
	case ExecutionModeNonTransactional:
		// A session-scoped execution fence survives each statement's implicit
		// transaction and prevents a recovering runner from overlapping an
		// orphaned executor whose outer runner lock disappeared.
		applyErr = db.WithSessionAdvisoryLock(
			migrationCtx,
			a.pool,
			migrationExecutionLock,
			migrationLockTimeout,
			func(conn *pgxpool.Conn) error {
				var err error
				updatedHistory, err = a.applyNonTransactionalMigration(
					migrationCtx,
					conn,
					migration,
					expectedHistory,
					0,
					true,
				)
				return err
			},
		)
	default:
		applyErr = fmt.Errorf("unsupported execution mode %q", migration.ExecutionMode)
	}
	close(stopHeartbeat)
	heartbeatErr := <-heartbeatDone
	cancelMigration(context.Canceled)

	if heartbeatErr != nil {
		return false, nil, errors.Join(applyErr, heartbeatErr)
	}
	if applyErr != nil {
		return false, nil, applyErr
	}
	if err := verifyMigrationLock(ctx, lockConn); err != nil {
		return false, nil, fmt.Errorf("migration committed but runner lock state is ambiguous: %w", err)
	}

	return true, updatedHistory, nil
}

func (a *Applier) applyTransactionalMigration(
	ctx context.Context,
	conn *pgxpool.Conn,
	migration Migration,
	expectedHistory []db.MigrationRecord,
) ([]db.MigrationRecord, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), migrationCleanupTimeout)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	// The session lock coordinates whole runs. This second transaction-scoped
	// fences prevent overlapping DDL even if an earlier runner's control
	// session disappeared while a non-transactional statement backend was
	// still finishing. Acquire them in the same order as recovery.
	for _, lockName := range []string{migrationExecutionLock, migrationStatementLock} {
		if _, err := tx.Exec(
			ctx,
			"SELECT pg_advisory_xact_lock($1)",
			db.AdvisoryLockKey(lockName),
		); err != nil {
			return nil, fmt.Errorf("failed to acquire migration transaction fence %q: %w", lockName, err)
		}
	}
	if err := a.tracker.ValidateSchemaWithExecutor(ctx, tx); err != nil {
		return nil, fmt.Errorf("migration tracking schema changed before execution: %w", err)
	}
	baseline, err := a.tracker.GetHistoryWithExecutor(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to verify migration history before execution: %w", err)
	}
	if !migrationHistoriesEqual(expectedHistory, baseline) {
		return nil, fmt.Errorf("migration history changed after this run planned its pending work")
	}

	metadata := metadataForMigration(migration)
	if err := a.tracker.MarkRunning(ctx, tx, metadata); err != nil {
		return nil, fmt.Errorf("failed to mark migration as running: %w", err)
	}
	runningHistory, err := a.tracker.GetHistoryWithExecutor(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to verify running migration history: %w", err)
	}
	if err := validateRunningHistoryTransition(baseline, runningHistory, metadata); err != nil {
		return nil, err
	}

	for i, statement := range migration.Statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return nil, newStatementExecutionError(migration, i, err)
		}
	}
	// Flush ordinary deferred constraints before inspecting the final ledger.
	// Migration files are trusted executable code; a durable post-commit check
	// below also makes unusual commit-time side effects visible rather than
	// reporting silent success.
	if _, err := tx.Exec(ctx, "SET CONSTRAINTS ALL IMMEDIATE"); err != nil {
		return nil, fmt.Errorf("failed while resolving deferred constraints: %w", err)
	}
	if err := a.tracker.ValidateSchemaWithExecutor(ctx, tx); err != nil {
		return nil, fmt.Errorf("migration changed the reserved tracking schema: %w", err)
	}
	historyAfterStatements, err := a.tracker.GetHistoryWithExecutor(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to verify migration history after SQL execution: %w", err)
	}
	if !migrationHistoriesEqual(runningHistory, historyAfterStatements) {
		return nil, fmt.Errorf("migration SQL modified the reserved migration history")
	}

	// Record migration version within the same transaction
	if err := a.tracker.MarkApplied(ctx, tx, metadata); err != nil {
		return nil, fmt.Errorf("failed to mark migration as applied: %w", err)
	}
	appliedHistory, err := a.tracker.GetHistoryWithExecutor(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to verify applied migration history: %w", err)
	}
	if err := validateAppliedHistoryTransition(runningHistory, appliedHistory, metadata); err != nil {
		return nil, err
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	verificationCtx, cancelVerification := context.WithTimeout(
		context.WithoutCancel(ctx),
		migrationCleanupTimeout,
	)
	defer cancelVerification()
	if err := a.tracker.ValidateSchemaWithExecutor(verificationCtx, conn); err != nil {
		return nil, fmt.Errorf("migration committed but durable tracking schema verification failed: %w", err)
	}
	durableHistory, err := a.tracker.GetHistoryWithExecutor(verificationCtx, conn)
	if err != nil {
		return nil, fmt.Errorf("migration committed but durable history verification failed: %w", err)
	}
	if !migrationHistoriesEqual(appliedHistory, durableHistory) {
		return nil, fmt.Errorf("migration committed but durable history differs from the verified transaction state")
	}

	return durableHistory, nil
}

func metadataForMigration(migration Migration) db.MigrationMetadata {
	return db.MigrationMetadata{
		Version:        migration.Version,
		Name:           migration.Name,
		Checksum:       migration.Checksum,
		ExecutionMode:  migration.ExecutionMode,
		StatementCount: len(migration.Statements),
	}
}

func verifyMigrationLock(ctx context.Context, lockConn *pgxpool.Conn) error {
	owned, err := db.SessionOwnsAdvisoryLock(ctx, lockConn, migrationLockName)
	if err != nil {
		return err
	}
	if !owned {
		return fmt.Errorf("runner session no longer owns advisory lock %q", migrationLockName)
	}
	return nil
}

func monitorMigrationLock(
	ctx context.Context,
	lockConn *pgxpool.Conn,
	stop <-chan struct{},
	cancelMigration context.CancelCauseFunc,
	done chan<- error,
) {
	ticker := time.NewTicker(migrationHeartbeatEvery)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			done <- nil
			return
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			heartbeatCtx, cancelHeartbeat := context.WithTimeout(ctx, migrationHeartbeatWait)
			err := verifyMigrationLock(heartbeatCtx, lockConn)
			cancelHeartbeat()
			if err != nil {
				err = fmt.Errorf("migration runner lost its advisory lock: %w", err)
				cancelMigration(err)
				done <- err
				return
			}
		}
	}
}
