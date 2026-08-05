package migration

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/db"
)

// RecoveryAction is an explicit operator decision for an incomplete
// non-transactional migration.
type RecoveryAction string

const (
	RecoveryActionRetry       RecoveryAction = "retry"
	RecoveryActionMarkApplied RecoveryAction = "mark_applied"
)

// RecoveryOptions identifies one incomplete migration and the operator's
// attested durable progress. ConfirmedThrough is a count, not a zero-based
// index: 0 means no statements are known to have committed.
type RecoveryOptions struct {
	Version          string
	Action           RecoveryAction
	ConfirmedThrough *int
}

func (opts RecoveryOptions) validate() error {
	if opts.Version == "" {
		return fmt.Errorf("recovery version must not be empty")
	}
	switch opts.Action {
	case RecoveryActionRetry:
		if opts.ConfirmedThrough == nil {
			return fmt.Errorf("retry recovery requires an explicit confirmed-through statement count")
		}
		if *opts.ConfirmedThrough < 0 {
			return fmt.Errorf("confirmed-through statement count must not be negative")
		}
	case RecoveryActionMarkApplied:
		if opts.ConfirmedThrough != nil {
			return fmt.Errorf("confirmed-through is only valid with retry recovery")
		}
	default:
		return fmt.Errorf("unsupported recovery action %q", opts.Action)
	}
	return nil
}

// Recover reconciles exactly one incomplete non-transactional migration.
// Normal Apply never calls this implicitly. The complete local inventory and
// immutable row identity must still match before any recovery transition.
func (a *Applier) Recover(ctx context.Context, migrations []Migration, opts RecoveryOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	if err := ValidateInventory(migrations); err != nil {
		return fmt.Errorf("invalid migration inventory: %w", err)
	}

	return db.WithSessionAdvisoryLock(
		ctx,
		a.pool,
		migrationLockName,
		migrationLockTimeout,
		func(lockConn *pgxpool.Conn) error {
			historyExists, err := a.tracker.HistoryExistsWithExecutor(ctx, lockConn)
			if err != nil {
				return err
			}
			if !historyExists {
				return fmt.Errorf("migration history does not exist; there is nothing to recover")
			}
			if err := a.tracker.ValidateSchemaWithExecutor(ctx, lockConn); err != nil {
				return err
			}
			history, err := a.tracker.GetHistoryWithExecutor(ctx, lockConn)
			if err != nil {
				return fmt.Errorf("failed to get migration history for recovery: %w", err)
			}
			migration, candidate, err := validateRecoveryCandidate(migrations, history, opts.Version)
			if err != nil {
				return fmt.Errorf("migration recovery validation failed: %w", err)
			}
			if opts.Action == RecoveryActionRetry {
				confirmed := *opts.ConfirmedThrough
				if confirmed < candidate.LastConfirmedStatement || confirmed >= candidate.StatementCount {
					return fmt.Errorf(
						"--confirmed-through must be between durable progress %d and %d for migration %s",
						candidate.LastConfirmedStatement,
						candidate.StatementCount-1,
						candidate.Version,
					)
				}
			}
			if err := verifyMigrationLock(ctx, lockConn); err != nil {
				return err
			}

			migrationCtx, cancelMigration := context.WithCancelCause(ctx)
			stopHeartbeat := make(chan struct{})
			heartbeatDone := make(chan error, 1)
			go monitorMigrationLock(migrationCtx, lockConn, stopHeartbeat, cancelMigration, heartbeatDone)

			var recoveredHistory []db.MigrationRecord
			recoveryErr := db.WithSessionAdvisoryLock(
				migrationCtx,
				a.pool,
				migrationExecutionLock,
				migrationLockTimeout,
				func(conn *pgxpool.Conn) error {
					if err := a.waitForNoActiveMigrationStatement(migrationCtx); err != nil {
						return err
					}
					if err := a.tracker.ValidateSchemaWithExecutor(migrationCtx, conn); err != nil {
						return fmt.Errorf("migration tracking schema changed before recovery: %w", err)
					}
					baseline, err := a.tracker.GetHistoryWithExecutor(migrationCtx, conn)
					if err != nil {
						return fmt.Errorf("failed to recheck migration history before recovery: %w", err)
					}
					if !migrationHistoriesEqual(history, baseline) {
						return fmt.Errorf("migration history changed after recovery was validated")
					}

					metadata := metadataForMigration(migration)
					switch opts.Action {
					case RecoveryActionMarkApplied:
						if err := a.tracker.MarkRecoveredApplied(migrationCtx, conn, candidate); err != nil {
							return err
						}
						appliedHistory, err := a.tracker.GetHistoryWithExecutor(migrationCtx, conn)
						if err != nil {
							return fmt.Errorf("failed to verify mark-applied recovery: %w", err)
						}
						if err := validateRecoveredAppliedHistoryTransition(baseline, appliedHistory, metadata); err != nil {
							return err
						}
						recoveredHistory = appliedHistory
						return nil

					case RecoveryActionRetry:
						confirmed := *opts.ConfirmedThrough
						if err := a.tracker.MarkRetrying(migrationCtx, conn, candidate, confirmed); err != nil {
							return err
						}
						readyHistory, err := a.tracker.GetHistoryWithExecutor(migrationCtx, conn)
						if err != nil {
							return fmt.Errorf("failed to verify retry recovery transition: %w", err)
						}
						if err := validateRetryHistoryTransition(baseline, readyHistory, metadata, confirmed); err != nil {
							return err
						}
						recoveredHistory, err = a.applyNonTransactionalMigration(
							migrationCtx,
							conn,
							migration,
							readyHistory,
							confirmed,
							false,
						)
						return err
					default:
						return fmt.Errorf("unsupported recovery action %q", opts.Action)
					}
				},
			)

			close(stopHeartbeat)
			heartbeatErr := <-heartbeatDone
			cancelMigration(context.Canceled)
			if heartbeatErr != nil {
				return errors.Join(recoveryErr, heartbeatErr)
			}
			if recoveryErr != nil {
				return recoveryErr
			}
			if err := verifyMigrationLock(ctx, lockConn); err != nil {
				return fmt.Errorf("recovery completed but runner lock state is ambiguous: %w", err)
			}
			if err := a.tracker.ValidateSchemaWithExecutor(ctx, lockConn); err != nil {
				return fmt.Errorf("recovery completed but tracking schema verification failed: %w", err)
			}
			durableHistory, err := a.tracker.GetHistoryWithExecutor(ctx, lockConn)
			if err != nil {
				return fmt.Errorf("recovery completed but durable history verification failed: %w", err)
			}
			if !migrationHistoriesEqual(recoveredHistory, durableHistory) {
				return fmt.Errorf("recovery completed but durable history differs from the verified state")
			}
			return nil
		},
	)
}
