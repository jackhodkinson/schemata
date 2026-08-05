package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/db"
)

const migrationFailureMessageLimit = 2048

// applyNonTransactionalMigration executes one top-level statement per implicit
// PostgreSQL transaction. startAt is the number of statements the durable
// ledger already confirms. When recordRunning is false, expectedHistory must
// already contain the running recovery transition.
func (a *Applier) applyNonTransactionalMigration(
	ctx context.Context,
	conn *pgxpool.Conn,
	migration Migration,
	expectedHistory []db.MigrationRecord,
	startAt int,
	recordRunning bool,
) ([]db.MigrationRecord, error) {
	metadata := metadataForMigration(migration)
	if startAt < 0 || startAt > metadata.StatementCount {
		return nil, fmt.Errorf("invalid non-transactional start progress %d/%d", startAt, metadata.StatementCount)
	}
	if err := verifyExecutionFence(ctx, conn); err != nil {
		return nil, err
	}
	if err := a.waitForNoActiveMigrationStatement(ctx); err != nil {
		return nil, err
	}
	if err := a.tracker.ValidateSchemaWithExecutor(ctx, conn); err != nil {
		return nil, fmt.Errorf("migration tracking schema changed before non-transactional execution: %w", err)
	}
	baseline, err := a.tracker.GetHistoryWithExecutor(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to verify migration history before non-transactional execution: %w", err)
	}
	if !migrationHistoriesEqual(expectedHistory, baseline) {
		return nil, fmt.Errorf("migration history changed after this run planned its non-transactional work")
	}

	runningHistory := baseline
	if recordRunning {
		if startAt != 0 {
			return nil, fmt.Errorf("new non-transactional migration cannot start after statement %d", startAt)
		}
		if err := a.tracker.MarkRunning(ctx, conn, metadata); err != nil {
			return nil, fmt.Errorf("failed to durably mark non-transactional migration as running: %w", err)
		}
		runningHistory, err = a.tracker.GetHistoryWithExecutor(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("failed to verify durable running migration history: %w", err)
		}
		if err := validateRunningHistoryTransition(baseline, runningHistory, metadata); err != nil {
			return nil, err
		}
	} else {
		current, exists := findMigrationRecord(runningHistory, metadata.Version)
		if !exists || current.MigrationMetadata != metadata || current.Status != db.MigrationStatusRunning ||
			current.LastConfirmedStatement != startAt {
			return nil, fmt.Errorf("recovery history row %s is not running at confirmed statement %d", metadata.Version, startAt)
		}
	}

	for i := startAt; i < len(migration.Statements); i++ {
		if err := verifyExecutionFence(ctx, conn); err != nil {
			return nil, err
		}

		statement := migration.Statements[i]
		// Each statement gets a brand-new physical session which is destroyed
		// before the ledger advances. This makes the execution boundary match
		// crash recovery: session-local state created directly, through a called
		// routine, or by a trigger cannot influence a later statement.
		var statementExecutionErr error
		execErr := db.WithSessionAdvisoryLock(
			ctx,
			a.pool,
			migrationStatementLock,
			migrationLockTimeout,
			func(statementConn *pgxpool.Conn) error {
				// The active-statement lock may have waited for an orphaned backend.
				// Re-prove the control fence and exact ledger before sending SQL.
				if err := verifyExecutionFence(ctx, conn); err != nil {
					return err
				}
				if err := a.tracker.ValidateSchemaWithExecutor(ctx, conn); err != nil {
					return fmt.Errorf("migration tracking schema changed before statement %d: %w", i+1, err)
				}
				currentHistory, err := a.tracker.GetHistoryWithExecutor(ctx, conn)
				if err != nil {
					return fmt.Errorf("failed to verify migration history before statement %d: %w", i+1, err)
				}
				if !migrationHistoriesEqual(runningHistory, currentHistory) {
					return fmt.Errorf("migration history changed before non-transactional statement %d", i+1)
				}

				_, statementExecutionErr = statementConn.Exec(ctx, statement)
				return statementExecutionErr
			},
		)
		if execErr != nil {
			statementErr := newStatementExecutionError(migration, i, execErr)
			// Only the migration statement's own PostgreSQL ErrorResponse can
			// prove rollback. Lock acquisition/release, connection destruction,
			// and control-fence errors remain ambiguous even if they happen to
			// wrap a different PgError after the statement committed.
			if statementExecutionErr == nil || !isDefinitePostgresStatementFailure(statementExecutionErr) {
				return nil, fmt.Errorf(
					"%w; PostgreSQL did not provide a statement error, so commit outcome is ambiguous and the migration is intentionally left running for explicit recovery",
					statementErr,
				)
			}
			return nil, a.recordNonTransactionalFailure(
				ctx,
				conn,
				metadata,
				runningHistory,
				i+1,
				statementErr,
			)
		}

		// The statement is committed, but its ledger confirmation is a second
		// autocommitted write. Any failure in this window remains deliberately
		// ambiguous and requires explicit --confirmed-through recovery.
		if err := a.tracker.ValidateSchemaWithExecutor(ctx, conn); err != nil {
			return nil, fmt.Errorf(
				"non-transactional statement %d committed but tracking schema verification failed; explicit recovery is required: %w",
				i+1,
				err,
			)
		}
		historyAfterStatement, err := a.tracker.GetHistoryWithExecutor(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf(
				"non-transactional statement %d committed but history verification failed; explicit recovery is required: %w",
				i+1,
				err,
			)
		}
		if !migrationHistoriesEqual(runningHistory, historyAfterStatement) {
			return nil, fmt.Errorf(
				"non-transactional statement %d committed but modified reserved migration history; explicit reconciliation is required",
				i+1,
			)
		}

		if err := a.tracker.MarkStatementConfirmed(ctx, conn, metadata, i+1); err != nil {
			return nil, fmt.Errorf(
				"non-transactional statement %d committed but its progress could not be confirmed; explicit recovery is required: %w",
				i+1,
				err,
			)
		}
		confirmedHistory, err := a.tracker.GetHistoryWithExecutor(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf(
				"non-transactional statement %d committed but durable progress could not be verified; explicit recovery is required: %w",
				i+1,
				err,
			)
		}
		if err := validateProgressHistoryTransition(runningHistory, confirmedHistory, metadata, i+1); err != nil {
			return nil, err
		}
		runningHistory = confirmedHistory
	}

	if err := verifyExecutionFence(ctx, conn); err != nil {
		return nil, err
	}
	if err := a.tracker.MarkApplied(ctx, conn, metadata); err != nil {
		return nil, fmt.Errorf("all non-transactional statements committed but migration could not be marked applied; explicit recovery is required: %w", err)
	}
	appliedHistory, err := a.tracker.GetHistoryWithExecutor(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("non-transactional migration was marked applied but history verification failed: %w", err)
	}
	if err := validateAppliedHistoryTransition(runningHistory, appliedHistory, metadata); err != nil {
		return nil, err
	}
	if err := a.tracker.ValidateSchemaWithExecutor(ctx, conn); err != nil {
		return nil, fmt.Errorf("non-transactional migration completed but durable tracking schema verification failed: %w", err)
	}
	if err := verifyExecutionFence(ctx, conn); err != nil {
		return nil, fmt.Errorf("non-transactional migration completed but execution fence state is ambiguous: %w", err)
	}

	return appliedHistory, nil
}

// waitForNoActiveMigrationStatement crosses the active-statement lock as a
// barrier. Once it returns while the caller still owns migrationExecutionLock,
// no backend from an earlier, disconnected controller can still be executing
// migration SQL.
func (a *Applier) waitForNoActiveMigrationStatement(ctx context.Context) error {
	if err := db.WithSessionAdvisoryLock(
		ctx,
		a.pool,
		migrationStatementLock,
		migrationLockTimeout,
		func(*pgxpool.Conn) error { return nil },
	); err != nil {
		return fmt.Errorf("failed to cross active migration statement fence: %w", err)
	}
	return nil
}

func isDefinitePostgresStatementFailure(err error) bool {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return false
	}
	severity := postgresError.SeverityUnlocalized
	if severity == "" {
		severity = postgresError.Severity
	}
	// FATAL and PANIC responses close the connection without returning it to
	// ReadyForQuery, so they do not prove the implicit transaction outcome.
	if !strings.EqualFold(severity, "ERROR") {
		return false
	}
	// Connection exceptions and server-shutdown states can be surfaced in an
	// ErrorResponse even though the connection boundary, and therefore command
	// outcome, is not trustworthy. PostgreSQL's dedicated
	// statement_completion_unknown code is ambiguous by definition.
	return postgresError.Code != "40003" &&
		!strings.HasPrefix(postgresError.Code, "08") &&
		!strings.HasPrefix(postgresError.Code, "57P") &&
		!strings.HasPrefix(postgresError.Code, "58")
}

func (a *Applier) recordNonTransactionalFailure(
	ctx context.Context,
	conn *pgxpool.Conn,
	metadata db.MigrationMetadata,
	runningHistory []db.MigrationRecord,
	failedStatement int,
	statementErr error,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), migrationCleanupTimeout)
	defer cancel()

	currentHistory, historyErr := a.tracker.GetHistoryWithExecutor(cleanupCtx, conn)
	if historyErr != nil || !migrationHistoriesEqual(runningHistory, currentHistory) {
		if historyErr == nil {
			historyErr = fmt.Errorf("migration history changed before the failure could be recorded")
		}
		return errors.Join(
			statementErr,
			fmt.Errorf("failure outcome could not be recorded and remains ambiguous; explicit recovery is required: %w", historyErr),
		)
	}

	message, code := migrationFailureDetails(statementErr)
	if err := a.tracker.MarkFailed(cleanupCtx, conn, metadata, failedStatement, message, code); err != nil {
		return errors.Join(
			statementErr,
			fmt.Errorf("failure outcome could not be recorded and remains ambiguous; explicit recovery is required: %w", err),
		)
	}
	failedHistory, err := a.tracker.GetHistoryWithExecutor(cleanupCtx, conn)
	if err != nil {
		return errors.Join(statementErr, fmt.Errorf("failed migration state could not be verified: %w", err))
	}
	if err := validateFailedHistoryTransition(runningHistory, failedHistory, metadata, failedStatement); err != nil {
		return errors.Join(statementErr, err)
	}

	return fmt.Errorf(
		"%w; migration is durably marked failed at statement %d and must be recovered explicitly",
		statementErr,
		failedStatement,
	)
}

func migrationFailureDetails(err error) (string, *string) {
	message := strings.Join(strings.Fields(err.Error()), " ")
	if len(message) > migrationFailureMessageLimit {
		message = message[:migrationFailureMessageLimit]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	if message == "" {
		message = "migration statement failed"
	}

	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code != "" {
		code := postgresError.Code
		return message, &code
	}
	return message, nil
}

func verifyExecutionFence(ctx context.Context, conn *pgxpool.Conn) error {
	owned, err := db.SessionOwnsAdvisoryLock(ctx, conn, migrationExecutionLock)
	if err != nil {
		return fmt.Errorf("failed to verify non-transactional execution fence: %w", err)
	}
	if !owned {
		return fmt.Errorf("non-transactional executor no longer owns advisory lock %q", migrationExecutionLock)
	}
	return nil
}
