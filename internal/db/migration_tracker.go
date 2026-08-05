package db

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	MigrationStatusRunning = "running"
	MigrationStatusApplied = "applied"
	MigrationStatusFailed  = "failed"
)

var expectedVersionColumns = []string{
	"attempt_count:integer:NO:1",
	"checksum:text:NO:<none>",
	"error_code:text:YES:<none>",
	"error_message:text:YES:<none>",
	"execution_mode:text:NO:<none>",
	"failed_statement:integer:YES:<none>",
	"finished_at:timestamp with time zone:YES:<none>",
	"last_confirmed_statement:integer:NO:0",
	"name:text:NO:<none>",
	"recovered_at:timestamp with time zone:YES:<none>",
	"recovery_action:text:YES:<none>",
	"started_at:timestamp with time zone:NO:clock_timestamp()",
	"statement_count:integer:NO:<none>",
	"status:text:NO:<none>",
	"version_num:text:NO:<none>",
}

var expectedVersionConstraints = []string{
	"version_attempt_count:c:true:CHECK (attempt_count > 0)",
	"version_checksum_format:c:true:CHECK (checksum ~ '^[0-9a-f]{64}$'::text)",
	"version_execution_mode:c:true:CHECK (execution_mode = ANY (ARRAY['transactional'::text, 'non_transactional'::text]))",
	"version_failed_statement_range:c:true:CHECK (failed_statement IS NULL OR failed_statement >= 1 AND failed_statement <= statement_count)",
	"version_last_confirmed_range:c:true:CHECK (last_confirmed_statement >= 0 AND last_confirmed_statement <= statement_count)",
	"version_pkey:p:true:PRIMARY KEY (version_num)",
	"version_recovery_action:c:true:CHECK (recovery_action = ANY (ARRAY['mark_applied'::text, 'retry'::text]))",
	"version_recovery_pair:c:true:CHECK ((recovered_at IS NULL) = (recovery_action IS NULL))",
	"version_statement_count:c:true:CHECK (statement_count >= 0)",
	"version_status:c:true:CHECK (status = ANY (ARRAY['running'::text, 'applied'::text, 'failed'::text]))",
	"version_status_state:c:true:CHECK (status = 'running'::text AND finished_at IS NULL AND failed_statement IS NULL AND error_message IS NULL AND error_code IS NULL OR status = 'applied'::text AND finished_at IS NOT NULL AND last_confirmed_statement = statement_count AND failed_statement IS NULL AND error_message IS NULL AND error_code IS NULL OR status = 'failed'::text AND finished_at IS NOT NULL AND failed_statement IS NOT NULL AND last_confirmed_statement < failed_statement AND error_message IS NOT NULL)",
}

// MigrationMetadata is the immutable identity recorded for a migration.
type MigrationMetadata struct {
	Version        string
	Name           string
	Checksum       string
	ExecutionMode  string
	StatementCount int
}

// MigrationRecord is one durable migration-history row.
type MigrationRecord struct {
	MigrationMetadata
	Status                 string
	StartedAt              time.Time
	FinishedAt             *time.Time
	ErrorMessage           *string
	ErrorCode              *string
	LastConfirmedStatement int
	FailedStatement        *int
	AttemptCount           int
	RecoveredAt            *time.Time
	RecoveryAction         *string
}

const (
	schemaName          = "schemata"
	tableName           = "version"
	versionSchemaMarker = "schemata:migration-history:v1"

	ensureSchemaLockName    = "schemata:ensure-schema"
	ensureSchemaLockTimeout = 30 * time.Second
	ensureCleanupTimeout    = 5 * time.Second
)

// MigrationTracker manages migration version tracking in the database
type MigrationTracker struct {
	pool *Pool
}

// NewMigrationTracker creates a new migration tracker
func NewMigrationTracker(pool *Pool) *MigrationTracker {
	return &MigrationTracker{pool: pool}
}

// EnsureSchema creates the schemata schema and version table if they don't exist.
// This is safe to call concurrently from multiple processes.
func (mt *MigrationTracker) EnsureSchema(ctx context.Context) error {
	return WithSessionAdvisoryLock(
		ctx,
		mt.pool,
		ensureSchemaLockName,
		ensureSchemaLockTimeout,
		func(conn *pgxpool.Conn) error {
			tx, err := conn.Begin(ctx)
			if err != nil {
				return fmt.Errorf("failed to begin migration tracking schema transaction: %w", err)
			}
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ensureCleanupTimeout)
				defer cancel()
				_ = tx.Rollback(cleanupCtx)
			}()

			if err := mt.ensureSchema(ctx, tx); err != nil {
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("failed to commit migration tracking schema: %w", err)
			}
			return nil
		},
	)
}

func (mt *MigrationTracker) ensureSchema(ctx context.Context, executor Executor) error {
	// PostgreSQL's CREATE SCHEMA IF NOT EXISTS can still race internally, so
	// EnsureSchema serializes this block with a session advisory lock.
	_, err := executor.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	var tableExists bool
	if err := executor.QueryRow(ctx, "SELECT to_regclass('schemata.version') IS NOT NULL").Scan(&tableExists); err != nil {
		return fmt.Errorf("failed to check migration tracking table: %w", err)
	}
	if tableExists {
		return mt.validateSchema(ctx, executor)
	}

	// This is the final pre-release tracking format. Existing installations of
	// older development builds must recreate the internal tracking schema; no
	// released version wrote the previous one-column format.
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s.%s (
			version_num TEXT CONSTRAINT version_pkey PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL CONSTRAINT version_checksum_format CHECK (checksum ~ '^[0-9a-f]{64}$'),
			execution_mode TEXT NOT NULL CONSTRAINT version_execution_mode CHECK (execution_mode IN ('transactional', 'non_transactional')),
			status TEXT NOT NULL CONSTRAINT version_status CHECK (status IN ('running', 'applied', 'failed')),
			started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			finished_at TIMESTAMPTZ,
			statement_count INTEGER NOT NULL CONSTRAINT version_statement_count CHECK (statement_count >= 0),
			last_confirmed_statement INTEGER NOT NULL DEFAULT 0,
			failed_statement INTEGER,
			error_message TEXT,
			error_code TEXT,
			attempt_count INTEGER NOT NULL DEFAULT 1 CONSTRAINT version_attempt_count CHECK (attempt_count > 0),
			recovered_at TIMESTAMPTZ,
			recovery_action TEXT CONSTRAINT version_recovery_action CHECK (recovery_action IN ('mark_applied', 'retry')),
			CONSTRAINT version_last_confirmed_range CHECK (last_confirmed_statement BETWEEN 0 AND statement_count),
			CONSTRAINT version_failed_statement_range CHECK (failed_statement IS NULL OR failed_statement BETWEEN 1 AND statement_count),
			CONSTRAINT version_recovery_pair CHECK ((recovered_at IS NULL) = (recovery_action IS NULL)),
			CONSTRAINT version_status_state CHECK (
				(status = 'running'
					AND finished_at IS NULL
					AND failed_statement IS NULL
					AND error_message IS NULL
					AND error_code IS NULL)
				OR (status = 'applied'
					AND finished_at IS NOT NULL
					AND last_confirmed_statement = statement_count
					AND failed_statement IS NULL
					AND error_message IS NULL
					AND error_code IS NULL)
				OR (status = 'failed'
					AND finished_at IS NOT NULL
					AND failed_statement IS NOT NULL
					AND last_confirmed_statement < failed_statement
					AND error_message IS NOT NULL)
			)
		)
	`, schemaName, tableName)

	_, err = executor.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create version table: %w", err)
	}
	if _, err := executor.Exec(ctx, fmt.Sprintf(
		"COMMENT ON TABLE %s.%s IS '%s'",
		schemaName,
		tableName,
		versionSchemaMarker,
	)); err != nil {
		return fmt.Errorf("failed to mark migration tracking schema version: %w", err)
	}

	if err := mt.validateSchema(ctx, executor); err != nil {
		return err
	}

	return nil
}

func (mt *MigrationTracker) validateSchema(ctx context.Context, executor Executor) error {
	var marker *string
	if err := executor.QueryRow(
		ctx,
		"SELECT obj_description(to_regclass('schemata.version'), 'pg_class')",
	).Scan(&marker); err != nil {
		return fmt.Errorf("failed to inspect migration tracking schema version: %w", err)
	}
	if marker == nil || *marker != versionSchemaMarker {
		found := "<none>"
		if marker != nil {
			found = *marker
		}
		return fmt.Errorf(
			"unsupported pre-release migration tracking schema %s.%s: expected marker %q, found %q; recreate the internal schemata schema before applying migrations",
			schemaName,
			tableName,
			versionSchemaMarker,
			found,
		)
	}

	var relKind, persistence, replicaIdentity, accessMethod, relOptions string
	var rowSecurity, forceRowSecurity, isPartition bool
	var triggerCount, ruleCount, policyCount, parentCount, childCount int64
	if err := executor.QueryRow(ctx, `
		SELECT cls.relkind::text,
		       cls.relpersistence::text,
		       cls.relrowsecurity,
		       cls.relforcerowsecurity,
		       cls.relispartition,
		       cls.relreplident::text,
		       am.amname,
		       COALESCE(pg_catalog.array_to_string(cls.reloptions, ','), ''),
		       (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid = cls.oid AND NOT tgisinternal),
		       (SELECT count(*) FROM pg_catalog.pg_rewrite WHERE ev_class = cls.oid),
		       (SELECT count(*) FROM pg_catalog.pg_policy WHERE polrelid = cls.oid),
		       (SELECT count(*) FROM pg_catalog.pg_inherits WHERE inhrelid = cls.oid),
		       (SELECT count(*) FROM pg_catalog.pg_inherits WHERE inhparent = cls.oid)
		FROM pg_catalog.pg_class AS cls
		JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace
		LEFT JOIN pg_catalog.pg_am AS am ON am.oid = cls.relam
		WHERE nsp.nspname = $1 AND cls.relname = $2
	`, schemaName, tableName).Scan(
		&relKind,
		&persistence,
		&rowSecurity,
		&forceRowSecurity,
		&isPartition,
		&replicaIdentity,
		&accessMethod,
		&relOptions,
		&triggerCount,
		&ruleCount,
		&policyCount,
		&parentCount,
		&childCount,
	); err != nil {
		return fmt.Errorf("failed to inspect migration tracking table behavior: %w", err)
	}
	if relKind != "r" || persistence != "p" || rowSecurity || forceRowSecurity || isPartition ||
		replicaIdentity != "d" || accessMethod != "heap" || relOptions != "" ||
		triggerCount != 0 || ruleCount != 0 || policyCount != 0 || parentCount != 0 || childCount != 0 {
		return fmt.Errorf(
			"unsupported behavior on migration tracking table %s.%s: relkind=%q persistence=%q row_security=%t force_row_security=%t partition=%t replica_identity=%q access_method=%q reloptions=%q triggers=%d rules=%d policies=%d parents=%d children=%d",
			schemaName,
			tableName,
			relKind,
			persistence,
			rowSecurity,
			forceRowSecurity,
			isPartition,
			replicaIdentity,
			accessMethod,
			relOptions,
			triggerCount,
			ruleCount,
			policyCount,
			parentCount,
			childCount,
		)
	}

	rows, err := executor.Query(ctx, `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY column_name
	`, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("failed to inspect migration tracking schema: %w", err)
	}
	defer rows.Close()

	var actual []string
	for rows.Next() {
		var column, dataType, nullable string
		var defaultValue *string
		if err := rows.Scan(&column, &dataType, &nullable, &defaultValue); err != nil {
			return fmt.Errorf("failed to inspect migration tracking column: %w", err)
		}
		defaultText := "<none>"
		if defaultValue != nil {
			defaultText = *defaultValue
		}
		actual = append(actual, fmt.Sprintf("%s:%s:%s:%s", column, dataType, nullable, defaultText))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to inspect migration tracking columns: %w", err)
	}

	if !slices.Equal(actual, expectedVersionColumns) {
		return fmt.Errorf(
			"unsupported pre-release migration tracking schema %s.%s: expected columns [%s], found [%s]; recreate the internal schemata schema before applying migrations",
			schemaName,
			tableName,
			strings.Join(expectedVersionColumns, ", "),
			strings.Join(actual, ", "),
		)
	}

	constraintRows, err := executor.Query(ctx, `
		SELECT con.conname, con.contype::text, con.convalidated,
		       pg_catalog.pg_get_constraintdef(con.oid, true)
		FROM pg_catalog.pg_constraint AS con
		JOIN pg_catalog.pg_class AS cls ON cls.oid = con.conrelid
		JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace
		WHERE nsp.nspname = $1 AND cls.relname = $2
		ORDER BY con.conname
	`, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("failed to inspect migration tracking constraints: %w", err)
	}
	defer constraintRows.Close()

	var constraints []string
	for constraintRows.Next() {
		var name, constraintType, definition string
		var validated bool
		if err := constraintRows.Scan(&name, &constraintType, &validated, &definition); err != nil {
			return fmt.Errorf("failed to inspect migration tracking constraint: %w", err)
		}
		constraints = append(constraints, fmt.Sprintf(
			"%s:%s:%t:%s",
			name,
			constraintType,
			validated,
			definition,
		))
	}
	if err := constraintRows.Err(); err != nil {
		return fmt.Errorf("failed to inspect migration tracking constraints: %w", err)
	}
	if !slices.Equal(constraints, expectedVersionConstraints) {
		return fmt.Errorf(
			"unsupported pre-release migration tracking constraints on %s.%s: expected [%s], found [%s]",
			schemaName,
			tableName,
			strings.Join(expectedVersionConstraints, ", "),
			strings.Join(constraints, ", "),
		)
	}

	return nil
}

// ValidateSchemaWithExecutor verifies the tracking table shape without
// creating or changing it.
func (mt *MigrationTracker) ValidateSchemaWithExecutor(ctx context.Context, executor Executor) error {
	return mt.validateSchema(ctx, executor)
}

// HistoryExistsWithExecutor reports whether the tracking table exists without
// creating it. This keeps dry-run planning side-effect free on a fresh target.
func (mt *MigrationTracker) HistoryExistsWithExecutor(ctx context.Context, executor Executor) (bool, error) {
	var exists bool
	if err := executor.QueryRow(ctx, "SELECT to_regclass('schemata.version') IS NOT NULL").Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check migration tracking table: %w", err)
	}
	return exists, nil
}

// GetHistoryWithExecutor returns the complete durable migration history.
func (mt *MigrationTracker) GetHistoryWithExecutor(ctx context.Context, executor Executor) ([]MigrationRecord, error) {
	query := fmt.Sprintf(`
		SELECT version_num, name, checksum, execution_mode, statement_count,
		       status, started_at, finished_at, last_confirmed_statement,
		       failed_statement, error_message, error_code, attempt_count,
		       recovered_at, recovery_action
		FROM %s.%s
		ORDER BY version_num
	`, schemaName, tableName)

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query migration history: %w", err)
	}
	defer rows.Close()

	var history []MigrationRecord
	for rows.Next() {
		var record MigrationRecord
		if err := rows.Scan(
			&record.Version,
			&record.Name,
			&record.Checksum,
			&record.ExecutionMode,
			&record.StatementCount,
			&record.Status,
			&record.StartedAt,
			&record.FinishedAt,
			&record.LastConfirmedStatement,
			&record.FailedStatement,
			&record.ErrorMessage,
			&record.ErrorCode,
			&record.AttemptCount,
			&record.RecoveredAt,
			&record.RecoveryAction,
		); err != nil {
			return nil, fmt.Errorf("failed to scan migration history: %w", err)
		}
		history = append(history, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration history: %w", err)
	}

	return history, nil
}

// GetAppliedVersions returns all applied migration versions
func (mt *MigrationTracker) GetAppliedVersions(ctx context.Context) ([]string, error) {
	return mt.GetAppliedVersionsWithExecutor(ctx, mt.pool)
}

// GetAppliedVersionsWithExecutor returns applied versions using a specific
// connection or transaction. This is used while a session advisory lock is
// held so history reads cannot escape to another pooled connection.
func (mt *MigrationTracker) GetAppliedVersionsWithExecutor(ctx context.Context, executor Executor) ([]string, error) {
	query := fmt.Sprintf(
		"SELECT version_num FROM %s.%s WHERE status = 'applied' ORDER BY version_num",
		schemaName,
		tableName,
	)

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied versions: %w", err)
	}
	defer rows.Close()

	var versions []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("failed to scan version: %w", err)
		}
		versions = append(versions, version)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating versions: %w", err)
	}

	return versions, nil
}

// MarkRunning records the start time and immutable identity for a migration.
// For transactional migrations this insert occurs inside the migration
// transaction and disappears automatically if the migration rolls back.
func (mt *MigrationTracker) MarkRunning(
	ctx context.Context,
	executor Executor,
	metadata MigrationMetadata,
) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s (
			version_num, name, checksum, execution_mode, statement_count,
			status, started_at, last_confirmed_statement
		) VALUES (
			$1, $2, $3, $4, $5,
			'running', clock_timestamp(), 0
		)
	`, schemaName, tableName)

	_, err := executor.Exec(
		ctx,
		query,
		metadata.Version,
		metadata.Name,
		metadata.Checksum,
		metadata.ExecutionMode,
		metadata.StatementCount,
	)
	if err != nil {
		return fmt.Errorf("failed to mark migration as running: %w", err)
	}

	return nil
}

// MarkApplied completes a previously running migration. Immutable metadata is
// repeated in the predicate so a stale or mismatched caller cannot complete a
// different history row.
func (mt *MigrationTracker) MarkApplied(
	ctx context.Context,
	executor Executor,
	metadata MigrationMetadata,
) error {
	query := fmt.Sprintf(`
		UPDATE %s.%s
		SET status = 'applied',
		    finished_at = clock_timestamp(),
		    last_confirmed_statement = statement_count
		WHERE version_num = $1
		  AND name = $2
		  AND checksum = $3
		  AND execution_mode = $4
		  AND statement_count = $5
		  AND status = 'running'
	`, schemaName, tableName)

	tag, err := executor.Exec(
		ctx,
		query,
		metadata.Version,
		metadata.Name,
		metadata.Checksum,
		metadata.ExecutionMode,
		metadata.StatementCount,
	)
	if err != nil {
		return fmt.Errorf("failed to mark migration as applied: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("failed to mark migration as applied: running history row did not match immutable metadata")
	}

	return nil
}

// GetPendingVersions returns versions that haven't been applied yet
func (mt *MigrationTracker) GetPendingVersions(ctx context.Context, availableVersions []string) ([]string, error) {
	return mt.GetPendingVersionsWithExecutor(ctx, mt.pool, availableVersions)
}

// GetPendingVersionsWithExecutor computes pending versions using a specific
// connection or transaction.
func (mt *MigrationTracker) GetPendingVersionsWithExecutor(
	ctx context.Context,
	executor Executor,
	availableVersions []string,
) ([]string, error) {
	appliedVersions, err := mt.GetAppliedVersionsWithExecutor(ctx, executor)
	if err != nil {
		return nil, err
	}

	// Create set of applied versions for efficient lookup
	appliedSet := make(map[string]bool)
	for _, v := range appliedVersions {
		appliedSet[v] = true
	}

	// Find pending versions
	var pending []string
	for _, v := range availableVersions {
		if !appliedSet[v] {
			pending = append(pending, v)
		}
	}

	// Sort pending versions
	sort.Strings(pending)

	return pending, nil
}
