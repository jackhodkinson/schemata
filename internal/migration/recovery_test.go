package migration

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func incompleteRecoveryRecord(migration Migration, status string, confirmed int) db.MigrationRecord {
	startedAt := time.Now().Add(-time.Minute)
	record := db.MigrationRecord{
		MigrationMetadata:      metadataForMigration(migration),
		Status:                 status,
		StartedAt:              startedAt,
		LastConfirmedStatement: confirmed,
		AttemptCount:           1,
	}
	if status == db.MigrationStatusFailed {
		finishedAt := time.Now()
		failed := confirmed + 1
		message := "statement failed"
		record.FinishedAt = &finishedAt
		record.FailedStatement = &failed
		record.ErrorMessage = &message
	}
	return record
}

func TestRecoveryOptionsRequireExplicitActionAndProgress(t *testing.T) {
	confirmed := 1
	negative := -1
	tests := []struct {
		name    string
		opts    RecoveryOptions
		wantErr string
	}{
		{name: "missing version", opts: RecoveryOptions{Action: RecoveryActionMarkApplied}, wantErr: "version must not be empty"},
		{name: "missing action", opts: RecoveryOptions{Version: "001"}, wantErr: "unsupported recovery action"},
		{name: "retry without progress", opts: RecoveryOptions{Version: "001", Action: RecoveryActionRetry}, wantErr: "requires an explicit confirmed-through"},
		{name: "retry with negative progress", opts: RecoveryOptions{Version: "001", Action: RecoveryActionRetry, ConfirmedThrough: &negative}, wantErr: "must not be negative"},
		{name: "mark applied with progress", opts: RecoveryOptions{Version: "001", Action: RecoveryActionMarkApplied, ConfirmedThrough: &confirmed}, wantErr: "only valid with retry"},
		{name: "retry", opts: RecoveryOptions{Version: "001", Action: RecoveryActionRetry, ConfirmedThrough: &confirmed}},
		{name: "mark applied", opts: RecoveryOptions{Version: "001", Action: RecoveryActionMarkApplied}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestValidateRecoveryCandidateRequiresOneMatchingIncompleteNonTransactionalMigration(t *testing.T) {
	base := preparedHistoryMigration(t, "001", "base", "SELECT 1;")
	partial := preparedHistoryMigration(
		t,
		"002",
		"partial",
		"-- schemata:depends-on 001\n-- schemata:transaction off\nSELECT 2; SELECT 3;",
	)
	history := []db.MigrationRecord{
		appliedHistoryRecord(base),
		incompleteRecoveryRecord(partial, db.MigrationStatusFailed, 1),
	}

	gotMigration, gotRecord, err := validateRecoveryCandidate([]Migration{base, partial}, history, partial.Version)
	require.NoError(t, err)
	assert.Equal(t, partial.Version, gotMigration.Version)
	assert.Equal(t, db.MigrationStatusFailed, gotRecord.Status)

	t.Run("applied row", func(t *testing.T) {
		changed := append([]db.MigrationRecord(nil), history...)
		changed[1] = appliedHistoryRecord(partial)
		_, _, err := validateRecoveryCandidate([]Migration{base, partial}, changed, partial.Version)
		require.ErrorContains(t, err, "not an incomplete migration")
	})

	t.Run("identity drift", func(t *testing.T) {
		changed := append([]db.MigrationRecord(nil), history...)
		changed[1].Checksum = strings.Repeat("f", 64)
		_, _, err := validateRecoveryCandidate([]Migration{base, partial}, changed, partial.Version)
		require.ErrorContains(t, err, "checksum mismatch")
	})

	t.Run("second incomplete row", func(t *testing.T) {
		changed := append([]db.MigrationRecord(nil), history...)
		baseRecord := incompleteRecoveryRecord(base, db.MigrationStatusRunning, 0)
		changed[0] = baseRecord
		_, _, err := validateRecoveryCandidate([]Migration{base, partial}, changed, partial.Version)
		require.ErrorContains(t, err, "also has incomplete")
	})
}

func TestMigrationFailureDetailsAreBoundedUTF8AndPreserveSQLStateAbsence(t *testing.T) {
	message, code := migrationFailureDetails(errors.New(strings.Repeat("🙂", migrationFailureMessageLimit)))
	assert.LessOrEqual(t, len(message), migrationFailureMessageLimit)
	assert.True(t, utf8.ValidString(message))
	assert.Nil(t, code)
}

func TestOnlyServerReportedStatementErrorsAreTreatedAsDefinite(t *testing.T) {
	assert.False(t, isDefinitePostgresStatementFailure(errors.New("connection reset")))
	assert.True(t, isDefinitePostgresStatementFailure(&pgconn.PgError{
		SeverityUnlocalized: "ERROR",
		Code:                "42P01",
		Message:             "missing relation",
	}))
	assert.False(t, isDefinitePostgresStatementFailure(&pgconn.PgError{
		SeverityUnlocalized: "FATAL",
		Code:                "57P01",
		Message:             "terminating connection due to administrator command",
	}))
	assert.False(t, isDefinitePostgresStatementFailure(&pgconn.PgError{
		SeverityUnlocalized: "ERROR",
		Code:                "08006",
		Message:             "connection failure",
	}))
	assert.False(t, isDefinitePostgresStatementFailure(&pgconn.PgError{
		SeverityUnlocalized: "ERROR",
		Code:                "40003",
		Message:             "statement completion unknown",
	}))
}

func TestValidateRecoveryTransitions(t *testing.T) {
	migration := preparedHistoryMigration(
		t,
		"001",
		"partial",
		"-- schemata:transaction off\nSELECT 1; SELECT 2;",
	)
	before := []db.MigrationRecord{incompleteRecoveryRecord(migration, db.MigrationStatusFailed, 0)}

	retryTime := time.Now()
	retryAction := db.MigrationRecoveryRetry
	retrying := before[0]
	retrying.Status = db.MigrationStatusRunning
	retrying.StartedAt = retryTime
	retrying.FinishedAt = nil
	retrying.FailedStatement = nil
	retrying.ErrorMessage = nil
	retrying.AttemptCount++
	retrying.RecoveredAt = &retryTime
	retrying.RecoveryAction = &retryAction
	require.NoError(t, validateRetryHistoryTransition(before, []db.MigrationRecord{retrying}, metadataForMigration(migration), 0))

	confirmed := retrying
	confirmed.LastConfirmedStatement = 1
	require.NoError(t, validateProgressHistoryTransition(
		[]db.MigrationRecord{retrying},
		[]db.MigrationRecord{confirmed},
		metadataForMigration(migration),
		1,
	))

	failedAt := 2
	failureMessage := "boom"
	finishedAt := time.Now()
	failed := confirmed
	failed.Status = db.MigrationStatusFailed
	failed.FinishedAt = &finishedAt
	failed.FailedStatement = &failedAt
	failed.ErrorMessage = &failureMessage
	require.NoError(t, validateFailedHistoryTransition(
		[]db.MigrationRecord{confirmed},
		[]db.MigrationRecord{failed},
		metadataForMigration(migration),
		2,
	))

	markAppliedAction := db.MigrationRecoveryMarkApplied
	recoveredAt := time.Now()
	markApplied := failed
	markApplied.Status = db.MigrationStatusApplied
	markApplied.FinishedAt = &recoveredAt
	markApplied.LastConfirmedStatement = markApplied.StatementCount
	markApplied.FailedStatement = nil
	markApplied.ErrorMessage = nil
	markApplied.RecoveredAt = &recoveredAt
	markApplied.RecoveryAction = &markAppliedAction
	require.NoError(t, validateRecoveredAppliedHistoryTransition(
		[]db.MigrationRecord{failed},
		[]db.MigrationRecord{markApplied},
		metadataForMigration(migration),
	))
}
