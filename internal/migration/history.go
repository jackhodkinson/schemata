package migration

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jackhodkinson/schemata/internal/db"
)

// validateMigrationHistory proves that durable database history is an exact,
// immutable subset of the local inventory before any SQL is executed.
func validateMigrationHistory(
	migrations []Migration,
	history []db.MigrationRecord,
) ([]string, error) {
	localByVersion := make(map[string]Migration, len(migrations))
	for i := range migrations {
		localByVersion[migrations[i].Version] = migrations[i]
	}

	applied := make(map[string]bool, len(history))
	for _, record := range history {
		if err := validateHistoryRecord(record); err != nil {
			return nil, fmt.Errorf("invalid database history for migration %s: %w", record.Version, err)
		}

		local, exists := localByVersion[record.Version]
		if !exists {
			return nil, fmt.Errorf(
				"database records migration %s, but that version is missing from the local inventory",
				record.Version,
			)
		}

		if record.Status != db.MigrationStatusApplied {
			return nil, fmt.Errorf(
				"migration %s has incomplete database status %q; reconcile it explicitly before applying more migrations",
				record.Version,
				record.Status,
			)
		}
		if record.Name != local.Name {
			return nil, fmt.Errorf(
				"applied migration %s was renamed: database records %q, local inventory has %q",
				record.Version,
				record.Name,
				local.Name,
			)
		}
		if record.Checksum != local.Checksum {
			return nil, fmt.Errorf(
				"applied migration %s checksum mismatch: database records %s, local source is %s",
				record.Version,
				record.Checksum,
				local.Checksum,
			)
		}
		if record.ExecutionMode != local.ExecutionMode {
			return nil, fmt.Errorf(
				"applied migration %s execution mode mismatch: database records %q, local source declares %q",
				record.Version,
				record.ExecutionMode,
				local.ExecutionMode,
			)
		}
		if record.StatementCount != len(local.Statements) {
			return nil, fmt.Errorf(
				"applied migration %s statement count mismatch: database records %d, local source has %d",
				record.Version,
				record.StatementCount,
				len(local.Statements),
			)
		}

		applied[record.Version] = true
	}

	for _, migration := range migrations {
		if !applied[migration.Version] {
			continue
		}
		for _, dependency := range migration.DependsOn {
			if !applied[dependency] {
				return nil, fmt.Errorf(
					"applied migration %s depends on migration %s, which is not recorded as applied",
					migration.Version,
					dependency,
				)
			}
		}
	}

	pending := make([]string, 0, len(migrations)-len(applied))
	for _, migration := range migrations {
		if !applied[migration.Version] {
			pending = append(pending, migration.Version)
		}
	}

	return pending, nil
}

func validateHistoryRecord(record db.MigrationRecord) error {
	if record.Version == "" || record.Name == "" {
		return fmt.Errorf("version and name must be non-empty")
	}
	if len(record.Checksum) != 64 {
		return fmt.Errorf("checksum must contain 64 lowercase hexadecimal characters")
	}
	if _, err := hex.DecodeString(record.Checksum); err != nil || record.Checksum != strings.ToLower(record.Checksum) {
		return fmt.Errorf("checksum must contain 64 lowercase hexadecimal characters")
	}
	if record.ExecutionMode != ExecutionModeTransactional && record.ExecutionMode != ExecutionModeNonTransactional {
		return fmt.Errorf("unknown execution mode %q", record.ExecutionMode)
	}
	if record.StatementCount < 0 || record.LastConfirmedStatement < 0 || record.LastConfirmedStatement > record.StatementCount {
		return fmt.Errorf("invalid statement progress %d/%d", record.LastConfirmedStatement, record.StatementCount)
	}
	if record.AttemptCount < 1 {
		return fmt.Errorf("attempt count must be positive")
	}
	if record.StartedAt.IsZero() {
		return fmt.Errorf("start time is missing")
	}
	if (record.RecoveredAt == nil) != (record.RecoveryAction == nil) {
		return fmt.Errorf("recovery timestamp and action must either both be set or both be absent")
	}

	switch record.Status {
	case db.MigrationStatusRunning:
		if record.FinishedAt != nil || record.FailedStatement != nil || record.ErrorMessage != nil || record.ErrorCode != nil {
			return fmt.Errorf("running status contains failure or completion metadata")
		}
	case db.MigrationStatusApplied:
		if record.FinishedAt == nil || record.LastConfirmedStatement != record.StatementCount {
			return fmt.Errorf("applied status does not confirm every statement")
		}
		if record.FailedStatement != nil || record.ErrorMessage != nil || record.ErrorCode != nil {
			return fmt.Errorf("applied status contains failure metadata")
		}
	case db.MigrationStatusFailed:
		if record.FinishedAt == nil || record.FailedStatement == nil || record.ErrorMessage == nil {
			return fmt.Errorf("failed status is missing failure metadata")
		}
		if *record.FailedStatement < 1 || *record.FailedStatement > record.StatementCount {
			return fmt.Errorf("failed statement index is outside the migration")
		}
		if record.LastConfirmedStatement >= *record.FailedStatement {
			return fmt.Errorf("failed statement must follow the last confirmed statement")
		}
	default:
		return fmt.Errorf("unknown status %q", record.Status)
	}

	return nil
}

func migrationHistoriesEqual(left, right []db.MigrationRecord) bool {
	if len(left) != len(right) {
		return false
	}
	rightByVersion := make(map[string]db.MigrationRecord, len(right))
	for _, record := range right {
		if _, duplicate := rightByVersion[record.Version]; duplicate {
			return false
		}
		rightByVersion[record.Version] = record
	}
	for _, record := range left {
		other, exists := rightByVersion[record.Version]
		if !exists || !migrationRecordsEqual(record, other) {
			return false
		}
	}
	return true
}

func migrationRecordsEqual(left, right db.MigrationRecord) bool {
	return left.MigrationMetadata == right.MigrationMetadata &&
		left.Status == right.Status &&
		left.StartedAt.Equal(right.StartedAt) &&
		optionalTimesEqual(left.FinishedAt, right.FinishedAt) &&
		optionalStringsEqual(left.ErrorMessage, right.ErrorMessage) &&
		optionalStringsEqual(left.ErrorCode, right.ErrorCode) &&
		left.LastConfirmedStatement == right.LastConfirmedStatement &&
		optionalIntsEqual(left.FailedStatement, right.FailedStatement) &&
		left.AttemptCount == right.AttemptCount &&
		optionalTimesEqual(left.RecoveredAt, right.RecoveredAt) &&
		optionalStringsEqual(left.RecoveryAction, right.RecoveryAction)
}

func optionalTimesEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func optionalStringsEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func optionalIntsEqual(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func findMigrationRecord(history []db.MigrationRecord, version string) (db.MigrationRecord, bool) {
	for _, record := range history {
		if record.Version == version {
			return record, true
		}
	}
	return db.MigrationRecord{}, false
}

func validateRunningHistoryTransition(
	before, running []db.MigrationRecord,
	metadata db.MigrationMetadata,
) error {
	if len(running) != len(before)+1 {
		return fmt.Errorf("running history contains %d rows; expected %d", len(running), len(before)+1)
	}
	current, exists := findMigrationRecord(running, metadata.Version)
	if !exists {
		return fmt.Errorf("running history row %s is missing", metadata.Version)
	}
	if current.MigrationMetadata != metadata || current.Status != db.MigrationStatusRunning {
		return fmt.Errorf("running history row %s does not match its immutable metadata or status", metadata.Version)
	}
	if err := validateHistoryRecord(current); err != nil {
		return fmt.Errorf("running history row %s is invalid: %w", metadata.Version, err)
	}

	withoutCurrent := make([]db.MigrationRecord, 0, len(before))
	for _, record := range running {
		if record.Version != metadata.Version {
			withoutCurrent = append(withoutCurrent, record)
		}
	}
	if !migrationHistoriesEqual(before, withoutCurrent) {
		return fmt.Errorf("existing migration history changed while recording migration %s", metadata.Version)
	}
	return nil
}

func validateAppliedHistoryTransition(
	running, applied []db.MigrationRecord,
	metadata db.MigrationMetadata,
) error {
	if len(applied) != len(running) {
		return fmt.Errorf("applied history contains %d rows; expected %d", len(applied), len(running))
	}
	runningCurrent, runningExists := findMigrationRecord(running, metadata.Version)
	appliedCurrent, appliedExists := findMigrationRecord(applied, metadata.Version)
	if !runningExists || !appliedExists {
		return fmt.Errorf("history transition for migration %s is missing", metadata.Version)
	}
	if appliedCurrent.MigrationMetadata != metadata || appliedCurrent.Status != db.MigrationStatusApplied {
		return fmt.Errorf("applied history row %s does not match its immutable metadata or status", metadata.Version)
	}
	if !appliedCurrent.StartedAt.Equal(runningCurrent.StartedAt) ||
		appliedCurrent.AttemptCount != runningCurrent.AttemptCount ||
		!optionalTimesEqual(appliedCurrent.RecoveredAt, runningCurrent.RecoveredAt) ||
		!optionalStringsEqual(appliedCurrent.RecoveryAction, runningCurrent.RecoveryAction) {
		return fmt.Errorf("applied history row %s changed start or recovery metadata", metadata.Version)
	}
	if err := validateHistoryRecord(appliedCurrent); err != nil {
		return fmt.Errorf("applied history row %s is invalid: %w", metadata.Version, err)
	}

	runningWithoutCurrent := make([]db.MigrationRecord, 0, len(running)-1)
	appliedWithoutCurrent := make([]db.MigrationRecord, 0, len(applied)-1)
	for _, record := range running {
		if record.Version != metadata.Version {
			runningWithoutCurrent = append(runningWithoutCurrent, record)
		}
	}
	for _, record := range applied {
		if record.Version != metadata.Version {
			appliedWithoutCurrent = append(appliedWithoutCurrent, record)
		}
	}
	if !migrationHistoriesEqual(runningWithoutCurrent, appliedWithoutCurrent) {
		return fmt.Errorf("existing migration history changed while completing migration %s", metadata.Version)
	}
	return nil
}
