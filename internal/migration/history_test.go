package migration

import (
	"testing"
	"time"

	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func preparedHistoryMigration(t *testing.T, version, name, sql string) Migration {
	t.Helper()
	migration := Migration{Version: version, Name: name, SQL: sql}
	require.NoError(t, migration.LoadSQL())
	return migration
}

func appliedHistoryRecord(migration Migration) db.MigrationRecord {
	startedAt := time.Now().Add(-time.Second)
	finishedAt := time.Now()
	return db.MigrationRecord{
		MigrationMetadata: db.MigrationMetadata{
			Version:        migration.Version,
			Name:           migration.Name,
			Checksum:       migration.Checksum,
			ExecutionMode:  migration.ExecutionMode,
			StatementCount: len(migration.Statements),
		},
		Status:                 db.MigrationStatusApplied,
		StartedAt:              startedAt,
		FinishedAt:             &finishedAt,
		LastConfirmedStatement: len(migration.Statements),
		AttemptCount:           1,
	}
}

func TestValidateMigrationHistoryReturnsPendingInventory(t *testing.T) {
	first := preparedHistoryMigration(t, "001", "first", "SELECT 1;")
	second := preparedHistoryMigration(t, "002", "second", "SELECT 2;")

	pending, err := validateMigrationHistory(
		[]Migration{first, second},
		[]db.MigrationRecord{appliedHistoryRecord(first)},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"002"}, pending)
}

func TestValidateMigrationHistoryRejectsDriftAndIncompleteState(t *testing.T) {
	local := preparedHistoryMigration(t, "001", "first", "SELECT 1;")

	tests := []struct {
		name    string
		record  db.MigrationRecord
		wantErr string
	}{
		{
			name: "missing local migration",
			record: func() db.MigrationRecord {
				record := appliedHistoryRecord(local)
				record.Version = "999"
				return record
			}(),
			wantErr: "missing from the local inventory",
		},
		{
			name: "renamed migration",
			record: func() db.MigrationRecord {
				record := appliedHistoryRecord(local)
				record.Name = "renamed"
				return record
			}(),
			wantErr: "was renamed",
		},
		{
			name: "changed checksum",
			record: func() db.MigrationRecord {
				record := appliedHistoryRecord(local)
				record.Checksum = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
				return record
			}(),
			wantErr: "checksum mismatch",
		},
		{
			name: "changed execution mode",
			record: func() db.MigrationRecord {
				record := appliedHistoryRecord(local)
				record.ExecutionMode = ExecutionModeNonTransactional
				return record
			}(),
			wantErr: "execution mode mismatch",
		},
		{
			name: "changed statement count",
			record: func() db.MigrationRecord {
				record := appliedHistoryRecord(local)
				record.StatementCount++
				record.LastConfirmedStatement++
				return record
			}(),
			wantErr: "statement count mismatch",
		},
		{
			name: "interrupted run",
			record: func() db.MigrationRecord {
				record := appliedHistoryRecord(local)
				record.Status = db.MigrationStatusRunning
				record.FinishedAt = nil
				record.LastConfirmedStatement = 0
				return record
			}(),
			wantErr: "incomplete database status",
		},
		{
			name: "failed progress gap",
			record: func() db.MigrationRecord {
				record := appliedHistoryRecord(local)
				finished := time.Now()
				failedStatement := 2
				message := "failed"
				record.Status = db.MigrationStatusFailed
				record.FinishedAt = &finished
				record.LastConfirmedStatement = 0
				record.FailedStatement = &failedStatement
				record.ErrorMessage = &message
				record.StatementCount = 2
				return record
			}(),
			wantErr: "immediately follow",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateMigrationHistory([]Migration{local}, []db.MigrationRecord{test.record})
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

func TestValidateMigrationHistoryRejectsMissingAppliedDependency(t *testing.T) {
	base := preparedHistoryMigration(t, "001", "base", "SELECT 1;")
	child := preparedHistoryMigration(t, "002", "child", "SELECT 2;")
	child.DependsOn = []string{base.Version}

	_, err := validateMigrationHistory(
		[]Migration{base, child},
		[]db.MigrationRecord{appliedHistoryRecord(child)},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not recorded as applied")
}
