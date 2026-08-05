package cli

import (
	"testing"

	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRecoveryOptions(t *testing.T) {
	tests := []struct {
		name             string
		retry            bool
		markApplied      bool
		confirmedChanged bool
		confirmed        int
		wantAction       migration.RecoveryAction
		wantConfirmed    *int
		wantErr          string
	}{
		{name: "no action", wantErr: "exactly one"},
		{name: "both actions", retry: true, markApplied: true, wantErr: "exactly one"},
		{name: "retry requires boundary", retry: true, wantErr: "requires --confirmed-through"},
		{name: "negative boundary", retry: true, confirmedChanged: true, confirmed: -1, wantErr: "must not be negative"},
		{name: "mark applied rejects boundary", markApplied: true, confirmedChanged: true, confirmed: 0, wantErr: "only valid with --retry"},
		{name: "retry", retry: true, confirmedChanged: true, confirmed: 2, wantAction: migration.RecoveryActionRetry, wantConfirmed: cliIntPointer(2)},
		{name: "mark applied", markApplied: true, wantAction: migration.RecoveryActionMarkApplied},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options, err := buildRecoveryOptions(
				"20260805000000",
				test.retry,
				test.markApplied,
				test.confirmedChanged,
				test.confirmed,
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "20260805000000", options.Version)
			assert.Equal(t, test.wantAction, options.Action)
			if test.wantConfirmed == nil {
				assert.Nil(t, options.ConfirmedThrough)
			} else {
				require.NotNil(t, options.ConfirmedThrough)
				assert.Equal(t, *test.wantConfirmed, *options.ConfirmedThrough)
			}
		})
	}
}

func cliIntPointer(value int) *int { return &value }

func TestConfirmRecoveryTargetIdentity(t *testing.T) {
	target := &config.DBConnection{Identity: &config.DatabaseIdentity{
		Database:         "app",
		SystemIdentifier: "000123",
	}}

	require.NoError(t, confirmRecoveryTargetIdentity(target, "app", "123"))
	assert.ErrorContains(t, confirmRecoveryTargetIdentity(nil, "app", "123"), "requires database identity pinning")
	assert.ErrorContains(t, confirmRecoveryTargetIdentity(&config.DBConnection{}, "app", "123"), "requires database identity pinning")
	assert.ErrorContains(t, confirmRecoveryTargetIdentity(target, "", ""), "requires --confirm-database")
	assert.ErrorContains(t, confirmRecoveryTargetIdentity(target, "wrong", "123"), "database confirmation mismatch")
	assert.ErrorContains(t, confirmRecoveryTargetIdentity(target, "app", "not-a-number"), "invalid --confirm-system-identifier")
	assert.ErrorContains(t, confirmRecoveryTargetIdentity(target, "app", "124"), "system-identifier confirmation mismatch")
}
