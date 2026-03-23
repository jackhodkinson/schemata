package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterPendingMigrations(t *testing.T) {
	pending := []string{"001", "002", "003", "004", "005"}
	allVersions := []string{"000", "001", "002", "003", "004", "005"} // 000 is already applied

	tests := []struct {
		name        string
		pending     []string
		allVersions []string
		opts        ApplyOptions
		want        []string
		wantErr     string
	}{
		{
			name:        "no filters returns all pending",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{},
			want:        []string{"001", "002", "003", "004", "005"},
		},
		{
			name:        "step 1 returns first only",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{Step: 1},
			want:        []string{"001"},
		},
		{
			name:        "step 3 returns first three",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{Step: 3},
			want:        []string{"001", "002", "003"},
		},
		{
			name:        "step larger than pending returns all",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{Step: 100},
			want:        []string{"001", "002", "003", "004", "005"},
		},
		{
			name:        "to version 003",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{ToVersion: "003"},
			want:        []string{"001", "002", "003"},
		},
		{
			name:        "to first version",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{ToVersion: "001"},
			want:        []string{"001"},
		},
		{
			name:        "to last version returns all",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{ToVersion: "005"},
			want:        []string{"001", "002", "003", "004", "005"},
		},
		{
			name:        "to version already applied",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{ToVersion: "000"},
			wantErr:     "already been applied",
		},
		{
			name:        "to nonexistent version",
			pending:     pending,
			allVersions: allVersions,
			opts:        ApplyOptions{ToVersion: "999"},
			wantErr:     "not found in migrations",
		},
		{
			name:        "empty pending with step returns empty",
			pending:     []string{},
			allVersions: allVersions,
			opts:        ApplyOptions{Step: 5},
			want:        []string{},
		},
		{
			name:        "empty pending with to version errors",
			pending:     []string{},
			allVersions: allVersions,
			opts:        ApplyOptions{ToVersion: "000"},
			wantErr:     "already been applied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := make([]string, len(tt.pending))
			copy(p, tt.pending)

			got, err := FilterPendingMigrations(p, tt.allVersions, tt.opts)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
