package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigParsing(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr bool
		check   func(*testing.T, *Config)
	}{
		{
			name: "simple URL format",
			yaml: `
dev: postgresql://localhost:5432/dev
target: postgresql://localhost:5432/target
schema: schema.sql
migrations: ./migrations
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Dev)
				require.NotNil(t, cfg.Dev.URL)
				assert.Equal(t, "postgresql://localhost:5432/dev", *cfg.Dev.URL)

				require.NotNil(t, cfg.Target)
				require.NotNil(t, cfg.Target.URL)
				assert.Equal(t, "postgresql://localhost:5432/target", *cfg.Target.URL)

				assert.Equal(t, "schema.sql", cfg.Schema.GetSchemaPath())
				assert.Equal(t, "./migrations", cfg.Migrations.GetDir())
				assert.Equal(t, "sql", cfg.Migrations.GetFormat())
			},
		},
		{
			name: "structured connection format",
			yaml: `
dev:
  host: localhost
  port: 5433
  username: postgres
  password: secret
  database: dev_db
target:
  host: prod.example.com
  port: 5432
  username: app_user
  database: prod_db
schema: schema.sql
migrations: ./migrations
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Dev)
				require.NotNil(t, cfg.Dev.Host)
				assert.Equal(t, "localhost", *cfg.Dev.Host)
				require.NotNil(t, cfg.Dev.Port)
				assert.Equal(t, 5433, *cfg.Dev.Port)
			},
		},
		{
			name: "multi-target format",
			yaml: `
dev: postgresql://localhost:5432/dev
targets:
  prod: postgresql://prod.example.com:5432/prod
  staging: postgresql://staging.example.com:5432/staging
schema:
  file: schema.sql
  include:
    - public
    - app
migrations: ./migrations
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				assert.Nil(t, cfg.Target)
				require.NotNil(t, cfg.Targets)
				assert.Len(t, cfg.Targets, 2)

				prodConn, ok := cfg.Targets["prod"]
				require.True(t, ok)
				require.NotNil(t, prodConn.URL)

				include, exclude := cfg.Schema.GetSchemaFilters()
				assert.Equal(t, []string{"public", "app"}, include)
				assert.Nil(t, exclude)
			},
		},
		{
			name: "structured migrations config with format",
			yaml: `
dev: postgresql://localhost:5432/dev
target: postgresql://localhost:5432/target
schema: schema.sql
migrations:
  dir: ./sql/migrations
  format: moo
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "./sql/migrations", cfg.Migrations.GetDir())
				assert.Equal(t, "moo", cfg.Migrations.GetFormat())
			},
		},
		{
			name: "missing required fields",
			yaml: `
dev: postgresql://localhost:5432/dev
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "both target and targets",
			yaml: `
dev: postgresql://localhost:5432/dev
target: postgresql://localhost:5432/target
targets:
  prod: postgresql://prod:5432/prod
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write yaml to temp file
			tmpfile, err := os.CreateTemp("", "config-*.yaml")
			require.NoError(t, err)
			defer os.Remove(tmpfile.Name())

			_, err = tmpfile.Write([]byte(tt.yaml))
			require.NoError(t, err)
			tmpfile.Close()

			// Load config
			cfg, err := Load(tmpfile.Name())

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)

			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestEnvVarExpansion(t *testing.T) {
	// Set test environment variables
	os.Setenv("TEST_DB_URL", "postgresql://localhost:5432/testdb")
	os.Setenv("TEST_HOST", "testhost")
	defer os.Unsetenv("TEST_DB_URL")
	defer os.Unsetenv("TEST_HOST")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple env var",
			input:    "${TEST_DB_URL}",
			expected: "postgresql://localhost:5432/testdb",
		},
		{
			name:     "env var with default (var exists)",
			input:    "${TEST_HOST:-fallback}",
			expected: "testhost",
		},
		{
			name:     "env var with default (var missing)",
			input:    "${MISSING_VAR:-defaultvalue}",
			expected: "defaultvalue",
		},
		{
			name:     "no env var",
			input:    "literal-value",
			expected: "literal-value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := expandEnvVar(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConnectionStringBuilder(t *testing.T) {
	tests := []struct {
		name     string
		conn     DBConnection
		expected string
	}{
		{
			name: "URL format",
			conn: DBConnection{
				URL: strPtr("postgresql://user:pass@host:5432/dbname"),
			},
			expected: "postgresql://user:pass@host:5432/dbname",
		},
		{
			name: "structured format with defaults",
			conn: DBConnection{
				Host:     strPtr("myhost"),
				Database: strPtr("mydb"),
			},
			expected: "host=myhost port=5432 user=postgres dbname=mydb",
		},
		{
			name: "structured format with all fields",
			conn: DBConnection{
				Host:     strPtr("prod.example.com"),
				Port:     intPtr(5433),
				Username: strPtr("appuser"),
				Password: strPtr("secret"),
				Database: strPtr("proddb"),
			},
			expected: "host=prod.example.com port=5433 user=appuser password=secret dbname=proddb",
		},
		{
			name: "structured with SSL",
			conn: DBConnection{
				Host:     strPtr("secure.example.com"),
				Database: strPtr("securedb"),
				SSL: &SSLConfig{
					Mode: SSLRequire,
				},
			},
			expected: "host=secure.example.com port=5432 user=postgres dbname=securedb sslmode=require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.conn.ToConnectionString()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetectEnvVar(t *testing.T) {
	os.Setenv("TEST_VAR", "test_value")
	defer os.Unsetenv("TEST_VAR")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "env var by value match",
			input:    "test_value",
			expected: "${TEST_VAR}",
		},
		{
			name:     "already has dollar prefix",
			input:    "$MY_VAR",
			expected: "${MY_VAR}",
		},
		{
			name:     "literal value",
			input:    "some-literal",
			expected: "some-literal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetectEnvVar(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper functions
func strPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}
