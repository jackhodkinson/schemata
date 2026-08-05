package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
dev: postgresql://postgres@localhost:5432/dev
target: postgresql://postgres@localhost:5432/target
schema: schema.sql
migrations: ./migrations
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Dev)
				require.NotNil(t, cfg.Dev.URL)
				assert.Equal(t, "postgresql://postgres@localhost:5432/dev", *cfg.Dev.URL)

				require.NotNil(t, cfg.Target)
				require.NotNil(t, cfg.Target.URL)
				assert.Equal(t, "postgresql://postgres@localhost:5432/target", *cfg.Target.URL)

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
			name: "URL mapping with expected identity",
			yaml: `
dev: postgresql://postgres@localhost:5432/dev
target:
  url: postgresql://app@prod.example.com:5432/app
  identity:
    database: app
    system-identifier: "18446744073709551615"
schema: schema.sql
migrations: ./migrations
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Target)
				require.NotNil(t, cfg.Target.URL)
				assert.Equal(t, "postgresql://app@prod.example.com:5432/app", *cfg.Target.URL)
				require.NotNil(t, cfg.Target.Identity)
				assert.Equal(t, "app", cfg.Target.Identity.Database)
				assert.Equal(t, "18446744073709551615", cfg.Target.Identity.SystemIdentifier)
				value, err := cfg.Target.Identity.SystemIdentifierValue()
				require.NoError(t, err)
				assert.Equal(t, ^uint64(0), value)
			},
		},
		{
			name: "multi-target format",
			yaml: `
dev: postgresql://postgres@localhost:5432/dev
targets:
  prod: postgresql://app@prod.example.com:5432/prod
  staging: postgresql://app@staging.example.com:5432/staging
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
dev: postgresql://postgres@localhost:5432/dev
target: postgresql://postgres@localhost:5432/target
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
			name: "database safety timeouts",
			yaml: `
dev: postgresql://postgres@localhost:5432/dev
target: postgresql://postgres@localhost:5432/target
database:
  connect-timeout: 12s
  statement-timeout: 45s
  lock-timeout: 1250ms
schema: schema.sql
migrations: ./migrations
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Database.ConnectTimeout)
				assert.Equal(t, 12*time.Second, cfg.Database.ConnectTimeout.Duration)
				require.NotNil(t, cfg.Database.StatementTimeout)
				assert.Equal(t, 45*time.Second, cfg.Database.StatementTimeout.Duration)
				require.NotNil(t, cfg.Database.LockTimeout)
				assert.Equal(t, 1250*time.Millisecond, cfg.Database.LockTimeout.Duration)
			},
		},
		{
			name: "explicitly disabled database timeout",
			yaml: `
target: postgresql://postgres@localhost:5432/target
database:
  statement-timeout: 0
schema: schema.sql
migrations: ./migrations
`,
			wantErr: false,
			check: func(t *testing.T, cfg *Config) {
				assert.Nil(t, cfg.Database.ConnectTimeout)
				require.NotNil(t, cfg.Database.StatementTimeout)
				assert.Zero(t, cfg.Database.StatementTimeout.Duration)
				assert.Nil(t, cfg.Database.LockTimeout)
			},
		},
		{
			name: "zero connect timeout",
			yaml: `
target: postgresql://postgres@localhost:5432/target
database:
  connect-timeout: 0
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "invalid database timeout",
			yaml: `
target: postgresql://postgres@localhost:5432/target
database:
  lock-timeout: eventually
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "negative database timeout",
			yaml: `
target: postgresql://postgres@localhost:5432/target
database:
  statement-timeout: -1s
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "missing required fields",
			yaml: `
dev: postgresql://postgres@localhost:5432/dev
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "both target and targets",
			yaml: `
dev: postgresql://postgres@localhost:5432/dev
target: postgresql://postgres@localhost:5432/target
targets:
  prod: postgresql://app@prod:5432/prod
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity requires database",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
    system-identifier: "123"
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity cannot be null",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity requires system identifier",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
    database: app
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity rejects non-decimal system identifier",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
    database: app
    system-identifier: "0x123"
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity requires quoted system identifier",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
    database: app
    system-identifier: 123
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity database must be a string",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
    database: 123
    system-identifier: "123"
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity rejects unknown fields",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
    database: app
    system-identifier: "123"
    cluster: production
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "identity rejects system identifier overflow",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identity:
    database: app
    system-identifier: "18446744073709551616"
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "URL mapping rejects structured connection fields",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  host: localhost
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "connection mapping rejects unknown identity typo",
			yaml: `
target:
  url: postgresql://app@localhost:5432/app
  identitiy:
    database: app
    system-identifier: "123"
schema: schema.sql
migrations: ./migrations
`,
			wantErr: true,
		},
		{
			name: "scalar connection URL must be a string",
			yaml: `
target: 123
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
			name: "structured format with default port",
			conn: DBConnection{
				Host:     strPtr("myhost"),
				Username: strPtr("postgres"),
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
			name: "structured format preserves an explicitly empty password",
			conn: DBConnection{
				Host:     strPtr("prod.example.com"),
				Username: strPtr("appuser"),
				Password: strPtr(""),
				Database: strPtr("proddb"),
			},
			expected: "host=prod.example.com port=5432 user=appuser password='' dbname=proddb",
		},
		{
			name: "structured with SSL",
			conn: DBConnection{
				Host:     strPtr("secure.example.com"),
				Username: strPtr("postgres"),
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

func TestExpandEnvVarRequiresMissingVariablesUnlessDefaulted(t *testing.T) {
	t.Setenv("SCHEMATA_PRESENT", "postgresql://db/app")

	got, err := expandEnvVar("${SCHEMATA_PRESENT}")
	require.NoError(t, err)
	assert.Equal(t, "postgresql://db/app", got)

	_, err = expandEnvVar("${SCHEMATA_MISSING}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SCHEMATA_MISSING")

	got, err = expandEnvVar("${SCHEMATA_MISSING:-postgresql://fallback/app}")
	require.NoError(t, err)
	assert.Equal(t, "postgresql://fallback/app", got)
}

func TestConfigRejectsImplicitOrInvalidConnections(t *testing.T) {
	base := Config{
		Schema:     SchemaConfig{File: "schema.sql"},
		Migrations: MigrationsConfig{Dir: "migrations"},
	}

	cfg := base
	cfg.Target = &DBConnection{}
	require.ErrorContains(t, cfg.Validate(), "host must be explicitly configured")

	cfg = base
	cfg.Target = &DBConnection{URL: strPtr("")}
	require.ErrorContains(t, cfg.Validate(), "URL must not be empty")

	cfg = base
	cfg.Target = &DBConnection{
		Host: strPtr("db"), Username: strPtr("app"), Database: strPtr("app"), Port: intPtr(70000),
	}
	require.ErrorContains(t, cfg.Validate(), "port must be between")

	cfg = base
	cfg.Target = &DBConnection{URL: strPtr("postgresql://app@db/app")}
	cfg.Migrations.Format = "unknown"
	require.ErrorContains(t, cfg.Validate(), "unsupported migrations format")
}

func TestDatabaseIdentityValidation(t *testing.T) {
	tests := []struct {
		name       string
		identity   DatabaseIdentity
		wantErr    string
		wantSystem uint64
	}{
		{
			name:       "valid",
			identity:   DatabaseIdentity{Database: "app", SystemIdentifier: "00123"},
			wantSystem: 123,
		},
		{
			name:     "missing database",
			identity: DatabaseIdentity{SystemIdentifier: "123"},
			wantErr:  "identity.database must be specified",
		},
		{
			name:     "missing system identifier",
			identity: DatabaseIdentity{Database: "app"},
			wantErr:  "identity.system-identifier must be specified",
		},
		{
			name:     "negative",
			identity: DatabaseIdentity{Database: "app", SystemIdentifier: "-1"},
			wantErr:  "non-decimal character",
		},
		{
			name:     "positive sign",
			identity: DatabaseIdentity{Database: "app", SystemIdentifier: "+1"},
			wantErr:  "non-decimal character",
		},
		{
			name:     "overflow",
			identity: DatabaseIdentity{Database: "app", SystemIdentifier: "18446744073709551616"},
			wantErr:  "outside the uint64 range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.identity.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			value, err := tt.identity.SystemIdentifierValue()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSystem, value)
		})
	}
}

func TestDatabaseTimeoutsMarshalAsDurationsAndOmitEmptySection(t *testing.T) {
	withoutTimeouts, err := yaml.Marshal(Config{})
	require.NoError(t, err)
	assert.NotContains(t, string(withoutTimeouts), "database:")

	connectTimeout := Duration{Duration: 12 * time.Second}
	statementTimeout := Duration{Duration: 90 * time.Second}
	withTimeouts, err := yaml.Marshal(Config{
		Database: DatabaseConfig{
			ConnectTimeout:   &connectTimeout,
			StatementTimeout: &statementTimeout,
		},
	})
	require.NoError(t, err)
	assert.Contains(t, string(withTimeouts), "connect-timeout: 12s")
	assert.Contains(t, string(withTimeouts), "statement-timeout: 1m30s")
}

func TestDBConnectionMarshalPreservesScalarURLAndMapsIdentity(t *testing.T) {
	url := "postgresql://app@db.example/app"

	scalar, err := yaml.Marshal(DBConnection{URL: &url})
	require.NoError(t, err)
	assert.Equal(t, "postgresql://app@db.example/app\n", string(scalar))

	mapped, err := yaml.Marshal(DBConnection{
		URL: &url,
		Identity: &DatabaseIdentity{
			Database:         "app",
			SystemIdentifier: "7561860200789946402",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, `url: postgresql://app@db.example/app
identity:
    database: "app"
    system-identifier: "7561860200789946402"
`, string(mapped))
}

func TestIdentityEnvironmentExpansionHappensBeforeValidation(t *testing.T) {
	t.Setenv("SCHEMATA_EXPECTED_DATABASE", "app")
	t.Setenv("SCHEMATA_EXPECTED_SYSTEM_IDENTIFIER", "7561860200789946402")

	configPath := t.TempDir() + "/schemata.yaml"
	err := os.WriteFile(configPath, []byte(`
target:
  url: postgresql://app@localhost:5432/app
  identity:
    database: ${SCHEMATA_EXPECTED_DATABASE}
    system-identifier: ${SCHEMATA_EXPECTED_SYSTEM_IDENTIFIER}
schema: schema.sql
migrations: ./migrations
`), 0o600)
	require.NoError(t, err)

	cfg, err := Load(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg.Target.Identity)
	assert.Equal(t, "app", cfg.Target.Identity.Database)
	assert.Equal(t, "7561860200789946402", cfg.Target.Identity.SystemIdentifier)
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
			name:     "already wrapped reference is preserved",
			input:    "${MY_VAR}",
			expected: "${MY_VAR}",
		},
		{
			name:     "invalid dollar reference is left literal",
			input:    "$1INVALID",
			expected: "$1INVALID",
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

func TestDetectEnvVarSortsDuplicateMatches(t *testing.T) {
	environment := []string{
		"Z_LAST=shared-secret",
		"INVALID-NAME=shared-secret",
		"A_FIRST=shared-secret",
	}
	assert.Equal(t, "${A_FIRST}", detectEnvVar("shared-secret", environment))
	assert.Equal(t, "${A_FIRST}", detectEnvVar("shared-secret", []string{
		"A_FIRST=shared-secret",
		"Z_LAST=shared-secret",
	}))
	assert.Equal(t, "", detectEnvVar("", []string{"EMPTY="}))
}

// Helper functions
func strPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}
