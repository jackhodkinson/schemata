package db

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresTimeoutValue(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		want    string
	}{
		{name: "disabled", timeout: 0, want: "0"},
		{name: "milliseconds", timeout: 250 * time.Millisecond, want: "250"},
		{name: "whole seconds", timeout: 3 * time.Second, want: "3000"},
		{name: "sub millisecond rounds up", timeout: time.Microsecond, want: "1"},
		{name: "remainder rounds up", timeout: time.Millisecond + time.Microsecond, want: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, postgresTimeoutValue(tt.timeout))
		})
	}
}

func TestConnectRejectsInvalidOptionsBeforeOpeningPool(t *testing.T) {
	url := "postgresql://postgres@localhost:1/unused"
	conn := &config.DBConnection{URL: &url}

	_, err := Connect(
		context.Background(),
		conn,
		WithConnectTimeout(-time.Second),
	)
	require.ErrorContains(t, err, "connect timeout must be greater than zero")

	_, err = Connect(
		context.Background(),
		conn,
		WithConnectTimeout(0),
	)
	require.ErrorContains(t, err, "connect timeout must be greater than zero")

	_, err = Connect(
		context.Background(),
		conn,
		WithTimeouts(-time.Second, time.Second),
	)
	require.ErrorContains(t, err, "statement timeout must not be negative")

	_, err = Connect(
		context.Background(),
		conn,
		WithTimeouts(time.Second, -time.Second),
	)
	require.ErrorContains(t, err, "lock timeout must not be negative")
}

func TestConnectRejectsInvalidIdentityBeforeOpeningPool(t *testing.T) {
	url := "postgresql://postgres@localhost:1/unused"
	conn := &config.DBConnection{
		URL: &url,
		Identity: &config.DatabaseIdentity{
			Database: "unused",
		},
	}

	_, err := Connect(context.Background(), conn)
	require.ErrorContains(t, err, "identity.system-identifier must be specified")
}

func TestRejectAmbientPostgresEnvironmentCoversPGXSettingsWithoutValues(t *testing.T) {
	require.Equal(t, []string{
		"PGAPPNAME",
		"PGCHANNELBINDING",
		"PGCONNECT_TIMEOUT",
		"PGDATABASE",
		"PGHOST",
		"PGMAXPROTOCOLVERSION",
		"PGMINPROTOCOLVERSION",
		"PGOPTIONS",
		"PGPASSFILE",
		"PGPASSWORD",
		"PGPORT",
		"PGREQUIREAUTH",
		"PGSERVICE",
		"PGSERVICEFILE",
		"PGSSLCERT",
		"PGSSLKEY",
		"PGSSLMODE",
		"PGSSLNEGOTIATION",
		"PGSSLPASSWORD",
		"PGSSLROOTCERT",
		"PGSSLSNI",
		"PGTARGETSESSIONATTRS",
		"PGTZ",
		"PGUSER",
	}, postgresEnvironmentVariables, "audit this list against pgx pgconn.parseEnvSettings when upgrading pgx")

	for _, environmentName := range postgresEnvironmentVariables {
		t.Run(environmentName, func(t *testing.T) {
			const secretValue = "must-not-appear-in-errors"
			err := rejectAmbientPostgresEnvironment(func(name string) (string, bool) {
				if name == environmentName {
					return secretValue, true
				}
				return "", false
			})
			require.ErrorContains(t, err, environmentName)
			assert.NotContains(t, err.Error(), secretValue)
		})
	}
}

func TestDeterministicPoolConfigFailsBeforePGXUsesHostileEnvironment(t *testing.T) {
	clearPostgresEnvironment(t)
	t.Setenv("PGSERVICE", "redirect")
	t.Setenv("PGSERVICEFILE", "/definitely/not/read/pg_service.conf")

	_, err := parseDeterministicPoolConfig("postgresql://app@db.example/app")
	require.ErrorContains(t, err, "ambient PostgreSQL environment settings are not permitted")
	require.ErrorContains(t, err, "PGSERVICE")
	require.ErrorContains(t, err, "PGSERVICEFILE")
	assert.NotContains(t, err.Error(), "/definitely/not/read/pg_service.conf")
}

func TestDeterministicPoolConfigNeutralizesImplicitCredentialAndTLSFiles(t *testing.T) {
	clearPostgresEnvironment(t)

	for _, test := range []struct {
		name             string
		connectionString string
		wantPassword     string
	}{
		{name: "URL absent password", connectionString: "postgresql://app@db.example/app"},
		{name: "URL explicit empty authority password", connectionString: "postgresql://app:@db.example/app"},
		{name: "URL explicit empty query password", connectionString: "postgresql://app@db.example/app?password="},
		{name: "keyword absent password", connectionString: "host=db.example user=app dbname=app"},
		{name: "keyword explicit empty password", connectionString: "host=db.example user=app password='' dbname=app"},
		{name: "keyword explicit password", connectionString: "host=db.example user=app password=configured dbname=app", wantPassword: "configured"},
	} {
		t.Run(test.name, func(t *testing.T) {
			poolConfig, err := parseDeterministicPoolConfig(test.connectionString)
			require.NoError(t, err)
			assert.Equal(t, "db.example", poolConfig.ConnConfig.Host)
			assert.Equal(t, uint16(5432), poolConfig.ConnConfig.Port)
			assert.Equal(t, "app", poolConfig.ConnConfig.User)
			assert.Equal(t, "app", poolConfig.ConnConfig.Database)
			assert.Equal(t, test.wantPassword, poolConfig.ConnConfig.Password)
			pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
			require.NoError(t, err)
			pool.Close()
		})
	}

	sanitizedURL, err := neutralizeImplicitConnectionFiles(
		"postgresql://app@db.example/app?sslrootcert=%2Fexplicit%2Froot.pem",
	)
	require.NoError(t, err)
	assert.Contains(t, sanitizedURL, "passfile="+url.QueryEscape(os.DevNull))
	assert.Contains(t, sanitizedURL, "sslcert=")
	assert.Contains(t, sanitizedURL, "sslkey=")
	assert.Contains(t, sanitizedURL, "sslrootcert=%2Fexplicit%2Froot.pem")

	sanitizedKeyword, err := neutralizeImplicitConnectionFiles(
		"host=db.example user=app password='' dbname=app sslrootcert=/explicit/root.pem",
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"passfile='"+os.DevNull+"' sslcert='' sslkey='' sslrootcert='' host=db.example user=app password='' dbname=app sslrootcert=/explicit/root.pem",
		sanitizedKeyword,
	)

	// pgx keyword parsing is last-wins. This failure proves the explicit path
	// after the blank neutralizer remains authoritative and is actually read.
	_, err = parseDeterministicPoolConfig(
		"host=db.example user=app dbname=app sslmode=verify-full sslrootcert=/definitely/not/a/certificate.pem",
	)
	require.ErrorContains(t, err, "PostgreSQL driver rejected")
	assert.NotContains(t, err.Error(), "/definitely/not/a/certificate.pem")
}

func TestDeterministicPoolConfigRejectsAmbientPasswordSources(t *testing.T) {
	for _, environmentName := range []string{"PGPASSWORD", "PGPASSFILE"} {
		t.Run(environmentName, func(t *testing.T) {
			clearPostgresEnvironment(t)
			t.Setenv(environmentName, "hostile-secret-source")
			_, err := parseDeterministicPoolConfig(
				"host=db.example user=app password='' dbname=app",
			)
			require.ErrorContains(t, err, environmentName)
			assert.NotContains(t, err.Error(), "hostile-secret-source")
		})
	}
}

func TestConnectRejectsExternalAndPoolConninfoBeforeOpeningPool(t *testing.T) {
	clearPostgresEnvironment(t)
	for _, parameter := range []string{"servicefile", "passfile", "options", "pool_max_conns"} {
		t.Run(parameter, func(t *testing.T) {
			url := "postgresql://app@localhost:1/unused?" + parameter + "=unsafe"
			_, err := Connect(context.Background(), &config.DBConnection{URL: &url})
			require.ErrorContains(t, err, "is not permitted")
		})
	}

	_, err := parseDeterministicPoolConfig(
		"host=db.example user=app dbname=app servicefile=/definitely/not/read/service.conf service=redirect",
	)
	require.ErrorContains(t, err, "PostgreSQL driver rejected")
}

func TestConnectionValidationAndParseErrorsNeverExposePasswords(t *testing.T) {
	clearPostgresEnvironment(t)
	const password = "distinctive-prefix'distinctive-suffix"

	structured := config.DBConnection{
		Host:     testStringPointer("db.example"),
		Username: testStringPointer("app"),
		Password: testStringPointer(password),
		Database: testStringPointer("app"),
		SSL: &config.SSLConfig{
			Mode:   config.SSLVerifyFull,
			CACert: testStringPointer("/definitely/not/a/certificate.pem"),
		},
	}
	connectionString, err := structured.ToConnectionString()
	require.NoError(t, err)
	_, err = parseDeterministicPoolConfig(connectionString)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), password)
	assert.NotContains(t, err.Error(), "distinctive-prefix")
	assert.NotContains(t, err.Error(), "distinctive-suffix")

	urlConnection := config.DBConnection{URL: testStringPointer(
		"postgresql://app:distinctive-prefix%27distinctive-suffix@db.example/app" +
			"?sslmode=verify-full&sslrootcert=%2Fdefinitely%2Fnot%2Fa%2Fcertificate.pem",
	)}
	require.NoError(t, urlConnection.Validate())
	urlString, err := urlConnection.ToConnectionString()
	require.NoError(t, err)
	_, err = parseDeterministicPoolConfig(urlString)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "distinctive-prefix")
	assert.NotContains(t, err.Error(), "distinctive-suffix")

	malformed := config.DBConnection{URL: testStringPointer(
		"postgresql://app:distinctive-malformed-secret@%zz/app",
	)}
	err = malformed.Validate()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "distinctive-malformed-secret")
}

func TestDefaultConnectionOptionsAreFinite(t *testing.T) {
	options := defaultConnectOptions()
	assert.Equal(t, DefaultConnectTimeout, options.connectTimeout)
	assert.Equal(t, DefaultStatementTimeout, options.statementTimeout)
	assert.Equal(t, DefaultLockTimeout, options.lockTimeout)
	assert.Positive(t, options.connectTimeout)
	assert.Positive(t, options.statementTimeout)
	assert.Positive(t, options.lockTimeout)
}

func TestApplyConnectionSafetyOptionsConfiguresEveryPhysicalConnection(t *testing.T) {
	clearPostgresEnvironment(t)
	poolConfig, err := parseDeterministicPoolConfig(
		"host=db.example user=app password='' dbname=app sslmode=disable",
	)
	require.NoError(t, err)
	options := connectOptions{
		connectTimeout:   7 * time.Second,
		statementTimeout: 8 * time.Second,
		lockTimeout:      9 * time.Second,
	}

	applyConnectionSafetyOptions(poolConfig, options)

	assert.Equal(t, 7*time.Second, poolConfig.ConnConfig.ConnectTimeout)
	assert.Equal(t, "8000", poolConfig.ConnConfig.RuntimeParams["statement_timeout"])
	assert.Equal(t, "9000", poolConfig.ConnConfig.RuntimeParams["lock_timeout"])
}

func TestConnectTimeoutBoundsHostnameResolution(t *testing.T) {
	clearPostgresEnvironment(t)
	poolConfig, err := parseDeterministicPoolConfig(
		"host=resolver-must-block.invalid user=app password='' dbname=app sslmode=disable",
	)
	require.NoError(t, err)
	poolConfig.ConnConfig.LookupFunc = func(ctx context.Context, _ string) ([]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	applyConnectionSafetyOptions(poolConfig, connectOptions{
		connectTimeout:   10 * time.Millisecond,
		statementTimeout: DefaultStatementTimeout,
		lockTimeout:      DefaultLockTimeout,
	})

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	require.NoError(t, err)
	defer pool.Close()

	outerContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- pool.Ping(outerContext)
	}()

	select {
	case err := <-result:
		require.Error(t, err)
		require.ErrorContains(t, err, "context deadline exceeded")
	case <-time.After(500 * time.Millisecond):
		cancel()
		t.Fatal("hostname resolution exceeded the configured connect timeout")
	}
}

func TestWithDatabaseConfigOverridesOnlyExplicitTimeouts(t *testing.T) {
	connectTimeout := config.Duration{Duration: 12 * time.Second}
	statementTimeout := config.Duration{Duration: 45 * time.Second}
	options := defaultConnectOptions()

	err := WithDatabaseConfig(config.DatabaseConfig{
		ConnectTimeout:   &connectTimeout,
		StatementTimeout: &statementTimeout,
	})(&options)
	require.NoError(t, err)
	assert.Equal(t, 12*time.Second, options.connectTimeout)
	assert.Equal(t, 45*time.Second, options.statementTimeout)
	assert.Equal(t, DefaultLockTimeout, options.lockTimeout)
}

func TestWithDatabaseConfigPreservesExplicitZero(t *testing.T) {
	disabled := config.Duration{}
	options := defaultConnectOptions()

	err := WithDatabaseConfig(config.DatabaseConfig{
		StatementTimeout: &disabled,
		LockTimeout:      &disabled,
	})(&options)
	require.NoError(t, err)
	assert.Equal(t, DefaultConnectTimeout, options.connectTimeout)
	assert.Zero(t, options.statementTimeout)
	assert.Zero(t, options.lockTimeout)
}

func TestVerifyTargetIdentity(t *testing.T) {
	t.Run("match", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "app", systemIdentifier: "123"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "000123"},
		)
		require.NoError(t, err)
	})

	t.Run("database mismatch", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "wrong", systemIdentifier: "123"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		var mismatch *TargetIdentityMismatchError
		require.ErrorAs(t, err, &mismatch)
		assert.Equal(t, "app", mismatch.ExpectedDatabase)
		assert.Equal(t, "wrong", mismatch.ActualDatabase)
		assert.Equal(t, uint64(123), mismatch.ExpectedSystemIdentifier)
		assert.Equal(t, uint64(123), mismatch.ActualSystemIdentifier)
	})

	t.Run("system identifier mismatch", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "app", systemIdentifier: "456"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		var mismatch *TargetIdentityMismatchError
		require.ErrorAs(t, err, &mismatch)
		assert.Equal(t, uint64(123), mismatch.ExpectedSystemIdentifier)
		assert.Equal(t, uint64(456), mismatch.ActualSystemIdentifier)
	})

	t.Run("invalid server value", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{database: "app", systemIdentifier: "not-a-number"},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		require.ErrorContains(t, err, "PostgreSQL returned invalid system identifier")
	})

	t.Run("permission failure", func(t *testing.T) {
		err := verifyTargetIdentity(
			context.Background(),
			stubIdentityQuerier{err: &pgconn.PgError{
				Code:    "42501",
				Message: "permission denied for function pg_control_system",
			}},
			"schemata",
			config.DatabaseIdentity{Database: "app", SystemIdentifier: "123"},
		)
		var permissionErr *TargetIdentityPermissionError
		require.ErrorAs(t, err, &permissionErr)
		assert.Equal(t, "schemata", permissionErr.Role)
	})
}

func TestTargetIdentityPermissionErrorIncludesPreciseRemediation(t *testing.T) {
	err := targetIdentityQueryError(`deploy"role`, &pgconn.PgError{
		Code:    "42501",
		Message: "permission denied for function pg_control_system",
	})

	require.ErrorContains(t, err, "permission denied while reading PostgreSQL system identity")
	assert.Contains(t, err.Error(), `GRANT pg_monitor TO "deploy""role";`)
	assert.Contains(t, err.Error(), `GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "deploy""role";`)
	var permissionErr *TargetIdentityPermissionError
	require.ErrorAs(t, err, &permissionErr)
	assert.Equal(t, `deploy"role`, permissionErr.Role)

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "42501", pgErr.Code)
}

type stubIdentityQuerier struct {
	database         string
	systemIdentifier string
	err              error
}

func clearPostgresEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range postgresEnvironmentVariables {
		t.Setenv(name, "")
	}
}

func testStringPointer(value string) *string {
	return &value
}

func (querier stubIdentityQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	return stubIdentityRow(querier)
}

type stubIdentityRow stubIdentityQuerier

func (row stubIdentityRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != 2 {
		return errors.New("expected two identity destinations")
	}
	database, ok := destinations[0].(*string)
	if !ok {
		return errors.New("database destination is not *string")
	}
	systemIdentifier, ok := destinations[1].(*string)
	if !ok {
		return errors.New("system identifier destination is not *string")
	}
	*database = row.database
	*systemIdentifier = row.systemIdentifier
	return nil
}
