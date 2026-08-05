package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSaveIsAtomicAndOwnerOnly(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "schemata.yaml")
	require.NoError(t, os.WriteFile(path, []byte("old, incomplete content"), 0644))

	url := "postgresql://app@db.example/app"
	config := &Config{
		Target:     &DBConnection{URL: &url},
		Schema:     SchemaConfig{FilePath: stringPointer("schema.sql")},
		Migrations: MigrationsConfig{FilePath: stringPointer("migrations")},
	}
	require.NoError(t, config.Save(path))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())

	loaded, err := Load(path)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(config, loaded))

	temporaryFiles, err := filepath.Glob(filepath.Join(directory, ".schemata.yaml.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, temporaryFiles)
}

func stringPointer(value string) *string { return &value }

func TestParseIsDeterministicAndRoundTripsEffectiveConfiguration(t *testing.T) {
	input := []byte(`
dev:
  host: dev-db
  username: dev
  password: ${DEV_PASSWORD}
  database: dev
targets:
  production:
    url: ${TARGET_URL}
    identity:
      database: ${TARGET_DATABASE}
      system-identifier: ${TARGET_SYSTEM_IDENTIFIER}
  staging: postgresql://app@staging.example:5432/app
database:
  connect-timeout: 12s
  statement-timeout: 1m30s
  lock-timeout: 1250ms
schema:
  file: schema.sql
  include: [public, app]
migrations:
  dir: migrations
  format: moo
`)
	environment := map[string]string{
		"DEV_PASSWORD":             "space and ' quote \\ slash",
		"TARGET_URL":               "postgresql://app@production.example:5432/app",
		"TARGET_DATABASE":          "app",
		"TARGET_SYSTEM_IDENTIFIER": "18446744073709551615",
	}
	lookup := lookupMap(environment)

	first, err := Parse(input, lookup)
	require.NoError(t, err)
	second, err := Parse(input, lookup)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(first, second), "repeated parse changed the decoded config")
	require.Equal(t, []string{"production", "staging"}, first.GetTargetNames())

	marshaled, err := yaml.Marshal(first)
	require.NoError(t, err)
	roundTripped, err := Parse(marshaled, nil)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(first, roundTripped), "YAML round trip changed effective config\n%s", marshaled)

	require.Equal(t, 12*time.Second, first.Database.ConnectTimeout.Duration)
	require.Equal(t, 90*time.Second, first.Database.StatementTimeout.Duration)
	require.Equal(t, 1250*time.Millisecond, first.Database.LockTimeout.Duration)
	require.Equal(t, "18446744073709551615", first.Targets["production"].Identity.SystemIdentifier)
	for _, name := range first.GetTargetNames() {
		before := first.Targets[name]
		after := roundTripped.Targets[name]
		beforeString, beforeErr := before.ToConnectionString()
		afterString, afterErr := after.ToConnectionString()
		require.Equal(t, beforeErr, afterErr)
		require.Equal(t, beforeString, afterString)
	}
}

func TestParseRejectsUnknownMalformedAndImplicitTargetsDeterministically(t *testing.T) {
	validSuffix := "\nschema: schema.sql\nmigrations: migrations\n"
	tests := []struct {
		name  string
		input []byte
	}{
		{name: "invalid UTF-8", input: []byte{'t', 'a', 'r', 'g', 'e', 't', ':', ' ', 0xff}},
		{name: "literal NUL", input: []byte("target: postgresql://app@db/app\x00prod" + validSuffix)},
		{name: "decoded NUL", input: []byte("target: \"postgresql://app@db/app\\0prod\"" + validSuffix)},
		{name: "unknown top-level field", input: []byte("target: postgresql://app@db/app\ntraget: postgresql://app@other/app" + validSuffix)},
		{name: "unknown database field", input: []byte("target: postgresql://app@db/app\ndatabase:\n  statement-timeuot: 1s" + validSuffix)},
		{name: "unknown connection field", input: []byte("target:\n  url: postgresql://app@db/app\n  databse: app" + validSuffix)},
		{name: "unknown identity field", input: []byte("target:\n  url: postgresql://app@db/app\n  identity:\n    database: app\n    system-identifier: \"1\"\n    cluster: prod" + validSuffix)},
		{name: "unknown SSL field", input: []byte("target:\n  host: db\n  username: app\n  database: app\n  ssl:\n    mode: require\n    ca-certt: root.pem" + validSuffix)},
		{name: "unknown schema field", input: []byte("target: postgresql://app@db/app\nschema:\n  file: schema.sql\n  includes: [public]\nmigrations: migrations\n")},
		{name: "unknown migrations field", input: []byte("target: postgresql://app@db/app\nschema: schema.sql\nmigrations:\n  dir: migrations\n  formats: sql\n")},
		{name: "unterminated placeholder", input: []byte("target: ${TARGET_URL" + validSuffix)},
		{name: "empty placeholder name", input: []byte("target: ${}" + validSuffix)},
		{name: "invalid placeholder name", input: []byte("target: ${1TARGET}" + validSuffix)},
		{name: "nested placeholder", input: []byte("target: ${TARGET:-${FALLBACK}}" + validSuffix)},
		{name: "unset placeholder", input: []byte("target: ${TARGET_URL}" + validSuffix)},
		{name: "empty scalar target", input: []byte("target: ''" + validSuffix)},
		{name: "implicit mapping target", input: []byte("target: {}" + validSuffix)},
		{name: "non-string target", input: []byte("target: 5432" + validSuffix)},
		{name: "malformed URL", input: []byte("target: postgresql://%" + validSuffix)},
		{name: "URL missing user", input: []byte("target: postgresql://db/app" + validSuffix)},
		{name: "URL missing host", input: []byte("target: postgresql://app@/app" + validSuffix)},
		{name: "URL missing database", input: []byte("target: postgresql://app@db" + validSuffix)},
		{name: "implicit keyword connection", input: []byte("target: sslmode=require" + validSuffix)},
		{name: "duplicate keyword", input: []byte("target: host=db host=other user=app dbname=app" + validSuffix)},
		{name: "ambiguous database keywords", input: []byte("target: host=db user=app dbname=app database=other" + validSuffix)},
		{name: "keyword invalid port", input: []byte("target: host=db port=not-a-number user=app dbname=app" + validSuffix)},
		{name: "keyword multiple ports", input: []byte("target: host=db port=5432,5433 user=app dbname=app" + validSuffix)},
		{name: "keyword multiple hosts", input: []byte("target: host=db,,other user=app dbname=app" + validSuffix)},
		{name: "keyword invalid sslmode", input: []byte("target: host=db user=app dbname=app sslmode=bogus" + validSuffix)},
		{name: "URL multiple authority hosts", input: []byte("target: postgresql://app@db,other/app" + validSuffix)},
		{name: "URL multiple query hosts", input: []byte("target: postgresql://app@/app?host=db,,other" + validSuffix)},
		{name: "URL decoded leading slash database", input: []byte("target: postgresql://app@db/%2Fprod" + validSuffix)},
		{name: "URL invalid sslmode", input: []byte("target: postgresql://app@db/app?sslmode=bogus" + validSuffix)},
		{name: "both schema filters", input: []byte("target: postgresql://app@db/app\nschema:\n  file: schema.sql\n  include: [public]\n  exclude: [private]\nmigrations: migrations\n")},
		{name: "trailing YAML document", input: []byte("target: postgresql://app@db/app" + validSuffix + "---\ntarget: postgresql://app@other/app\n")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, firstErr := Parse(test.input, nil)
			_, secondErr := Parse(test.input, nil)
			require.Error(t, firstErr)
			require.Equal(t, firstErr.Error(), secondErr.Error())
		})
	}
}

func TestDBConnectionScalarAndMappingRoundTripsPreserveEffectiveTarget(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "scalar URL", input: `postgresql://app@db.example:5432/app`},
		{name: "scalar URL with query user", input: `postgresql://db.example:5432/app?user=app`},
		{name: "scalar URL with socket host", input: `postgresql://app@/app?host=%2Fvar%2Frun%2Fpostgresql`},
		{name: "scalar keyword", input: `host=db.example user=app dbname=app sslmode=require`},
		{name: "mapped URL and identity", input: `
url: postgresql://app@db.example:5432/app
identity:
  database: app
  system-identifier: "18446744073709551615"
`},
		{name: "structured fields", input: `
host: db.example
port: 6543
username: app
password: "a password with ' and \\"
database: app
ssl:
  mode: verify-full
  ca-cert: /certs/root.pem
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var before DBConnection
			require.NoError(t, yaml.Unmarshal([]byte(test.input), &before))
			require.NoError(t, before.Validate())
			beforeString, err := before.ToConnectionString()
			require.NoError(t, err)

			encoded, err := yaml.Marshal(before)
			require.NoError(t, err)
			var after DBConnection
			require.NoError(t, yaml.Unmarshal(encoded, &after))
			require.NoError(t, after.Validate())
			afterString, err := after.ToConnectionString()
			require.NoError(t, err)

			require.True(t, reflect.DeepEqual(before, after), "connection changed after round trip\n%s", encoded)
			require.Equal(t, beforeString, afterString)
		})
	}
}

func TestPostgresURLValidationRejectsDecodedAndAmbiguousValues(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{name: "authority user", url: "postgresql://app@db.example:5432/app"},
		{name: "query user", url: "postgresql://db.example:5432/app?user=app"},
		{name: "socket query host", url: "postgresql://app@/app?host=%2Fvar%2Frun%2Fpostgresql"},
		{name: "escaped slash in database", url: "postgresql://app@db.example/app%2Ftenant"},
		{name: "decoded leading slash database", url: "postgresql://app@db.example/%2Fprod", wantErr: "encoded slash"},
		{name: "multiple authority hosts", url: "postgresql://app@db.example,other.example/app", wantErr: "multiple hosts"},
		{name: "multiple query hosts", url: "postgresql://app@/app?host=db.example,,other.example", wantErr: "multiple hosts"},
		{name: "missing user", url: "postgresql://db.example/app", wantErr: "explicitly specify a user"},
		{name: "user in authority and query", url: "postgresql://app@db.example/app?user=other", wantErr: "both authority and query"},
		{name: "host in authority and query", url: "postgresql://app@db.example/app?host=other", wantErr: "both authority and query"},
		{name: "database in path and query", url: "postgresql://app@db.example/app?dbname=other", wantErr: "both path and query"},
		{name: "password in authority and query", url: "postgresql://app:secret@db.example/app?password=other", wantErr: "both authority and query"},
		{name: "port in authority and query", url: "postgresql://app@db.example:5432/app?port=5433", wantErr: "both authority and query"},
		{name: "NUL username", url: "postgresql://app%00admin@db.example/app", wantErr: "NUL"},
		{name: "NUL password", url: "postgresql://app:secret%00suffix@db.example/app", wantErr: "NUL"},
		{name: "NUL database", url: "postgresql://app@db.example/app%00suffix", wantErr: "NUL"},
		{name: "NUL query value", url: "postgresql://app@db.example/app?application_name=schemata%00other", wantErr: "NUL"},
		{name: "newline query value", url: "postgresql://app@db.example/app?application_name=schemata%0Aother", wantErr: "control"},
		{name: "invalid UTF-8 query value", url: "postgresql://app@db.example/app?application_name=%FF", wantErr: "UTF-8"},
		{name: "fragment", url: "postgresql://app@db.example/app#ignored", wantErr: "fragments"},
		{name: "extra raw path segment", url: "postgresql://app@db.example/app/tenant", wantErr: "exactly one"},
		{name: "duplicate query value", url: "postgresql://db.example/app?user=app&user=other", wantErr: "exactly once"},
		{name: "ambiguous database aliases", url: "postgresql://app@db.example/app?database=one&dbname=two", wantErr: "both database and dbname"},
		{name: "malformed query escape", url: "postgresql://app@db.example/app?user=%", wantErr: "malformed"},
		{name: "invalid query name", url: "postgresql://app@db.example/app?bad-name=value", wantErr: "name"},
		{name: "zero query port", url: "postgresql://app@db.example/app?port=0", wantErr: "between 1 and 65535"},
		{name: "multiple query ports", url: "postgresql://app@db.example/app?port=5432,5433", wantErr: "exactly one port"},
		{name: "invalid ssl mode", url: "postgresql://app@db.example/app?sslmode=bogus", wantErr: "unsupported ssl mode"},
		{name: "overflow authority port", url: "postgresql://app@db.example:65536/app", wantErr: "between 1 and 65535"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := DBConnection{URL: strPtr(test.url)}
			err := connection.Validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				separator := "?"
				if strings.Contains(test.url, "?") {
					separator = "&"
				}
				_, pgxErr := pgconn.ParseConfigWithOptions(
					test.url+separator+"sslmode=disable",
					pgconn.ParseConfigOptions{ConnStringAllowedKeys: []string{
						"host", "port", "user", "password", "dbname", "database", "sslmode",
					}},
				)
				require.NoError(t, pgxErr, "strict validator accepted a URL shape pgx rejects")
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestExplicitConnectionStringsUseMinimalParameterAllowList(t *testing.T) {
	accepted := []string{
		"postgresql://app@db.example:5432/app?sslmode=verify-full&sslrootcert=%2Fcerts%2Froot.pem&sslcert=%2Fcerts%2Fclient.pem&sslkey=%2Fcerts%2Fclient.key",
		"host=db.example port=5432 user=app password=secret dbname=app sslmode=verify-full sslrootcert=/certs/root.pem sslcert=/certs/client.pem sslkey=/certs/client.key",
	}
	for _, connectionString := range accepted {
		connection := DBConnection{URL: strPtr(connectionString)}
		require.NoError(t, connection.Validate(), connectionString)
	}

	rejectedParameters := []string{
		"service",
		"servicefile",
		"passfile",
		"options",
		"application_name",
		"statement_timeout",
		"default_query_exec_mode",
		"pool_max_conns",
		"future_pgx_option",
	}
	for _, parameter := range rejectedParameters {
		t.Run(parameter+" URL", func(t *testing.T) {
			connection := DBConnection{URL: strPtr(
				"postgresql://app@db.example/app?" + parameter + "=unsafe",
			)}
			require.ErrorContains(t, connection.Validate(), "is not permitted")
		})
		t.Run(parameter+" keyword", func(t *testing.T) {
			connection := DBConnection{URL: strPtr(
				"host=db.example user=app dbname=app " + parameter + "=unsafe",
			)}
			require.ErrorContains(t, connection.Validate(), "is not permitted")
		})
	}
}

func TestStructuredConnectionStringEscapesValuesWithoutAddingParameters(t *testing.T) {
	connection := DBConnection{
		Host:     strPtr("db host"),
		Port:     intPtr(5432),
		Username: strPtr("role'name"),
		Password: strPtr("secret x=y\\z' sslmode=disable"),
		Database: strPtr("app db"),
		SSL: &SSLConfig{
			Mode:       SSLVerifyFull,
			CACert:     strPtr("/certs/root cert.pem"),
			ClientCert: strPtr("/certs/client'cert.pem"),
			ClientKey:  strPtr("/certs/client\\key.pem"),
		},
	}

	connectionString, err := connection.ToConnectionString()
	require.NoError(t, err)
	fields, err := parseKeywordValueConnectionString(connectionString)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"host":        "db host",
		"port":        "5432",
		"user":        "role'name",
		"password":    "secret x=y\\z' sslmode=disable",
		"dbname":      "app db",
		"sslmode":     "verify-full",
		"sslrootcert": "/certs/root cert.pem",
		"sslcert":     "/certs/client'cert.pem",
		"sslkey":      "/certs/client\\key.pem",
	}, fields)
	assert.Equal(t, "verify-full", fields["sslmode"], "password text must not inject sslmode")

	pgxConnection := connection
	pgxConnection.SSL = nil
	pgxConnectionString, err := pgxConnection.ToConnectionString()
	require.NoError(t, err)
	pgxConfig, err := parseRenderedConnectionWithPGX(pgxConnectionString)
	require.NoError(t, err)
	require.Equal(t, "db host", pgxConfig.Host)
	require.Equal(t, uint16(5432), pgxConfig.Port)
	require.Equal(t, "role'name", pgxConfig.User)
	require.Equal(t, "secret x=y\\z' sslmode=disable", pgxConfig.Password)
	require.Equal(t, "app db", pgxConfig.Database)
}

func TestEnvironmentExpansionUsesOnlyInjectedLookupAndRejectsMalformedInput(t *testing.T) {
	lookup := lookupMap(map[string]string{"PRESENT": "value", "EMPTY": ""})
	tests := []struct {
		input   string
		want    string
		wantErr string
	}{
		{input: "literal", want: "literal"},
		{input: "before-${PRESENT}-after", want: "before-value-after"},
		{input: "${MISSING:-fallback:value}", want: "fallback:value"},
		{input: "${EMPTY:-fallback}", want: "fallback"},
		{input: "${MISSING}", wantErr: "not set or is empty"},
		{input: "${}", wantErr: "invalid environment variable name"},
		{input: "${BAD-NAME}", wantErr: "invalid environment variable name"},
		{input: "${UNFINISHED", wantErr: "unterminated"},
		{input: "${OUTER:-${INNER}}", wantErr: "nested"},
		{input: "\x00", wantErr: "NUL"},
		{input: string([]byte{0xff}), wantErr: "UTF-8"},
	}

	for _, test := range tests {
		got, err := expandEnvVarWithLookup(test.input, lookup)
		if test.wantErr != "" {
			require.ErrorContains(t, err, test.wantErr, "input %q", test.input)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
}

func TestDurationAndSystemIdentifierBoundariesRoundTrip(t *testing.T) {
	durations := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{input: "0", want: 0},
		{input: "1ns", want: time.Nanosecond},
		{input: "2562047h47m16.854775807s", want: time.Duration(1<<63 - 1)},
		{input: "-1ns", wantErr: true},
		{input: "2562047h47m16.854775808s", wantErr: true},
		{input: "eventually", wantErr: true},
	}
	for _, test := range durations {
		var duration Duration
		err := yaml.Unmarshal([]byte(test.input), &duration)
		if test.wantErr {
			require.Error(t, err, test.input)
			continue
		}
		require.NoError(t, err, test.input)
		require.Equal(t, test.want, duration.Duration)
		encoded, err := yaml.Marshal(duration)
		require.NoError(t, err)
		var roundTripped Duration
		require.NoError(t, yaml.Unmarshal(encoded, &roundTripped))
		require.Equal(t, duration, roundTripped)
	}

	identifiers := []struct {
		input   string
		want    uint64
		wantErr bool
	}{
		{input: "0", want: 0},
		{input: "000001", want: 1},
		{input: "18446744073709551615", want: ^uint64(0)},
		{input: "", wantErr: true},
		{input: "18446744073709551616", wantErr: true},
		{input: "-1", wantErr: true},
		{input: "1\x00", wantErr: true},
		{input: string([]byte{'1', 0xff}), wantErr: true},
	}
	for _, test := range identifiers {
		identity := DatabaseIdentity{Database: "app", SystemIdentifier: test.input}
		err := identity.Validate()
		if test.wantErr {
			require.Error(t, err, test.input)
			continue
		}
		require.NoError(t, err, test.input)
		value, err := identity.SystemIdentifierValue()
		require.NoError(t, err)
		require.Equal(t, test.want, value)
	}
}

func TestDatabaseIdentityMarshalQuotesYAMLSyntax(t *testing.T) {
	identity := DatabaseIdentity{Database: "<<", SystemIdentifier: "0"}

	encoded, err := yaml.Marshal(identity)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `database: "<<"`)

	var roundTripped DatabaseIdentity
	require.NoError(t, yaml.Unmarshal(encoded, &roundTripped))
	require.Equal(t, identity, roundTripped)
}

func TestToConnectionStringRejectsImplicitMalformedAndNULConnections(t *testing.T) {
	tests := []DBConnection{
		{},
		{Host: strPtr("db")},
		{URL: strPtr("postgresql://db")},
		{URL: strPtr("host=db user=app")},
		{Host: strPtr("db"), Username: strPtr("app"), Database: strPtr("app"), Password: strPtr("bad\x00password")},
		{Host: strPtr(string([]byte{'d', 'b', 0xff})), Username: strPtr("app"), Database: strPtr("app")},
	}
	for _, connection := range tests {
		_, err := connection.ToConnectionString()
		require.Error(t, err)
	}
}

func lookupMap(values map[string]string) LookupEnvFunc {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func parseRenderedConnectionWithPGX(connectionString string) (*pgconn.Config, error) {
	return pgconn.ParseConfigWithOptions(
		"passfile='' sslcert='' sslkey='' sslrootcert='' "+connectionString+" sslmode=disable",
		pgconn.ParseConfigOptions{ConnStringAllowedKeys: []string{
			"host", "port", "user", "password", "dbname", "sslmode",
			"sslrootcert", "sslcert", "sslkey", "passfile",
		}},
	)
}
