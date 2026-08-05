package config

import (
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

func FuzzParseConfig(f *testing.F) {
	seeds := []string{
		"target: postgresql://app@localhost:5432/app\nschema: schema.sql\nmigrations: migrations\n",
		"target: ${TARGET_URL}\nschema: schema.sql\nmigrations: migrations\n",
		"target:\n  host: localhost\n  port: 5432\n  username: app\n  password: ${PASSWORD:-local}\n  database: app\nschema: schema.sql\nmigrations: migrations\n",
		"target:\n  url: postgresql://app@db.example:5432/app\n  identity:\n    database: app\n    system-identifier: \"18446744073709551615\"\ndatabase:\n  statement-timeout: 15m\n  lock-timeout: 10s\nschema: schema.sql\nmigrations: migrations\n",
		"targets:\n  production: postgresql://app@prod.example:5432/app\n  staging: postgresql://app@staging.example:5432/app\nschema:\n  file: schema.sql\n  exclude: [private]\nmigrations:\n  dir: migrations\n  format: moo\n",
		"target: {}\nschema: schema.sql\nmigrations: migrations\n",
		"target: ${UNFINISHED\nschema: schema.sql\nmigrations: migrations\n",
		"target: \"postgresql://app@db/app\\0other\"\nschema: schema.sql\nmigrations: migrations\n",
		"target: postgresql://app@db/app\nschema: schema.sql\nmigrations: migrations\nunknown: true\n",
	}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}

	environment := lookupMap(map[string]string{
		"TARGET_URL": "postgresql://app@prod.example:5432/app",
		"PASSWORD":   "secret with spaces and ' quotes",
	})
	f.Fuzz(func(t *testing.T, input []byte) {
		first, firstErr := Parse(input, environment)
		second, secondErr := Parse(input, environment)
		if errorText(firstErr) != errorText(secondErr) {
			t.Fatalf("parse is nondeterministic: first=%v second=%v", firstErr, secondErr)
		}
		if firstErr != nil {
			return
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("parse returned different configs for identical input")
		}
		if first == nil {
			t.Fatal("successful parse returned a nil config")
		}
		if err := first.Validate(); err != nil {
			t.Fatalf("successful parse returned an invalid config: %v", err)
		}

		encoded, err := yaml.Marshal(first)
		if err != nil {
			t.Fatalf("valid config cannot be marshaled: %v", err)
		}
		roundTripped, err := Parse(encoded, nil)
		if err != nil {
			t.Fatalf("resolved config cannot be parsed after YAML round trip: %v\n%s", err, encoded)
		}
		if !reflect.DeepEqual(first, roundTripped) {
			t.Fatalf("YAML round trip changed effective target, identity, or timeouts\nbefore: %#v\nafter: %#v", first, roundTripped)
		}
	})
}

func FuzzDBConnectionYAML(f *testing.F) {
	for _, seed := range []string{
		"postgresql://app@localhost:5432/app",
		"postgresql://localhost:5432/app?user=app",
		"host=localhost user=app dbname=app sslmode=require",
		"url: postgresql://app@db.example:5432/app\nidentity:\n  database: app\n  system-identifier: \"0\"\n",
		"host: localhost\nport: 5432\nusername: app\npassword: secret\ndatabase: app\n",
		"{}",
		"5432",
		"url: \"postgresql://app@db/app\\0other\"",
		"postgresql://app%00admin@db/app",
		"postgresql://app@db/app%00other",
		"postgresql://app@db/app?application_name=x%0Aother",
		"postgresql://app@db/app?user=app&user=other",
		"postgresql://app@db/app?servicefile=/tmp/redirect&service=prod",
		"host=db user=app dbname=app passfile=/tmp/passwords",
		"host=db user=app dbname=app pool_max_conns=1000",
		"host: db\nusername: app\ndatabase: app\nunknown: true\n",
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, input []byte) {
		var first DBConnection
		firstDecodeErr := yaml.Unmarshal(input, &first)
		var second DBConnection
		secondDecodeErr := yaml.Unmarshal(input, &second)
		if errorText(firstDecodeErr) != errorText(secondDecodeErr) {
			t.Fatalf("connection decode is nondeterministic: first=%v second=%v", firstDecodeErr, secondDecodeErr)
		}
		if firstDecodeErr != nil {
			return
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatal("connection decode returned different values for identical input")
		}

		firstValidateErr := first.Validate()
		secondValidateErr := second.Validate()
		if errorText(firstValidateErr) != errorText(secondValidateErr) {
			t.Fatalf("connection validation is nondeterministic: first=%v second=%v", firstValidateErr, secondValidateErr)
		}
		if firstValidateErr != nil {
			if _, err := first.ToConnectionString(); err == nil {
				t.Fatal("invalid connection was accepted by ToConnectionString")
			}
			return
		}

		beforeString, err := first.ToConnectionString()
		if err != nil {
			t.Fatalf("valid connection cannot be rendered: %v", err)
		}
		encoded, err := yaml.Marshal(first)
		if err != nil {
			t.Fatalf("valid connection cannot be marshaled: %v", err)
		}
		var roundTripped DBConnection
		if err := yaml.Unmarshal(encoded, &roundTripped); err != nil {
			t.Fatalf("valid connection cannot be decoded after round trip: %v\n%s", err, encoded)
		}
		if err := roundTripped.Validate(); err != nil {
			t.Fatalf("round-tripped connection became invalid: %v\n%s", err, encoded)
		}
		afterString, err := roundTripped.ToConnectionString()
		if err != nil {
			t.Fatalf("round-tripped connection cannot be rendered: %v", err)
		}
		if !reflect.DeepEqual(first, roundTripped) || beforeString != afterString {
			t.Fatalf("connection round trip changed effective target")
		}
	})
}

func FuzzEnvironmentPlaceholderExpansion(f *testing.F) {
	f.Add("${NAME}", "NAME", "value", true)
	f.Add("${NAME:-fallback}", "NAME", "", false)
	f.Add("prefix-${NAME}-suffix", "NAME", "value", true)
	f.Add("${UNFINISHED", "NAME", "value", true)
	f.Add("${1INVALID}", "1INVALID", "value", true)
	f.Add("${OUTER:-${INNER}}", "OUTER", "", false)
	f.Add("literal", "NAME", "value", true)
	f.Add("\x00", "NAME", "value", true)

	f.Fuzz(func(t *testing.T, input, environmentName, environmentValue string, exists bool) {
		lookup := func(name string) (string, bool) {
			if exists && name == environmentName {
				return environmentValue, true
			}
			return "", false
		}
		first, firstErr := expandEnvVarWithLookup(input, lookup)
		second, secondErr := expandEnvVarWithLookup(input, lookup)
		if errorText(firstErr) != errorText(secondErr) || first != second {
			t.Fatalf("environment expansion is nondeterministic: first=%q/%v second=%q/%v", first, firstErr, second, secondErr)
		}
		if firstErr == nil && (!utf8.ValidString(first) || containsNUL(first)) {
			t.Fatalf("environment expansion accepted invalid output %q", first)
		}
	})
}

func FuzzDatabaseIdentity(f *testing.F) {
	f.Add("app", "0")
	f.Add("app", "000001")
	f.Add("app", "18446744073709551615")
	f.Add("app", "18446744073709551616")
	f.Add("app", "-1")
	f.Add("", "1")
	f.Add("app", "1\x00")

	f.Fuzz(func(t *testing.T, database, systemIdentifier string) {
		identity := DatabaseIdentity{Database: database, SystemIdentifier: systemIdentifier}
		firstErr := identity.Validate()
		secondErr := identity.Validate()
		if errorText(firstErr) != errorText(secondErr) {
			t.Fatalf("identity validation is nondeterministic: first=%v second=%v", firstErr, secondErr)
		}
		if firstErr != nil {
			return
		}
		firstValue, firstValueErr := identity.SystemIdentifierValue()
		secondValue, secondValueErr := identity.SystemIdentifierValue()
		if errorText(firstValueErr) != errorText(secondValueErr) || firstValue != secondValue {
			t.Fatal("system identifier parsing is nondeterministic")
		}

		encoded, err := yaml.Marshal(identity)
		if err != nil {
			t.Fatalf("valid identity cannot be marshaled: %v", err)
		}
		var roundTripped DatabaseIdentity
		if err := yaml.Unmarshal(encoded, &roundTripped); err != nil {
			t.Fatalf("valid identity cannot be decoded after round trip: %v", err)
		}
		if !reflect.DeepEqual(identity, roundTripped) {
			t.Fatalf("identity changed after YAML round trip: before=%#v after=%#v\n%s", identity, roundTripped, encoded)
		}
	})
}

func FuzzDuration(f *testing.F) {
	for _, seed := range []string{"0", "1ns", "1250ms", "15m", "2562047h47m16.854775807s", "-1ns", "eventually", "[]", "\"1s\\0\""} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, input []byte) {
		var first Duration
		firstErr := yaml.Unmarshal(input, &first)
		var second Duration
		secondErr := yaml.Unmarshal(input, &second)
		if errorText(firstErr) != errorText(secondErr) || first != second {
			t.Fatalf("duration decode is nondeterministic: first=%v/%v second=%v/%v", first, firstErr, second, secondErr)
		}
		if firstErr != nil {
			return
		}
		if first.Duration < 0 {
			t.Fatalf("negative duration was accepted: %s", first.Duration)
		}
		encoded, err := yaml.Marshal(first)
		if err != nil {
			t.Fatalf("valid duration cannot be marshaled: %v", err)
		}
		var roundTripped Duration
		if err := yaml.Unmarshal(encoded, &roundTripped); err != nil {
			t.Fatalf("valid duration cannot be decoded after round trip: %v", err)
		}
		if first != roundTripped {
			t.Fatalf("duration changed after YAML round trip: %v != %v", first, roundTripped)
		}
	})
}

func FuzzConnectionStringConstruction(f *testing.F) {
	f.Add("localhost", "app", "secret", "app", 5432, "", "", "", "")
	f.Add("db host", "role'name", "secret x=y\\z' sslmode=disable", "app db", 6543, "verify-full", "/root cert", "/client'cert", "/client\\key")
	f.Add("/var/run/postgresql", "role\nname", "line one\nline two\t\\'", "app\ndatabase", 5432, "disable", "", "", "")
	f.Add("", "", "", "", 0, "invalid", "", "", "")
	f.Add("db\x00host", "app", "secret", "app", 5432, "disable", "", "", "")

	f.Fuzz(func(t *testing.T, host, username, password, database string, port int, sslMode, caCert, clientCert, clientKey string) {
		connection := DBConnection{
			Host:     &host,
			Port:     &port,
			Username: &username,
			Password: &password,
			Database: &database,
		}
		if sslMode != "" {
			connection.SSL = &SSLConfig{
				Mode:       SSLMode(sslMode),
				CACert:     &caCert,
				ClientCert: &clientCert,
				ClientKey:  &clientKey,
			}
		}

		first, firstErr := connection.ToConnectionString()
		second, secondErr := connection.ToConnectionString()
		if errorText(firstErr) != errorText(secondErr) || first != second {
			t.Fatalf("connection rendering is nondeterministic: first=%q/%v second=%q/%v", first, firstErr, second, secondErr)
		}
		validateErr := connection.Validate()
		if validateErr != nil {
			if firstErr == nil {
				t.Fatalf("invalid connection was rendered: %q", first)
			}
			return
		}
		if firstErr != nil {
			t.Fatalf("valid connection was not rendered: %v", firstErr)
		}

		fields, err := parseKeywordValueConnectionString(first)
		if err != nil {
			t.Fatalf("rendered connection is not valid keyword/value syntax: %v; %q", err, first)
		}
		expected := map[string]string{
			"host":   host,
			"port":   strconv.Itoa(port),
			"user":   username,
			"dbname": database,
		}
		expected["password"] = password
		if connection.SSL != nil {
			expected["sslmode"] = sslMode
			expected["sslrootcert"] = caCert
			expected["sslcert"] = clientCert
			expected["sslkey"] = clientKey
		}
		if !reflect.DeepEqual(expected, fields) {
			t.Fatalf("rendered connection changed or injected fields\nwant: %#v\ngot:  %#v\n%s", expected, fields, first)
		}

		pgxConnection := connection
		pgxConnection.SSL = nil
		pgxInput, err := pgxConnection.ToConnectionString()
		if err != nil {
			t.Fatalf("valid core connection was not rendered: %v", err)
		}
		pgxConfig, err := parseRenderedConnectionWithPGX(pgxInput)
		if err != nil {
			t.Fatalf("pgx rejected rendered connection: %v; %q", err, first)
		}
		if pgxConfig.Host != host || pgxConfig.Port != uint16(port) ||
			pgxConfig.User != username || pgxConfig.Password != password || pgxConfig.Database != database {
			t.Fatalf(
				"pgx changed rendered connection fields: want host=%q port=%d user=%q password=%q database=%q; got host=%q port=%d user=%q password=%q database=%q",
				host, port, username, password, database,
				pgxConfig.Host, pgxConfig.Port, pgxConfig.User, pgxConfig.Password, pgxConfig.Database,
			)
		}
	})
}

func FuzzExplicitConnectionParameterRejection(f *testing.F) {
	for _, seed := range []string{
		"service",
		"servicefile",
		"passfile",
		"options",
		"application_name",
		"default_query_exec_mode",
		"pool_max_conns",
		"future_pgx_option",
	} {
		f.Add(seed, "unsafe")
	}

	f.Fuzz(func(t *testing.T, name, value string) {
		if !validConnectionParameterName(name) {
			return
		}
		if _, allowed := explicitConnectionParameters[name]; allowed {
			return
		}
		if !utf8.ValidString(value) || containsNUL(value) {
			return
		}

		urlConnection := DBConnection{URL: stringPointer(
			"postgresql://app@db.example/app?" + url.QueryEscape(name) + "=" + url.QueryEscape(value),
		)}
		if err := urlConnection.Validate(); err == nil {
			t.Fatalf("URL connection accepted non-allow-listed parameter %q", name)
		}

		quotedValue, err := quoteConnectionValue(value)
		if err != nil {
			return
		}
		keywordConnection := DBConnection{URL: stringPointer(
			"host=db.example user=app dbname=app " + name + "=" + quotedValue,
		)}
		if err := keywordConnection.Validate(); err == nil {
			t.Fatalf("keyword connection accepted non-allow-listed parameter %q", name)
		}
	})
}

func FuzzDetectEnvVarDeterminism(f *testing.F) {
	f.Add("shared-secret", "Z_LAST", "A_FIRST")
	f.Add("$VALID_NAME", "Z_LAST", "A_FIRST")
	f.Add("${VALID_NAME}", "Z_LAST", "A_FIRST")
	f.Add("$1INVALID", "Z_LAST", "A_FIRST")

	f.Fuzz(func(t *testing.T, value, firstName, secondName string) {
		firstEnvironment := []string{
			firstName + "=" + value,
			secondName + "=" + value,
		}
		secondEnvironment := []string{
			secondName + "=" + value,
			firstName + "=" + value,
		}
		first := detectEnvVar(value, firstEnvironment)
		second := detectEnvVar(value, secondEnvironment)
		if first != second {
			t.Fatalf("environment order changed detection: first=%q second=%q", first, second)
		}
		if first == value {
			return
		}
		if !strings.HasPrefix(first, "${") || !strings.HasSuffix(first, "}") ||
			!validEnvironmentName(first[2:len(first)-1]) {
			t.Fatalf("detection produced an invalid environment reference %q", first)
		}
	})
}

func containsNUL(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] == 0 {
			return true
		}
	}
	return false
}
