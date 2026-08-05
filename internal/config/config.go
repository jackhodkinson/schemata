package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// Config represents the main schemata configuration
type Config struct {
	Dev        *DBConnection           `yaml:"dev,omitempty"`
	Target     *DBConnection           `yaml:"target,omitempty"`
	Targets    map[string]DBConnection `yaml:"targets,omitempty"`
	Database   DatabaseConfig          `yaml:"database,omitempty"`
	Schema     SchemaConfig            `yaml:"schema"`
	Migrations MigrationsConfig        `yaml:"migrations"`
}

// DatabaseConfig controls PostgreSQL session safety settings for every
// connection Schemata opens. Nil values use the finite defaults in the db
// package; an explicit zero disables the corresponding PostgreSQL timeout.
type DatabaseConfig struct {
	ConnectTimeout   *Duration `yaml:"connect-timeout,omitempty"`
	StatementTimeout *Duration `yaml:"statement-timeout,omitempty"`
	LockTimeout      *Duration `yaml:"lock-timeout,omitempty"`
}

// IsZero allows generated YAML to omit the database section when both values
// use Schemata's defaults.
func (dc DatabaseConfig) IsZero() bool {
	return dc.ConnectTimeout == nil && dc.StatementTimeout == nil && dc.LockTimeout == nil
}

// Validate rejects timeout values that PostgreSQL cannot use safely.
func (dc DatabaseConfig) Validate() error {
	if dc.ConnectTimeout != nil && dc.ConnectTimeout.Duration <= 0 {
		return fmt.Errorf("database.connect-timeout must be greater than zero")
	}
	for _, timeout := range []struct {
		name  string
		value *Duration
	}{
		{name: "statement-timeout", value: dc.StatementTimeout},
		{name: "lock-timeout", value: dc.LockTimeout},
	} {
		if timeout.value != nil && timeout.value.Duration < 0 {
			return fmt.Errorf("database.%s must not be negative", timeout.name)
		}
	}

	return nil
}

// Duration is a YAML duration such as "30s", "5m", or "0". PostgreSQL
// timeout settings have millisecond resolution; smaller positive values are
// rounded up when a connection is configured.
type Duration struct {
	time.Duration
}

// UnmarshalYAML parses a human-readable Go duration and rejects negative
// values. The bare value 0 is accepted as an explicit timeout opt-out.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar value")
	}
	if err := validateText("duration", node.Value); err != nil {
		return err
	}

	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	if parsed < 0 {
		return fmt.Errorf("duration must not be negative")
	}

	d.Duration = parsed
	return nil
}

// MarshalYAML preserves the human-readable duration form in generated config.
func (d Duration) MarshalYAML() (interface{}, error) {
	return d.String(), nil
}

// MigrationsConfig can be either a simple directory path or detailed configuration
type MigrationsConfig struct {
	// Simple format (just a directory path string)
	FilePath *string `yaml:"-"`

	// Detailed format
	Dir    string `yaml:"dir,omitempty"`
	Format string `yaml:"format,omitempty"` // "sql" (default) or "moo"
}

// UnmarshalYAML implements custom unmarshaling for MigrationsConfig
func (mc *MigrationsConfig) UnmarshalYAML(node *yaml.Node) error {
	*mc = MigrationsConfig{}

	if node.Kind == yaml.ScalarNode {
		if node.Tag != "!!str" {
			return fmt.Errorf("migrations must be a directory string or mapping")
		}
		mc.FilePath = &node.Value
		return nil
	}

	knownFields := map[string]struct{}{"dir": {}, "format": {}}
	if err := validateMappingFields(node, "migrations", knownFields); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		if node.Content[index+1].Tag != "!!str" {
			return fmt.Errorf("migrations.%s must be a string", node.Content[index].Value)
		}
	}

	type migrationsConfigAlias struct {
		Dir    string `yaml:"dir"`
		Format string `yaml:"format,omitempty"`
	}

	var details migrationsConfigAlias
	if err := node.Decode(&details); err != nil {
		return err
	}

	mc.Dir = details.Dir
	mc.Format = details.Format
	return nil
}

// MarshalYAML implements custom marshaling for MigrationsConfig
func (mc MigrationsConfig) MarshalYAML() (interface{}, error) {
	if mc.FilePath != nil {
		return *mc.FilePath, nil
	}

	type migrationsConfigAlias struct {
		Dir    string `yaml:"dir"`
		Format string `yaml:"format,omitempty"`
	}

	return migrationsConfigAlias{
		Dir:    mc.Dir,
		Format: mc.Format,
	}, nil
}

// GetDir returns the migrations directory path
func (mc *MigrationsConfig) GetDir() string {
	if mc.FilePath != nil {
		return *mc.FilePath
	}
	return mc.Dir
}

// GetFormat returns the migration file format ("sql" by default)
func (mc *MigrationsConfig) GetFormat() string {
	if mc.Format != "" {
		return mc.Format
	}
	return "sql"
}

// DBConnection can be either a URL string or connection details
type DBConnection struct {
	// URL is emitted as a scalar for legacy/simple configurations and as the
	// "url" key when connection metadata such as Identity is present.
	URL *string `yaml:"url,omitempty"`

	// Structured format
	Host     *string    `yaml:"host,omitempty"`
	Port     *int       `yaml:"port,omitempty"`
	Username *string    `yaml:"username,omitempty"`
	Password *string    `yaml:"password,omitempty"`
	Database *string    `yaml:"database,omitempty"`
	SSL      *SSLConfig `yaml:"ssl,omitempty"`

	// Identity pins this connection to an expected database in an expected
	// PostgreSQL cluster. It is optional for compatibility, but production
	// targets should always configure it.
	Identity *DatabaseIdentity `yaml:"identity,omitempty"`
}

// explicitConnectionParameters is the complete conninfo surface accepted from
// configuration. Keeping this list deliberately small prevents pgx-specific
// pool/runtime options and external providers such as servicefile or passfile
// from changing the effective target before Schemata's safety checks run.
var explicitConnectionParameters = map[string]struct{}{
	"host":        {},
	"port":        {},
	"user":        {},
	"password":    {},
	"dbname":      {},
	"database":    {},
	"sslmode":     {},
	"sslrootcert": {},
	"sslcert":     {},
	"sslkey":      {},
}

// AllowedConnectionStringParameters returns the complete user-configurable
// PostgreSQL conninfo surface. The returned slice is sorted and independent so
// runtime callers can safely extend it with private neutralizer parameters.
func AllowedConnectionStringParameters() []string {
	return sortedStringKeys(explicitConnectionParameters)
}

// DatabaseIdentity uniquely identifies a PostgreSQL target within a cluster.
// SystemIdentifier is kept as a decimal string because PostgreSQL cluster
// identifiers are unsigned 64-bit values and YAML/JSON number handling is not
// consistently lossless at that width.
type DatabaseIdentity struct {
	Database         string `yaml:"database"`
	SystemIdentifier string `yaml:"system-identifier"`
}

// UnmarshalYAML keeps the identifier lossless and rejects misspelled identity
// fields instead of silently disabling part of the safety check.
func (identity *DatabaseIdentity) UnmarshalYAML(node *yaml.Node) error {
	*identity = DatabaseIdentity{}

	knownFields := map[string]struct{}{
		"database": {}, "system-identifier": {},
	}
	if err := validateMappingFields(node, "identity", knownFields); err != nil {
		return fmt.Errorf("identity must be a mapping with database and system-identifier: %w", err)
	}
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		value := node.Content[index+1]
		if _, ok := knownFields[key.Value]; !ok {
			return fmt.Errorf("unknown identity field %q", key.Value)
		}
		if key.Value == "database" && value.Tag != "!!str" {
			return fmt.Errorf("identity.database must be a string")
		}
		if key.Value == "system-identifier" && value.Tag != "!!str" {
			return fmt.Errorf("identity.system-identifier must be a quoted decimal string")
		}
	}

	type databaseIdentityAlias DatabaseIdentity
	var details databaseIdentityAlias
	if err := node.Decode(&details); err != nil {
		return err
	}
	*identity = DatabaseIdentity(details)
	return nil
}

// MarshalYAML quotes both values so database names that resemble YAML syntax
// (for example, the merge key "<<") cannot change type when reloaded, and the
// uint64 representation remains lossless.
func (identity DatabaseIdentity) MarshalYAML() (interface{}, error) {
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  "!!map",
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "database"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: identity.Database, Style: yaml.DoubleQuotedStyle},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "system-identifier"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: identity.SystemIdentifier, Style: yaml.DoubleQuotedStyle},
		},
	}, nil
}

// Validate requires a complete identity and a decimal uint64 PostgreSQL
// system identifier.
func (identity DatabaseIdentity) Validate() error {
	if err := validateText("identity.database", identity.Database); err != nil {
		return err
	}
	if err := validateText("identity.system-identifier", identity.SystemIdentifier); err != nil {
		return err
	}
	if strings.TrimSpace(identity.Database) == "" {
		return fmt.Errorf("identity.database must be specified")
	}
	if identity.SystemIdentifier == "" {
		return fmt.Errorf("identity.system-identifier must be specified")
	}
	if _, err := identity.SystemIdentifierValue(); err != nil {
		return fmt.Errorf("identity.system-identifier must be a decimal uint64: %w", err)
	}
	return nil
}

// SystemIdentifierValue parses the lossless string representation used in
// configuration.
func (identity DatabaseIdentity) SystemIdentifierValue() (uint64, error) {
	for _, char := range identity.SystemIdentifier {
		if char < '0' || char > '9' {
			return 0, fmt.Errorf("%q contains a non-decimal character", identity.SystemIdentifier)
		}
	}

	value, err := strconv.ParseUint(identity.SystemIdentifier, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is outside the uint64 range", identity.SystemIdentifier)
	}
	return value, nil
}

// UnmarshalYAML implements custom unmarshaling for DBConnection
// This allows it to handle both string URLs and structured objects
func (db *DBConnection) UnmarshalYAML(node *yaml.Node) error {
	*db = DBConnection{}

	if node.Kind == yaml.ScalarNode {
		if node.Tag != "!!str" {
			return fmt.Errorf("connection URL must be a string")
		}
		var urlStr string
		if err := node.Decode(&urlStr); err != nil {
			return fmt.Errorf("connection must be a URL string or mapping: %w", err)
		}
		db.URL = &urlStr
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("connection must be a URL string or mapping")
	}
	knownFields := map[string]struct{}{
		"url": {}, "host": {}, "port": {}, "username": {},
		"password": {}, "database": {}, "ssl": {}, "identity": {},
	}
	if err := validateMappingFields(node, "connection", knownFields); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		value := node.Content[index+1]
		switch key.Value {
		case "url", "host", "username", "password", "database":
			if value.Tag != "!!str" {
				return fmt.Errorf("connection.%s must be a string", key.Value)
			}
		case "port":
			if value.Tag != "!!int" {
				return fmt.Errorf("connection.port must be an integer")
			}
		case "ssl":
			if value.Kind != yaml.MappingNode {
				return fmt.Errorf("ssl must be a mapping")
			}
		case "identity":
			if value.Kind != yaml.MappingNode {
				return fmt.Errorf("identity must be a mapping with database and system-identifier")
			}
		}
	}

	// Otherwise, unmarshal URL/identity or structured connection details.
	type dbConnAlias DBConnection
	var details dbConnAlias
	if err := node.Decode(&details); err != nil {
		return err
	}

	*db = DBConnection(details)
	return nil
}

// MarshalYAML implements custom marshaling for DBConnection
func (db DBConnection) MarshalYAML() (interface{}, error) {
	if db.URL != nil && db.Identity == nil && !db.hasStructuredConnectionFields() {
		return *db.URL, nil
	}

	// Marshal as structured object
	type dbConnAlias DBConnection
	return dbConnAlias(db), nil
}

func (db DBConnection) hasStructuredConnectionFields() bool {
	return db.Host != nil || db.Port != nil || db.Username != nil ||
		db.Password != nil || db.Database != nil || db.SSL != nil
}

// SSLConfig represents SSL/TLS connection configuration
type SSLConfig struct {
	Mode       SSLMode `yaml:"mode"`
	CACert     *string `yaml:"ca-cert,omitempty"`
	ClientCert *string `yaml:"client-cert,omitempty"`
	ClientKey  *string `yaml:"client-key,omitempty"`
}

// UnmarshalYAML rejects misspelled TLS fields and values with implicit YAML
// types. Certificate paths and modes must always be explicit strings.
func (ssl *SSLConfig) UnmarshalYAML(node *yaml.Node) error {
	*ssl = SSLConfig{}
	knownFields := map[string]struct{}{
		"mode": {}, "ca-cert": {}, "client-cert": {}, "client-key": {},
	}
	if err := validateMappingFields(node, "ssl", knownFields); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		if node.Content[index+1].Tag != "!!str" {
			return fmt.Errorf("ssl.%s must be a string", node.Content[index].Value)
		}
	}

	type sslConfigAlias SSLConfig
	var details sslConfigAlias
	if err := node.Decode(&details); err != nil {
		return err
	}
	*ssl = SSLConfig(details)
	return nil
}

// SSLMode represents the SSL connection mode
type SSLMode string

const (
	SSLDisable    SSLMode = "disable"
	SSLAllow      SSLMode = "allow"
	SSLPrefer     SSLMode = "prefer"
	SSLRequire    SSLMode = "require"
	SSLVerifyCA   SSLMode = "verify-ca"
	SSLVerifyFull SSLMode = "verify-full"
)

// SchemaConfig can be either a simple schema path or detailed configuration
type SchemaConfig struct {
	// Simple format (just a file path string)
	FilePath *string `yaml:"-"`

	// Detailed format
	File    string    `yaml:"file,omitempty"`
	Include *[]string `yaml:"include,omitempty"`
	Exclude *[]string `yaml:"exclude,omitempty"`
}

// UnmarshalYAML implements custom unmarshaling for SchemaConfig
func (sc *SchemaConfig) UnmarshalYAML(node *yaml.Node) error {
	*sc = SchemaConfig{}

	if node.Kind == yaml.ScalarNode {
		if node.Tag != "!!str" {
			return fmt.Errorf("schema must be a file string or mapping")
		}
		sc.FilePath = &node.Value
		return nil
	}

	knownFields := map[string]struct{}{"file": {}, "include": {}, "exclude": {}}
	if err := validateMappingFields(node, "schema", knownFields); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		value := node.Content[index+1]
		if key.Value == "file" {
			if value.Tag != "!!str" {
				return fmt.Errorf("schema.file must be a string")
			}
			continue
		}
		if value.Kind != yaml.SequenceNode {
			return fmt.Errorf("schema.%s must be a list of strings", key.Value)
		}
		for _, item := range value.Content {
			if item.Tag != "!!str" {
				return fmt.Errorf("schema.%s must contain only strings", key.Value)
			}
		}
	}

	type schemaConfigAlias struct {
		File    string    `yaml:"file"`
		Include *[]string `yaml:"include,omitempty"`
		Exclude *[]string `yaml:"exclude,omitempty"`
	}

	var details schemaConfigAlias
	if err := node.Decode(&details); err != nil {
		return err
	}

	sc.File = details.File
	sc.Include = details.Include
	sc.Exclude = details.Exclude
	return nil
}

// MarshalYAML implements custom marshaling for SchemaConfig
func (sc SchemaConfig) MarshalYAML() (interface{}, error) {
	if sc.FilePath != nil {
		return *sc.FilePath, nil
	}

	type schemaConfigAlias struct {
		File    string    `yaml:"file"`
		Include *[]string `yaml:"include,omitempty"`
		Exclude *[]string `yaml:"exclude,omitempty"`
	}

	return schemaConfigAlias{
		File:    sc.File,
		Include: sc.Include,
		Exclude: sc.Exclude,
	}, nil
}

// GetSchemaPath returns the schema path from the config
func (sc *SchemaConfig) GetSchemaPath() string {
	if sc.FilePath != nil {
		return *sc.FilePath
	}
	return sc.File
}

// GetSchemaFilters returns the include/exclude filters
// Defaults to ["public"] if no filters specified
func (sc *SchemaConfig) GetSchemaFilters() (include []string, exclude []string) {
	if sc.Include != nil {
		return *sc.Include, nil
	}
	if sc.Exclude != nil {
		return nil, *sc.Exclude
	}
	// Default to public schema only
	return []string{"public"}, nil
}

// LookupEnvFunc supplies environment values to Parse. Passing the lookup in
// keeps decoding deterministic and allows callers such as fuzzers to parse
// configuration without reading or mutating process-wide environment state.
type LookupEnvFunc func(string) (string, bool)

// Parse decodes, expands, and validates one YAML configuration document.
// Unknown fields, invalid UTF-8, NUL bytes, and trailing YAML documents are
// rejected so malformed input cannot silently become a different target.
func Parse(data []byte, lookupEnv LookupEnvFunc) (*Config, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("failed to parse config file: configuration must be valid UTF-8")
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return nil, fmt.Errorf("failed to parse config file: configuration must not contain NUL bytes")
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
		return nil, fmt.Errorf("failed to parse config file: multiple YAML documents are not allowed")
	}

	if lookupEnv == nil {
		lookupEnv = func(string) (string, bool) { return "", false }
	}
	if err := cfg.expandEnvVars(lookupEnv); err != nil {
		return nil, fmt.Errorf("failed to expand environment variables: %w", err)
	}

	// Validate only after expansion so an unset variable cannot turn a valid
	// looking target into an empty or implicit connection.
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Load reads and parses a configuration file using the process environment.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	return Parse(data, os.LookupEnv)
}

// Save atomically writes the configuration with owner-only permissions. A
// configuration may contain database passwords and private-key paths, so it
// must never be exposed through the process umask or a partially written file.
func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary config file: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err := temporary.Chmod(0600); err != nil {
		return fmt.Errorf("failed to secure temporary config file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("failed to write temporary config file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("failed to flush temporary config file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("failed to close temporary config file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("failed to replace config file atomically: %w", err)
	}
	committed = true

	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("config file was written but its directory could not be opened for durability: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("config file was written but its directory could not be flushed: %w", err)
	}

	return nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("configuration is nil")
	}
	if err := c.Database.Validate(); err != nil {
		return err
	}

	// Must have either target or targets (not both)
	if c.Target != nil && c.Targets != nil {
		return fmt.Errorf("cannot have both 'target' and 'targets' in config")
	}

	if c.Target == nil && c.Targets == nil {
		return fmt.Errorf("must have either 'target' or 'targets' in config")
	}
	if c.Targets != nil && len(c.Targets) == 0 {
		return fmt.Errorf("'targets' must contain at least one target")
	}

	if c.Dev != nil {
		if err := c.Dev.Validate(); err != nil {
			return fmt.Errorf("invalid dev connection: %w", err)
		}
	}
	if c.Target != nil {
		if err := c.Target.Validate(); err != nil {
			return fmt.Errorf("invalid target connection: %w", err)
		}
	}
	for _, name := range sortedConnectionNames(c.Targets) {
		conn := c.Targets[name]
		if err := validateText("target name", name); err != nil {
			return err
		}
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("target name must not be empty")
		}
		if err := conn.Validate(); err != nil {
			return fmt.Errorf("invalid target %q connection: %w", name, err)
		}
	}

	// Schema path must be specified
	if err := validateText("schema path", c.Schema.GetSchemaPath()); err != nil {
		return err
	}
	if strings.TrimSpace(c.Schema.GetSchemaPath()) == "" {
		return fmt.Errorf("schema path must be specified")
	}
	if c.Schema.Include != nil && c.Schema.Exclude != nil {
		return fmt.Errorf("schema include and exclude filters cannot both be specified")
	}
	for _, filter := range appendFilterCopies(c.Schema.Include, c.Schema.Exclude) {
		if err := validateText("schema filter", filter); err != nil {
			return err
		}
		if strings.TrimSpace(filter) == "" {
			return fmt.Errorf("schema filters must not be empty")
		}
	}

	// Migrations directory must be specified
	if err := validateText("migrations directory", c.Migrations.GetDir()); err != nil {
		return err
	}
	if strings.TrimSpace(c.Migrations.GetDir()) == "" {
		return fmt.Errorf("migrations directory must be specified")
	}
	if err := validateText("migrations format", c.Migrations.GetFormat()); err != nil {
		return err
	}
	switch c.Migrations.GetFormat() {
	case "sql", "moo":
	default:
		return fmt.Errorf("unsupported migrations format %q (supported: sql, moo)", c.Migrations.GetFormat())
	}

	return nil
}

// Validate rejects implicit connection defaults for configured databases.
func (db *DBConnection) Validate() error {
	if db == nil {
		return fmt.Errorf("connection is nil")
	}
	if db.Identity != nil {
		if err := db.Identity.Validate(); err != nil {
			return fmt.Errorf("invalid target identity: %w", err)
		}
	}
	if db.URL != nil {
		if err := validateText("connection URL", *db.URL); err != nil {
			return err
		}
		if strings.TrimSpace(*db.URL) == "" {
			return fmt.Errorf("connection URL must not be empty")
		}
		if db.hasStructuredConnectionFields() {
			return fmt.Errorf("connection URL cannot be combined with host, port, username, password, database, or ssl fields")
		}
		if err := validateConnectionStringSyntax(*db.URL); err != nil {
			return err
		}
		return nil
	}
	required := []struct {
		name  string
		value *string
	}{
		{"host", db.Host},
		{"username", db.Username},
		{"database", db.Database},
	}
	for _, field := range required {
		if field.value != nil {
			if err := validateText("connection "+field.name, *field.value); err != nil {
				return err
			}
		}
		if field.value == nil || strings.TrimSpace(*field.value) == "" {
			return fmt.Errorf("%s must be explicitly configured", field.name)
		}
	}
	if err := validateSingleConnectionHost(*db.Host); err != nil {
		return err
	}
	if db.Password != nil {
		if err := validateText("connection password", *db.Password); err != nil {
			return err
		}
	}
	if db.Port != nil && (*db.Port < 1 || *db.Port > 65535) {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if db.SSL != nil {
		for _, field := range []struct {
			name  string
			value *string
		}{
			{name: "ca-cert", value: db.SSL.CACert},
			{name: "client-cert", value: db.SSL.ClientCert},
			{name: "client-key", value: db.SSL.ClientKey},
		} {
			if field.value != nil {
				if err := validateText("ssl."+field.name, *field.value); err != nil {
					return err
				}
			}
		}
		switch db.SSL.Mode {
		case SSLDisable, SSLAllow, SSLPrefer, SSLRequire, SSLVerifyCA, SSLVerifyFull:
		default:
			return fmt.Errorf("unsupported ssl mode %q", db.SSL.Mode)
		}
	}
	return nil
}

// GetSingleTarget returns the single target connection, or error if multi-target
func (c *Config) GetSingleTarget() (*DBConnection, error) {
	if c.Target != nil {
		return c.Target, nil
	}

	if c.Targets != nil {
		return nil, fmt.Errorf("multiple targets configured; must specify --target flag")
	}

	return nil, fmt.Errorf("no target configured")
}

// GetTargetByName returns a target by name (for multi-target configs)
func (c *Config) GetTargetByName(name string) (*DBConnection, error) {
	if c.Target != nil && name == "target" {
		return c.Target, nil
	}

	if c.Targets != nil {
		if conn, ok := c.Targets[name]; ok {
			return &conn, nil
		}
		return nil, fmt.Errorf("target '%s' not found in config", name)
	}

	return nil, fmt.Errorf("no targets configured")
}

// GetTargetNames returns all available target names
func (c *Config) GetTargetNames() []string {
	if c.Target != nil {
		return []string{"target"}
	}

	return sortedConnectionNames(c.Targets)
}

// ExpandEnvVars expands environment variables in the configuration
func (c *Config) ExpandEnvVars() error {
	return c.expandEnvVars(os.LookupEnv)
}

func (c *Config) expandEnvVars(lookupEnv LookupEnvFunc) error {
	if c.Dev != nil {
		if err := c.Dev.expandEnvVars(lookupEnv); err != nil {
			return err
		}
	}

	if c.Target != nil {
		if err := c.Target.expandEnvVars(lookupEnv); err != nil {
			return err
		}
	}

	for _, name := range sortedConnectionNames(c.Targets) {
		conn := c.Targets[name]
		connCopy := conn
		if err := connCopy.expandEnvVars(lookupEnv); err != nil {
			return fmt.Errorf("failed to expand env vars for target '%s': %w", name, err)
		}
		c.Targets[name] = connCopy
	}

	return nil
}

// ExpandEnvVars expands environment variables in the connection
func (db *DBConnection) ExpandEnvVars() error {
	return db.expandEnvVars(os.LookupEnv)
}

func (db *DBConnection) expandEnvVars(lookupEnv LookupEnvFunc) error {
	if db == nil {
		return fmt.Errorf("connection is nil")
	}
	if db.URL != nil {
		expanded, err := expandEnvVarWithLookup(*db.URL, lookupEnv)
		if err != nil {
			return err
		}
		db.URL = &expanded
	}

	if db.Host != nil {
		expanded, err := expandEnvVarWithLookup(*db.Host, lookupEnv)
		if err != nil {
			return err
		}
		db.Host = &expanded
	}

	if db.Username != nil {
		expanded, err := expandEnvVarWithLookup(*db.Username, lookupEnv)
		if err != nil {
			return err
		}
		db.Username = &expanded
	}

	if db.Password != nil {
		expanded, err := expandEnvVarWithLookup(*db.Password, lookupEnv)
		if err != nil {
			return err
		}
		db.Password = &expanded
	}

	if db.Database != nil {
		expanded, err := expandEnvVarWithLookup(*db.Database, lookupEnv)
		if err != nil {
			return err
		}
		db.Database = &expanded
	}

	if db.Identity != nil {
		database, err := expandEnvVarWithLookup(db.Identity.Database, lookupEnv)
		if err != nil {
			return fmt.Errorf("failed to expand identity.database: %w", err)
		}
		systemIdentifier, err := expandEnvVarWithLookup(db.Identity.SystemIdentifier, lookupEnv)
		if err != nil {
			return fmt.Errorf("failed to expand identity.system-identifier: %w", err)
		}
		db.Identity.Database = database
		db.Identity.SystemIdentifier = systemIdentifier
	}
	if db.SSL != nil {
		for _, field := range []struct {
			name  string
			value **string
		}{
			{name: "ca-cert", value: &db.SSL.CACert},
			{name: "client-cert", value: &db.SSL.ClientCert},
			{name: "client-key", value: &db.SSL.ClientKey},
		} {
			if *field.value == nil {
				continue
			}
			expanded, err := expandEnvVarWithLookup(**field.value, lookupEnv)
			if err != nil {
				return fmt.Errorf("failed to expand ssl.%s: %w", field.name, err)
			}
			*field.value = &expanded
		}
	}

	return nil
}

// expandEnvVar expands environment variable references in a string
// Supports ${VAR} and ${VAR:-default} syntax
func expandEnvVar(s string) (string, error) {
	return expandEnvVarWithLookup(s, os.LookupEnv)
}

// expandEnvVarWithLookup is the deterministic implementation used by Parse.
// It deliberately rejects malformed or nested placeholders rather than
// leaving them in a connection string as apparently literal text.
func expandEnvVarWithLookup(s string, lookupEnv LookupEnvFunc) (string, error) {
	if err := validateText("environment template", s); err != nil {
		return "", err
	}
	if lookupEnv == nil {
		lookupEnv = func(string) (string, bool) { return "", false }
	}

	var result strings.Builder
	for cursor := 0; cursor < len(s); {
		relativeStart := strings.Index(s[cursor:], "${")
		if relativeStart < 0 {
			result.WriteString(s[cursor:])
			break
		}
		start := cursor + relativeStart
		result.WriteString(s[cursor:start])
		relativeEnd := strings.IndexByte(s[start+2:], '}')
		if relativeEnd < 0 {
			return "", fmt.Errorf("unterminated environment placeholder at byte %d", start)
		}
		end := start + 2 + relativeEnd
		body := s[start+2 : end]
		if strings.Contains(body, "${") {
			return "", fmt.Errorf("nested environment placeholders are not supported")
		}

		varName := body
		fallback := ""
		hasFallback := false
		if separator := strings.Index(body, ":-"); separator >= 0 {
			varName = body[:separator]
			fallback = body[separator+2:]
			hasFallback = true
		}
		if !validEnvironmentName(varName) {
			return "", fmt.Errorf("invalid environment variable name %q", varName)
		}

		value, exists := lookupEnv(varName)
		if exists && value != "" {
			if err := validateText("environment variable "+varName, value); err != nil {
				return "", err
			}
			result.WriteString(value)
		} else if hasFallback {
			if err := validateText("environment fallback", fallback); err != nil {
				return "", err
			}
			result.WriteString(fallback)
		} else {
			return "", fmt.Errorf("environment variable %s is not set or is empty", varName)
		}
		cursor = end + 1
	}
	return result.String(), nil
}

// ToConnectionString converts a DBConnection to a PostgreSQL connection string
func (db *DBConnection) ToConnectionString() (string, error) {
	if err := db.Validate(); err != nil {
		return "", fmt.Errorf("invalid connection: %w", err)
	}
	if db.URL != nil {
		return *db.URL, nil
	}

	// Build connection string from parts
	parts := []string{}

	host, err := quoteConnectionValue(*db.Host)
	if err != nil {
		return "", err
	}
	parts = append(parts, "host="+host)

	port := 5432
	if db.Port != nil {
		port = *db.Port
	}
	parts = append(parts, fmt.Sprintf("port=%d", port))

	user, err := quoteConnectionValue(*db.Username)
	if err != nil {
		return "", err
	}
	parts = append(parts, "user="+user)

	if db.Password != nil {
		password, err := quoteConnectionValue(*db.Password)
		if err != nil {
			return "", err
		}
		parts = append(parts, "password="+password)
	}

	dbname, err := quoteConnectionValue(*db.Database)
	if err != nil {
		return "", err
	}
	parts = append(parts, "dbname="+dbname)

	// Add SSL configuration
	if db.SSL != nil {
		parts = append(parts, fmt.Sprintf("sslmode=%s", db.SSL.Mode))
		if db.SSL.CACert != nil {
			value, err := quoteConnectionValue(*db.SSL.CACert)
			if err != nil {
				return "", err
			}
			parts = append(parts, "sslrootcert="+value)
		}
		if db.SSL.ClientCert != nil {
			value, err := quoteConnectionValue(*db.SSL.ClientCert)
			if err != nil {
				return "", err
			}
			parts = append(parts, "sslcert="+value)
		}
		if db.SSL.ClientKey != nil {
			value, err := quoteConnectionValue(*db.SSL.ClientKey)
			if err != nil {
				return "", err
			}
			parts = append(parts, "sslkey="+value)
		}
	}

	return strings.Join(parts, " "), nil
}

func validateMappingFields(node *yaml.Node, objectName string, knownFields map[string]struct{}) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", objectName)
	}
	if len(node.Content)%2 != 0 {
		return fmt.Errorf("%s mapping is malformed", objectName)
	}
	seen := make(map[string]struct{}, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return fmt.Errorf("%s field names must be strings", objectName)
		}
		if _, ok := knownFields[key.Value]; !ok {
			return fmt.Errorf("unknown %s field %q", objectName, key.Value)
		}
		if _, duplicate := seen[key.Value]; duplicate {
			return fmt.Errorf("duplicate %s field %q", objectName, key.Value)
		}
		seen[key.Value] = struct{}{}
	}
	return nil
}

func validateText(fieldName, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", fieldName)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%s must not contain NUL bytes", fieldName)
	}
	return nil
}

func validEnvironmentName(name string) bool {
	if name == "" || !((name[0] >= 'A' && name[0] <= 'Z') || (name[0] >= 'a' && name[0] <= 'z') || name[0] == '_') {
		return false
	}
	for index := 1; index < len(name); index++ {
		char := name[index]
		if !((char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

func sortedConnectionNames(connections map[string]DBConnection) []string {
	names := make([]string, 0, len(connections))
	for name := range connections {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func appendFilterCopies(filters ...*[]string) []string {
	var values []string
	for _, filter := range filters {
		if filter != nil {
			values = append(values, (*filter)...)
		}
	}
	return values
}

func validateConnectionStringSyntax(connectionString string) error {
	if strings.HasPrefix(connectionString, "postgres://") || strings.HasPrefix(connectionString, "postgresql://") {
		return validatePostgresURL(connectionString)
	}

	fields, err := parseKeywordValueConnectionString(connectionString)
	if err != nil {
		return fmt.Errorf("connection must be a PostgreSQL URL or valid keyword/value string: %w", err)
	}
	for _, name := range sortedStringKeys(fields) {
		if err := validateExplicitConnectionParameter(name); err != nil {
			return err
		}
	}
	for _, field := range []string{"host", "user"} {
		if strings.TrimSpace(fields[field]) == "" {
			return fmt.Errorf("keyword/value connection must explicitly specify %s", field)
		}
	}
	if err := validateSingleConnectionHost(fields["host"]); err != nil {
		return fmt.Errorf("keyword/value connection: %w", err)
	}
	if port, present := fields["port"]; present {
		if err := validatePortList(port); err != nil {
			return fmt.Errorf("keyword/value connection port is invalid: %w", err)
		}
	}
	if sslMode, present := fields["sslmode"]; present {
		if err := validateSSLMode(sslMode); err != nil {
			return fmt.Errorf("keyword/value connection: %w", err)
		}
	}
	if _, hasDBName := fields["dbname"]; hasDBName {
		if _, hasDatabase := fields["database"]; hasDatabase {
			return fmt.Errorf("keyword/value connection cannot specify both dbname and database")
		}
	}
	database := fields["dbname"]
	if database == "" {
		database = fields["database"]
	}
	if strings.TrimSpace(database) == "" {
		return fmt.Errorf("keyword/value connection must explicitly specify dbname")
	}
	return nil
}

func validatePostgresURL(connectionString string) error {
	parsed, err := url.Parse(connectionString)
	if err != nil {
		// net/url errors may include the original URL, including credentials.
		return fmt.Errorf("connection URL is malformed")
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return fmt.Errorf("connection URL must use the postgres or postgresql scheme")
	}
	if parsed.Opaque != "" {
		return fmt.Errorf("connection URL must use hierarchical // syntax")
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("connection URL fragments are not supported")
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return fmt.Errorf("connection URL query is malformed: %w", err)
	}
	queryNames := make([]string, 0, len(query))
	for key := range query {
		queryNames = append(queryNames, key)
	}
	sort.Strings(queryNames)
	for _, key := range queryNames {
		values := query[key]
		if !validConnectionParameterName(key) {
			return fmt.Errorf("connection URL query parameter name %q is invalid", key)
		}
		if len(values) != 1 {
			return fmt.Errorf("connection URL query parameter %q must be specified exactly once", key)
		}
		if err := validateURLComponent("query parameter "+key, values[0]); err != nil {
			return err
		}
		if err := validateExplicitConnectionParameter(key); err != nil {
			return err
		}
	}
	if _, hasDatabase := query["database"]; hasDatabase {
		if _, hasDBName := query["dbname"]; hasDBName {
			return fmt.Errorf("connection URL cannot specify both database and dbname query parameters")
		}
	}

	user := ""
	authorityUser := false
	authorityPassword := false
	if parsed.User != nil {
		user = parsed.User.Username()
		authorityUser = user != ""
		if err := validateURLComponent("username", user); err != nil {
			return err
		}
		if password, present := parsed.User.Password(); present {
			authorityPassword = true
			if err := validateURLComponent("password", password); err != nil {
				return err
			}
		}
	}
	if queryUser, present := singleQueryValue(query, "user"); present {
		if authorityUser {
			return fmt.Errorf("connection URL cannot specify user in both authority and query")
		}
		user = queryUser
	}
	if strings.TrimSpace(user) == "" {
		return fmt.Errorf("connection URL must explicitly specify a user")
	}
	if _, queryPassword := singleQueryValue(query, "password"); queryPassword && authorityPassword {
		return fmt.Errorf("connection URL cannot specify password in both authority and query")
	}

	if parsed.Host != "" {
		if err := validateURLAuthorityHosts(parsed.Host); err != nil {
			return err
		}
	}
	host := parsed.Host
	if queryHost, present := singleQueryValue(query, "host"); present {
		if parsed.Host != "" {
			return fmt.Errorf("connection URL cannot specify host in both authority and query")
		}
		host = queryHost
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("connection URL must explicitly specify a host")
	}
	if err := validateSingleConnectionHost(host); err != nil {
		return fmt.Errorf("connection URL: %w", err)
	}
	if err := validateURLComponent("host", host); err != nil {
		return err
	}

	escapedPath := parsed.EscapedPath()
	database := ""
	if escapedPath != "" {
		if escapedPath[0] != '/' || strings.Contains(escapedPath[1:], "/") {
			return fmt.Errorf("connection URL database path must contain exactly one escaped path segment")
		}
		database = strings.TrimPrefix(parsed.Path, "/")
		if strings.HasPrefix(database, "/") {
			return fmt.Errorf("connection URL database must not begin with an encoded slash")
		}
		if err := validateURLComponent("database", database); err != nil {
			return err
		}
	}
	if queryDatabase, present := singleQueryValue(query, "database"); present {
		if database != "" {
			return fmt.Errorf("connection URL cannot specify database in both path and query")
		}
		database = queryDatabase
	}
	if queryDBName, present := singleQueryValue(query, "dbname"); present {
		if database != "" {
			return fmt.Errorf("connection URL cannot specify database in both path and query")
		}
		database = queryDBName
	}
	if strings.TrimSpace(database) == "" {
		return fmt.Errorf("connection URL must explicitly specify a database")
	}

	if queryPort, present := singleQueryValue(query, "port"); present {
		if authorityHasPort(parsed.Host) {
			return fmt.Errorf("connection URL cannot specify port in both authority and query")
		}
		if err := validatePortList(queryPort); err != nil {
			return fmt.Errorf("connection URL query port is invalid: %w", err)
		}
	}
	if sslMode, present := singleQueryValue(query, "sslmode"); present {
		if err := validateSSLMode(sslMode); err != nil {
			return fmt.Errorf("connection URL: %w", err)
		}
	}
	return nil
}

func authorityHasPort(authority string) bool {
	for host := range strings.SplitSeq(authority, ",") {
		if net.ParseIP(strings.Trim(host, "[]")) != nil || !strings.Contains(host, ":") {
			continue
		}
		_, port, err := net.SplitHostPort(host)
		if err == nil && port != "" {
			return true
		}
	}
	return false
}

func validateURLAuthorityHosts(authority string) error {
	if strings.Contains(authority, ",") {
		return fmt.Errorf("connection URL must identify exactly one server; multiple hosts are not supported")
	}
	for host := range strings.SplitSeq(authority, ",") {
		if host == "" {
			return fmt.Errorf("connection URL contains an empty host")
		}
		if err := validateURLComponent("host", host); err != nil {
			return err
		}
		if net.ParseIP(strings.Trim(host, "[]")) != nil || !strings.Contains(host, ":") {
			continue
		}
		name, port, err := net.SplitHostPort(host)
		if err != nil {
			return fmt.Errorf("connection URL host %q is malformed: %w", host, err)
		}
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("connection URL contains an empty host")
		}
		if port != "" {
			if err := validatePortList(port); err != nil {
				return fmt.Errorf("connection URL host port is invalid: %w", err)
			}
		}
	}
	return nil
}

func validatePortList(ports string) error {
	if strings.Contains(ports, ",") {
		return fmt.Errorf("exactly one port must be specified")
	}
	for port := range strings.SplitSeq(ports, ",") {
		value, err := strconv.ParseUint(port, 10, 16)
		if err != nil || value == 0 {
			return fmt.Errorf("port %q must be between 1 and 65535", port)
		}
	}
	return nil
}

func validateSingleConnectionHost(host string) error {
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("host must be explicitly configured")
	}
	if strings.Contains(host, ",") {
		return fmt.Errorf("host must identify exactly one server; multiple hosts are not supported")
	}
	if err := validateText("connection host", host); err != nil {
		return err
	}
	return nil
}

func validateSSLMode(value string) error {
	switch SSLMode(value) {
	case SSLDisable, SSLAllow, SSLPrefer, SSLRequire, SSLVerifyCA, SSLVerifyFull:
		return nil
	default:
		return fmt.Errorf("unsupported ssl mode %q", value)
	}
}

func validateURLComponent(name, value string) error {
	if err := validateText("connection URL "+name, value); err != nil {
		return err
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return fmt.Errorf("connection URL %s must not contain control characters", name)
		}
	}
	return nil
}

func singleQueryValue(query url.Values, name string) (string, bool) {
	values, present := query[name]
	if !present || len(values) == 0 {
		return "", false
	}
	return values[0], true
}

func validConnectionParameterName(name string) bool {
	if name == "" || !((name[0] >= 'A' && name[0] <= 'Z') ||
		(name[0] >= 'a' && name[0] <= 'z') || name[0] == '_') {
		return false
	}
	for index := 1; index < len(name); index++ {
		char := name[index]
		if !((char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

func validateExplicitConnectionParameter(name string) error {
	if _, allowed := explicitConnectionParameters[name]; !allowed {
		return fmt.Errorf(
			"connection parameter %q is not permitted; configure only target identity, credentials, and TLS settings explicitly",
			name,
		)
	}
	return nil
}

func sortedStringKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// parseKeywordValueConnectionString validates libpq's keyword/value syntax.
// It does not apply defaults or inspect environment/service files.
func parseKeywordValueConnectionString(input string) (map[string]string, error) {
	fields := make(map[string]string)
	for cursor := 0; ; {
		for cursor < len(input) && isConnectionSpace(input[cursor]) {
			cursor++
		}
		if cursor == len(input) {
			break
		}
		keyStart := cursor
		for cursor < len(input) && ((input[cursor] >= 'A' && input[cursor] <= 'Z') ||
			(input[cursor] >= 'a' && input[cursor] <= 'z') ||
			(cursor > keyStart && input[cursor] >= '0' && input[cursor] <= '9') || input[cursor] == '_') {
			cursor++
		}
		if cursor == keyStart {
			return nil, fmt.Errorf("expected a connection parameter at byte %d", cursor)
		}
		key := input[keyStart:cursor]
		for cursor < len(input) && isConnectionSpace(input[cursor]) {
			cursor++
		}
		if cursor >= len(input) || input[cursor] != '=' {
			return nil, fmt.Errorf("connection parameter %q is missing '='", key)
		}
		cursor++
		for cursor < len(input) && isConnectionSpace(input[cursor]) {
			cursor++
		}

		var value strings.Builder
		if cursor < len(input) && input[cursor] == '\'' {
			cursor++
			closed := false
			for cursor < len(input) {
				char := input[cursor]
				cursor++
				if char == '\'' {
					closed = true
					break
				}
				if char == '\\' {
					if cursor >= len(input) {
						return nil, fmt.Errorf("connection value for %q ends with an escape", key)
					}
					char = input[cursor]
					cursor++
				}
				value.WriteByte(char)
			}
			if !closed {
				return nil, fmt.Errorf("connection value for %q has an unterminated quote", key)
			}
			if cursor < len(input) && !isConnectionSpace(input[cursor]) {
				return nil, fmt.Errorf("unexpected character after quoted value for %q", key)
			}
		} else {
			for cursor < len(input) && !isConnectionSpace(input[cursor]) {
				char := input[cursor]
				cursor++
				if char == '\\' {
					if cursor >= len(input) {
						return nil, fmt.Errorf("connection value for %q ends with an escape", key)
					}
					char = input[cursor]
					cursor++
				}
				value.WriteByte(char)
			}
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, fmt.Errorf("duplicate connection parameter %q", key)
		}
		fields[key] = value.String()
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("connection string contains no parameters")
	}
	return fields, nil
}

func isConnectionSpace(char byte) bool {
	switch char {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

func quoteConnectionValue(value string) (string, error) {
	if err := validateText("connection value", value); err != nil {
		return "", err
	}
	needsQuotes := value == ""
	for _, char := range value {
		if unicode.IsSpace(char) || unicode.IsControl(char) || char == '\'' || char == '\\' {
			needsQuotes = true
			break
		}
	}
	if !needsQuotes {
		return value, nil
	}

	var quoted strings.Builder
	quoted.Grow(len(value) + 2)
	quoted.WriteByte('\'')
	for _, char := range value {
		if char == '\'' || char == '\\' {
			quoted.WriteByte('\\')
		}
		quoted.WriteRune(char)
	}
	quoted.WriteByte('\'')
	return quoted.String(), nil
}

// DetectEnvVar returns a stable ${NAME} reference when value is already a
// valid environment reference or exactly matches one process environment
// value. Duplicate matches are resolved by variable name, not os.Environ's
// platform-dependent order.
func DetectEnvVar(value string) string {
	return detectEnvVar(value, os.Environ())
}

func detectEnvVar(value string, environment []string) string {
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
		name := value[2 : len(value)-1]
		if validEnvironmentName(name) {
			return value
		}
		return value
	}
	if strings.HasPrefix(value, "$") {
		name := strings.TrimPrefix(value, "$")
		if validEnvironmentName(name) {
			return fmt.Sprintf("${%s}", name)
		}
		return value
	}
	if value == "" {
		return value
	}

	matches := make([]string, 0, 1)
	for _, entry := range environment {
		name, environmentValue, found := strings.Cut(entry, "=")
		if found && environmentValue == value && validEnvironmentName(name) {
			matches = append(matches, name)
		}
	}
	if len(matches) == 0 {
		return value
	}
	sort.Strings(matches)
	return fmt.Sprintf("${%s}", matches[0])
}
