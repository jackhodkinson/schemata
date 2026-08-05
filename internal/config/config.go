package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

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
	StatementTimeout *Duration `yaml:"statement-timeout,omitempty"`
	LockTimeout      *Duration `yaml:"lock-timeout,omitempty"`
}

// IsZero allows generated YAML to omit the database section when both values
// use Schemata's defaults.
func (dc DatabaseConfig) IsZero() bool {
	return dc.StatementTimeout == nil && dc.LockTimeout == nil
}

// Validate rejects timeout values that PostgreSQL cannot use safely.
func (dc DatabaseConfig) Validate() error {
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
	// Try to unmarshal as a string first
	var dirPath string
	if err := node.Decode(&dirPath); err == nil {
		mc.FilePath = &dirPath
		return nil
	}

	// Otherwise, unmarshal as structured config
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
	// Simple URL format
	URL *string `yaml:"-"`

	// Structured format
	Host     *string    `yaml:"host,omitempty"`
	Port     *int       `yaml:"port,omitempty"`
	Username *string    `yaml:"username,omitempty"`
	Password *string    `yaml:"password,omitempty"`
	Database *string    `yaml:"database,omitempty"`
	SSL      *SSLConfig `yaml:"ssl,omitempty"`
}

// UnmarshalYAML implements custom unmarshaling for DBConnection
// This allows it to handle both string URLs and structured objects
func (db *DBConnection) UnmarshalYAML(node *yaml.Node) error {
	// Try to unmarshal as a string first
	var urlStr string
	if err := node.Decode(&urlStr); err == nil {
		db.URL = &urlStr
		return nil
	}

	// Otherwise, unmarshal as structured connection details
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
	if db.URL != nil {
		return *db.URL, nil
	}

	// Marshal as structured object
	type dbConnAlias DBConnection
	return dbConnAlias(db), nil
}

// SSLConfig represents SSL/TLS connection configuration
type SSLConfig struct {
	Mode       SSLMode `yaml:"mode"`
	CACert     *string `yaml:"ca-cert,omitempty"`
	ClientCert *string `yaml:"client-cert,omitempty"`
	ClientKey  *string `yaml:"client-key,omitempty"`
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
	// Try to unmarshal as a string first
	var filePath string
	if err := node.Decode(&filePath); err == nil {
		sc.FilePath = &filePath
		return nil
	}

	// Otherwise, unmarshal as structured config
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

// Load reads and parses a configuration file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Expand environment variables
	if err := cfg.ExpandEnvVars(); err != nil {
		return nil, fmt.Errorf("failed to expand environment variables: %w", err)
	}

	// Validate only after expansion so an unset variable cannot turn a valid
	// looking target into an empty or implicit connection.
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Save writes the configuration to a file
func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
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
	for name, conn := range c.Targets {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("target name must not be empty")
		}
		if err := conn.Validate(); err != nil {
			return fmt.Errorf("invalid target %q connection: %w", name, err)
		}
	}

	// Schema path must be specified
	if c.Schema.GetSchemaPath() == "" {
		return fmt.Errorf("schema path must be specified")
	}

	// Migrations directory must be specified
	if c.Migrations.GetDir() == "" {
		return fmt.Errorf("migrations directory must be specified")
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
	if db.URL != nil {
		if strings.TrimSpace(*db.URL) == "" {
			return fmt.Errorf("connection URL must not be empty")
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
		if field.value == nil || strings.TrimSpace(*field.value) == "" {
			return fmt.Errorf("%s must be explicitly configured", field.name)
		}
	}
	if db.Port != nil && (*db.Port < 1 || *db.Port > 65535) {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if db.SSL != nil {
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

	names := make([]string, 0, len(c.Targets))
	for name := range c.Targets {
		names = append(names, name)
	}
	return names
}

// ExpandEnvVars expands environment variables in the configuration
func (c *Config) ExpandEnvVars() error {
	if c.Dev != nil {
		if err := c.Dev.ExpandEnvVars(); err != nil {
			return err
		}
	}

	if c.Target != nil {
		if err := c.Target.ExpandEnvVars(); err != nil {
			return err
		}
	}

	for name, conn := range c.Targets {
		connCopy := conn
		if err := connCopy.ExpandEnvVars(); err != nil {
			return fmt.Errorf("failed to expand env vars for target '%s': %w", name, err)
		}
		c.Targets[name] = connCopy
	}

	return nil
}

// ExpandEnvVars expands environment variables in the connection
func (db *DBConnection) ExpandEnvVars() error {
	if db.URL != nil {
		expanded, err := expandEnvVar(*db.URL)
		if err != nil {
			return err
		}
		db.URL = &expanded
	}

	if db.Host != nil {
		expanded, err := expandEnvVar(*db.Host)
		if err != nil {
			return err
		}
		db.Host = &expanded
	}

	if db.Username != nil {
		expanded, err := expandEnvVar(*db.Username)
		if err != nil {
			return err
		}
		db.Username = &expanded
	}

	if db.Password != nil {
		expanded, err := expandEnvVar(*db.Password)
		if err != nil {
			return err
		}
		db.Password = &expanded
	}

	if db.Database != nil {
		expanded, err := expandEnvVar(*db.Database)
		if err != nil {
			return err
		}
		db.Database = &expanded
	}

	return nil
}

// Regex to match ${VAR} or ${VAR:-default} syntax
var envVarRegex = regexp.MustCompile(`\$\{([^}:]+)(?::-([^}]*))?\}`)

// expandEnvVar expands environment variable references in a string
// Supports ${VAR} and ${VAR:-default} syntax
func expandEnvVar(s string) (string, error) {
	var result strings.Builder
	last := 0
	for _, indexes := range envVarRegex.FindAllStringSubmatchIndex(s, -1) {
		result.WriteString(s[last:indexes[0]])
		varName := s[indexes[2]:indexes[3]]
		value, exists := os.LookupEnv(varName)
		if exists && value != "" {
			result.WriteString(value)
		} else if indexes[4] >= 0 {
			result.WriteString(s[indexes[4]:indexes[5]])
		} else {
			return "", fmt.Errorf("environment variable %s is not set or is empty", varName)
		}
		last = indexes[1]
	}
	result.WriteString(s[last:])
	return result.String(), nil
}

// ToConnectionString converts a DBConnection to a PostgreSQL connection string
func (db *DBConnection) ToConnectionString() (string, error) {
	if db.URL != nil {
		return *db.URL, nil
	}

	// Build connection string from parts
	parts := []string{}

	host := "localhost"
	if db.Host != nil && *db.Host != "" {
		host = *db.Host
	}
	parts = append(parts, fmt.Sprintf("host=%s", host))

	port := 5432
	if db.Port != nil {
		port = *db.Port
	}
	parts = append(parts, fmt.Sprintf("port=%d", port))

	user := "postgres"
	if db.Username != nil && *db.Username != "" {
		user = *db.Username
	}
	parts = append(parts, fmt.Sprintf("user=%s", user))

	if db.Password != nil && *db.Password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", *db.Password))
	}

	dbname := user
	if db.Database != nil && *db.Database != "" {
		dbname = *db.Database
	}
	parts = append(parts, fmt.Sprintf("dbname=%s", dbname))

	// Add SSL configuration
	if db.SSL != nil {
		parts = append(parts, fmt.Sprintf("sslmode=%s", db.SSL.Mode))
		if db.SSL.CACert != nil {
			parts = append(parts, fmt.Sprintf("sslrootcert=%s", *db.SSL.CACert))
		}
		if db.SSL.ClientCert != nil {
			parts = append(parts, fmt.Sprintf("sslcert=%s", *db.SSL.ClientCert))
		}
		if db.SSL.ClientKey != nil {
			parts = append(parts, fmt.Sprintf("sslkey=%s", *db.SSL.ClientKey))
		}
	}

	return strings.Join(parts, " "), nil
}

// DetectEnvVar returns the value wrapped in ${} syntax if it looks like an env var reference
func DetectEnvVar(value string) string {
	// If value starts with $, assume it's already an env var
	if strings.HasPrefix(value, "$") {
		varName := strings.TrimPrefix(value, "$")
		return fmt.Sprintf("${%s}", varName)
	}

	// Check if this value matches an environment variable
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		if len(pair) == 2 && pair[1] == value {
			return fmt.Sprintf("${%s}", pair[0])
		}
	}

	// Return as-is if not an env var
	return value
}
