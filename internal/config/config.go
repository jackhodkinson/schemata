package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the main schemata configuration
type Config struct {
	Dev        *DBConnection           `yaml:"dev,omitempty"`
	Target     *DBConnection           `yaml:"target,omitempty"`
	Targets    map[string]DBConnection `yaml:"targets,omitempty"`
	Schema     SchemaConfig            `yaml:"schema"`
	Migrations string                  `yaml:"migrations"`
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

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Expand environment variables
	if err := cfg.ExpandEnvVars(); err != nil {
		return nil, fmt.Errorf("failed to expand environment variables: %w", err)
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
	// Must have either target or targets (not both)
	if c.Target != nil && c.Targets != nil {
		return fmt.Errorf("cannot have both 'target' and 'targets' in config")
	}

	if c.Target == nil && c.Targets == nil {
		return fmt.Errorf("must have either 'target' or 'targets' in config")
	}

	// Schema path must be specified
	if c.Schema.GetSchemaPath() == "" {
		return fmt.Errorf("schema path must be specified")
	}

	// Migrations directory must be specified
	if c.Migrations == "" {
		return fmt.Errorf("migrations directory must be specified")
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
	return envVarRegex.ReplaceAllStringFunc(s, func(match string) string {
		matches := envVarRegex.FindStringSubmatch(match)
		if len(matches) < 2 {
			return match
		}

		varName := matches[1]
		defaultValue := ""
		if len(matches) > 2 {
			defaultValue = matches[2]
		}

		if value := os.Getenv(varName); value != "" {
			return value
		}

		return defaultValue
	}), nil
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
