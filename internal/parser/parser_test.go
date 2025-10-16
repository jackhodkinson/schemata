package parser

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseSimpleTable tests parsing a basic table
func TestParseSimpleTable(t *testing.T) {
	sql := `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		);
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	t.Logf("Parsed %d objects", len(objectMap))
	for key, obj := range objectMap {
		t.Logf("  Object: kind=%s, name=%s, type=%T", key.Kind, key.Name, obj.Payload)
	}

	require.Len(t, objectMap, 1, "Should have parsed 1 table")

	// Find the table
	var table schema.Table
	var found bool
	for _, obj := range objectMap {
		if tbl, ok := obj.Payload.(schema.Table); ok {
			table = tbl
			found = true
			break
		}
	}

	require.True(t, found, "Should have found a table object")
	assert.Equal(t, schema.TableName("users"), table.Name)
	assert.Len(t, table.Columns, 3)
	assert.NotNil(t, table.PrimaryKey)
	if table.PrimaryKey != nil {
		assert.Len(t, table.PrimaryKey.Cols, 1)
		assert.Equal(t, schema.ColumnName("id"), table.PrimaryKey.Cols[0])
	}
}

// TestParseTableWithConstraints tests parsing a table with various constraints
func TestParseTableWithConstraints(t *testing.T) {
	sql := `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			price NUMERIC(10,2),
			category_id INTEGER,
			CONSTRAINT chk_price CHECK (price > 0),
			CONSTRAINT fk_category FOREIGN KEY (category_id)
				REFERENCES categories(id) ON DELETE CASCADE
		);
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	t.Logf("Parsed %d objects", len(objectMap))
	for key := range objectMap {
		t.Logf("  Object: kind=%s, name=%s", key.Kind, key.Name)
	}

	// Find the table
	var table schema.Table
	var found bool
	for _, obj := range objectMap {
		if tbl, ok := obj.Payload.(schema.Table); ok {
			table = tbl
			found = true
			break
		}
	}

	require.True(t, found)
	assert.Equal(t, schema.TableName("products"), table.Name)
	assert.Len(t, table.Columns, 4)

	t.Logf("Table has %d checks, %d foreign keys", len(table.Checks), len(table.ForeignKeys))
	if len(table.Checks) > 0 {
		t.Logf("Check constraint: %s", table.Checks[0].Expr)
	}

	assert.Len(t, table.Checks, 1, "Should have 1 check constraint")
	if len(table.Checks) > 0 {
		assert.Contains(t, string(table.Checks[0].Expr), "price")
	}
	assert.Len(t, table.ForeignKeys, 1, "Should have 1 foreign key")
	if len(table.ForeignKeys) > 0 {
		assert.Equal(t, schema.Cascade, table.ForeignKeys[0].OnDelete)
	}
}

// TestParseIndex tests parsing CREATE INDEX statements
func TestParseIndex(t *testing.T) {
	sql := `
		CREATE UNIQUE INDEX idx_users_email ON users(email);
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Find the index
	var index schema.Index
	for _, obj := range objectMap {
		if idx, ok := obj.Payload.(schema.Index); ok {
			index = idx
			break
		}
	}

	assert.Equal(t, "idx_users_email", index.Name)
	assert.Equal(t, schema.TableName("users"), index.Table)
	assert.True(t, index.Unique)
	assert.Len(t, index.KeyExprs, 1)
}

// TestParsePartialIndex tests parsing a partial index with WHERE clause
func TestParsePartialIndex(t *testing.T) {
	sql := `
		CREATE INDEX idx_active_users ON users(created_at)
		WHERE active = true;
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Find the index
	var index schema.Index
	for _, obj := range objectMap {
		if idx, ok := obj.Payload.(schema.Index); ok {
			index = idx
			break
		}
	}

	assert.Equal(t, "idx_active_users", index.Name)
	assert.NotNil(t, index.Predicate)
	assert.Contains(t, string(*index.Predicate), "active")
}

// TestParseEnum tests parsing CREATE TYPE enum
func TestParseEnum(t *testing.T) {
	sql := `
		CREATE TYPE status AS ENUM ('pending', 'active', 'inactive');
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Find the enum
	var enum schema.EnumDef
	for _, obj := range objectMap {
		if e, ok := obj.Payload.(schema.EnumDef); ok {
			enum = e
			break
		}
	}

	assert.Equal(t, schema.TypeName("status"), enum.Name)
	assert.Equal(t, []string{"pending", "active", "inactive"}, enum.Values)
}

// TestParseDomain tests parsing CREATE DOMAIN
func TestParseDomain(t *testing.T) {
	sql := `
		CREATE DOMAIN email_address AS TEXT
		CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$');
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Find the domain
	var domain schema.DomainDef
	for _, obj := range objectMap {
		if d, ok := obj.Payload.(schema.DomainDef); ok {
			domain = d
			break
		}
	}

	assert.Equal(t, schema.TypeName("email_address"), domain.Name)
	assert.Equal(t, schema.TypeName("text"), domain.BaseType)
	assert.NotNil(t, domain.Check)
}

// TestParseView tests parsing CREATE VIEW
func TestParseView(t *testing.T) {
	sql := `
		CREATE VIEW active_users AS
		SELECT id, name, email FROM users WHERE active = true;
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Find the view
	var view schema.View
	for _, obj := range objectMap {
		if v, ok := obj.Payload.(schema.View); ok {
			view = v
			break
		}
	}

	assert.Equal(t, "active_users", view.Name)
	assert.Equal(t, schema.RegularView, view.Type)
	assert.Contains(t, view.Definition.Query, "SELECT")
}

// TestParseSequence tests parsing CREATE SEQUENCE
func TestParseSequence(t *testing.T) {
	sql := `
		CREATE SEQUENCE user_id_seq
		START 1000
		INCREMENT 1
		MINVALUE 1
		MAXVALUE 9999999
		CACHE 10;
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Find the sequence
	var seq schema.Sequence
	for _, obj := range objectMap {
		if s, ok := obj.Payload.(schema.Sequence); ok {
			seq = s
			break
		}
	}

	assert.Equal(t, "user_id_seq", seq.Name)
	assert.NotNil(t, seq.Start)
	assert.Equal(t, int64(1000), *seq.Start)
	assert.NotNil(t, seq.Increment)
	assert.Equal(t, int64(1), *seq.Increment)
}

// TestParseFunction tests parsing CREATE FUNCTION
func TestParseFunction(t *testing.T) {
	sql := `
		CREATE FUNCTION get_user_count() RETURNS INTEGER
		LANGUAGE SQL
		IMMUTABLE
		AS $$
			SELECT COUNT(*) FROM users;
		$$;
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Find the function
	var fn schema.Function
	for _, obj := range objectMap {
		if f, ok := obj.Payload.(schema.Function); ok {
			fn = f
			break
		}
	}

	assert.Equal(t, "get_user_count", fn.Name)
	assert.Equal(t, schema.SQL, fn.Language)
	assert.Equal(t, schema.Immutable, fn.Volatility)
}

// TestParseMultipleStatements tests parsing multiple DDL statements
func TestParseMultipleStatements(t *testing.T) {
	sql := `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);
		CREATE INDEX idx_posts_user ON posts(user_id);
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Should have 2 tables and 1 index
	tableCount := 0
	indexCount := 0

	for _, obj := range objectMap {
		switch obj.Payload.(type) {
		case schema.Table:
			tableCount++
		case schema.Index:
			indexCount++
		}
	}

	assert.Equal(t, 2, tableCount)
	assert.Equal(t, 1, indexCount)
}

// TestParseComplexSchema tests parsing a realistic schema
func TestParseComplexSchema(t *testing.T) {
	sql := `
		CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

		CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

		CREATE TABLE users (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			username VARCHAR(50) NOT NULL UNIQUE,
			email VARCHAR(255) NOT NULL,
			role user_role NOT NULL DEFAULT 'user',
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			CONSTRAINT valid_email CHECK (email ~ '^[A-Za-z0-9._%+-]+@')
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			title VARCHAR(200) NOT NULL,
			content TEXT,
			author_id UUID NOT NULL,
			published_at TIMESTAMP,
			CONSTRAINT fk_author FOREIGN KEY (author_id)
				REFERENCES users(id) ON DELETE CASCADE
		);

		CREATE INDEX idx_posts_author ON posts(author_id);
		CREATE INDEX idx_posts_published ON posts(published_at)
			WHERE published_at IS NOT NULL;

		CREATE VIEW published_posts AS
			SELECT p.*, u.username
			FROM posts p
			JOIN users u ON p.author_id = u.id
			WHERE p.published_at IS NOT NULL;
	`

	parser := NewParser()
	objectMap, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Count object types
	counts := make(map[schema.ObjectKind]int)
	for key := range objectMap {
		counts[key.Kind]++
	}

	assert.Equal(t, 1, counts[schema.ExtensionKind], "should have 1 extension")
	assert.Equal(t, 1, counts[schema.TypeKind], "should have 1 type")
	assert.Equal(t, 2, counts[schema.TableKind], "should have 2 tables")
	assert.Equal(t, 2, counts[schema.IndexKind], "should have 2 indexes")
	assert.Equal(t, 1, counts[schema.ViewKind], "should have 1 view")
}

// TestHashingConsistency tests that identical objects produce identical hashes
func TestHashingConsistency(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

	parser := NewParser()

	// Parse the same SQL twice
	objectMap1, err1 := parser.ParseSQL(sql)
	require.NoError(t, err1)

	objectMap2, err2 := parser.ParseSQL(sql)
	require.NoError(t, err2)

	// Extract hashes
	var hash1, hash2 string
	for _, obj := range objectMap1 {
		hash1 = obj.Hash
		break
	}
	for _, obj := range objectMap2 {
		hash2 = obj.Hash
		break
	}

	assert.Equal(t, hash1, hash2, "identical objects should produce identical hashes")
}

// TestParseFileNotFound tests error handling for missing files
func TestParseFileNotFound(t *testing.T) {
	parser := NewParser()
	_, err := parser.ParseFile("/nonexistent/file.sql")
	assert.Error(t, err)
}

// TestParseInvalidSQL tests error handling for invalid SQL
func TestParseInvalidSQL(t *testing.T) {
	sql := "THIS IS NOT VALID SQL;"

	parser := NewParser()
	_, err := parser.ParseSQL(sql)
	assert.Error(t, err)
}
