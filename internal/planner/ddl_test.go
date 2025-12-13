package planner

import (
	"strings"
	"testing"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateCreateTable(t *testing.T) {
	gen := NewDDLGenerator()

	table := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "email", Type: "TEXT", NotNull: true},
			{Name: "name", Type: "TEXT", NotNull: false},
		},
		PrimaryKey: &schema.PrimaryKey{
			Cols: []schema.ColumnName{"id"},
		},
	}

	stmt, err := gen.GenerateCreateStatement(table)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE TABLE public.users")
	assert.Contains(t, stmt, "id INTEGER NOT NULL")
	assert.Contains(t, stmt, "email TEXT NOT NULL")
	assert.Contains(t, stmt, "name TEXT")
	assert.Contains(t, stmt, "PRIMARY KEY (id)")
}

func TestGenerateCreateTableWithConstraints(t *testing.T) {
	gen := NewDDLGenerator()

	defaultExpr := schema.Expr("NOW()")
	table := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "email", Type: "TEXT", NotNull: true},
			{Name: "created_at", Type: "TIMESTAMP", Default: &defaultExpr},
		},
		PrimaryKey: &schema.PrimaryKey{
			Cols: []schema.ColumnName{"id"},
		},
		Uniques: []schema.UniqueConstraint{
			{Name: "users_email_unique", Cols: []schema.ColumnName{"email"}},
		},
		Checks: []schema.CheckConstraint{
			{Name: "email_valid", Expr: "email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"},
		},
	}

	stmt, err := gen.GenerateCreateStatement(table)
	require.NoError(t, err)

	assert.Contains(t, stmt, "DEFAULT NOW()")
	assert.Contains(t, stmt, "CONSTRAINT users_email_unique UNIQUE (email)")
	assert.Contains(t, stmt, "CONSTRAINT email_valid CHECK")
}

func TestGenerateCreateTableWithCollationAndComment(t *testing.T) {
	gen := NewDDLGenerator()

	collation := "en-US-x-icu"
	comment := "Email address"
	table := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{
				Name:      "email",
				Type:      "TEXT",
				Collation: &collation,
				Comment:   &comment,
			},
		},
	}

	stmt, err := gen.GenerateCreateStatement(table)
	require.NoError(t, err)

	assert.Contains(t, stmt, "COLLATE \"en-US-x-icu\"")
	assert.Contains(t, stmt, "COMMENT ON COLUMN public.users.email IS 'Email address';")
}

func TestGenerateCreateTableWithGeneratedColumn(t *testing.T) {
	gen := NewDDLGenerator()

	genExpr := schema.Expr("COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')")
	table := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{
				Name:      "full_name",
				Type:      "TEXT",
				Generated: &schema.GeneratedSpec{Expr: genExpr, Stored: true},
			},
		},
	}

	stmt, err := gen.GenerateCreateStatement(table)
	require.NoError(t, err)

	assert.Contains(t, stmt, "GENERATED ALWAYS AS (COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')) STORED")
}

func TestGenerateCreateTableWithForeignKey(t *testing.T) {
	gen := NewDDLGenerator()

	table := schema.Table{
		Schema: "public",
		Name:   "posts",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "user_id", Type: "INTEGER", NotNull: true},
			{Name: "title", Type: "TEXT", NotNull: true},
		},
		PrimaryKey: &schema.PrimaryKey{
			Cols: []schema.ColumnName{"id"},
		},
		ForeignKeys: []schema.ForeignKey{
			{
				Name: "fk_posts_user",
				Cols: []schema.ColumnName{"user_id"},
				Ref: schema.ForeignKeyRef{
					Schema: "public",
					Table:  "users",
					Cols:   []schema.ColumnName{"id"},
				},
				OnDelete: schema.Cascade,
				OnUpdate: schema.NoAction,
			},
		},
	}

	stmt, err := gen.GenerateCreateStatement(table)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CONSTRAINT fk_posts_user FOREIGN KEY (user_id)")
	assert.Contains(t, stmt, "REFERENCES public.users (id)")
	assert.Contains(t, stmt, "ON DELETE CASCADE")
}

func TestGenerateCreateTableWithNotValidConstraints(t *testing.T) {
	gen := NewDDLGenerator()

	table := schema.Table{
		Schema: "public",
		Name:   "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "customer_id", Type: "INTEGER"},
			{Name: "code", Type: "TEXT"},
		},
		PrimaryKey: &schema.PrimaryKey{
			Cols: []schema.ColumnName{"id"},
		},
		Uniques: []schema.UniqueConstraint{
			{Name: "orders_code_key", Cols: []schema.ColumnName{"code"}, NullsDistinct: true, NotValid: true},
		},
		Checks: []schema.CheckConstraint{
			{Name: "orders_id_positive", Expr: "id > 0", NotValid: true},
		},
		ForeignKeys: []schema.ForeignKey{
			{
				Name: "orders_customer_fk",
				Cols: []schema.ColumnName{"customer_id"},
				Ref: schema.ForeignKeyRef{
					Schema: "public",
					Table:  "customers",
					Cols:   []schema.ColumnName{"id"},
				},
				OnDelete: schema.NoAction,
				OnUpdate: schema.NoAction,
				NotValid: true,
			},
		},
	}

	stmt, err := gen.GenerateCreateStatement(table)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CONSTRAINT orders_code_key UNIQUE (code) NOT VALID")
	assert.Contains(t, stmt, "CONSTRAINT orders_id_positive CHECK (id > 0) NOT VALID")
	assert.Contains(t, stmt, "CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES public.customers (id) NOT VALID")
}

func TestGenerateCreateIndex(t *testing.T) {
	gen := NewDDLGenerator()

	index := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email",
		Unique: false,
		Method: schema.BTree,
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "lower(email)"},
		},
	}

	stmt, err := gen.GenerateCreateStatement(index)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE INDEX idx_users_email")
	assert.Contains(t, stmt, "ON public.users")
	assert.Contains(t, stmt, "USING btree")
	assert.Contains(t, stmt, "lower(email)")
}

func TestGenerateCreateUniqueIndex(t *testing.T) {
	gen := NewDDLGenerator()

	index := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_users_email_unique",
		Unique: true,
		Method: schema.BTree,
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "email"},
		},
	}

	stmt, err := gen.GenerateCreateStatement(index)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE UNIQUE INDEX")
	assert.Contains(t, stmt, "idx_users_email_unique")
}

func TestGenerateCreatePartialIndex(t *testing.T) {
	gen := NewDDLGenerator()

	predicate := schema.Expr("deleted_at IS NULL")
	index := schema.Index{
		Schema: "public",
		Table:  "users",
		Name:   "idx_active_users",
		Unique: false,
		Method: schema.BTree,
		KeyExprs: []schema.IndexKeyExpr{
			{Expr: "email"},
		},
		Predicate: &predicate,
	}

	stmt, err := gen.GenerateCreateStatement(index)
	require.NoError(t, err)

	assert.Contains(t, stmt, "WHERE (deleted_at IS NULL)")
}

func TestGenerateCreateView(t *testing.T) {
	gen := NewDDLGenerator()

	view := schema.View{
		Schema: "public",
		Name:   "active_users",
		Type:   schema.RegularView,
		Definition: schema.ViewDefinition{
			Query: "SELECT * FROM users WHERE deleted_at IS NULL",
		},
	}

	stmt, err := gen.GenerateCreateStatement(view)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE VIEW public.active_users AS")
	assert.Contains(t, stmt, "SELECT * FROM users WHERE deleted_at IS NULL")
}

func TestGenerateCreateMaterializedView(t *testing.T) {
	gen := NewDDLGenerator()

	view := schema.View{
		Schema: "public",
		Name:   "user_stats",
		Type:   schema.MaterializedView,
		Definition: schema.ViewDefinition{
			Query: "SELECT user_id, COUNT(*) as post_count FROM posts GROUP BY user_id",
		},
	}

	stmt, err := gen.GenerateCreateStatement(view)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, stmt, "public.user_stats")
}

func TestGenerateCreateFunction(t *testing.T) {
	gen := NewDDLGenerator()

	arg1Name := "a"
	arg2Name := "b"
	fn := schema.Function{
		Schema:     "public",
		Name:       "add",
		Language:   "plpgsql",
		Volatility: schema.Immutable,
		Args: []schema.FunctionArg{
			{Name: &arg1Name, Type: "INTEGER"},
			{Name: &arg2Name, Type: "INTEGER"},
		},
		Returns: schema.ReturnsType{Type: "INTEGER"},
		Body:    "BEGIN RETURN a + b; END;",
	}

	stmt, err := gen.GenerateCreateStatement(fn)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE FUNCTION public.add")
	assert.Contains(t, stmt, "a INTEGER, b INTEGER")
	assert.Contains(t, stmt, "RETURNS INTEGER")
	assert.Contains(t, stmt, "LANGUAGE plpgsql")
	assert.Contains(t, stmt, "VOLATILITY IMMUTABLE")
	assert.Contains(t, stmt, "BEGIN RETURN a + b; END;")
}

func TestGenerateCreateSequence(t *testing.T) {
	gen := NewDDLGenerator()

	start := int64(1000)
	increment := int64(1)
	seq := schema.Sequence{
		Schema:    "public",
		Name:      "user_id_seq",
		Start:     &start,
		Increment: &increment,
		Cycle:     false,
	}

	stmt, err := gen.GenerateCreateStatement(seq)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE SEQUENCE public.user_id_seq")
	assert.Contains(t, stmt, "START 1000")
	assert.Contains(t, stmt, "INCREMENT 1")
}

func TestGenerateCreateEnum(t *testing.T) {
	gen := NewDDLGenerator()

	enum := schema.EnumDef{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive", "pending"},
	}

	stmt, err := gen.GenerateCreateStatement(enum)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE TYPE public.status AS ENUM")
	assert.Contains(t, stmt, "'active'")
	assert.Contains(t, stmt, "'inactive'")
	assert.Contains(t, stmt, "'pending'")
}

func TestGenerateCreateDomain(t *testing.T) {
	gen := NewDDLGenerator()

	checkExpr := schema.Expr("VALUE > 0")
	domain := schema.DomainDef{
		Schema:   "public",
		Name:     "positive_int",
		BaseType: "INTEGER",
		NotNull:  true,
		Check:    &checkExpr,
	}

	stmt, err := gen.GenerateCreateStatement(domain)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE DOMAIN public.positive_int AS INTEGER")
	assert.Contains(t, stmt, "NOT NULL")
	assert.Contains(t, stmt, "CHECK (VALUE > 0)")
}

func TestGenerateCreateExtension(t *testing.T) {
	gen := NewDDLGenerator()

	ext := schema.Extension{
		Schema: "public",
		Name:   "uuid-ossp",
	}

	stmt, err := gen.GenerateCreateStatement(ext)
	require.NoError(t, err)

	assert.Contains(t, stmt, "CREATE EXTENSION IF NOT EXISTS uuid-ossp")
}

func TestGenerateDropTable(t *testing.T) {
	gen := NewDDLGenerator()

	key := schema.ObjectKey{
		Kind:   schema.TableKind,
		Schema: "public",
		Name:   "users",
	}

	stmt, err := gen.generateDrop(key)
	require.NoError(t, err)

	assert.Contains(t, stmt, "DROP TABLE IF EXISTS public.users")
	assert.NotContains(t, stmt, "CASCADE")
}

func TestGenerateDropTableWithCascadeEnabled(t *testing.T) {
	gen := NewDDLGenerator(WithAllowCascade(true))

	key := schema.ObjectKey{
		Kind:   schema.TableKind,
		Schema: "public",
		Name:   "users",
	}

	stmt, err := gen.generateDrop(key)
	require.NoError(t, err)

	assert.Contains(t, stmt, "DROP TABLE IF EXISTS public.users")
	assert.Contains(t, stmt, "CASCADE")
}

func TestGenerateDropIndex(t *testing.T) {
	gen := NewDDLGenerator()

	key := schema.ObjectKey{
		Kind:   schema.IndexKind,
		Schema: "public",
		Name:   "idx_users_email",
	}

	stmt, err := gen.generateDrop(key)
	require.NoError(t, err)

	assert.Contains(t, stmt, "DROP INDEX IF EXISTS public.idx_users_email")
}

func TestGenerateDropView(t *testing.T) {
	gen := NewDDLGenerator()

	key := schema.ObjectKey{
		Kind:   schema.ViewKind,
		Schema: "public",
		Name:   "active_users",
	}

	stmt, err := gen.generateDrop(key)
	require.NoError(t, err)

	assert.Contains(t, stmt, "DROP VIEW IF EXISTS public.active_users")
}

func TestGenerateDDLFromDiff(t *testing.T) {
	gen := NewDDLGenerator()

	// Create a diff with various operations
	usersKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"}
	postsKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "posts"}
	oldTableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "old_table"}

	objectMap := schema.SchemaObjectMap{
		usersKey: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
					{Name: "email", Type: "TEXT", NotNull: true},
				},
			},
		},
		postsKey: {
			Payload: schema.Table{
				Schema: "public",
				Name:   "posts",
				Columns: []schema.Column{
					{Name: "id", Type: "INTEGER", NotNull: true},
				},
			},
		},
	}

	diff := &differ.Diff{
		ToCreate: []schema.ObjectKey{usersKey},
		ToDrop:   []schema.ObjectKey{oldTableKey},
		ToAlter:  []differ.AlterOperation{},
	}

	ddl, err := gen.GenerateDDL(diff, objectMap)
	require.NoError(t, err)

	// Should contain CREATE for users
	assert.Contains(t, ddl, "CREATE TABLE public.users")

	// Should contain DROP for old_table
	assert.Contains(t, ddl, "DROP TABLE IF EXISTS public.old_table")
}

func TestGenerateDDLOrdering(t *testing.T) {
	gen := NewDDLGenerator()

	// The DDL should be in order: CREATE, ALTER, DROP
	tableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "test"}
	oldTableKey := schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "old"}

	objectMap := schema.SchemaObjectMap{
		tableKey: {
			Payload: schema.Table{
				Schema:  "public",
				Name:    "test",
				Columns: []schema.Column{{Name: "id", Type: "INTEGER"}},
			},
		},
	}

	diff := &differ.Diff{
		ToCreate: []schema.ObjectKey{tableKey},
		ToDrop:   []schema.ObjectKey{oldTableKey},
		ToAlter:  []differ.AlterOperation{},
	}

	ddl, err := gen.GenerateDDL(diff, objectMap)
	require.NoError(t, err)

	lines := strings.Split(ddl, "\n")
	createIdx := -1
	dropIdx := -1

	for i, line := range lines {
		if strings.Contains(line, "CREATE TABLE") {
			createIdx = i
		}
		if strings.Contains(line, "DROP TABLE") {
			dropIdx = i
		}
	}

	// CREATE should come before DROP
	assert.Greater(t, dropIdx, createIdx, "DROP should come after CREATE")
}

func TestGenerateAlterTableAddGeneratedColumn(t *testing.T) {
	gen := NewDDLGenerator()

	genExpr := schema.Expr("COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')")

	oldTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
	}

	newTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
			{
				Name:      "full_name",
				Type:      "TEXT",
				Generated: &schema.GeneratedSpec{Expr: genExpr, Stored: true},
			},
		},
	}

	alter := differ.AlterOperation{
		Key:       schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"},
		Changes:   []string{"add column full_name"},
		OldObject: oldTable,
		NewObject: newTable,
	}

	statements, err := gen.generateAlterTable(newTable, &oldTable, alter)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	assert.Equal(t,
		"ALTER TABLE public.users ADD COLUMN full_name TEXT GENERATED ALWAYS AS (COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')) STORED;",
		statements[0])
}

func TestGenerateAlterTableModifyGeneratedColumn(t *testing.T) {
	gen := NewDDLGenerator()

	oldExpr := schema.Expr("COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')")
	newExpr := schema.Expr("UPPER(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))")

	oldTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
			{
				Name:      "full_name",
				Type:      "TEXT",
				Generated: &schema.GeneratedSpec{Expr: oldExpr, Stored: true},
			},
		},
	}

	newTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
			{
				Name:      "full_name",
				Type:      "TEXT",
				Generated: &schema.GeneratedSpec{Expr: newExpr, Stored: true},
			},
		},
	}

	alter := differ.AlterOperation{
		Key:       schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"},
		Changes:   []string{"alter column full_name: generated spec changed"},
		OldObject: oldTable,
		NewObject: newTable,
	}

	statements, err := gen.generateAlterTable(newTable, &oldTable, alter)
	require.NoError(t, err)
	require.Len(t, statements, 2)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN full_name DROP EXPRESSION;", statements[0])
	assert.Equal(t,
		"ALTER TABLE public.users ALTER COLUMN full_name ADD GENERATED ALWAYS AS (UPPER(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) STORED;",
		statements[1])
}

func TestGenerateAlterTableIdentitySpecChanged(t *testing.T) {
	gen := NewDDLGenerator()

	oldTable := schema.Table{
		Schema: "public",
		Name:   "accounts",
		Columns: []schema.Column{
			{
				Name:     "id",
				Type:     "BIGINT",
				Identity: &schema.IdentitySpec{Always: false},
			},
		},
	}

	newTable := schema.Table{
		Schema: "public",
		Name:   "accounts",
		Columns: []schema.Column{
			{
				Name: "id",
				Type: "BIGINT",
				Identity: &schema.IdentitySpec{
					Always: true,
					SequenceOptions: []schema.SequenceOption{
						{Type: "START WITH", Value: 100, HasValue: true},
						{Type: "INCREMENT BY", Value: 10, HasValue: true},
					},
				},
			},
		},
	}

	alter := differ.AlterOperation{
		Key:       schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "accounts"},
		Changes:   []string{"alter column id: identity spec changed"},
		OldObject: oldTable,
		NewObject: newTable,
	}

	statements, err := gen.generateAlterTable(newTable, &oldTable, alter)
	require.NoError(t, err)
	require.Len(t, statements, 2)
	assert.Equal(t, "ALTER TABLE public.accounts ALTER COLUMN id DROP IDENTITY IF EXISTS;", statements[0])
	assert.Equal(t, "ALTER TABLE public.accounts ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10);", statements[1])
}

func TestGenerateAlterTableHandlesCollationAndCommentChanges(t *testing.T) {
	gen := NewDDLGenerator()

	oldComment := "Old email comment"
	oldTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{
				Name:    "email",
				Type:    "TEXT",
				Comment: &oldComment,
			},
		},
	}

	collation := "en-US-x-icu"
	newComment := "New email comment"
	newTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{
				Name:      "email",
				Type:      "TEXT",
				Collation: &collation,
				Comment:   &newComment,
			},
		},
	}

	alter := differ.AlterOperation{
		Key:       schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"},
		Changes:   []string{"alter column email: collation changed", "alter column email: comment changed"},
		OldObject: oldTable,
		NewObject: newTable,
	}

	statements, err := gen.generateAlterTable(newTable, &oldTable, alter)
	require.NoError(t, err)

	require.Contains(t, statements, "ALTER TABLE public.users ALTER COLUMN email TYPE TEXT COLLATE \"en-US-x-icu\";")
	require.Contains(t, statements, "COMMENT ON COLUMN public.users.email IS 'New email comment';")
}

func TestGenerateAlterTableAddColumnWithCollationAndComment(t *testing.T) {
	gen := NewDDLGenerator()

	oldTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
	}

	collation := "en-US-x-icu"
	comment := "Display name"
	newTable := schema.Table{
		Schema: "public",
		Name:   "users",
		Columns: []schema.Column{
			{Name: "id", Type: "INTEGER"},
			{
				Name:      "display_name",
				Type:      "TEXT",
				Collation: &collation,
				Comment:   &comment,
			},
		},
	}

	alter := differ.AlterOperation{
		Key:       schema.ObjectKey{Kind: schema.TableKind, Schema: "public", Name: "users"},
		Changes:   []string{"add column display_name"},
		OldObject: oldTable,
		NewObject: newTable,
	}

	statements, err := gen.generateAlterTable(newTable, &oldTable, alter)
	require.NoError(t, err)

	require.Contains(t, statements, "ALTER TABLE public.users ADD COLUMN display_name TEXT COLLATE \"en-US-x-icu\";")
	require.Contains(t, statements, "COMMENT ON COLUMN public.users.display_name IS 'Display name';")
}
