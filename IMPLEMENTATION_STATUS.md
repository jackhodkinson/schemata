# Schemata Implementation Status

## Overview
This document tracks the implementation progress of the Schemata CLI tool - a declarative Postgres schema migration tool built in Go.

**Last Updated:** 2025-10-16

---

## ✅ Completed Components

### 1. Project Foundation
- ✅ Go module initialized (`github.com/jackhodkinson/schemata`)
- ✅ Dependencies added:
  - `github.com/pganalyze/pg_query_go/v5` - SQL parsing via libpg_query
  - `github.com/jackc/pgx/v5` - Postgres driver and connection pooling
  - `github.com/spf13/cobra` - CLI framework
  - `gopkg.in/yaml.v3` - YAML config parsing
  - `github.com/stretchr/testify` - Testing framework
- ✅ Project structure following Go best practices
- ✅ Docker Compose environment for testing (3 Postgres instances: dev, target, staging)

### 2. Schema Types (`pkg/schema/types.go`)
- ✅ Comprehensive type definitions for all Postgres objects:
  - Tables, Columns, Constraints (PK, UNIQUE, CHECK, FK)
  - Indexes (with expressions, predicates, include columns)
  - Views (regular and materialized)
  - Functions (with arguments, returns, volatility, parallel safety)
  - Sequences (with full configuration)
  - Enums, Domains, Composite types
  - Triggers, Policies
  - Extensions, Grants
- ✅ Strong typing using type aliases (SchemaName, TableName, ColumnName, etc.)
- ✅ ObjectKey system for unique identification in diff algorithm
- ✅ HashedObject wrapper for efficient comparison

### 3. Configuration System (`internal/config/`)
- ✅ YAML configuration parsing
- ✅ Environment variable expansion (`${VAR}` and `${VAR:-default}` syntax)
- ✅ Multiple connection formats:
  - Simple URL: `target: ${TARGET_URL}`
  - Structured: `{host, port, username, password, database, ssl}`
- ✅ Multi-target support: `targets: {prod: ..., staging: ...}`
- ✅ Schema filtering (include/exclude schemas)
- ✅ SSL configuration (all modes: disable, allow, prefer, require, verify-ca, verify-full)
- ✅ Connection string builder
- ✅ Config validation and auto-detection of environment variables

### 4. Database Connection (`internal/db/connection.go`)
- ✅ Connection pooling using pgx/v5
- ✅ Connection health checks
- ✅ Graceful connection closing

### 5. Migration Tracking (`internal/db/migration_tracker.go`)
- ✅ `schemata.version` table management
- ✅ Track applied migrations by version (timestamp)
- ✅ Query applied/pending versions
- ✅ Mark migrations as applied
- ✅ Atomic operations with proper error handling

### 6. Catalog Introspection (`internal/db/catalog.go`)
- ✅ Query all Postgres system catalogs:
  - `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`
  - `pg_constraint`, `pg_index`, `pg_proc`, `pg_trigger`
  - `pg_policy`, `pg_sequence`, `pg_extension`, `pg_enum`
- ✅ Extract complete schema information:
  - Tables with columns, constraints, indexes, partitions
  - Views (including materialized views)
  - Functions with full signatures
  - Triggers, Policies, Sequences, Enums, Domains, Extensions
- ✅ Schema filtering support (include/exclude lists)
- ✅ Filter implicit indexes (backing PK/UNIQUE constraints)
- ✅ Proper normalization of catalog data

### 7. SQL Parser (`internal/parser/parser.go`)
- ✅ **CRITICAL:** Uses `pg_query_go` (libpg_query wrapper) for parsing
- ✅ Parse SQL files into structured schema objects
- ✅ Support for all major DDL statements:
  - CREATE TABLE (with all constraint types)
  - CREATE INDEX (with expressions, predicates, include)
  - CREATE VIEW/MATERIALIZED VIEW
  - CREATE FUNCTION (with arguments, returns, options)
  - CREATE SEQUENCE (with all options)
  - CREATE TYPE (enum, domain, composite)
  - CREATE EXTENSION
  - CREATE TRIGGER
  - CREATE POLICY
- ✅ Extract normalized objects from parsed AST
- ✅ Proper handling of schema-qualified names
- ⚠️ **Known Issue:** CGO compilation error on macOS (pg_query_go C library issue)

### 8. Schema Differ (`internal/differ/`)
- ✅ Stable SHA-256 hashing of objects
- ✅ Normalization for consistent comparison
- ✅ Three-phase diff algorithm:
  - Set operations: to_create, to_drop, maybe_alter
  - Deep comparison for altered objects
  - Type-specific comparators
- ✅ Detailed change tracking for:
  - Tables: column changes, constraint changes, option changes
  - Indexes: method, expressions, predicates, uniqueness
  - Views: definition changes
  - Functions: signature, body, volatility changes
  - Sequences: parameter changes
  - Enums: value changes (order-sensitive)
  - Domains: base type, constraints, defaults
- ✅ IsEmpty() check for no-op diffs

### 9. DDL Generator (`internal/planner/ddl.go`)
- ✅ Generate CREATE statements for all object types
- ✅ Generate DROP statements with CASCADE where appropriate
- ✅ Generate ALTER statements (basic implementation)
- ✅ Proper SQL formatting and quoting
- ✅ Support for complex constructs:
  - Foreign keys with ON DELETE/UPDATE actions
  - Check constraints with expressions
  - Partial indexes with WHERE clauses
  - Generated columns
  - Identity columns
  - Sequence options

### 10. Migration System (`internal/migration/`)
- ✅ **Scanner** (`scanner.go`):
  - Scan migration directory for `*.sql` files
  - Parse filename format: `YYYYMMDDHHMMSS-name.sql`
  - Sort migrations by version
  - Load SQL content on demand
- ✅ **Generator** (`generator.go`):
  - Generate timestamp-based versions
  - Convert names to kebab-case
  - Create migration files with SQL content
  - Validate migration names
- ✅ **Applier** (`applier.go`):
  - Apply migrations in transactions
  - Track applied versions
  - Support for dry-run mode
  - Optional continue-on-error
  - Rollback on failure

### 11. CLI Commands (`internal/cli/`, `cmd/schemata/`)
- ✅ **Root command** (`root.go`):
  - Global flags: `--config`, `--verbose`
  - Help and version info
- ✅ **init** (`init.go`):
  - Create configuration file
  - Support for dev/target/multiple targets
  - Auto-detect environment variables
- ✅ **dump** (`dump.go`):
  - Extract schema from target DB
  - Generate DDL
  - Write to schema file
  - Schema filtering support
- ✅ **create** (`create.go`):
  - Create empty migration files
  - Proper filename generation
- ✅ **apply** (`apply.go`):
  - Low-level migration application
  - Support for `--dev` or `--target`
  - Dry-run support
  - Transaction-based application
- ⚠️ **generate** (`generate.go`): Stub created, needs implementation
- ⚠️ **migrate** (`migrate.go`): Stub created, needs implementation
- ⚠️ **diff** (`diff.go`): Stub created, needs implementation

---

## 🚧 In Progress / Needs Implementation

### High Priority
1. **Fix pg_query_go CGO build issue on macOS**
   - Current error: `strchrnul` symbol conflict
   - May need to use Docker for parser or find workaround
   - Alternative: Use pg_query_go v4 or different build flags

2. **Complete generate command**
   - Apply migrations to dev DB
   - Parse schema.sql using parser
   - Build object maps with hashing
   - Diff dev DB vs schema.sql
   - Generate DDL from diff
   - Create migration file

3. **Complete migrate command**
   - Check migrations match schema.sql (via diff)
   - Apply pending migrations to target
   - Proper error messages

4. **Complete diff command**
   - Compare target DB to schema.sql (default)
   - Compare dev DB (with migrations) to schema.sql (`--from migrations`)
   - Display human-readable diff

### Medium Priority
5. **Improve ALTER statement generation**
   - Current implementation generates TODOs
   - Need proper column ADD/DROP/ALTER logic
   - Need constraint recreation logic

6. **Dependency graph and topological sort**
   - For complex schemas with dependencies
   - Proper order for CREATE/DROP operations
   - Handle circular dependencies

7. **Object normalization enhancements**
   - Normalize function bodies (whitespace, etc.)
   - Normalize view queries
   - Handle more edge cases

8. **Parser improvements**
   - Better error messages with line numbers
   - Handle more SQL constructs
   - Support for ALTER statements (if parsing existing migrations)

### Low Priority
9. **Rename detection**
   - Detect when object is renamed vs dropped+created
   - Optimize migrations to use RENAME

10. **Rollback support**
    - Store down migrations
    - Implement rollback command

11. **Migration verification**
    - Checksum verification
    - Detect manual changes to applied migrations

---

## 🧪 Testing Status

### Unit Tests
- ❌ Config package tests
- ❌ Parser tests
- ❌ Differ tests
- ❌ DDL generator tests
- ❌ Migration scanner/generator tests

### Integration Tests
- ❌ End-to-end workflow tests
- ❌ Database connection tests
- ❌ Migration application tests
- ❌ Multi-target tests

### Test Infrastructure
- ✅ Docker Compose environment ready
- ✅ Three test databases configured
- ❌ Test fixtures (schemas, migrations)
- ❌ Test helper utilities

---

## 📋 Architecture Highlights

### Modular Design
- **Clear separation of concerns:**
  - `pkg/schema` - Public schema types
  - `internal/config` - Configuration handling
  - `internal/db` - Database operations
  - `internal/parser` - SQL parsing (pg_query_go)
  - `internal/differ` - Schema comparison
  - `internal/planner` - DDL generation
  - `internal/migration` - Migration management
  - `internal/cli` - Command handlers
  - `cmd/schemata` - Entry point

### Key Design Decisions
1. **Used pg_query_go** - Critical requirement, uses official libpg_query
2. **Hashed objects** - Efficient comparison without deep inspection
3. **Type-safe** - Strong typing throughout with type aliases
4. **Transaction-safe** - All migrations in transactions
5. **Extensible** - Easy to add new object types
6. **Testable** - Interfaces and dependency injection where appropriate

### Code Quality
- ✅ Follows Go idioms and conventions
- ✅ Clear error messages with context
- ✅ Proper resource cleanup (defer, Close())
- ✅ Consistent naming conventions
- ✅ Well-organized package structure

---

## 🔧 Known Issues

1. **CGO Build Error (macOS)**
   - **Issue:** pg_query_go v5 has C compilation error on macOS
   - **Impact:** Parser cannot be built/tested
   - **Workaround:** Use Docker for development or try v4
   - **Status:** Blocking parser functionality

2. **Incomplete ALTER Generation**
   - **Issue:** ALTER statements generate TODOs
   - **Impact:** Cannot modify existing objects in migrations
   - **Workaround:** Manual ALTER or DROP/CREATE
   - **Status:** Medium priority fix

3. **No Dependency Ordering**
   - **Issue:** Objects created/dropped in arbitrary order
   - **Impact:** May fail for schemas with dependencies
   - **Workaround:** Manual ordering in migrations
   - **Status:** Medium priority fix

---

## 🎯 Next Steps

### Immediate (To Get Working System)
1. Resolve pg_query_go build issue
2. Implement generate command
3. Implement migrate command
4. Implement diff command
5. Write basic integration tests

### Short Term (First Release)
1. Improve ALTER statement generation
2. Add dependency graph ordering
3. Write comprehensive test suite
4. Add detailed documentation
5. Test with real-world schemas

### Long Term (Future Enhancements)
1. Rename detection
2. Rollback support
3. Migration verification/checksums
4. Performance optimizations (Merkle trees)
5. Interactive init command
6. Schema visualization
7. Migration conflict detection

---

## 📊 Code Statistics

```
Estimated Lines of Code:
- pkg/schema/types.go: ~600 lines
- internal/config/: ~400 lines
- internal/db/: ~800 lines (catalog ~700, connection ~50, migration_tracker ~150)
- internal/parser/: ~900 lines
- internal/differ/: ~400 lines
- internal/planner/: ~450 lines
- internal/migration/: ~300 lines
- internal/cli/: ~400 lines
- cmd/schemata/: ~10 lines

Total: ~4,260 lines of Go code
```

### Test Coverage
- Current: 0% (no tests written yet)
- Target: >80% for critical paths

---

## 💡 Key Learnings from Haskell Attempt

1. **Parser is critical** - Using pg_query_go ensures robustness
2. **Normalization is complex** - Need to handle many edge cases
3. **Constraint naming** - Must strip auto-generated CHECK names
4. **Implicit indexes** - Must filter indexes backing PK/UNIQUE
5. **Type system helps** - Strong typing caught many issues early
6. **Catalog queries are complex** - Lots of joins and edge cases

---

## 🤝 Contributing

### Code Standards
- Follow Go conventions (gofmt, golint)
- Write tests for new features
- Update this document with changes
- Keep modules focused and small
- Document complex logic

### Testing Requirements
- Unit tests for all packages
- Integration tests for workflows
- Test edge cases and error paths
- High-quality tests over quantity

---

## 📝 Notes

- This implementation follows the plan in `PLAN.md`
- Architecture details in `ARCHITECTURE.md` (read-only)
- User docs in `README.md` (read-only)
- All documentation auto-updated except README and ARCHITECTURE
