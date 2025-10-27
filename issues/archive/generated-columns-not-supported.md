# Generated Columns Not Supported

## Summary

Schemata cannot properly handle PostgreSQL generated columns (computed columns). While it can detect that a column should be generated, it fails to:
1. Generate the initial `ADD COLUMN` statement with the `GENERATED ALWAYS AS` clause
2. Detect or generate DDL for changes to the generation expression
3. Output valid DDL - instead outputs `TODO` comments

## Severity

**High** - Generated columns are a common PostgreSQL feature (available since PostgreSQL 12). Users cannot use schemata to manage schemas containing generated columns.

## Environment

- PostgreSQL version: 17.x (tested on local instance)
- Schemata: latest (as of 2025-10-20)

## Steps to Reproduce

1. Add a generated column to schema.sql:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(201) GENERATED ALWAYS AS (COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')) STORED
);
```

2. Generate a migration:
```bash
schemata generate 'add full_name generated column'
```

3. The generated migration is incomplete - it's missing the `GENERATED ALWAYS AS` clause:
```sql
ALTER TABLE public.users ADD COLUMN full_name varchar(201);
```

4. Apply the migration:
```bash
schemata migrate
```

This fails the pre-flight check.

5. Check what schemata detects:
```bash
schemata diff --from migrations
```

## Expected Behavior

### Initial Migration Generation
The generated migration should include the complete column definition:

```sql
ALTER TABLE public.users ADD COLUMN full_name varchar(201)
    GENERATED ALWAYS AS (COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')) STORED;
```

### Change Detection
If the generation expression changes, schemata should generate DDL to handle it (typically requires dropping and recreating the column).

### Diff Output
Should provide actionable DDL, not TODO comments.

## Actual Behavior

### Initial Migration Generation
Generated migration is incomplete - missing the `GENERATED ALWAYS AS` clause:
```sql
ALTER TABLE public.users ADD COLUMN full_name varchar(201);
```

### Change Detection
Schemata detects the issue but outputs a TODO comment:
```
Objects to ALTER (1):
  ~ table: public.users
      alter column full_name: generated spec changed

DDL Preview:
---
-- TODO: ALTER TABLE public.users ALTER COLUMN full_name: generated spec changed
---
```

### Pre-flight Check
Migration application fails with:
```
Error: pre-flight check failed: migrations are out of sync with schema.sql:
  0 to create, 0 to drop, 1 to alter
```

## Impact

- **Cannot add generated columns** - migrations don't include the generation expression
- **Cannot modify generated columns** - no DDL generated for changes
- **Blocks migration application** - pre-flight check fails
- **No workaround available** - manual migration editing required

## Root Cause

Schemata's schema parser and DDL generator don't support the `GENERATED ALWAYS AS ... STORED` syntax. The code needs to:

1. **Parse generated column definitions** from schema.sql
   - Extract the generation expression
   - Determine if it's STORED or VIRTUAL (PostgreSQL only supports STORED)

2. **Query generated column info from PostgreSQL**
   - Query `pg_attribute.attgenerated` and `pg_attrdef.adbin` to get generation expressions from the database

3. **Generate proper DDL**
   - For ADD COLUMN: include the full `GENERATED ALWAYS AS` clause
   - For modifications: handle the fact that you can't directly alter a generated column's expression (requires DROP/ADD)

4. **Compare generation expressions**
   - Normalize expressions before comparison
   - Handle differences in whitespace, casting, etc.

## Suggested Implementation Areas

1. **Schema Parser** (`pkg/parser` or similar):
   - Add support for parsing `GENERATED ALWAYS AS` syntax
   - Store generation expression in column metadata

2. **Database Queries** (PostgreSQL catalog queries):
   - Query `information_schema.columns.generation_expression` or
   - Query `pg_attribute.attgenerated` + `pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid)`

3. **DDL Generator**:
   - Include generation clause when generating ADD COLUMN statements
   - Generate appropriate DDL for changes (likely DROP + ADD COLUMN)

4. **Schema Comparison**:
   - Compare generation expressions
   - Normalize expressions for accurate comparison

## PostgreSQL Documentation

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- Available since PostgreSQL 12
- Syntax: `column_name data_type GENERATED ALWAYS AS (expression) STORED`

## Reproduction Repository

Test case available in: `/Users/jackhodkinson/code/test-schemata`

Files:
- Schema: `schema.sql` (includes generated column)
- Migration: `migrations/20251020161833-add-full-name-generated-column.sql` (incomplete)

## Related Features

This issue likely affects other computed/derived column features:
- Identity columns (`GENERATED ALWAYS AS IDENTITY`)
- Virtual generated columns (if PostgreSQL adds support in the future)

## Additional Context

This was discovered during systematic testing of PostgreSQL features. Generated columns are widely used for:
- Computed values (like full names from parts)
- Normalized/transformed data (like lowercase email for searches)
- Extracted JSON fields
- Full-text search vectors (though `tsvector` has its own considerations)
